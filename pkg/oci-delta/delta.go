package ocidelta

import (
	"encoding/json"
	"fmt"
	"io"

	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	mediaTypeDelta              = "application/vnd.redhat.oci-delta.v1"
	mediaTypeTarDiff            = "application/vnd.tar-diff"
	annotationDeltaTarget       = "io.github.containers.delta.target"
	annotationDeltaSource       = "io.github.containers.delta.source"
	annotationDeltaSourceConfig = "io.github.containers.delta.source-config"
	annotationDeltaTo           = "io.github.containers.delta.to"
	annotationDeltaReused       = "io.github.containers.delta.reused"
	annotationDeltaReusedDiffID = "io.github.containers.delta.reused-diff-id"
	annotationDeltaContent      = "io.github.containers.delta.content"
)

type DeltaArtifact struct {
	reader              OCIReader
	imageManifest       v1.Manifest
	imageConfig         v1.Image
	imageManifestDigest digest.Digest
	imageConfigDigest   digest.Digest
	sourceConfigDigest  string
	deltaLayerByTo      map[digest.Digest]v1.Descriptor
}

func ParseDeltaArtifact(reader OCIReader, log Logger) (*DeltaArtifact, error) {
	indexData, err := readAll(reader, "index.json")
	if err != nil {
		return nil, fmt.Errorf("delta does not contain index.json")
	}
	var ociIndex v1.Index
	if err := json.Unmarshal(indexData, &ociIndex); err != nil {
		return nil, fmt.Errorf("failed to parse index.json: %w", err)
	}
	if len(ociIndex.Manifests) == 0 {
		return nil, fmt.Errorf("delta contains no manifests")
	}

	deltaManifestDigest := ociIndex.Manifests[0].Digest
	log.Debug("  Delta manifest: %s", deltaManifestDigest.Encoded()[:16])

	deltaManifestData, err := readAll(reader, blobTarName(deltaManifestDigest))
	if err != nil {
		return nil, fmt.Errorf("failed to read delta manifest: %w", err)
	}
	var deltaManifest v1.Manifest
	if err := json.Unmarshal(deltaManifestData, &deltaManifest); err != nil {
		return nil, fmt.Errorf("failed to parse delta manifest: %w", err)
	}
	if deltaManifest.ArtifactType != mediaTypeDelta {
		return nil, fmt.Errorf("not a delta artifact (artifactType: %s)", deltaManifest.ArtifactType)
	}

	sourceConfigDigest := deltaManifest.Annotations[annotationDeltaSourceConfig]

	var imageManifestDesc, imageConfigDesc *v1.Descriptor
	deltaLayerByTo := make(map[digest.Digest]v1.Descriptor)
	for i := range deltaManifest.Layers {
		layer := &deltaManifest.Layers[i]
		switch layer.Annotations[annotationDeltaContent] {
		case "image-manifest":
			imageManifestDesc = layer
		case "image-config":
			imageConfigDesc = layer
		case "image-layer":
			toStr := layer.Annotations[annotationDeltaTo]
			if toStr == "" {
				continue
			}
			toDigest, err := digest.Parse(toStr)
			if err != nil {
				log.Warning("invalid delta.to annotation %q: %v", toStr, err)
				continue
			}
			deltaLayerByTo[toDigest] = *layer
		}
	}
	if imageManifestDesc == nil {
		return nil, fmt.Errorf("delta manifest contains no embedded image manifest layer")
	}
	if imageConfigDesc == nil {
		return nil, fmt.Errorf("delta manifest contains no embedded image config layer")
	}

	imageManifestData, err := readAll(reader, blobTarName(imageManifestDesc.Digest))
	if err != nil {
		return nil, fmt.Errorf("failed to read embedded image manifest: %w", err)
	}
	var imageManifest v1.Manifest
	if err := json.Unmarshal(imageManifestData, &imageManifest); err != nil {
		return nil, fmt.Errorf("failed to parse embedded image manifest: %w", err)
	}
	log.Debug("  Image manifest: %s (%d layers)", imageManifestDesc.Digest.Encoded()[:16], len(imageManifest.Layers))

	imageConfigData, err := readAll(reader, blobTarName(imageConfigDesc.Digest))
	if err != nil {
		return nil, fmt.Errorf("failed to read embedded image config: %w", err)
	}
	var imageConfig v1.Image
	if err := json.Unmarshal(imageConfigData, &imageConfig); err != nil {
		return nil, fmt.Errorf("failed to parse embedded image config: %w", err)
	}
	log.Debug("  Image config: %s (%d diff_ids)", imageConfigDesc.Digest.Encoded()[:16], len(imageConfig.RootFS.DiffIDs))

	return &DeltaArtifact{
		reader:              reader,
		imageManifest:       imageManifest,
		imageConfig:         imageConfig,
		imageManifestDigest: imageManifestDesc.Digest,
		imageConfigDigest:   imageConfigDesc.Digest,
		sourceConfigDigest:  sourceConfigDigest,
		deltaLayerByTo:      deltaLayerByTo,
	}, nil
}

func (d *DeltaArtifact) Close() error {
	return d.reader.Close()
}

func (d *DeltaArtifact) SourceConfigDigest() string {
	return d.sourceConfigDigest
}

func (d *DeltaArtifact) ReadBlob(dgst digest.Digest) ([]byte, error) {
	return readAll(d.reader, blobTarName(dgst))
}

func (d *DeltaArtifact) GetBlobReader(dgst digest.Digest) (io.ReadSeekCloser, error) {
	r, _, err := d.reader.ReadFile(blobTarName(dgst))
	return r, err
}

func (d *DeltaArtifact) GetBlobSize(dgst digest.Digest) (int64, error) {
	r, size, err := d.reader.ReadFile(blobTarName(dgst))
	if err != nil {
		return 0, err
	}
	r.Close()
	return size, nil
}
