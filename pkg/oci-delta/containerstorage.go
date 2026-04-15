package ocidelta

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/containers/storage"
	"github.com/containers/storage/types"
	tarpatch "github.com/containers/tar-diff/pkg/tar-patch"
	digest "github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type containerStorageDataSource struct {
	tarpatch.DataSource
	store   storage.Store
	imageID string
}

func (s *containerStorageDataSource) Cleanup() error {
	_, err := s.store.UnmountImage(s.imageID, true)
	return err
}

func OpenContainerStorage(graphRoot string) (storage.Store, error) {
	storeOpts, err := types.DefaultStoreOptions()
	if err != nil {
		return nil, fmt.Errorf("failed to get default store options: %w", err)
	}
	if graphRoot != "" {
		storeOpts.GraphRoot = graphRoot
	}

	store, err := storage.GetStore(storeOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open container storage: %w", err)
	}
	return store, nil
}

func findImageByConfigDigest(store storage.Store, configDigest string, log Logger) (string, error) {
	images, err := store.Images()
	if err != nil {
		return "", fmt.Errorf("failed to list images: %w", err)
	}

	log.Debug("Found %d images in container storage", len(images))

	for _, img := range images {
		manifestData, err := store.ImageBigData(img.ID, "manifest")
		if err != nil {
			continue
		}

		var manifest struct {
			Config struct {
				Digest string `json:"digest"`
			} `json:"config"`
		}
		if err := json.Unmarshal(manifestData, &manifest); err != nil {
			continue
		}

		log.Debug("  Image %s: config digest %s", img.ID[:16], manifest.Config.Digest)

		if manifest.Config.Digest == configDigest {
			log.Debug("Matched image: %s", img.ID[:16])
			return img.ID, nil
		}
	}

	return "", fmt.Errorf("no image found with config digest %s", configDigest)
}

func ResolveContainerStorageDataSource(store storage.Store, sourceConfigDigest string, log Logger) (DataSource, error) {
	imageID, err := findImageByConfigDigest(store, sourceConfigDigest, log)
	if err != nil {
		return nil, err
	}

	mountPoint, err := store.MountImage(imageID, nil, "")
	if err != nil {
		return nil, fmt.Errorf("failed to mount image %s: %w", imageID[:16], err)
	}

	log.Debug("Mounted image at %s", mountPoint)

	return &containerStorageDataSource{
		DataSource: tarpatch.NewFilesystemDataSource(mountPoint),
		store:      store,
		imageID:    imageID,
	}, nil
}

func imageHasSignatures(store storage.Store, imageID string) (bool, error) {
	sizes, _, err := getSignatureSizes(store, imageID)
	if err != nil {
		return false, err
	}
	return len(sizes) > 0, nil
}

const sigstoreJSONPrefix = "\x00sigstore-json\n"

type sigstoreJSONRepresentation struct {
	MIMEType    string            `json:"mimeType"`
	Payload     []byte            `json:"payload"`
	Annotations map[string]string `json:"annotations"`
}

type storageImageMetadata struct {
	SignatureSizes  []int                   `json:"signature-sizes,omitempty"`
	SignaturesSizes map[digest.Digest][]int `json:"signatures-sizes,omitempty"`
}

func getSignatureSizes(store storage.Store, imageID string) ([]int, string, error) {
	img, err := store.Image(imageID)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get image: %w", err)
	}

	var meta storageImageMetadata
	if img.Metadata != "" {
		if err := json.Unmarshal([]byte(img.Metadata), &meta); err != nil {
			return nil, "", fmt.Errorf("failed to parse image metadata: %w", err)
		}
	}

	if len(meta.SignatureSizes) > 0 {
		return meta.SignatureSizes, "signatures", nil
	}

	manifestData, err := store.ImageBigData(imageID, "manifest")
	if err != nil {
		return nil, "", nil
	}
	manifestDigest := digest.FromBytes(manifestData)
	if sizes, ok := meta.SignaturesSizes[manifestDigest]; ok && len(sizes) > 0 {
		key := "signature-" + manifestDigest.Encoded()
		return sizes, key, nil
	}

	return nil, "", nil
}

func parseSigstoreBlobs(blob []byte, sizes []int) []sigstoreJSONRepresentation {
	var sigs []sigstoreJSONRepresentation
	offset := 0
	for _, size := range sizes {
		if offset+size > len(blob) {
			break
		}
		raw := blob[offset : offset+size]
		offset += size

		if !strings.HasPrefix(string(raw), sigstoreJSONPrefix) {
			continue
		}
		jsonData := raw[len(sigstoreJSONPrefix):]

		var rep sigstoreJSONRepresentation
		if err := json.Unmarshal(jsonData, &rep); err != nil {
			continue
		}
		sigs = append(sigs, rep)
	}
	return sigs
}

func buildSignatureArtifact(sigs []sigstoreJSONRepresentation) (*signatureArtifact, error) {
	var layers []v1.Descriptor
	blobs := make(map[digest.Digest][]byte)

	for _, sig := range sigs {
		payloadDigest := digest.FromBytes(sig.Payload)
		blobs[payloadDigest] = sig.Payload

		annotations := make(map[string]string)
		for k, v := range sig.Annotations {
			annotations[k] = v
		}

		layers = append(layers, v1.Descriptor{
			MediaType:   sig.MIMEType,
			Digest:      payloadDigest,
			Size:        int64(len(sig.Payload)),
			Annotations: annotations,
		})
	}

	if len(layers) == 0 {
		return nil, fmt.Errorf("no valid sigstore signatures found")
	}

	configData := []byte("{}")
	configDigest := digest.FromBytes(configData)
	blobs[configDigest] = configData

	manifest := v1.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		Config: v1.Descriptor{
			MediaType: v1.MediaTypeImageConfig,
			Digest:    configDigest,
			Size:      int64(len(configData)),
		},
		Layers: layers,
	}
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal signature manifest: %w", err)
	}
	manifestDigest := digest.FromBytes(manifestData)

	return &signatureArtifact{
		manifestData:   manifestData,
		manifestDigest: manifestDigest,
		manifest:       manifest,
		blobs:          blobs,
	}, nil
}

func ExtractContainerStorageSignatures(store storage.Store, imageID string, log Logger) ([]OCIReader, error) {
	sizes, key, err := getSignatureSizes(store, imageID)
	if err != nil {
		return nil, err
	}
	if len(sizes) == 0 {
		if log != nil {
			log.Debug("No signatures found in container storage for image %s", imageID[:16])
		}
		return nil, nil
	}

	blob, err := store.ImageBigData(imageID, key)
	if err != nil {
		return nil, fmt.Errorf("failed to read signature data (key %s): %w", key, err)
	}

	sigstoreSigs := parseSigstoreBlobs(blob, sizes)
	if len(sigstoreSigs) == 0 {
		if log != nil {
			log.Debug("No sigstore signatures found in container storage for image %s", imageID[:16])
		}
		return nil, nil
	}

	if log != nil {
		log.Debug("Found %d sigstore signature(s) in container storage for image %s", len(sigstoreSigs), imageID[:16])
	}

	artifact, err := buildSignatureArtifact(sigstoreSigs)
	if err != nil {
		return nil, err
	}

	blobs := make(map[digest.Digest][]byte)
	blobs[artifact.manifestDigest] = artifact.manifestData
	for d, data := range artifact.blobs {
		blobs[d] = data
	}

	return []OCIReader{&memOCIReader{
		manifestDigest: artifact.manifestDigest,
		blobs:          blobs,
	}}, nil
}
