package bootcdelta

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	tarpatch "github.com/containers/tar-diff/pkg/tar-patch"
	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type ApplyOptions struct {
	DeltaPath   string
	OutputPath  string
	DeltaSource string
	TmpDir      string
	Debug       func(format string, args ...interface{})
	Warning     func(format string, args ...interface{})
}

type deltaMetadata struct {
	index          v1.Index
	manifestDigest digest.Digest
	manifest       v1.Manifest
	layerDigests   map[digest.Digest]bool
	layerToDiffID  map[digest.Digest]digest.Digest
}

func parseDeltaMetadata(opts *ApplyOptions, r io.Reader) (*deltaMetadata, error) {
	deltaTar := tar.NewReader(r)

	var index v1.Index
	var manifestDigest digest.Digest
	var manifest v1.Manifest
	var config v1.Image
	layerDigests := make(map[digest.Digest]bool)
	layerToDiffID := make(map[digest.Digest]digest.Digest)

	blobs := make(map[digest.Digest][]byte)

	for {
		header, err := deltaTar.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read delta tar: %w", err)
		}

		if header.Name == "index.json" {
			data, err := io.ReadAll(deltaTar)
			if err != nil {
				return nil, fmt.Errorf("failed to read index.json: %w", err)
			}
			if err := json.Unmarshal(data, &index); err != nil {
				return nil, fmt.Errorf("failed to parse index.json: %w", err)
			}
			if len(index.Manifests) == 0 {
				return nil, fmt.Errorf("delta archive contains no manifests")
			}
			if len(index.Manifests) > 1 {
				return nil, fmt.Errorf("delta archive contains multiple manifests (%d), only single-image archives are supported", len(index.Manifests))
			}
			manifestDigest = index.Manifests[0].Digest
			opts.Debug("  Index points to manifest: %s", manifestDigest.Encoded()[:16])
			continue
		}

		if isBlobPath(header.Name) && header.Size < maxBlobSizeToReadInMemory {
			d := digestFromBlobPath(header.Name)
			data, err := io.ReadAll(deltaTar)
			if err != nil {
				continue
			}
			blobs[d] = data
		} else {
			_, _ = io.Copy(io.Discard, deltaTar)
		}
	}

	// Ensure index.json was found
	if manifestDigest == "" {
		return nil, fmt.Errorf("delta archive does not contain index.json")
	}

	// Now follow the chain: index → manifest → config
	manifestData, ok := blobs[manifestDigest]
	if !ok {
		return nil, fmt.Errorf("manifest %s referenced by index not found in delta", manifestDigest.Encoded()[:16])
	}
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return nil, fmt.Errorf("failed to parse manifest: %w", err)
	}
	opts.Debug("  Found manifest: %s with %d layers", manifestDigest.Encoded()[:16], len(manifest.Layers))

	configDigest := manifest.Config.Digest
	for _, layer := range manifest.Layers {
		layerDigests[layer.Digest] = true
	}

	if configDigest == "" {
		return nil, fmt.Errorf("manifest has no config digest")
	}
	configData, ok := blobs[configDigest]
	if !ok {
		return nil, fmt.Errorf("config %s referenced by manifest not found in delta", configDigest.Encoded()[:16])
	}
	if err := json.Unmarshal(configData, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}
	opts.Debug("  Found config: %s with %d diff_ids", configDigest.Encoded()[:16], len(config.RootFS.DiffIDs))

	for i, layer := range manifest.Layers {
		if i < len(config.RootFS.DiffIDs) {
			layerToDiffID[layer.Digest] = config.RootFS.DiffIDs[i]
		}
	}

	opts.Debug("Found %d layer digests", len(layerDigests))

	return &deltaMetadata{
		index:          index,
		manifestDigest: manifestDigest,
		manifest:       manifest,
		layerDigests:   layerDigests,
		layerToDiffID:  layerToDiffID,
	}, nil
}

func ApplyDelta(opts ApplyOptions) error {
	opts.Debug("Applying delta: %s", opts.DeltaPath)
	opts.Debug("Output: %s", opts.OutputPath)
	opts.Debug("Delta source: %s", opts.DeltaSource)

	// First pass: read delta archive to find manifests, configs, and layer info
	opts.Debug("\nAnalyzing delta file...")
	deltaFile, err := os.Open(opts.DeltaPath)
	if err != nil {
		return fmt.Errorf("failed to open delta file: %w", err)
	}
	defer deltaFile.Close()

	metadata, err := parseDeltaMetadata(&opts, deltaFile)
	if err != nil {
		return err
	}

	// Second pass: stream through delta and write output
	opts.Debug("\nProcessing delta file...")
	deltaFile.Seek(0, 0)

	deltaTar2 := tar.NewReader(deltaFile)

	digestMapping := make(map[digest.Digest]digest.Digest)
	blobSizes := make(map[digest.Digest]int64)

	outFile, err := os.Create(opts.OutputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	tarWriter := tar.NewWriter(outFile)
	defer tarWriter.Close()

	for {
		header, err := deltaTar2.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read delta tar: %w", err)
		}

		// Handle oci-layout
		if header.Name == "oci-layout" {
			opts.Debug("\nWriting oci-layout")
			if err := copyTarEntry(tarWriter, header, deltaTar2); err != nil {
				return err
			}
			continue
		}

		// Handle index.json - will be rewritten later
		if header.Name == "index.json" {
			_, _ = io.Copy(io.Discard, deltaTar2)
			continue
		}

		// Handle blobs
		if isBlobPath(header.Name) {
			d := digestFromBlobPath(header.Name)

			// Skip if this is the manifest (will be rewritten)
			if d == metadata.manifestDigest {
				_, _ = io.Copy(io.Discard, deltaTar2)
				continue
			}

			// Handle layers
			if metadata.layerDigests[d] {
				// Peek to check if tar-diff
				buffered := bufio.NewReader(deltaTar2)
				peek, err := buffered.Peek(8)
				if err != nil && err != io.EOF {
					return err
				}

				if isTarDiff(peek) {
					// Tar-diff layer - stream to reconstruction
					if err := processLayerDiff(&opts, tarWriter, d, buffered, metadata.layerToDiffID, &digestMapping, &blobSizes); err != nil {
						return err
					}
				} else {
					// Regular compressed layer - stream directly
					opts.Debug("  Copying compressed layer %s (%d bytes)", d.Encoded()[:16], header.Size)
					if err := writeTarFileFromReader(tarWriter, blobTarName(d), header.Size, buffered); err != nil {
						return err
					}
				}
				continue
			}

			// Copy non-layer, non-manifest blobs as-is
			if err := copyTarEntry(tarWriter, header, deltaTar2); err != nil {
				return err
			}
			continue
		}

		// Copy any other files as-is
		if err := copyTarEntry(tarWriter, header, deltaTar2); err != nil {
			return err
		}
	}

	opts.Debug("\nWriting updated manifest...")

	// Update layer digests in the manifest for reconstructed layers
	for i := range metadata.manifest.Layers {
		if newDigest, ok := digestMapping[metadata.manifest.Layers[i].Digest]; ok {
			if metadata.manifest.Layers[i].Annotations == nil {
				metadata.manifest.Layers[i].Annotations = make(map[string]string)
			}
			metadata.manifest.Layers[i].Annotations["org.containers.bootc.delta.original-digest"] = metadata.manifest.Layers[i].Digest.String()
			metadata.manifest.Layers[i].Digest = newDigest
			metadata.manifest.Layers[i].Size = blobSizes[newDigest]
		}
	}

	manifestJSON, err := json.Marshal(metadata.manifest)
	if err != nil {
		return err
	}

	hManifest := sha256.New()
	hManifest.Write(manifestJSON)
	newManifestDigest := digest.NewDigestFromBytes(digest.SHA256, hManifest.Sum(nil))

	if err := writeTarFile(tarWriter, blobTarName(newManifestDigest), manifestJSON); err != nil {
		return err
	}

	// Update index to point to new manifest
	metadata.index.Manifests[0].Digest = newManifestDigest
	metadata.index.Manifests[0].Size = int64(len(manifestJSON))

	indexJSON, err := json.Marshal(metadata.index)
	if err != nil {
		return err
	}

	opts.Debug("\nWriting index.json")
	if err := writeTarFile(tarWriter, "index.json", indexJSON); err != nil {
		return err
	}

	opts.Debug("\nDelta applied successfully!")
	return nil
}

func processLayerDiff(opts *ApplyOptions, tarWriter *tar.Writer, layerDigest digest.Digest, tarDiffReader io.Reader, layerToDiffID map[digest.Digest]digest.Digest, digestMapping *map[digest.Digest]digest.Digest, blobSizes *map[digest.Digest]int64) error {
	opts.Debug("  Reconstructing tar-diff layer %s", layerDigest.Encoded()[:16])

	// Create temporary file for compressed data
	tmpFile, err := os.CreateTemp(opts.TmpDir, "bootc-delta-layer-*.gz")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Create hash writers
	diffIDHash := sha256.New()     // Hash of uncompressed data
	compressedHash := sha256.New() // Hash of compressed data

	// Create gzip writer that writes to both compressedHash and tmpFile
	compressedMulti := io.MultiWriter(compressedHash, tmpFile)
	gzWriter, err := gzip.NewWriterLevel(compressedMulti, gzip.BestCompression)
	if err != nil {
		return err
	}
	gzWriter.Name = ""
	gzWriter.ModTime = time.Unix(0, 0)

	// Chain: tar-patch → [diffIDHash, gzWriter] → [compressedHash, tmpFile]
	uncompressedMulti := io.MultiWriter(diffIDHash, gzWriter)

	dataSource := tarpatch.NewFilesystemDataSource(opts.DeltaSource)
	if err := tarpatch.Apply(tarDiffReader, dataSource, uncompressedMulti); err != nil {
		gzWriter.Close()
		return fmt.Errorf("tar-patch failed: %w", err)
	}

	if err := gzWriter.Close(); err != nil {
		return fmt.Errorf("failed to close gzip writer: %w", err)
	}

	// Get the diff_id from the uncompressed hash
	actualDiffID := digest.NewDigestFromBytes(digest.SHA256, diffIDHash.Sum(nil))
	opts.Debug("    Computed diff_id: %s", actualDiffID.Encoded()[:16])

	if expectedDiffID, ok := layerToDiffID[layerDigest]; ok {
		opts.Debug("    Expected diff_id: %s", expectedDiffID.Encoded()[:16])
		if actualDiffID != expectedDiffID {
			return fmt.Errorf("diff_id mismatch for layer %s: expected %s, got %s",
				layerDigest.Encoded()[:16], expectedDiffID.Encoded()[:16], actualDiffID.Encoded()[:16])
		}
		opts.Debug("    Validated diff_id: %s", actualDiffID.Encoded()[:16])
	}

	// Get the compressed digest and size
	newDigest := digest.NewDigestFromBytes(digest.SHA256, compressedHash.Sum(nil))
	compressedInfo, err := tmpFile.Stat()
	if err != nil {
		return err
	}
	compressedSize := compressedInfo.Size()

	(*digestMapping)[layerDigest] = newDigest
	(*blobSizes)[newDigest] = compressedSize

	opts.Debug("    Compressed to %d bytes, new digest: %s", compressedSize, newDigest.Encoded()[:16])

	// Stream the compressed file into the tar archive
	if _, err := tmpFile.Seek(0, 0); err != nil {
		return err
	}
	tarName := blobTarName(newDigest)
	opts.Debug("    Writing tar header for: %s", tarName)
	if err := writeTarFileFromReader(tarWriter, tarName, compressedSize, tmpFile); err != nil {
		return fmt.Errorf("failed to write tar header for %s: %w", tarName, err)
	}

	return nil
}
