package bootcdelta

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"io"
	"os"

	tardiff "github.com/containers/tar-diff/pkg/tar-diff"
	digest "github.com/opencontainers/go-digest"
)

type CreateOptions struct {
	OldImage   string
	NewImage   string
	OutputPath string
	TmpDir     string
	Verbose    bool
	Debug      func(format string, args ...interface{})
	Warning    func(format string, args ...interface{})
}

type CreateStats struct {
	OldLayers           int
	NewLayers           int
	ProcessedLayers     int
	SkippedLayers       int
	ProcessedLayerBytes int64
	TarDiffLayerBytes   int64
	OriginalLayerBytes  int64
}

func CreateDelta(opts CreateOptions) (*CreateStats, error) {
	stats := &CreateStats{}

	opts.Debug("Indexing old image: %s", opts.OldImage)
	oldTarIndex, err := indexTarArchive(opts.OldImage)
	if err != nil {
		return nil, fmt.Errorf("failed to index old image: %w", err)
	}
	defer oldTarIndex.Close()

	opts.Debug("Indexing new image: %s", opts.NewImage)
	newTarIndex, err := indexTarArchive(opts.NewImage)
	if err != nil {
		return nil, fmt.Errorf("failed to index new image: %w", err)
	}
	defer newTarIndex.Close()

	opts.Debug("Parsing old image")
	old, err := parseOCIImage(oldTarIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to parse old image: %w", err)
	}
	stats.OldLayers = len(old.layers)
	opts.Debug("  Found %d layers in old image", stats.OldLayers)

	opts.Debug("Parsing new image")
	new, err := parseOCIImage(newTarIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to parse new image: %w", err)
	}
	stats.NewLayers = len(new.layers)
	opts.Debug("  Found %d layers in new image", stats.NewLayers)

	// Find layers with new content (diff_id not in old image)
	newOnlyLayers := make(map[digest.Digest]bool)
	for diffID, newLayerDigest := range new.diffIDToDigest {
		if _, exists := old.diffIDToDigest[diffID]; !exists {
			newOnlyLayers[newLayerDigest] = true
			opts.Debug("  New layer: %s (diff_id: %s)", newLayerDigest.Encoded()[:16], diffID.Encoded()[:16])
		}
	}
	stats.ProcessedLayers = len(newOnlyLayers)
	stats.SkippedLayers = len(new.layers) - len(newOnlyLayers)
	opts.Debug("Layers with new content (will process): %d", stats.ProcessedLayers)
	opts.Debug("Layers with existing content (will skip): %d", stats.SkippedLayers)

	outFile, err := os.Create(opts.OutputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	tarWriter := tar.NewWriter(outFile)
	defer tarWriter.Close()

	opts.Debug("\nWriting oci-layout")
	ociLayoutData, err := new.tarIndex.ReadFile("oci-layout")
	if err != nil {
		return nil, fmt.Errorf("failed to read oci-layout: %w", err)
	}
	if err := writeTarFile(tarWriter, "oci-layout", ociLayoutData); err != nil {
		return nil, err
	}

	opts.Debug("\nWriting index.json")
	indexData, err := json.Marshal(new.index)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal index: %w", err)
	}
	if err := writeTarFile(tarWriter, "index.json", indexData); err != nil {
		return nil, err
	}

	opts.Debug("\nProcessing layers...")
	for layerDigest := range new.layers {
		if newOnlyLayers[layerDigest] {
			if err := processNewLayer(&opts, stats, tarWriter, old, new, layerDigest, opts.TmpDir); err != nil {
				return nil, err
			}
		} else {
			opts.Debug("  Skipping layer with existing content %s", layerDigest.Encoded()[:16])
		}
	}

	// We need to also copy the remaining blobs, like the manifest and the config json
	opts.Debug("\nWriting non-layer blobs...")
	for name := range new.tarIndex.entries {
		if !isBlobPath(name) {
			continue
		}
		d := digestFromBlobPath(name)
		if d != "" && !new.layers[d] {
			if err := writeBlobTarFile(tarWriter, new.tarIndex, d); err != nil {
				return nil, err
			}
		}
	}

	return stats, nil
}

func processNewLayer(opts *CreateOptions, stats *CreateStats, tarWriter *tar.Writer, old *OCIImage, new *OCIImage, blobDigest digest.Digest, tmpDir string) error {
	originalSize, err := new.tarIndex.GetSize(blobTarName(blobDigest))
	if err != nil {
		return fmt.Errorf("failed to get layer size %s: %w", blobDigest.Encoded()[:16], err)
	}

	opts.Debug("  Processing new layer %s (%d bytes)", blobDigest.Encoded()[:16], originalSize)
	stats.ProcessedLayerBytes += originalSize

	tarDiffFile, err := os.CreateTemp(tmpDir, "bootc-delta-*.tar-diff")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	tarDiffOutput := tarDiffFile.Name()
	tarDiffFile.Close()
	defer os.Remove(tarDiffOutput)

	if err := runTarDiff(old, new, blobDigest, tarDiffOutput); err != nil {
		opts.Warning("tar-diff failed for layer %s: %v, using original", blobDigest.Encoded()[:16], err)
		stats.OriginalLayerBytes += originalSize
		return writeBlobTarFile(tarWriter, new.tarIndex, blobDigest)
	}

	tarDiffInfo, err := os.Stat(tarDiffOutput)
	if err == nil && tarDiffInfo.Size() < originalSize {
		opts.Debug("    Using tar-diff: %d bytes (saved %d bytes)", tarDiffInfo.Size(), originalSize-tarDiffInfo.Size())
		if err := writeTarFileFromFile(tarWriter, blobTarName(blobDigest), tarDiffOutput); err != nil {
			return err
		}
		stats.TarDiffLayerBytes += tarDiffInfo.Size()
	} else {
		opts.Debug("    Using original: %d bytes (tar-diff was %d bytes)", originalSize, tarDiffInfo.Size())
		stats.OriginalLayerBytes += originalSize
		return writeBlobTarFile(tarWriter, new.tarIndex, blobDigest)
	}

	return nil
}

func runTarDiff(old *OCIImage, new *OCIImage, newLayerDigest digest.Digest, output string) error {
	var oldFiles []io.ReadSeeker

	// Get readers for all old image layers
	for layerDigest := range old.layers {
		r, err := old.tarIndex.GetReader(blobTarName(layerDigest))
		if err != nil {
			return err
		}
		oldFiles = append(oldFiles, r)
	}

	// Get reader for new layer
	newFile, err := new.tarIndex.GetReader(blobTarName(newLayerDigest))
	if err != nil {
		return err
	}

	outFile, err := os.Create(output)
	if err != nil {
		return err
	}
	defer outFile.Close()

	opts := tardiff.NewOptions()
	opts.SetSourcePrefixes([]string{"sysroot/ostree/repo/objects/"})

	return tardiff.Diff(oldFiles, newFile, outFile, opts)
}
