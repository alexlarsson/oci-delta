package ocidelta

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	storageTransport "github.com/containers/image/v5/storage"
	"github.com/containers/storage"
	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type OCIReader interface {
	GetManifestDigest() (digest.Digest, error)
	ReadBlob(d digest.Digest) (io.ReadSeekCloser, int64, error)
	Close() error
}

type OCIWriter interface {
	WriteFile(name string, data []byte) error
	WriteFileFromReader(name string, size int64, r io.Reader) error
	Close() error
}

func OpenOCIReader(ref string, tmpDir string, log Logger) (OCIReader, error) {
	if strings.HasPrefix(ref, "containers-storage:") {
		csRef := ref[len("containers-storage:"):]
		imgRef, err := storageTransport.Transport.ParseReference(csRef)
		if err != nil {
			return nil, fmt.Errorf("failed to parse container storage reference %q: %w", csRef, err)
		}
		resolvedRef, img, err := storageTransport.ResolveReference(imgRef)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve image %q: %w", csRef, err)
		}
		store := resolvedRef.Transport().(storageTransport.StoreTransport).GetStoreIfSet()
		reader, err := newCSReader(store, img, tmpDir, log)
		if err != nil {
			store.Shutdown(false)
			return nil, err
		}
		return reader, nil
	}
	if strings.HasPrefix(ref, "oci-archive:") {
		return indexTarArchive(ref[len("oci-archive:"):])
	}
	if strings.HasPrefix(ref, "oci:") {
		return NewDirOCIReader(ref[len("oci:"):]), nil
	}
	return indexTarArchive(ref)
}

func OpenOCIWriter(ref string) (OCIWriter, error) {
	if strings.HasPrefix(ref, "oci-archive:") {
		return newTarOCIWriter(ref[len("oci-archive:"):])
	}
	if strings.HasPrefix(ref, "oci:") {
		return newDirOCIWriter(ref[len("oci:"):])
	}
	return newTarOCIWriter(ref)
}

func parseManifestDigestFromIndex(data []byte) (digest.Digest, error) {
	var index v1.Index
	if err := json.Unmarshal(data, &index); err != nil {
		return "", fmt.Errorf("failed to parse index.json: %w", err)
	}
	if len(index.Manifests) == 0 {
		return "", fmt.Errorf("index.json contains no manifests")
	}
	if len(index.Manifests) > 1 {
		return "", fmt.Errorf("index.json contains multiple manifests (%d), only single-image archives are supported", len(index.Manifests))
	}
	desc := index.Manifests[0]
	if desc.MediaType == v1.MediaTypeImageIndex ||
		desc.MediaType == "application/vnd.docker.distribution.manifest.list.v2+json" {
		return "", fmt.Errorf("index.json contains a manifest list, only single-image archives are supported")
	}
	return desc.Digest, nil
}

func readBlob(reader OCIReader, d digest.Digest) ([]byte, error) {
	r, _, err := reader.ReadBlob(d)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// TarIndex — tar archive backed OCIReader

type TarIndex struct {
	file    *os.File
	entries map[string]*TarEntry
}

type TarEntry struct {
	offset int64
	size   int64
}

type readSeekNopCloser struct {
	io.ReadSeeker
}

func (readSeekNopCloser) Close() error { return nil }

type offsetTracker struct {
	r      io.Reader
	offset int64
}

func (ot *offsetTracker) Read(p []byte) (n int, err error) {
	n, err = ot.r.Read(p)
	ot.offset += int64(n)
	return
}

func indexTarArchive(path string) (*TarIndex, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	tracker := &offsetTracker{r: f}
	tr := tar.NewReader(tracker)
	entries := make(map[string]*TarEntry)

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			f.Close()
			return nil, err
		}

		dataOffset := tracker.offset

		entries[hdr.Name] = &TarEntry{
			offset: dataOffset,
			size:   hdr.Size,
		}

		if _, err := io.Copy(io.Discard, tr); err != nil {
			f.Close()
			return nil, err
		}
	}

	return &TarIndex{
		file:    f,
		entries: entries,
	}, nil
}

func (idx *TarIndex) GetManifestDigest() (digest.Digest, error) {
	entry, ok := idx.entries["index.json"]
	if !ok {
		return "", fmt.Errorf("index.json not found in tar")
	}
	data := make([]byte, entry.size)
	if _, err := idx.file.ReadAt(data, entry.offset); err != nil {
		return "", fmt.Errorf("failed to read index.json: %w", err)
	}
	return parseManifestDigestFromIndex(data)
}

func (idx *TarIndex) ReadBlob(d digest.Digest) (io.ReadSeekCloser, int64, error) {
	entry, ok := idx.entries[blobTarName(d)]
	if !ok {
		return nil, 0, fmt.Errorf("blob not found in tar: %s", d)
	}

	return readSeekNopCloser{io.NewSectionReader(idx.file, entry.offset, entry.size)}, entry.size, nil
}

func (idx *TarIndex) Close() error {
	if idx.file != nil {
		return idx.file.Close()
	}
	return nil
}

// DirOCIReader — directory backed OCIReader

type DirOCIReader struct {
	dir string
}

func NewDirOCIReader(dir string) *DirOCIReader {
	return &DirOCIReader{dir: dir}
}

func (d *DirOCIReader) GetManifestDigest() (digest.Digest, error) {
	data, err := os.ReadFile(filepath.Join(d.dir, "index.json"))
	if err != nil {
		return "", fmt.Errorf("failed to read index.json: %w", err)
	}
	return parseManifestDigestFromIndex(data)
}

func (d *DirOCIReader) ReadBlob(dgst digest.Digest) (io.ReadSeekCloser, int64, error) {
	f, err := os.Open(filepath.Join(d.dir, blobTarName(dgst)))
	if err != nil {
		return nil, 0, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return f, info.Size(), nil
}

func (d *DirOCIReader) Close() error {
	return nil
}

// tarOCIWriter — tar archive backed OCIWriter

type tarOCIWriter struct {
	file *os.File
	tw   *tar.Writer
	dirs map[string]bool
}

func newTarOCIWriter(path string) (*tarOCIWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	tw := tar.NewWriter(f)
	return &tarOCIWriter{file: f, tw: tw, dirs: make(map[string]bool)}, nil
}

func (w *tarOCIWriter) ensureParentDirs(name string) error {
	parts := strings.Split(name, "/")
	for i := 1; i < len(parts); i++ {
		dir := strings.Join(parts[:i], "/") + "/"
		if !w.dirs[dir] {
			if err := writeTarDir(w.tw, dir); err != nil {
				return err
			}
			w.dirs[dir] = true
		}
	}
	return nil
}

func (w *tarOCIWriter) WriteFile(name string, data []byte) error {
	if err := w.ensureParentDirs(name); err != nil {
		return err
	}
	return writeTarFile(w.tw, name, data)
}

func (w *tarOCIWriter) WriteFileFromReader(name string, size int64, r io.Reader) error {
	if err := w.ensureParentDirs(name); err != nil {
		return err
	}
	return writeTarFileFromReader(w.tw, name, size, r)
}

func (w *tarOCIWriter) Close() error {
	err := w.tw.Close()
	if err2 := w.file.Close(); err == nil {
		err = err2
	}
	return err
}

// dirOCIWriter — directory backed OCIWriter

type dirOCIWriter struct {
	dir string
}

func newDirOCIWriter(dir string) (*dirOCIWriter, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return &dirOCIWriter{dir: dir}, nil
}

func (w *dirOCIWriter) WriteFile(name string, data []byte) error {
	path := filepath.Join(w.dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func (w *dirOCIWriter) WriteFileFromReader(name string, size int64, r io.Reader) error {
	path := filepath.Join(w.dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, r)
	return err
}

func (w *dirOCIWriter) Close() error {
	return nil
}

// memOCIReader — in-memory OCIReader

type memOCIReader struct {
	manifestDigest digest.Digest
	blobs          map[digest.Digest][]byte
}

func (m *memOCIReader) GetManifestDigest() (digest.Digest, error) {
	return m.manifestDigest, nil
}

func (m *memOCIReader) ReadBlob(d digest.Digest) (io.ReadSeekCloser, int64, error) {
	data, ok := m.blobs[d]
	if !ok {
		return nil, 0, fmt.Errorf("blob not found: %s", d)
	}
	return readSeekNopCloser{bytes.NewReader(data)}, int64(len(data)), nil
}

func (m *memOCIReader) Close() error { return nil }

// csOCIReader — container storage backed OCIReader
//
// Reads the original manifest and config from container storage big data,
// and exports layer tars to temp files. The layers are uncompressed (from
// store.Diff), but named by their original compressed digest so that the
// original manifest is preserved. tar-diff handles uncompressed input via
// AutoDecompress.

type csOCIReader struct {
	manifestDigest digest.Digest
	files          map[string][]byte // in-memory blobs (manifest, config)
	layerFiles     map[string]string // digest path -> temp file path
	tmpDir         string
	signatures     []OCIReader
	store          storage.Store
}

func exportStorageLayers(store storage.Store, manifest *v1.Manifest, diffIDs []digest.Digest, exportDir string, log Logger) (map[string]string, error) {
	layerFiles := make(map[string]string)

	for i, layerDesc := range manifest.Layers {
		if i >= len(diffIDs) {
			break
		}
		diffID := diffIDs[i]

		existing, err := store.LayersByUncompressedDigest(diffID)
		if err != nil || len(existing) == 0 {
			return nil, fmt.Errorf("layer with diff_id %s not found in storage", diffID)
		}

		sl := existing[0]
		diffReader, err := store.Diff(sl.Parent, sl.ID, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to export layer %s: %w", diffID.Encoded()[:16], err)
		}

		layerPath := filepath.Join(exportDir, layerDesc.Digest.Encoded())
		outFile, err := os.Create(layerPath)
		if err != nil {
			diffReader.Close()
			return nil, fmt.Errorf("failed to create layer temp file: %w", err)
		}

		_, err = io.Copy(outFile, diffReader)
		outFile.Close()
		diffReader.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to write layer %s: %w", diffID.Encoded()[:16], err)
		}

		layerFiles[blobTarName(layerDesc.Digest)] = layerPath
		if log != nil {
			log.Debug("  Exported layer %d/%d %s", i+1, len(manifest.Layers), layerDesc.Digest.Encoded()[:16])
		}
	}

	return layerFiles, nil
}

func newCSReader(store storage.Store, img *storage.Image, tmpDir string, log Logger) (*csOCIReader, error) {
	manifestData, err := store.ImageBigData(img.ID, "manifest")
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest: %w", err)
	}
	manifestDigest := computeDigest(manifestData)

	var manifest v1.Manifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return nil, fmt.Errorf("failed to parse manifest: %w", err)
	}

	if manifest.MediaType != v1.MediaTypeImageManifest {
		return nil, fmt.Errorf("image %s has unsupported manifest type %q, only OCI manifests are supported", img.ID[:16], manifest.MediaType)
	}

	configData, err := store.ImageBigData(img.ID, manifest.Config.Digest.String())
	if err != nil {
		return nil, fmt.Errorf("failed to read config: %w", err)
	}

	var config v1.Image
	if err := json.Unmarshal(configData, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	exportDir, err := os.MkdirTemp(tmpDir, "cs-layers-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}

	var sigs []OCIReader
	extracted, err := ExtractContainerStorageSignatures(store, img.ID, log)
	if err != nil {
		if log != nil {
			log.Debug("Could not extract signatures: %v", err)
		}
	} else {
		sigs = extracted
	}

	layerFiles, err := exportStorageLayers(store, &manifest, config.RootFS.DiffIDs, exportDir, log)
	if err != nil {
		os.RemoveAll(exportDir)
		return nil, err
	}

	files := map[string][]byte{
		blobTarName(manifestDigest):         manifestData,
		blobTarName(manifest.Config.Digest): configData,
	}

	return &csOCIReader{
		manifestDigest: manifestDigest,
		files:          files,
		layerFiles:     layerFiles,
		tmpDir:         exportDir,
		signatures:     sigs,
		store:          store,
	}, nil
}

func (r *csOCIReader) GetManifestDigest() (digest.Digest, error) {
	return r.manifestDigest, nil
}

func (r *csOCIReader) ReadBlob(d digest.Digest) (io.ReadSeekCloser, int64, error) {
	name := blobTarName(d)
	if data, ok := r.files[name]; ok {
		return readSeekNopCloser{bytes.NewReader(data)}, int64(len(data)), nil
	}
	if path, ok := r.layerFiles[name]; ok {
		f, err := os.Open(path)
		if err != nil {
			return nil, 0, err
		}
		info, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, 0, err
		}
		return f, info.Size(), nil
	}
	return nil, 0, fmt.Errorf("blob not found: %s", d)
}

func (r *csOCIReader) Close() error {
	os.RemoveAll(r.tmpDir)
	r.store.Shutdown(false)
	return nil
}

func ExtractedSignatures(reader OCIReader) []OCIReader {
	if cs, ok := reader.(*csOCIReader); ok {
		return cs.signatures
	}
	return nil
}
