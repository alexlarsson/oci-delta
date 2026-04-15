package ocidelta

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	digest "github.com/opencontainers/go-digest"
)

// --- TarIndex tests ---

func createTestTar(t *testing.T, files map[string][]byte) string {
	t.Helper()
	tmp, err := os.CreateTemp("", "tarindex-test-*.tar")
	if err != nil {
		t.Fatal(err)
	}
	w, err := newTarOCIWriter(tmp.Name(), "")
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range files {
		if err := w.WriteFile(name, data); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return tmp.Name()
}

func TestTarIndexReadBlob(t *testing.T) {
	blobContent := []byte("blob content here")
	d := digest.FromBytes(blobContent)
	files := map[string][]byte{
		"index.json":   []byte(`{"schemaVersion":2}`),
		blobTarName(d): blobContent,
	}
	path := createTestTar(t, files)
	defer os.Remove(path)

	idx, err := indexTarArchive(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	r, size, err := idx.ReadBlob(d)
	if err != nil {
		t.Fatalf("ReadBlob(%s): %v", d, err)
	}
	if size != int64(len(blobContent)) {
		t.Errorf("ReadBlob size = %d, want %d", size, len(blobContent))
	}
	got, err := io.ReadAll(r)
	r.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, blobContent) {
		t.Errorf("ReadBlob = %q, want %q", got, blobContent)
	}
}

func TestTarIndexReadBlobMissing(t *testing.T) {
	path := createTestTar(t, map[string][]byte{"a": []byte("x")})
	defer os.Remove(path)

	idx, err := indexTarArchive(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	_, _, err = idx.ReadBlob(digest.FromBytes([]byte("nonexistent")))
	if err == nil {
		t.Error("expected error for missing blob")
	}
}

func TestTarIndexSeekable(t *testing.T) {
	data := []byte("seekable content")
	d := digest.FromBytes(data)
	path := createTestTar(t, map[string][]byte{blobTarName(d): data})
	defer os.Remove(path)

	idx, err := indexTarArchive(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	r, _, err := idx.ReadBlob(d)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	buf := make([]byte, 4)
	r.Read(buf)
	if string(buf) != "seek" {
		t.Errorf("first read = %q, want \"seek\"", buf)
	}

	r.Seek(0, io.SeekStart)
	buf2 := make([]byte, 4)
	r.Read(buf2)
	if string(buf2) != "seek" {
		t.Errorf("after seek = %q, want \"seek\"", buf2)
	}
}

func TestTarIndexInvalidArchive(t *testing.T) {
	tmp, _ := os.CreateTemp("", "bad-tar-*")
	tmp.Write([]byte("not a tar file"))
	tmp.Close()
	defer os.Remove(tmp.Name())

	_, err := indexTarArchive(tmp.Name(), "")
	if err == nil {
		t.Error("expected error for invalid tar")
	}
}

func TestTarIndexNotFound(t *testing.T) {
	_, err := indexTarArchive("/nonexistent/path.tar", "")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

// --- DirOCIReader tests ---

func TestDirOCIReaderReadBlob(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "blobs", "sha256"), 0755)
	blobContent := []byte("blob")
	d := digest.FromBytes(blobContent)
	os.WriteFile(filepath.Join(dir, "blobs", "sha256", d.Encoded()), blobContent, 0644)

	reader := NewDirOCIReader(dir, "")

	r, size, err := reader.ReadBlob(d)
	if err != nil {
		t.Fatal(err)
	}
	if size != 4 {
		t.Errorf("size = %d, want 4", size)
	}
	data, _ := io.ReadAll(r)
	r.Close()
	if string(data) != "blob" {
		t.Errorf("content = %q", data)
	}
}

func TestDirOCIReaderMissing(t *testing.T) {
	reader := NewDirOCIReader(t.TempDir(), "")
	_, _, err := reader.ReadBlob(digest.FromBytes([]byte("missing")))
	if err == nil {
		t.Error("expected error for missing blob")
	}
}

// --- tarOCIWriter tests ---

func TestTarOCIWriterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	tarPath := filepath.Join(dir, "test.tar")

	blob1 := []byte("layer data")
	blob2 := []byte("another blob")
	d1 := digest.FromBytes(blob1)
	d2 := digest.FromBytes(blob2)
	blobs := map[digest.Digest][]byte{d1: blob1, d2: blob2}

	w, err := newTarOCIWriter(tarPath, "")
	if err != nil {
		t.Fatal(err)
	}
	for d, data := range blobs {
		if err := w.WriteFile(blobTarName(d), data); err != nil {
			t.Fatalf("WriteFile(%s): %v", d, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	idx, err := indexTarArchive(tarPath, "")
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	for d, want := range blobs {
		r, _, err := idx.ReadBlob(d)
		if err != nil {
			t.Fatalf("ReadBlob(%s): %v", d, err)
		}
		got, _ := io.ReadAll(r)
		r.Close()
		if !bytes.Equal(got, want) {
			t.Errorf("ReadBlob(%s) = %q, want %q", d, got, want)
		}
	}
}

func TestTarOCIWriterFromReader(t *testing.T) {
	dir := t.TempDir()
	tarPath := filepath.Join(dir, "test.tar")

	content := []byte("streamed content")
	d := digest.FromBytes(content)

	w, err := newTarOCIWriter(tarPath, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteFileFromReader(blobTarName(d), int64(len(content)), strings.NewReader(string(content))); err != nil {
		t.Fatal(err)
	}
	w.Close()

	idx, err := indexTarArchive(tarPath, "")
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	r, _, err := idx.ReadBlob(d)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(r)
	r.Close()
	if string(got) != string(content) {
		t.Errorf("got %q, want %q", got, content)
	}
}

func TestTarOCIWriterParentDirs(t *testing.T) {
	dir := t.TempDir()
	tarPath := filepath.Join(dir, "test.tar")

	w, err := newTarOCIWriter(tarPath, "")
	if err != nil {
		t.Fatal(err)
	}
	w.WriteFile("a/b/c/file.txt", []byte("deep"))
	w.WriteFile("a/b/other.txt", []byte("shallow"))
	w.Close()

	idx, err := indexTarArchive(tarPath, "")
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	for _, dirName := range []string{"a/", "a/b/", "a/b/c/"} {
		if _, ok := idx.entries[dirName]; !ok {
			t.Errorf("missing parent dir entry %q", dirName)
		}
	}
}

// --- dirOCIWriter tests ---

func TestDirOCIWriterWriteFile(t *testing.T) {
	dir := t.TempDir()
	w, err := newDirOCIWriter(filepath.Join(dir, "output"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.WriteFile("blobs/sha256/test", []byte("content")); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(filepath.Join(dir, "output", "blobs", "sha256", "test"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "content" {
		t.Errorf("got %q, want \"content\"", got)
	}
}

func TestDirOCIWriterFromReader(t *testing.T) {
	dir := t.TempDir()
	w, err := newDirOCIWriter(filepath.Join(dir, "output"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	content := "from reader"
	if err := w.WriteFileFromReader("data.bin", int64(len(content)), strings.NewReader(content)); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(filepath.Join(dir, "output", "data.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != content {
		t.Errorf("got %q, want %q", got, content)
	}
}

// --- readAll helper ---

func TestReadBlob(t *testing.T) {
	content := []byte("hello")
	d := digest.FromBytes(content)
	reader := newMemoryReader(map[string][]byte{
		blobTarName(d): content,
	})
	data, err := readBlob(reader, d)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Errorf("got %q, want \"hello\"", data)
	}

	_, err = readBlob(reader, digest.FromBytes([]byte("missing")))
	if err == nil {
		t.Error("expected error for missing blob")
	}
}

// --- OpenOCIReader/Writer dispatch ---

func TestOpenOCIReaderOCIDir(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "index.json"), []byte("{}"), 0644)

	reader, err := OpenOCIReader("oci:"+dir, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if _, ok := reader.(*DirOCIReader); !ok {
		t.Errorf("expected *DirOCIReader, got %T", reader)
	}
}

func TestOpenOCIReaderOCIArchive(t *testing.T) {
	path := createTestTar(t, map[string][]byte{"index.json": []byte("{}")})
	defer os.Remove(path)

	reader, err := OpenOCIReader("oci-archive:"+path, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if _, ok := reader.(*TarIndex); !ok {
		t.Errorf("expected *TarIndex, got %T", reader)
	}
}

func TestOpenOCIReaderDefaultArchive(t *testing.T) {
	path := createTestTar(t, map[string][]byte{"index.json": []byte("{}")})
	defer os.Remove(path)

	reader, err := OpenOCIReader(path, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if _, ok := reader.(*TarIndex); !ok {
		t.Errorf("expected *TarIndex for bare path, got %T", reader)
	}
}

func TestOpenOCIWriterTar(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenOCIWriter(filepath.Join(dir, "out.tar"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, ok := w.(*tarOCIWriter); !ok {
		t.Errorf("expected *tarOCIWriter, got %T", w)
	}
}

func TestOpenOCIWriterOCIArchive(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenOCIWriter("oci-archive:" + filepath.Join(dir, "out.tar"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, ok := w.(*tarOCIWriter); !ok {
		t.Errorf("expected *tarOCIWriter, got %T", w)
	}
}

func TestOpenOCIWriterOCIDir(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenOCIWriter("oci:" + filepath.Join(dir, "out"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, ok := w.(*dirOCIWriter); !ok {
		t.Errorf("expected *dirOCIWriter, got %T", w)
	}
}
