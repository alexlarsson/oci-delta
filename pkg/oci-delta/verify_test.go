package ocidelta

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"

	sigstoreSignature "github.com/sigstore/sigstore/pkg/signature"

	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type testLogger struct{}

func (l *testLogger) Debug(format string, args ...interface{})   {}
func (l *testLogger) Warning(format string, args ...interface{}) {}

func createTestKey(t *testing.T) (sigstoreSignature.Verifier, *ecdsa.PrivateKey) {
	t.Helper()
	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := sigstoreSignature.LoadVerifier(privKey.Public(), crypto.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	return verifier, privKey
}

func signPayload(t *testing.T, privKey *ecdsa.PrivateKey, payload []byte) string {
	t.Helper()
	signer, err := sigstoreSignature.LoadECDSASignerVerifier(privKey, crypto.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	sig, err := signer.SignMessage(bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(sig)
}

type memReader struct {
	blobs map[digest.Digest][]byte
}

func (m *memReader) ReadBlob(d digest.Digest) (io.ReadSeekCloser, int64, digest.Digest, error) {
	data, ok := m.blobs[d]
	if !ok {
		return nil, 0, "", os.ErrNotExist
	}
	r := bytes.NewReader(data)
	return readSeekNopCloser{r}, int64(len(data)), d, nil
}

func (m *memReader) GetManifestDigest() (digest.Digest, error) {
	return "", fmt.Errorf("not supported")
}

func (m *memReader) Close() error { return nil }

func buildTestDelta(t *testing.T, manifestDigest digest.Digest, sigManifest v1.Manifest, sigBlobs map[digest.Digest][]byte) *DeltaArtifact {
	t.Helper()
	return &DeltaArtifact{
		reader:              &memReader{blobs: sigBlobs},
		imageManifestDigest: manifestDigest,
		signatures:          []EmbeddedSignature{{Manifest: sigManifest}},
	}
}

func TestVerifyDeltaSignature(t *testing.T) {
	verifier, privKey := createTestKey(t)
	log := &testLogger{}

	manifestDigest := digest.FromString("test-manifest-content")

	payload, _ := json.Marshal(map[string]any{
		"critical": map[string]any{
			"type":     "cosign container image signature",
			"image":    map[string]string{"docker-manifest-digest": manifestDigest.String()},
			"identity": map[string]string{"docker-reference": "example.com/test:latest"},
		},
		"optional": map[string]any{},
	})

	base64Sig := signPayload(t, privKey, payload)
	payloadDigest := digest.FromBytes(payload)

	sigManifest := v1.Manifest{
		Layers: []v1.Descriptor{{
			MediaType: "application/vnd.dev.cosign.simplesigning.v1+json",
			Digest:    payloadDigest,
			Size:      int64(len(payload)),
			Annotations: map[string]string{
				cosignSignatureAnnotationKey: base64Sig,
			},
		}},
	}

	blobs := map[digest.Digest][]byte{
		payloadDigest: payload,
	}

	delta := buildTestDelta(t, manifestDigest, sigManifest, blobs)

	if err := VerifyDeltaSignature(delta, verifier, log); err != nil {
		t.Fatalf("expected verification to pass: %v", err)
	}
}

func TestVerifyDeltaSignatureWrongKey(t *testing.T) {
	_, privKey := createTestKey(t)
	otherVerifier, _ := createTestKey(t)
	log := &testLogger{}

	manifestDigest := digest.FromString("test-manifest-content")

	payload, _ := json.Marshal(map[string]any{
		"critical": map[string]any{
			"type":     "cosign container image signature",
			"image":    map[string]string{"docker-manifest-digest": manifestDigest.String()},
			"identity": map[string]string{"docker-reference": "example.com/test:latest"},
		},
		"optional": map[string]any{},
	})

	base64Sig := signPayload(t, privKey, payload)
	payloadDigest := digest.FromBytes(payload)

	sigManifest := v1.Manifest{
		Layers: []v1.Descriptor{{
			MediaType: "application/vnd.dev.cosign.simplesigning.v1+json",
			Digest:    payloadDigest,
			Size:      int64(len(payload)),
			Annotations: map[string]string{
				cosignSignatureAnnotationKey: base64Sig,
			},
		}},
	}

	blobs := map[digest.Digest][]byte{
		payloadDigest: payload,
	}

	delta := buildTestDelta(t, manifestDigest, sigManifest, blobs)

	if err := VerifyDeltaSignature(delta, otherVerifier, log); err == nil {
		t.Fatal("expected verification to fail with wrong key")
	}
}

func TestVerifyDeltaSignatureWrongDigest(t *testing.T) {
	verifier, privKey := createTestKey(t)
	log := &testLogger{}

	realDigest := digest.FromString("real-manifest")
	wrongDigest := digest.FromString("wrong-manifest")

	payload, _ := json.Marshal(map[string]any{
		"critical": map[string]any{
			"type":     "cosign container image signature",
			"image":    map[string]string{"docker-manifest-digest": wrongDigest.String()},
			"identity": map[string]string{"docker-reference": "example.com/test:latest"},
		},
		"optional": map[string]any{},
	})

	base64Sig := signPayload(t, privKey, payload)
	payloadDigest := digest.FromBytes(payload)

	sigManifest := v1.Manifest{
		Layers: []v1.Descriptor{{
			MediaType: "application/vnd.dev.cosign.simplesigning.v1+json",
			Digest:    payloadDigest,
			Size:      int64(len(payload)),
			Annotations: map[string]string{
				cosignSignatureAnnotationKey: base64Sig,
			},
		}},
	}

	blobs := map[digest.Digest][]byte{
		payloadDigest: payload,
	}

	delta := buildTestDelta(t, realDigest, sigManifest, blobs)

	err := VerifyDeltaSignature(delta, verifier, log)
	if err == nil {
		t.Fatal("expected verification to fail with wrong manifest digest")
	}
}

func TestVerifyDeltaSignatureNoSignatures(t *testing.T) {
	verifier, _ := createTestKey(t)
	log := &testLogger{}

	delta := &DeltaArtifact{
		reader:              &memReader{blobs: map[digest.Digest][]byte{}},
		imageManifestDigest: digest.FromString("test"),
		signatures:          nil,
	}

	err := VerifyDeltaSignature(delta, verifier, log)
	if err == nil {
		t.Fatal("expected error for delta with no signatures")
	}
}
