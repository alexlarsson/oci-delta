package ocidelta

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"

	digest "github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	sigstoreSignature "github.com/sigstore/sigstore/pkg/signature"
	sigpayload "github.com/sigstore/sigstore/pkg/signature/payload"
)

const (
	cosignSignatureAnnotationKey = "dev.cosignproject.cosign/signature"
)

func VerifyDeltaSignature(delta *DeltaArtifact, verifier sigstoreSignature.Verifier, log Logger) error {
	sigs := delta.Signatures()
	if len(sigs) == 0 {
		return fmt.Errorf("delta contains no embedded signatures")
	}

	expectedDigest := delta.ImageManifestDigest()

	var firstErr error
	for i, sig := range sigs {
		if err := verifySignatureManifest(delta, verifier, expectedDigest, &sig, log); err == nil {
			log.Debug("Signature verification passed")
			return nil
		} else {
			log.Debug("Signature %d verification failed: %v", i, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	return fmt.Errorf("no embedded signature could be verified: %w", firstErr)
}

func verifySignatureLayer(delta *DeltaArtifact, verifier sigstoreSignature.Verifier, expectedDigest digest.Digest, layer *v1.Descriptor, log Logger) error {
	base64Sig, ok := layer.Annotations[cosignSignatureAnnotationKey]
	if !ok {
		return fmt.Errorf("no signature annotation")
	}

	payload, err := delta.ReadBlob(layer.Digest)
	if err != nil {
		return fmt.Errorf("failed to read signature payload %s: %w", layer.Digest.Encoded()[:16], err)
	}

	rawSig, err := base64.StdEncoding.DecodeString(base64Sig)
	if err != nil {
		return fmt.Errorf("failed to decode signature: %w", err)
	}

	if err := verifier.VerifySignature(bytes.NewReader(rawSig), bytes.NewReader(payload)); err != nil {
		return fmt.Errorf("cryptographic signature verification failed: %w", err)
	}

	var p sigpayload.SimpleContainerImage
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to parse signature payload: %w", err)
	}
	if p.Critical.Type != sigpayload.CosignSignatureType {
		return fmt.Errorf("unexpected signature type: %s", p.Critical.Type)
	}

	payloadDigest, err := digest.Parse(p.Critical.Image.DockerManifestDigest)
	if err != nil {
		return fmt.Errorf("invalid docker-manifest-digest in signature payload: %w", err)
	}
	if payloadDigest != expectedDigest {
		return fmt.Errorf("signature is for manifest %s, but delta targets %s",
			payloadDigest.Encoded()[:16], expectedDigest.Encoded()[:16])
	}

	log.Debug("  Verified signature for manifest %s (ref: %s)",
		expectedDigest.Encoded()[:16], p.Critical.Identity.DockerReference)
	return nil
}

func verifySignatureManifest(delta *DeltaArtifact, verifier sigstoreSignature.Verifier, expectedDigest digest.Digest, sig *EmbeddedSignature, log Logger) error {
	var firstErr error
	for i, layer := range sig.Manifest.Layers {
		if err := verifySignatureLayer(delta, verifier, expectedDigest, &layer, log); err == nil {
			return nil
		} else {
			log.Debug("  Signature layer %d verification failed: %v", i, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	if firstErr != nil {
		return firstErr
	}
	return fmt.Errorf("no signature layers found")
}
