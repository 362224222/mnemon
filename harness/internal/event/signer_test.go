package event

import (
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestEd25519SignerCopiesKeysAndSeparatesPublicationDomain(t *testing.T) {
	t.Parallel()

	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	signer, err := NewEd25519Signer(privateKey)
	if err != nil {
		t.Fatalf("NewEd25519Signer() error = %v", err)
	}
	privateKey[0] ^= 0xff
	channel, _ := model.ParseChannelID("channel-a")
	message := publicationSigningMessage(channel, model.Sum([]byte(`{"event":"body"}`)))
	if !hasPublicationDomain(message) {
		t.Fatalf("signature message lacks domain: %q", message)
	}
	signature, err := signer.Sign(context.Background(), message)
	if err != nil {
		t.Fatalf("Sign() error = %v", err)
	}
	if !ed25519.Verify(publicKey, message, signature) {
		t.Fatal("signature did not verify against copied private key")
	}
	if ed25519.Verify(publicKey, []byte(`{"event":"body"}`), signature) {
		t.Fatal("signature verified without its domain prefix")
	}
}

func TestVerifyPublicationRejectsWrongKeyOrSignature(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 16, 5, 4, 3, 2, time.UTC)
	factory, publicKey := testFactory(t, now)
	candidate, _ := NewOfferCandidate("review", 0)
	bundle, err := factory.AdmitAgent(context.Background(), testStamp(t, "verify", true, true, now), candidate)
	if err != nil {
		t.Fatalf("AdmitAgent() error = %v", err)
	}
	wrongKey, _, _ := ed25519.GenerateKey(nil)
	if err := VerifyPublication(wrongKey, bundle.Publication()); !errors.Is(err, ErrSignature) {
		t.Fatalf("wrong-key verification error = %v", err)
	}
	tampered, err := model.AttachSignature(bundle.Publication().Body(), make([]byte, ed25519.SignatureSize))
	if err != nil {
		t.Fatalf("AttachSignature() error = %v", err)
	}
	if err := VerifyPublication(publicKey, tampered); !errors.Is(err, ErrSignature) {
		t.Fatalf("wrong-signature verification error = %v", err)
	}
}

func TestEd25519SignerRejectsBadKeysAndCancellation(t *testing.T) {
	t.Parallel()

	if _, err := NewEd25519Signer(make([]byte, 4)); !errors.Is(err, ErrSignature) {
		t.Fatalf("short key error = %v", err)
	}
	_, privateKey, _ := ed25519.GenerateKey(nil)
	signer, _ := NewEd25519Signer(privateKey)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := signer.Sign(ctx, []byte("message")); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Sign() error = %v", err)
	}
}
