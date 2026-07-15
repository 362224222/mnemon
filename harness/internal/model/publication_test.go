package model

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestPublicationSignatureMessageAndVerification(t *testing.T) {
	t.Parallel()
	home, reviewer := mustPeer(t, "peer-signature-home"), mustPeer(t, "peer-signature-reviewer")
	event := mustWorkEvent(t, home, home, reviewer, EventReviewOffered)
	body, err := NewPublicationBody(event)
	if err != nil {
		t.Fatal(err)
	}
	message, err := PublicationSigningMessage(body.Key().ChannelID(), body.Digest())
	if err != nil {
		t.Fatal(err)
	}
	if len(message) <= len(PublicationSignatureDomain) || string(message[:len(PublicationSignatureDomain)]) != PublicationSignatureDomain {
		t.Fatal("signature message lost its domain")
	}
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := AttachSignature(body, ed25519.Sign(privateKey, message))
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyPublication(publicKey, publication); err != nil {
		t.Fatalf("VerifyPublication() error = %v", err)
	}
	wrong, _, _ := ed25519.GenerateKey(nil)
	if err := VerifyPublication(wrong, publication); err == nil {
		t.Fatal("wrong publication key was accepted")
	}
}

func TestSignedPublicationCanonicalIdentityAndCopies(t *testing.T) {
	t.Parallel()

	home, reviewer := mustPeer(t, "peer-home"), mustPeer(t, "peer-reviewer")
	event := mustWorkEvent(t, home, home, reviewer, EventReviewOffered)
	signature := testPublicationSignature()
	body, err := NewPublicationBody(event)
	if err != nil {
		t.Fatalf("NewPublicationBody() error = %v", err)
	}
	publication, err := AttachSignature(body, signature)
	if err != nil {
		t.Fatalf("AttachSignature() error = %v", err)
	}
	if body.Digest() != Sum(body.CanonicalJSON().Bytes()) || publication.Key().ChannelID() != event.Scope().ChannelID() {
		t.Fatalf("publication identity/digest mismatch")
	}
	signature[0] = 'x'
	copySignature := publication.OriginSignature()
	copySignature[0] = 'y'
	if !bytes.Equal(publication.OriginSignature(), testPublicationSignature()) {
		t.Fatalf("publication signature is mutable")
	}
	if _, err := AttachSignature(body, []byte("short")); !errors.Is(err, ErrInvalid) {
		t.Fatalf("short Ed25519 signature error = %v", err)
	}
}

func TestPublicationBytesDoNotDependOnLocalImportProjection(t *testing.T) {
	t.Parallel()

	home, reviewer := mustPeer(t, "peer-home"), mustPeer(t, "peer-reviewer")
	spec := validEventSpec(t, mustEventScope(t, home, home), EventReviewOffered, reviewer)
	local, err := NewEvent(spec)
	if err != nil {
		t.Fatalf("NewEvent(local) error = %v", err)
	}
	spec.Source = EventSourceImported
	imported, err := NewEvent(spec)
	if err != nil {
		t.Fatalf("NewEvent(imported) error = %v", err)
	}
	localBody, _ := NewPublicationBody(local)
	importedBody, _ := NewPublicationBody(imported)
	if local.CanonicalJSON().String() != imported.CanonicalJSON().String() || localBody.Digest() != importedBody.Digest() {
		t.Fatalf("local-only source projection changed immutable wire identity")
	}
	localWire, _ := AttachSignature(localBody, testPublicationSignature())
	importedWire, _ := AttachSignature(importedBody, testPublicationSignature())
	if localWire.WireJSON().String() != importedWire.WireJSON().String() {
		t.Fatalf("Gossip/Pull publication bytes differ after import projection")
	}
}

func TestSignedPublicationAppliesExactWireLimit(t *testing.T) {
	t.Parallel()

	home, reviewer := mustPeer(t, "peer-home"), mustPeer(t, "peer-reviewer")
	base := validEventSpec(t, mustEventScope(t, home, home), EventReviewOffered, reviewer)
	low, high := 0, MaxPublicationBytes
	var largest Event
	for low <= high {
		mid := low + (high-low)/2
		payload, _ := JSONFrom(map[string]any{"content": strings.Repeat("x", mid)})
		candidate := base
		candidate.Payload = payload
		event, err := NewEvent(candidate)
		if err == nil {
			largest = event
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	if largest.ID().IsZero() {
		t.Fatal("failed to construct a bounded Event")
	}
	body, err := NewPublicationBody(largest)
	if err != nil {
		t.Fatalf("NewPublicationBody() error = %v", err)
	}
	if _, err := AttachSignature(body, testPublicationSignature()); !errors.Is(err, ErrLimit) {
		t.Fatalf("publication overhead error = %v, want ErrLimit", err)
	}
}

func TestPublicationRecordLeaseAndSource(t *testing.T) {
	t.Parallel()

	home, reviewer := mustPeer(t, "peer-home"), mustPeer(t, "peer-reviewer")
	event := mustWorkEvent(t, home, home, reviewer, EventReviewOffered)
	body, _ := NewPublicationBody(event)
	publication, _ := AttachSignature(body, testPublicationSignature())
	now := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	lease := now.Add(time.Minute)
	spec := PublicationRecordSpec{publication, PublicationLeased, 1, now, "worker-a", &lease, nil, "", now, now}
	if _, err := NewPublicationRecord(spec); err != nil {
		t.Fatalf("NewPublicationRecord() error = %v", err)
	}
	spec.LeaseOwner, spec.LeaseUntil = "", nil
	if _, err := NewPublicationRecord(spec); !errors.Is(err, ErrInvariant) {
		t.Fatalf("missing lease error = %v", err)
	}
}

func testPublicationSignature() []byte {
	return bytes.Repeat([]byte{'s'}, ed25519.SignatureSize)
}
