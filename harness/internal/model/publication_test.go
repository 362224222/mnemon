package model

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
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

func TestParseSignedPublicationRoundTrip(t *testing.T) {
	t.Parallel()

	home, reviewer := mustPeer(t, "peer-wire-home"), mustPeer(t, "peer-wire-reviewer")
	event := mustWorkEvent(t, home, home, reviewer, EventReviewOffered)
	body, err := NewPublicationBody(event)
	if err != nil {
		t.Fatal(err)
	}
	want, err := AttachSignature(body, testPublicationSignature())
	if err != nil {
		t.Fatal(err)
	}

	got, err := ParseSignedPublication(want.WireJSON().Bytes())
	if err != nil {
		t.Fatalf("ParseSignedPublication() error = %v", err)
	}
	if got.Digest() != want.Digest() || got.Key() != want.Key() ||
		got.Event().Digest() != want.Event().Digest() || got.Event().Source() != EventSourceLocal ||
		!bytes.Equal(got.WireJSON().Bytes(), want.WireJSON().Bytes()) ||
		!bytes.Equal(got.OriginSignature(), want.OriginSignature()) {
		t.Fatal("parsed publication did not preserve its immutable wire identity")
	}

	noncanonical := append([]byte{' '}, want.WireJSON().Bytes()...)
	if _, err := ParseSignedPublication(noncanonical); !errors.Is(err, ErrInvalid) {
		t.Fatalf("noncanonical publication error = %v, want ErrInvalid", err)
	}
}

func TestParsePublicationEvidenceRoundTripAndVerification(t *testing.T) {
	t.Parallel()

	home, reviewer := mustPeer(t, "peer-evidence-home"), mustPeer(t, "peer-evidence-reviewer")
	event := mustWorkEvent(t, home, home, reviewer, EventReviewOffered)
	body, err := NewPublicationBody(event)
	if err != nil {
		t.Fatal(err)
	}
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	message, err := PublicationSigningMessage(body.Key().ChannelID(), body.Digest())
	if err != nil {
		t.Fatal(err)
	}
	publication, err := AttachSignature(body, ed25519.Sign(privateKey, message))
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := ParsePublicationEvidence(publication.WireJSON().Bytes())
	if err != nil {
		t.Fatalf("ParsePublicationEvidence() error = %v", err)
	}
	scope := event.Scope()
	if evidence.IsZero() || !evidence.IsSupported() || evidence.SchemaVersion() != SchemaVersion ||
		evidence.ChannelID() != scope.ChannelID() || evidence.OriginPeerID() != scope.OriginPeerID() ||
		evidence.OriginEpoch() != scope.OriginEpoch() ||
		evidence.OriginSequence() != scope.OriginSequence() ||
		evidence.ChannelSequence() != scope.ChannelSequence() || evidence.EventID() != event.ID() ||
		evidence.EventDigest() != event.Digest() || evidence.OriginMember() != scope.OriginMember() ||
		evidence.PublicationRoster() != scope.PublicationRoster() || evidence.Digest() != publication.Digest() ||
		evidence.Audience().Len() != event.Audience().Len() ||
		!evidence.Audience().Contains(reviewer) ||
		!bytes.Equal(evidence.WireJSON().Bytes(), publication.WireJSON().Bytes()) ||
		!bytes.Equal(evidence.OriginSignature(), publication.OriginSignature()) {
		t.Fatalf("publication evidence lost stable identity: %#v", evidence)
	}
	if err := VerifyPublicationEvidence(publicKey, evidence); err != nil {
		t.Fatalf("VerifyPublicationEvidence() error = %v", err)
	}
	wrongKey, _, _ := ed25519.GenerateKey(nil)
	if err := VerifyPublicationEvidence(wrongKey, evidence); err == nil {
		t.Fatal("wrong publication evidence key was accepted")
	}
	if err := VerifyPublicationEvidence(publicKey, PublicationEvidence{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero publication evidence error = %v", err)
	}

	signature := evidence.OriginSignature()
	signature[0] ^= 0xff
	wire := evidence.WireJSON().Bytes()
	wire[0] = 'x'
	if !bytes.Equal(evidence.OriginSignature(), publication.OriginSignature()) ||
		!bytes.Equal(evidence.WireJSON().Bytes(), publication.WireJSON().Bytes()) {
		t.Fatal("PublicationEvidence accessors exposed mutable bytes")
	}
}

func TestParsePublicationEvidenceRetainsSignedUnsupportedBodies(t *testing.T) {
	t.Parallel()

	home, reviewer := mustPeer(t, "peer-unsupported-home"), mustPeer(t, "peer-unsupported-reviewer")
	event := mustWorkEvent(t, home, home, reviewer, EventReviewOffered)
	body, _ := NewPublicationBody(event)
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	message, _ := PublicationSigningMessage(body.Key().ChannelID(), body.Digest())
	base, err := AttachSignature(body, ed25519.Sign(privateKey, message))
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name       string
		mutate     func(map[string]any)
		wantSchema uint64
	}{
		{name: "future schema and unknown body field", wantSchema: SchemaVersion + 1,
			mutate: func(body map[string]any) {
				body["schema_version"] = float64(SchemaVersion + 1)
				body["future_semantics"] = map[string]any{"mode": "opaque"}
			}},
		{name: "current schema unknown Event type", wantSchema: SchemaVersion,
			mutate: func(body map[string]any) {
				body["event"].(map[string]any)["event_type"] = "review.future"
			}},
		{name: "current schema unknown inner field", wantSchema: SchemaVersion,
			mutate: func(body map[string]any) {
				body["future_extension"] = true
			}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			raw := resignPublicationBody(t, base.WireJSON().Bytes(), privateKey, test.mutate)
			evidence, err := ParsePublicationEvidence(raw)
			if err != nil {
				t.Fatalf("ParsePublicationEvidence() error = %v", err)
			}
			if evidence.IsSupported() || evidence.SchemaVersion() != test.wantSchema ||
				evidence.ChannelID() != event.Scope().ChannelID() ||
				evidence.OriginPeerID() != event.Scope().OriginPeerID() ||
				evidence.Audience().Len() != event.Audience().Len() ||
				!bytes.Equal(evidence.WireJSON().Bytes(), raw) {
				t.Fatalf("unsupported evidence projection = %#v", evidence)
			}
			if err := VerifyPublicationEvidence(publicKey, evidence); err != nil {
				t.Fatalf("unsupported evidence signature error = %v", err)
			}
			if _, err := ParseSignedPublication(raw); err == nil {
				t.Fatal("strict SignedPublication parser accepted an unsupported body")
			}
		})
	}
}

func TestParsePublicationEvidenceRejectsMalformedStableEnvelope(t *testing.T) {
	t.Parallel()

	home, reviewer := mustPeer(t, "peer-evidence-malformed-home"),
		mustPeer(t, "peer-evidence-malformed-reviewer")
	event := mustWorkEvent(t, home, home, reviewer, EventReviewOffered)
	body, _ := NewPublicationBody(event)
	publicKey, privateKey, _ := ed25519.GenerateKey(nil)
	message, _ := PublicationSigningMessage(body.Key().ChannelID(), body.Digest())
	publication, _ := AttachSignature(body, ed25519.Sign(privateKey, message))
	raw := publication.WireJSON().Bytes()

	otherA := mustPeer(t, "peer-evidence-order-z")
	otherB := mustPeer(t, "peer-evidence-order-a")
	overAudience := make([]any, MaxChildWorks+1)
	for index := range overAudience {
		overAudience[index] = reviewer.String()
	}
	tests := []struct {
		name string
		raw  func() []byte
	}{
		{name: "unknown outer field", raw: func() []byte {
			return mutateCanonicalPublication(t, raw, func(outer map[string]any) {
				outer["extension"] = true
			})
		}},
		{name: "wrong-case outer field", raw: func() []byte {
			return mutateCanonicalPublication(t, raw, func(outer map[string]any) {
				outer["Origin_Signature"] = outer["origin_signature"]
				delete(outer, "origin_signature")
			})
		}},
		{name: "missing outer digest", raw: func() []byte {
			return mutateCanonicalPublication(t, raw, func(outer map[string]any) {
				delete(outer, "publication_digest")
			})
		}},
		{name: "wrong claimed digest", raw: func() []byte {
			return mutateCanonicalPublication(t, raw, func(outer map[string]any) {
				outer["publication_digest"] = Sum([]byte("wrong evidence body")).String()
			})
		}},
		{name: "short signature", raw: func() []byte {
			return mutateCanonicalPublication(t, raw, func(outer map[string]any) {
				outer["origin_signature"] = "c2hvcnQ="
			})
		}},
		{name: "missing stable Channel", raw: func() []byte {
			return resignPublicationBody(t, raw, privateKey, func(body map[string]any) {
				delete(body, "channel_id")
			})
		}},
		{name: "wrong-case stable Channel", raw: func() []byte {
			return resignPublicationBody(t, raw, privateKey, func(body map[string]any) {
				body["Channel_ID"] = body["channel_id"]
				delete(body, "channel_id")
			})
		}},
		{name: "zero stable sequence", raw: func() []byte {
			return resignPublicationBody(t, raw, privateKey, func(body map[string]any) {
				body["channel_seq"] = float64(0)
			})
		}},
		{name: "invalid Event digest", raw: func() []byte {
			return resignPublicationBody(t, raw, privateKey, func(body map[string]any) {
				body["event_digest"] = "sha256:not-a-digest"
			})
		}},
		{name: "zero origin member revision", raw: func() []byte {
			return resignPublicationBody(t, raw, privateKey, func(body map[string]any) {
				body["origin_member"].(map[string]any)["revision"] = float64(0)
			})
		}},
		{name: "empty audience", raw: func() []byte {
			return resignPublicationBody(t, raw, privateKey, func(body map[string]any) {
				body["audience"] = []any{}
			})
		}},
		{name: "oversized audience", raw: func() []byte {
			return resignPublicationBody(t, raw, privateKey, func(body map[string]any) {
				body["audience"] = overAudience
			})
		}},
		{name: "unsorted audience", raw: func() []byte {
			return resignPublicationBody(t, raw, privateKey, func(body map[string]any) {
				body["audience"] = []any{otherA.String(), otherB.String()}
			})
		}},
		{name: "origin in audience", raw: func() []byte {
			return resignPublicationBody(t, raw, privateKey, func(body map[string]any) {
				body["audience"] = []any{event.Scope().OriginPeerID().String()}
			})
		}},
		{name: "duplicate outer key", raw: func() []byte {
			end := bytes.IndexByte(raw, ',')
			result := append([]byte{'{'}, raw[1:end]...)
			result = append(result, ',')
			return append(result, raw[1:]...)
		}},
		{name: "noncanonical outer", raw: func() []byte {
			return append([]byte{' '}, raw...)
		}},
		{name: "wire limit", raw: func() []byte {
			return bytes.Repeat([]byte{'x'}, MaxPublicationBytes+1)
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := ParsePublicationEvidence(test.raw()); err == nil {
				t.Fatal("malformed stable publication evidence was accepted")
			}
		})
	}

	var wire signedPublicationWire
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	wire.OriginSignature[0] ^= 0xff
	tampered, err := JSONFrom(wire)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := ParsePublicationEvidence(tampered.Bytes())
	if err != nil {
		t.Fatalf("opaque 64-byte signature should reach authority verification: %v", err)
	}
	if err := VerifyPublicationEvidence(publicKey, evidence); err == nil {
		t.Fatal("tampered 64-byte publication signature was authenticated")
	}
}

func TestParseSignedPublicationRejectsSchemaAndBindingTampering(t *testing.T) {
	t.Parallel()

	home, reviewer := mustPeer(t, "peer-tamper-home"), mustPeer(t, "peer-tamper-reviewer")
	event := mustWorkEvent(t, home, home, reviewer, EventReviewOffered)
	body, _ := NewPublicationBody(event)
	publication, _ := AttachSignature(body, testPublicationSignature())
	raw := publication.WireJSON().Bytes()

	tests := map[string]func(map[string]any){
		"unknown outer field": func(outer map[string]any) {
			outer["authority"] = "forged"
		},
		"unsupported body schema": func(outer map[string]any) {
			outer["publication"].(map[string]any)["schema_version"] = float64(2)
		},
		"claimed digest": func(outer map[string]any) {
			outer["publication_digest"] = Sum([]byte("tampered")).String()
		},
		"duplicated Channel": func(outer map[string]any) {
			outer["publication"].(map[string]any)["channel_id"] = "channel-forged"
		},
		"embedded Event": func(outer map[string]any) {
			body := outer["publication"].(map[string]any)
			body["event"].(map[string]any)["summary"] = "tampered"
		},
		"short signature": func(outer map[string]any) {
			outer["origin_signature"] = "c2hvcnQ="
		},
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := ParseSignedPublication(mutateCanonicalPublication(t, raw, mutate)); err == nil {
				t.Fatal("tampered publication was accepted")
			}
		})
	}
}

func TestParseSignedPublicationAppliesWireLimitBeforeDecode(t *testing.T) {
	t.Parallel()

	raw := bytes.Repeat([]byte{'x'}, MaxPublicationBytes+1)
	if _, err := ParseSignedPublication(raw); !errors.Is(err, ErrLimit) {
		t.Fatalf("oversized publication error = %v, want ErrLimit", err)
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

func mutateCanonicalPublication(t *testing.T, raw []byte, mutate func(map[string]any)) []byte {
	t.Helper()
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	mutate(value)
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := NewJSON(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return canonical.Bytes()
}

func resignPublicationBody(t *testing.T, raw []byte, privateKey ed25519.PrivateKey,
	mutate func(map[string]any),
) []byte {
	t.Helper()
	var wire signedPublicationWire
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(wire.Publication, &body); err != nil {
		t.Fatal(err)
	}
	originalChannel, err := ParseChannelID(body["channel_id"].(string))
	if err != nil {
		t.Fatal(err)
	}
	mutate(body)
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := NewJSON(encoded)
	if err != nil {
		t.Fatal(err)
	}
	digest := Sum(canonical.Bytes())
	message, err := PublicationSigningMessage(originalChannel, digest)
	if err != nil {
		t.Fatal(err)
	}
	wire.Publication = canonical.Bytes()
	wire.PublicationDigest = digest.String()
	wire.OriginSignature = ed25519.Sign(privateKey, message)
	result, err := JSONFrom(wire)
	if err != nil {
		t.Fatal(err)
	}
	return result.Bytes()
}
