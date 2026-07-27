package model

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"
)

func FuzzParseSignedPublication(f *testing.F) {
	valid := signedPublicationFuzzSeed(f)
	for _, raw := range [][]byte{
		nil,
		{},
		[]byte(`{}`),
		[]byte{0xff, 0x00, 0x01},
		append([]byte(" "), valid...),
		append(append([]byte(nil), valid...), '\n'),
		bytes.Repeat([]byte{'x'}, MaxPublicationBytes+1),
	} {
		f.Add(raw)
	}
	for _, cut := range []int{1, len(valid) / 2, len(valid) - 1} {
		f.Add(append([]byte(nil), valid[:cut]...))
	}
	f.Add(valid)

	f.Fuzz(func(t *testing.T, raw []byte) {
		publication, err := ParseSignedPublication(raw)
		if err != nil {
			if len(raw) > MaxPublicationBytes && !errors.Is(err, ErrLimit) {
				t.Fatalf("oversized publication error = %v, want ErrLimit", err)
			}
			return
		}
		if len(raw) == 0 || len(raw) > MaxPublicationBytes {
			t.Fatalf("ParseSignedPublication accepted %d bytes", len(raw))
		}
		if !bytes.Equal(publication.WireJSON().Bytes(), raw) {
			t.Fatal("parsed publication did not retain the exact canonical wire")
		}
		if publication.Digest() != Sum(publication.CanonicalJSON().Bytes()) {
			t.Fatal("parsed publication digest differs from its canonical body")
		}

		rebuiltBody, err := NewPublicationBody(publication.Event())
		if err != nil {
			t.Fatalf("rebuild publication body: %v", err)
		}
		rebuilt, err := AttachSignature(rebuiltBody, publication.OriginSignature())
		if err != nil {
			t.Fatalf("reattach parsed publication signature: %v", err)
		}
		if !bytes.Equal(rebuilt.WireJSON().Bytes(), raw) {
			t.Fatal("parsed publication did not round-trip through production constructors")
		}

		roundTrip, err := ParseSignedPublication(rebuilt.WireJSON().Bytes())
		if err != nil {
			t.Fatalf("parse rebuilt publication: %v", err)
		}
		if roundTrip.Key() != publication.Key() ||
			roundTrip.Digest() != publication.Digest() ||
			roundTrip.Event().Digest() != publication.Event().Digest() ||
			!bytes.Equal(roundTrip.OriginSignature(), publication.OriginSignature()) {
			t.Fatal("publication round-trip changed immutable identity")
		}
	})
}

func signedPublicationFuzzSeed(f *testing.F) []byte {
	f.Helper()
	home, err := ParsePeerID("peer-publication-fuzz-home")
	if err != nil {
		f.Fatal(err)
	}
	reviewer, err := ParsePeerID("peer-publication-fuzz-reviewer")
	if err != nil {
		f.Fatal(err)
	}
	channelID, err := ParseChannelID("channel-publication-fuzz")
	if err != nil {
		f.Fatal(err)
	}
	originEpoch, err := ParseOriginEpoch("epoch-publication-fuzz")
	if err != nil {
		f.Fatal(err)
	}
	workID, err := ParseWorkID("work-publication-fuzz")
	if err != nil {
		f.Fatal(err)
	}
	work, err := NewWorkRef(home, workID)
	if err != nil {
		f.Fatal(err)
	}
	head, err := NewRecordHead(1, Sum([]byte("publication fuzz roster")))
	if err != nil {
		f.Fatal(err)
	}
	scope, err := NewEventScope(channelID, home, originEpoch, 1, 1, head, head, work)
	if err != nil {
		f.Fatal(err)
	}
	eventID, err := ParseEventID("event-publication-fuzz")
	if err != nil {
		f.Fatal(err)
	}
	audience, err := NewAudience([]PeerID{reviewer})
	if err != nil {
		f.Fatal(err)
	}
	payload, err := NewJSON([]byte(`{"iteration":1}`))
	if err != nil {
		f.Fatal(err)
	}
	acceptedAt := time.Date(2026, 7, 27, 12, 0, 0, 123, time.UTC)
	event, err := NewEvent(EventSpec{
		ID:             eventID,
		Scope:          scope,
		Source:         EventSourceLocal,
		ActorPrincipal: "principal-publication-fuzz",
		Type:           EventReviewOffered,
		Audience:       audience,
		Summary:        "publication fuzz seed",
		Payload:        payload,
		CreatedAt:      acceptedAt,
		AcceptedAt:     acceptedAt,
	})
	if err != nil {
		f.Fatal(err)
	}
	body, err := NewPublicationBody(event)
	if err != nil {
		f.Fatal(err)
	}
	publication, err := AttachSignature(body,
		bytes.Repeat([]byte{0x5a}, ed25519.SignatureSize))
	if err != nil {
		f.Fatal(err)
	}
	return publication.WireJSON().Bytes()
}
