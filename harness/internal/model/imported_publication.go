package model

import (
	"bytes"
	"fmt"
)

// ProjectImportedPublication creates the receiver-local projection of an
// already authenticated signed publication. Event source is local-only state,
// so changing it to imported must not change any immutable Event or
// publication identity.
//
// The caller remains responsible for authenticating the origin signature with
// VerifyPublication and the applicable Channel authority. This helper strictly
// revalidates the publication's internal/wire consistency before projecting it.
func ProjectImportedPublication(publication *SignedPublication) (SignedPublication, error) {
	if publication == nil {
		return SignedPublication{}, invalid("imported publication", "must not be nil")
	}
	if publication.Event().ID().IsZero() || publication.Event().Digest().IsZero() ||
		publication.Digest().IsZero() || publication.CanonicalJSON().IsZero() ||
		publication.WireJSON().IsZero() || len(publication.OriginSignature()) == 0 {
		return SignedPublication{}, invalid("imported publication", "complete signed publication is required")
	}

	// Rebuild from the value-facing Event API first. This detects an internally
	// inconsistent value instead of silently treating its wire bytes as the
	// authoritative replacement for corrupted in-memory fields.
	event := publication.Event()
	rebuiltEvent, err := rebuildPublicationEvent(event, event.Source())
	if err != nil {
		return SignedPublication{}, fmt.Errorf("project imported publication: %w", err)
	}
	if rebuiltEvent.Digest() != event.Digest() ||
		!bytes.Equal(rebuiltEvent.CanonicalJSON().Bytes(), event.CanonicalJSON().Bytes()) {
		return SignedPublication{}, invariant("signed publication Event fields disagree with its canonical identity")
	}
	rebuiltBody, err := NewPublicationBody(rebuiltEvent)
	if err != nil {
		return SignedPublication{}, fmt.Errorf("project imported publication body: %w", err)
	}
	if rebuiltBody.Key() != publication.Key() || rebuiltBody.Digest() != publication.Digest() ||
		!bytes.Equal(rebuiltBody.CanonicalJSON().Bytes(), publication.CanonicalJSON().Bytes()) {
		return SignedPublication{}, invariant("signed publication body fields disagree with its canonical identity")
	}
	rebuiltPublication, err := AttachSignature(rebuiltBody, publication.OriginSignature())
	if err != nil {
		return SignedPublication{}, fmt.Errorf("project imported publication signature: %w", err)
	}
	if !bytes.Equal(rebuiltPublication.WireJSON().Bytes(), publication.WireJSON().Bytes()) {
		return SignedPublication{}, invariant("signed publication fields disagree with its exact wire")
	}

	// Parse the exact wire again so the result cannot share collection backing
	// arrays with the caller's in-memory publication.
	parsed, err := ParseSignedPublication(publication.WireJSON().Bytes())
	if err != nil {
		return SignedPublication{}, fmt.Errorf("project imported publication wire: %w", err)
	}
	if parsed.Key() != publication.Key() || parsed.Digest() != publication.Digest() ||
		parsed.Event().Key() != event.Key() || parsed.Event().Digest() != event.Digest() ||
		!bytes.Equal(parsed.OriginSignature(), publication.OriginSignature()) {
		return SignedPublication{}, invariant("parsed publication wire disagrees with its in-memory identity")
	}

	importedEvent, err := rebuildPublicationEvent(parsed.Event(), EventSourceImported)
	if err != nil {
		return SignedPublication{}, fmt.Errorf("project imported Event: %w", err)
	}
	importedBody, err := NewPublicationBody(importedEvent)
	if err != nil {
		return SignedPublication{}, fmt.Errorf("project imported publication body: %w", err)
	}
	imported, err := AttachSignature(importedBody, parsed.OriginSignature())
	if err != nil {
		return SignedPublication{}, fmt.Errorf("project imported publication signature: %w", err)
	}
	if imported.Event().Key() != parsed.Event().Key() || imported.Event().Digest() != parsed.Event().Digest() ||
		imported.Key() != parsed.Key() || imported.Digest() != parsed.Digest() ||
		!bytes.Equal(imported.Event().CanonicalJSON().Bytes(), parsed.Event().CanonicalJSON().Bytes()) ||
		!bytes.Equal(imported.CanonicalJSON().Bytes(), parsed.CanonicalJSON().Bytes()) ||
		!bytes.Equal(imported.OriginSignature(), parsed.OriginSignature()) ||
		!bytes.Equal(imported.WireJSON().Bytes(), parsed.WireJSON().Bytes()) {
		return SignedPublication{}, invariant("import source projection changed immutable publication identity")
	}
	return imported, nil
}

func rebuildPublicationEvent(event Event, source EventSource) (Event, error) {
	return NewEvent(EventSpec{
		ID:             event.ID(),
		Scope:          event.Scope(),
		Source:         source,
		ActorPrincipal: event.ActorPrincipal(),
		Type:           event.Type(),
		Audience:       event.Audience(),
		Summary:        event.Summary(),
		Payload:        event.Payload(),
		Artifacts:      event.Artifacts(),
		CausedBy:       event.CausedBy(),
		CreatedAt:      event.CreatedAt(),
		AcceptedAt:     event.AcceptedAt(),
	})
}
