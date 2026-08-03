package selector

import (
	"bytes"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

const (
	SeedOpinionVersion           = 1
	MaxSeedOpinionCanonicalBytes = 512
)

// SeedOpinion is the complete machine-readable part of one local semantic
// judgment. Rationale and evidence remain separate R7 Artifacts; selector
// needs only the exact scope and A/B choice. Cognitive independence is a
// composition precondition and is not proven by this value.
type SeedOpinion struct {
	selectionID SelectionID
	preference  Preference
	canonical   []byte
	digest      agency.Digest
}

type seedOpinionWire struct {
	Preference  string `json:"preference"`
	SelectionID string `json:"selection_id"`
	Version     uint32 `json:"version"`
}

func NewSeedOpinion(selectionID SelectionID, preference Preference) (SeedOpinion, error) {
	if selectionID.IsZero() || !validPreference(preference) {
		return SeedOpinion{}, fmt.Errorf("seed opinion scope and preference are required: %w", ErrInvalid)
	}
	canonical, err := canonicalMarshal(seedOpinionWire{Preference: preference.String(),
		SelectionID: selectionID.String(), Version: SeedOpinionVersion})
	if err != nil {
		return SeedOpinion{}, err
	}
	if len(canonical) > MaxSeedOpinionCanonicalBytes {
		return SeedOpinion{}, fmt.Errorf("seed opinion has %d bytes (max %d): %w",
			len(canonical), MaxSeedOpinionCanonicalBytes, ErrLimit)
	}
	return SeedOpinion{selectionID: selectionID, preference: preference,
		canonical: canonical, digest: agency.Sum(canonical)}, nil
}

func ParseSeedOpinionCanonical(value []byte) (SeedOpinion, error) {
	if err := validateFrameSize("seed opinion", value, MaxSeedOpinionCanonicalBytes); err != nil {
		return SeedOpinion{}, err
	}
	var wire seedOpinionWire
	if err := decodeClosedFrame("seed opinion", value, &wire); err != nil {
		return SeedOpinion{}, err
	}
	if wire.Version != SeedOpinionVersion {
		return SeedOpinion{}, fmt.Errorf("seed opinion version %d: %w", wire.Version, ErrInvalid)
	}
	selectionID, err := ParseSelectionID(wire.SelectionID)
	if err != nil {
		return SeedOpinion{}, fmt.Errorf("seed opinion selection: %w", err)
	}
	preference, err := ParsePreference(wire.Preference)
	if err != nil {
		return SeedOpinion{}, err
	}
	opinion, err := NewSeedOpinion(selectionID, preference)
	if err != nil {
		return SeedOpinion{}, err
	}
	if !bytes.Equal(value, opinion.canonical) {
		return SeedOpinion{}, fmt.Errorf("seed opinion is not exact canonical JSON: %w", ErrInvalid)
	}
	return opinion, nil
}

func (o SeedOpinion) SelectionID() SelectionID { return o.selectionID }
func (o SeedOpinion) Preference() Preference   { return o.preference }
func (o SeedOpinion) CanonicalBytes() []byte   { return append([]byte(nil), o.canonical...) }
func (o SeedOpinion) Digest() agency.Digest    { return o.digest }
func (o SeedOpinion) valid() bool {
	return !o.selectionID.IsZero() && validPreference(o.preference) &&
		len(o.canonical) > 0 && agency.Sum(o.canonical) == o.digest
}

// BindAcceptedSeedOpinion derives selector provenance only from one exact R7
// request and its accepted Receipt. The trusted composition adapter must pass
// the actual durable admission objects; this package checks their in-process
// consistency without importing or reading the R7 authority store.
func BindAcceptedSeedOpinion(request agency.BoundIntent, receipt agency.Receipt,
	descriptor SelectionDescriptor, opinion SeedOpinion,
) (AcceptedSeedOpinion, error) {
	if descriptor.validate() != nil || !opinion.valid() ||
		opinion.selectionID != descriptor.id || request.OperationKey().IsZero() ||
		request.RequestDigest().IsZero() ||
		request.Attachment().Principal().IsZero() {
		return AcceptedSeedOpinion{}, fmt.Errorf("accepted seed binding is incomplete: %w", ErrInvalid)
	}
	event, present := receipt.Event()
	if receipt.Outcome() != agency.ReceiptOutcomeAccepted || !present ||
		receipt.OperationKey() != request.OperationKey() ||
		receipt.RequestDigest() != request.RequestDigest() {
		return AcceptedSeedOpinion{}, fmt.Errorf("Receipt does not accept the exact seed request: %w", ErrConflict)
	}
	artifacts := request.Artifacts()
	if !containsExactDigest(artifacts, descriptor.id.digest) ||
		!containsExactDigest(artifacts, opinion.digest) {
		return AcceptedSeedOpinion{}, fmt.Errorf(
			"accepted seed request does not cite its descriptor and opinion Artifacts: %w", ErrConflict)
	}
	return restoreAcceptedSeedOpinion(opinion, request.Attachment().Principal(), event)
}

func containsExactDigest(values []agency.Digest, expected agency.Digest) bool {
	found := false
	for _, value := range values {
		if value == expected {
			if found {
				return false
			}
			found = true
		}
	}
	return found
}
