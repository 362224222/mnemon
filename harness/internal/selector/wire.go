package selector

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

const (
	SampleFrameVersion       = 1
	MaxSampleQueryFrameBytes = 512
	MaxSampleVoteFrameBytes  = 2 << 10
)

type sampleQueryFrame struct {
	Version     uint32 `json:"version"`
	SelectionID string `json:"selection_id"`
	Round       uint32 `json:"round"`
	Nonce       string `json:"nonce"`
}

type sampleVoteFrame struct {
	Version       uint32 `json:"version"`
	SelectionID   string `json:"selection_id"`
	Round         uint32 `json:"round"`
	Nonce         string `json:"nonce"`
	Preference    string `json:"preference"`
	ClaimedSource string `json:"claimed_source"`
}

// CanonicalBytes encodes only the machine fields needed to correlate one
// bounded sample query. It carries no natural language, Artifact bytes, or R7
// authority.
func (q SampleQuery) CanonicalBytes() ([]byte, error) {
	if err := validateSampleQueryFrame(q); err != nil {
		return nil, err
	}
	encoded, err := canonicalMarshal(sampleQueryFrame{Version: SampleFrameVersion,
		SelectionID: q.selectionID.String(), Round: q.round, Nonce: q.nonce.String()})
	if err != nil {
		return nil, err
	}
	if len(encoded) > MaxSampleQueryFrameBytes {
		return nil, fmt.Errorf("sample query has %d bytes (max %d): %w",
			len(encoded), MaxSampleQueryFrameBytes, ErrLimit)
	}
	return encoded, nil
}

// ParseSampleQueryCanonical accepts exactly one closed, canonical JSON frame.
func ParseSampleQueryCanonical(value []byte) (SampleQuery, error) {
	if err := validateFrameSize("sample query", value, MaxSampleQueryFrameBytes); err != nil {
		return SampleQuery{}, err
	}
	var wire sampleQueryFrame
	if err := decodeClosedFrame("sample query", value, &wire); err != nil {
		return SampleQuery{}, err
	}
	if wire.Version != SampleFrameVersion {
		return SampleQuery{}, fmt.Errorf("sample query version %d: %w", wire.Version, ErrInvalid)
	}
	selectionID, err := ParseSelectionID(wire.SelectionID)
	if err != nil {
		return SampleQuery{}, fmt.Errorf("sample query selection: %w", err)
	}
	nonce, err := agency.ParseDigest(wire.Nonce)
	if err != nil {
		return SampleQuery{}, fmt.Errorf("sample query nonce: %w", err)
	}
	query, err := NewSampleQuery(selectionID, wire.Round, nonce)
	if err != nil {
		return SampleQuery{}, err
	}
	if err := validateSampleQueryFrame(query); err != nil {
		return SampleQuery{}, err
	}
	canonical, err := query.CanonicalBytes()
	if err != nil {
		return SampleQuery{}, err
	}
	if !bytes.Equal(value, canonical) {
		return SampleQuery{}, fmt.Errorf("sample query is not exact canonical JSON: %w", ErrInvalid)
	}
	return query, nil
}

// CanonicalBytes encodes one claimed vote. Counting authority is deliberately
// absent: AuthenticateSampleVote must bind this claim to an independently
// authenticated peer before ApplyRound can consume it.
func (v SampleVote) CanonicalBytes() ([]byte, error) {
	if err := validateSampleVoteFrame(v); err != nil {
		return nil, err
	}
	encoded, err := canonicalMarshal(sampleVoteFrame{Version: SampleFrameVersion,
		SelectionID: v.selectionID.String(), Round: v.round, Nonce: v.nonce.String(),
		Preference: v.preference.String(), ClaimedSource: v.claimedBy.String()})
	if err != nil {
		return nil, err
	}
	if len(encoded) > MaxSampleVoteFrameBytes {
		return nil, fmt.Errorf("sample vote has %d bytes (max %d): %w",
			len(encoded), MaxSampleVoteFrameBytes, ErrLimit)
	}
	return encoded, nil
}

// ParseSampleVoteCanonical accepts exactly one closed, canonical JSON frame.
func ParseSampleVoteCanonical(value []byte) (SampleVote, error) {
	if err := validateFrameSize("sample vote", value, MaxSampleVoteFrameBytes); err != nil {
		return SampleVote{}, err
	}
	var wire sampleVoteFrame
	if err := decodeClosedFrame("sample vote", value, &wire); err != nil {
		return SampleVote{}, err
	}
	if wire.Version != SampleFrameVersion {
		return SampleVote{}, fmt.Errorf("sample vote version %d: %w", wire.Version, ErrInvalid)
	}
	selectionID, err := ParseSelectionID(wire.SelectionID)
	if err != nil {
		return SampleVote{}, fmt.Errorf("sample vote selection: %w", err)
	}
	nonce, err := agency.ParseDigest(wire.Nonce)
	if err != nil {
		return SampleVote{}, fmt.Errorf("sample vote nonce: %w", err)
	}
	preference, err := ParsePreference(wire.Preference)
	if err != nil {
		return SampleVote{}, err
	}
	claimedSource, err := NewParticipantID(wire.ClaimedSource)
	if err != nil {
		return SampleVote{}, fmt.Errorf("sample vote claimed source: %w", err)
	}
	vote, err := NewSampleVote(selectionID, wire.Round, nonce, preference, claimedSource)
	if err != nil {
		return SampleVote{}, err
	}
	if err := validateSampleVoteFrame(vote); err != nil {
		return SampleVote{}, err
	}
	canonical, err := vote.CanonicalBytes()
	if err != nil {
		return SampleVote{}, err
	}
	if !bytes.Equal(value, canonical) {
		return SampleVote{}, fmt.Errorf("sample vote is not exact canonical JSON: %w", ErrInvalid)
	}
	return vote, nil
}

func validateSampleQueryFrame(query SampleQuery) error {
	if query.selectionID.IsZero() || query.round == 0 || query.nonce.IsZero() {
		return fmt.Errorf("sample query fields are incomplete: %w", ErrInvalid)
	}
	if query.round > MaxRounds {
		return fmt.Errorf("sample query round %d exceeds %d: %w", query.round, MaxRounds, ErrLimit)
	}
	return nil
}

func validateSampleVoteFrame(vote SampleVote) error {
	if vote.selectionID.IsZero() || vote.round == 0 || vote.nonce.IsZero() ||
		!validPreference(vote.preference) || vote.claimedBy.IsZero() {
		return fmt.Errorf("sample vote fields are incomplete: %w", ErrInvalid)
	}
	if vote.round > MaxRounds {
		return fmt.Errorf("sample vote round %d exceeds %d: %w", vote.round, MaxRounds, ErrLimit)
	}
	return nil
}

func validateFrameSize(name string, value []byte, maximum int) error {
	if len(value) == 0 {
		return fmt.Errorf("%s is empty: %w", name, ErrInvalid)
	}
	if len(value) > maximum {
		return fmt.Errorf("%s has %d bytes (max %d): %w", name, len(value), maximum, ErrLimit)
	}
	return nil
}

func decodeClosedFrame(name string, value []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode %s: %v: %w", name, err, ErrInvalid)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return fmt.Errorf("decode %s: %v: %w", name, err, ErrInvalid)
	}
	return nil
}
