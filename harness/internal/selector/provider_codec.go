package selector

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

func canonicalSample(sample []ParticipantID) ([]byte, error) {
	values := make([]string, len(sample))
	for index, peer := range sample {
		if peer.IsZero() {
			return nil, fmt.Errorf("sample contains zero participant: %w", ErrInvalid)
		}
		values[index] = peer.String()
	}
	sort.Strings(values)
	for index := 1; index < len(values); index++ {
		if values[index-1] == values[index] {
			return nil, fmt.Errorf("sample contains duplicate participant: %w", ErrInvalid)
		}
	}
	return canonicalMarshal(values)
}

func parseSampleCanonical(value []byte) ([]ParticipantID, error) {
	var wire []string
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return nil, fmt.Errorf("decode pending sample: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, fmt.Errorf("decode pending sample: %w", err)
	}
	sample := make([]ParticipantID, len(wire))
	for index, raw := range wire {
		peer, err := NewParticipantID(raw)
		if err != nil {
			return nil, fmt.Errorf("decode pending sample participant %d: %w", index, err)
		}
		sample[index] = peer
	}
	canonical, err := canonicalSample(sample)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(value, canonical) {
		return nil, fmt.Errorf("pending sample is not canonical: %w", ErrState)
	}
	return sample, nil
}

func parseObservationCanonical(value []byte, digest agency.Digest,
	descriptor SelectionDescriptor, state SelectionState,
) (PreferenceObservation, error) {
	if len(value) == 0 || digest.IsZero() || agency.Sum(value) != digest {
		return PreferenceObservation{}, fmt.Errorf("stored observation digest mismatch: %w", ErrState)
	}
	wire, err := decodeObservationWire(value)
	if err != nil {
		return PreferenceObservation{}, err
	}
	if err := validateObservationAuthority(wire, descriptor, state); err != nil {
		return PreferenceObservation{}, err
	}
	result, preference, reason, err := observationValues(wire, descriptor, state)
	if err != nil {
		return PreferenceObservation{}, err
	}
	observation, err := newObservation(descriptor, state, result, preference, reason)
	if err != nil || !bytes.Equal(value, observation.canonical) || observation.Digest() != digest {
		return PreferenceObservation{}, fmt.Errorf("stored observation is not canonical: %w", ErrState)
	}
	return observation, nil
}

func decodeObservationWire(value []byte) (observationWire, error) {
	var wire observationWire
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return observationWire{}, fmt.Errorf("decode stored observation: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return observationWire{}, fmt.Errorf("decode stored observation: %w", err)
	}
	return wire, nil
}

func validateObservationAuthority(wire observationWire, descriptor SelectionDescriptor,
	state SelectionState,
) error {
	if wire.SelectionID != descriptor.id.String() || wire.Margin != state.margin ||
		wire.Rounds != state.round || wire.RosterDigest != descriptor.rosterHash.String() ||
		wire.ProfileDigest != descriptor.profile.Digest().String() {
		return fmt.Errorf("stored observation authority mismatch: %w", ErrState)
	}
	return nil
}

func observationValues(wire observationWire, descriptor SelectionDescriptor,
	state SelectionState,
) (ObservationResult, Preference, InconclusiveReason, error) {
	result := ObservationResult(wire.Result)
	switch result {
	case ObservationThresholdReached:
		preference, err := thresholdObservationPreference(wire, descriptor, state)
		return result, preference, "", err
	case ObservationInconclusive:
		reason, err := inconclusiveObservationReason(wire, descriptor, state)
		return result, 0, reason, err
	default:
		return "", 0, "", fmt.Errorf("stored observation result %q: %w", result, ErrState)
	}
}

func thresholdObservationPreference(wire observationWire, descriptor SelectionDescriptor,
	state SelectionState,
) (Preference, error) {
	if wire.Preference == nil || wire.Reason != nil {
		return 0, fmt.Errorf("stored threshold observation shape: %w", ErrState)
	}
	preference, err := ParsePreference(*wire.Preference)
	if err != nil || abs64(state.margin) < int64(descriptor.profile.threshold) ||
		(preference == PreferenceA) != (state.margin > 0) {
		return 0, fmt.Errorf("stored threshold observation value: %w", ErrState)
	}
	return preference, nil
}

func inconclusiveObservationReason(wire observationWire, descriptor SelectionDescriptor,
	state SelectionState,
) (InconclusiveReason, error) {
	if wire.Preference != nil || wire.Reason == nil {
		return "", fmt.Errorf("stored inconclusive observation shape: %w", ErrState)
	}
	reason := InconclusiveReason(*wire.Reason)
	if reason != ReasonExpired &&
		(reason != ReasonRoundLimit || state.round < descriptor.profile.maxRounds) {
		return "", fmt.Errorf("stored inconclusive observation reason: %w", ErrState)
	}
	return reason, nil
}

type voteWire struct {
	Nonce      string `json:"nonce"`
	Preference string `json:"preference"`
	Round      uint32 `json:"round"`
	Selection  string `json:"selection_id"`
	Source     string `json:"source"`
}

func canonicalVoteSet(votes []SampleVote) ([]byte, error) {
	wire := make([]voteWire, len(votes))
	for index, vote := range votes {
		wire[index] = voteWire{Selection: vote.selectionID.String(), Round: vote.round,
			Nonce: vote.nonce.String(), Preference: vote.preference.String(), Source: vote.source.String()}
	}
	sort.Slice(wire, func(left, right int) bool {
		return lessVoteWire(wire[left], wire[right])
	})
	return canonicalMarshal(wire)
}

func lessVoteWire(left, right voteWire) bool {
	leftFields := [...]string{left.Selection, fmt.Sprint(left.Round), left.Nonce,
		left.Preference, left.Source}
	rightFields := [...]string{right.Selection, fmt.Sprint(right.Round), right.Nonce,
		right.Preference, right.Source}
	for index := range leftFields {
		if leftFields[index] != rightFields[index] {
			return leftFields[index] < rightFields[index]
		}
	}
	return false
}
