package selector

import (
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

type ObservationResult string

const (
	ObservationStable       ObservationResult = "stable"
	ObservationInconclusive ObservationResult = "inconclusive"
)

type InconclusiveReason string

const (
	ReasonRoundLimit InconclusiveReason = "round_limit"
	ReasonExpired    InconclusiveReason = "expired"
)

// PreferenceObservation is a bounded, canonical report. It is evidence about
// the selector run, not an R7 effect or a claim that either candidate is true.
type PreferenceObservation struct {
	selectionID   SelectionID
	result        ObservationResult
	preference    Preference
	reason        InconclusiveReason
	margin        int64
	rounds        uint32
	rosterDigest  agency.Digest
	profileDigest agency.Digest
	canonical     []byte
}

type observationWire struct {
	Margin        int64   `json:"margin"`
	Preference    *string `json:"preference"`
	ProfileDigest string  `json:"profile_digest"`
	Reason        *string `json:"reason"`
	Result        string  `json:"result"`
	RosterDigest  string  `json:"roster_digest"`
	Rounds        uint32  `json:"rounds"`
	SelectionID   string  `json:"selection_id"`
}

// Observe returns ready=false while another round remains legal. Stable
// margins take precedence over an expiry or round limit reached by that round.
func Observe(descriptor SelectionDescriptor, state SelectionState, now time.Time) (PreferenceObservation, bool, error) {
	if err := descriptor.validate(); err != nil {
		return PreferenceObservation{}, false, err
	}
	if err := state.validate(descriptor); err != nil {
		return PreferenceObservation{}, false, err
	}
	if now.IsZero() {
		return PreferenceObservation{}, false, fmt.Errorf("observation clock is required: %w", ErrInvalid)
	}
	if state.margin >= int64(descriptor.profile.threshold) {
		observation, err := newObservation(descriptor, state, ObservationStable, PreferenceA, "")
		return observation, true, err
	}
	if state.margin <= -int64(descriptor.profile.threshold) {
		observation, err := newObservation(descriptor, state, ObservationStable, PreferenceB, "")
		return observation, true, err
	}
	if state.round >= descriptor.profile.maxRounds {
		observation, err := newObservation(descriptor, state, ObservationInconclusive, 0, ReasonRoundLimit)
		return observation, true, err
	}
	if !now.Round(0).UTC().Before(descriptor.expiresAt) {
		observation, err := newObservation(descriptor, state, ObservationInconclusive, 0, ReasonExpired)
		return observation, true, err
	}
	return PreferenceObservation{}, false, nil
}

func newObservation(descriptor SelectionDescriptor, state SelectionState, result ObservationResult,
	preference Preference, reason InconclusiveReason,
) (PreferenceObservation, error) {
	profileDigest := descriptor.profile.Digest()
	var preferenceWire, reasonWire *string
	if result == ObservationStable {
		value := preference.String()
		preferenceWire = &value
	} else {
		value := string(reason)
		reasonWire = &value
	}
	wire := observationWire{
		Margin: state.margin, Preference: preferenceWire, ProfileDigest: profileDigest.String(),
		Reason: reasonWire, Result: string(result), RosterDigest: descriptor.rosterHash.String(),
		Rounds: state.round, SelectionID: descriptor.id.String(),
	}
	canonical, err := canonicalMarshal(wire)
	if err != nil {
		return PreferenceObservation{}, fmt.Errorf("canonicalize preference observation: %w", err)
	}
	return PreferenceObservation{
		selectionID: descriptor.id, result: result, preference: preference, reason: reason,
		margin: state.margin, rounds: state.round, rosterDigest: descriptor.rosterHash,
		profileDigest: profileDigest, canonical: canonical,
	}, nil
}

func (o PreferenceObservation) SelectionID() SelectionID     { return o.selectionID }
func (o PreferenceObservation) Result() ObservationResult    { return o.result }
func (o PreferenceObservation) Reason() InconclusiveReason   { return o.reason }
func (o PreferenceObservation) Margin() int64                { return o.margin }
func (o PreferenceObservation) Rounds() uint32               { return o.rounds }
func (o PreferenceObservation) RosterDigest() agency.Digest  { return o.rosterDigest }
func (o PreferenceObservation) ProfileDigest() agency.Digest { return o.profileDigest }
func (o PreferenceObservation) CanonicalBytes() []byte       { return append([]byte(nil), o.canonical...) }
func (o PreferenceObservation) Digest() agency.Digest        { return agency.Sum(o.canonical) }

func (o PreferenceObservation) StablePreference() (Preference, bool) {
	return o.preference, o.result == ObservationStable && validPreference(o.preference)
}
