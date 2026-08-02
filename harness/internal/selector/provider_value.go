package selector

import (
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

const (
	// MinEligiblePeersPerSample makes activation explicitly require k to be
	// much smaller than the authenticated peer universe. The selector does not
	// silently degrade to querying every peer in a small roster.
	MinEligiblePeersPerSample = 4
	MaxActiveSelections       = 32
	// MaxStoredSelections bounds the complete private selector database. When
	// the bound is reached, the oldest observed selections are removed before
	// a new owner-created selection is admitted. Active selections are never
	// evicted.
	MaxStoredSelections       = 64
	MaxPendingRounds          = 8
	MaxSelectionQueryMessages = 4096
	MaxDescriptorBytes        = 64 << 10
)

// MaxStoredRoundSettlements is a deliberately loose store-wide bound. A
// selection can settle at most max_rounds times, and activation already caps
// sample_size*max_rounds at MaxSelectionQueryMessages.
const MaxStoredRoundSettlements = MaxStoredSelections * MaxSelectionQueryMessages

var (
	ErrClosed        = errors.New("selector store is closed")
	ErrNotFound      = errors.New("selector selection not found")
	ErrConflict      = errors.New("selector durable state conflict")
	ErrNotActive     = errors.New("selector selection is not active")
	ErrActivation    = errors.New("selector activation precondition failed")
	ErrStoreCapacity = errors.New("selector store capacity reached")
)

type SelectionPhase string

const (
	PhaseAwaitingSeed SelectionPhase = "awaiting_seed"
	PhaseActive       SelectionPhase = "active"
	PhaseObserved     SelectionPhase = "observed"
)

// AcceptedSeedOpinion is an independently admitted local Principal opinion.
// The caller must construct it only after verifying that Event is an accepted
// R7 Event authored by Principal. The selector deliberately cannot read or
// mutate the R7 authority store, so it treats that verified pair as opaque
// provenance and never promotes the opinion into an R7 fact.
type AcceptedSeedOpinion struct {
	principal  agency.AgentPrincipalID
	event      agency.EventRef
	preference Preference
}

func NewAcceptedSeedOpinion(principal agency.AgentPrincipalID, event agency.EventRef,
	preference Preference,
) (AcceptedSeedOpinion, error) {
	if principal.IsZero() || event.IsZero() || !validPreference(preference) {
		return AcceptedSeedOpinion{}, fmt.Errorf("seed opinion fields are incomplete: %w", ErrInvalid)
	}
	return AcceptedSeedOpinion{principal: principal, event: event, preference: preference}, nil
}

func (s AcceptedSeedOpinion) Principal() agency.AgentPrincipalID { return s.principal }
func (s AcceptedSeedOpinion) Event() agency.EventRef             { return s.event }
func (s AcceptedSeedOpinion) Preference() Preference             { return s.preference }

func (s AcceptedSeedOpinion) valid() bool {
	return !s.principal.IsZero() && !s.event.IsZero() && validPreference(s.preference)
}

// PendingRound is a durable prepare result. A network adapter may query only
// Sample using Query, then submit its bounded replies to ApplyObservations.
// The adapter must not derive a new sample or nonce on retry.
type PendingRound struct {
	query         SampleQuery
	sample        []ParticipantID
	deadline      time.Time
	stateRevision uint64
}

func (r PendingRound) Query() SampleQuery      { return r.query }
func (r PendingRound) Sample() []ParticipantID { return append([]ParticipantID(nil), r.sample...) }
func (r PendingRound) Deadline() time.Time     { return r.deadline }
func (r PendingRound) StateRevision() uint64   { return r.stateRevision }
func (r PendingRound) valid() bool {
	return !r.query.selectionID.IsZero() && r.query.round > 0 && !r.query.nonce.IsZero() &&
		len(r.sample) > 0 && !r.deadline.IsZero() && r.stateRevision > 0
}

// SelectionSnapshot is an immutable provider-local projection. Observation is
// an observational value only; callers may store its bytes as an Artifact or
// submit a separate Agent Intent, but this package never does either.
type SelectionSnapshot struct {
	descriptor  SelectionDescriptor
	self        ParticipantID
	phase       SelectionPhase
	seed        AcceptedSeedOpinion
	state       SelectionState
	pending     PendingRound
	observation PreferenceObservation
	revision    uint64
}

func (s SelectionSnapshot) Descriptor() SelectionDescriptor { return s.descriptor }
func (s SelectionSnapshot) Self() ParticipantID             { return s.self }
func (s SelectionSnapshot) Phase() SelectionPhase           { return s.phase }
func (s SelectionSnapshot) Revision() uint64                { return s.revision }
func (s SelectionSnapshot) Seed() (AcceptedSeedOpinion, bool) {
	return s.seed, s.seed.valid()
}
func (s SelectionSnapshot) State() (SelectionState, bool) {
	return s.state, !s.state.selectionID.IsZero()
}
func (s SelectionSnapshot) PendingRound() (PendingRound, bool) {
	return s.pending, s.pending.valid()
}
func (s SelectionSnapshot) Observation() (PreferenceObservation, bool) {
	return s.observation, len(s.observation.canonical) > 0
}

func validateProviderActivation(descriptor SelectionDescriptor, self ParticipantID) error {
	if err := descriptor.validate(); err != nil {
		return err
	}
	if len(descriptor.canonical) > MaxDescriptorBytes {
		return fmt.Errorf("descriptor bytes %d exceed %d: %w",
			len(descriptor.canonical), MaxDescriptorBytes, ErrLimit)
	}
	if self.IsZero() || !descriptor.contains(self) {
		return fmt.Errorf("local participant is outside authenticated roster: %w", ErrActivation)
	}
	eligible := len(descriptor.roster) - 1
	required := int(descriptor.profile.sampleSize) * MinEligiblePeersPerSample
	if eligible < required {
		return fmt.Errorf("eligible peers %d are below fixed activation bound %d*k=%d: %w",
			eligible, MinEligiblePeersPerSample, required, ErrActivation)
	}
	messageBudget := uint64(descriptor.profile.sampleSize) * uint64(descriptor.profile.maxRounds)
	if messageBudget > MaxSelectionQueryMessages {
		return fmt.Errorf("query message budget %d exceeds %d: %w",
			messageBudget, MaxSelectionQueryMessages, ErrActivation)
	}
	return nil
}
