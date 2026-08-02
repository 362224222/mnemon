package selector

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	DescriptorVersion = 1

	MaxRosterPeers         = 256
	MaxSampleSize          = 128
	MaxRounds              = 10_000
	MinRoundTimeout        = time.Millisecond
	MaxRoundTimeout        = time.Minute
	MaxRoundBudgetDuration = 24 * time.Hour
)

var (
	ErrInvalid = errors.New("invalid selector value")
	ErrLimit   = errors.New("selector limit exceeded")
	ErrState   = errors.New("invalid selector state")
)

// Preference is the complete R8 choice set. Zero is invalid.
type Preference uint8

const (
	PreferenceA Preference = iota + 1
	PreferenceB
)

func (p Preference) String() string {
	switch p {
	case PreferenceA:
		return "A"
	case PreferenceB:
		return "B"
	default:
		return ""
	}
}

func ParsePreference(value string) (Preference, error) {
	switch value {
	case "A":
		return PreferenceA, nil
	case "B":
		return PreferenceB, nil
	default:
		return 0, fmt.Errorf("preference %q: %w", value, ErrInvalid)
	}
}

func validPreference(value Preference) bool {
	return value == PreferenceA || value == PreferenceB
}

// Profile freezes every machine-controlled bound used by one selection.
type Profile struct {
	sampleSize   uint32
	alpha        uint32
	threshold    uint32
	maxRounds    uint32
	roundTimeout time.Duration
}

func NewProfile(sampleSize, alpha, threshold, maxRounds uint32, roundTimeout time.Duration) (Profile, error) {
	profile := Profile{sampleSize, alpha, threshold, maxRounds, roundTimeout}
	if err := profile.validate(); err != nil {
		return Profile{}, err
	}
	return profile, nil
}

func (p Profile) SampleSize() uint32          { return p.sampleSize }
func (p Profile) Alpha() uint32               { return p.alpha }
func (p Profile) Threshold() uint32           { return p.threshold }
func (p Profile) MaxRounds() uint32           { return p.maxRounds }
func (p Profile) RoundTimeout() time.Duration { return p.roundTimeout }

func (p Profile) validate() error {
	if p.sampleSize == 0 || p.sampleSize > MaxSampleSize {
		return fmt.Errorf("sample size %d (want 1..%d): %w", p.sampleSize, MaxSampleSize, ErrLimit)
	}
	if p.alpha <= p.sampleSize/2 || p.alpha > p.sampleSize {
		return fmt.Errorf("alpha %d is not a strict sample majority: %w", p.alpha, ErrInvalid)
	}
	if p.maxRounds == 0 || p.maxRounds > MaxRounds {
		return fmt.Errorf("max rounds %d (want 1..%d): %w", p.maxRounds, MaxRounds, ErrLimit)
	}
	if p.threshold == 0 || p.threshold > p.maxRounds {
		return fmt.Errorf("threshold %d (want 1..max rounds): %w", p.threshold, ErrInvalid)
	}
	if p.roundTimeout < MinRoundTimeout || p.roundTimeout > MaxRoundTimeout ||
		p.roundTimeout%time.Millisecond != 0 {
		return fmt.Errorf("round timeout %s is outside the bounded millisecond profile: %w",
			p.roundTimeout, ErrLimit)
	}
	if time.Duration(p.maxRounds) > MaxRoundBudgetDuration/p.roundTimeout {
		return fmt.Errorf("max rounds times timeout exceeds %s: %w", MaxRoundBudgetDuration, ErrLimit)
	}
	return nil
}

type profileWire struct {
	Alpha              uint32 `json:"alpha"`
	MaxRounds          uint32 `json:"max_rounds"`
	RoundTimeoutMillis int64  `json:"round_timeout_ms"`
	SampleSize         uint32 `json:"sample_size"`
	Threshold          uint32 `json:"threshold"`
}

func (p Profile) wire() profileWire {
	return profileWire{p.alpha, p.maxRounds, p.roundTimeout.Milliseconds(), p.sampleSize, p.threshold}
}

func (p Profile) Digest() model.Digest {
	canonical, err := model.CanonicalMarshal(p.wire())
	if err != nil {
		return model.Digest{}
	}
	return model.Sum(canonical)
}

// SelectionID is the digest of exact canonical SelectionDescriptor bytes.
type SelectionID struct{ digest model.Digest }

func ParseSelectionID(value string) (SelectionID, error) {
	digest, err := model.ParseDigest(value)
	if err != nil {
		return SelectionID{}, fmt.Errorf("selection ID: %w", err)
	}
	return SelectionID{digest}, nil
}

func (id SelectionID) Digest() model.Digest { return id.digest }
func (id SelectionID) IsZero() bool         { return id.digest.IsZero() }
func (id SelectionID) String() string       { return id.digest.String() }

func (id SelectionID) MarshalJSON() ([]byte, error) {
	if id.IsZero() {
		return nil, fmt.Errorf("selection ID: %w", ErrInvalid)
	}
	return json.Marshal(id.String())
}

// SelectionDescriptor is an immutable, canonical binary selection scope.
type SelectionDescriptor struct {
	question   model.Digest
	candidateA model.Digest
	candidateB model.Digest
	roster     []model.PeerID
	profile    Profile
	expiresAt  time.Time
	canonical  []byte
	id         SelectionID
	rosterHash model.Digest
}

type descriptorWire struct {
	CandidateAArtifactDigest string      `json:"candidate_a_artifact_digest"`
	CandidateBArtifactDigest string      `json:"candidate_b_artifact_digest"`
	ExpiresAt                string      `json:"expires_at"`
	MachineProfile           profileWire `json:"machine_profile"`
	ParticipantRoster        []string    `json:"participant_roster"`
	QuestionArtifactDigest   string      `json:"question_artifact_digest"`
	Version                  uint32      `json:"version"`
}

func NewSelectionDescriptor(question, candidateA, candidateB model.Digest, roster []model.PeerID,
	profile Profile, expiresAt time.Time,
) (SelectionDescriptor, error) {
	if question.IsZero() || candidateA.IsZero() || candidateB.IsZero() {
		return SelectionDescriptor{}, fmt.Errorf("question and candidate digests are required: %w", ErrInvalid)
	}
	if candidateA == candidateB {
		return SelectionDescriptor{}, fmt.Errorf("candidate digests must differ: %w", ErrInvalid)
	}
	if err := profile.validate(); err != nil {
		return SelectionDescriptor{}, err
	}
	canonicalRoster, rosterWire, err := normalizeRoster(roster, profile.sampleSize)
	if err != nil {
		return SelectionDescriptor{}, err
	}
	canonicalExpiry, err := normalizeExpiry(expiresAt)
	if err != nil {
		return SelectionDescriptor{}, err
	}
	wire := descriptorWire{
		CandidateAArtifactDigest: candidateA.String(), CandidateBArtifactDigest: candidateB.String(),
		ExpiresAt: canonicalExpiry.Format(time.RFC3339Nano), MachineProfile: profile.wire(),
		ParticipantRoster: rosterWire, QuestionArtifactDigest: question.String(), Version: DescriptorVersion,
	}
	canonical, err := model.CanonicalMarshal(wire)
	if err != nil {
		return SelectionDescriptor{}, fmt.Errorf("canonicalize selection descriptor: %w", err)
	}
	rosterCanonical, err := model.CanonicalMarshal(rosterWire)
	if err != nil {
		return SelectionDescriptor{}, fmt.Errorf("canonicalize selection roster: %w", err)
	}
	return SelectionDescriptor{
		question: question, candidateA: candidateA, candidateB: candidateB,
		roster: canonicalRoster, profile: profile, expiresAt: canonicalExpiry,
		canonical: canonical, id: SelectionID{model.Sum(canonical)}, rosterHash: model.Sum(rosterCanonical),
	}, nil
}

func normalizeRoster(roster []model.PeerID, sampleSize uint32) ([]model.PeerID, []string, error) {
	if len(roster) == 0 || len(roster) > MaxRosterPeers {
		return nil, nil, fmt.Errorf("roster size %d (want 1..%d): %w", len(roster), MaxRosterPeers, ErrLimit)
	}
	if len(roster) <= int(sampleSize) {
		return nil, nil, fmt.Errorf("roster size %d must exceed sample size %d: %w",
			len(roster), sampleSize, ErrInvalid)
	}
	result := append([]model.PeerID(nil), roster...)
	for _, peer := range result {
		if peer.IsZero() {
			return nil, nil, fmt.Errorf("roster contains zero peer: %w", ErrInvalid)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].String() < result[j].String() })
	wire := make([]string, len(result))
	for index, peer := range result {
		wire[index] = peer.String()
		if index > 0 && wire[index-1] == wire[index] {
			return nil, nil, fmt.Errorf("roster contains duplicate peer %q: %w", wire[index], ErrInvalid)
		}
	}
	return result, wire, nil
}

func normalizeExpiry(value time.Time) (time.Time, error) {
	if value.IsZero() {
		return time.Time{}, fmt.Errorf("expiry is required: %w", ErrInvalid)
	}
	canonical := value.Round(0).UTC()
	wire := canonical.Format(time.RFC3339Nano)
	parsed, err := time.Parse(time.RFC3339Nano, wire)
	if err != nil || !parsed.Equal(canonical) {
		return time.Time{}, fmt.Errorf("expiry is not canonical RFC3339Nano: %w", ErrInvalid)
	}
	return canonical, nil
}

func (d SelectionDescriptor) ID() SelectionID                { return d.id }
func (d SelectionDescriptor) QuestionDigest() model.Digest   { return d.question }
func (d SelectionDescriptor) CandidateADigest() model.Digest { return d.candidateA }
func (d SelectionDescriptor) CandidateBDigest() model.Digest { return d.candidateB }
func (d SelectionDescriptor) Profile() Profile               { return d.profile }
func (d SelectionDescriptor) ExpiresAt() time.Time           { return d.expiresAt }
func (d SelectionDescriptor) RosterDigest() model.Digest     { return d.rosterHash }
func (d SelectionDescriptor) CanonicalBytes() []byte         { return append([]byte(nil), d.canonical...) }
func (d SelectionDescriptor) ParticipantRoster() []model.PeerID {
	return append([]model.PeerID(nil), d.roster...)
}

func (d SelectionDescriptor) contains(peer model.PeerID) bool {
	index := sort.Search(len(d.roster), func(index int) bool {
		return d.roster[index].String() >= peer.String()
	})
	return index < len(d.roster) && d.roster[index] == peer
}

func (d SelectionDescriptor) validate() error {
	if d.id.IsZero() || len(d.canonical) == 0 || d.rosterHash.IsZero() || len(d.roster) == 0 {
		return fmt.Errorf("zero selection descriptor: %w", ErrInvalid)
	}
	return d.profile.validate()
}
