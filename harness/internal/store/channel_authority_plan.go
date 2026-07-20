package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const channelAuthorityFingerprintDomain = "mnemon/r5/channel-authority-plan/1"

var (
	ErrChannelAuthorityPlan         = errors.New("invalid Channel authority plan")
	ErrChannelAuthorityPlanDiverged = errors.New("Channel authority diverged from prepared plan")
)

// ChannelAuthorityPlanResolution classifies durable state after an unknown Commit outcome.
type ChannelAuthorityPlanResolution string

const (
	ChannelAuthorityPlanUnchanged ChannelAuthorityPlanResolution = "unchanged"
	ChannelAuthorityPlanCandidate ChannelAuthorityPlanResolution = "candidate"
	ChannelAuthorityPlanDiverged  ChannelAuthorityPlanResolution = "diverged"
)

func (resolution ChannelAuthorityPlanResolution) Valid() bool {
	return resolution == ChannelAuthorityPlanUnchanged ||
		resolution == ChannelAuthorityPlanCandidate ||
		resolution == ChannelAuthorityPlanDiverged
}

// PrepareChannelRosterMerge freezes a coherent preimage and candidate in a rollback-only transaction.
func (s *Store) PrepareChannelRosterMerge(ctx context.Context, spec MergeChannelRosterSpec) (ChannelRosterMergePlan, error) {
	validated, err := validateChannelRosterMergeSpec(s, ctx, spec)
	if err != nil {
		return ChannelRosterMergePlan{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ChannelRosterMergePlan{}, fmt.Errorf("prepare Channel roster merge: begin: %w", err)
	}
	defer tx.Rollback()
	before, err := readChannelAuthorityPlanMesh(ctx, tx)
	if err != nil {
		return ChannelRosterMergePlan{}, err
	}
	node, authority, err := readChannelRosterAuthority(ctx, tx, validated.ChannelID)
	if err != nil {
		return ChannelRosterMergePlan{}, err
	}
	result, conflictID, err := s.applyChannelRosterMerge(ctx, tx, node.PeerID(), authority, validated)
	if err != nil {
		return ChannelRosterMergePlan{}, err
	}
	core, err := finishChannelAuthorityPlan(s, ctx, tx, before)
	if err != nil {
		return ChannelRosterMergePlan{}, err
	}
	replay := result
	if replay.Status == ChannelRosterApplied {
		replay.Status = ChannelRosterDuplicate
	}
	return ChannelRosterMergePlan{channelAuthorityPlan: core, spec: validated, result: replay,
		expectedHead: authority.channel.RosterHead(), conflictID: conflictID}, nil
}

// CommitChannelRosterMerge applies only the preimage; candidate replay succeeds and divergence fails closed.
func (s *Store) CommitChannelRosterMerge(ctx context.Context, plan ChannelRosterMergePlan) (MergeChannelRosterResult, error) {
	if plan.spec.ChannelID.IsZero() {
		return MergeChannelRosterResult{}, ErrChannelAuthorityPlan
	}
	tx, resolution, err := s.beginChannelAuthorityPlan(ctx, plan.channelAuthorityPlan, false)
	if err != nil {
		return MergeChannelRosterResult{}, err
	}
	defer tx.Rollback()
	if resolution == ChannelAuthorityPlanCandidate {
		present, evidenceErr := channelRosterPlanEvidence(ctx, tx, plan)
		if evidenceErr != nil {
			return MergeChannelRosterResult{}, evidenceErr
		}
		if !present {
			return MergeChannelRosterResult{}, ErrChannelAuthorityPlanDiverged
		}
		return plan.result, nil
	}
	if resolution != ChannelAuthorityPlanUnchanged {
		return MergeChannelRosterResult{}, ErrChannelAuthorityPlanDiverged
	}
	node, authority, err := readChannelRosterAuthority(ctx, tx, plan.spec.ChannelID)
	if err != nil || authority.channel.RosterHead() != plan.expectedHead {
		return MergeChannelRosterResult{}, ErrChannelAuthorityPlanDiverged
	}
	result, conflictID, err := s.applyChannelRosterMerge(ctx, tx, node.PeerID(), authority, plan.spec)
	if err != nil {
		return MergeChannelRosterResult{}, err
	}
	expected := plan.result
	if plan.ChangesAuthority() && expected.Status == ChannelRosterDuplicate {
		expected.Status = ChannelRosterApplied
	}
	if conflictID != plan.conflictID || result.Status != expected.Status {
		return MergeChannelRosterResult{}, ErrChannelAuthorityPlanDiverged
	}
	after, err := inspectChannelAuthorityPlan(ctx, tx, plan.channelAuthorityPlan)
	if err != nil {
		return MergeChannelRosterResult{}, err
	}
	if after != ChannelAuthorityPlanCandidate &&
		!(after == ChannelAuthorityPlanUnchanged && !plan.ChangesAuthority()) {
		return MergeChannelRosterResult{}, ErrChannelAuthorityPlanDiverged
	}
	if err := tx.Commit(); err != nil {
		return MergeChannelRosterResult{}, mapChannelRosterError(err)
	}
	return result, nil
}

func channelRosterPlanEvidence(ctx context.Context, tx *sql.Tx, plan ChannelRosterMergePlan) (bool, error) {
	if plan.conflictID == "" {
		return true, nil
	}
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM channel_conflicts
		WHERE conflict_id=? AND channel_id=?`, plan.conflictID, plan.spec.ChannelID.String()).Scan(&count)
	return err == nil && count == 1, err
}

// InboundChannelBaselinePlan freezes a pending binding and its activation candidate.
type InboundChannelBaselinePlan struct {
	channelAuthorityPlan
	spec               InstallInboundChannelBaselineSpec
	result             InstallInboundChannelBaselineResult
	expectedRosterHead model.RecordHead
	expectedBinding    model.PeerBinding
}

func (p InboundChannelBaselinePlan) Result() InstallInboundChannelBaselineResult { return p.result }

// InstallInboundChannelBaseline atomically installs the cursor and activates its pending binding.
func (s *Store) InstallInboundChannelBaseline(ctx context.Context, spec InstallInboundChannelBaselineSpec) (InstallInboundChannelBaselineResult, error) {
	plan, err := s.PrepareInboundChannelBaseline(ctx, spec)
	if err != nil {
		return InstallInboundChannelBaselineResult{}, err
	}
	return s.CommitInboundChannelBaseline(ctx, plan)
}

func (s *Store) PrepareInboundChannelBaseline(ctx context.Context, spec InstallInboundChannelBaselineSpec) (InboundChannelBaselinePlan, error) {
	validated, err := validateInboundChannelBaselineSpec(s, ctx, spec)
	if err != nil {
		return InboundChannelBaselinePlan{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return InboundChannelBaselinePlan{}, fmt.Errorf("prepare inbound Channel baseline: begin: %w", err)
	}
	defer tx.Rollback()
	before, err := readChannelAuthorityPlanMesh(ctx, tx)
	if err != nil {
		return InboundChannelBaselinePlan{}, err
	}
	_, authority, binding, err := readChannelBaselineAuthority(ctx, tx,
		validated.Baseline.ChannelID, validated.Baseline.OriginPeerID)
	if err != nil {
		return InboundChannelBaselinePlan{}, err
	}
	result, err := applyInboundChannelBaseline(ctx, tx, authority, binding, validated)
	if err != nil {
		return InboundChannelBaselinePlan{}, err
	}
	core, err := finishChannelAuthorityPlan(s, ctx, tx, before)
	if err != nil {
		return InboundChannelBaselinePlan{}, err
	}
	replay := result
	replay.Installed = false
	return InboundChannelBaselinePlan{channelAuthorityPlan: core, spec: validated, result: replay,
		expectedRosterHead: authority.channel.RosterHead(), expectedBinding: binding}, nil
}

func (s *Store) ResolveInboundChannelBaseline(ctx context.Context, plan InboundChannelBaselinePlan) (ChannelAuthorityPlanResolution, error) {
	tx, resolution, err := s.beginChannelAuthorityPlan(ctx, plan.channelAuthorityPlan, true)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	if resolution == ChannelAuthorityPlanCandidate && verifyInboundChannelBaselineEvidence(ctx, tx, plan) != nil {
		resolution = ChannelAuthorityPlanDiverged
	}
	return resolution, nil
}

func validateInboundChannelBaselineSpec(s *Store, ctx context.Context, spec InstallInboundChannelBaselineSpec) (InstallInboundChannelBaselineSpec, error) {
	baseline := spec.Baseline
	if s == nil || s.db == nil || ctx == nil || spec.AuthenticatedPeerID.IsZero() ||
		!validChannelDataBaseline(baseline) || spec.AuthenticatedPeerID != baseline.OriginPeerID {
		return InstallInboundChannelBaselineSpec{}, ErrChannelBaselineInput
	}
	at, err := canonicalStoreTime(spec.At)
	if err != nil || at.IsZero() {
		return InstallInboundChannelBaselineSpec{}, ErrChannelBaselineInput
	}
	spec.At = at
	return spec, nil
}

func validChannelDataBaseline(baseline ChannelDataBaseline) bool {
	return !baseline.ChannelID.IsZero() && !baseline.OriginPeerID.IsZero() &&
		!baseline.OriginEpoch.IsZero() && baseline.BaselineChannelSequence <= model.MaxSQLiteInteger
}

// channelAuthorityPlan is embedded only in opaque domain plans and is bound to one Store.
type channelAuthorityPlan struct {
	store                                   *Store
	candidate                               ChannelMeshAuthority
	beforeFingerprint, candidateFingerprint model.Digest
}

func newChannelAuthorityPlan(st *Store, before, candidate ChannelMeshAuthority) (channelAuthorityPlan, error) {
	if st == nil || st.db == nil {
		return channelAuthorityPlan{}, ErrChannelAuthorityPlan
	}
	var fingerprints [2]model.Digest
	for index, authority := range []ChannelMeshAuthority{before, candidate} {
		fingerprint, err := fingerprintChannelMeshAuthority(authority)
		if err != nil {
			return channelAuthorityPlan{}, err
		}
		fingerprints[index] = fingerprint
	}
	return channelAuthorityPlan{store: st, candidate: candidate,
		beforeFingerprint: fingerprints[0], candidateFingerprint: fingerprints[1]}, nil
}
func finishChannelAuthorityPlan(st *Store, ctx context.Context, tx *sql.Tx,
	before ChannelMeshAuthority) (channelAuthorityPlan, error) {
	candidate, err := readChannelAuthorityPlanMesh(ctx, tx)
	if err != nil {
		return channelAuthorityPlan{}, err
	}
	return newChannelAuthorityPlan(st, before, candidate)
}
func (plan channelAuthorityPlan) Candidate() ChannelMeshAuthority { return plan.candidate }
func (plan channelAuthorityPlan) BeforeFingerprint() model.Digest { return plan.beforeFingerprint }
func (p channelAuthorityPlan) CandidateFingerprint() model.Digest { return p.candidateFingerprint }
func (p channelAuthorityPlan) ChangesAuthority() bool {
	return p.beforeFingerprint != p.candidateFingerprint
}
func (plan channelAuthorityPlan) validFor(st *Store) bool {
	return st != nil && st.db != nil && plan.store == st &&
		!plan.beforeFingerprint.IsZero() && !plan.candidateFingerprint.IsZero()
}
func (plan channelAuthorityPlan) classify(current model.Digest) ChannelAuthorityPlanResolution {
	if current == plan.beforeFingerprint {
		return ChannelAuthorityPlanUnchanged
	}
	if current == plan.candidateFingerprint {
		return ChannelAuthorityPlanCandidate
	}
	return ChannelAuthorityPlanDiverged
}

func (s *Store) beginChannelAuthorityPlan(ctx context.Context, plan channelAuthorityPlan,
	readOnly bool) (*sql.Tx, ChannelAuthorityPlanResolution, error) {
	if ctx == nil || !plan.validFor(s) {
		return nil, "", ErrChannelAuthorityPlan
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: readOnly})
	if err != nil {
		return nil, "", fmt.Errorf("begin Channel authority plan: %w", err)
	}
	resolution, err := inspectChannelAuthorityPlan(ctx, tx, plan)
	if err != nil {
		_ = tx.Rollback()
		return nil, "", err
	}
	return tx, resolution, nil
}

func inspectChannelAuthorityPlan(ctx context.Context, tx *sql.Tx, plan channelAuthorityPlan) (ChannelAuthorityPlanResolution, error) {
	current, err := readChannelAuthorityPlanMesh(ctx, tx)
	if err != nil {
		return "", err
	}
	fingerprint, err := fingerprintChannelMeshAuthority(current)
	if err != nil {
		return "", err
	}
	return plan.classify(fingerprint), nil
}

func readChannelAuthorityPlanMesh(ctx context.Context, tx *sql.Tx) (ChannelMeshAuthority, error) {
	node, err := readNode(ctx, tx)
	if err != nil {
		return ChannelMeshAuthority{}, fmt.Errorf("%w: read Node: %v", ErrChannelAuthorityPlan, err)
	}
	channelIDs, err := readChannelMeshIDs(ctx, tx)
	if err != nil || len(channelIDs) > model.MaxChannelsPerNode {
		return ChannelMeshAuthority{}, fmt.Errorf("%w: Channel set: %v", ErrChannelAuthorityPlan, err)
	}
	channels := make([]ChannelMeshChannel, 0, len(channelIDs))
	for _, channelID := range channelIDs {
		verified, readErr := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), channelID)
		if readErr != nil {
			return ChannelMeshAuthority{}, fmt.Errorf("%w: Channel %q: %w",
				ErrChannelAuthorityPlan, channelID.String(), readErr)
		}
		live := make([]model.PeerBinding, 0, len(verified.bindings))
		for _, binding := range verified.bindings {
			if binding.State() == model.BindingPending || binding.State() == model.BindingActive {
				live = append(live, binding)
			} else if binding.State() != model.BindingRevoked {
				return ChannelMeshAuthority{}, fmt.Errorf("%w: unknown binding state", ErrChannelAuthorityPlan)
			}
		}
		channels = append(channels, ChannelMeshChannel{channel: verified.channel,
			roster: verified.roster, bindings: live})
	}
	return ChannelMeshAuthority{localPeerID: node.PeerID(), channels: channels}, nil
}

// fingerprintChannelMeshAuthority streams bounded runtime authority in canonical Store order.
func fingerprintChannelMeshAuthority(authority ChannelMeshAuthority) (model.Digest, error) {
	if authority.LocalPeerID().IsZero() {
		return model.Digest{}, ErrChannelAuthorityPlan
	}
	digest := sha256.New()
	writeAuthorityField(digest, []byte(channelAuthorityFingerprintDomain))
	writeAuthorityField(digest, []byte(authority.LocalPeerID().String()))
	channels := authority.Channels()
	if len(channels) > model.MaxChannelsPerNode {
		return model.Digest{}, ErrChannelAuthorityPlan
	}
	writeAuthorityUint(digest, uint64(len(channels)))
	var previous model.ChannelID
	for _, durable := range channels {
		channel := durable.Channel()
		if channel.ID().IsZero() || (!previous.IsZero() && previous.String() >= channel.ID().String()) {
			return model.Digest{}, ErrChannelAuthorityPlan
		}
		previous = channel.ID()
		writeAuthorityChannel(digest, durable)
	}
	return model.DigestFromBytes(digest.Sum(nil))
}

func writeAuthorityChannel(digest hash.Hash, durable ChannelMeshChannel) {
	channel := durable.Channel()
	writeAuthorityField(digest, channel.Descriptor().WireJSON().Bytes())
	for _, value := range []string{channel.ID().String(), string(channel.Status())} {
		writeAuthorityField(digest, []byte(value))
	}
	writeAuthorityUint(digest, channel.RosterHead().Revision())
	writeAuthorityField(digest, channel.RosterHead().Digest().Bytes())
	members := durable.Roster().Members()
	writeAuthorityUint(digest, uint64(len(members)))
	for _, member := range members {
		writeAuthorityField(digest, member.WireJSON().Bytes())
	}
	bindings := durable.Bindings()
	writeAuthorityUint(digest, uint64(len(bindings)))
	for _, binding := range bindings {
		writeAuthorityBinding(digest, binding)
	}
}

func writeAuthorityBinding(digest hash.Hash, binding model.PeerBinding) {
	for _, value := range []string{binding.PeerID().String(), binding.OriginEpoch().String(),
		string(binding.State())} {
		writeAuthorityField(digest, []byte(value))
	}
	writeAuthorityField(digest, binding.PublicKey())
	for _, values := range [][]string{binding.Multiaddrs(), binding.Protocols()} {
		writeAuthorityUint(digest, uint64(len(values)))
		for _, value := range values {
			writeAuthorityField(digest, []byte(value))
		}
	}
	writeAuthorityField(digest, binding.Limits().Bytes())
	for _, head := range []model.RecordHead{binding.MemberHead(), binding.RosterHead()} {
		writeAuthorityUint(digest, head.Revision())
		writeAuthorityField(digest, head.Digest().Bytes())
	}
}

func writeAuthorityUint(digest hash.Hash, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = digest.Write(encoded[:])
}

func writeAuthorityField(digest hash.Hash, value []byte) {
	writeAuthorityUint(digest, uint64(len(value)))
	_, _ = digest.Write(value)
}
