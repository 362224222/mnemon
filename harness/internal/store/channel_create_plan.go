package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// CreateChannelPlan is an opaque, Store-bound Channel authority candidate.
// It deliberately retains only bearer-secret-free durable values.
type CreateChannelPlan struct {
	channelAuthorityPlan
	candidate createChannelCandidate
	result    CreateChannelResult
}

type createChannelCandidate struct {
	channel model.Channel
	genesis model.Member
	grant   model.OpenEnrollmentGrant
}

func (plan CreateChannelPlan) Result() CreateChannelResult { return plan.result }

// PrepareCreateChannel validates transient token authority and freezes the
// exact durable candidate in a rollback-only transaction.
func (s *Store) PrepareCreateChannel(ctx context.Context,
	spec CreateChannelSpec,
) (CreateChannelPlan, error) {
	candidate, err := validateCreateChannelSpec(s, ctx, spec)
	if err != nil {
		return CreateChannelPlan{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CreateChannelPlan{}, fmt.Errorf("prepare create Channel: begin: %w", err)
	}
	defer tx.Rollback()
	before, err := readChannelAuthorityPlanMesh(ctx, tx)
	if err != nil {
		return CreateChannelPlan{}, err
	}
	node, err := readNode(ctx, tx)
	if err != nil {
		return CreateChannelPlan{}, fmt.Errorf("prepare create Channel: Node: %w", err)
	}
	result, err := applyCreateChannel(ctx, tx, node, candidate)
	if err != nil {
		return CreateChannelPlan{}, err
	}
	core, err := finishChannelAuthorityPlan(s, ctx, tx, before)
	if err != nil {
		return CreateChannelPlan{}, err
	}
	result.Created = false
	return CreateChannelPlan{channelAuthorityPlan: core, candidate: candidate, result: result}, nil
}

// CommitCreateChannel applies only the prepared preimage. An exact committed
// candidate is a successful replay; every other durable state fails closed.
func (s *Store) CommitCreateChannel(ctx context.Context,
	plan CreateChannelPlan,
) (CreateChannelResult, error) {
	if !plan.validFor(s) {
		return CreateChannelResult{}, ErrChannelAuthorityPlan
	}
	tx, resolution, err := s.beginChannelAuthorityPlan(ctx, plan.channelAuthorityPlan, false)
	if err != nil {
		return CreateChannelResult{}, err
	}
	defer tx.Rollback()
	committed, evidenceErr := createChannelPlanEvidence(ctx, tx, plan)
	if evidenceErr != nil {
		return CreateChannelResult{}, evidenceErr
	}
	if committed && createChannelPlanMatchesRuntime(resolution, plan) {
		return plan.result, nil
	}
	if !plan.ChangesAuthority() && resolution == ChannelAuthorityPlanUnchanged {
		return CreateChannelResult{}, ErrChannelAuthorityPlanDiverged
	}
	if resolution != ChannelAuthorityPlanUnchanged {
		return CreateChannelResult{}, ErrChannelAuthorityPlanDiverged
	}
	node, err := readNode(ctx, tx)
	if err != nil {
		return CreateChannelResult{}, ErrChannelAuthorityPlanDiverged
	}
	result, err := applyCreateChannel(ctx, tx, node, plan.candidate)
	if err != nil {
		return CreateChannelResult{}, err
	}
	after, err := inspectChannelAuthorityPlan(ctx, tx, plan.channelAuthorityPlan)
	if err != nil {
		return CreateChannelResult{}, err
	}
	if after != ChannelAuthorityPlanCandidate &&
		!(after == ChannelAuthorityPlanUnchanged && !plan.ChangesAuthority()) {
		return CreateChannelResult{}, ErrChannelAuthorityPlanDiverged
	}
	if committed, evidenceErr := createChannelPlanEvidence(ctx, tx, plan); evidenceErr != nil {
		return CreateChannelResult{}, evidenceErr
	} else if !committed {
		return CreateChannelResult{}, ErrChannelAuthorityPlanDiverged
	}
	if err := tx.Commit(); err != nil {
		return CreateChannelResult{}, mapChannelCreateError(err)
	}
	return result, nil
}

// ResolveCreateChannel classifies a response-loss outcome from exact durable
// create evidence without authorizing an obsolete runtime snapshot. Later
// progress remains replayable only through a newly prepared no-op plan.
func (s *Store) ResolveCreateChannel(ctx context.Context,
	plan CreateChannelPlan,
) (ChannelAuthorityPlanResolution, error) {
	if !plan.validFor(s) {
		return "", ErrChannelAuthorityPlan
	}
	tx, resolution, err := s.beginChannelAuthorityPlan(ctx, plan.channelAuthorityPlan, true)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	committed, err := createChannelPlanEvidence(ctx, tx, plan)
	if err != nil {
		return "", err
	}
	if committed && createChannelPlanMatchesRuntime(resolution, plan) {
		return ChannelAuthorityPlanCandidate, nil
	}
	if !plan.ChangesAuthority() && resolution == ChannelAuthorityPlanUnchanged {
		return ChannelAuthorityPlanDiverged, nil
	}
	if resolution == ChannelAuthorityPlanUnchanged {
		return ChannelAuthorityPlanUnchanged, nil
	}
	return ChannelAuthorityPlanDiverged, nil
}

func createChannelPlanMatchesRuntime(resolution ChannelAuthorityPlanResolution,
	plan CreateChannelPlan,
) bool {
	return resolution == ChannelAuthorityPlanCandidate ||
		(!plan.ChangesAuthority() && resolution == ChannelAuthorityPlanUnchanged)
}

func validateCreateChannelSpec(s *Store, ctx context.Context,
	spec CreateChannelSpec,
) (createChannelCandidate, error) {
	if s == nil || s.db == nil || ctx == nil || spec.Genesis.IsZero() || spec.Token.IsZero() {
		return createChannelCandidate{}, ErrChannelCreateInput
	}
	channel, err := cloneCreateChannel(spec.Channel)
	if err != nil {
		return createChannelCandidate{}, ErrChannelCreateInput
	}
	genesis, err := model.ParseMember(spec.Genesis.WireJSON().Bytes())
	if err != nil {
		return createChannelCandidate{}, ErrChannelCreateInput
	}
	grant, grantErr := model.NewOpenEnrollmentGrantForToken(spec.Token, channel.CreatedAt())
	genesisAddresses, genesisAddressErr := model.AdvertisedAddressDigest(genesis.Multiaddrs())
	tokenAddresses, tokenAddressErr := model.AdvertisedAddressDigest(spec.Token.Payload().OwnerMultiaddrs())
	if !validCreateChannelProjection(channel) || grantErr != nil ||
		!validCreateGrantProjection(channel, grant, spec.Token, genesisAddresses,
			genesisAddressErr, tokenAddresses, tokenAddressErr) {
		return createChannelCandidate{}, fmt.Errorf("%w: inconsistent Channel, topic or grant",
			ErrChannelCreateInput)
	}
	roster, err := model.NewVerifiedRoster(channel.Descriptor(), []model.Member{genesis})
	if err != nil || roster.Head() != channel.RosterHead() {
		return createChannelCandidate{}, fmt.Errorf("%w: genesis authority: %v", ErrChannelCreateInput, err)
	}
	return createChannelCandidate{channel: channel, genesis: genesis, grant: grant}, nil
}

func validCreateChannelProjection(channel model.Channel) bool {
	return !channel.ID().IsZero() && channel.Status() == model.ChannelActive &&
		channel.TopicState() == model.TopicNotJoined && channel.UpdatedAt() == channel.CreatedAt()
}

func validCreateGrantProjection(channel model.Channel, grant model.OpenEnrollmentGrant,
	token model.EnrollmentToken, genesisAddresses model.Digest, genesisAddressErr error,
	tokenAddresses model.Digest, tokenAddressErr error,
) bool {
	return grant.ChannelID() == channel.ID() &&
		bytes.Equal(token.Payload().Descriptor().WireJSON().Bytes(), channel.Descriptor().WireJSON().Bytes()) &&
		genesisAddressErr == nil && tokenAddressErr == nil && genesisAddresses == tokenAddresses &&
		grant.CreatedAt() == channel.CreatedAt() && grant.MaxUses() == channel.MemberLimit()-1
}

func cloneCreateChannel(channel model.Channel) (model.Channel, error) {
	if channel.ID().IsZero() {
		return model.Channel{}, ErrChannelCreateInput
	}
	descriptor, err := model.ParseSignedChannelDescriptor(channel.Descriptor().WireJSON().Bytes())
	if err != nil {
		return model.Channel{}, err
	}
	return model.NewChannel(model.ChannelSpec{Descriptor: descriptor, LocalAlias: channel.LocalAlias(),
		RosterHead: channel.RosterHead(), Status: channel.Status(), TopicState: channel.TopicState(),
		UpdatedAt: channel.UpdatedAt()})
}

func createChannelPlanEvidence(ctx context.Context, tx *sql.Tx,
	plan CreateChannelPlan,
) (bool, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM channels WHERE channel_id=?`,
		plan.candidate.channel.ID().String()).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("inspect create Channel evidence: %w", err)
	}
	if count == 0 {
		return false, nil
	}
	result, err := replayCreateChannel(ctx, tx, plan.candidate.genesis.PeerID(), plan.candidate)
	if err != nil {
		if errors.Is(err, ErrChannelCreateConflict) {
			return false, nil
		}
		return false, err
	}
	if result.GrantID != plan.result.GrantID || verifyCreatePublicationEpoch(ctx, tx,
		plan.candidate) != nil {
		return false, nil
	}
	return true, nil
}

func verifyCreatePublicationEpoch(ctx context.Context, tx *sql.Tx,
	candidate createChannelCandidate,
) error {
	var peerText, epochText, updatedText string
	var floor, head uint64
	err := tx.QueryRowContext(ctx, `SELECT origin_peer_id,origin_epoch,source_floor_channel_seq,
		source_head_channel_seq,updated_at FROM publication_epochs WHERE channel_id=?`,
		candidate.channel.ID().String()).Scan(&peerText, &epochText, &floor, &head, &updatedText)
	updatedAt, timeErr := parseCanonicalStoreTime(updatedText)
	if err != nil || timeErr != nil || peerText != candidate.genesis.PeerID().String() ||
		epochText != candidate.genesis.OriginEpoch().String() || floor == 0 ||
		floor > model.MaxSQLiteInteger || head > model.MaxSQLiteInteger || floor > head+1 ||
		updatedAt.Before(candidate.channel.CreatedAt()) {
		return ErrChannelCreateConflict
	}
	return nil
}
