package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrChannelRuntimeInput     = errors.New("invalid Channel runtime projection input")
	ErrChannelRuntimeAuthority = errors.New("Channel runtime authority is unavailable")
	ErrChannelRuntimeConflict  = errors.New("Channel runtime projection changed")
)

// ChannelTopicProjection is the durable half of one runtime topic session.
// A joined value is meaningful only while the current daemon also owns a live
// session for this exact roster head.
type ChannelTopicProjection struct {
	ChannelID  model.ChannelID
	Status     model.ChannelStatus
	RosterHead model.RecordHead
	TopicState model.TopicState
	UpdatedAt  time.Time
}

type BeginChannelTopicRuntimeResult struct {
	Topics     []ChannelTopicProjection
	Downgraded uint8
}

type CompareAndSetChannelTopicStateSpec struct {
	ChannelID          model.ChannelID
	ExpectedStatus     model.ChannelStatus
	ExpectedRosterHead model.RecordHead
	ExpectedTopicState model.TopicState
	TopicState         model.TopicState
	At                 time.Time
}

type CompareAndSetChannelTopicStateResult struct {
	Topic   ChannelTopicProjection
	Changed bool
}

// BeginChannelTopicRuntime invalidates every joined marker from a previous
// process in one transaction. Repeating startup after the downgrade is a
// read-only success and therefore does not drift Channel timestamps.
func (s *Store) BeginChannelTopicRuntime(ctx context.Context,
	at time.Time,
) (BeginChannelTopicRuntimeResult, error) {
	if s == nil || s.db == nil || ctx == nil {
		return BeginChannelTopicRuntimeResult{}, ErrChannelRuntimeInput
	}
	canonicalAt, err := canonicalStoreTime(at)
	if err != nil || canonicalAt.IsZero() {
		return BeginChannelTopicRuntimeResult{}, ErrChannelRuntimeInput
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return BeginChannelTopicRuntimeResult{}, fmt.Errorf("begin Channel topic runtime: begin: %w", err)
	}
	defer tx.Rollback()
	node, err := readNode(ctx, tx)
	if err != nil {
		return BeginChannelTopicRuntimeResult{}, fmt.Errorf("%w: Node: %v", ErrChannelRuntimeAuthority, err)
	}
	channelIDs, err := readChannelMeshIDs(ctx, tx)
	if err != nil {
		return BeginChannelTopicRuntimeResult{}, fmt.Errorf("%w: %v", ErrChannelRuntimeAuthority, err)
	}
	topics := make([]ChannelTopicProjection, 0, len(channelIDs))
	var downgraded uint8
	for _, channelID := range channelIDs {
		authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), channelID)
		if err != nil {
			return BeginChannelTopicRuntimeResult{}, fmt.Errorf("%w: Channel %q: %v",
				ErrChannelRuntimeAuthority, channelID.String(), err)
		}
		channel := authority.channel
		projection := channelTopicProjection(channel)
		if channel.Status() == model.ChannelActive && channel.TopicState() == model.TopicJoined {
			if canonicalAt.Before(channel.UpdatedAt()) {
				return BeginChannelTopicRuntimeResult{}, ErrChannelRuntimeConflict
			}
			mutation, err := tx.ExecContext(ctx, `UPDATE channels SET topic_state=?,updated_at=?
				WHERE channel_id=? AND status=? AND roster_head_revision=? AND roster_head_hash=?
				AND topic_state=? AND updated_at=?`, string(model.TopicJoining), storeTime(canonicalAt),
				channel.ID().String(), string(model.ChannelActive), channel.RosterHead().Revision(),
				channel.RosterHead().Digest().Bytes(), string(model.TopicJoined), storeTime(channel.UpdatedAt()))
			if err != nil || exactlyOne(mutation) != nil {
				if err == nil {
					err = errors.New("joined Channel changed during startup")
				}
				return BeginChannelTopicRuntimeResult{}, fmt.Errorf("%w: reset Channel %q: %v",
					ErrChannelRuntimeConflict, channel.ID().String(), err)
			}
			projection.TopicState = model.TopicJoining
			projection.UpdatedAt = canonicalAt
			downgraded++
		}
		topics = append(topics, projection)
	}
	if err := tx.Commit(); err != nil {
		return BeginChannelTopicRuntimeResult{}, fmt.Errorf("begin Channel topic runtime: commit: %w", err)
	}
	return BeginChannelTopicRuntimeResult{Topics: topics, Downgraded: downgraded}, nil
}

// CompareAndSetChannelTopicState commits a network result only against the
// status, signed roster generation and topic phase observed by its caller.
// Exact result replay succeeds without another write.
func (s *Store) CompareAndSetChannelTopicState(ctx context.Context,
	spec CompareAndSetChannelTopicStateSpec,
) (CompareAndSetChannelTopicStateResult, error) {
	if s == nil || s.db == nil || ctx == nil || spec.ChannelID.IsZero() ||
		!spec.ExpectedStatus.Valid() || spec.ExpectedRosterHead.IsZero() ||
		!runtimeTopicState(spec.ExpectedTopicState) || !runtimeTopicState(spec.TopicState) {
		return CompareAndSetChannelTopicStateResult{}, ErrChannelRuntimeInput
	}
	at, err := canonicalStoreTime(spec.At)
	if err != nil || at.IsZero() {
		return CompareAndSetChannelTopicStateResult{}, ErrChannelRuntimeInput
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CompareAndSetChannelTopicStateResult{}, fmt.Errorf("set Channel topic state: begin: %w", err)
	}
	defer tx.Rollback()
	node, err := readNode(ctx, tx)
	if err != nil {
		return CompareAndSetChannelTopicStateResult{}, fmt.Errorf("%w: Node: %v", ErrChannelRuntimeAuthority, err)
	}
	authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), spec.ChannelID)
	if err != nil {
		return CompareAndSetChannelTopicStateResult{}, fmt.Errorf("%w: %v", ErrChannelRuntimeAuthority, err)
	}
	channel := authority.channel
	if channel.Status() != model.ChannelActive {
		return CompareAndSetChannelTopicStateResult{}, ErrChannelRuntimeAuthority
	}
	if channel.Status() != spec.ExpectedStatus || channel.RosterHead() != spec.ExpectedRosterHead {
		return CompareAndSetChannelTopicStateResult{}, ErrChannelRuntimeConflict
	}
	if spec.ExpectedTopicState != spec.TopicState &&
		!validRuntimeTopicTransition(spec.ExpectedTopicState, spec.TopicState) {
		return CompareAndSetChannelTopicStateResult{}, ErrChannelRuntimeConflict
	}
	if channel.TopicState() == spec.TopicState {
		if err := tx.Commit(); err != nil {
			return CompareAndSetChannelTopicStateResult{}, fmt.Errorf("set Channel topic state: commit replay: %w", err)
		}
		return CompareAndSetChannelTopicStateResult{Topic: channelTopicProjection(channel)}, nil
	}
	if channel.TopicState() != spec.ExpectedTopicState ||
		!validRuntimeTopicTransition(channel.TopicState(), spec.TopicState) || at.Before(channel.UpdatedAt()) {
		return CompareAndSetChannelTopicStateResult{}, ErrChannelRuntimeConflict
	}
	mutation, err := tx.ExecContext(ctx, `UPDATE channels SET topic_state=?,updated_at=?
		WHERE channel_id=? AND status=? AND roster_head_revision=? AND roster_head_hash=?
		AND topic_state=? AND updated_at=?`, string(spec.TopicState), storeTime(at), channel.ID().String(),
		string(spec.ExpectedStatus), spec.ExpectedRosterHead.Revision(),
		spec.ExpectedRosterHead.Digest().Bytes(), string(spec.ExpectedTopicState), storeTime(channel.UpdatedAt()))
	if err != nil || exactlyOne(mutation) != nil {
		if err == nil {
			err = errors.New("Channel topic authority changed during transition")
		}
		return CompareAndSetChannelTopicStateResult{}, fmt.Errorf("%w: %v", ErrChannelRuntimeConflict, err)
	}
	if err := tx.Commit(); err != nil {
		return CompareAndSetChannelTopicStateResult{}, fmt.Errorf("set Channel topic state: commit: %w", err)
	}
	projection := channelTopicProjection(channel)
	projection.TopicState, projection.UpdatedAt = spec.TopicState, at
	return CompareAndSetChannelTopicStateResult{Topic: projection, Changed: true}, nil
}

// ReadChannelTopicRuntime reconstructs all topic projections from signed
// authority in one coherent read snapshot.
func (s *Store) ReadChannelTopicRuntime(ctx context.Context) ([]ChannelTopicProjection, error) {
	if s == nil || s.db == nil || ctx == nil {
		return nil, ErrChannelRuntimeInput
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("read Channel topic runtime: begin: %w", err)
	}
	defer tx.Rollback()
	node, err := readNode(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("%w: Node: %v", ErrChannelRuntimeAuthority, err)
	}
	channelIDs, err := readChannelMeshIDs(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrChannelRuntimeAuthority, err)
	}
	result := make([]ChannelTopicProjection, 0, len(channelIDs))
	for _, channelID := range channelIDs {
		authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), channelID)
		if err != nil {
			return nil, fmt.Errorf("%w: Channel %q: %v", ErrChannelRuntimeAuthority,
				channelID.String(), err)
		}
		result = append(result, channelTopicProjection(authority.channel))
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("read Channel topic runtime: commit: %w", err)
	}
	return result, nil
}

func channelTopicProjection(channel model.Channel) ChannelTopicProjection {
	return ChannelTopicProjection{ChannelID: channel.ID(), Status: channel.Status(),
		RosterHead: channel.RosterHead(), TopicState: channel.TopicState(), UpdatedAt: channel.UpdatedAt()}
}

func runtimeTopicState(state model.TopicState) bool {
	return state == model.TopicNotJoined || state == model.TopicJoining || state == model.TopicJoined
}

func validRuntimeTopicTransition(from, to model.TopicState) bool {
	switch from {
	case model.TopicNotJoined:
		return to == model.TopicJoining
	case model.TopicJoining:
		return to == model.TopicJoined || to == model.TopicNotJoined
	case model.TopicJoined:
		return to == model.TopicJoining
	default:
		return false
	}
}
