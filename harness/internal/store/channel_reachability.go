package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// PeerReachabilityProjection is an observation attached to exact signed
// binding authority. LastSeenAt records successful reachability only.
type PeerReachabilityProjection struct {
	ChannelID    model.ChannelID
	PeerID       model.PeerID
	OriginEpoch  model.OriginEpoch
	RosterHead   model.RecordHead
	BindingState model.BindingState
	Reachability model.Reachability
	LastSeenAt   time.Time
	HasLastSeen  bool
}

type SetPeerReachabilitySpec struct {
	ChannelID            model.ChannelID
	PeerID               model.PeerID
	OriginEpoch          model.OriginEpoch
	ExpectedRosterHead   model.RecordHead
	ExpectedMemberHead   model.RecordHead
	ExpectedBindingState model.BindingState
	Reachability         model.Reachability
	At                   time.Time
}

type SetPeerReachabilityResult struct {
	Peer    PeerReachabilityProjection
	Changed bool
}

// SetPeerReachability fences one observation to the exact current nonterminal signed binding.
func (s *Store) SetPeerReachability(ctx context.Context,
	spec SetPeerReachabilitySpec,
) (SetPeerReachabilityResult, error) {
	if !validSetPeerReachabilitySpec(s, ctx, spec) {
		return SetPeerReachabilityResult{}, ErrChannelRuntimeInput
	}
	at, err := canonicalStoreTime(spec.At)
	if err != nil || at.IsZero() {
		return SetPeerReachabilityResult{}, ErrChannelRuntimeInput
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SetPeerReachabilityResult{}, fmt.Errorf("set Peer reachability: begin: %w", err)
	}
	defer tx.Rollback()
	result, err := setPeerReachability(ctx, tx, spec, at)
	if err != nil {
		return SetPeerReachabilityResult{}, err
	}
	phase := "commit"
	if !result.Changed {
		phase = "commit replay"
	}
	if err := tx.Commit(); err != nil {
		return SetPeerReachabilityResult{}, fmt.Errorf("set Peer reachability: %s: %w", phase, err)
	}
	return result, nil
}

func validSetPeerReachabilitySpec(s *Store, ctx context.Context,
	spec SetPeerReachabilitySpec,
) bool {
	return s != nil && s.db != nil && ctx != nil && !spec.ChannelID.IsZero() &&
		!spec.PeerID.IsZero() && !spec.OriginEpoch.IsZero() && !spec.ExpectedRosterHead.IsZero() &&
		!spec.ExpectedMemberHead.IsZero() && spec.ExpectedBindingState.Valid() && spec.Reachability.Valid()
}

func setPeerReachability(ctx context.Context, tx *sql.Tx, spec SetPeerReachabilitySpec,
	at time.Time,
) (SetPeerReachabilityResult, error) {
	rosterHead, binding, member, err := readExactPeerReachabilityBinding(ctx, tx, spec)
	if err != nil {
		return SetPeerReachabilityResult{}, err
	}
	lastSeen, hasLastSeen := binding.LastSeenAt()
	projection := peerReachabilityProjection(rosterHead, binding)
	if replayedPeerReachability(binding, spec, at, lastSeen, hasLastSeen) {
		return SetPeerReachabilityResult{Peer: projection}, nil
	}
	if !validPeerReachabilityTime(binding, member, spec, at, lastSeen, hasLastSeen) {
		return SetPeerReachabilityResult{}, ErrChannelRuntimeConflict
	}
	nextLastSeen, expectedLastSeen := peerReachabilityLastSeenValues(spec, at, lastSeen, hasLastSeen)
	if err := updatePeerReachability(ctx, tx, binding, spec, nextLastSeen, expectedLastSeen); err != nil {
		return SetPeerReachabilityResult{}, err
	}
	projection.Reachability = spec.Reachability
	if spec.Reachability == model.ReachabilityReachable {
		projection.LastSeenAt, projection.HasLastSeen = at, true
	}
	return SetPeerReachabilityResult{Peer: projection, Changed: true}, nil
}

func readExactPeerReachabilityBinding(ctx context.Context, tx *sql.Tx,
	spec SetPeerReachabilitySpec,
) (model.RecordHead, model.PeerBinding, model.Member, error) {
	node, err := readNode(ctx, tx)
	if err != nil {
		return model.RecordHead{}, model.PeerBinding{}, model.Member{},
			fmt.Errorf("%w: Node: %v", ErrChannelRuntimeAuthority, err)
	}
	authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), spec.ChannelID)
	if err != nil {
		return model.RecordHead{}, model.PeerBinding{}, model.Member{},
			fmt.Errorf("%w: %v", ErrChannelRuntimeAuthority, err)
	}
	if authority.channel.Status() != model.ChannelActive {
		return model.RecordHead{}, model.PeerBinding{}, model.Member{}, ErrChannelRuntimeAuthority
	}
	if authority.roster.Head() != spec.ExpectedRosterHead {
		return model.RecordHead{}, model.PeerBinding{}, model.Member{}, ErrChannelRuntimeConflict
	}
	binding, ok := currentRuntimeBinding(authority.bindings, spec.PeerID)
	if !ok || binding.State() == model.BindingRevoked {
		return model.RecordHead{}, model.PeerBinding{}, model.Member{}, ErrChannelRuntimeAuthority
	}
	if binding.OriginEpoch() != spec.OriginEpoch || binding.MemberHead() != spec.ExpectedMemberHead ||
		binding.State() != spec.ExpectedBindingState {
		return model.RecordHead{}, model.PeerBinding{}, model.Member{}, ErrChannelRuntimeConflict
	}
	member, ok := authority.roster.CurrentMember(spec.PeerID)
	if !ok || member.Head() != binding.MemberHead() {
		return model.RecordHead{}, model.PeerBinding{}, model.Member{}, ErrChannelRuntimeAuthority
	}
	return authority.roster.Head(), binding, member, nil
}

func replayedPeerReachability(binding model.PeerBinding, spec SetPeerReachabilitySpec,
	at, lastSeen time.Time, hasLastSeen bool,
) bool {
	return binding.Reachability() == spec.Reachability &&
		(spec.Reachability != model.ReachabilityReachable || (hasLastSeen && lastSeen.Equal(at)))
}

func validPeerReachabilityTime(binding model.PeerBinding, member model.Member,
	spec SetPeerReachabilitySpec, at, lastSeen time.Time, hasLastSeen bool,
) bool {
	return !at.Before(binding.JoinedAt()) && !at.Before(member.CreatedAt()) &&
		(!hasLastSeen || (!at.Before(lastSeen) &&
			(spec.Reachability != model.ReachabilityReachable ||
				binding.Reachability() == model.ReachabilityReachable || at.After(lastSeen))))
}

func peerReachabilityLastSeenValues(spec SetPeerReachabilitySpec, at, lastSeen time.Time,
	hasLastSeen bool,
) (any, any) {
	var next, expected any
	if hasLastSeen {
		next, expected = storeTime(lastSeen), storeTime(lastSeen)
	}
	if spec.Reachability == model.ReachabilityReachable {
		next = storeTime(at)
	}
	return next, expected
}

func updatePeerReachability(ctx context.Context, tx *sql.Tx, binding model.PeerBinding,
	spec SetPeerReachabilitySpec, nextLastSeen, expectedLastSeen any,
) error {
	mutation, err := tx.ExecContext(ctx, `UPDATE peer_bindings SET reachability=?,last_seen_at=?
		WHERE channel_id=? AND peer_id=? AND origin_epoch=? AND member_revision=?
		AND member_record_hash=? AND state=? AND reachability=? AND last_seen_at IS ?`,
		string(spec.Reachability), nextLastSeen, spec.ChannelID.String(), spec.PeerID.String(),
		spec.OriginEpoch.String(), binding.MemberHead().Revision(), binding.MemberHead().Digest().Bytes(),
		string(binding.State()), string(binding.Reachability()), expectedLastSeen)
	if err == nil {
		err = exactlyOne(mutation)
	}
	if err != nil {
		return fmt.Errorf("%w: PeerBinding changed during reachability update: %v",
			ErrChannelRuntimeConflict, err)
	}
	return nil
}

// ReadPeerReachability returns current verified binding evidence, including
// terminal forensic state, without changing it.
func (s *Store) ReadPeerReachability(ctx context.Context, channelID model.ChannelID,
	peerID model.PeerID,
) (PeerReachabilityProjection, error) {
	if s == nil || s.db == nil || ctx == nil || channelID.IsZero() || peerID.IsZero() {
		return PeerReachabilityProjection{}, ErrChannelRuntimeInput
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return PeerReachabilityProjection{}, fmt.Errorf("read Peer reachability: begin: %w", err)
	}
	defer tx.Rollback()
	node, err := readNode(ctx, tx)
	if err != nil {
		return PeerReachabilityProjection{}, fmt.Errorf("%w: Node: %v", ErrChannelRuntimeAuthority, err)
	}
	authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), channelID)
	if err != nil {
		return PeerReachabilityProjection{}, fmt.Errorf("%w: %v", ErrChannelRuntimeAuthority, err)
	}
	binding, ok := currentRuntimeBinding(authority.bindings, peerID)
	if !ok {
		return PeerReachabilityProjection{}, ErrChannelRuntimeAuthority
	}
	result := peerReachabilityProjection(authority.roster.Head(), binding)
	if err := tx.Commit(); err != nil {
		return PeerReachabilityProjection{}, fmt.Errorf("read Peer reachability: commit: %w", err)
	}
	return result, nil
}

func currentRuntimeBinding(bindings []model.PeerBinding, peerID model.PeerID) (model.PeerBinding, bool) {
	for _, binding := range bindings {
		if binding.PeerID() == peerID {
			return binding, true
		}
	}
	return model.PeerBinding{}, false
}

func peerReachabilityProjection(rosterHead model.RecordHead,
	binding model.PeerBinding,
) PeerReachabilityProjection {
	lastSeen, hasLastSeen := binding.LastSeenAt()
	return PeerReachabilityProjection{ChannelID: binding.ChannelID(), PeerID: binding.PeerID(),
		OriginEpoch: binding.OriginEpoch(), RosterHead: rosterHead, BindingState: binding.State(),
		Reachability: binding.Reachability(), LastSeenAt: lastSeen, HasLastSeen: hasLastSeen}
}
