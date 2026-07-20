package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// PeerReachabilityProjection is an observation attached to the exact current
// signed binding authority. LastSeenAt records successful reachability only;
// unreachable and unknown transitions preserve it.
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
	ChannelID          model.ChannelID
	PeerID             model.PeerID
	OriginEpoch        model.OriginEpoch
	ExpectedRosterHead model.RecordHead
	Reachability       model.Reachability
	At                 time.Time
}

type SetPeerReachabilityResult struct {
	Peer    PeerReachabilityProjection
	Changed bool
}

// SetPeerReachability updates only the current nonterminal signed binding.
// Successful observations advance last_seen_at; negative observations never
// erase it. Older callbacks cannot overwrite a newer successful observation.
func (s *Store) SetPeerReachability(ctx context.Context,
	spec SetPeerReachabilitySpec,
) (SetPeerReachabilityResult, error) {
	if !validPeerReachabilitySpec(s, ctx, spec) {
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
	authority, binding, err := readPeerReachabilityAuthority(ctx, tx, spec)
	if err != nil {
		return SetPeerReachabilityResult{}, err
	}
	lastSeen, hasLastSeen := binding.LastSeenAt()
	projection := peerReachabilityProjection(authority.roster.Head(), binding)
	if peerReachabilityReplay(binding, spec, at, lastSeen, hasLastSeen) {
		if err := tx.Commit(); err != nil {
			return SetPeerReachabilityResult{}, fmt.Errorf("set Peer reachability: commit replay: %w", err)
		}
		return SetPeerReachabilityResult{Peer: projection}, nil
	}
	if err := validatePeerReachabilityTransition(authority.roster, binding, spec, at, lastSeen,
		hasLastSeen); err != nil {
		return SetPeerReachabilityResult{}, err
	}
	if err := updatePeerReachability(ctx, tx, binding, spec, at, lastSeen, hasLastSeen); err != nil {
		return SetPeerReachabilityResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return SetPeerReachabilityResult{}, fmt.Errorf("set Peer reachability: commit: %w", err)
	}
	projection.Reachability = spec.Reachability
	if spec.Reachability == model.ReachabilityReachable {
		projection.LastSeenAt, projection.HasLastSeen = at, true
	}
	return SetPeerReachabilityResult{Peer: projection, Changed: true}, nil
}

func validPeerReachabilitySpec(s *Store, ctx context.Context, spec SetPeerReachabilitySpec) bool {
	return s != nil && s.db != nil && ctx != nil && !spec.ChannelID.IsZero() && !spec.PeerID.IsZero() &&
		!spec.OriginEpoch.IsZero() && !spec.ExpectedRosterHead.IsZero() && spec.Reachability.Valid()
}

func readPeerReachabilityAuthority(ctx context.Context, tx *sql.Tx,
	spec SetPeerReachabilitySpec,
) (verifiedChannelAuthority, model.PeerBinding, error) {
	node, err := readNode(ctx, tx)
	if err != nil {
		return verifiedChannelAuthority{}, model.PeerBinding{},
			fmt.Errorf("%w: Node: %v", ErrChannelRuntimeAuthority, err)
	}
	authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), spec.ChannelID)
	if err != nil {
		return verifiedChannelAuthority{}, model.PeerBinding{},
			fmt.Errorf("%w: %v", ErrChannelRuntimeAuthority, err)
	}
	if authority.channel.Status() != model.ChannelActive {
		return verifiedChannelAuthority{}, model.PeerBinding{}, ErrChannelRuntimeAuthority
	}
	if authority.roster.Head() != spec.ExpectedRosterHead {
		return verifiedChannelAuthority{}, model.PeerBinding{}, ErrChannelRuntimeConflict
	}
	binding, ok := currentRuntimeBinding(authority.bindings, spec.PeerID)
	if !ok || binding.State() == model.BindingRevoked {
		return verifiedChannelAuthority{}, model.PeerBinding{}, ErrChannelRuntimeAuthority
	}
	if binding.OriginEpoch() != spec.OriginEpoch {
		return verifiedChannelAuthority{}, model.PeerBinding{}, ErrChannelRuntimeConflict
	}
	return authority, binding, nil
}

func peerReachabilityReplay(binding model.PeerBinding, spec SetPeerReachabilitySpec, at,
	lastSeen time.Time, hasLastSeen bool,
) bool {
	return binding.Reachability() == spec.Reachability &&
		(spec.Reachability != model.ReachabilityReachable || hasLastSeen && lastSeen.Equal(at))
}

func validatePeerReachabilityTransition(roster model.VerifiedRoster, binding model.PeerBinding,
	spec SetPeerReachabilitySpec, at, lastSeen time.Time, hasLastSeen bool,
) error {
	member, ok := roster.CurrentMember(spec.PeerID)
	if !ok || at.Before(binding.JoinedAt()) || at.Before(member.CreatedAt()) ||
		hasLastSeen && (at.Before(lastSeen) || spec.Reachability == model.ReachabilityReachable &&
			binding.Reachability() != model.ReachabilityReachable && !at.After(lastSeen)) {
		return ErrChannelRuntimeConflict
	}
	return nil
}

func updatePeerReachability(ctx context.Context, tx *sql.Tx, binding model.PeerBinding,
	spec SetPeerReachabilitySpec, at, lastSeen time.Time, hasLastSeen bool,
) error {
	var nextLastSeen, expectedLastSeen any
	if hasLastSeen {
		nextLastSeen, expectedLastSeen = storeTime(lastSeen), storeTime(lastSeen)
	}
	if spec.Reachability == model.ReachabilityReachable {
		nextLastSeen = storeTime(at)
	}
	mutation, err := tx.ExecContext(ctx, `UPDATE peer_bindings SET reachability=?,last_seen_at=?
		WHERE channel_id=? AND peer_id=? AND origin_epoch=? AND member_revision=?
		AND member_record_hash=? AND state=? AND reachability=? AND last_seen_at IS ?`,
		string(spec.Reachability), nextLastSeen, spec.ChannelID.String(), spec.PeerID.String(),
		spec.OriginEpoch.String(), binding.MemberHead().Revision(), binding.MemberHead().Digest().Bytes(),
		string(binding.State()), string(binding.Reachability()), expectedLastSeen)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrChannelRuntimeConflict, err)
	}
	if err := exactlyOne(mutation); err != nil {
		return fmt.Errorf("%w: %v", ErrChannelRuntimeConflict,
			errors.New("PeerBinding changed during reachability update"))
	}
	return nil
}

// ReadPeerReachability returns the current verified binding observation,
// including terminal forensic state, without changing it.
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
