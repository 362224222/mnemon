package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var ErrChannelRosterInput, ErrChannelRosterConflict = errors.New("invalid Channel roster merge input"), errors.New("Channel roster conflicts with durable authority")

type ChannelRosterMergeStatus string

const (
	ChannelRosterDuplicate  ChannelRosterMergeStatus = "duplicate"
	ChannelRosterApplied    ChannelRosterMergeStatus = "applied"
	ChannelRosterGap        ChannelRosterMergeStatus = "gap"
	ChannelRosterConflicted ChannelRosterMergeStatus = "conflicted"
)

type MergeChannelRosterSpec struct {
	ChannelID                    model.ChannelID
	AuthenticatedTransportPeerID model.PeerID
	Records                      []model.Member
	At                           time.Time
}

type MergeChannelRosterResult struct {
	Status               ChannelRosterMergeStatus
	Channel              model.Channel
	Roster               model.VerifiedRoster
	ExpectedNextRevision uint64
}

// ChannelRosterMergePlan freezes one roster mutation and its complete runtime candidate.
type ChannelRosterMergePlan struct {
	channelAuthorityPlan
	spec         MergeChannelRosterSpec
	result       MergeChannelRosterResult
	expectedHead model.RecordHead
	conflictID   string
}

func (plan ChannelRosterMergePlan) Result() MergeChannelRosterResult { return plan.result }

// MergeChannelRoster is the direct compatibility path; runtime callers use Prepare/Commit/Resolve.
func (s *Store) MergeChannelRoster(ctx context.Context, spec MergeChannelRosterSpec) (MergeChannelRosterResult, error) {
	plan, err := s.PrepareChannelRosterMerge(ctx, spec)
	if err != nil {
		return MergeChannelRosterResult{}, err
	}
	return s.CommitChannelRosterMerge(ctx, plan)
}

func (s *Store) ResolveChannelRosterMerge(ctx context.Context, plan ChannelRosterMergePlan) (ChannelAuthorityPlanResolution, error) {
	tx, resolution, err := s.beginChannelAuthorityPlan(ctx, plan.channelAuthorityPlan, true)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	if plan.conflictID != "" && resolution != ChannelAuthorityPlanDiverged &&
		(!plan.ChangesAuthority() || resolution == ChannelAuthorityPlanCandidate) {
		present, evidenceErr := channelRosterPlanEvidence(ctx, tx, plan)
		if evidenceErr != nil {
			return "", evidenceErr
		}
		if present {
			resolution = ChannelAuthorityPlanCandidate
		} else if resolution == ChannelAuthorityPlanCandidate {
			resolution = ChannelAuthorityPlanDiverged
		}
	}
	return resolution, nil
}

func validateChannelRosterMergeSpec(s *Store, ctx context.Context, spec MergeChannelRosterSpec) (MergeChannelRosterSpec, error) {
	if s == nil || s.db == nil || ctx == nil || spec.ChannelID.IsZero() ||
		spec.AuthenticatedTransportPeerID.IsZero() || len(spec.Records) == 0 ||
		len(spec.Records) > model.MaxMemberRecordsPerChannel {
		return MergeChannelRosterSpec{}, ErrChannelRosterInput
	}
	at, err := canonicalStoreTime(spec.At)
	if err != nil {
		return MergeChannelRosterSpec{}, ErrChannelRosterInput
	}
	spec.At, spec.Records = at, append([]model.Member(nil), spec.Records...)
	first := spec.Records[0].Head().Revision()
	for index, record := range spec.Records {
		if first == 0 || record.IsZero() || record.ChannelID() != spec.ChannelID ||
			record.Head().Revision() != first+uint64(index) ||
			record.Head().Revision() > model.MaxMemberRecordsPerChannel {
			return MergeChannelRosterSpec{}, ErrChannelRosterInput
		}
	}
	return spec, nil
}

func readChannelRosterAuthority(ctx context.Context, tx *sql.Tx,
	channelID model.ChannelID) (model.Node, verifiedChannelAuthority, error) {
	node, err := readNode(ctx, tx)
	if err != nil {
		return model.Node{}, verifiedChannelAuthority{}, fmt.Errorf("merge Channel roster: Node: %w", err)
	}
	authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), channelID)
	if err != nil {
		return model.Node{}, verifiedChannelAuthority{}, fmt.Errorf("%w: %v", ErrChannelRosterConflict, err)
	}
	return node, authority, nil
}

func (s *Store) applyChannelRosterMerge(ctx context.Context, tx *sql.Tx, localPeer model.PeerID,
	authority verifiedChannelAuthority, spec MergeChannelRosterSpec) (MergeChannelRosterResult, string, error) {
	if authority.channel.Status() == model.ChannelAbandoned {
		return MergeChannelRosterResult{}, "", ErrChannelRosterConflict
	}
	if validateChannelRosterPage(authority.channel.Descriptor(), spec.Records) != nil {
		return MergeChannelRosterResult{}, "", ErrChannelRosterInput
	}
	existing := authority.roster.Members()
	firstRevision := spec.Records[0].Head().Revision()
	expected := uint64(len(existing)) + 1
	if firstRevision > expected {
		if !rosterTransportAuthorized(spec.AuthenticatedTransportPeerID, authority.roster) {
			return MergeChannelRosterResult{}, "", ErrChannelRosterInput
		}
		return MergeChannelRosterResult{Status: ChannelRosterGap, Channel: authority.channel,
			Roster: authority.roster, ExpectedNextRevision: expected}, "", nil
	}
	prefixLength := int(firstRevision - 1)
	if prefixLength > len(existing) {
		return MergeChannelRosterResult{}, "", ErrChannelRosterInput
	}
	members := append([]model.Member(nil), existing[:prefixLength]...)
	members = append(members, spec.Records...)
	candidate, err := model.NewVerifiedRoster(authority.channel.Descriptor(), members)
	if err != nil || !rosterTransportAuthorized(spec.AuthenticatedTransportPeerID, authority.roster, candidate) {
		return MergeChannelRosterResult{}, "", ErrChannelRosterInput
	}
	for index, overlap := 0, min(len(existing)-prefixLength, len(spec.Records)); index < overlap; index++ {
		incumbent, challenger := existing[prefixLength+index], spec.Records[index]
		if bytes.Equal(incumbent.WireJSON().Bytes(), challenger.WireJSON().Bytes()) {
			continue
		}
		conflictID, idErr := channelRosterConflictID(authority.channel.ID(), incumbent, challenger)
		if idErr != nil {
			return MergeChannelRosterResult{}, "", ErrChannelRosterInput
		}
		result, applyErr := s.applyChannelRosterConflict(ctx, tx, localPeer, authority, candidate,
			incumbent, challenger, spec.AuthenticatedTransportPeerID, spec.At, conflictID)
		return result, conflictID, applyErr
	}
	if len(members) <= len(existing) {
		return MergeChannelRosterResult{Status: ChannelRosterDuplicate, Channel: authority.channel,
			Roster: authority.roster, ExpectedNextRevision: expected}, "", nil
	}
	result, err := s.applyChannelRosterSuffix(ctx, tx, localPeer, authority, candidate, spec.At)
	return result, "", err
}

func (s *Store) applyChannelRosterSuffix(ctx context.Context, tx *sql.Tx, localPeer model.PeerID,
	authority verifiedChannelAuthority, candidate model.VerifiedRoster, at time.Time) (MergeChannelRosterResult, error) {
	if authority.channel.Status() == model.ChannelConflicted || at.Before(authority.channel.UpdatedAt()) ||
		at.Before(candidate.Members()[len(candidate.Members())-1].CreatedAt()) {
		return MergeChannelRosterResult{}, ErrChannelRosterConflict
	}
	for _, record := range candidate.Members()[len(authority.roster.Members()):] {
		if err := insertChannelMember(ctx, tx, record); err != nil {
			return MergeChannelRosterResult{}, mapChannelRosterError(err)
		}
	}
	channel, err := mergedChannelProjection(localPeer, authority.channel, candidate, at)
	if err != nil {
		return MergeChannelRosterResult{}, ErrChannelRosterConflict
	}
	updated, err := tx.ExecContext(ctx, `UPDATE channels SET roster_head_revision=?,roster_head_hash=?,
		status=?,topic_state=?,updated_at=? WHERE channel_id=? AND roster_head_revision=?
		AND roster_head_hash=? AND status=? AND topic_state=?`, channel.RosterHead().Revision(),
		channel.RosterHead().Digest().Bytes(), string(channel.Status()), string(channel.TopicState()),
		storeTime(channel.UpdatedAt()), channel.ID().String(), authority.channel.RosterHead().Revision(),
		authority.channel.RosterHead().Digest().Bytes(), string(authority.channel.Status()),
		string(authority.channel.TopicState()))
	if err != nil || exactlyOne(updated) != nil {
		return MergeChannelRosterResult{}, ErrChannelRosterConflict
	}
	if err := syncChannelRosterBindings(ctx, tx, localPeer, channel, candidate,
		authority.bindings, at); err != nil {
		return MergeChannelRosterResult{}, mapChannelRosterError(err)
	}
	if err := applyChannelRosterEgressEffects(ctx, tx, localPeer, authority.roster,
		channel, candidate, at); err != nil {
		return MergeChannelRosterResult{}, mapChannelRosterError(err)
	}
	committed, err := readVerifiedChannelAuthority(ctx, tx, localPeer, channel.ID())
	if err != nil || committed.channel.RosterHead() != candidate.Head() {
		return MergeChannelRosterResult{}, ErrChannelRosterConflict
	}
	return MergeChannelRosterResult{Status: ChannelRosterApplied, Channel: committed.channel,
		Roster: committed.roster, ExpectedNextRevision: committed.roster.Head().Revision() + 1}, nil
}

func validateChannelRosterPage(descriptor model.SignedChannelDescriptor, records []model.Member) error {
	var previous model.Member
	var lastCreatedAt time.Time
	ownerClosed := false
	pageCurrent := make(map[model.PeerID]model.Member)
	for index, member := range records {
		if ownerClosed || model.VerifyMember(descriptor, member) != nil {
			return ErrChannelRosterInput
		}
		if index != 0 {
			previousDigest, ok := member.PreviousDigest()
			if !ok || previousDigest != previous.Head().Digest() {
				return ErrChannelRosterInput
			}
		}
		if !lastCreatedAt.IsZero() && member.CreatedAt().Before(lastCreatedAt) {
			return ErrChannelRosterInput
		}
		if prior, ok := pageCurrent[member.PeerID()]; ok {
			if prior.Status().Terminal() || prior.OriginEpoch() != member.OriginEpoch() ||
				!bytes.Equal(prior.PublicKey(), member.PublicKey()) {
				return ErrChannelRosterInput
			}
		}
		if member.PeerID() == descriptor.Descriptor().OwnerPeerID() {
			if member.Status() == model.MemberRevoked {
				return ErrChannelRosterInput
			}
			ownerClosed = member.Status() == model.MemberLeft
		}
		pageCurrent[member.PeerID()] = member
		previous = member
		lastCreatedAt = member.CreatedAt()
	}
	return nil
}

func rosterTransportAuthorized(transport model.PeerID, rosters ...model.VerifiedRoster) bool {
	for _, roster := range rosters {
		if member, ok := roster.CurrentMember(transport); ok && member.Status() == model.MemberActive {
			return true
		}
	}
	return false
}

func mergedChannelProjection(localPeer model.PeerID, current model.Channel,
	roster model.VerifiedRoster, at time.Time) (model.Channel, error) {
	status, topic := current.Status(), current.TopicState()
	owner, ownerOK := roster.CurrentMember(current.OwnerPeerID())
	local, localOK := roster.CurrentMember(localPeer)
	if !ownerOK || !localOK {
		return model.Channel{}, ErrChannelRosterConflict
	}
	switch {
	case owner.Status() == model.MemberLeft:
		status, topic = model.ChannelClosed, model.TopicLeft
	case owner.Status().Terminal():
		return model.Channel{}, ErrChannelRosterConflict
	case local.Status().Terminal():
		status, topic = model.ChannelLeft, model.TopicLeft
	case status == model.ChannelActive && topic == model.TopicJoined:
		topic = model.TopicJoining
	}
	return model.NewChannel(model.ChannelSpec{Descriptor: current.Descriptor(),
		LocalAlias: current.LocalAlias(), RosterHead: roster.Head(), Status: status,
		TopicState: topic, UpdatedAt: at})
}

func (s *Store) applyChannelRosterConflict(ctx context.Context, tx *sql.Tx,
	localPeer model.PeerID, authority verifiedChannelAuthority, candidate model.VerifiedRoster,
	incumbent, challenger model.Member, transport model.PeerID, at time.Time,
	conflictID string) (MergeChannelRosterResult, error) {
	if incumbent.Head().Revision() != challenger.Head().Revision() ||
		incumbent.Head().Digest() == challenger.Head().Digest() ||
		at.Before(authority.channel.UpdatedAt()) || at.Before(incumbent.CreatedAt()) ||
		at.Before(challenger.CreatedAt()) || candidate.Head().Revision() < challenger.Head().Revision() {
		return MergeChannelRosterResult{}, ErrChannelRosterInput
	}
	if replay, found, err := readChannelRosterConflictReplay(ctx, tx, authority,
		challenger, conflictID); err != nil || found {
		return replay, err
	}
	if conflictID == "" {
		return MergeChannelRosterResult{}, ErrChannelRosterInput
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO channel_conflicts(conflict_id,channel_id,revision,
		incumbent_record_hash,incumbent_signed_record_json,incumbent_owner_signature,
		challenger_record_hash,challenger_signed_record_json,challenger_owner_signature,
		transport_peer_id,detected_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, conflictID,
		authority.channel.ID().String(), incumbent.Head().Revision(), incumbent.Head().Digest().Bytes(),
		incumbent.SignedRecord().Bytes(), incumbent.OwnerSignature(), challenger.Head().Digest().Bytes(),
		challenger.SignedRecord().Bytes(), challenger.OwnerSignature(), transport.String(), storeTime(at))
	if err != nil {
		return MergeChannelRosterResult{}, mapChannelRosterError(err)
	}
	updated, err := tx.ExecContext(ctx, `UPDATE channels SET status='conflicted',topic_state='blocked',
		updated_at=? WHERE channel_id=? AND roster_head_revision=? AND roster_head_hash=?`, storeTime(at),
		authority.channel.ID().String(), authority.channel.RosterHead().Revision(),
		authority.channel.RosterHead().Digest().Bytes())
	if err != nil || exactlyOne(updated) != nil {
		return MergeChannelRosterResult{}, ErrChannelRosterConflict
	}
	if err := blockChannelEgress(ctx, tx, authority.channel.ID(), nil,
		"signed roster conflict", at); err != nil {
		return MergeChannelRosterResult{}, mapChannelRosterError(err)
	}
	committed, err := readVerifiedChannelAuthority(ctx, tx, localPeer,
		authority.channel.ID())
	if err != nil || committed.channel.Status() != model.ChannelConflicted {
		return MergeChannelRosterResult{}, ErrChannelRosterConflict
	}
	return channelRosterConflictResult(committed), nil
}

func applyChannelRosterEgressEffects(ctx context.Context, tx *sql.Tx, localPeer model.PeerID,
	previous model.VerifiedRoster, channel model.Channel, candidate model.VerifiedRoster, at time.Time) error {
	if channel.Status().Terminal() || channel.Status() == model.ChannelConflicted {
		return blockChannelEgress(ctx, tx, channel.ID(), nil, "Channel membership is terminal", at)
	}
	for peerID, prior := range currentRosterMembers(previous) {
		current, ok := candidate.CurrentMember(peerID)
		if !ok || peerID == localPeer || prior.Status() != model.MemberActive || !current.Status().Terminal() {
			continue
		}
		if err := blockChannelEgress(ctx, tx, channel.ID(), &peerID,
			"target membership is terminal", at); err != nil {
			return err
		}
	}
	return nil
}

func currentRosterMembers(roster model.VerifiedRoster) map[model.PeerID]model.Member {
	current := make(map[model.PeerID]model.Member)
	for _, member := range roster.Members() {
		current[member.PeerID()] = member
	}
	return current
}

func blockChannelEgress(ctx context.Context, tx *sql.Tx, channelID model.ChannelID,
	target *model.PeerID, reason string, at time.Time) error {
	if target == nil {
		if _, err := tx.ExecContext(ctx, `UPDATE gossip_publications SET status='blocked',
			lease_owner=NULL,lease_until=NULL,last_error=?,updated_at=? WHERE channel_id=?
			AND status IN ('queued','leased')`, reason, storeTime(at), channelID.String()); err != nil {
			return fmt.Errorf("block Channel publications: %w", err)
		}
	}
	query := `UPDATE peer_deliveries SET status='blocked',last_error=?,updated_at=?
		WHERE channel_id=? AND status='pending'`
	args := []any{reason, storeTime(at), channelID.String()}
	if target != nil {
		query += ` AND target_peer_id=?`
		args = append(args, target.String())
	}
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("block Channel deliveries: %w", err)
	}
	return nil
}

func channelRosterConflictID(channelID model.ChannelID, incumbent, challenger model.Member) (string, error) {
	identity, err := model.JSONFrom(struct {
		Challenger string `json:"challenger"`
		ChannelID  string `json:"channel_id"`
		Incumbent  string `json:"incumbent"`
		Revision   uint64 `json:"revision"`
	}{Challenger: challenger.Head().Digest().String(), ChannelID: channelID.String(),
		Incumbent: incumbent.Head().Digest().String(), Revision: incumbent.Head().Revision()})
	if err != nil {
		return "", err
	}
	return "channel-conflict-" + strings.TrimPrefix(model.Sum(append(
		[]byte("mnemon/r5/channel-roster-conflict/1\x00"), identity.Bytes()...)).String(), "sha256:"), nil
}

func mapChannelRosterError(err error) error {
	if errors.Is(err, ErrChannelAuthorityInvariant) || errors.Is(err, ErrChannelJoinConflict) ||
		strings.Contains(err.Error(), "constraint failed") || strings.Contains(err.Error(), "channel_") {
		return fmt.Errorf("%w: %v", ErrChannelRosterConflict, err)
	}
	return fmt.Errorf("merge Channel roster: %w", err)
}
