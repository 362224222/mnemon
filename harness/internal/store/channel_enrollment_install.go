package store

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type InstallJoinedChannelSpec struct {
	AuthenticatedOwnerPeerID model.PeerID
	LocalAlias               string
	Descriptor               model.SignedChannelDescriptor
	Transcript               model.EnrollmentTranscript
	Receipt                  model.EnrollmentReceipt
	Members                  []model.Member
	At                       time.Time
}

type InstallJoinedChannelResult struct {
	Installed bool
	Status    ChannelEnrollmentStatus
	Channel   model.Channel
	Roster    model.VerifiedRoster
}

// InstallJoinedChannel is the joiner's atomic replica boundary. It persists no
// grant or verifier authority: only signed descriptor/roster/receipt evidence,
// the local publication epoch and pending bindings are installed.
func (s *Store) InstallJoinedChannel(ctx context.Context,
	spec InstallJoinedChannelSpec,
) (InstallJoinedChannelResult, error) {
	if s == nil || s.db == nil || ctx == nil {
		return InstallJoinedChannelResult{}, ErrChannelJoinInput
	}
	roster, at, err := validateJoinedChannelInput(spec)
	if err != nil {
		return InstallJoinedChannelResult{}, err
	}
	member, err := verifyJoinedMemberEvidence(spec, roster, at)
	if err != nil {
		return InstallJoinedChannelResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return InstallJoinedChannelResult{}, fmt.Errorf("install joined Channel: begin: %w", err)
	}
	defer tx.Rollback()
	node, err := readJoinedChannelNode(ctx, tx, spec, roster)
	if err != nil {
		return InstallJoinedChannelResult{}, err
	}
	exists, err := joinedChannelExists(ctx, tx, roster.Descriptor().Descriptor().ID())
	if err != nil {
		return InstallJoinedChannelResult{}, err
	}
	if exists {
		result, err := replayJoinedChannel(ctx, tx, node, spec, roster)
		if err != nil {
			return InstallJoinedChannelResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return InstallJoinedChannelResult{}, mapChannelJoinError(err)
		}
		return result, nil
	}
	if err := verifyJoinedChannelReservation(ctx, tx, node, spec); err != nil {
		return InstallJoinedChannelResult{}, err
	}
	result, declined, err := declineTerminalJoinedChannel(ctx, tx, node, spec, roster)
	if err != nil {
		return InstallJoinedChannelResult{}, err
	}
	if declined {
		return result, nil
	}
	if err := consumeJoinedChannelReservation(ctx, tx, spec.Transcript.RequestID()); err != nil {
		return InstallJoinedChannelResult{}, err
	}
	channel, err := insertJoinedChannelReplica(ctx, tx, node, spec, roster, at)
	if err != nil {
		return InstallJoinedChannelResult{}, err
	}
	committed, err := verifyJoinedChannelReplica(ctx, tx, node, spec, channel, roster, member)
	if err != nil {
		return InstallJoinedChannelResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return InstallJoinedChannelResult{}, mapChannelJoinError(err)
	}
	return InstallJoinedChannelResult{Installed: true, Status: ChannelEnrollmentAccepted,
		Channel: committed.channel, Roster: committed.roster}, nil
}

// validateJoinedChannelInput checks the completeness of the join response and
// that descriptor, transcript and authenticated owner identity all bind to
// the same Channel before the verified roster is accepted.
func validateJoinedChannelInput(spec InstallJoinedChannelSpec,
) (model.VerifiedRoster, time.Time, error) {
	if spec.AuthenticatedOwnerPeerID.IsZero() || spec.Descriptor.IsZero() ||
		spec.Transcript.IsZero() || spec.Receipt.IsZero() || len(spec.Members) == 0 {
		return model.VerifiedRoster{}, time.Time{}, ErrChannelJoinInput
	}
	at, err := canonicalStoreTime(spec.At)
	roster, rosterErr := model.NewVerifiedRoster(spec.Descriptor, spec.Members)
	if err != nil || rosterErr != nil || spec.AuthenticatedOwnerPeerID != spec.Descriptor.Descriptor().OwnerPeerID() ||
		spec.Transcript.OwnerPeerID() != spec.AuthenticatedOwnerPeerID ||
		spec.Transcript.ChannelID() != spec.Descriptor.Descriptor().ID() {
		return model.VerifiedRoster{}, time.Time{}, ErrChannelJoinInput
	}
	return roster, at, nil
}

// verifyJoinedMemberEvidence proves the receipt names the joiner's first
// roster authority immediately after the challenged head, and that the
// install time does not precede the evidence it installs.
func verifyJoinedMemberEvidence(spec InstallJoinedChannelSpec, roster model.VerifiedRoster,
	at time.Time,
) (model.Member, error) {
	member, ok := memberAtHead(roster, spec.Receipt.MemberHead())
	joinIdentity, identityErr := spec.Transcript.JoinIdentityDigest()
	previous, hasPrevious := member.PreviousDigest()
	if !ok || !firstRosterAuthorityForPeer(roster, member) ||
		member.PeerID() != spec.Transcript.JoinerPeerID() ||
		identityErr != nil || spec.Receipt.RequestID() != spec.Transcript.RequestID() ||
		spec.Receipt.GrantID() != spec.Transcript.GrantID() ||
		spec.Receipt.JoinIdentityDigest() != joinIdentity ||
		!hasPrevious || member.Head().Revision() != spec.Transcript.RosterHead().Revision()+1 ||
		previous != spec.Transcript.RosterHead().Digest() ||
		model.VerifyEnrollmentReceiptEvidence(spec.Descriptor, member, spec.Receipt) != nil {
		return model.Member{}, ErrChannelJoinInput
	}
	latestMembers := roster.Members()
	if at.Before(latestMembers[len(latestMembers)-1].CreatedAt()) || at.Before(spec.Receipt.AcceptedAt()) {
		return model.Member{}, ErrChannelJoinInput
	}
	return member, nil
}

// readJoinedChannelNode proves the local Node is the transcript's joiner in
// its current origin epoch and currently appears in the verified roster.
func readJoinedChannelNode(ctx context.Context, tx *sql.Tx, spec InstallJoinedChannelSpec,
	roster model.VerifiedRoster,
) (model.Node, error) {
	node, err := readNode(ctx, tx)
	if err != nil || node.PeerID() != spec.Transcript.JoinerPeerID() ||
		node.OriginEpoch() != spec.Transcript.JoinerOriginEpoch() {
		return model.Node{}, ErrChannelJoinInput
	}
	if _, exists := roster.CurrentMember(node.PeerID()); !exists {
		return model.Node{}, ErrChannelJoinInput
	}
	return node, nil
}

func joinedChannelExists(ctx context.Context, tx *sql.Tx, channelID model.ChannelID) (bool, error) {
	var channelCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM channels WHERE channel_id=?`,
		channelID.String()).Scan(&channelCount); err != nil {
		return false, mapChannelJoinError(err)
	}
	return channelCount != 0, nil
}

// declineTerminalJoinedChannel settles a fresh install whose verified roster
// already terminates the owner or the local member: the reservation is
// consumed and committed, but no replica rows are written.
func declineTerminalJoinedChannel(ctx context.Context, tx *sql.Tx, node model.Node,
	spec InstallJoinedChannelSpec, roster model.VerifiedRoster,
) (InstallJoinedChannelResult, bool, error) {
	currentLocal, localExists := roster.CurrentMember(node.PeerID())
	currentOwner, ownerExists := roster.CurrentMember(spec.Descriptor.Descriptor().OwnerPeerID())
	if !localExists || !ownerExists {
		return InstallJoinedChannelResult{}, false, ErrChannelJoinInput
	}
	if !currentOwner.Status().Terminal() && !currentLocal.Status().Terminal() {
		return InstallJoinedChannelResult{}, false, nil
	}
	if err := consumeJoinedChannelReservation(ctx, tx, spec.Transcript.RequestID()); err != nil {
		return InstallJoinedChannelResult{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return InstallJoinedChannelResult{}, false, mapChannelJoinError(err)
	}
	status := ChannelEnrollmentChannelClosed
	if currentLocal.Status().Terminal() {
		status = ChannelEnrollmentMemberRevoked
	}
	return InstallJoinedChannelResult{Status: status, Roster: roster}, true, nil
}

// insertJoinedChannelReplica persists the joiner's replica rows: the Channel
// projection, every verified member, the receipt evidence, the local
// publication epoch and the pending PeerBindings.
func insertJoinedChannelReplica(ctx context.Context, tx *sql.Tx, node model.Node,
	spec InstallJoinedChannelSpec, roster model.VerifiedRoster, at time.Time,
) (model.Channel, error) {
	channel, err := model.NewChannel(model.ChannelSpec{Descriptor: spec.Descriptor,
		LocalAlias: spec.LocalAlias, RosterHead: roster.Head(), Status: model.ChannelActive,
		TopicState: model.TopicNotJoined, UpdatedAt: at})
	if err != nil {
		return model.Channel{}, fmt.Errorf("%w: Channel projection: %v", ErrChannelJoinInput, err)
	}
	descriptor := channel.Descriptor().Descriptor()
	_, err = tx.ExecContext(ctx, `INSERT INTO channels(channel_id,name,local_alias,owner_peer_id,
		owner_public_key,descriptor_json,descriptor_digest,descriptor_signature,member_limit,
		roster_head_revision,roster_head_hash,status,topic_state,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, channel.ID().String(), channel.Name(), channel.LocalAlias(),
		channel.OwnerPeerID().String(), channel.OwnerPublicKey(), descriptor.CanonicalJSON().Bytes(),
		descriptor.Digest().Bytes(), channel.Descriptor().OwnerSignature(), channel.MemberLimit(),
		channel.RosterHead().Revision(), channel.RosterHead().Digest().Bytes(), string(channel.Status()),
		string(channel.TopicState()), storeTime(channel.CreatedAt()), storeTime(channel.UpdatedAt()))
	if err != nil {
		return model.Channel{}, mapChannelJoinError(err)
	}
	for _, rosterMember := range roster.Members() {
		if err := insertChannelMember(ctx, tx, rosterMember); err != nil {
			return model.Channel{}, mapChannelJoinError(err)
		}
	}
	if err := insertEnrollmentReceipt(ctx, tx, spec.Receipt, ""); err != nil {
		return model.Channel{}, mapChannelJoinError(err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO publication_epochs(channel_id,origin_peer_id,origin_epoch,
		source_floor_channel_seq,source_head_channel_seq,updated_at) VALUES(?,?,?,1,0,?)`,
		channel.ID().String(), node.PeerID().String(), node.OriginEpoch().String(), storeTime(at))
	if err != nil {
		return model.Channel{}, mapChannelJoinError(err)
	}
	if err := insertPendingRosterBindings(ctx, tx, node.PeerID(), channel, roster, at); err != nil {
		return model.Channel{}, mapChannelJoinError(err)
	}
	return channel, nil
}

// verifyJoinedChannelReplica re-reads the committed replica and proves the
// roster head and the stored receipt evidence match the installed response.
func verifyJoinedChannelReplica(ctx context.Context, tx *sql.Tx, node model.Node,
	spec InstallJoinedChannelSpec, channel model.Channel, roster model.VerifiedRoster,
	member model.Member,
) (verifiedChannelAuthority, error) {
	committed, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), channel.ID())
	if err != nil {
		return verifiedChannelAuthority{}, mapChannelJoinError(err)
	}
	if committed.channel.RosterHead() != roster.Head() {
		return verifiedChannelAuthority{}, ErrChannelJoinConflict
	}
	storedReceipt, ownerUse, err := readEnrollmentReceipt(ctx, tx, spec.Receipt.ReceiptID())
	if err != nil || ownerUse.Valid || !bytes.Equal(storedReceipt.WireJSON().Bytes(), spec.Receipt.WireJSON().Bytes()) ||
		model.VerifyEnrollmentReceiptEvidence(channel.Descriptor(), member, storedReceipt) != nil {
		return verifiedChannelAuthority{}, ErrChannelJoinConflict
	}
	return committed, nil
}

func replayJoinedChannel(ctx context.Context, tx *sql.Tx, node model.Node,
	spec InstallJoinedChannelSpec, incoming model.VerifiedRoster,
) (InstallJoinedChannelResult, error) {
	authority, err := verifyJoinedChannelReplay(ctx, tx, node, spec, incoming)
	if err != nil {
		return InstallJoinedChannelResult{}, err
	}
	if len(incoming.Members()) > len(authority.roster.Members()) {
		authority, err = applyJoinedChannelSuffix(ctx, tx, node, authority, incoming, spec.At)
		if err != nil {
			return InstallJoinedChannelResult{}, err
		}
	}
	status, ok := replayedEnrollmentStatus(authority.channel, authority.roster, node.PeerID())
	if !ok {
		return InstallJoinedChannelResult{}, ErrChannelJoinConflict
	}
	return InstallJoinedChannelResult{Status: status, Channel: authority.channel,
		Roster: authority.roster}, nil
}

// verifyJoinedChannelReplay proves the delayed response matches the durable
// replica: identical alias and descriptor, a shared verified member prefix,
// and the exact stored receipt without owner-side use linkage.
func verifyJoinedChannelReplay(ctx context.Context, tx *sql.Tx, node model.Node,
	spec InstallJoinedChannelSpec, incoming model.VerifiedRoster,
) (verifiedChannelAuthority, error) {
	authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(),
		incoming.Descriptor().Descriptor().ID())
	if err != nil || authority.channel.LocalAlias() != spec.LocalAlias ||
		!bytes.Equal(authority.channel.Descriptor().WireJSON().Bytes(), spec.Descriptor.WireJSON().Bytes()) {
		return verifiedChannelAuthority{}, ErrChannelJoinConflict
	}
	storedMembers := authority.roster.Members()
	incomingMembers := incoming.Members()
	common := len(storedMembers)
	if len(incomingMembers) < common {
		common = len(incomingMembers)
	}
	// Either side may be the longer verified prefix: a delayed response is a
	// read, while a newer response is an atomic suffix apply.
	for index := 0; index < common; index++ {
		if !bytes.Equal(storedMembers[index].WireJSON().Bytes(), incomingMembers[index].WireJSON().Bytes()) {
			return verifiedChannelAuthority{}, ErrChannelJoinConflict
		}
	}
	storedReceipt, ownerUse, err := readEnrollmentReceipt(ctx, tx, spec.Receipt.ReceiptID())
	if err != nil || ownerUse.Valid ||
		!bytes.Equal(storedReceipt.WireJSON().Bytes(), spec.Receipt.WireJSON().Bytes()) {
		return verifiedChannelAuthority{}, ErrChannelJoinConflict
	}
	return authority, nil
}

// applyJoinedChannelSuffix commits the newer verified roster suffix onto the
// replica: new members, the rolled-forward Channel head and lifecycle, and
// the synchronized PeerBindings, then re-reads the committed authority.
func applyJoinedChannelSuffix(ctx context.Context, tx *sql.Tx, node model.Node,
	authority verifiedChannelAuthority, incoming model.VerifiedRoster, at time.Time,
) (verifiedChannelAuthority, error) {
	if at.Before(authority.channel.UpdatedAt()) || authority.channel.Status() == model.ChannelConflicted ||
		authority.channel.Status() == model.ChannelAbandoned {
		return verifiedChannelAuthority{}, ErrChannelJoinConflict
	}
	for _, member := range incoming.Members()[len(authority.roster.Members()):] {
		if err := insertChannelMember(ctx, tx, member); err != nil {
			return verifiedChannelAuthority{}, mapChannelJoinError(err)
		}
	}
	status, topic, err := joinedChannelSuffixLifecycle(node, authority, incoming)
	if err != nil {
		return verifiedChannelAuthority{}, err
	}
	channel, err := model.NewChannel(model.ChannelSpec{Descriptor: authority.channel.Descriptor(),
		LocalAlias: authority.channel.LocalAlias(), RosterHead: incoming.Head(), Status: status,
		TopicState: topic, UpdatedAt: at})
	if err != nil {
		return verifiedChannelAuthority{}, ErrChannelJoinConflict
	}
	updated, err := tx.ExecContext(ctx, `UPDATE channels SET roster_head_revision=?,roster_head_hash=?,
		status=?,topic_state=?,updated_at=? WHERE channel_id=? AND roster_head_revision=?
		AND roster_head_hash=?`, channel.RosterHead().Revision(), channel.RosterHead().Digest().Bytes(),
		string(channel.Status()), string(channel.TopicState()), storeTime(channel.UpdatedAt()),
		channel.ID().String(), authority.channel.RosterHead().Revision(),
		authority.channel.RosterHead().Digest().Bytes())
	if err != nil {
		return verifiedChannelAuthority{}, mapChannelJoinError(err)
	}
	if changed, changedErr := updated.RowsAffected(); changedErr != nil || changed != 1 {
		return verifiedChannelAuthority{}, ErrChannelJoinConflict
	}
	if err := syncChannelRosterBindings(ctx, tx, node.PeerID(), channel, incoming,
		authority.bindings, at); err != nil {
		return verifiedChannelAuthority{}, mapChannelJoinError(err)
	}
	committed, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), channel.ID())
	if err != nil || committed.channel.RosterHead() != incoming.Head() {
		return verifiedChannelAuthority{}, ErrChannelJoinConflict
	}
	return committed, nil
}

// joinedChannelSuffixLifecycle derives the replica Channel status and topic
// from the owner and local member terminals in the newer verified roster.
func joinedChannelSuffixLifecycle(node model.Node, authority verifiedChannelAuthority,
	incoming model.VerifiedRoster,
) (model.ChannelStatus, model.TopicState, error) {
	currentLocal, ok := incoming.CurrentMember(node.PeerID())
	if !ok {
		return "", "", ErrChannelJoinConflict
	}
	currentOwner, ownerOK := incoming.CurrentMember(authority.channel.OwnerPeerID())
	if !ownerOK {
		return "", "", ErrChannelJoinConflict
	}
	status, topic := authority.channel.Status(), authority.channel.TopicState()
	if currentOwner.Status() == model.MemberLeft {
		status, topic = model.ChannelClosed, model.TopicLeft
	} else if currentOwner.Status().Terminal() {
		return "", "", ErrChannelJoinConflict
	} else if currentLocal.Status().Terminal() {
		status, topic = model.ChannelLeft, model.TopicLeft
	} else if status != model.ChannelActive {
		return "", "", ErrChannelJoinConflict
	}
	return status, topic, nil
}
