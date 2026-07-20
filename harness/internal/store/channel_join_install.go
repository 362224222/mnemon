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

type InstallJoinedChannelSpec struct {
	AuthenticatedOwnerPeerID model.PeerID
	OwnerOutcome             ChannelEnrollmentStatus
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

// InstallJoinedChannel is a transitional Store-only bridge. Production
// composition uses the typed plan so runtime and durable authority cannot
// advance through independent paths.
func (s *Store) InstallJoinedChannel(ctx context.Context,
	spec InstallJoinedChannelSpec,
) (InstallJoinedChannelResult, error) {
	plan, err := s.PrepareJoinedChannelInstall(ctx, spec)
	if err != nil {
		return InstallJoinedChannelResult{}, err
	}
	return s.CommitJoinedChannelInstall(ctx, plan)
}

func applyJoinedChannelInstall(ctx context.Context, tx *sql.Tx, node model.Node,
	candidate joinedChannelInstallCandidate, reservation joinedChannelReservationFence,
) (InstallJoinedChannelResult, error) {
	roster := candidate.roster
	currentLocal, exists := roster.CurrentMember(node.PeerID())
	if !exists || node.PeerID() != candidate.transcript.JoinerPeerID() ||
		node.OriginEpoch() != candidate.transcript.JoinerOriginEpoch() {
		return InstallJoinedChannelResult{}, ErrChannelJoinInput
	}
	var channelExists int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM channels WHERE channel_id=?`,
		candidate.descriptor.Descriptor().ID().String()).Scan(&channelExists); err != nil {
		return InstallJoinedChannelResult{}, mapChannelJoinError(err)
	}
	if channelExists != 0 {
		return replayJoinedChannel(ctx, tx, node, candidate.installSpec(), roster)
	}
	if reservation.isZero() || verifyJoinedChannelReservationFence(ctx, tx, reservation) != nil {
		return InstallJoinedChannelResult{}, ErrChannelJoinConflict
	}
	currentOwner, ownerExists := roster.CurrentMember(candidate.descriptor.Descriptor().OwnerPeerID())
	if !ownerExists {
		return InstallJoinedChannelResult{}, ErrChannelJoinInput
	}
	if currentOwner.Status().Terminal() || currentLocal.Status().Terminal() {
		return finishTerminalJoinedChannelInstall(ctx, tx, candidate, currentLocal)
	}
	return installJoinedChannelReplica(ctx, tx, node, candidate)
}

func finishTerminalJoinedChannelInstall(ctx context.Context, tx *sql.Tx,
	candidate joinedChannelInstallCandidate, currentLocal model.Member,
) (InstallJoinedChannelResult, error) {
	if err := consumeJoinedChannelReservation(ctx, tx, candidate.transcript.RequestID()); err != nil {
		return InstallJoinedChannelResult{}, err
	}
	status := ChannelEnrollmentChannelClosed
	if currentLocal.Status().Terminal() {
		status = ChannelEnrollmentMemberRevoked
	}
	return InstallJoinedChannelResult{Status: status, Roster: candidate.roster}, nil
}

func installJoinedChannelReplica(ctx context.Context, tx *sql.Tx, node model.Node,
	candidate joinedChannelInstallCandidate,
) (InstallJoinedChannelResult, error) {
	if err := consumeJoinedChannelReservation(ctx, tx, candidate.transcript.RequestID()); err != nil {
		return InstallJoinedChannelResult{}, err
	}
	channel, err := model.NewChannel(model.ChannelSpec{Descriptor: candidate.descriptor,
		LocalAlias: candidate.localAlias, RosterHead: candidate.roster.Head(), Status: model.ChannelActive,
		TopicState: model.TopicNotJoined, UpdatedAt: candidate.at})
	if err != nil {
		return InstallJoinedChannelResult{}, fmt.Errorf("%w: Channel projection: %v", ErrChannelJoinInput, err)
	}
	if err := insertJoinedChannelAuthority(ctx, tx, node, channel, candidate); err != nil {
		return InstallJoinedChannelResult{}, err
	}
	committed, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), channel.ID())
	if err != nil {
		return InstallJoinedChannelResult{}, mapChannelJoinError(err)
	}
	if committed.channel.RosterHead() != candidate.roster.Head() {
		return InstallJoinedChannelResult{}, ErrChannelJoinConflict
	}
	member, _ := memberAtHead(candidate.roster, candidate.receipt.MemberHead())
	storedReceipt, ownerUse, err := readEnrollmentReceipt(ctx, tx, candidate.receipt.ReceiptID())
	if err != nil || ownerUse.Valid ||
		!bytes.Equal(storedReceipt.WireJSON().Bytes(), candidate.receipt.WireJSON().Bytes()) ||
		model.VerifyEnrollmentReceiptEvidence(channel.Descriptor(), member, storedReceipt) != nil {
		return InstallJoinedChannelResult{}, ErrChannelJoinConflict
	}
	return InstallJoinedChannelResult{Installed: true, Status: ChannelEnrollmentAccepted,
		Channel: committed.channel, Roster: committed.roster}, nil
}

func insertJoinedChannelAuthority(ctx context.Context, tx *sql.Tx, node model.Node,
	channel model.Channel, candidate joinedChannelInstallCandidate,
) error {
	if err := insertChannelProjection(ctx, tx, channel); err != nil {
		return mapChannelJoinError(err)
	}
	for _, member := range candidate.roster.Members() {
		if err := insertChannelMember(ctx, tx, member); err != nil {
			return mapChannelJoinError(err)
		}
	}
	if err := insertEnrollmentReceipt(ctx, tx, candidate.receipt, ""); err != nil {
		return mapChannelJoinError(err)
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO publication_epochs(channel_id,origin_peer_id,origin_epoch,
		source_floor_channel_seq,source_head_channel_seq,updated_at) VALUES(?,?,?,1,0,?)`,
		channel.ID().String(), node.PeerID().String(), node.OriginEpoch().String(), storeTime(candidate.at))
	if err != nil {
		return mapChannelJoinError(err)
	}
	return mapChannelJoinError(insertPendingRosterBindings(ctx, tx, node.PeerID(), channel,
		candidate.roster, candidate.at))
}

func readJoinedChannelReplayAuthority(ctx context.Context, tx *sql.Tx, node model.Node,
	spec InstallJoinedChannelSpec, expectedHead model.RecordHead,
) (verifiedChannelAuthority, error) {
	authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(),
		spec.Descriptor.Descriptor().ID())
	if err != nil {
		return verifiedChannelAuthority{}, mapChannelJoinError(err)
	}
	if authority.channel.LocalAlias() != spec.LocalAlias ||
		!bytes.Equal(authority.channel.Descriptor().WireJSON().Bytes(),
			spec.Descriptor.WireJSON().Bytes()) ||
		(!expectedHead.IsZero() && authority.channel.RosterHead() != expectedHead) {
		return verifiedChannelAuthority{}, ErrChannelJoinConflict
	}
	return authority, nil
}

func mapChannelJoinError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrChannelAuthorityInvariant) {
		return fmt.Errorf("install joined Channel: %w", err)
	}
	message := err.Error()
	if strings.Contains(message, "node_channel_limit") {
		return fmt.Errorf("%w: %v", ErrNodeChannelLimit, err)
	}
	if strings.Contains(message, "UNIQUE constraint failed") ||
		strings.Contains(message, "FOREIGN KEY constraint failed") ||
		strings.Contains(message, "join reservation conflicts") ||
		strings.Contains(message, "conflicts with reserved join") ||
		strings.Contains(message, "join reservation attempt") ||
		strings.Contains(message, "join reservation state or time") ||
		errors.Is(err, ErrChannelEnrollmentConflict) {
		return fmt.Errorf("%w: %v", ErrChannelJoinConflict, err)
	}
	return fmt.Errorf("install joined Channel: %w", err)
}
