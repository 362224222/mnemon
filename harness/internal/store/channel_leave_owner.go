package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type AcceptChannelLeaveSpec struct {
	AuthenticatedPeerID model.PeerID
	Request             model.SignedChannelLeaveRequest
	Signer              ChannelAuthoritySigner
	At                  time.Time
}

type AcceptChannelLeaveResult struct {
	Channel      model.Channel
	Roster       model.VerifiedRoster
	ActiveMember model.Member
	Terminal     model.Member
	Receipt      model.SignedChannelLeaveReceipt
	Replay       bool
}

// AcceptChannelLeave is the owner's sole voluntary-leave mutation boundary.
// It verifies the secure requester, appends an owner-signed left record when
// the member is still active, and persists the exact signed receipt in the
// same transaction. A prior remove wins by terminal precedence and is
// acknowledged without forging a replacement left record.
func (s *Store) AcceptChannelLeave(ctx context.Context,
	spec AcceptChannelLeaveSpec,
) (AcceptChannelLeaveResult, error) {
	at, err := validateAcceptChannelLeaveInput(s, ctx, spec)
	if err != nil {
		return AcceptChannelLeaveResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AcceptChannelLeaveResult{}, fmt.Errorf("accept Channel leave: begin: %w", err)
	}
	defer tx.Rollback()
	node, authority, err := readChannelLeaveAuthority(ctx, tx, spec.Request.Record().ChannelID())
	if err != nil {
		return AcceptChannelLeaveResult{}, err
	}
	active, current, err := authorizeOwnerChannelLeave(node, authority, spec, at)
	if err != nil {
		return AcceptChannelLeaveResult{}, err
	}
	if replay, found, err := readAcceptedOwnerChannelLeave(ctx, tx, authority, active,
		spec.Request); err != nil || found {
		if err != nil {
			return AcceptChannelLeaveResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return AcceptChannelLeaveResult{}, mapChannelLeaveError(err)
		}
		return replay, nil
	}
	return s.acceptFreshOwnerChannelLeave(ctx, tx, node.PeerID(), authority,
		active, current, spec, at)
}

func validateAcceptChannelLeaveInput(s *Store, ctx context.Context,
	spec AcceptChannelLeaveSpec,
) (time.Time, error) {
	if s == nil || s.db == nil || ctx == nil || spec.Signer == nil ||
		spec.AuthenticatedPeerID.IsZero() || spec.Request.IsZero() ||
		spec.Request.Record().MemberPeerID() != spec.AuthenticatedPeerID {
		return time.Time{}, ErrChannelLeaveInput
	}
	at, err := canonicalStoreTime(spec.At)
	if err != nil || at.Before(spec.Request.Record().RequestedAt()) {
		return time.Time{}, ErrChannelLeaveInput
	}
	return at, nil
}

func authorizeOwnerChannelLeave(node model.Node, authority verifiedChannelAuthority,
	spec AcceptChannelLeaveSpec, at time.Time,
) (model.Member, model.Member, error) {
	requestRecord := spec.Request.Record()
	if node.PeerID() != authority.channel.OwnerPeerID() ||
		spec.AuthenticatedPeerID == node.PeerID() || authority.channel.Status() != model.ChannelActive ||
		at.Before(authority.channel.UpdatedAt()) {
		return model.Member{}, model.Member{}, ErrChannelLeaveConflict
	}
	members := authority.roster.Members()
	knownRevision := requestRecord.KnownRosterHead().Revision()
	if knownRevision > uint64(len(members)) ||
		members[knownRevision-1].Head() != requestRecord.KnownRosterHead() {
		return model.Member{}, model.Member{}, ErrChannelLeaveConflict
	}
	prefix, err := model.NewVerifiedRoster(authority.channel.Descriptor(), members[:knownRevision])
	if err != nil {
		return model.Member{}, model.Member{}, ErrChannelLeaveAuthority
	}
	prefixMember, ok := prefix.CurrentMember(spec.AuthenticatedPeerID)
	active, activeErr := channelLeaveHistoricalMember(authority.roster,
		requestRecord.ActiveMemberHead(), spec.AuthenticatedPeerID)
	current, currentOK := authority.roster.CurrentMember(spec.AuthenticatedPeerID)
	if !ok || prefixMember.Status() != model.MemberActive || activeErr != nil || !currentOK ||
		model.VerifyChannelLeaveRequest(authority.channel.Descriptor(), active, spec.Request) != nil {
		return model.Member{}, model.Member{}, ErrChannelLeaveInput
	}
	return active, current, nil
}

func (s *Store) acceptFreshOwnerChannelLeave(ctx context.Context, tx *sql.Tx,
	localPeerID model.PeerID, authority verifiedChannelAuthority, active, current model.Member,
	spec AcceptChannelLeaveSpec, at time.Time,
) (AcceptChannelLeaveResult, error) {
	candidate, terminal, appended, err := ownerChannelLeaveCandidate(ctx, authority, current, spec, at)
	if err != nil {
		return AcceptChannelLeaveResult{}, err
	}
	receipt, err := signOwnerChannelLeaveReceipt(ctx, authority, active, candidate,
		terminal, spec, at)
	if err != nil {
		return AcceptChannelLeaveResult{}, err
	}
	result := MergeChannelRosterResult{Status: ChannelRosterDuplicate, Channel: authority.channel,
		Roster: authority.roster, ExpectedNextRevision: authority.roster.Head().Revision() + 1}
	if appended {
		result, err = s.applyChannelRosterCandidateTx(ctx, tx, localPeerID, authority, candidate, at)
		if err != nil {
			return AcceptChannelLeaveResult{}, err
		}
	}
	if err := persistAcceptedOwnerChannelLeave(ctx, tx, spec.Request, receipt, at); err != nil {
		return AcceptChannelLeaveResult{}, err
	}
	if err := verifyAcceptedOwnerChannelLeave(ctx, tx, localPeerID, result.Roster.Head(),
		spec.Request, receipt); err != nil {
		return AcceptChannelLeaveResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return AcceptChannelLeaveResult{}, mapChannelLeaveError(err)
	}
	return AcceptChannelLeaveResult{Channel: result.Channel, Roster: result.Roster,
		ActiveMember: active, Terminal: terminal, Receipt: receipt}, nil
}

func ownerChannelLeaveCandidate(ctx context.Context, authority verifiedChannelAuthority,
	current model.Member, spec AcceptChannelLeaveSpec, at time.Time,
) (model.VerifiedRoster, model.Member, bool, error) {
	if current.Status().Terminal() {
		return authority.roster, current, false, nil
	}
	if current.Status() != model.MemberActive ||
		len(authority.roster.Members()) >= model.MaxMemberRecordsPerChannel {
		return model.VerifiedRoster{}, model.Member{}, false, ErrChannelLeaveConflict
	}
	previous := authority.roster.Head().Digest()
	record, err := model.NewMemberRecord(model.MemberRecordSpec{
		ChannelID: authority.channel.ID(), DescriptorDigest: authority.channel.Descriptor().Descriptor().Digest(),
		Revision: authority.roster.Head().Revision() + 1, PreviousDigest: &previous,
		PeerID: current.PeerID(), OriginEpoch: current.OriginEpoch(), DisplayLabel: current.DisplayLabel(),
		PublicKey: current.PublicKey(), Multiaddrs: current.Multiaddrs(), Protocols: current.Protocols(),
		Limits: current.Limits(), Status: model.MemberLeft, CreatedAt: at})
	if err != nil {
		return model.VerifiedRoster{}, model.Member{}, false, ErrChannelLeaveInput
	}
	message, _ := model.MemberRecordSigningMessage(record.ChannelID(), record.Digest())
	signature, err := spec.Signer.Sign(ctx, message)
	if err != nil {
		return model.VerifiedRoster{}, model.Member{}, false,
			fmt.Errorf("accept Channel leave: sign terminal member: %w", err)
	}
	terminal, err := model.AttachMemberSignature(record, signature)
	if err != nil || model.VerifyMember(authority.channel.Descriptor(), terminal) != nil {
		return model.VerifiedRoster{}, model.Member{}, false, ErrChannelLeaveAuthority
	}
	members := append(authority.roster.Members(), terminal)
	candidate, err := model.NewVerifiedRoster(authority.channel.Descriptor(), members)
	if err != nil {
		return model.VerifiedRoster{}, model.Member{}, false, ErrChannelLeaveAuthority
	}
	return candidate, terminal, true, nil
}

func signOwnerChannelLeaveReceipt(ctx context.Context, authority verifiedChannelAuthority,
	active model.Member, candidate model.VerifiedRoster, terminal model.Member,
	spec AcceptChannelLeaveSpec, at time.Time,
) (model.SignedChannelLeaveReceipt, error) {
	knownRevision := spec.Request.Record().KnownRosterHead().Revision()
	members := candidate.Members()
	record, err := model.NewChannelLeaveReceiptRecord(model.ChannelLeaveReceiptRecordSpec{
		ChannelID: authority.channel.ID(), MemberPeerID: spec.AuthenticatedPeerID,
		RequestDigest: spec.Request.Digest(), RosterRecords: members[knownRevision:],
		FinalRosterHead: candidate.Head(), AcceptedAt: at})
	if err != nil {
		return model.SignedChannelLeaveReceipt{}, ErrChannelLeaveInput
	}
	message, _ := model.ChannelLeaveReceiptSigningMessage(authority.channel.ID(), record.Digest())
	signature, err := spec.Signer.Sign(ctx, message)
	if err != nil {
		return model.SignedChannelLeaveReceipt{},
			fmt.Errorf("accept Channel leave: sign receipt: %w", err)
	}
	receipt, err := model.AttachChannelLeaveReceiptSignature(record, signature)
	if err != nil || model.VerifyChannelLeaveReceipt(authority.channel.Descriptor(), active,
		spec.Request, receipt) != nil || terminal.PeerID() != spec.AuthenticatedPeerID ||
		!terminal.Status().Terminal() {
		return model.SignedChannelLeaveReceipt{}, ErrChannelLeaveAuthority
	}
	return receipt, nil
}

func persistAcceptedOwnerChannelLeave(ctx context.Context, tx *sql.Tx,
	request model.SignedChannelLeaveRequest, receipt model.SignedChannelLeaveReceipt, at time.Time,
) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO channel_leave_requests(request_id,channel_id,
		member_peer_id,request_digest,request_json,member_signature,status,attempts,
		next_attempt_at,receipt_json,created_at,updated_at)
		VALUES(?,?,?,?,?,?,'accepted',0,?,?,?,?)`, request.RequestID().String(),
		request.Record().ChannelID().String(), request.Record().MemberPeerID().String(),
		request.Digest().Bytes(), request.Record().CanonicalJSON().Bytes(), request.MemberSignature(),
		storeTime(request.Record().RequestedAt()), receipt.WireJSON().Bytes(),
		storeTime(request.Record().RequestedAt()), storeTime(at))
	if err != nil {
		return mapChannelLeaveError(err)
	}
	return nil
}

func verifyAcceptedOwnerChannelLeave(ctx context.Context, tx *sql.Tx, localPeerID model.PeerID,
	expectedHead model.RecordHead, request model.SignedChannelLeaveRequest,
	receipt model.SignedChannelLeaveReceipt,
) error {
	committed, err := readVerifiedChannelAuthority(ctx, tx, localPeerID,
		request.Record().ChannelID())
	if err != nil || committed.roster.Head() != expectedHead ||
		!channelLeaveReceiptMatchesRoster(receipt, committed.roster) {
		return ErrChannelLeaveAuthority
	}
	var raw []byte
	err = tx.QueryRowContext(ctx, `SELECT receipt_json FROM channel_leave_requests WHERE request_id=?
		AND status='accepted'`, request.RequestID().String()).Scan(&raw)
	if err != nil || !bytes.Equal(raw, receipt.WireJSON().Bytes()) {
		return ErrChannelLeaveAuthority
	}
	return nil
}

func readAcceptedOwnerChannelLeave(ctx context.Context, tx *sql.Tx,
	authority verifiedChannelAuthority, active model.Member, request model.SignedChannelLeaveRequest,
) (AcceptChannelLeaveResult, bool, error) {
	var channelText, memberText, status string
	var digest, requestJSON, signature, receiptJSON []byte
	err := tx.QueryRowContext(ctx, `SELECT channel_id,member_peer_id,request_digest,request_json,
		member_signature,status,receipt_json FROM channel_leave_requests WHERE request_id=?`,
		request.RequestID().String()).Scan(&channelText, &memberText, &digest, &requestJSON,
		&signature, &status, &receiptJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return AcceptChannelLeaveResult{}, false, nil
	}
	if err != nil {
		return AcceptChannelLeaveResult{}, false, mapChannelLeaveError(err)
	}
	storedDigest, digestErr := model.DigestFromBytes(digest)
	receipt, receiptErr := model.ParseSignedChannelLeaveReceipt(receiptJSON)
	if digestErr != nil || receiptErr != nil || status != "accepted" ||
		channelText != authority.channel.ID().String() || memberText != request.Record().MemberPeerID().String() ||
		storedDigest != request.Digest() || !bytes.Equal(requestJSON, request.Record().CanonicalJSON().Bytes()) ||
		!bytes.Equal(signature, request.MemberSignature()) ||
		model.VerifyChannelLeaveReceipt(authority.channel.Descriptor(), active, request, receipt) != nil ||
		!channelLeaveReceiptMatchesRoster(receipt, authority.roster) {
		return AcceptChannelLeaveResult{}, false, ErrChannelLeaveConflict
	}
	terminal, ok := receiptTerminalMember(receipt, request.Record().MemberPeerID())
	if !ok {
		return AcceptChannelLeaveResult{}, false, ErrChannelLeaveAuthority
	}
	return AcceptChannelLeaveResult{Channel: authority.channel, Roster: authority.roster,
		ActiveMember: active, Terminal: terminal, Receipt: receipt, Replay: true}, true, nil
}

func channelLeaveReceiptMatchesRoster(receipt model.SignedChannelLeaveReceipt,
	roster model.VerifiedRoster,
) bool {
	members := roster.Members()
	for _, record := range receipt.Record().RosterRecords() {
		index := record.Head().Revision() - 1
		if index >= uint64(len(members)) ||
			!bytes.Equal(record.WireJSON().Bytes(), members[index].WireJSON().Bytes()) {
			return false
		}
	}
	return true
}

func receiptTerminalMember(receipt model.SignedChannelLeaveReceipt,
	peerID model.PeerID,
) (model.Member, bool) {
	for _, member := range receipt.Record().RosterRecords() {
		if member.PeerID() == peerID && member.Status().Terminal() {
			return member, true
		}
	}
	return model.Member{}, false
}
