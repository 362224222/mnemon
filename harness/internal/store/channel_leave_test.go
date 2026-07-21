package store

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestChannelLeavePersistsLocalExitRetriesAndOwnerTerminalAtomically(t *testing.T) {
	t.Parallel()
	fixture := newInstalledJoinedChannelFixture(t, "leave-lifecycle", "leave-team", 0x51, 0x52)
	request := signedStoreLeaveRequest(t, fixture, fixture.at.Add(time.Second))
	result, err := fixture.store.BeginChannelLeave(context.Background(), BeginChannelLeaveSpec{
		ChannelID: fixture.spec.Descriptor.Descriptor().ID(), Request: request})
	if err != nil || result.Replay || result.Channel.Status() != model.ChannelLeaving ||
		result.Channel.TopicState() != model.TopicLeft || result.Request.RequestID() != request.RequestID() {
		t.Fatalf("BeginChannelLeave() = (%#v,%v)", result, err)
	}
	assertChannelLeaveRow(t, fixture.store, request, "queued", 0, nil)
	replayed, err := fixture.store.BeginChannelLeave(context.Background(), BeginChannelLeaveSpec{
		ChannelID: fixture.spec.Descriptor.Descriptor().ID()})
	if err != nil || !replayed.Replay ||
		!bytes.Equal(replayed.Request.WireJSON().Bytes(), request.WireJSON().Bytes()) ||
		!replayed.Channel.UpdatedAt().Equal(result.Channel.UpdatedAt()) {
		t.Fatalf("BeginChannelLeave(replay) = (%#v,%v)", replayed, err)
	}
	targets, err := fixture.store.ReadDueChannelLeaveTargets(context.Background(),
		request.Record().RequestedAt())
	if err != nil || len(targets) != 1 || targets[0].Request().RequestID() != request.RequestID() ||
		targets[0].Owner().PeerID() != fixture.owner.channel.Owner().PeerID() || targets[0].Attempts() != 0 {
		t.Fatalf("ReadDueChannelLeaveTargets() = (%#v,%v)", targets, err)
	}
	attemptedAt := request.Record().RequestedAt().Add(time.Second)
	retryAt := attemptedAt.Add(time.Minute)
	if err := fixture.store.StartChannelLeaveAttempt(context.Background(), StartChannelLeaveAttemptSpec{
		RequestID: request.RequestID(), ExpectedNextAttemptAt: request.Record().RequestedAt(),
		AttemptedAt: attemptedAt, RetryAt: retryAt}); err != nil {
		t.Fatal(err)
	}
	if targets, err := fixture.store.ReadDueChannelLeaveTargets(context.Background(),
		retryAt.Add(-time.Nanosecond)); err != nil || len(targets) != 0 {
		t.Fatalf("early retry targets = (%#v,%v)", targets, err)
	}
	targets, err = fixture.store.ReadDueChannelLeaveTargets(context.Background(), retryAt)
	if err != nil || len(targets) != 1 || targets[0].Attempts() != 1 ||
		!targets[0].NextAttemptAt().Equal(retryAt) {
		t.Fatalf("due retry targets = (%#v,%v)", targets, err)
	}
	receipt := signedStoreLeaveReceipt(t, fixture, request, attemptedAt.Add(time.Second))
	settledAt := receipt.Record().AcceptedAt().Add(time.Second)
	settled, err := fixture.store.SettleChannelLeave(context.Background(), SettleChannelLeaveSpec{
		RequestID: request.RequestID(), Receipt: receipt, At: settledAt})
	if err != nil || settled.Replay || settled.Channel.Status() != model.ChannelLeft ||
		settled.Channel.TopicState() != model.TopicLeft {
		t.Fatalf("SettleChannelLeave() = (%#v,%v)", settled, err)
	}
	assertChannelLeaveRow(t, fixture.store, request, "accepted", 1, receipt.WireJSON().Bytes())
	replayedSettle, err := fixture.store.SettleChannelLeave(context.Background(), SettleChannelLeaveSpec{
		RequestID: request.RequestID(), Receipt: receipt, At: settledAt.Add(time.Second)})
	if err != nil || !replayedSettle.Replay || replayedSettle.Roster.Head() != settled.Roster.Head() {
		t.Fatalf("SettleChannelLeave(replay) = (%#v,%v)", replayedSettle, err)
	}
}

func TestChannelLeaveRejectsForgedRequestAndReceiptWithoutPartialState(t *testing.T) {
	t.Parallel()
	fixture := newInstalledJoinedChannelFixture(t, "leave-rollback", "leave-rollback-team", 0x61, 0x62)
	request := signedStoreLeaveRequest(t, fixture, fixture.at.Add(time.Second))
	forgedRequest, err := model.AttachChannelLeaveRequestSignature(request.Record(),
		bytes.Repeat([]byte{0x77}, ed25519.SignatureSize))
	if err != nil {
		t.Fatal(err)
	}
	_, err = fixture.store.BeginChannelLeave(context.Background(), BeginChannelLeaveSpec{
		ChannelID: fixture.spec.Descriptor.Descriptor().ID(), Request: forgedRequest})
	if !errors.Is(err, ErrChannelLeaveInput) {
		t.Fatalf("forged request error = %v", err)
	}
	assertChannelLeaveProjection(t, fixture.store, fixture.spec.Descriptor.Descriptor().ID(), "active", 0)
	if _, err := fixture.store.BeginChannelLeave(context.Background(), BeginChannelLeaveSpec{
		ChannelID: fixture.spec.Descriptor.Descriptor().ID(), Request: request}); err != nil {
		t.Fatal(err)
	}
	receipt := signedStoreLeaveReceipt(t, fixture, request,
		request.Record().RequestedAt().Add(time.Second))
	forgedReceipt, err := model.AttachChannelLeaveReceiptSignature(receipt.Record(),
		bytes.Repeat([]byte{0x55}, ed25519.SignatureSize))
	if err != nil {
		t.Fatal(err)
	}
	_, err = fixture.store.SettleChannelLeave(context.Background(), SettleChannelLeaveSpec{
		RequestID: request.RequestID(), Receipt: forgedReceipt,
		At: receipt.Record().AcceptedAt().Add(time.Second)})
	if !errors.Is(err, ErrChannelLeaveInput) {
		t.Fatalf("forged receipt error = %v", err)
	}
	assertChannelLeaveProjection(t, fixture.store, fixture.spec.Descriptor.Descriptor().ID(), "leaving", 1)
	assertChannelLeaveRow(t, fixture.store, request, "queued", 0, nil)
}

func signedStoreLeaveRequest(t *testing.T, fixture installedJoinedChannelFixture,
	at time.Time,
) model.SignedChannelLeaveRequest {
	t.Helper()
	local, ok := fixture.accepted.Roster.CurrentMember(fixture.owner.joiner.PeerID())
	if !ok {
		t.Fatal("joined roster has no local member")
	}
	record, err := model.NewChannelLeaveRequestRecord(model.ChannelLeaveRequestRecordSpec{
		ChannelID: fixture.spec.Descriptor.Descriptor().ID(), MemberPeerID: fixture.owner.joiner.PeerID(),
		ActiveMemberHead: local.Head(), KnownRosterHead: fixture.accepted.Roster.Head(), RequestedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	message, _ := model.ChannelLeaveRequestSigningMessage(record.ChannelID(), record.Digest())
	request, err := model.AttachChannelLeaveRequestSignature(record,
		ed25519.Sign(ed25519Private(fixture.owner.joiner), message))
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func signedStoreLeaveReceipt(t *testing.T, fixture installedJoinedChannelFixture,
	request model.SignedChannelLeaveRequest, at time.Time,
) model.SignedChannelLeaveReceipt {
	t.Helper()
	local, ok := fixture.accepted.Roster.CurrentMember(fixture.owner.joiner.PeerID())
	if !ok {
		t.Fatal("joined roster has no local member")
	}
	previous := fixture.accepted.Roster.Head().Digest()
	terminal, _ := signAndAppendRosterMember(t, fixture.spec.Descriptor,
		fixture.owner.signer, fixture.accepted.Roster, model.MemberRecordSpec{
			ChannelID:        fixture.spec.Descriptor.Descriptor().ID(),
			DescriptorDigest: fixture.spec.Descriptor.Descriptor().Digest(),
			Revision:         fixture.accepted.Roster.Head().Revision() + 1, PreviousDigest: &previous,
			PeerID: local.PeerID(), OriginEpoch: local.OriginEpoch(), DisplayLabel: local.DisplayLabel(),
			PublicKey: local.PublicKey(), Multiaddrs: local.Multiaddrs(), Protocols: local.Protocols(),
			Limits: local.Limits(), Status: model.MemberLeft, CreatedAt: at})
	record, err := model.NewChannelLeaveReceiptRecord(model.ChannelLeaveReceiptRecordSpec{
		ChannelID: request.Record().ChannelID(), MemberPeerID: request.Record().MemberPeerID(),
		RequestDigest: request.Digest(), RosterRecords: []model.Member{terminal},
		FinalRosterHead: terminal.Head(), AcceptedAt: at.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	message, _ := model.ChannelLeaveReceiptSigningMessage(record.ChannelID(), record.Digest())
	receipt, err := model.AttachChannelLeaveReceiptSignature(record,
		ed25519.Sign(ed25519Private(fixture.owner.channel.Owner()), message))
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

func assertChannelLeaveRow(t *testing.T, st *Store, request model.SignedChannelLeaveRequest,
	wantStatus string, wantAttempts uint64, wantReceipt []byte,
) {
	t.Helper()
	var status string
	var attempts uint64
	var requestJSON, signature, receipt []byte
	if err := st.db.QueryRow(`SELECT status,attempts,request_json,member_signature,receipt_json
		FROM channel_leave_requests WHERE request_id=?`, request.RequestID().String()).Scan(
		&status, &attempts, &requestJSON, &signature, &receipt); err != nil {
		t.Fatal(err)
	}
	if status != wantStatus || attempts != wantAttempts ||
		!bytes.Equal(requestJSON, request.Record().CanonicalJSON().Bytes()) ||
		!bytes.Equal(signature, request.MemberSignature()) || !bytes.Equal(receipt, wantReceipt) {
		t.Fatalf("leave row = status %q attempts %d request=%d signature=%d receipt=%d",
			status, attempts, len(requestJSON), len(signature), len(receipt))
	}
}

func assertChannelLeaveProjection(t *testing.T, st *Store, channelID model.ChannelID,
	wantStatus string, wantRequests int,
) {
	t.Helper()
	var status string
	var requests int
	if err := st.db.QueryRow(`SELECT status FROM channels WHERE channel_id=?`,
		channelID.String()).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM channel_leave_requests WHERE channel_id=?`,
		channelID.String()).Scan(&requests); err != nil {
		t.Fatal(err)
	}
	if status != wantStatus || requests != wantRequests {
		t.Fatalf("leave projection = status %q requests %d, want %q/%d",
			status, requests, wantStatus, wantRequests)
	}
}
