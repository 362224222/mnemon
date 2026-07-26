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
	beginStoreChannelLeave(t, fixture, request)
	attemptedAt := exerciseStoreChannelLeaveRetry(t, fixture.store, request)
	settleStoreChannelLeave(t, fixture, request, attemptedAt)
}

func beginStoreChannelLeave(t *testing.T, fixture installedJoinedChannelFixture,
	request model.SignedChannelLeaveRequest,
) {
	t.Helper()
	operation := testChannelLeaveOperation("leave-lifecycle")
	result, err := fixture.store.BeginChannelLeave(context.Background(), BeginChannelLeaveSpec{
		ChannelID: fixture.spec.Descriptor.Descriptor().ID(), Request: request,
		Operation: operation, At: request.Record().RequestedAt()})
	if err != nil || result.Replay || result.Channel.Status() != model.ChannelLeaving ||
		result.Channel.TopicState() != model.TopicLeft || result.Request.RequestID() != request.RequestID() {
		t.Fatalf("BeginChannelLeave() = (%#v,%v)", result, err)
	}
	assertChannelLeaveRow(t, fixture.store, request, "queued", 0, nil)
	replayed, err := fixture.store.BeginChannelLeave(context.Background(), BeginChannelLeaveSpec{
		ChannelID: fixture.spec.Descriptor.Descriptor().ID(), Operation: operation,
		At: request.Record().RequestedAt()})
	if err != nil || !replayed.Replay ||
		!bytes.Equal(replayed.Request.WireJSON().Bytes(), request.WireJSON().Bytes()) ||
		!replayed.Channel.UpdatedAt().Equal(result.Channel.UpdatedAt()) {
		t.Fatalf("BeginChannelLeave(replay) = (%#v,%v)", replayed, err)
	}
	authority, found, err := fixture.store.ReadChannelLeaveOperation(context.Background(), operation)
	if err != nil || !found || authority.ChannelID() != request.Record().ChannelID() {
		t.Fatalf("ReadChannelLeaveOperation() = (%#v,%t,%v)", authority, found, err)
	}
	changed := operation
	changed.RequestDigest = model.Sum([]byte("changed Channel leave request"))
	if _, _, err := fixture.store.ReadChannelLeaveOperation(context.Background(), changed); !errors.Is(err, ErrChannelLeaveOperationMismatch) {
		t.Fatalf("changed Channel leave operation error = %v", err)
	}
	crossRoute := ChannelMutationOperation{Kind: ChannelMutationInvite,
		OperationKeyHash: operation.OperationKeyHash,
		RequestDigest:    model.Sum([]byte("cross-route invite request"))}
	if _, _, err := fixture.store.ReadChannelMutation(context.Background(), crossRoute); !errors.Is(err, ErrChannelMutationMismatch) {
		t.Fatalf("leave key reused for invite error = %v", err)
	}
}

func settleStoreChannelLeave(t *testing.T, fixture installedJoinedChannelFixture,
	request model.SignedChannelLeaveRequest, attemptedAt time.Time,
) {
	t.Helper()
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
		ChannelID: fixture.spec.Descriptor.Descriptor().ID(), Request: forgedRequest,
		Operation: testChannelLeaveOperation("leave-forged"),
		At:        forgedRequest.Record().RequestedAt()})
	if !errors.Is(err, ErrChannelLeaveInput) {
		t.Fatalf("forged request error = %v", err)
	}
	assertChannelLeaveProjection(t, fixture.store, fixture.spec.Descriptor.Descriptor().ID(), "active", 0)
	if _, err := fixture.store.BeginChannelLeave(context.Background(), BeginChannelLeaveSpec{
		ChannelID: fixture.spec.Descriptor.Descriptor().ID(), Request: request,
		Operation: testChannelLeaveOperation("leave-valid"),
		At:        request.Record().RequestedAt()}); err != nil {
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

func TestChannelLeaveSettlementAcceptsOwnerClosePrecedence(t *testing.T) {
	t.Parallel()
	fixture := newInstalledJoinedChannelFixture(t, "leave-owner-close",
		"leave-owner-close-team", 0x63, 0x64)
	request := signedStoreLeaveRequest(t, fixture, fixture.at.Add(time.Second))
	if _, err := fixture.store.BeginChannelLeave(context.Background(), BeginChannelLeaveSpec{
		ChannelID: fixture.spec.Descriptor.Descriptor().ID(), Request: request,
		Operation: testChannelLeaveOperation("leave-owner-close"),
		At:        request.Record().RequestedAt()}); err != nil {
		t.Fatal(err)
	}
	receipt := signedStoreOwnerCloseReceipt(t, fixture, request,
		request.Record().RequestedAt().Add(time.Second))
	settled, err := fixture.store.SettleChannelLeave(context.Background(), SettleChannelLeaveSpec{
		RequestID: request.RequestID(), Receipt: receipt,
		At: receipt.Record().AcceptedAt().Add(time.Second)})
	local, ok := settled.Roster.CurrentMember(fixture.owner.joiner.PeerID())
	if err != nil || settled.Channel.Status() != model.ChannelClosed || !ok ||
		local.Status() != model.MemberActive {
		t.Fatalf("SettleChannelLeave(owner close) = (%#v,%v)", settled, err)
	}
	assertChannelLeaveRow(t, fixture.store, request, "accepted", 0, receipt.WireJSON().Bytes())
}

func testChannelLeaveOperation(seed string) ChannelLeaveOperation {
	return ChannelLeaveOperation{
		OperationKeyHash: model.Sum([]byte("channel-leave-operation-key:" + seed)),
		RequestDigest:    model.Sum([]byte("channel-leave-request:" + seed)),
	}
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

func signedStoreOwnerCloseReceipt(t *testing.T, fixture installedJoinedChannelFixture,
	request model.SignedChannelLeaveRequest, at time.Time,
) model.SignedChannelLeaveReceipt {
	t.Helper()
	owner, ok := fixture.accepted.Roster.CurrentMember(
		fixture.spec.Descriptor.Descriptor().OwnerPeerID())
	if !ok {
		t.Fatal("joined roster has no owner")
	}
	previous := fixture.accepted.Roster.Head().Digest()
	ownerLeft, _ := signAndAppendRosterMember(t, fixture.spec.Descriptor,
		fixture.owner.signer, fixture.accepted.Roster, model.MemberRecordSpec{
			ChannelID:        fixture.spec.Descriptor.Descriptor().ID(),
			DescriptorDigest: fixture.spec.Descriptor.Descriptor().Digest(),
			Revision:         fixture.accepted.Roster.Head().Revision() + 1, PreviousDigest: &previous,
			PeerID: owner.PeerID(), OriginEpoch: owner.OriginEpoch(), DisplayLabel: owner.DisplayLabel(),
			PublicKey: owner.PublicKey(), Multiaddrs: owner.Multiaddrs(), Protocols: owner.Protocols(),
			Limits: owner.Limits(), Status: model.MemberLeft, CreatedAt: at})
	record, err := model.NewChannelLeaveReceiptRecord(model.ChannelLeaveReceiptRecordSpec{
		ChannelID: request.Record().ChannelID(), MemberPeerID: request.Record().MemberPeerID(),
		RequestDigest: request.Digest(), RosterRecords: []model.Member{ownerLeft},
		FinalRosterHead: ownerLeft.Head(), AcceptedAt: at.Add(time.Second)})
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
