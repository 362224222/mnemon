package node

import (
	"context"
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestChannelMemberReconcilerSettlesDurableLeaveInExistingSerialWorker(t *testing.T) {
	target, receipt := newChannelMemberLeaveTarget(t, "member-reconciler-leave")
	response, err := peer.NewLeaveReceipt(receipt)
	if err != nil {
		t.Fatal(err)
	}
	backend := &fakeChannelMemberBackend{leaveTarget: target, hasLeave: true}
	client := &fakeChannelMemberClient{leaveResponse: response}
	clock := &mutableChannelMemberClock{at: target.nextAttemptAt}
	worker, err := newChannelMemberReconciler(backend, client, clock, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.runCycle(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	starts, settlements := backend.leaveStarts, backend.leaveSettle
	backend.mu.Unlock()
	client.mu.Lock()
	leaves := client.leaves
	client.mu.Unlock()
	snapshot := worker.Snapshot()
	if starts != 1 || leaves != 1 || settlements != 1 || snapshot.LeaveRequests != 1 ||
		snapshot.LeaveSettlements != 1 || snapshot.Hellos != 0 ||
		snapshot.MaximumInFlight != 1 || snapshot.InFlight != 0 {
		t.Fatalf("leave cycle = start %d wire %d settle %d snapshot %#v",
			starts, leaves, settlements, snapshot)
	}
}

func newChannelMemberLeaveTarget(t *testing.T,
	seed string,
) (channelMemberLeaveTarget, model.SignedChannelLeaveReceipt) {
	t.Helper()
	created := time.Date(2026, 7, 21, 4, 0, 0, 0, time.UTC)
	fixture := testkit.NewSignedChannelAt(t, seed, created)
	localIdentity := testkit.NewIdentity(t, seed+"-local")
	local := fixture.AppendActiveIdentity(t, localIdentity).Member()
	requestAt := fixture.Channel().UpdatedAt().Add(time.Second)
	requestRecord, err := model.NewChannelLeaveRequestRecord(model.ChannelLeaveRequestRecordSpec{
		ChannelID: fixture.Channel().ID(), MemberPeerID: local.PeerID(),
		ActiveMemberHead: local.Head(), KnownRosterHead: fixture.Roster().Head(), RequestedAt: requestAt})
	if err != nil {
		t.Fatal(err)
	}
	requestMessage, _ := model.ChannelLeaveRequestSigningMessage(fixture.Channel().ID(),
		requestRecord.Digest())
	request, err := model.AttachChannelLeaveRequestSignature(requestRecord,
		ed25519.Sign(channelMemberTestPrivateKey(t, localIdentity), requestMessage))
	if err != nil {
		t.Fatal(err)
	}
	leaving, err := model.NewChannel(model.ChannelSpec{Descriptor: fixture.Descriptor(),
		LocalAlias: fixture.Channel().LocalAlias(), RosterHead: fixture.Roster().Head(),
		Status: model.ChannelLeaving, TopicState: model.TopicLeft, UpdatedAt: requestAt})
	if err != nil {
		t.Fatal(err)
	}
	owner, _ := fixture.Roster().CurrentMember(fixture.Owner().PeerID())
	preLeaveRoster := fixture.Roster()
	terminal := fixture.AppendTerminal(t, local.PeerID(), model.MemberLeft).Member()
	receiptRecord, err := model.NewChannelLeaveReceiptRecord(model.ChannelLeaveReceiptRecordSpec{
		ChannelID: leaving.ID(), MemberPeerID: local.PeerID(), RequestDigest: request.Digest(),
		RosterRecords: []model.Member{terminal}, FinalRosterHead: terminal.Head(), AcceptedAt: requestAt})
	if err != nil {
		t.Fatal(err)
	}
	receiptMessage, _ := model.ChannelLeaveReceiptSigningMessage(leaving.ID(), receiptRecord.Digest())
	receipt, err := model.AttachChannelLeaveReceiptSignature(receiptRecord,
		ed25519.Sign(channelMemberTestPrivateKey(t, fixture.Owner()), receiptMessage))
	if err != nil {
		t.Fatal(err)
	}
	return channelMemberLeaveTarget{channel: leaving, roster: preLeaveRoster, request: request,
		owner: owner, nextAttemptAt: requestAt}, receipt
}

func channelMemberTestPrivateKey(t *testing.T, identity testkit.Identity) ed25519.PrivateKey {
	t.Helper()
	privateKey, err := identity.Libp2pPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := privateKey.Raw()
	if err != nil {
		t.Fatal(err)
	}
	return ed25519.PrivateKey(raw)
}
