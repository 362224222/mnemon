package node

import (
	"context"
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestChannelMemberReconcilerSettlesDurableLeaveInExistingSerialWorker(t *testing.T) {
	t.Parallel()
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

func TestChannelMemberReconcilerTerminalizesPermanentAndExhaustedLeaveAttempts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		attempts     uint64
		clientError  error
		wantStarts   int
		wantRequests int
		wantAttempts uint64
		wantFailure  store.ChannelLeaveFailureCode
	}{
		{name: "permanent response", clientError: peer.ErrChannelMemberClientResponse,
			wantStarts: 1, wantRequests: 1, wantAttempts: 1,
			wantFailure: store.ChannelLeaveFailurePermanent},
		{name: "fifth transient response", attempts: store.ChannelLeaveMaximumAttempts - 1,
			clientError: peer.ErrChannelMemberClientTransport,
			wantStarts:  1, wantRequests: 1, wantAttempts: store.ChannelLeaveMaximumAttempts,
			wantFailure: store.ChannelLeaveFailureAttemptsExhausted},
		{name: "exhausted after crash", attempts: store.ChannelLeaveMaximumAttempts,
			wantAttempts: store.ChannelLeaveMaximumAttempts,
			wantFailure:  store.ChannelLeaveFailureAttemptsExhausted},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			target, _ := newChannelMemberLeaveTarget(t, "member-reconciler-leave-"+test.name)
			target.attempts = test.attempts
			backend := &fakeChannelMemberBackend{leaveTarget: target, hasLeave: true}
			client := &fakeChannelMemberClient{leaveError: test.clientError}
			clock := &mutableChannelMemberClock{at: target.nextAttemptAt}
			worker, err := newChannelMemberReconciler(backend, client, clock, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if err := worker.runCycle(context.Background(), false); err != nil {
				t.Fatal(err)
			}
			backend.mu.Lock()
			starts, failures := backend.leaveStarts, backend.leaveFails
			failedAttempts, failure := backend.leaveFailedAttempts, backend.leaveFailure
			backend.mu.Unlock()
			client.mu.Lock()
			requests := client.leaves
			client.mu.Unlock()
			if starts != test.wantStarts || requests != test.wantRequests || failures != 1 ||
				failedAttempts != test.wantAttempts || failure != test.wantFailure {
				t.Fatalf("terminal leave = start %d request %d fail %d attempt %d code %q",
					starts, requests, failures, failedAttempts, failure)
			}
			snapshot := worker.Snapshot()
			if snapshot.PermanentFailures != 1 || snapshot.RetryableFailures != 0 ||
				snapshot.LastFailure != string(test.wantFailure) {
				t.Fatalf("terminal leave snapshot = %#v", snapshot)
			}
		})
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
