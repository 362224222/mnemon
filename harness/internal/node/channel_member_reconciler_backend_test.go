package node

import (
	"context"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestDurableChannelMemberReconcileBackendTranslatesExactFences(t *testing.T) {
	target := newChannelMemberReconcilerTarget(t, "member-reconciler-backend")
	st := &recordingChannelMemberStore{baseline: store.ChannelDataBaseline{
		ChannelID: target.channel.ID(), OriginPeerID: target.localMember.PeerID(),
		OriginEpoch: target.localMember.OriginEpoch(), BaselineChannelSequence: 9}}
	controller := &recordingChannelMemberController{}
	backend := durableChannelMemberReconcileBackend{store: st, controller: controller}
	at := target.channel.UpdatedAt().Add(time.Hour)
	records := target.roster.Members()
	if err := backend.merge(context.Background(), target, records, target.roster.Head(), at); err != nil {
		t.Fatal(err)
	}
	baseline, err := backend.reserve(context.Background(), target, at)
	if err != nil || baseline != st.baseline {
		t.Fatalf("reserve = (%#v,%v)", baseline, err)
	}
	ack, err := peer.NewDataBaselineAck(peer.DataBaselineSpec{ChannelID: baseline.ChannelID,
		OriginPeerID: baseline.OriginPeerID, OriginEpoch: baseline.OriginEpoch,
		BaselineChannelSequence: baseline.BaselineChannelSequence})
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.confirm(context.Background(), target, ack, at); err != nil {
		t.Fatal(err)
	}
	if err := backend.reachability(context.Background(), target, model.ReachabilityReachable, at); err != nil {
		t.Fatal(err)
	}
	leave, receipt := newChannelMemberLeaveTarget(t, "member-reconciler-backend-leave")
	if err := backend.startLeave(context.Background(), leave, at, at.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := backend.settleLeave(context.Background(), leave, receipt, at); err != nil {
		t.Fatal(err)
	}
	if controller.hello.ChannelID != target.channel.ID() ||
		controller.hello.AuthenticatedPeerID != target.remoteMember.PeerID() ||
		controller.confirmed != target.channel.ID() ||
		st.reserve.TargetPeerID != target.remoteMember.PeerID() ||
		st.confirm.AuthenticatedPeerID != target.remoteMember.PeerID() ||
		st.confirm.Ack.BaselineChannelSequence != 9 ||
		st.reach.ExpectedRosterHead != target.roster.Head() ||
		st.reach.OriginEpoch != target.remoteMember.OriginEpoch() ||
		st.start.RequestID != leave.request.RequestID() ||
		controller.settled != leave.request.RequestID() || controller.receipt.IsZero() {
		t.Fatalf("translated controls = controller %#v Store %#v %#v %#v",
			controller.hello, st.reserve, st.confirm, st.reach)
	}
}

func TestNewChannelMemberReconcilerRejectsIncompleteComposition(t *testing.T) {
	if worker, err := NewChannelMemberReconciler(ChannelMemberReconcilerOptions{}); worker != nil || err == nil {
		t.Fatalf("incomplete worker = (%#v,%v)", worker, err)
	}
	st := &recordingChannelMemberStore{}
	controller := &recordingChannelMemberController{}
	client := &fakeChannelMemberClient{}
	worker, err := NewChannelMemberReconciler(ChannelMemberReconcilerOptions{
		Store: st, Client: client, Controller: controller})
	if err != nil || worker == nil || worker.period != channelMemberReconcileDefaultPeriod {
		t.Fatalf("complete worker = (%#v,%v)", worker, err)
	}
	targets, err := (durableChannelMemberReconcileBackend{store: st,
		controller: controller}).targets(context.Background())
	if err != nil || len(targets) != 0 {
		t.Fatalf("empty durable targets = (%#v,%v)", targets, err)
	}
}

type recordingChannelMemberStore struct {
	baseline store.ChannelDataBaseline
	reserve  store.ReserveOutboundChannelBaselineSpec
	confirm  store.ConfirmOutboundChannelBaselineSpec
	reach    store.SetPeerReachabilitySpec
	leaves   []store.ChannelLeaveTarget
	start    store.StartChannelLeaveAttemptSpec
}

func (st *recordingChannelMemberStore) ReadDueChannelLeaveTargets(context.Context,
	time.Time,
) ([]store.ChannelLeaveTarget, error) {
	return append([]store.ChannelLeaveTarget(nil), st.leaves...), nil
}

func (st *recordingChannelMemberStore) StartChannelLeaveAttempt(_ context.Context,
	spec store.StartChannelLeaveAttemptSpec,
) error {
	st.start = spec
	return nil
}

func (*recordingChannelMemberStore) ReadChannelMemberReadinessAuthority(context.Context,
) (store.ChannelMemberReadinessAuthority, error) {
	return store.ChannelMemberReadinessAuthority{}, nil
}

func (st *recordingChannelMemberStore) ReserveOutboundChannelBaseline(_ context.Context,
	spec store.ReserveOutboundChannelBaselineSpec,
) (store.ReserveOutboundChannelBaselineResult, error) {
	st.reserve = spec
	return store.ReserveOutboundChannelBaselineResult{Baseline: st.baseline}, nil
}

func (st *recordingChannelMemberStore) ConfirmOutboundChannelBaseline(_ context.Context,
	spec store.ConfirmOutboundChannelBaselineSpec,
) (store.ConfirmOutboundChannelBaselineResult, error) {
	st.confirm = spec
	return store.ConfirmOutboundChannelBaselineResult{Ack: spec.Ack}, nil
}

func (st *recordingChannelMemberStore) SetPeerReachability(_ context.Context,
	spec store.SetPeerReachabilitySpec,
) (store.SetPeerReachabilityResult, error) {
	st.reach = spec
	return store.SetPeerReachabilityResult{}, nil
}

type recordingChannelMemberController struct {
	hello     peer.ChannelMemberHelloControl
	confirmed model.ChannelID
	settled   model.ChannelLeaveRequestID
	receipt   model.SignedChannelLeaveReceipt
}

func (controller *recordingChannelMemberController) SettleMemberLeaveRuntimeGate(
	_ context.Context, requestID model.ChannelLeaveRequestID,
	receipt model.SignedChannelLeaveReceipt, _ time.Time,
) error {
	controller.settled = requestID
	controller.receipt = receipt
	return nil
}

func (controller *recordingChannelMemberController) ConfirmMemberBaselineRuntimeGate(_ context.Context,
	channelID model.ChannelID,
) error {
	controller.confirmed = channelID
	return nil
}

func (controller *recordingChannelMemberController) ReconcileMemberHelloGate(_ context.Context,
	control peer.ChannelMemberHelloControl,
) (peer.ChannelMemberHelloAuthority, error) {
	controller.hello = control
	return peer.ChannelMemberHelloAuthority{}, nil
}

func (*recordingChannelMemberController) FreezeMemberRosterForSync(context.Context,
	peer.ChannelMemberSyncControl,
) (peer.ChannelMemberRosterSnapshot, error) {
	return peer.ChannelMemberRosterSnapshot{}, nil
}

func (*recordingChannelMemberController) InstallMemberBaselineGate(context.Context,
	peer.ChannelMemberBaselineControl,
) (peer.ChannelMemberBaselineAuthority, error) {
	return peer.ChannelMemberBaselineAuthority{}, nil
}
