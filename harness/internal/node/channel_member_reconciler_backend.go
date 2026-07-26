package node

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

const channelMemberReconcileDefaultPeriod = 5 * time.Second

var (
	ErrChannelMemberReconciler        = errors.New("Channel member reconciler")
	ErrChannelMemberReconcilerRunning = fmt.Errorf("%w: worker has already run", ErrChannelMemberReconciler)
)

type ChannelMemberReconcilerStore interface {
	ReadChannelMemberReadinessAuthority(context.Context) (store.ChannelMemberReadinessAuthority, error)
	ReadDueChannelLeaveTargets(context.Context, time.Time) ([]store.ChannelLeaveTarget, error)
	StartChannelLeaveAttempt(context.Context, store.StartChannelLeaveAttemptSpec) error
	FailChannelLeaveAttempt(context.Context, store.FailChannelLeaveAttemptSpec) error
	ReserveOutboundChannelBaseline(context.Context, store.ReserveOutboundChannelBaselineSpec) (store.ReserveOutboundChannelBaselineResult, error)
	ConfirmOutboundChannelBaseline(context.Context, store.ConfirmOutboundChannelBaselineSpec) (store.ConfirmOutboundChannelBaselineResult, error)
	SetPeerReachability(context.Context, store.SetPeerReachabilitySpec) (store.SetPeerReachabilityResult, error)
}

type ChannelMemberControlClient interface {
	Hello(context.Context, model.PeerID, []byte, peer.MemberHello) (peer.MemberHelloAck, error)
	Sync(context.Context, model.PeerID, []byte, peer.SyncRequest) ([]peer.SyncPage, error)
	InstallBaseline(context.Context, model.PeerID, []byte, peer.DataBaseline) (peer.DataBaselineAck, error)
	Leave(context.Context, model.PeerID, []byte, peer.LeaveRequest) (peer.LeaveReceipt, error)
}

type ChannelMemberReconcilerClock interface{ Now() time.Time }

type ChannelMemberReconcilerOptions struct {
	Store      ChannelMemberReconcilerStore
	Client     ChannelMemberControlClient
	Controller ChannelMemberReconcilerController
	Clock      ChannelMemberReconcilerClock
	Period     time.Duration
}

type ChannelMemberReconcilerController interface {
	peer.ChannelMemberController
	ConfirmMemberBaselineRuntimeGate(context.Context, model.ChannelID) error
	SettleMemberLeaveRuntimeGate(context.Context, model.ChannelLeaveRequestID,
		model.SignedChannelLeaveReceipt, time.Time) error
}

type channelMemberTarget struct {
	channel       model.Channel
	roster        model.VerifiedRoster
	localMember   model.Member
	remoteMember  model.Member
	binding       model.PeerBinding
	outboundReady bool
}

func (target channelMemberTarget) key() channelMemberTargetKey {
	return channelMemberTargetKey{channelID: target.channel.ID(), peerID: target.remoteMember.PeerID()}
}

type channelMemberTargetKey struct {
	channelID model.ChannelID
	peerID    model.PeerID
}

type channelMemberLeaveTarget struct {
	channel         model.Channel
	roster          model.VerifiedRoster
	request         model.SignedChannelLeaveRequest
	owner           model.Member
	attempts        uint64
	retryGeneration uint64
	nextAttemptAt   time.Time
}

type channelMemberReconcileBackend interface {
	targets(context.Context) ([]channelMemberTarget, error)
	leaveTargets(context.Context, time.Time) ([]channelMemberLeaveTarget, error)
	startLeave(context.Context, channelMemberLeaveTarget, time.Time, time.Time) error
	failLeave(context.Context, channelMemberLeaveTarget, uint64, time.Time,
		store.ChannelLeaveFailureCode, time.Time) error
	settleLeave(context.Context, channelMemberLeaveTarget,
		model.SignedChannelLeaveReceipt, time.Time) error
	merge(context.Context, channelMemberTarget, []model.Member, model.RecordHead, time.Time) error
	reserve(context.Context, channelMemberTarget, time.Time) (store.ChannelDataBaseline, error)
	confirm(context.Context, channelMemberTarget, peer.DataBaselineAck, time.Time) error
	reachability(context.Context, channelMemberTarget, model.Reachability, time.Time) error
}

type durableChannelMemberReconcileBackend struct {
	store      ChannelMemberReconcilerStore
	controller ChannelMemberReconcilerController
}

type wallChannelMemberReconcilerClock struct{}

func (wallChannelMemberReconcilerClock) Now() time.Time { return time.Now() }

func NewChannelMemberReconciler(options ChannelMemberReconcilerOptions) (*ChannelMemberReconciler, error) {
	if options.Store == nil || options.Client == nil || options.Controller == nil {
		return nil, fmt.Errorf("%w: Store, client and runtime controller are required",
			ErrChannelMemberReconciler)
	}
	if options.Clock == nil {
		options.Clock = wallChannelMemberReconcilerClock{}
	}
	if options.Period == 0 {
		options.Period = channelMemberReconcileDefaultPeriod
	}
	backend := durableChannelMemberReconcileBackend{store: options.Store,
		controller: options.Controller}
	return newChannelMemberReconciler(backend, options.Client, options.Clock, options.Period)
}

func (backend durableChannelMemberReconcileBackend) targets(ctx context.Context) ([]channelMemberTarget, error) {
	authority, err := backend.store.ReadChannelMemberReadinessAuthority(ctx)
	if err != nil {
		return nil, err
	}
	mesh := authority.MeshAuthority()
	result := make([]channelMemberTarget, 0, model.MaxChannelsPerNode*(model.MaxMembersPerChannel-1))
	for _, durable := range mesh.Channels() {
		channelTargets, err := backend.channelTargets(mesh.LocalPeerID(), durable,
			authority.Readiness(durable.Channel().ID()))
		if err != nil {
			return nil, err
		}
		result = append(result, channelTargets...)
	}
	return result, nil
}

func (backend durableChannelMemberReconcileBackend) leaveTargets(ctx context.Context,
	at time.Time,
) ([]channelMemberLeaveTarget, error) {
	durable, err := backend.store.ReadDueChannelLeaveTargets(ctx, at)
	if err != nil {
		return nil, err
	}
	result := make([]channelMemberLeaveTarget, len(durable))
	for index, target := range durable {
		result[index] = channelMemberLeaveTarget{channel: target.Channel(), roster: target.Roster(),
			request: target.Request(), owner: target.Owner(), attempts: target.Attempts(),
			retryGeneration: target.RetryGeneration(),
			nextAttemptAt:   target.NextAttemptAt()}
	}
	return result, nil
}

func (backend durableChannelMemberReconcileBackend) startLeave(ctx context.Context,
	target channelMemberLeaveTarget, attemptedAt, retryAt time.Time,
) error {
	return backend.store.StartChannelLeaveAttempt(ctx, store.StartChannelLeaveAttemptSpec{
		RequestID: target.request.RequestID(), ExpectedGeneration: target.retryGeneration,
		ExpectedAttempts:      target.attempts,
		ExpectedNextAttemptAt: target.nextAttemptAt, AttemptedAt: attemptedAt, RetryAt: retryAt})
}

func (backend durableChannelMemberReconcileBackend) failLeave(ctx context.Context,
	target channelMemberLeaveTarget, attempts uint64, nextAttemptAt time.Time,
	failure store.ChannelLeaveFailureCode, at time.Time,
) error {
	return backend.store.FailChannelLeaveAttempt(ctx, store.FailChannelLeaveAttemptSpec{
		RequestID: target.request.RequestID(), ExpectedGeneration: target.retryGeneration,
		ExpectedAttempts:      attempts,
		ExpectedNextAttemptAt: nextAttemptAt, Failure: failure, FailedAt: at})
}

func (backend durableChannelMemberReconcileBackend) settleLeave(ctx context.Context,
	target channelMemberLeaveTarget, receipt model.SignedChannelLeaveReceipt, at time.Time,
) error {
	return backend.controller.SettleMemberLeaveRuntimeGate(ctx, target.request.RequestID(), receipt, at)
}

func (backend durableChannelMemberReconcileBackend) channelTargets(localPeerID model.PeerID,
	durable store.ChannelMeshChannel, readiness []store.ChannelPeerReadiness,
) ([]channelMemberTarget, error) {
	channel, roster := durable.Channel(), durable.Roster()
	if channel.Status() != model.ChannelActive {
		return nil, nil
	}
	local, ok := roster.CurrentMember(localPeerID)
	if !ok || local.Status() != model.MemberActive {
		return nil, fmt.Errorf("%w: active Channel lacks local member authority",
			ErrChannelMemberReconciler)
	}
	outbound, err := projectChannelMemberOutboundReadiness(channel, roster, readiness)
	if err != nil {
		return nil, err
	}
	result := make([]channelMemberTarget, 0, len(durable.Bindings()))
	for _, binding := range durable.Bindings() {
		remote, ok := roster.CurrentMember(binding.PeerID())
		if !ok || remote.Status() != model.MemberActive || binding.State() == model.BindingRevoked {
			return nil, fmt.Errorf("%w: live binding lacks active signed authority",
				ErrChannelMemberReconciler)
		}
		ready, exists := outbound[binding.PeerID()]
		if !exists {
			return nil, fmt.Errorf("%w: live binding lacks baseline projection",
				ErrChannelMemberReconciler)
		}
		result = append(result, channelMemberTarget{channel: channel, roster: roster,
			localMember: local, remoteMember: remote, binding: binding, outboundReady: ready})
	}
	return result, nil
}

func projectChannelMemberOutboundReadiness(channel model.Channel, roster model.VerifiedRoster,
	readiness []store.ChannelPeerReadiness,
) (map[model.PeerID]bool, error) {
	outbound := make(map[model.PeerID]bool, len(readiness))
	for _, peerReadiness := range readiness {
		if peerReadiness.ChannelID != channel.ID() || peerReadiness.PeerID.IsZero() ||
			peerReadiness.RosterHead != roster.Head() {
			return nil, fmt.Errorf("%w: baseline readiness changed authority",
				ErrChannelMemberReconciler)
		}
		if _, duplicate := outbound[peerReadiness.PeerID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate baseline readiness", ErrChannelMemberReconciler)
		}
		outbound[peerReadiness.PeerID] = peerReadiness.OutboundReady
	}
	return outbound, nil
}

func (backend durableChannelMemberReconcileBackend) merge(ctx context.Context,
	target channelMemberTarget, records []model.Member, head model.RecordHead, at time.Time,
) error {
	if len(records) == 0 {
		return nil
	}
	_, err := backend.controller.ReconcileMemberHelloGate(ctx, peer.ChannelMemberHelloControl{
		AuthenticatedPeerID: target.remoteMember.PeerID(), ChannelID: target.channel.ID(),
		ActiveMemberRecord: target.remoteMember, KnownRosterHead: head,
		ProofRecords: records, At: at})
	return err
}

func (backend durableChannelMemberReconcileBackend) reserve(ctx context.Context,
	target channelMemberTarget, at time.Time,
) (store.ChannelDataBaseline, error) {
	result, err := backend.store.ReserveOutboundChannelBaseline(ctx,
		store.ReserveOutboundChannelBaselineSpec{ChannelID: target.channel.ID(),
			TargetPeerID: target.remoteMember.PeerID(), At: at})
	return result.Baseline, err
}

func (backend durableChannelMemberReconcileBackend) confirm(ctx context.Context,
	target channelMemberTarget, ack peer.DataBaselineAck, at time.Time,
) error {
	_, err := backend.store.ConfirmOutboundChannelBaseline(ctx,
		store.ConfirmOutboundChannelBaselineSpec{AuthenticatedPeerID: target.remoteMember.PeerID(),
			Ack: store.ChannelDataBaselineAck{ChannelID: ack.ChannelID(),
				OriginPeerID: ack.OriginPeerID(), OriginEpoch: ack.OriginEpoch(),
				BaselineChannelSequence: ack.BaselineChannelSequence()}, At: at})
	if err != nil {
		return err
	}
	return backend.controller.ConfirmMemberBaselineRuntimeGate(ctx, target.channel.ID())
}

func (backend durableChannelMemberReconcileBackend) reachability(ctx context.Context,
	target channelMemberTarget, reachability model.Reachability, at time.Time,
) error {
	_, err := backend.store.SetPeerReachability(ctx, store.SetPeerReachabilitySpec{
		ChannelID: target.channel.ID(), PeerID: target.remoteMember.PeerID(),
		OriginEpoch: target.remoteMember.OriginEpoch(), ExpectedRosterHead: target.roster.Head(),
		Reachability: reachability, At: at})
	return err
}

var _ ChannelMemberReconcilerStore = (*store.Store)(nil)
var _ ChannelMemberControlClient = (*peer.ChannelMemberClient)(nil)
