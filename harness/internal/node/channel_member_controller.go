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

const channelAuthorityResolveTimeout = 10 * time.Second

var ErrChannelMemberAuthority = errors.New("mnemond Channel member authority")

type channelMemberAuthorityStore interface {
	ReadChannelMeshAuthority(context.Context) (store.ChannelMeshAuthority, error)
	PrepareChannelRosterMerge(context.Context, store.MergeChannelRosterSpec) (store.ChannelRosterMergePlan, error)
	CommitChannelRosterMerge(context.Context, store.ChannelRosterMergePlan) (store.MergeChannelRosterResult, error)
	ResolveChannelRosterMerge(context.Context, store.ChannelRosterMergePlan) (store.ChannelAuthorityPlanResolution, error)
	PrepareInboundChannelBaseline(context.Context, store.InstallInboundChannelBaselineSpec) (store.InboundChannelBaselinePlan, error)
	CommitInboundChannelBaseline(context.Context, store.InboundChannelBaselinePlan) (store.InstallInboundChannelBaselineResult, error)
	ResolveInboundChannelBaseline(context.Context, store.InboundChannelBaselinePlan) (store.ChannelAuthorityPlanResolution, error)
}

type channelMemberAuthorityTransition interface {
	Install() error
	Abort() error
	FailClosed(error) error
}

type channelMemberAuthorityRuntime interface {
	begin(store.ChannelMeshAuthority) (channelMemberAuthorityTransition, error)
}

type meshChannelMemberAuthorityRuntime struct{ runtime *peer.MeshRuntime }

func (runtime meshChannelMemberAuthorityRuntime) begin(
	candidate store.ChannelMeshAuthority,
) (channelMemberAuthorityTransition, error) {
	return runtime.runtime.BeginAuthorityTransition(candidate)
}

// ChannelMemberController is the production prepare -> runtime drain -> Store
// CAS -> runtime install boundary for /mnemon/channel/1 member traffic. Its
// capacity-one owner is the boundary production composition must reuse for
// every Channel authority mutation; Store remains the only durable source of truth.
type ChannelMemberController struct {
	store   channelMemberAuthorityStore
	runtime channelMemberAuthorityRuntime
	token   chan struct{}
}

var _ peer.ChannelMemberController = (*ChannelMemberController)(nil)

func NewChannelMemberController(st *store.Store,
	runtime *peer.MeshRuntime,
) (*ChannelMemberController, error) {
	if st == nil || runtime == nil {
		return nil, fmt.Errorf("%w: Store and mesh runtime are required", ErrChannelMemberAuthority)
	}
	return newChannelMemberController(st, meshChannelMemberAuthorityRuntime{runtime: runtime})
}

func newChannelMemberController(st channelMemberAuthorityStore,
	runtime channelMemberAuthorityRuntime,
) (*ChannelMemberController, error) {
	if st == nil || runtime == nil {
		return nil, fmt.Errorf("%w: Store and mesh runtime are required", ErrChannelMemberAuthority)
	}
	token := make(chan struct{}, 1)
	token <- struct{}{}
	return &ChannelMemberController{store: st, runtime: runtime, token: token}, nil
}

func (controller *ChannelMemberController) ReconcileMemberHelloGate(ctx context.Context,
	control peer.ChannelMemberHelloControl,
) (peer.ChannelMemberHelloAuthority, error) {
	release, err := controller.acquire(ctx)
	if err != nil {
		return peer.ChannelMemberHelloAuthority{}, err
	}
	defer release()
	if len(control.ProofRecords) == 0 {
		roster, readErr := controller.readRoster(ctx, control.ChannelID)
		return peer.ChannelMemberHelloAuthority{Roster: roster}, readErr
	}
	plan, err := controller.store.PrepareChannelRosterMerge(ctx, store.MergeChannelRosterSpec{
		ChannelID: control.ChannelID, AuthenticatedTransportPeerID: control.AuthenticatedPeerID,
		Records: control.ProofRecords, At: control.At,
	})
	if err != nil {
		return peer.ChannelMemberHelloAuthority{}, mapChannelMemberAuthorityError(err)
	}
	result, err := executeChannelAuthorityPlan(ctx, controller.runtime,
		channelAuthorityPlanSteps[store.MergeChannelRosterResult]{
			candidate: plan.Candidate(), changes: plan.ChangesAuthority(), expected: plan.Result(),
			commit: func(commitCtx context.Context) (store.MergeChannelRosterResult, error) {
				return controller.store.CommitChannelRosterMerge(commitCtx, plan)
			},
			resolve: func(resolveCtx context.Context) (store.ChannelAuthorityPlanResolution, error) {
				return controller.store.ResolveChannelRosterMerge(resolveCtx, plan)
			},
		})
	if err != nil {
		return peer.ChannelMemberHelloAuthority{}, mapChannelMemberAuthorityError(err)
	}
	switch result.Status {
	case store.ChannelRosterApplied, store.ChannelRosterDuplicate:
		return peer.ChannelMemberHelloAuthority{Roster: result.Roster}, nil
	case store.ChannelRosterGap:
		return peer.ChannelMemberHelloAuthority{}, peer.ErrChannelMemberRosterGap
	case store.ChannelRosterConflicted:
		return peer.ChannelMemberHelloAuthority{}, peer.ErrChannelMemberRosterConflict
	default:
		return peer.ChannelMemberHelloAuthority{}, fmt.Errorf(
			"%w: Store returned unknown roster result", ErrChannelMemberAuthority)
	}
}

func (controller *ChannelMemberController) FreezeMemberRosterForSync(ctx context.Context,
	control peer.ChannelMemberSyncControl,
) (peer.ChannelMemberRosterSnapshot, error) {
	release, err := controller.acquire(ctx)
	if err != nil {
		return peer.ChannelMemberRosterSnapshot{}, err
	}
	defer release()
	roster, err := controller.readRoster(ctx, control.ChannelID)
	return peer.ChannelMemberRosterSnapshot{Roster: roster}, err
}

func (controller *ChannelMemberController) InstallMemberBaselineGate(ctx context.Context,
	control peer.ChannelMemberBaselineControl,
) (peer.ChannelMemberBaselineAuthority, error) {
	release, err := controller.acquire(ctx)
	if err != nil {
		return peer.ChannelMemberBaselineAuthority{}, err
	}
	defer release()
	baseline := store.ChannelDataBaseline{ChannelID: control.Baseline.ChannelID,
		OriginPeerID: control.Baseline.OriginPeerID, OriginEpoch: control.Baseline.OriginEpoch,
		BaselineChannelSequence: control.Baseline.BaselineChannelSequence}
	plan, err := controller.store.PrepareInboundChannelBaseline(ctx,
		store.InstallInboundChannelBaselineSpec{AuthenticatedPeerID: control.AuthenticatedPeerID,
			Baseline: baseline, At: control.At})
	if err != nil {
		return peer.ChannelMemberBaselineAuthority{}, mapChannelMemberAuthorityError(err)
	}
	result, err := executeChannelAuthorityPlan(ctx, controller.runtime,
		channelAuthorityPlanSteps[store.InstallInboundChannelBaselineResult]{
			candidate: plan.Candidate(), changes: plan.ChangesAuthority(), expected: plan.Result(),
			commit: func(commitCtx context.Context) (store.InstallInboundChannelBaselineResult, error) {
				return controller.store.CommitInboundChannelBaseline(commitCtx, plan)
			},
			resolve: func(resolveCtx context.Context) (store.ChannelAuthorityPlanResolution, error) {
				return controller.store.ResolveInboundChannelBaseline(resolveCtx, plan)
			},
		})
	if err != nil {
		return peer.ChannelMemberBaselineAuthority{}, mapChannelMemberAuthorityError(err)
	}
	roster, err := rosterFromChannelMesh(plan.Candidate(), result.Baseline.ChannelID)
	if err != nil {
		return peer.ChannelMemberBaselineAuthority{}, err
	}
	committed := peer.DataBaselineSpec{ChannelID: result.Baseline.ChannelID,
		OriginPeerID: result.Baseline.OriginPeerID, OriginEpoch: result.Baseline.OriginEpoch,
		BaselineChannelSequence: result.Baseline.BaselineChannelSequence}
	return peer.ChannelMemberBaselineAuthority{Baseline: committed, Roster: roster}, nil
}

func (controller *ChannelMemberController) acquire(ctx context.Context) (func(), error) {
	if controller == nil || controller.store == nil || controller.runtime == nil ||
		controller.token == nil || ctx == nil {
		return nil, fmt.Errorf("%w: controller is unavailable", ErrChannelMemberAuthority)
	}
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: mutation wait: %w", ErrChannelMemberAuthority, ctx.Err())
	case <-controller.token:
		return func() { controller.token <- struct{}{} }, nil
	}
}

func (controller *ChannelMemberController) readRoster(ctx context.Context,
	channelID model.ChannelID,
) (model.VerifiedRoster, error) {
	mesh, err := controller.store.ReadChannelMeshAuthority(ctx)
	if err != nil {
		return model.VerifiedRoster{}, fmt.Errorf("%w: read mesh: %w", ErrChannelMemberAuthority, err)
	}
	return rosterFromChannelMesh(mesh, channelID)
}

func rosterFromChannelMesh(mesh store.ChannelMeshAuthority,
	channelID model.ChannelID,
) (model.VerifiedRoster, error) {
	for _, channel := range mesh.Channels() {
		if channel.Channel().ID() == channelID {
			return channel.Roster(), nil
		}
	}
	return model.VerifiedRoster{}, peer.ErrChannelMemberNotMember
}

type channelAuthorityPlanSteps[T any] struct {
	candidate store.ChannelMeshAuthority
	changes   bool
	expected  T
	commit    func(context.Context) (T, error)
	resolve   func(context.Context) (store.ChannelAuthorityPlanResolution, error)
}

func executeChannelAuthorityPlan[T any](ctx context.Context,
	runtime channelMemberAuthorityRuntime, steps channelAuthorityPlanSteps[T],
) (T, error) {
	var zero T
	if !steps.changes {
		// A runtime-equivalent plan may still carry durable, non-runtime
		// evidence (for example, another signed challenger for an already
		// conflicted Channel). Commit remains the only authority for deciding
		// whether that evidence is a replay or a fresh write.
		committed, commitErr := steps.commit(ctx)
		if commitErr == nil {
			return committed, nil
		}
		// A lost response still has an unknown durable outcome. Acquire and
		// drain the unchanged runtime generation before resolving so an
		// unreadable or diverged Store can close the same fail-closed boundary.
		transition, beginErr := runtime.begin(steps.candidate)
		if beginErr != nil {
			return zero, errors.Join(commitErr, fmt.Errorf(
				"%w: prepare runtime transition after unknown commit: %w",
				ErrChannelMemberAuthority, beginErr))
		}
		return resolveChannelAuthorityCommit(ctx, transition, steps, commitErr)
	}
	transition, err := runtime.begin(steps.candidate)
	if err != nil {
		return zero, fmt.Errorf("%w: prepare runtime transition: %w", ErrChannelMemberAuthority, err)
	}
	committed, commitErr := steps.commit(ctx)
	if commitErr == nil {
		if err := transition.Install(); err != nil {
			return zero, fmt.Errorf("%w: install committed authority: %w", ErrChannelMemberAuthority, err)
		}
		return committed, nil
	}
	return resolveChannelAuthorityCommit(ctx, transition, steps, commitErr)
}

func resolveChannelAuthorityCommit[T any](ctx context.Context,
	transition channelMemberAuthorityTransition, steps channelAuthorityPlanSteps[T],
	commitErr error,
) (T, error) {
	var zero T
	resolveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), channelAuthorityResolveTimeout)
	resolution, resolveErr := steps.resolve(resolveCtx)
	cancel()
	switch {
	case resolveErr == nil && resolution == store.ChannelAuthorityPlanUnchanged:
		if abortErr := transition.Abort(); abortErr != nil {
			return zero, errors.Join(commitErr, fmt.Errorf(
				"%w: abort uncommitted authority: %w", ErrChannelMemberAuthority, abortErr))
		}
		return zero, commitErr
	case resolveErr == nil && resolution == store.ChannelAuthorityPlanCandidate:
		if installErr := transition.Install(); installErr != nil {
			return zero, fmt.Errorf("%w: install resolved authority: %w",
				ErrChannelMemberAuthority, installErr)
		}
		return steps.expected, nil
	default:
		cause := errors.Join(commitErr, resolveErr,
			fmt.Errorf("%w: durable authority outcome diverged", ErrChannelMemberAuthority))
		if failErr := transition.FailClosed(cause); failErr != nil {
			return zero, failErr
		}
		return zero, cause
	}
}

func mapChannelMemberAuthorityError(err error) error {
	switch {
	case errors.Is(err, store.ErrChannelBaselineConflict):
		return peer.ErrChannelMemberBaselineConflict
	case errors.Is(err, store.ErrChannelBaselineEpochMismatch):
		return peer.ErrChannelMemberEpochMismatch
	case errors.Is(err, store.ErrChannelBaselineAuthority):
		return peer.ErrChannelMemberNotMember
	case errors.Is(err, store.ErrChannelRosterConflict),
		errors.Is(err, store.ErrChannelRosterInput):
		return peer.ErrChannelMemberRosterConflict
	default:
		return err
	}
}
