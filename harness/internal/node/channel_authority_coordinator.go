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

var ErrChannelAuthority = errors.New("mnemond Channel authority")

type channelAuthorityStore interface {
	ReadChannelMeshAuthority(context.Context) (store.ChannelMeshAuthority, error)
	PrepareChannelEnrollment(context.Context,
		store.PrepareChannelEnrollmentSpec,
	) (store.PrepareChannelEnrollmentResult, error)
	PrepareChannelEnrollmentSigning(context.Context,
		store.PrepareChannelEnrollmentSigningSpec,
	) (store.ChannelEnrollmentSigningPlan, error)
	PrepareSignedChannelEnrollment(context.Context, store.ChannelEnrollmentSigningPlan,
		store.ChannelEnrollmentSignatures,
	) (store.ChannelEnrollmentPlan, error)
	CommitChannelEnrollment(context.Context,
		store.ChannelEnrollmentPlan,
	) (store.AcceptChannelEnrollmentResult, error)
	ResolveChannelEnrollment(context.Context,
		store.ChannelEnrollmentPlan,
	) (store.ChannelAuthorityPlanResolution, error)
	PrepareCreateChannel(context.Context, store.CreateChannelSpec) (store.CreateChannelPlan, error)
	CommitCreateChannel(context.Context, store.CreateChannelPlan) (store.CreateChannelResult, error)
	ResolveCreateChannel(context.Context, store.CreateChannelPlan) (store.ChannelAuthorityPlanResolution, error)
	PrepareChannelRosterMerge(context.Context, store.MergeChannelRosterSpec) (store.ChannelRosterMergePlan, error)
	CommitChannelRosterMerge(context.Context, store.ChannelRosterMergePlan) (store.MergeChannelRosterResult, error)
	ResolveChannelRosterMerge(context.Context, store.ChannelRosterMergePlan) (store.ChannelAuthorityPlanResolution, error)
	PrepareInboundChannelBaseline(context.Context, store.InstallInboundChannelBaselineSpec) (store.InboundChannelBaselinePlan, error)
	CommitInboundChannelBaseline(context.Context, store.InboundChannelBaselinePlan) (store.InstallInboundChannelBaselineResult, error)
	ResolveInboundChannelBaseline(context.Context, store.InboundChannelBaselinePlan) (store.ChannelAuthorityPlanResolution, error)
}

type channelAuthorityTransition interface {
	Install() error
	Abort() error
	FailClosed(error) error
}

type channelAuthorityRuntime interface {
	begin(store.ChannelMeshAuthority) (channelAuthorityTransition, error)
}

type meshChannelAuthorityRuntime struct{ runtime *peer.MeshRuntime }

func (runtime meshChannelAuthorityRuntime) begin(
	candidate store.ChannelMeshAuthority,
) (channelAuthorityTransition, error) {
	return runtime.runtime.BeginAuthorityTransition(candidate)
}

// ChannelAuthorityCoordinator is the Node-private prepare -> runtime drain ->
// Store CAS -> runtime install boundary for every local Channel authority
// mutation. Its single capacity-one token serializes create, member roster,
// and baseline authority while Store remains the durable source of truth.
type ChannelAuthorityCoordinator struct {
	store   channelAuthorityStore
	runtime channelAuthorityRuntime
	signer  store.ChannelAuthoritySigner
	token   chan struct{}
}

var _ peer.ChannelMemberController = (*ChannelAuthorityCoordinator)(nil)

func NewChannelAuthorityCoordinator(ctx context.Context, st *store.Store,
	runtime *peer.MeshRuntime, identity *Identity,
) (*ChannelAuthorityCoordinator, error) {
	if ctx == nil || st == nil || runtime == nil || identity == nil || identity.PeerID().IsZero() {
		return nil, fmt.Errorf("%w: Store, mesh runtime, and Node identity are required",
			ErrChannelAuthority)
	}
	mesh, err := st.ReadChannelMeshAuthority(ctx)
	if err != nil || mesh.LocalPeerID() != identity.PeerID() {
		return nil, fmt.Errorf("%w: Node identity does not match durable authority",
			ErrChannelAuthority)
	}
	return newChannelAuthorityCoordinator(st, meshChannelAuthorityRuntime{runtime: runtime},
		identity.PublicationSigner())
}

func newChannelAuthorityCoordinator(st channelAuthorityStore,
	runtime channelAuthorityRuntime, signer store.ChannelAuthoritySigner,
) (*ChannelAuthorityCoordinator, error) {
	if st == nil || runtime == nil || nilChannelAuthoritySigner(signer) {
		return nil, fmt.Errorf("%w: Store, mesh runtime, and signer are required", ErrChannelAuthority)
	}
	token := make(chan struct{}, 1)
	token <- struct{}{}
	return &ChannelAuthorityCoordinator{store: st, runtime: runtime, signer: signer, token: token}, nil
}

// CreateChannel prepares and commits the exact Store-bound create candidate
// while holding the same logical Channel authority token as remote member
// reconciliation.
func (coordinator *ChannelAuthorityCoordinator) CreateChannel(ctx context.Context,
	spec store.CreateChannelSpec,
) (store.CreateChannelResult, error) {
	release, err := coordinator.acquire(ctx)
	if err != nil {
		return store.CreateChannelResult{}, err
	}
	defer release()
	plan, err := coordinator.store.PrepareCreateChannel(ctx, spec)
	if err != nil {
		return store.CreateChannelResult{}, err
	}
	return executeChannelAuthorityPlan(ctx, coordinator.runtime,
		channelAuthorityPlanSteps[store.CreateChannelResult]{
			candidate: plan.Candidate(), changes: plan.ChangesAuthority(), expected: plan.Result(),
			commit: func(commitCtx context.Context) (store.CreateChannelResult, error) {
				return coordinator.store.CommitCreateChannel(commitCtx, plan)
			},
			resolve: func(resolveCtx context.Context) (store.ChannelAuthorityPlanResolution, error) {
				return coordinator.store.ResolveCreateChannel(resolveCtx, plan)
			},
		})
}

func (coordinator *ChannelAuthorityCoordinator) ReconcileMemberHelloGate(ctx context.Context,
	control peer.ChannelMemberHelloControl,
) (peer.ChannelMemberHelloAuthority, error) {
	release, err := coordinator.acquire(ctx)
	if err != nil {
		return peer.ChannelMemberHelloAuthority{}, err
	}
	defer release()
	if len(control.ProofRecords) == 0 {
		roster, readErr := coordinator.readRoster(ctx, control.ChannelID)
		return peer.ChannelMemberHelloAuthority{Roster: roster}, readErr
	}
	plan, err := coordinator.store.PrepareChannelRosterMerge(ctx, store.MergeChannelRosterSpec{
		ChannelID: control.ChannelID, AuthenticatedTransportPeerID: control.AuthenticatedPeerID,
		Records: control.ProofRecords, At: control.At,
	})
	if err != nil {
		return peer.ChannelMemberHelloAuthority{}, mapChannelAuthorityError(err)
	}
	result, err := executeChannelAuthorityPlan(ctx, coordinator.runtime,
		channelAuthorityPlanSteps[store.MergeChannelRosterResult]{
			candidate: plan.Candidate(), changes: plan.ChangesAuthority(), expected: plan.Result(),
			commit: func(commitCtx context.Context) (store.MergeChannelRosterResult, error) {
				return coordinator.store.CommitChannelRosterMerge(commitCtx, plan)
			},
			resolve: func(resolveCtx context.Context) (store.ChannelAuthorityPlanResolution, error) {
				return coordinator.store.ResolveChannelRosterMerge(resolveCtx, plan)
			},
		})
	if err != nil {
		return peer.ChannelMemberHelloAuthority{}, mapChannelAuthorityError(err)
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
			"%w: Store returned unknown roster result", ErrChannelAuthority)
	}
}

func (coordinator *ChannelAuthorityCoordinator) FreezeMemberRosterForSync(ctx context.Context,
	control peer.ChannelMemberSyncControl,
) (peer.ChannelMemberRosterSnapshot, error) {
	release, err := coordinator.acquire(ctx)
	if err != nil {
		return peer.ChannelMemberRosterSnapshot{}, err
	}
	defer release()
	roster, err := coordinator.readRoster(ctx, control.ChannelID)
	return peer.ChannelMemberRosterSnapshot{Roster: roster}, err
}

func (coordinator *ChannelAuthorityCoordinator) InstallMemberBaselineGate(ctx context.Context,
	control peer.ChannelMemberBaselineControl,
) (peer.ChannelMemberBaselineAuthority, error) {
	release, err := coordinator.acquire(ctx)
	if err != nil {
		return peer.ChannelMemberBaselineAuthority{}, err
	}
	defer release()
	baseline := store.ChannelDataBaseline{ChannelID: control.Baseline.ChannelID,
		OriginPeerID: control.Baseline.OriginPeerID, OriginEpoch: control.Baseline.OriginEpoch,
		BaselineChannelSequence: control.Baseline.BaselineChannelSequence}
	plan, err := coordinator.store.PrepareInboundChannelBaseline(ctx,
		store.InstallInboundChannelBaselineSpec{AuthenticatedPeerID: control.AuthenticatedPeerID,
			Baseline: baseline, At: control.At})
	if err != nil {
		return peer.ChannelMemberBaselineAuthority{}, mapChannelAuthorityError(err)
	}
	result, err := executeChannelAuthorityPlan(ctx, coordinator.runtime,
		channelAuthorityPlanSteps[store.InstallInboundChannelBaselineResult]{
			candidate: plan.Candidate(), changes: plan.ChangesAuthority(), expected: plan.Result(),
			commit: func(commitCtx context.Context) (store.InstallInboundChannelBaselineResult, error) {
				return coordinator.store.CommitInboundChannelBaseline(commitCtx, plan)
			},
			resolve: func(resolveCtx context.Context) (store.ChannelAuthorityPlanResolution, error) {
				return coordinator.store.ResolveInboundChannelBaseline(resolveCtx, plan)
			},
		})
	if err != nil {
		return peer.ChannelMemberBaselineAuthority{}, mapChannelAuthorityError(err)
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

func (coordinator *ChannelAuthorityCoordinator) acquire(ctx context.Context) (func(), error) {
	if coordinator == nil || coordinator.store == nil || coordinator.runtime == nil ||
		coordinator.token == nil || ctx == nil {
		return nil, fmt.Errorf("%w: coordinator is unavailable", ErrChannelAuthority)
	}
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: mutation wait: %w", ErrChannelAuthority, ctx.Err())
	case <-coordinator.token:
		return func() { coordinator.token <- struct{}{} }, nil
	}
}

func (coordinator *ChannelAuthorityCoordinator) readRoster(ctx context.Context,
	channelID model.ChannelID,
) (model.VerifiedRoster, error) {
	mesh, err := coordinator.store.ReadChannelMeshAuthority(ctx)
	if err != nil {
		return model.VerifiedRoster{}, fmt.Errorf("%w: read mesh: %w", ErrChannelAuthority, err)
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
	runtime channelAuthorityRuntime, steps channelAuthorityPlanSteps[T],
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
				ErrChannelAuthority, beginErr))
		}
		return resolveChannelAuthorityCommit(ctx, transition, steps, commitErr)
	}
	transition, err := runtime.begin(steps.candidate)
	if err != nil {
		return zero, fmt.Errorf("%w: prepare runtime transition: %w", ErrChannelAuthority, err)
	}
	committed, commitErr := steps.commit(ctx)
	if commitErr == nil {
		if err := transition.Install(); err != nil {
			return zero, fmt.Errorf("%w: install committed authority: %w", ErrChannelAuthority, err)
		}
		return committed, nil
	}
	return resolveChannelAuthorityCommit(ctx, transition, steps, commitErr)
}

func resolveChannelAuthorityCommit[T any](ctx context.Context,
	transition channelAuthorityTransition, steps channelAuthorityPlanSteps[T],
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
				"%w: abort uncommitted authority: %w", ErrChannelAuthority, abortErr))
		}
		return zero, commitErr
	case resolveErr == nil && resolution == store.ChannelAuthorityPlanCandidate:
		if installErr := transition.Install(); installErr != nil {
			return zero, fmt.Errorf("%w: install resolved authority: %w",
				ErrChannelAuthority, installErr)
		}
		return steps.expected, nil
	default:
		cause := errors.Join(commitErr, resolveErr,
			fmt.Errorf("%w: durable authority outcome diverged", ErrChannelAuthority))
		if failErr := transition.FailClosed(cause); failErr != nil {
			return zero, failErr
		}
		return zero, cause
	}
}

func mapChannelAuthorityError(err error) error {
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
