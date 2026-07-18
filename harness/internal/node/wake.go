package node

import (
	"context"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

var errAdmittedWakeStore = errors.Join(agent.ErrWakeStoreNotInvoked,
	errors.New("managed wake Store admission is unavailable"))

func wakeStoreNotInvoked(err error) error {
	if err == nil {
		return errAdmittedWakeStore
	}
	return errors.Join(agent.ErrWakeStoreNotInvoked, err)
}

// admittedWakeStore places the controller drain fence around one Store call,
// never around attachment filesystem work, Runtime launch, or adapter.Run.
// That narrow boundary lets shutdown wait for committed durable mutations
// without inheriting an unbounded external process lifetime.
type admittedWakeStore struct {
	store     *store.Store
	admission ManagedAdmission
}

func (admitted *admittedWakeStore) PreclaimAgentWake(ctx context.Context,
	spec store.AgentWakePreclaimSpec,
) (store.AgentClaimResult, error) {
	if admitted == nil || admitted.admission == nil {
		return store.AgentClaimResult{}, errAdmittedWakeStore
	}
	release, err := admitted.admission.EnterWorker(ctx)
	if err != nil {
		return store.AgentClaimResult{}, wakeStoreNotInvoked(err)
	}
	if release == nil {
		return store.AgentClaimResult{}, errAdmittedWakeStore
	}
	defer release()
	if admitted.store == nil {
		return store.AgentClaimResult{}, errAdmittedWakeStore
	}
	return admitted.store.PreclaimAgentWake(ctx, spec)
}

func (admitted *admittedWakeStore) ListReapableAgentRunAttachments(ctx context.Context,
	spec store.AgentAttachmentCleanupSpec,
) ([]store.ReapableAgentRunAttachment, error) {
	if admitted == nil || admitted.admission == nil {
		return nil, errAdmittedWakeStore
	}
	release, err := admitted.admission.EnterWorker(ctx)
	if err != nil {
		return nil, wakeStoreNotInvoked(err)
	}
	if release == nil {
		return nil, errAdmittedWakeStore
	}
	defer release()
	if admitted.store == nil {
		return nil, errAdmittedWakeStore
	}
	return admitted.store.ListReapableAgentRunAttachments(ctx, spec)
}

func (admitted *admittedWakeStore) ListIncompleteManagedAgentRuns(ctx context.Context) ([]model.AgentRun, error) {
	if admitted == nil || admitted.admission == nil {
		return nil, errAdmittedWakeStore
	}
	release, err := admitted.admission.EnterWorker(ctx)
	if err != nil {
		return nil, wakeStoreNotInvoked(err)
	}
	if release == nil {
		return nil, errAdmittedWakeStore
	}
	defer release()
	if admitted.store == nil {
		return nil, errAdmittedWakeStore
	}
	return admitted.store.ListIncompleteManagedAgentRuns(ctx)
}

func (admitted *admittedWakeStore) AbandonUnregisteredAgentRun(ctx context.Context,
	spec store.AgentUnregisteredRunSpec,
) (store.AgentRuntimeTransitionResult, error) {
	if admitted == nil || admitted.admission == nil {
		return store.AgentRuntimeTransitionResult{}, errAdmittedWakeStore
	}
	release, err := admitted.admission.EnterWorker(ctx)
	if err != nil {
		return store.AgentRuntimeTransitionResult{}, wakeStoreNotInvoked(err)
	}
	if release == nil {
		return store.AgentRuntimeTransitionResult{}, errAdmittedWakeStore
	}
	defer release()
	if admitted.store == nil {
		return store.AgentRuntimeTransitionResult{}, errAdmittedWakeStore
	}
	return admitted.store.AbandonUnregisteredAgentRun(ctx, spec)
}

func (admitted *admittedWakeStore) SettleOrphanedAgentRuntime(ctx context.Context,
	spec store.AgentRuntimeOrphanSpec,
) (store.AgentRuntimeTransitionResult, error) {
	if admitted == nil || admitted.admission == nil {
		return store.AgentRuntimeTransitionResult{}, errAdmittedWakeStore
	}
	release, err := admitted.admission.EnterWorker(ctx)
	if err != nil {
		return store.AgentRuntimeTransitionResult{}, wakeStoreNotInvoked(err)
	}
	if release == nil {
		return store.AgentRuntimeTransitionResult{}, errAdmittedWakeStore
	}
	defer release()
	if admitted.store == nil {
		return store.AgentRuntimeTransitionResult{}, errAdmittedWakeStore
	}
	return admitted.store.SettleOrphanedAgentRuntime(ctx, spec)
}

func (admitted *admittedWakeStore) RecordAgentRuntimeLaunch(ctx context.Context,
	spec store.AgentRuntimeLaunchSpec,
) (store.AgentRuntimeTransitionResult, error) {
	if admitted == nil || admitted.admission == nil {
		return store.AgentRuntimeTransitionResult{}, errAdmittedWakeStore
	}
	release, err := admitted.admission.EnterWorker(ctx)
	if err != nil {
		return store.AgentRuntimeTransitionResult{}, wakeStoreNotInvoked(err)
	}
	if release == nil {
		return store.AgentRuntimeTransitionResult{}, errAdmittedWakeStore
	}
	defer release()
	if admitted.store == nil {
		return store.AgentRuntimeTransitionResult{}, errAdmittedWakeStore
	}
	return admitted.store.RecordAgentRuntimeLaunch(ctx, spec)
}

func (admitted *admittedWakeStore) RecordAgentWakeDelivery(ctx context.Context,
	spec store.AgentWakeDeliverySpec,
) (store.AgentRuntimeTransitionResult, error) {
	if admitted == nil || admitted.admission == nil {
		return store.AgentRuntimeTransitionResult{}, errAdmittedWakeStore
	}
	release, err := admitted.admission.EnterWorker(ctx)
	if err != nil {
		return store.AgentRuntimeTransitionResult{}, wakeStoreNotInvoked(err)
	}
	if release == nil {
		return store.AgentRuntimeTransitionResult{}, errAdmittedWakeStore
	}
	defer release()
	if admitted.store == nil {
		return store.AgentRuntimeTransitionResult{}, errAdmittedWakeStore
	}
	return admitted.store.RecordAgentWakeDelivery(ctx, spec)
}

func (admitted *admittedWakeStore) FinishAgentRuntime(ctx context.Context,
	spec store.AgentRuntimeFinishSpec,
) (store.AgentRuntimeTransitionResult, error) {
	if admitted == nil || admitted.admission == nil {
		return store.AgentRuntimeTransitionResult{}, errAdmittedWakeStore
	}
	release, err := admitted.admission.EnterWorker(ctx)
	if err != nil {
		return store.AgentRuntimeTransitionResult{}, wakeStoreNotInvoked(err)
	}
	if release == nil {
		return store.AgentRuntimeTransitionResult{}, errAdmittedWakeStore
	}
	defer release()
	if admitted.store == nil {
		return store.AgentRuntimeTransitionResult{}, errAdmittedWakeStore
	}
	return admitted.store.FinishAgentRuntime(ctx, spec)
}

func (admitted *admittedWakeStore) FailAgentRuntime(ctx context.Context,
	spec store.AgentRuntimeFailureSpec,
) (store.AgentRuntimeTransitionResult, error) {
	if admitted == nil || admitted.admission == nil {
		return store.AgentRuntimeTransitionResult{}, errAdmittedWakeStore
	}
	release, err := admitted.admission.EnterWorker(ctx)
	if err != nil {
		return store.AgentRuntimeTransitionResult{}, wakeStoreNotInvoked(err)
	}
	if release == nil {
		return store.AgentRuntimeTransitionResult{}, errAdmittedWakeStore
	}
	defer release()
	if admitted.store == nil {
		return store.AgentRuntimeTransitionResult{}, errAdmittedWakeStore
	}
	return admitted.store.FailAgentRuntime(ctx, spec)
}

// newManagedWakeWorker is the Node-owned composition boundary for the managed
// Runtime loop. The admission-aware value is shared by the preparer and
// worker, while the activation check and adapter remain outside admission.
func newManagedWakeWorker(st *store.Store, nodeState string, profile model.Profile,
	clock Clock, install InstallationVerifier, adapter agent.WakeWorkerAdapter,
	admission ManagedAdmission,
) (*agent.WakeWorker, error) {
	if st == nil || install == nil || admission == nil {
		return nil, fmt.Errorf("compose managed wake worker: Store, installation verifier and admission are required")
	}
	admitted := &admittedWakeStore{store: st, admission: admission}
	preparer, err := agent.NewWakeAttachmentPreparer(admitted, agent.WakeAttachmentOptions{
		NodeState: nodeState, AssetRevision: profile.ActiveAssetRevision(), Clock: clock,
	})
	if err != nil {
		return nil, err
	}
	activation := controllerActivationGate{expected: profile, install: install}
	gate := agent.WakeWorkerGateFunc(func(ctx context.Context, current model.Profile) error {
		if apiErr := activation.Check(ctx, current); apiErr != nil {
			return apiErr
		}
		return nil
	})
	return agent.NewWakeWorker(agent.WakeWorkerOptions{
		Profile: profile, AssetRevision: profile.ActiveAssetRevision(), Store: admitted,
		Preparer: preparer, Adapter: adapter, Gate: gate, Clock: clock,
	})
}

var _ agent.WakePreclaimStore = (*admittedWakeStore)(nil)
var _ agent.WakeWorkerStore = (*admittedWakeStore)(nil)
