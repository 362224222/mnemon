package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

var ErrWakeWorker = errors.New("managed wake worker")

const (
	defaultWakeWorkerPoll        = time.Second
	defaultWakeWorkerBackoff     = 5 * time.Second
	defaultWakeWorkerSettlement  = 10 * time.Second
	maxWakeWorkerPoll            = time.Minute
	maxWakeWorkerBackoff         = 5 * time.Minute
	maxWakeWorkerSettlement      = 30 * time.Second
	wakeWorkerSettlementAttempts = 2

	wakeWorkerIssueGate             = "activation_or_installation_unavailable"
	wakeWorkerIssuePrepare          = "wake_preparation_unavailable"
	wakeWorkerIssuePrepareRun       = "wake_attachment_publish_failed"
	wakeWorkerIssueAdapter          = "managed_runtime_failed"
	wakeWorkerIssueCallback         = "durable_runtime_callback_failed"
	wakeWorkerIssueRecoveryLive     = "startup_runtime_still_live"
	wakeWorkerIssueRecoveryInvalid  = "startup_runtime_evidence_invalid"
	wakeWorkerIssueDurable          = "durable_runtime_transition_failed"
	wakeWorkerIssueRuntimeUnproven  = "managed_runtime_exit_unproven"
	wakeWorkerIssueAdapterInvariant = "managed_runtime_result_invalid"

	wakeWorkerPrepareFailure  = "managed Runtime attachment could not be published"
	wakeWorkerAdapterFailure  = "managed Runtime execution failed"
	wakeWorkerCallbackFailure = "managed Runtime durable callback failed"
	wakeWorkerRecoveryFailure = "managed Runtime was orphaned during startup recovery"
	wakeWorkerUnregistered    = "managed Runtime launch was not registered"
)

// WakeWorkerStore is the complete durable surface reachable by the worker.
// Claim selection remains inside WakeAttachmentPreparer; admission and all
// other Store operations are deliberately absent.
type WakeWorkerStore interface {
	ListIncompleteManagedAgentRuns(context.Context) ([]model.AgentRun, error)
	AbandonUnregisteredAgentRun(context.Context,
		store.AgentUnregisteredRunSpec) (store.AgentRuntimeTransitionResult, error)
	SettleOrphanedAgentRuntime(context.Context,
		store.AgentRuntimeOrphanSpec) (store.AgentRuntimeTransitionResult, error)
	RecordAgentRuntimeLaunch(context.Context,
		store.AgentRuntimeLaunchSpec) (store.AgentRuntimeTransitionResult, error)
	RecordAgentWakeDelivery(context.Context,
		store.AgentWakeDeliverySpec) (store.AgentRuntimeTransitionResult, error)
	FinishAgentRuntime(context.Context,
		store.AgentRuntimeFinishSpec) (store.AgentRuntimeTransitionResult, error)
	FailAgentRuntime(context.Context,
		store.AgentRuntimeFailureSpec) (store.AgentRuntimeTransitionResult, error)
}

type WakeWorkerPreparer interface {
	Prepare(context.Context, model.Profile) (PreparedWake, error)
}

type WakeWorkerAdapter interface {
	Run(context.Context, CodexWakeRequest) (CodexWakeResult, error)
}

// WakeWorkerGate revalidates activation and installation without exposing a
// controller admission primitive to the worker.
type WakeWorkerGate interface {
	Check(context.Context, model.Profile) error
}

type WakeWorkerGateFunc func(context.Context, model.Profile) error

func (check WakeWorkerGateFunc) Check(ctx context.Context, profile model.Profile) error {
	if check == nil {
		return errors.New("wake worker gate is unavailable")
	}
	return check(ctx, profile)
}

type WakeWorkerTimer interface {
	After(time.Duration) <-chan time.Time
}

type wallWakeWorkerTimer struct{}

func (wallWakeWorkerTimer) After(duration time.Duration) <-chan time.Time {
	return time.After(duration)
}

type WakeWorkerOptions struct {
	Profile           model.Profile
	AssetRevision     string
	Store             WakeWorkerStore
	Preparer          WakeWorkerPreparer
	Adapter           WakeWorkerAdapter
	Gate              WakeWorkerGate
	Clock             ServiceClock
	Timer             WakeWorkerTimer
	PollInterval      time.Duration
	BackoffInterval   time.Duration
	SettlementTimeout time.Duration
}

type WakeWorkerSnapshot struct {
	Running    bool
	Ready      bool
	Healthy    bool
	Recovering bool
	LastError  string
}

// WakeWorker is one bounded, serial managed-Runtime loop. It owns no admission
// state and cannot create a second in-flight wake: Prepare and adapter.Run are
// called only from this one Run goroutine.
type WakeWorker struct {
	profile           model.Profile
	assetRevision     string
	store             WakeWorkerStore
	preparer          WakeWorkerPreparer
	adapter           WakeWorkerAdapter
	gate              WakeWorkerGate
	clock             ServiceClock
	timer             WakeWorkerTimer
	pollInterval      time.Duration
	backoffInterval   time.Duration
	settlementTimeout time.Duration
	recoverRuntime    func(context.Context, model.JSON, func() time.Time) (runtimeProcessRecovery, error)

	mu       sync.Mutex
	running  bool
	ready    bool
	healthy  bool
	recovery bool
	lastErr  string
}

type wakeWorkerCallbackKind uint8

const (
	wakeWorkerLaunchCallback wakeWorkerCallbackKind = iota + 1
	wakeWorkerWakeCallback
)

type wakeWorkerCallbackSnapshot struct {
	launchFailed bool
	wakeFailed   bool
}

// wakeWorkerCallbackState closes the adapter callback lifetime before final
// settlement. A callback that started before adapter.Run returned is drained;
// one that escaped Run is rejected without reaching Store.
type wakeWorkerCallbackState struct {
	mu           sync.Mutex
	condition    *sync.Cond
	closed       bool
	inFlight     uint32
	launchFailed bool
	wakeFailed   bool
}

func newWakeWorkerCallbackState() *wakeWorkerCallbackState {
	state := &wakeWorkerCallbackState{}
	state.condition = sync.NewCond(&state.mu)
	return state
}

func (state *wakeWorkerCallbackState) begin() bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed {
		return false
	}
	state.inFlight++
	return true
}

func (state *wakeWorkerCallbackState) end() {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.inFlight == 0 {
		return
	}
	state.inFlight--
	if state.closed && state.inFlight == 0 {
		state.condition.Broadcast()
	}
}

func (state *wakeWorkerCallbackState) markFailed(kind wakeWorkerCallbackKind) error {
	state.mu.Lock()
	switch kind {
	case wakeWorkerLaunchCallback:
		state.launchFailed = true
	case wakeWorkerWakeCallback:
		state.wakeFailed = true
	}
	state.mu.Unlock()
	return errors.New(wakeWorkerCallbackFailure)
}

func (state *wakeWorkerCallbackState) closeAndWait() wakeWorkerCallbackSnapshot {
	state.mu.Lock()
	state.closed = true
	for state.inFlight != 0 {
		state.condition.Wait()
	}
	snapshot := wakeWorkerCallbackSnapshot{launchFailed: state.launchFailed,
		wakeFailed: state.wakeFailed}
	state.mu.Unlock()
	return snapshot
}

func NewWakeWorker(options WakeWorkerOptions) (*WakeWorker, error) {
	if options.Profile.ID() != model.TeamworkProfileID() || !options.Profile.Enabled() ||
		options.Profile.Runtime() != model.RuntimeCodexAppServer || options.AssetRevision == "" ||
		options.Profile.ActiveAssetRevision() != options.AssetRevision || options.Store == nil ||
		options.Preparer == nil || options.Adapter == nil || options.Gate == nil {
		return nil, fmt.Errorf("%w: active Codex Profile, Store, gate, preparer and adapter are required",
			ErrWakeWorker)
	}
	if options.Clock == nil {
		options.Clock = wallServiceClock{}
	}
	if options.Timer == nil {
		options.Timer = wallWakeWorkerTimer{}
	}
	if options.PollInterval == 0 {
		options.PollInterval = defaultWakeWorkerPoll
	}
	if options.BackoffInterval == 0 {
		options.BackoffInterval = defaultWakeWorkerBackoff
	}
	if options.SettlementTimeout == 0 {
		options.SettlementTimeout = defaultWakeWorkerSettlement
	}
	if options.PollInterval < time.Millisecond || options.PollInterval > maxWakeWorkerPoll ||
		options.BackoffInterval < time.Millisecond || options.BackoffInterval > maxWakeWorkerBackoff ||
		options.SettlementTimeout < time.Millisecond ||
		options.SettlementTimeout > maxWakeWorkerSettlement {
		return nil, fmt.Errorf("%w: poll, backoff or settlement bound is invalid", ErrWakeWorker)
	}
	return &WakeWorker{profile: options.Profile, assetRevision: options.AssetRevision,
		store: options.Store, preparer: options.Preparer, adapter: options.Adapter,
		gate: options.Gate, clock: options.Clock, timer: options.Timer,
		pollInterval: options.PollInterval, backoffInterval: options.BackoffInterval,
		settlementTimeout: options.SettlementTimeout, recoverRuntime: recoverRuntimeProcess,
		healthy: true}, nil
}

func (worker *WakeWorker) Snapshot() WakeWorkerSnapshot {
	if worker == nil {
		return WakeWorkerSnapshot{}
	}
	worker.mu.Lock()
	defer worker.mu.Unlock()
	return WakeWorkerSnapshot{Running: worker.running, Ready: worker.ready,
		Healthy: worker.healthy, Recovering: worker.recovery, LastError: worker.lastErr}
}

// Run performs startup recovery before the first gate or claim, then polls in
// one goroutine until cancellation or a fail-closed fatal condition. Context
// cancellation is a graceful worker stop; any already-proven Runtime exit is
// still settled through an independent bounded context before Run returns.
func (worker *WakeWorker) Run(ctx context.Context) error {
	if worker == nil || ctx == nil {
		return fmt.Errorf("%w: worker and context are required", ErrWakeWorker)
	}
	if err := ctx.Err(); err != nil {
		return nil
	}
	if err := worker.beginRun(); err != nil {
		return err
	}
	defer worker.endRun()

	for {
		recovered, issue := worker.recoverStartup(ctx)
		if issue != "" {
			return worker.fail(issue)
		}
		if ctx.Err() != nil {
			return nil
		}
		if recovered {
			worker.setState(false, false, "")
			break
		}
		if !worker.wait(ctx, worker.backoffInterval) {
			return nil
		}
	}

	for {
		delay, issue := worker.tick(ctx)
		if issue != "" {
			return worker.fail(issue)
		}
		if ctx.Err() != nil {
			return nil
		}
		if !worker.wait(ctx, delay) {
			return nil
		}
	}
}

func (worker *WakeWorker) beginRun() error {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if !worker.healthy {
		return fmt.Errorf("%w: worker is unhealthy", ErrWakeWorker)
	}
	if worker.running {
		return fmt.Errorf("%w: worker is already running", ErrWakeWorker)
	}
	worker.running, worker.ready, worker.recovery, worker.lastErr = true, false, true, ""
	return nil
}

func (worker *WakeWorker) endRun() {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	worker.running, worker.ready, worker.recovery = false, false, false
}

func (worker *WakeWorker) fail(issue string) error {
	worker.mu.Lock()
	worker.ready, worker.healthy, worker.recovery, worker.lastErr = false, false, false, issue
	worker.mu.Unlock()
	return fmt.Errorf("%w: %s", ErrWakeWorker, issue)
}

func (worker *WakeWorker) setState(ready, recovering bool, issue string) {
	worker.mu.Lock()
	worker.ready, worker.recovery, worker.lastErr = ready, recovering, issue
	worker.mu.Unlock()
}

func (worker *WakeWorker) wait(ctx context.Context, duration time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-worker.timer.After(duration):
		return true
	}
}

func (worker *WakeWorker) recoverStartup(ctx context.Context) (bool, string) {
	worker.setState(false, true, "")
	runs, err := worker.store.ListIncompleteManagedAgentRuns(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return false, ""
		}
		return false, wakeWorkerIssueDurable
	}
	for _, run := range runs {
		fence, authorityOK := worker.runAuthority(run)
		if !authorityOK {
			return false, wakeWorkerIssueRecoveryInvalid
		}
		_, launched := run.RuntimeStartedAt()
		diagnosticEmpty := run.LauncherDiagnostic().String() == `{}`
		runtimeIDsEmpty := run.RuntimeIDs().String() == `{}`
		if !launched && diagnosticEmpty && runtimeIDsEmpty {
			at, err := worker.trustedNow()
			if err != nil {
				return false, wakeWorkerIssueRecoveryInvalid
			}
			transition, err := worker.abandonUnregistered(run, fence, at)
			if err != nil || !validWakeWorkerAbandonTransition(transition, run,
				wakeWorkerUnregistered, at) {
				return false, wakeWorkerIssueDurable
			}
			continue
		}
		if !launched || diagnosticEmpty || runtimeIDsEmpty {
			return false, wakeWorkerIssueRecoveryInvalid
		}
		recovery, err := worker.recoverRuntime(ctx, run.RuntimeIDs(), worker.clock.Now)
		if err != nil {
			switch {
			case ctx.Err() != nil:
				return false, ""
			case errors.Is(err, ErrRuntimeProcessLive):
				worker.setState(false, true, wakeWorkerIssueRecoveryLive)
				return false, ""
			default:
				return false, wakeWorkerIssueRecoveryInvalid
			}
		}
		if recovery.Receipt.IsZero() {
			return false, wakeWorkerIssueRecoveryInvalid
		}
		if _, err := canonicalRuntimeProcessTime(recovery.At); err != nil {
			return false, wakeWorkerIssueRecoveryInvalid
		}
		transition, err := worker.settleOrphan(run, fence, recovery)
		if err != nil || !validWakeWorkerOrphanTransition(transition, run, recovery,
			wakeWorkerRecoveryFailure) {
			return false, wakeWorkerIssueDurable
		}
	}
	return true, ""
}

// recoverPrepareAmbiguity does not inherit the Prepare context. A preclaim
// may have committed immediately before that context was cancelled or its
// response was lost; using the same context for the rescan would leave the
// durable claim undiscovered. The independent bound makes cancellation-safe
// recovery finite while treating an exhausted rescan as a durable failure.
func (worker *WakeWorker) recoverPrepareAmbiguity() (bool, string) {
	recoveryCtx, cancel := context.WithTimeout(context.Background(), worker.settlementTimeout)
	recovered, issue := worker.recoverStartup(recoveryCtx)
	timedOut := recoveryCtx.Err() != nil
	cancel()
	if !recovered && issue == "" && timedOut {
		return false, wakeWorkerIssueDurable
	}
	return recovered, issue
}

func (worker *WakeWorker) tick(ctx context.Context) (time.Duration, string) {
	if err := worker.gate.Check(ctx, worker.profile); err != nil {
		if ctx.Err() == nil {
			worker.setState(false, false, wakeWorkerIssueGate)
		}
		return worker.backoffInterval, ""
	}
	prepared, prepareErr := worker.preparer.Prepare(ctx, worker.profile)
	if prepareErr != nil {
		// Admission can prove that a retained mutation seal rejected this
		// operation before Store was invoked. That is not an ambiguous preclaim
		// commit and must not start an independent rescan through the same seal.
		if errors.Is(prepareErr, ErrWakeStoreNotInvoked) {
			if ctx.Err() == nil {
				worker.setState(false, false, wakeWorkerIssuePrepare)
			}
			return worker.backoffInterval, ""
		}
		if prepared.Status() == store.AgentClaimActionable && !prepared.Run().ID().IsZero() {
			if issue := worker.failPreparedRun(prepared); issue != "" {
				return worker.backoffInterval, issue
			}
			worker.setState(false, false, wakeWorkerIssuePrepareRun)
			return worker.backoffInterval, ""
		}
		// Preclaim is a commit boundary. An error without a returned Run may
		// therefore be a lost response after Store created one. Perform exactly
		// one startup-style rescan before retrying so the claim is abandoned now
		// instead of remaining busy until its lease expires.
		recovered, issue := worker.recoverPrepareAmbiguity()
		if issue != "" {
			return worker.backoffInterval, issue
		}
		if !recovered {
			return worker.backoffInterval, ""
		}
		if ctx.Err() == nil {
			worker.setState(false, false, wakeWorkerIssuePrepare)
		}
		return worker.backoffInterval, ""
	}
	if prepared.Status() != store.AgentClaimActionable {
		if !prepared.Status().Valid() || !prepared.Run().ID().IsZero() {
			recovered, issue := worker.recoverPrepareAmbiguity()
			if issue != "" {
				return worker.backoffInterval, issue
			}
			if !recovered {
				return worker.backoffInterval, ""
			}
			return worker.backoffInterval, wakeWorkerIssuePrepare
		}
		worker.setState(true, false, "")
		return worker.pollInterval, ""
	}
	if prepared.Run().ID().IsZero() || prepared.Environment() == "" {
		if !prepared.Run().ID().IsZero() {
			if issue := worker.failPreparedRun(prepared); issue != "" {
				return worker.backoffInterval, issue
			}
		} else {
			recovered, issue := worker.recoverPrepareAmbiguity()
			if issue != "" {
				return worker.backoffInterval, issue
			}
			if !recovered {
				return worker.backoffInterval, ""
			}
			return worker.backoffInterval, wakeWorkerIssuePrepareRun
		}
		worker.setState(false, false, wakeWorkerIssuePrepareRun)
		return worker.backoffInterval, ""
	}
	fence, ok := worker.runAuthority(prepared.Run())
	if !ok {
		return worker.backoffInterval, wakeWorkerIssueDurable
	}
	worker.setState(true, false, "")
	return worker.runPrepared(ctx, prepared, fence)
}

func (worker *WakeWorker) runPrepared(ctx context.Context, prepared PreparedWake,
	fence model.Digest,
) (time.Duration, string) {
	run := prepared.Run()
	callbackState := newWakeWorkerCallbackState()
	callbacks := CodexWakeCallbacks{
		RecordLaunch: func(callbackCtx context.Context, evidence CodexLaunchEvidence) error {
			if !callbackState.begin() {
				return errors.New(wakeWorkerCallbackFailure)
			}
			defer callbackState.end()
			if callbackCtx == nil {
				return callbackState.markFailed(wakeWorkerLaunchCallback)
			}
			transition, err := worker.store.RecordAgentRuntimeLaunch(callbackCtx,
				store.AgentRuntimeLaunchSpec{ProfileID: worker.profile.ID(),
					ExpectedAssetRevision: worker.assetRevision, RunID: run.ID(),
					ClaimFenceHash: fence, HandlingRecovery: run.HandlingRecovery(),
					LauncherDiagnostic: evidence.Diagnostic, RuntimeIDs: evidence.RuntimeIDs,
					At: evidence.At})
			if err != nil || !validWakeWorkerLaunchTransition(transition, run, evidence) {
				return callbackState.markFailed(wakeWorkerLaunchCallback)
			}
			return nil
		},
		RecordWake: func(callbackCtx context.Context, evidence CodexWakeEvidence) error {
			if !callbackState.begin() {
				return errors.New(wakeWorkerCallbackFailure)
			}
			defer callbackState.end()
			if callbackCtx == nil {
				return callbackState.markFailed(wakeWorkerWakeCallback)
			}
			transition, err := worker.store.RecordAgentWakeDelivery(callbackCtx,
				store.AgentWakeDeliverySpec{ProfileID: worker.profile.ID(),
					ExpectedAssetRevision: worker.assetRevision, RunID: run.ID(),
					ClaimFenceHash: fence, HandlingRecovery: run.HandlingRecovery(),
					WakeReceipt: evidence.Receipt, At: evidence.At})
			if err != nil || !validWakeWorkerWakeTransition(transition, run, evidence, false) {
				return callbackState.markFailed(wakeWorkerWakeCallback)
			}
			return nil
		},
	}
	result, adapterErr := worker.adapter.Run(ctx, CodexWakeRequest{
		RunAttachmentEnvironment: prepared.Environment(), Callbacks: callbacks})
	callback := callbackState.closeAndWait()
	if callback.launchFailed && !result.LaunchAt.IsZero() &&
		!result.Diagnostic.IsZero() && !result.RuntimeIDs.IsZero() {
		evidence := CodexLaunchEvidence{At: result.LaunchAt,
			Diagnostic: result.Diagnostic, RuntimeIDs: result.RuntimeIDs}
		transition, err := worker.recordLaunch(run, fence, evidence)
		if err == nil && validWakeWorkerLaunchTransition(transition, run, evidence) {
			callback.launchFailed = false
		}
		// A failed replay remains a callback failure, but does not bypass
		// process-exit settlement. Unlike wake evidence, FailAgentRuntime
		// carries and atomically verifies this same launch identity, so it can
		// still close the Run without producing a contradictory receipt.
	}
	if callback.wakeFailed {
		if !result.WakeDelivered || result.WakeAt.IsZero() || result.WakeReceipt.IsZero() {
			return worker.backoffInterval, wakeWorkerIssueCallback
		}
		evidence := CodexWakeEvidence{At: result.WakeAt, Receipt: result.WakeReceipt}
		transition, err := worker.recordWake(run, fence, evidence)
		if err != nil || !validWakeWorkerWakeTransition(transition, run, evidence, true) {
			// The completion receipt now truthfully says that the Hook cue was
			// observed. It cannot be persisted until the exact durable wake
			// callback is proved Applied or Replayed.
			return worker.backoffInterval, wakeWorkerIssueCallback
		}
		callback.wakeFailed = false
	}
	durableCallbackFailed := callback.launchFailed || callback.wakeFailed
	if !result.ProcessExited || result.CompletionReceipt.IsZero() || result.At.IsZero() {
		return worker.backoffInterval, wakeWorkerIssueRuntimeUnproven
	}
	if adapterErr == nil && !durableCallbackFailed && result.WakeDelivered {
		transition, err := worker.finishRuntime(run, fence, result)
		if err != nil || !validWakeWorkerFinishTransition(transition, run, result) {
			return worker.backoffInterval, wakeWorkerIssueDurable
		}
		worker.setState(true, false, "")
		return worker.pollInterval, ""
	}
	errorText := wakeWorkerAdapterFailure
	if durableCallbackFailed {
		errorText = wakeWorkerCallbackFailure
	}
	transition, err := worker.failRuntime(run, fence, result, errorText)
	if err != nil || !validWakeWorkerFailureTransition(transition, run, result, errorText) {
		return worker.backoffInterval, wakeWorkerIssueDurable
	}
	if durableCallbackFailed {
		return worker.backoffInterval, wakeWorkerIssueCallback
	}
	if adapterErr == nil {
		return worker.backoffInterval, wakeWorkerIssueAdapterInvariant
	}
	worker.setState(false, false, wakeWorkerIssueAdapter)
	return worker.backoffInterval, ""
}

func (worker *WakeWorker) failPreparedRun(prepared PreparedWake) string {
	run := prepared.Run()
	fence, ok := worker.runAuthority(run)
	if !ok {
		return wakeWorkerIssueDurable
	}
	at, err := worker.trustedNow()
	if err != nil {
		return wakeWorkerIssueAdapterInvariant
	}
	receipt, err := codexCompletionReceipt("launch_failed", "", "", false,
		"not_started", nil)
	if err != nil {
		return wakeWorkerIssueAdapterInvariant
	}
	diagnostic, err := model.JSONFrom(struct {
		Adapter       string `json:"adapter"`
		Failure       string `json:"failure"`
		SchemaVersion int    `json:"schema_version"`
	}{codexAdapterName, wakeWorkerIssuePrepareRun, 1})
	if err != nil {
		return wakeWorkerIssueAdapterInvariant
	}
	empty, err := model.NewJSON([]byte(`{}`))
	if err != nil {
		return wakeWorkerIssueAdapterInvariant
	}
	result := CodexWakeResult{At: at, Diagnostic: diagnostic, RuntimeIDs: empty,
		CompletionReceipt: receipt, ProcessExited: true}
	transition, err := worker.failRuntime(run, fence, result, wakeWorkerPrepareFailure)
	if err != nil || !validWakeWorkerFailureTransition(transition, run, result,
		wakeWorkerPrepareFailure) {
		return wakeWorkerIssueDurable
	}
	return ""
}

func (worker *WakeWorker) runAuthority(run model.AgentRun) (model.Digest, bool) {
	fence, hasFence := run.ClaimFenceHash()
	_, hasHandling := run.HandlingID()
	return fence, hasFence && hasHandling && !fence.IsZero() && !run.ID().IsZero() &&
		run.ProfileID() == worker.profile.ID() && run.Runtime() == worker.profile.Runtime() &&
		run.Launcher() == "mnemond-wake"
}

func (worker *WakeWorker) trustedNow() (time.Time, error) {
	return canonicalRuntimeProcessTime(worker.clock.Now())
}

func (worker *WakeWorker) abandonUnregistered(run model.AgentRun, fence model.Digest,
	at time.Time,
) (store.AgentRuntimeTransitionResult, error) {
	spec := store.AgentUnregisteredRunSpec{
		ProfileID: worker.profile.ID(), ExpectedAssetRevision: worker.assetRevision,
		RunID: run.ID(), ClaimFenceHash: fence, HandlingRecovery: run.HandlingRecovery(),
		Error: wakeWorkerUnregistered, At: at}
	return worker.retryTransition(func(ctx context.Context) (store.AgentRuntimeTransitionResult, error) {
		return worker.store.AbandonUnregisteredAgentRun(ctx, spec)
	})
}

func (worker *WakeWorker) settleOrphan(run model.AgentRun, fence model.Digest,
	recovery runtimeProcessRecovery,
) (store.AgentRuntimeTransitionResult, error) {
	spec := store.AgentRuntimeOrphanSpec{
		ProfileID: worker.profile.ID(), ExpectedAssetRevision: worker.assetRevision,
		RunID: run.ID(), ClaimFenceHash: fence, HandlingRecovery: run.HandlingRecovery(),
		CompletionReceipt: recovery.Receipt, Error: wakeWorkerRecoveryFailure, At: recovery.At}
	return worker.retryTransition(func(ctx context.Context) (store.AgentRuntimeTransitionResult, error) {
		return worker.store.SettleOrphanedAgentRuntime(ctx, spec)
	})
}

func (worker *WakeWorker) finishRuntime(run model.AgentRun, fence model.Digest,
	result CodexWakeResult,
) (store.AgentRuntimeTransitionResult, error) {
	spec := store.AgentRuntimeFinishSpec{
		ProfileID: worker.profile.ID(), ExpectedAssetRevision: worker.assetRevision,
		RunID: run.ID(), ClaimFenceHash: fence, HandlingRecovery: run.HandlingRecovery(),
		CompletionReceipt: result.CompletionReceipt, At: result.At}
	return worker.retryTransition(func(ctx context.Context) (store.AgentRuntimeTransitionResult, error) {
		return worker.store.FinishAgentRuntime(ctx, spec)
	})
}

func (worker *WakeWorker) failRuntime(run model.AgentRun, fence model.Digest,
	result CodexWakeResult, errorText string,
) (store.AgentRuntimeTransitionResult, error) {
	diagnostic, runtimeIDs := result.Diagnostic, result.RuntimeIDs
	if diagnostic.IsZero() {
		diagnostic, _ = model.NewJSON([]byte(`{}`))
	}
	if runtimeIDs.IsZero() {
		runtimeIDs, _ = model.NewJSON([]byte(`{}`))
	}
	spec := store.AgentRuntimeFailureSpec{
		ProfileID: worker.profile.ID(), ExpectedAssetRevision: worker.assetRevision,
		RunID: run.ID(), ClaimFenceHash: fence, HandlingRecovery: run.HandlingRecovery(),
		LauncherDiagnostic: diagnostic, RuntimeIDs: runtimeIDs,
		CompletionReceipt: result.CompletionReceipt, Error: errorText, At: result.At}
	return worker.retryTransition(func(ctx context.Context) (store.AgentRuntimeTransitionResult, error) {
		return worker.store.FailAgentRuntime(ctx, spec)
	})
}

func (worker *WakeWorker) recordWake(run model.AgentRun, fence model.Digest,
	evidence CodexWakeEvidence,
) (store.AgentRuntimeTransitionResult, error) {
	spec := store.AgentWakeDeliverySpec{ProfileID: worker.profile.ID(),
		ExpectedAssetRevision: worker.assetRevision, RunID: run.ID(),
		ClaimFenceHash: fence, HandlingRecovery: run.HandlingRecovery(),
		WakeReceipt: evidence.Receipt, At: evidence.At}
	return worker.retryTransition(func(ctx context.Context) (store.AgentRuntimeTransitionResult, error) {
		return worker.store.RecordAgentWakeDelivery(ctx, spec)
	})
}

func (worker *WakeWorker) recordLaunch(run model.AgentRun, fence model.Digest,
	evidence CodexLaunchEvidence,
) (store.AgentRuntimeTransitionResult, error) {
	spec := store.AgentRuntimeLaunchSpec{ProfileID: worker.profile.ID(),
		ExpectedAssetRevision: worker.assetRevision, RunID: run.ID(),
		ClaimFenceHash: fence, HandlingRecovery: run.HandlingRecovery(),
		LauncherDiagnostic: evidence.Diagnostic, RuntimeIDs: evidence.RuntimeIDs, At: evidence.At}
	return worker.retryTransition(func(ctx context.Context) (store.AgentRuntimeTransitionResult, error) {
		return worker.store.RecordAgentRuntimeLaunch(ctx, spec)
	})
}

type wakeWorkerTransitionCall func(context.Context) (store.AgentRuntimeTransitionResult, error)

func (worker *WakeWorker) retryTransition(call wakeWorkerTransitionCall,
) (store.AgentRuntimeTransitionResult, error) {
	var result store.AgentRuntimeTransitionResult
	var err error
	for attempt := 0; attempt < wakeWorkerSettlementAttempts; attempt++ {
		settlementCtx, cancel := context.WithTimeout(context.Background(), worker.settlementTimeout)
		result, err = call(settlementCtx)
		cancel()
		if err == nil {
			return result, nil
		}
	}
	return result, err
}

func validWakeWorkerAuthority(result store.AgentRuntimeTransitionResult,
	expected model.AgentRun,
) bool {
	if (result.Status != store.AgentRuntimeApplied && result.Status != store.AgentRuntimeReplayed) ||
		result.Run.ID() != expected.ID() || result.Run.ProfileID() != expected.ProfileID() ||
		result.Run.Runtime() != expected.Runtime() || result.Run.Launcher() != expected.Launcher() ||
		result.Run.Cause().String() != expected.Cause().String() ||
		result.Run.HandlingAttempt() != expected.HandlingAttempt() ||
		result.Run.HandlingRecovery() != expected.HandlingRecovery() ||
		!result.Run.StartedAt().Equal(expected.StartedAt()) {
		return false
	}
	expectedHandling, expectedHasHandling := expected.HandlingID()
	storedHandling, storedHasHandling := result.Run.HandlingID()
	expectedFence, expectedHasFence := expected.ClaimFenceHash()
	storedFence, storedHasFence := result.Run.ClaimFenceHash()
	expectedLease, expectedHasLease := expected.LeaseUntil()
	storedLease, storedHasLease := result.Run.LeaseUntil()
	expectedAttachment, expectedHasAttachment := expected.AttachmentTokenHash()
	storedAttachment, storedHasAttachment := result.Run.AttachmentTokenHash()
	expectedExpiry, expectedHasExpiry := expected.AttachmentExpiresAt()
	storedExpiry, storedHasExpiry := result.Run.AttachmentExpiresAt()
	if !expectedHasHandling || !storedHasHandling || storedHandling != expectedHandling ||
		expectedHasFence != storedHasFence || !expectedHasFence || storedFence != expectedFence ||
		expectedHasLease != storedHasLease || !expectedHasLease || !storedLease.Equal(expectedLease) ||
		expectedHasAttachment != storedHasAttachment || !expectedHasAttachment ||
		storedAttachment != expectedAttachment || expectedHasExpiry != storedHasExpiry ||
		!expectedHasExpiry || !storedExpiry.Equal(expectedExpiry) {
		return false
	}
	handling := result.Handling
	if handling.ID() != expectedHandling || handling.ProfileID() != expected.ProfileID() ||
		handling.RecoveryCount() < expected.HandlingRecovery() ||
		handling.RecoveryCount() == expected.HandlingRecovery() &&
			handling.Attempts() < expected.HandlingAttempt() ||
		handling.UpdatedAt().Before(expected.StartedAt()) {
		return false
	}
	return true
}

func validWakeWorkerAbandonTransition(result store.AgentRuntimeTransitionResult,
	expected model.AgentRun, errorText string, at time.Time,
) bool {
	if !validWakeWorkerAuthority(result, expected) || !result.Run.Status().Terminal() ||
		(result.Run.Status() != model.AgentRunFailed && result.Run.Status() != model.AgentRunDead) ||
		result.Run.Error() != errorText || result.Run.LauncherDiagnostic().String() != `{}` ||
		result.Run.RuntimeIDs().String() != `{}` {
		return false
	}
	finishedAt, finished := result.Run.FinishedAt()
	_, runtimeStarted := result.Run.RuntimeStartedAt()
	_, attached := result.Run.AttachedAt()
	_, wakeDelivered := result.Run.WakeDeliveredAt()
	_, wakeReceipt := result.Run.WakeReceipt()
	_, currentRead := result.Run.CurrentReadReceipt()
	_, outcome := result.Run.OutcomeReceipt()
	_, completionAt := result.Run.CompletionAt()
	_, completion := result.Run.CompletionReceipt()
	return finished && finishedAt.Equal(at) && !runtimeStarted && !attached && !wakeDelivered &&
		!wakeReceipt && !currentRead && !outcome && !completionAt && !completion
}

func validWakeWorkerOrphanTransition(result store.AgentRuntimeTransitionResult,
	expected model.AgentRun, recovery runtimeProcessRecovery, errorText string,
) bool {
	if !validWakeWorkerAuthority(result, expected) || !result.Run.Status().Terminal() ||
		result.Run.Error() != errorText ||
		result.Run.LauncherDiagnostic().String() != expected.LauncherDiagnostic().String() ||
		result.Run.RuntimeIDs().String() != expected.RuntimeIDs().String() {
		return false
	}
	finishedAt, finished := result.Run.FinishedAt()
	completionAt, completedAt := result.Run.CompletionAt()
	completion, completed := result.Run.CompletionReceipt()
	return finished && !finishedAt.After(recovery.At) && completedAt &&
		completionAt.Equal(recovery.At) && completed && completion.String() == recovery.Receipt.String()
}

func validWakeWorkerLaunchTransition(result store.AgentRuntimeTransitionResult,
	expected model.AgentRun, evidence CodexLaunchEvidence,
) bool {
	if !validWakeWorkerAuthority(result, expected) || result.Run.Status() != model.AgentRunRunning ||
		result.Run.LauncherDiagnostic().String() != evidence.Diagnostic.String() ||
		result.Run.RuntimeIDs().String() != evidence.RuntimeIDs.String() {
		return false
	}
	startedAt, started := result.Run.RuntimeStartedAt()
	_, finished := result.Run.FinishedAt()
	_, completion := result.Run.CompletionReceipt()
	fence, _ := expected.ClaimFenceHash()
	handlingFence, hasHandlingFence := result.Handling.ClaimTokenHash()
	handlingLease, hasHandlingLease := result.Handling.LeaseUntil()
	expectedLease, _ := expected.LeaseUntil()
	return started && startedAt.Equal(evidence.At) && !finished && !completion &&
		result.Handling.Status() == model.HandlingClaimed && hasHandlingFence &&
		handlingFence == fence && hasHandlingLease && handlingLease.Equal(expectedLease) &&
		handlingLease.After(evidence.At) &&
		result.Handling.Attempts() == expected.HandlingAttempt() &&
		result.Handling.RecoveryCount() == expected.HandlingRecovery()
}

func validWakeWorkerWakeTransition(result store.AgentRuntimeTransitionResult,
	expected model.AgentRun, evidence CodexWakeEvidence, allowTerminal bool,
) bool {
	if !validWakeWorkerAuthority(result, expected) {
		return false
	}
	wakeAt, delivered := result.Run.WakeDeliveredAt()
	wakeReceipt, hasReceipt := result.Run.WakeReceipt()
	startedAt, started := result.Run.RuntimeStartedAt()
	_, completion := result.Run.CompletionReceipt()
	if !delivered || !wakeAt.Equal(evidence.At) || !hasReceipt ||
		wakeReceipt.String() != evidence.Receipt.String() || !started || startedAt.After(evidence.At) ||
		completion {
		return false
	}
	if result.Run.Status() == model.AgentRunRunning {
		fence, _ := expected.ClaimFenceHash()
		handlingFence, hasFence := result.Handling.ClaimTokenHash()
		handlingLease, hasLease := result.Handling.LeaseUntil()
		expectedLease, _ := expected.LeaseUntil()
		return result.Handling.Status() == model.HandlingClaimed && hasFence && handlingFence == fence &&
			hasLease && handlingLease.Equal(expectedLease) && handlingLease.After(evidence.At) &&
			result.Handling.Attempts() == expected.HandlingAttempt() &&
			result.Handling.RecoveryCount() == expected.HandlingRecovery()
	}
	return allowTerminal && result.Run.Status().Terminal()
}

func validWakeWorkerFinishTransition(result store.AgentRuntimeTransitionResult,
	expected model.AgentRun, wakeResult CodexWakeResult,
) bool {
	if !validWakeWorkerAuthority(result, expected) ||
		(result.Run.Status() != model.AgentRunRuntimeFinished && !result.Run.Status().Terminal()) ||
		result.Run.Error() != "" || !wakeResult.WakeDelivered || wakeResult.WakeAt.IsZero() ||
		wakeResult.WakeReceipt.IsZero() ||
		result.Run.LauncherDiagnostic().String() != wakeResult.Diagnostic.String() ||
		result.Run.RuntimeIDs().String() != wakeResult.RuntimeIDs.String() {
		return false
	}
	finishedAt, finished := result.Run.FinishedAt()
	completionAt, completedAt := result.Run.CompletionAt()
	completion, completed := result.Run.CompletionReceipt()
	wakeAt, delivered := result.Run.WakeDeliveredAt()
	wakeReceipt, hasWakeReceipt := result.Run.WakeReceipt()
	return finished && !finishedAt.After(wakeResult.At) && completedAt && completionAt.Equal(wakeResult.At) &&
		completed && completion.String() == wakeResult.CompletionReceipt.String() && delivered &&
		wakeAt.Equal(wakeResult.WakeAt) && !wakeAt.After(wakeResult.At) && hasWakeReceipt &&
		wakeReceipt.String() == wakeResult.WakeReceipt.String()
}

func validWakeWorkerFailureTransition(result store.AgentRuntimeTransitionResult,
	expected model.AgentRun, wakeResult CodexWakeResult, errorText string,
) bool {
	if !validWakeWorkerAuthority(result, expected) || !result.Run.Status().Terminal() ||
		result.Run.Error() != errorText {
		return false
	}
	diagnostic, runtimeIDs := wakeResult.Diagnostic, wakeResult.RuntimeIDs
	if diagnostic.IsZero() {
		diagnostic, _ = model.NewJSON([]byte(`{}`))
	}
	if runtimeIDs.IsZero() {
		runtimeIDs, _ = model.NewJSON([]byte(`{}`))
	}
	if result.Run.LauncherDiagnostic().String() != diagnostic.String() ||
		result.Run.RuntimeIDs().String() != runtimeIDs.String() {
		return false
	}
	finishedAt, finished := result.Run.FinishedAt()
	completionAt, completedAt := result.Run.CompletionAt()
	completion, completed := result.Run.CompletionReceipt()
	wakeAt, delivered := result.Run.WakeDeliveredAt()
	wakeReceipt, hasWakeReceipt := result.Run.WakeReceipt()
	if !finished || finishedAt.After(wakeResult.At) || !completedAt ||
		!completionAt.Equal(wakeResult.At) || !completed ||
		completion.String() != wakeResult.CompletionReceipt.String() ||
		delivered != wakeResult.WakeDelivered || hasWakeReceipt != wakeResult.WakeDelivered {
		return false
	}
	if !wakeResult.WakeDelivered {
		return wakeResult.WakeAt.IsZero() && wakeResult.WakeReceipt.IsZero()
	}
	return !wakeResult.WakeReceipt.IsZero() && wakeReceipt.String() == wakeResult.WakeReceipt.String() &&
		wakeAt.Equal(wakeResult.WakeAt) && !wakeAt.After(wakeResult.At)
}
