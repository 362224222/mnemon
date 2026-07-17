package agent

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestWakeWorkerGatesBeforePrepareAndUsesBoundedPolling(t *testing.T) {
	at := time.Date(2026, 7, 18, 9, 0, 0, 0, time.UTC)
	profile := wakeWorkerTestProfile(t, at)
	st := newWakeWorkerTestStore()
	var sequenceMu sync.Mutex
	var sequence []string
	appendSequence := func(value string) {
		sequenceMu.Lock()
		sequence = append(sequence, value)
		sequenceMu.Unlock()
	}
	timer := newWakeWorkerTestTimer()
	ctx, cancel := context.WithCancel(context.Background())
	var prepareCalls atomic.Int32
	preparer := wakeWorkerPreparerFunc(func(context.Context, model.Profile) (PreparedWake, error) {
		appendSequence("prepare")
		if prepareCalls.Add(1) == 2 {
			cancel()
		}
		return PreparedWake{status: store.AgentClaimNone}, nil
	})
	worker := newWakeWorkerForTest(t, profile, st, preparer,
		wakeWorkerAdapterFunc(func(context.Context, CodexWakeRequest) (CodexWakeResult, error) {
			t.Fatal("adapter ran without actionable work")
			return CodexWakeResult{}, nil
		}), WakeWorkerGateFunc(func(context.Context, model.Profile) error {
			appendSequence("gate")
			return nil
		}), timer)
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	if delay := <-timer.calls; delay != worker.pollInterval {
		t.Fatalf("first delay = %s, want poll %s", delay, worker.pollInterval)
	}
	timer.tick()
	if err := <-done; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	sequenceMu.Lock()
	got := append([]string(nil), sequence...)
	sequenceMu.Unlock()
	if strings.Join(got, ",") != "gate,prepare,gate,prepare" {
		t.Fatalf("tick sequence = %v", got)
	}
	if snapshot := worker.Snapshot(); snapshot.Running || !snapshot.Healthy || snapshot.Ready {
		t.Fatalf("stopped snapshot = %#v", snapshot)
	}
}

func TestWakeWorkerTransitionValidatorFixture(t *testing.T) {
	at := time.Date(2026, 7, 18, 9, 30, 0, 0, time.UTC)
	profile := wakeWorkerTestProfile(t, at)
	run := wakeWorkerTestRun(t, profile, "run-worker-validator", at, false)
	st := newWakeWorkerTestStore(run)
	evidence := CodexLaunchEvidence{At: at.Add(time.Second),
		Diagnostic: wakeWorkerJSON(t, `{"adapter":"test"}`),
		RuntimeIDs: wakeWorkerJSON(t, `{"runtime":"test"}`)}
	fence, _ := run.ClaimFenceHash()
	transition, err := st.RecordAgentRuntimeLaunch(context.Background(), store.AgentRuntimeLaunchSpec{
		ProfileID: profile.ID(), ExpectedAssetRevision: profile.ActiveAssetRevision(), RunID: run.ID(),
		ClaimFenceHash: fence, HandlingRecovery: run.HandlingRecovery(),
		LauncherDiagnostic: evidence.Diagnostic, RuntimeIDs: evidence.RuntimeIDs, At: evidence.At})
	if err != nil || !validWakeWorkerLaunchTransition(transition, run, evidence) {
		t.Fatalf("launch validator = %t, error=%v, run=%#v, handling=%#v",
			validWakeWorkerLaunchTransition(transition, run, evidence), err, transition.Run, transition.Handling)
	}
	alreadySettled := transition
	alreadySettled.Status = store.AgentRuntimeAlreadySettled
	if validWakeWorkerLaunchTransition(alreadySettled, run, evidence) {
		t.Fatal("launch validator accepted already_settled")
	}
	wakeEvidence := CodexWakeEvidence{At: at.Add(2 * time.Second),
		Receipt: wakeWorkerJSON(t, `{"wake":"test"}`)}
	transition, err = st.RecordAgentWakeDelivery(context.Background(), store.AgentWakeDeliverySpec{
		ProfileID: profile.ID(), ExpectedAssetRevision: profile.ActiveAssetRevision(), RunID: run.ID(),
		ClaimFenceHash: fence, HandlingRecovery: run.HandlingRecovery(), WakeReceipt: wakeEvidence.Receipt,
		At: wakeEvidence.At})
	if err != nil || !validWakeWorkerWakeTransition(transition, run, wakeEvidence, false) {
		t.Fatalf("wake validator = %t, error=%v, run=%#v, handling=%#v",
			validWakeWorkerWakeTransition(transition, run, wakeEvidence, false), err,
			transition.Run, transition.Handling)
	}
	alreadySettled = transition
	alreadySettled.Status = store.AgentRuntimeAlreadySettled
	if validWakeWorkerWakeTransition(alreadySettled, run, wakeEvidence, false) {
		t.Fatal("wake validator accepted already_settled")
	}
}

func TestWakeWorkerRetriesSettlementCommitResponseLossWithExactEvidence(t *testing.T) {
	at := time.Date(2026, 7, 18, 9, 40, 0, 0, time.UTC)
	profile := wakeWorkerTestProfile(t, at)
	lostResponse := errors.New("durable commit response lost")

	t.Run("abandon", func(t *testing.T) {
		run := wakeWorkerTestRun(t, profile, "run-worker-retry-abandon", at, false)
		base := newWakeWorkerTestStore(run)
		var calls atomic.Int32
		st := &wakeWorkerStoreOverrides{WakeWorkerStore: base}
		st.abandon = func(ctx context.Context,
			spec store.AgentUnregisteredRunSpec,
		) (store.AgentRuntimeTransitionResult, error) {
			result, err := base.AbandonUnregisteredAgentRun(ctx, spec)
			if err == nil && calls.Add(1) == 1 {
				return store.AgentRuntimeTransitionResult{}, lostResponse
			}
			return result, err
		}
		worker := newWakeWorkerTransitionTestWorker(t, profile, st)
		fence, _ := run.ClaimFenceHash()
		settledAt := at.Add(time.Second)
		transition, err := worker.abandonUnregistered(run, fence, settledAt)
		if err != nil || calls.Load() != wakeWorkerSettlementAttempts ||
			transition.Status != store.AgentRuntimeReplayed ||
			!validWakeWorkerAbandonTransition(transition, run, wakeWorkerUnregistered, settledAt) {
			t.Fatalf("abandon retry = (%#v, %v), calls=%d", transition, err, calls.Load())
		}
		transition.Status = store.AgentRuntimeAlreadySettled
		if validWakeWorkerAbandonTransition(transition, run, wakeWorkerUnregistered, settledAt) {
			t.Fatal("abandon validator accepted already_settled")
		}
	})

	t.Run("orphan", func(t *testing.T) {
		run := wakeWorkerTestRun(t, profile, "run-worker-retry-orphan", at, true)
		base := newWakeWorkerTestStore(run)
		var calls atomic.Int32
		st := &wakeWorkerStoreOverrides{WakeWorkerStore: base}
		st.settle = func(ctx context.Context,
			spec store.AgentRuntimeOrphanSpec,
		) (store.AgentRuntimeTransitionResult, error) {
			result, err := base.SettleOrphanedAgentRuntime(ctx, spec)
			if err == nil && calls.Add(1) == 1 {
				return store.AgentRuntimeTransitionResult{}, lostResponse
			}
			return result, err
		}
		worker := newWakeWorkerTransitionTestWorker(t, profile, st)
		fence, _ := run.ClaimFenceHash()
		recovery := runtimeProcessRecovery{State: runtimeProcessGone,
			Receipt: wakeWorkerJSON(t, `{"process_exit":"confirmed"}`), At: at.Add(2 * time.Second)}
		transition, err := worker.settleOrphan(run, fence, recovery)
		if err != nil || calls.Load() != wakeWorkerSettlementAttempts ||
			transition.Status != store.AgentRuntimeReplayed ||
			!validWakeWorkerOrphanTransition(transition, run, recovery, wakeWorkerRecoveryFailure) {
			t.Fatalf("orphan retry = (%#v, %v), calls=%d", transition, err, calls.Load())
		}
		transition.Status = store.AgentRuntimeAlreadySettled
		if validWakeWorkerOrphanTransition(transition, run, recovery, wakeWorkerRecoveryFailure) {
			t.Fatal("orphan validator accepted already_settled")
		}
	})

	t.Run("finish", func(t *testing.T) {
		run := wakeWorkerTestRun(t, profile, "run-worker-retry-finish", at, true)
		base := newWakeWorkerTestStore(run)
		fence, _ := run.ClaimFenceHash()
		wakeEvidence := CodexWakeEvidence{At: at.Add(2 * time.Second),
			Receipt: wakeWorkerJSON(t, `{"wake":"test"}`)}
		if _, err := base.RecordAgentWakeDelivery(context.Background(), store.AgentWakeDeliverySpec{
			ProfileID: profile.ID(), ExpectedAssetRevision: profile.ActiveAssetRevision(),
			RunID: run.ID(), ClaimFenceHash: fence, HandlingRecovery: run.HandlingRecovery(),
			WakeReceipt: wakeEvidence.Receipt, At: wakeEvidence.At}); err != nil {
			t.Fatal(err)
		}
		completion, err := codexCompletionReceipt("completed", "thread-worker", "turn-worker", true,
			"wait_without_signal", nil)
		if err != nil {
			t.Fatal(err)
		}
		wakeResult := CodexWakeResult{At: at.Add(3 * time.Second), WakeAt: wakeEvidence.At,
			Diagnostic: run.LauncherDiagnostic(), RuntimeIDs: run.RuntimeIDs(),
			WakeReceipt: wakeEvidence.Receipt, CompletionReceipt: completion,
			WakeDelivered: true, ProcessExited: true}
		var calls atomic.Int32
		st := &wakeWorkerStoreOverrides{WakeWorkerStore: base}
		st.finish = func(ctx context.Context,
			spec store.AgentRuntimeFinishSpec,
		) (store.AgentRuntimeTransitionResult, error) {
			result, err := base.FinishAgentRuntime(ctx, spec)
			if err == nil && calls.Add(1) == 1 {
				return store.AgentRuntimeTransitionResult{}, lostResponse
			}
			return result, err
		}
		worker := newWakeWorkerTransitionTestWorker(t, profile, st)
		transition, err := worker.finishRuntime(run, fence, wakeResult)
		if err != nil || calls.Load() != wakeWorkerSettlementAttempts ||
			transition.Status != store.AgentRuntimeReplayed ||
			!validWakeWorkerFinishTransition(transition, run, wakeResult) {
			t.Fatalf("finish retry = (%#v, %v), calls=%d", transition, err, calls.Load())
		}
		transition.Status = store.AgentRuntimeAlreadySettled
		if validWakeWorkerFinishTransition(transition, run, wakeResult) {
			t.Fatal("finish validator accepted already_settled")
		}
	})

	t.Run("failure", func(t *testing.T) {
		run := wakeWorkerTestRun(t, profile, "run-worker-retry-failure", at, true)
		base := newWakeWorkerTestStore(run)
		fence, _ := run.ClaimFenceHash()
		completion, err := codexCompletionReceipt("failed", "thread-worker", "turn-worker", false,
			"wait_without_signal", nil)
		if err != nil {
			t.Fatal(err)
		}
		wakeResult := CodexWakeResult{At: at.Add(3 * time.Second),
			Diagnostic: run.LauncherDiagnostic(), RuntimeIDs: run.RuntimeIDs(),
			CompletionReceipt: completion, ProcessExited: true}
		var calls atomic.Int32
		st := &wakeWorkerStoreOverrides{WakeWorkerStore: base}
		st.fail = func(ctx context.Context,
			spec store.AgentRuntimeFailureSpec,
		) (store.AgentRuntimeTransitionResult, error) {
			result, err := base.FailAgentRuntime(ctx, spec)
			if err == nil && calls.Add(1) == 1 {
				return store.AgentRuntimeTransitionResult{}, lostResponse
			}
			return result, err
		}
		worker := newWakeWorkerTransitionTestWorker(t, profile, st)
		transition, err := worker.failRuntime(run, fence, wakeResult, wakeWorkerAdapterFailure)
		if err != nil || calls.Load() != wakeWorkerSettlementAttempts ||
			transition.Status != store.AgentRuntimeReplayed ||
			!validWakeWorkerFailureTransition(transition, run, wakeResult, wakeWorkerAdapterFailure) {
			t.Fatalf("failure retry = (%#v, %v), calls=%d", transition, err, calls.Load())
		}
		transition.Status = store.AgentRuntimeAlreadySettled
		if validWakeWorkerFailureTransition(transition, run, wakeResult, wakeWorkerAdapterFailure) {
			t.Fatal("failure validator accepted already_settled")
		}
	})

	t.Run("bounded", func(t *testing.T) {
		run := wakeWorkerTestRun(t, profile, "run-worker-retry-bounded", at, true)
		base := newWakeWorkerTestStore(run)
		var calls atomic.Int32
		st := &wakeWorkerStoreOverrides{WakeWorkerStore: base}
		st.finish = func(context.Context,
			store.AgentRuntimeFinishSpec,
		) (store.AgentRuntimeTransitionResult, error) {
			calls.Add(1)
			return store.AgentRuntimeTransitionResult{}, lostResponse
		}
		worker := newWakeWorkerTransitionTestWorker(t, profile, st)
		fence, _ := run.ClaimFenceHash()
		_, err := worker.finishRuntime(run, fence, CodexWakeResult{
			At: at.Add(3 * time.Second), CompletionReceipt: wakeWorkerJSON(t, `{"done":true}`)})
		if !errors.Is(err, lostResponse) || calls.Load() != wakeWorkerSettlementAttempts {
			t.Fatalf("bounded retry error/calls = %v/%d", err, calls.Load())
		}
	})
}

func TestWakeWorkerFailsClosedOnAlreadySettledFinalResponse(t *testing.T) {
	at := time.Date(2026, 7, 18, 9, 50, 0, 0, time.UTC)
	profile := wakeWorkerTestProfile(t, at)
	run := wakeWorkerTestRun(t, profile, "run-worker-already-settled", at, false)
	prepared := wakeWorkerPrepared(t, run)
	base := newWakeWorkerTestStore(run)
	var prepareCalls atomic.Int32
	st := &wakeWorkerStoreOverrides{WakeWorkerStore: base}
	st.finish = func(ctx context.Context,
		spec store.AgentRuntimeFinishSpec,
	) (store.AgentRuntimeTransitionResult, error) {
		result, err := base.FinishAgentRuntime(ctx, spec)
		result.Status = store.AgentRuntimeAlreadySettled
		return result, err
	}
	diagnostic := wakeWorkerJSON(t, `{"adapter":"test"}`)
	runtimeIDs := wakeWorkerJSON(t, `{"runtime":"test"}`)
	wakeReceipt := wakeWorkerJSON(t, `{"wake":"test"}`)
	completion, err := codexCompletionReceipt("completed", "thread-worker", "turn-worker", true,
		"wait_without_signal", nil)
	if err != nil {
		t.Fatal(err)
	}
	worker := newWakeWorkerForTest(t, profile, st,
		wakeWorkerPreparerFunc(func(context.Context, model.Profile) (PreparedWake, error) {
			prepareCalls.Add(1)
			return prepared, nil
		}), wakeWorkerAdapterFunc(func(adapterCtx context.Context,
			request CodexWakeRequest,
		) (CodexWakeResult, error) {
			if err := request.Callbacks.RecordLaunch(adapterCtx, CodexLaunchEvidence{At: at.Add(time.Second),
				Diagnostic: diagnostic, RuntimeIDs: runtimeIDs}); err != nil {
				t.Fatal(err)
			}
			if err := request.Callbacks.RecordWake(adapterCtx, CodexWakeEvidence{At: at.Add(2 * time.Second),
				Receipt: wakeReceipt}); err != nil {
				t.Fatal(err)
			}
			return CodexWakeResult{At: at.Add(3 * time.Second), Diagnostic: diagnostic,
				RuntimeIDs: runtimeIDs, WakeAt: at.Add(2 * time.Second), WakeReceipt: wakeReceipt,
				CompletionReceipt: completion, WakeDelivered: true, ProcessExited: true}, nil
		}), WakeWorkerGateFunc(func(context.Context, model.Profile) error { return nil }),
		newWakeWorkerTestTimer())
	if err := worker.Run(context.Background()); !errors.Is(err, ErrWakeWorker) {
		t.Fatalf("Run() error = %v", err)
	}
	if prepareCalls.Load() != 1 {
		t.Fatalf("Prepare calls after already_settled = %d", prepareCalls.Load())
	}
	if snapshot := worker.Snapshot(); snapshot.Healthy || snapshot.Ready ||
		snapshot.LastError != wakeWorkerIssueDurable {
		t.Fatalf("worker snapshot = %#v", snapshot)
	}
}

func TestWakeWorkerRecordsCallbacksAndSettlesNormalOrFailedRuntime(t *testing.T) {
	at := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name       string
		adapterErr error
		wantFinal  string
		wantError  string
	}{
		{name: "normal", wantFinal: "finish"},
		{name: "adapter failure", adapterErr: errors.New("private /operator/path"),
			wantFinal: "fail", wantError: wakeWorkerAdapterFailure},
	} {
		t.Run(test.name, func(t *testing.T) {
			profile := wakeWorkerTestProfile(t, at)
			run := wakeWorkerTestRun(t, profile, "run-worker-"+strings.ReplaceAll(test.name, " ", "-"),
				at, false)
			prepared := wakeWorkerPrepared(t, run)
			st := newWakeWorkerTestStore(run)
			ctx, cancel := context.WithCancel(context.Background())
			var sequenceMu sync.Mutex
			var sequence []string
			appendSequence := func(value string) {
				sequenceMu.Lock()
				sequence = append(sequence, value)
				sequenceMu.Unlock()
			}
			st.onLaunch = func(callbackCtx context.Context, spec store.AgentRuntimeLaunchSpec) error {
				if callbackCtx != ctx || spec.RunID != run.ID() {
					t.Errorf("launch callback authority = %#v, ctx equal=%t", spec, callbackCtx == ctx)
				}
				appendSequence("launch")
				return nil
			}
			st.onWake = func(callbackCtx context.Context, spec store.AgentWakeDeliverySpec) error {
				if callbackCtx != ctx || spec.RunID != run.ID() {
					t.Errorf("wake callback authority = %#v, ctx equal=%t", spec, callbackCtx == ctx)
				}
				appendSequence("wake")
				return nil
			}
			st.onFinish = func(settleCtx context.Context, spec store.AgentRuntimeFinishSpec) error {
				appendSequence("finish")
				if settleCtx.Err() != nil || spec.RunID != run.ID() || spec.CompletionReceipt.IsZero() {
					t.Errorf("finish settlement = %#v, ctx=%v", spec, settleCtx.Err())
				}
				return nil
			}
			st.onFail = func(settleCtx context.Context, spec store.AgentRuntimeFailureSpec) error {
				appendSequence("fail")
				if settleCtx.Err() != nil || spec.RunID != run.ID() || spec.Error != test.wantError ||
					strings.Contains(spec.Error, "operator") {
					t.Errorf("failure settlement = %#v, ctx=%v", spec, settleCtx.Err())
				}
				return nil
			}
			adapter := wakeWorkerAdapterFunc(func(adapterCtx context.Context,
				request CodexWakeRequest,
			) (CodexWakeResult, error) {
				appendSequence("adapter")
				diagnostic := wakeWorkerJSON(t, `{"adapter":"test"}`)
				runtimeIDs := wakeWorkerJSON(t, `{"runtime":"test"}`)
				if err := request.Callbacks.RecordLaunch(adapterCtx, CodexLaunchEvidence{
					At: at.Add(time.Second), Diagnostic: diagnostic, RuntimeIDs: runtimeIDs}); err != nil {
					return CodexWakeResult{}, err
				}
				wakeDelivered := test.adapterErr == nil
				wakeReceipt := model.JSON{}
				wakeAt := time.Time{}
				if wakeDelivered {
					wakeAt = at.Add(2 * time.Second)
					wakeReceipt = wakeWorkerJSON(t, `{"wake":"test"}`)
					if err := request.Callbacks.RecordWake(adapterCtx, CodexWakeEvidence{
						At: wakeAt, Receipt: wakeReceipt}); err != nil {
						return CodexWakeResult{}, err
					}
				}
				status := "failed"
				if test.adapterErr == nil {
					status = "completed"
				}
				completion, err := codexCompletionReceipt(status, "thread-worker", "turn-worker",
					wakeDelivered, "wait_without_signal", nil)
				if err != nil {
					t.Fatal(err)
				}
				// Prove final settlement does not reuse the now-cancelled adapter context.
				cancel()
				return CodexWakeResult{At: at.Add(3 * time.Second), Diagnostic: diagnostic,
					RuntimeIDs: runtimeIDs, WakeAt: wakeAt, WakeReceipt: wakeReceipt,
					CompletionReceipt: completion, WakeDelivered: wakeDelivered,
					ProcessExited: true}, test.adapterErr
			})
			worker := newWakeWorkerForTest(t, profile, st,
				wakeWorkerPreparerFunc(func(context.Context, model.Profile) (PreparedWake, error) {
					appendSequence("prepare")
					return prepared, nil
				}), adapter, WakeWorkerGateFunc(func(context.Context, model.Profile) error {
					appendSequence("gate")
					return nil
				}), newWakeWorkerTestTimer())
			if err := worker.Run(ctx); err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			sequenceMu.Lock()
			got := append([]string(nil), sequence...)
			sequenceMu.Unlock()
			want := "gate,prepare,adapter,launch"
			if test.adapterErr == nil {
				want += ",wake"
			}
			want += "," + test.wantFinal
			if strings.Join(got, ",") != want {
				t.Fatalf("Runtime sequence = %v, want %s", got, want)
			}
			if snapshot := worker.Snapshot(); !snapshot.Healthy || snapshot.Running {
				t.Fatalf("worker snapshot = %#v", snapshot)
			}
		})
	}
}

func TestWakeWorkerIsSingleConcurrencyAndSettlesCancellation(t *testing.T) {
	at := time.Date(2026, 7, 18, 11, 0, 0, 0, time.UTC)
	profile := wakeWorkerTestProfile(t, at)
	run := wakeWorkerTestRun(t, profile, "run-worker-single", at, false)
	prepared := wakeWorkerPrepared(t, run)
	st := newWakeWorkerTestStore(run)
	var failed atomic.Int32
	st.onFail = func(ctx context.Context, spec store.AgentRuntimeFailureSpec) error {
		if ctx.Err() != nil || spec.Error != wakeWorkerAdapterFailure {
			t.Errorf("cancel failure spec = %#v, ctx=%v", spec, ctx.Err())
		}
		failed.Add(1)
		return nil
	}
	entered := make(chan struct{})
	var active, maximum atomic.Int32
	adapter := wakeWorkerAdapterFunc(func(ctx context.Context,
		request CodexWakeRequest,
	) (CodexWakeResult, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			observed := maximum.Load()
			if current <= observed || maximum.CompareAndSwap(observed, current) {
				break
			}
		}
		close(entered)
		<-ctx.Done()
		completion, err := codexCompletionReceipt("failed", "thread-worker", "turn-worker", false,
			"wait_without_signal", nil)
		if err != nil {
			t.Fatal(err)
		}
		return CodexWakeResult{At: at.Add(time.Second),
			Diagnostic:        wakeWorkerJSON(t, `{"adapter":"test"}`),
			RuntimeIDs:        wakeWorkerJSON(t, `{"runtime":"test"}`),
			CompletionReceipt: completion, ProcessExited: true}, ctx.Err()
	})
	worker := newWakeWorkerForTest(t, profile, st,
		wakeWorkerPreparerFunc(func(context.Context, model.Profile) (PreparedWake, error) {
			return prepared, nil
		}), adapter, WakeWorkerGateFunc(func(context.Context, model.Profile) error { return nil }),
		newWakeWorkerTestTimer())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("adapter did not start")
	}
	if err := worker.Run(context.Background()); !errors.Is(err, ErrWakeWorker) {
		t.Fatalf("concurrent Run() error = %v", err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("cancelled Run() error = %v", err)
	}
	if maximum.Load() != 1 || failed.Load() != 1 {
		t.Fatalf("maximum adapter concurrency/failures = %d/%d", maximum.Load(), failed.Load())
	}
}

func TestWakeWorkerFailsClosedAfterDurableCallbackError(t *testing.T) {
	at := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	profile := wakeWorkerTestProfile(t, at)
	run := wakeWorkerTestRun(t, profile, "run-worker-callback", at, false)
	prepared := wakeWorkerPrepared(t, run)
	st := newWakeWorkerTestStore(run)
	st.onLaunch = func(context.Context, store.AgentRuntimeLaunchSpec) error {
		return errors.New("private durable detail")
	}
	var failCalls atomic.Int32
	st.onFail = func(_ context.Context, spec store.AgentRuntimeFailureSpec) error {
		failCalls.Add(1)
		if spec.Error != wakeWorkerCallbackFailure || strings.Contains(spec.Error, "private") {
			t.Errorf("callback failure Error = %q", spec.Error)
		}
		return nil
	}
	adapter := wakeWorkerAdapterFunc(func(ctx context.Context,
		request CodexWakeRequest,
	) (CodexWakeResult, error) {
		diagnostic := wakeWorkerJSON(t, `{"adapter":"test"}`)
		runtimeIDs := wakeWorkerJSON(t, `{"runtime":"test"}`)
		callbackErr := request.Callbacks.RecordLaunch(ctx, CodexLaunchEvidence{
			At: at.Add(time.Second), Diagnostic: diagnostic, RuntimeIDs: runtimeIDs})
		if callbackErr == nil || strings.Contains(callbackErr.Error(), "private") {
			t.Fatalf("launch callback error = %v", callbackErr)
		}
		completion, err := codexCompletionReceipt("failed", "", "", false,
			"wait_without_signal", nil)
		if err != nil {
			t.Fatal(err)
		}
		return CodexWakeResult{At: at.Add(2 * time.Second), Diagnostic: diagnostic,
			RuntimeIDs: runtimeIDs, CompletionReceipt: completion, ProcessExited: true}, callbackErr
	})
	worker := newWakeWorkerForTest(t, profile, st,
		wakeWorkerPreparerFunc(func(context.Context, model.Profile) (PreparedWake, error) {
			return prepared, nil
		}), adapter, WakeWorkerGateFunc(func(context.Context, model.Profile) error { return nil }),
		newWakeWorkerTestTimer())
	if err := worker.Run(context.Background()); !errors.Is(err, ErrWakeWorker) {
		t.Fatalf("Run() error = %v", err)
	}
	if snapshot := worker.Snapshot(); snapshot.Healthy || snapshot.Ready ||
		snapshot.LastError != wakeWorkerIssueCallback {
		t.Fatalf("fatal callback snapshot = %#v", snapshot)
	}
	if failCalls.Load() != 1 {
		t.Fatalf("final failure settlements = %d", failCalls.Load())
	}
}

func TestWakeWorkerRecoversExactLaunchAfterCallbackResponseLoss(t *testing.T) {
	at := time.Date(2026, 7, 18, 12, 10, 0, 0, time.UTC)
	profile := wakeWorkerTestProfile(t, at)
	run := wakeWorkerTestRun(t, profile, "run-worker-launch-response-loss", at, false)
	prepared := wakeWorkerPrepared(t, run)
	base := newWakeWorkerTestStore(run)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var launchCalls atomic.Int32
	var replayStatus store.AgentRuntimeTransitionStatus
	st := &wakeWorkerStoreOverrides{WakeWorkerStore: base}
	st.recordLaunch = func(callbackCtx context.Context,
		spec store.AgentRuntimeLaunchSpec,
	) (store.AgentRuntimeTransitionResult, error) {
		result, err := base.RecordAgentRuntimeLaunch(callbackCtx, spec)
		if err != nil {
			return result, err
		}
		if launchCalls.Add(1) == 1 {
			return store.AgentRuntimeTransitionResult{}, errors.New("launch commit response lost")
		}
		replayStatus = result.Status
		return result, nil
	}
	base.onFail = func(context.Context, store.AgentRuntimeFailureSpec) error {
		cancel()
		return nil
	}
	diagnostic := wakeWorkerJSON(t, `{"adapter":"test"}`)
	runtimeIDs := wakeWorkerJSON(t, `{"runtime":"test"}`)
	completion, err := codexCompletionReceipt("failed", "", "", false,
		"wait_without_signal", nil)
	if err != nil {
		t.Fatal(err)
	}
	worker := newWakeWorkerForTest(t, profile, st,
		wakeWorkerPreparerFunc(func(context.Context, model.Profile) (PreparedWake, error) {
			return prepared, nil
		}), wakeWorkerAdapterFunc(func(adapterCtx context.Context,
			request CodexWakeRequest,
		) (CodexWakeResult, error) {
			launchAt := at.Add(time.Second)
			callbackErr := request.Callbacks.RecordLaunch(adapterCtx, CodexLaunchEvidence{
				At: launchAt, Diagnostic: diagnostic, RuntimeIDs: runtimeIDs})
			if callbackErr == nil || strings.Contains(callbackErr.Error(), "commit response") {
				t.Fatalf("ambiguous launch callback error = %v", callbackErr)
			}
			return CodexWakeResult{At: at.Add(2 * time.Second), LaunchAt: launchAt,
				Diagnostic: diagnostic, RuntimeIDs: runtimeIDs, CompletionReceipt: completion,
				ProcessExited: true}, callbackErr
		}), WakeWorkerGateFunc(func(context.Context, model.Profile) error { return nil }),
		newWakeWorkerTestTimer())
	if err := worker.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if launchCalls.Load() != 2 || replayStatus != store.AgentRuntimeReplayed {
		t.Fatalf("launch calls/replay = %d/%q", launchCalls.Load(), replayStatus)
	}
	base.mu.Lock()
	settled := base.runs[run.ID()]
	base.mu.Unlock()
	startedAt, launched := settled.RuntimeStartedAt()
	settledCompletion, completed := settled.CompletionReceipt()
	if !launched || !startedAt.Equal(at.Add(time.Second)) ||
		settled.LauncherDiagnostic().String() != diagnostic.String() ||
		settled.RuntimeIDs().String() != runtimeIDs.String() || !completed ||
		settledCompletion.String() != completion.String() {
		t.Fatalf("settled launch evidence = %#v", settled)
	}
	if snapshot := worker.Snapshot(); !snapshot.Healthy || snapshot.Running ||
		snapshot.LastError != wakeWorkerIssueAdapter {
		t.Fatalf("worker snapshot = %#v", snapshot)
	}
}

func TestWakeWorkerRecoversExactWakeAfterCallbackResponseLoss(t *testing.T) {
	at := time.Date(2026, 7, 18, 12, 15, 0, 0, time.UTC)
	profile := wakeWorkerTestProfile(t, at)
	run := wakeWorkerTestRun(t, profile, "run-worker-wake-response-loss", at, false)
	prepared := wakeWorkerPrepared(t, run)
	base := newWakeWorkerTestStore(run)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wakeCalls atomic.Int32
	var replayStatus store.AgentRuntimeTransitionStatus
	st := &wakeWorkerStoreOverrides{WakeWorkerStore: base}
	st.recordWake = func(callbackCtx context.Context,
		spec store.AgentWakeDeliverySpec,
	) (store.AgentRuntimeTransitionResult, error) {
		result, err := base.RecordAgentWakeDelivery(callbackCtx, spec)
		if err != nil {
			return result, err
		}
		if wakeCalls.Add(1) == 1 {
			return store.AgentRuntimeTransitionResult{}, errors.New("wake commit response lost")
		}
		replayStatus = result.Status
		return result, nil
	}
	base.onFail = func(context.Context, store.AgentRuntimeFailureSpec) error {
		cancel()
		return nil
	}
	diagnostic := wakeWorkerJSON(t, `{"adapter":"test"}`)
	runtimeIDs := wakeWorkerJSON(t, `{"runtime":"test"}`)
	wakeReceipt := wakeWorkerJSON(t, `{"wake":"test"}`)
	completion, err := codexCompletionReceipt("failed", "thread-worker", "turn-worker", true,
		"wait_without_signal", nil)
	if err != nil {
		t.Fatal(err)
	}
	worker := newWakeWorkerForTest(t, profile, st,
		wakeWorkerPreparerFunc(func(context.Context, model.Profile) (PreparedWake, error) {
			return prepared, nil
		}), wakeWorkerAdapterFunc(func(adapterCtx context.Context,
			request CodexWakeRequest,
		) (CodexWakeResult, error) {
			if err := request.Callbacks.RecordLaunch(adapterCtx, CodexLaunchEvidence{
				At: at.Add(time.Second), Diagnostic: diagnostic, RuntimeIDs: runtimeIDs}); err != nil {
				t.Fatal(err)
			}
			callbackErr := request.Callbacks.RecordWake(adapterCtx, CodexWakeEvidence{
				At: at.Add(2 * time.Second), Receipt: wakeReceipt})
			if callbackErr == nil || strings.Contains(callbackErr.Error(), "commit response") {
				t.Fatalf("ambiguous wake callback error = %v", callbackErr)
			}
			return CodexWakeResult{At: at.Add(3 * time.Second), Diagnostic: diagnostic,
				RuntimeIDs: runtimeIDs, WakeAt: at.Add(2 * time.Second), WakeReceipt: wakeReceipt,
				CompletionReceipt: completion, WakeDelivered: true, ProcessExited: true}, callbackErr
		}), WakeWorkerGateFunc(func(context.Context, model.Profile) error { return nil }),
		newWakeWorkerTestTimer())
	if err := worker.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if wakeCalls.Load() != 2 || replayStatus != store.AgentRuntimeReplayed {
		t.Fatalf("wake calls/replay = %d/%q", wakeCalls.Load(), replayStatus)
	}
	base.mu.Lock()
	settled := base.runs[run.ID()]
	base.mu.Unlock()
	wakeAt, delivered := settled.WakeDeliveredAt()
	receipt, hasReceipt := settled.WakeReceipt()
	settledCompletion, completed := settled.CompletionReceipt()
	if !delivered || !wakeAt.Equal(at.Add(2*time.Second)) || !hasReceipt ||
		receipt.String() != wakeReceipt.String() || !completed ||
		settledCompletion.String() != completion.String() ||
		!strings.Contains(settledCompletion.String(), `"wake_delivered":true`) {
		t.Fatalf("settled wake evidence = %#v", settled)
	}
	if snapshot := worker.Snapshot(); !snapshot.Healthy || snapshot.Running ||
		snapshot.LastError != wakeWorkerIssueAdapter {
		t.Fatalf("worker snapshot = %#v", snapshot)
	}
}

func TestWakeWorkerCallbackGateDrainsAndRejectsEscapedCallbacks(t *testing.T) {
	at := time.Date(2026, 7, 18, 12, 20, 0, 0, time.UTC)
	profile := wakeWorkerTestProfile(t, at)

	t.Run("drains callback already in flight", func(t *testing.T) {
		run := wakeWorkerTestRun(t, profile, "run-worker-callback-drain", at, false)
		prepared := wakeWorkerPrepared(t, run)
		st := newWakeWorkerTestStore(run)
		storeEntered := make(chan struct{})
		releaseStore := make(chan struct{})
		callbackDone := make(chan error, 1)
		st.onLaunch = func(context.Context, store.AgentRuntimeLaunchSpec) error {
			close(storeEntered)
			<-releaseStore
			return nil
		}
		diagnostic := wakeWorkerJSON(t, `{"adapter":"test"}`)
		runtimeIDs := wakeWorkerJSON(t, `{"runtime":"test"}`)
		completion, err := codexCompletionReceipt("failed", "", "", false,
			"wait_without_signal", nil)
		if err != nil {
			t.Fatal(err)
		}
		worker := newWakeWorkerForTest(t, profile, st,
			wakeWorkerPreparerFunc(func(context.Context, model.Profile) (PreparedWake, error) {
				return PreparedWake{}, nil
			}), wakeWorkerAdapterFunc(func(adapterCtx context.Context,
				request CodexWakeRequest,
			) (CodexWakeResult, error) {
				go func() {
					callbackDone <- request.Callbacks.RecordLaunch(adapterCtx, CodexLaunchEvidence{
						At: at.Add(time.Second), Diagnostic: diagnostic, RuntimeIDs: runtimeIDs})
				}()
				<-storeEntered
				return CodexWakeResult{At: at.Add(2 * time.Second), Diagnostic: diagnostic,
						RuntimeIDs: runtimeIDs, CompletionReceipt: completion, ProcessExited: true},
					errors.New("adapter failed after launch")
			}), WakeWorkerGateFunc(func(context.Context, model.Profile) error { return nil }),
			newWakeWorkerTestTimer())
		fence, _ := run.ClaimFenceHash()
		done := make(chan struct {
			delay time.Duration
			issue string
		}, 1)
		go func() {
			delay, issue := worker.runPrepared(context.Background(), prepared, fence)
			done <- struct {
				delay time.Duration
				issue string
			}{delay: delay, issue: issue}
		}()
		select {
		case result := <-done:
			t.Fatalf("runPrepared returned before callback drain: %#v", result)
		case <-time.After(20 * time.Millisecond):
		}
		close(releaseStore)
		if err := <-callbackDone; err != nil {
			t.Fatalf("in-flight callback error = %v", err)
		}
		result := <-done
		if result.delay != worker.backoffInterval || result.issue != "" {
			t.Fatalf("runPrepared result = %#v", result)
		}
	})

	t.Run("rejects callback after adapter return", func(t *testing.T) {
		run := wakeWorkerTestRun(t, profile, "run-worker-callback-escape", at, false)
		prepared := wakeWorkerPrepared(t, run)
		st := newWakeWorkerTestStore(run)
		var launchStoreCalls, wakeStoreCalls atomic.Int32
		st.onLaunch = func(context.Context, store.AgentRuntimeLaunchSpec) error {
			launchStoreCalls.Add(1)
			return nil
		}
		st.onWake = func(context.Context, store.AgentWakeDeliverySpec) error {
			wakeStoreCalls.Add(1)
			return nil
		}
		diagnostic := wakeWorkerJSON(t, `{"adapter":"test"}`)
		runtimeIDs := wakeWorkerJSON(t, `{"runtime":"test"}`)
		completion, err := codexCompletionReceipt("failed", "", "", false,
			"wait_without_signal", nil)
		if err != nil {
			t.Fatal(err)
		}
		var escaped CodexWakeCallbacks
		worker := newWakeWorkerForTest(t, profile, st,
			wakeWorkerPreparerFunc(func(context.Context, model.Profile) (PreparedWake, error) {
				return PreparedWake{}, nil
			}), wakeWorkerAdapterFunc(func(_ context.Context,
				request CodexWakeRequest,
			) (CodexWakeResult, error) {
				escaped = request.Callbacks
				return CodexWakeResult{At: at.Add(2 * time.Second), Diagnostic: diagnostic,
						RuntimeIDs: runtimeIDs, CompletionReceipt: completion, ProcessExited: true},
					errors.New("adapter returned")
			}), WakeWorkerGateFunc(func(context.Context, model.Profile) error { return nil }),
			newWakeWorkerTestTimer())
		fence, _ := run.ClaimFenceHash()
		if delay, issue := worker.runPrepared(context.Background(), prepared, fence); delay != worker.backoffInterval || issue != "" {
			t.Fatalf("runPrepared = (%s, %q)", delay, issue)
		}
		if err := escaped.RecordLaunch(context.Background(), CodexLaunchEvidence{
			At: at.Add(time.Second), Diagnostic: diagnostic, RuntimeIDs: runtimeIDs}); err == nil {
			t.Fatal("escaped launch callback succeeded")
		}
		if err := escaped.RecordWake(context.Background(), CodexWakeEvidence{
			At: at.Add(time.Second), Receipt: wakeWorkerJSON(t, `{"wake":"late"}`)}); err == nil {
			t.Fatal("escaped wake callback succeeded")
		}
		if launchStoreCalls.Load() != 0 || wakeStoreCalls.Load() != 0 {
			t.Fatalf("escaped Store calls = %d/%d", launchStoreCalls.Load(), wakeStoreCalls.Load())
		}
	})
}

func TestWakeWorkerStopsUnhealthyWithoutProcessExitProof(t *testing.T) {
	at := time.Date(2026, 7, 18, 12, 30, 0, 0, time.UTC)
	profile := wakeWorkerTestProfile(t, at)
	run := wakeWorkerTestRun(t, profile, "run-worker-unproven-exit", at, false)
	prepared := wakeWorkerPrepared(t, run)
	st := newWakeWorkerTestStore(run)
	var failCalls atomic.Int32
	st.onFail = func(context.Context, store.AgentRuntimeFailureSpec) error {
		failCalls.Add(1)
		return nil
	}
	worker := newWakeWorkerForTest(t, profile, st,
		wakeWorkerPreparerFunc(func(context.Context, model.Profile) (PreparedWake, error) {
			return prepared, nil
		}), wakeWorkerAdapterFunc(func(context.Context, CodexWakeRequest) (CodexWakeResult, error) {
			return CodexWakeResult{}, errors.New("private uncertain process detail")
		}), WakeWorkerGateFunc(func(context.Context, model.Profile) error { return nil }),
		newWakeWorkerTestTimer())
	if err := worker.Run(context.Background()); !errors.Is(err, ErrWakeWorker) ||
		strings.Contains(err.Error(), "private") {
		t.Fatalf("Run() error = %v", err)
	}
	if failCalls.Load() != 0 {
		t.Fatalf("FailAgentRuntime calls without exit proof = %d", failCalls.Load())
	}
	if snapshot := worker.Snapshot(); snapshot.Healthy || snapshot.Ready ||
		snapshot.LastError != wakeWorkerIssueRuntimeUnproven {
		t.Fatalf("unproven exit snapshot = %#v", snapshot)
	}
}

func TestWakeWorkerSettlesActionablePrepareFailureWithoutStartingAdapter(t *testing.T) {
	at := time.Date(2026, 7, 18, 13, 0, 0, 0, time.UTC)
	profile := wakeWorkerTestProfile(t, at)
	run := wakeWorkerTestRun(t, profile, "run-worker-publish", at, false)
	st := newWakeWorkerTestStore(run)
	ctx, cancel := context.WithCancel(context.Background())
	st.onFail = func(settleCtx context.Context, spec store.AgentRuntimeFailureSpec) error {
		if settleCtx.Err() != nil || spec.Error != wakeWorkerPrepareFailure ||
			spec.RuntimeIDs.String() != `{}` ||
			!strings.Contains(spec.LauncherDiagnostic.String(), wakeWorkerIssuePrepareRun) ||
			strings.Contains(spec.LauncherDiagnostic.String(), "private-path") ||
			!strings.Contains(spec.CompletionReceipt.String(), `"exit_method":"not_started"`) {
			t.Errorf("prepare failure spec = %#v, ctx=%v", spec, settleCtx.Err())
		}
		cancel()
		return nil
	}
	var adapterCalls atomic.Int32
	worker := newWakeWorkerForTest(t, profile, st,
		wakeWorkerPreparerFunc(func(context.Context, model.Profile) (PreparedWake, error) {
			return PreparedWake{status: store.AgentClaimActionable, run: run},
				errors.New("private-path/collision")
		}), wakeWorkerAdapterFunc(func(context.Context, CodexWakeRequest) (CodexWakeResult, error) {
			adapterCalls.Add(1)
			return CodexWakeResult{}, nil
		}), WakeWorkerGateFunc(func(context.Context, model.Profile) error { return nil }),
		newWakeWorkerTestTimer())
	if err := worker.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if adapterCalls.Load() != 0 {
		t.Fatalf("adapter calls = %d", adapterCalls.Load())
	}
}

func TestWakeWorkerRescansAmbiguousPrepareCommitBeforeRetry(t *testing.T) {
	at := time.Date(2026, 7, 18, 13, 30, 0, 0, time.UTC)
	profile := wakeWorkerTestProfile(t, at)
	run := wakeWorkerTestRun(t, profile, "run-worker-ambiguous-prepare", at, false)
	st := newWakeWorkerTestStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var listCalls, prepareCalls, abandonCalls, adapterCalls atomic.Int32
	st.onList = func() { listCalls.Add(1) }
	st.onAbandon = func(settleCtx context.Context, _ store.AgentUnregisteredRunSpec) error {
		if settleCtx.Err() != nil {
			t.Errorf("ambiguous Prepare recovery inherited cancellation: %v", settleCtx.Err())
		}
		abandonCalls.Add(1)
		return nil
	}
	worker := newWakeWorkerForTest(t, profile, st,
		wakeWorkerPreparerFunc(func(context.Context, model.Profile) (PreparedWake, error) {
			if prepareCalls.Add(1) != 1 {
				t.Fatal("Prepare retried before ambiguous claim recovery")
			}
			// Model a preclaim transaction that committed while its response was
			// lost: the caller receives neither status nor Run, but startup
			// recovery can discover the durable incomplete Run.
			st.mu.Lock()
			st.runs[run.ID()] = run
			st.handlings[run.ID()] = wakeWorkerClaimedHandling(run)
			st.incomplete = append(st.incomplete, run)
			st.mu.Unlock()
			cancel()
			return PreparedWake{}, errors.New("preclaim response lost")
		}), wakeWorkerAdapterFunc(func(context.Context, CodexWakeRequest) (CodexWakeResult, error) {
			adapterCalls.Add(1)
			return CodexWakeResult{}, nil
		}), WakeWorkerGateFunc(func(context.Context, model.Profile) error { return nil }),
		newWakeWorkerTestTimer())
	if err := worker.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if listCalls.Load() != 2 || prepareCalls.Load() != 1 || abandonCalls.Load() != 1 ||
		adapterCalls.Load() != 0 {
		t.Fatalf("list/prepare/abandon/adapter calls = %d/%d/%d/%d", listCalls.Load(),
			prepareCalls.Load(), abandonCalls.Load(), adapterCalls.Load())
	}
	st.mu.Lock()
	remaining := len(st.incomplete)
	settled := st.runs[run.ID()]
	st.mu.Unlock()
	if remaining != 0 || !settled.Status().Terminal() || settled.Error() != wakeWorkerUnregistered {
		t.Fatalf("ambiguous Prepare recovery = remaining %d, run %#v", remaining, settled)
	}
}

func TestWakeWorkerFailsClosedAfterInvalidPrepareRescan(t *testing.T) {
	at := time.Date(2026, 7, 18, 13, 40, 0, 0, time.UTC)
	profile := wakeWorkerTestProfile(t, at)
	run := wakeWorkerTestRun(t, profile, "run-worker-invalid-prepare", at, false)
	st := newWakeWorkerTestStore()
	var listCalls, prepareCalls, adapterCalls atomic.Int32
	st.onList = func() { listCalls.Add(1) }
	worker := newWakeWorkerForTest(t, profile, st,
		wakeWorkerPreparerFunc(func(context.Context, model.Profile) (PreparedWake, error) {
			prepareCalls.Add(1)
			return PreparedWake{status: store.AgentClaimNone, run: run}, nil
		}), wakeWorkerAdapterFunc(func(context.Context, CodexWakeRequest) (CodexWakeResult, error) {
			adapterCalls.Add(1)
			return CodexWakeResult{}, nil
		}), WakeWorkerGateFunc(func(context.Context, model.Profile) error { return nil }),
		newWakeWorkerTestTimer())
	if err := worker.Run(context.Background()); !errors.Is(err, ErrWakeWorker) {
		t.Fatalf("Run() error = %v", err)
	}
	if listCalls.Load() != 2 || prepareCalls.Load() != 1 || adapterCalls.Load() != 0 {
		t.Fatalf("list/prepare/adapter calls = %d/%d/%d", listCalls.Load(),
			prepareCalls.Load(), adapterCalls.Load())
	}
	if snapshot := worker.Snapshot(); snapshot.Healthy || snapshot.Ready ||
		snapshot.LastError != wakeWorkerIssuePrepare {
		t.Fatalf("worker snapshot = %#v", snapshot)
	}
}

func TestWakeWorkerStartupRecoveryGoneLiveAndUnregistered(t *testing.T) {
	at := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
	t.Run("gone is settled before first gate", func(t *testing.T) {
		profile := wakeWorkerTestProfile(t, at)
		run := wakeWorkerTestRun(t, profile, "run-worker-orphan-gone", at, true)
		st := newWakeWorkerRecoveryTestStore(run)
		ctx, cancel := context.WithCancel(context.Background())
		var sequenceMu sync.Mutex
		var sequence []string
		appendSequence := func(value string) {
			sequenceMu.Lock()
			sequence = append(sequence, value)
			sequenceMu.Unlock()
		}
		st.onList = func() { appendSequence("list") }
		st.onSettle = func(settleCtx context.Context, spec store.AgentRuntimeOrphanSpec) error {
			appendSequence("settle")
			if settleCtx.Err() != nil || spec.Error != wakeWorkerRecoveryFailure ||
				spec.RunID != run.ID() {
				t.Errorf("orphan settlement = %#v, ctx=%v", spec, settleCtx.Err())
			}
			return nil
		}
		worker := newWakeWorkerForTest(t, profile, st,
			wakeWorkerPreparerFunc(func(context.Context, model.Profile) (PreparedWake, error) {
				appendSequence("prepare")
				cancel()
				return PreparedWake{status: store.AgentClaimNone}, nil
			}), wakeWorkerAdapterFunc(func(context.Context, CodexWakeRequest) (CodexWakeResult, error) {
				t.Fatal("adapter ran during recovery test")
				return CodexWakeResult{}, nil
			}), WakeWorkerGateFunc(func(context.Context, model.Profile) error {
				appendSequence("gate")
				return nil
			}), newWakeWorkerTestTimer())
		worker.recoverRuntime = func(context.Context, model.JSON,
			func() time.Time,
		) (runtimeProcessRecovery, error) {
			appendSequence("recover")
			return runtimeProcessRecovery{State: runtimeProcessGone,
				Receipt: wakeWorkerJSON(t, `{"process_exit":"confirmed"}`), At: at.Add(3 * time.Second)}, nil
		}
		if err := worker.Run(ctx); err != nil {
			t.Fatalf("Run() error = %v", err)
		}
		sequenceMu.Lock()
		got := strings.Join(sequence, ",")
		sequenceMu.Unlock()
		if got != "list,recover,settle,gate,prepare" {
			t.Fatalf("startup sequence = %s", got)
		}
	})

	t.Run("exact live stays recovering then settles", func(t *testing.T) {
		profile := wakeWorkerTestProfile(t, at)
		run := wakeWorkerTestRun(t, profile, "run-worker-orphan-live", at, true)
		st := newWakeWorkerRecoveryTestStore(run)
		ctx, cancel := context.WithCancel(context.Background())
		timer := newWakeWorkerTestTimer()
		var recoverCalls atomic.Int32
		var gateCalls atomic.Int32
		worker := newWakeWorkerForTest(t, profile, st,
			wakeWorkerPreparerFunc(func(context.Context, model.Profile) (PreparedWake, error) {
				cancel()
				return PreparedWake{status: store.AgentClaimNone}, nil
			}), wakeWorkerAdapterFunc(func(context.Context, CodexWakeRequest) (CodexWakeResult, error) {
				return CodexWakeResult{}, errors.New("unexpected adapter")
			}), WakeWorkerGateFunc(func(context.Context, model.Profile) error {
				gateCalls.Add(1)
				return nil
			}), timer)
		worker.recoverRuntime = func(context.Context, model.JSON,
			func() time.Time,
		) (runtimeProcessRecovery, error) {
			if recoverCalls.Add(1) == 1 {
				return runtimeProcessRecovery{}, ErrRuntimeProcessLive
			}
			return runtimeProcessRecovery{State: runtimeProcessGone,
				Receipt: wakeWorkerJSON(t, `{"process_exit":"confirmed"}`), At: at.Add(4 * time.Second)}, nil
		}
		done := make(chan error, 1)
		go func() { done <- worker.Run(ctx) }()
		if delay := <-timer.calls; delay != worker.backoffInterval {
			t.Fatalf("live recovery delay = %s", delay)
		}
		if snapshot := worker.Snapshot(); !snapshot.Healthy || !snapshot.Recovering ||
			snapshot.Ready || snapshot.LastError != wakeWorkerIssueRecoveryLive || gateCalls.Load() != 0 {
			t.Fatalf("live recovery snapshot = %#v, gate=%d", snapshot, gateCalls.Load())
		}
		timer.tick()
		if err := <-done; err != nil {
			t.Fatalf("Run() error = %v", err)
		}
		if recoverCalls.Load() != 2 || gateCalls.Load() != 1 {
			t.Fatalf("recovery/gate calls = %d/%d", recoverCalls.Load(), gateCalls.Load())
		}
	})

	t.Run("unregistered launch is abandoned without process proof", func(t *testing.T) {
		profile := wakeWorkerTestProfile(t, at)
		run := wakeWorkerTestRun(t, profile, "run-worker-unregistered", at, false)
		st := newWakeWorkerRecoveryTestStore(run)
		ctx, cancel := context.WithCancel(context.Background())
		var abandoned atomic.Int32
		st.onAbandon = func(settleCtx context.Context, spec store.AgentUnregisteredRunSpec) error {
			if settleCtx.Err() != nil || spec.RunID != run.ID() || spec.Error != wakeWorkerUnregistered {
				t.Errorf("unregistered settlement = %#v, ctx=%v", spec, settleCtx.Err())
			}
			abandoned.Add(1)
			return nil
		}
		worker := newWakeWorkerForTest(t, profile, st,
			wakeWorkerPreparerFunc(func(context.Context, model.Profile) (PreparedWake, error) {
				cancel()
				return PreparedWake{status: store.AgentClaimNone}, nil
			}), wakeWorkerAdapterFunc(func(context.Context, CodexWakeRequest) (CodexWakeResult, error) {
				return CodexWakeResult{}, errors.New("unexpected adapter")
			}), WakeWorkerGateFunc(func(context.Context, model.Profile) error { return nil }),
			newWakeWorkerTestTimer())
		worker.recoverRuntime = func(context.Context, model.JSON,
			func() time.Time,
		) (runtimeProcessRecovery, error) {
			t.Fatal("unregistered Run reached process recovery")
			return runtimeProcessRecovery{}, nil
		}
		if err := worker.Run(ctx); err != nil {
			t.Fatalf("Run() error = %v", err)
		}
		if abandoned.Load() != 1 {
			t.Fatalf("abandon calls = %d", abandoned.Load())
		}
	})

	t.Run("uncertain process identity is fatal", func(t *testing.T) {
		profile := wakeWorkerTestProfile(t, at)
		run := wakeWorkerTestRun(t, profile, "run-worker-orphan-uncertain", at, true)
		st := newWakeWorkerRecoveryTestStore(run)
		worker := newWakeWorkerForTest(t, profile, st,
			wakeWorkerPreparerFunc(func(context.Context, model.Profile) (PreparedWake, error) {
				return PreparedWake{}, nil
			}), wakeWorkerAdapterFunc(func(context.Context, CodexWakeRequest) (CodexWakeResult, error) {
				return CodexWakeResult{}, nil
			}), WakeWorkerGateFunc(func(context.Context, model.Profile) error { return nil }),
			newWakeWorkerTestTimer())
		worker.recoverRuntime = func(context.Context, model.JSON,
			func() time.Time,
		) (runtimeProcessRecovery, error) {
			return runtimeProcessRecovery{}, ErrRuntimeProcessUncertain
		}
		if err := worker.Run(context.Background()); !errors.Is(err, ErrWakeWorker) {
			t.Fatalf("Run() error = %v", err)
		}
		if snapshot := worker.Snapshot(); snapshot.Healthy ||
			snapshot.LastError != wakeWorkerIssueRecoveryInvalid {
			t.Fatalf("uncertain recovery snapshot = %#v", snapshot)
		}
	})
}

func TestWakeWorkerRetriesGateAndPrepareFailuresWithoutClaimedRun(t *testing.T) {
	at := time.Date(2026, 7, 18, 15, 0, 0, 0, time.UTC)
	profile := wakeWorkerTestProfile(t, at)
	for _, test := range []struct {
		name      string
		gateFails bool
	}{
		{name: "gate", gateFails: true},
		{name: "prepare"},
	} {
		t.Run(test.name, func(t *testing.T) {
			st := newWakeWorkerTestStore()
			ctx, cancel := context.WithCancel(context.Background())
			timer := newWakeWorkerTestTimer()
			var gateCalls, prepareCalls atomic.Int32
			worker := newWakeWorkerForTest(t, profile, st,
				wakeWorkerPreparerFunc(func(context.Context, model.Profile) (PreparedWake, error) {
					call := prepareCalls.Add(1)
					if !test.gateFails && call == 2 {
						cancel()
					}
					return PreparedWake{}, errors.New("private prepare path")
				}), wakeWorkerAdapterFunc(func(context.Context, CodexWakeRequest) (CodexWakeResult, error) {
					t.Fatal("adapter ran on transient failure")
					return CodexWakeResult{}, nil
				}), WakeWorkerGateFunc(func(context.Context, model.Profile) error {
					if gateCalls.Add(1) == 1 && test.gateFails {
						return errors.New("private installation path")
					}
					if test.gateFails {
						cancel()
					}
					return nil
				}), timer)
			done := make(chan error, 1)
			go func() { done <- worker.Run(ctx) }()
			if delay := <-timer.calls; delay != worker.backoffInterval {
				t.Fatalf("retry delay = %s", delay)
			}
			if snapshot := worker.Snapshot(); !snapshot.Healthy || snapshot.Ready ||
				(snapshot.LastError != wakeWorkerIssueGate && snapshot.LastError != wakeWorkerIssuePrepare) {
				t.Fatalf("retry snapshot = %#v", snapshot)
			}
			timer.tick()
			if err := <-done; err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			wantPrepareCalls := int32(2)
			if test.gateFails {
				wantPrepareCalls = 1
			}
			if prepareCalls.Load() != wantPrepareCalls {
				t.Fatalf("gate/prepare calls = %d/%d", gateCalls.Load(), prepareCalls.Load())
			}
		})
	}
}

type wakeWorkerPreparerFunc func(context.Context, model.Profile) (PreparedWake, error)

func (prepare wakeWorkerPreparerFunc) Prepare(ctx context.Context,
	profile model.Profile,
) (PreparedWake, error) {
	return prepare(ctx, profile)
}

type wakeWorkerAdapterFunc func(context.Context, CodexWakeRequest) (CodexWakeResult, error)

func (run wakeWorkerAdapterFunc) Run(ctx context.Context,
	request CodexWakeRequest,
) (CodexWakeResult, error) {
	return run(ctx, request)
}

type wakeWorkerTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *wakeWorkerTestClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = clock.now.Add(time.Second)
	return clock.now
}

type wakeWorkerTestTimer struct {
	calls chan time.Duration
	ticks chan time.Time
}

func newWakeWorkerTestTimer() *wakeWorkerTestTimer {
	return &wakeWorkerTestTimer{calls: make(chan time.Duration, 8), ticks: make(chan time.Time, 8)}
}

func (timer *wakeWorkerTestTimer) After(duration time.Duration) <-chan time.Time {
	timer.calls <- duration
	return timer.ticks
}

func (timer *wakeWorkerTestTimer) tick() { timer.ticks <- time.Now() }

type wakeWorkerTestStore struct {
	mu         sync.Mutex
	runs       map[model.RunID]model.AgentRun
	handlings  map[model.RunID]model.Handling
	incomplete []model.AgentRun
	onList     func()
	onAbandon  func(context.Context, store.AgentUnregisteredRunSpec) error
	onSettle   func(context.Context, store.AgentRuntimeOrphanSpec) error
	onLaunch   func(context.Context, store.AgentRuntimeLaunchSpec) error
	onWake     func(context.Context, store.AgentWakeDeliverySpec) error
	onFinish   func(context.Context, store.AgentRuntimeFinishSpec) error
	onFail     func(context.Context, store.AgentRuntimeFailureSpec) error
}

type wakeWorkerStoreOverrides struct {
	WakeWorkerStore
	abandon func(context.Context, store.AgentUnregisteredRunSpec) (
		store.AgentRuntimeTransitionResult, error)
	settle func(context.Context, store.AgentRuntimeOrphanSpec) (
		store.AgentRuntimeTransitionResult, error)
	recordLaunch func(context.Context, store.AgentRuntimeLaunchSpec) (
		store.AgentRuntimeTransitionResult, error)
	recordWake func(context.Context, store.AgentWakeDeliverySpec) (
		store.AgentRuntimeTransitionResult, error)
	finish func(context.Context, store.AgentRuntimeFinishSpec) (
		store.AgentRuntimeTransitionResult, error)
	fail func(context.Context, store.AgentRuntimeFailureSpec) (
		store.AgentRuntimeTransitionResult, error)
}

func (override *wakeWorkerStoreOverrides) AbandonUnregisteredAgentRun(ctx context.Context,
	spec store.AgentUnregisteredRunSpec,
) (store.AgentRuntimeTransitionResult, error) {
	if override.abandon != nil {
		return override.abandon(ctx, spec)
	}
	return override.WakeWorkerStore.AbandonUnregisteredAgentRun(ctx, spec)
}

func (override *wakeWorkerStoreOverrides) SettleOrphanedAgentRuntime(ctx context.Context,
	spec store.AgentRuntimeOrphanSpec,
) (store.AgentRuntimeTransitionResult, error) {
	if override.settle != nil {
		return override.settle(ctx, spec)
	}
	return override.WakeWorkerStore.SettleOrphanedAgentRuntime(ctx, spec)
}

func (override *wakeWorkerStoreOverrides) RecordAgentRuntimeLaunch(ctx context.Context,
	spec store.AgentRuntimeLaunchSpec,
) (store.AgentRuntimeTransitionResult, error) {
	if override.recordLaunch != nil {
		return override.recordLaunch(ctx, spec)
	}
	return override.WakeWorkerStore.RecordAgentRuntimeLaunch(ctx, spec)
}

func (override *wakeWorkerStoreOverrides) RecordAgentWakeDelivery(ctx context.Context,
	spec store.AgentWakeDeliverySpec,
) (store.AgentRuntimeTransitionResult, error) {
	if override.recordWake != nil {
		return override.recordWake(ctx, spec)
	}
	return override.WakeWorkerStore.RecordAgentWakeDelivery(ctx, spec)
}

func (override *wakeWorkerStoreOverrides) FinishAgentRuntime(ctx context.Context,
	spec store.AgentRuntimeFinishSpec,
) (store.AgentRuntimeTransitionResult, error) {
	if override.finish != nil {
		return override.finish(ctx, spec)
	}
	return override.WakeWorkerStore.FinishAgentRuntime(ctx, spec)
}

func (override *wakeWorkerStoreOverrides) FailAgentRuntime(ctx context.Context,
	spec store.AgentRuntimeFailureSpec,
) (store.AgentRuntimeTransitionResult, error) {
	if override.fail != nil {
		return override.fail(ctx, spec)
	}
	return override.WakeWorkerStore.FailAgentRuntime(ctx, spec)
}

func newWakeWorkerTestStore(runs ...model.AgentRun) *wakeWorkerTestStore {
	result := &wakeWorkerTestStore{runs: make(map[model.RunID]model.AgentRun),
		handlings: make(map[model.RunID]model.Handling)}
	for _, run := range runs {
		result.runs[run.ID()] = run
		result.handlings[run.ID()] = wakeWorkerClaimedHandling(run)
	}
	return result
}

func newWakeWorkerRecoveryTestStore(runs ...model.AgentRun) *wakeWorkerTestStore {
	result := newWakeWorkerTestStore(runs...)
	result.incomplete = append([]model.AgentRun(nil), runs...)
	return result
}

func (fake *wakeWorkerTestStore) ListIncompleteManagedAgentRuns(context.Context) ([]model.AgentRun, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.onList != nil {
		fake.onList()
	}
	return append([]model.AgentRun(nil), fake.incomplete...), nil
}

func (fake *wakeWorkerTestStore) AbandonUnregisteredAgentRun(ctx context.Context,
	spec store.AgentUnregisteredRunSpec,
) (store.AgentRuntimeTransitionResult, error) {
	if fake.onAbandon != nil {
		if err := fake.onAbandon(ctx, spec); err != nil {
			return store.AgentRuntimeTransitionResult{}, err
		}
	}
	return fake.applyAbandon(spec)
}

func (fake *wakeWorkerTestStore) SettleOrphanedAgentRuntime(ctx context.Context,
	spec store.AgentRuntimeOrphanSpec,
) (store.AgentRuntimeTransitionResult, error) {
	if fake.onSettle != nil {
		if err := fake.onSettle(ctx, spec); err != nil {
			return store.AgentRuntimeTransitionResult{}, err
		}
	}
	return fake.applyOrphan(spec)
}

func (fake *wakeWorkerTestStore) RecordAgentRuntimeLaunch(ctx context.Context,
	spec store.AgentRuntimeLaunchSpec,
) (store.AgentRuntimeTransitionResult, error) {
	if fake.onLaunch != nil {
		if err := fake.onLaunch(ctx, spec); err != nil {
			return store.AgentRuntimeTransitionResult{}, err
		}
	}
	return fake.applyLaunch(spec)
}

func (fake *wakeWorkerTestStore) RecordAgentWakeDelivery(ctx context.Context,
	spec store.AgentWakeDeliverySpec,
) (store.AgentRuntimeTransitionResult, error) {
	if fake.onWake != nil {
		if err := fake.onWake(ctx, spec); err != nil {
			return store.AgentRuntimeTransitionResult{}, err
		}
	}
	return fake.applyWake(spec)
}

func (fake *wakeWorkerTestStore) FinishAgentRuntime(ctx context.Context,
	spec store.AgentRuntimeFinishSpec,
) (store.AgentRuntimeTransitionResult, error) {
	if fake.onFinish != nil {
		if err := fake.onFinish(ctx, spec); err != nil {
			return store.AgentRuntimeTransitionResult{}, err
		}
	}
	return fake.applyFinish(spec)
}

func (fake *wakeWorkerTestStore) FailAgentRuntime(ctx context.Context,
	spec store.AgentRuntimeFailureSpec,
) (store.AgentRuntimeTransitionResult, error) {
	if fake.onFail != nil {
		if err := fake.onFail(ctx, spec); err != nil {
			return store.AgentRuntimeTransitionResult{}, err
		}
	}
	return fake.applyFailure(spec)
}

func (fake *wakeWorkerTestStore) applyAbandon(spec store.AgentUnregisteredRunSpec,
) (store.AgentRuntimeTransitionResult, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	run := fake.runs[spec.RunID]
	if _, complete := run.CompletionReceipt(); !complete && run.Status().Terminal() &&
		run.Error() == spec.Error {
		return fake.resultLocked(store.AgentRuntimeReplayed, spec.RunID), nil
	}
	terminal, err := wakeWorkerMutateRun(run, func(runSpec *model.AgentRunSpec) {
		runSpec.Status = model.AgentRunFailed
		runSpec.FinishedAt = &spec.At
		runSpec.Error = spec.Error
	})
	if err != nil {
		return store.AgentRuntimeTransitionResult{}, err
	}
	handling, err := wakeWorkerPendingHandling(fake.handlings[spec.RunID], spec.At,
		"runtime_unregistered", spec.Error)
	if err != nil {
		return store.AgentRuntimeTransitionResult{}, err
	}
	fake.runs[spec.RunID], fake.handlings[spec.RunID] = terminal, handling
	fake.removeIncompleteLocked(spec.RunID)
	return fake.resultLocked(store.AgentRuntimeApplied, spec.RunID), nil
}

func (fake *wakeWorkerTestStore) applyOrphan(spec store.AgentRuntimeOrphanSpec,
) (store.AgentRuntimeTransitionResult, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	run := fake.runs[spec.RunID]
	if completion, complete := run.CompletionReceipt(); complete &&
		completion.String() == spec.CompletionReceipt.String() && run.Error() == spec.Error {
		return fake.resultLocked(store.AgentRuntimeReplayed, spec.RunID), nil
	}
	terminal, err := wakeWorkerMutateRun(run, func(runSpec *model.AgentRunSpec) {
		runSpec.Status = model.AgentRunFailed
		runSpec.FinishedAt = &spec.At
		runSpec.CompletionAt = &spec.At
		runSpec.CompletionReceipt = &spec.CompletionReceipt
		runSpec.Error = spec.Error
	})
	if err != nil {
		return store.AgentRuntimeTransitionResult{}, err
	}
	handling, err := wakeWorkerPendingHandling(fake.handlings[spec.RunID], spec.At,
		"runtime_orphaned", spec.Error)
	if err != nil {
		return store.AgentRuntimeTransitionResult{}, err
	}
	fake.runs[spec.RunID], fake.handlings[spec.RunID] = terminal, handling
	fake.removeIncompleteLocked(spec.RunID)
	return fake.resultLocked(store.AgentRuntimeApplied, spec.RunID), nil
}

func (fake *wakeWorkerTestStore) applyLaunch(spec store.AgentRuntimeLaunchSpec,
) (store.AgentRuntimeTransitionResult, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	run := fake.runs[spec.RunID]
	if startedAt, started := run.RuntimeStartedAt(); started && startedAt.Equal(spec.At) &&
		run.LauncherDiagnostic().String() == spec.LauncherDiagnostic.String() &&
		run.RuntimeIDs().String() == spec.RuntimeIDs.String() {
		return fake.resultLocked(store.AgentRuntimeReplayed, spec.RunID), nil
	}
	launched, err := wakeWorkerMutateRun(run, func(runSpec *model.AgentRunSpec) {
		runSpec.Status = model.AgentRunRunning
		runSpec.RuntimeStartedAt = &spec.At
		runSpec.LauncherDiagnostic = spec.LauncherDiagnostic
		runSpec.RuntimeIDs = spec.RuntimeIDs
	})
	if err != nil {
		return store.AgentRuntimeTransitionResult{}, err
	}
	fake.runs[spec.RunID] = launched
	fake.replaceIncompleteLocked(launched)
	return fake.resultLocked(store.AgentRuntimeApplied, spec.RunID), nil
}

func (fake *wakeWorkerTestStore) applyWake(spec store.AgentWakeDeliverySpec,
) (store.AgentRuntimeTransitionResult, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	run := fake.runs[spec.RunID]
	if wakeAt, delivered := run.WakeDeliveredAt(); delivered && wakeAt.Equal(spec.At) {
		receipt, _ := run.WakeReceipt()
		if receipt.String() == spec.WakeReceipt.String() {
			return fake.resultLocked(store.AgentRuntimeReplayed, spec.RunID), nil
		}
	}
	woken, err := wakeWorkerMutateRun(run, func(runSpec *model.AgentRunSpec) {
		runSpec.WakeDeliveredAt = &spec.At
		runSpec.WakeReceipt = &spec.WakeReceipt
	})
	if err != nil {
		return store.AgentRuntimeTransitionResult{}, err
	}
	fake.runs[spec.RunID] = woken
	fake.replaceIncompleteLocked(woken)
	return fake.resultLocked(store.AgentRuntimeApplied, spec.RunID), nil
}

func (fake *wakeWorkerTestStore) applyFinish(spec store.AgentRuntimeFinishSpec,
) (store.AgentRuntimeTransitionResult, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	run := fake.runs[spec.RunID]
	if completion, complete := run.CompletionReceipt(); complete &&
		completion.String() == spec.CompletionReceipt.String() && run.Error() == "" {
		return fake.resultLocked(store.AgentRuntimeReplayed, spec.RunID), nil
	}
	finished, err := wakeWorkerMutateRun(run, func(runSpec *model.AgentRunSpec) {
		runSpec.Status = model.AgentRunRuntimeFinished
		runSpec.FinishedAt = &spec.At
		runSpec.CompletionAt = &spec.At
		runSpec.CompletionReceipt = &spec.CompletionReceipt
		runSpec.Error = ""
	})
	if err != nil {
		return store.AgentRuntimeTransitionResult{}, err
	}
	fake.runs[spec.RunID] = finished
	fake.removeIncompleteLocked(spec.RunID)
	return fake.resultLocked(store.AgentRuntimeApplied, spec.RunID), nil
}

func (fake *wakeWorkerTestStore) applyFailure(spec store.AgentRuntimeFailureSpec,
) (store.AgentRuntimeTransitionResult, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	run := fake.runs[spec.RunID]
	if completion, complete := run.CompletionReceipt(); complete &&
		completion.String() == spec.CompletionReceipt.String() && run.Error() == spec.Error &&
		run.LauncherDiagnostic().String() == spec.LauncherDiagnostic.String() &&
		run.RuntimeIDs().String() == spec.RuntimeIDs.String() {
		return fake.resultLocked(store.AgentRuntimeReplayed, spec.RunID), nil
	}
	failed, err := wakeWorkerMutateRun(run, func(runSpec *model.AgentRunSpec) {
		runSpec.Status = model.AgentRunFailed
		runSpec.FinishedAt = &spec.At
		runSpec.CompletionAt = &spec.At
		runSpec.CompletionReceipt = &spec.CompletionReceipt
		runSpec.LauncherDiagnostic = spec.LauncherDiagnostic
		runSpec.RuntimeIDs = spec.RuntimeIDs
		runSpec.Error = spec.Error
	})
	if err != nil {
		return store.AgentRuntimeTransitionResult{}, err
	}
	handling, err := wakeWorkerPendingHandling(fake.handlings[spec.RunID], spec.At,
		"runtime_failed", spec.Error)
	if err != nil {
		return store.AgentRuntimeTransitionResult{}, err
	}
	fake.runs[spec.RunID], fake.handlings[spec.RunID] = failed, handling
	fake.removeIncompleteLocked(spec.RunID)
	return fake.resultLocked(store.AgentRuntimeApplied, spec.RunID), nil
}

func (fake *wakeWorkerTestStore) resultLocked(status store.AgentRuntimeTransitionStatus,
	runID model.RunID,
) store.AgentRuntimeTransitionResult {
	return store.AgentRuntimeTransitionResult{Status: status, Run: fake.runs[runID],
		Handling: fake.handlings[runID]}
}

func (fake *wakeWorkerTestStore) replaceIncompleteLocked(updated model.AgentRun) {
	for index, run := range fake.incomplete {
		if run.ID() == updated.ID() {
			fake.incomplete[index] = updated
			return
		}
	}
}

func (fake *wakeWorkerTestStore) removeIncompleteLocked(runID model.RunID) {
	for index, run := range fake.incomplete {
		if run.ID() == runID {
			fake.incomplete = append(fake.incomplete[:index], fake.incomplete[index+1:]...)
			return
		}
	}
}

func wakeWorkerClaimedHandling(run model.AgentRun) model.Handling {
	handlingID, _ := run.HandlingID()
	fence, _ := run.ClaimFenceHash()
	lease, _ := run.LeaseUntil()
	eventID, err := model.ParseEventID("event-" + run.ID().String())
	if err != nil {
		panic(err)
	}
	handling, err := model.NewHandling(model.HandlingSpec{ID: handlingID,
		ProfileID: run.ProfileID(), EventID: eventID, Status: model.HandlingClaimed,
		AvailableAt: run.StartedAt(), ClaimOwner: "wake-worker-owner", ClaimTokenHash: &fence,
		LeaseUntil: &lease, Attempts: run.HandlingAttempt(), LastDisposition: "claimed",
		RecoveryCount: run.HandlingRecovery(), CreatedAt: run.StartedAt(), UpdatedAt: run.StartedAt()})
	if err != nil {
		panic(err)
	}
	return handling
}

func wakeWorkerPendingHandling(current model.Handling, at time.Time, disposition, errorText string,
) (model.Handling, error) {
	return model.NewHandling(model.HandlingSpec{ID: current.ID(), ProfileID: current.ProfileID(),
		EventID: current.EventID(), Status: model.HandlingPending, Priority: current.Priority(),
		AvailableAt: at.Add(time.Second), Attempts: current.Attempts(), LastDisposition: disposition,
		LastError: errorText, RecoveryCount: current.RecoveryCount(), CreatedAt: current.CreatedAt(),
		UpdatedAt: at})
}

func wakeWorkerMutateRun(run model.AgentRun,
	mutate func(*model.AgentRunSpec),
) (model.AgentRun, error) {
	spec := model.AgentRunSpec{ID: run.ID(), ProfileID: run.ProfileID(), Cause: run.Cause(),
		HandlingAttempt: run.HandlingAttempt(), HandlingRecovery: run.HandlingRecovery(),
		Launcher: run.Launcher(), Runtime: run.Runtime(), LauncherDiagnostic: run.LauncherDiagnostic(),
		RuntimeIDs: run.RuntimeIDs(), Status: run.Status(), StartedAt: run.StartedAt(), Error: run.Error()}
	if value, ok := run.HandlingID(); ok {
		spec.HandlingID = &value
	}
	if value, ok := run.ClaimFenceHash(); ok {
		spec.ClaimFenceHash = &value
	}
	if value, ok := run.LeaseUntil(); ok {
		spec.LeaseUntil = &value
	}
	if value, ok := run.AttachmentTokenHash(); ok {
		spec.AttachmentTokenHash = &value
	}
	if value, ok := run.AttachmentExpiresAt(); ok {
		spec.AttachmentExpiresAt = &value
	}
	if value, ok := run.AttachedAt(); ok {
		spec.AttachedAt = &value
	}
	if value, ok := run.RuntimeStartedAt(); ok {
		spec.RuntimeStartedAt = &value
	}
	if value, ok := run.WakeDeliveredAt(); ok {
		spec.WakeDeliveredAt = &value
	}
	if value, ok := run.WakeReceipt(); ok {
		spec.WakeReceipt = &value
	}
	if value, ok := run.FinishedAt(); ok {
		spec.FinishedAt = &value
	}
	if value, ok := run.CompletionAt(); ok {
		spec.CompletionAt = &value
	}
	if value, ok := run.CurrentReadReceipt(); ok {
		spec.CurrentReadReceipt = &value
	}
	if value, ok := run.OutcomeReceipt(); ok {
		spec.OutcomeReceipt = &value
	}
	if value, ok := run.CompletionReceipt(); ok {
		spec.CompletionReceipt = &value
	}
	mutate(&spec)
	return model.NewAgentRun(spec)
}

func newWakeWorkerForTest(t *testing.T, profile model.Profile, st WakeWorkerStore,
	preparer WakeWorkerPreparer, adapter WakeWorkerAdapter, gate WakeWorkerGate,
	timer WakeWorkerTimer,
) *WakeWorker {
	t.Helper()
	worker, err := NewWakeWorker(WakeWorkerOptions{Profile: profile,
		AssetRevision: profile.ActiveAssetRevision(), Store: st, Preparer: preparer,
		Adapter: adapter, Gate: gate, Clock: &wakeWorkerTestClock{now: profile.UpdatedAt()},
		Timer: timer, PollInterval: time.Millisecond, BackoffInterval: 2 * time.Millisecond,
		SettlementTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func newWakeWorkerTransitionTestWorker(t *testing.T, profile model.Profile,
	st WakeWorkerStore,
) *WakeWorker {
	t.Helper()
	return newWakeWorkerForTest(t, profile, st,
		wakeWorkerPreparerFunc(func(context.Context, model.Profile) (PreparedWake, error) {
			return PreparedWake{}, nil
		}), wakeWorkerAdapterFunc(func(context.Context, CodexWakeRequest) (CodexWakeResult, error) {
			return CodexWakeResult{}, nil
		}), WakeWorkerGateFunc(func(context.Context, model.Profile) error { return nil }),
		newWakeWorkerTestTimer())
}

func wakeWorkerTestProfile(t *testing.T, at time.Time) model.Profile {
	t.Helper()
	profile, err := model.NewProfile(model.ProfileSpec{ID: model.TeamworkProfileID(),
		Principal: "principal-wake-worker", WorkspaceRoot: "/workspace", Host: model.HostCodex,
		Runtime: model.RuntimeCodexAppServer, CredentialHash: model.Sum([]byte("wake-worker-credential")),
		ActiveAssetRevision: "asset-wake-worker", HandlingBudget: model.DefaultHandlingBudget().JSON(),
		Enabled: true, CreatedAt: at, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

func wakeWorkerTestRun(t *testing.T, profile model.Profile, text string, at time.Time,
	launched bool,
) model.AgentRun {
	t.Helper()
	runID, err := model.ParseRunID(text)
	if err != nil {
		t.Fatal(err)
	}
	handlingID, _ := model.ParseHandlingID("handling-" + text)
	fence := model.Sum([]byte("fence-" + text))
	lease := at.Add(5 * time.Minute)
	empty := wakeWorkerJSON(t, `{}`)
	spec := model.AgentRunSpec{ID: runID, ProfileID: profile.ID(), HandlingID: &handlingID,
		Cause: wakeWorkerJSON(t, `{"kind":"wake"}`), HandlingAttempt: 1,
		ClaimFenceHash: &fence, LeaseUntil: &lease, AttachmentTokenHash: &fence,
		AttachmentExpiresAt: &lease, Launcher: "mnemond-wake", Runtime: profile.Runtime(),
		LauncherDiagnostic: empty, RuntimeIDs: empty, Status: model.AgentRunStarting, StartedAt: at}
	if launched {
		started := at.Add(time.Second)
		spec.Status, spec.RuntimeStartedAt = model.AgentRunRunning, &started
		spec.LauncherDiagnostic = wakeWorkerJSON(t, `{"adapter":"test"}`)
		spec.RuntimeIDs = wakeWorkerJSON(t, `{"runtime":"test"}`)
	}
	run, err := model.NewAgentRun(spec)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func wakeWorkerPrepared(t *testing.T, run model.AgentRun) PreparedWake {
	t.Helper()
	nodeState := filepath.Join(t.TempDir(), "node")
	if err := os.Mkdir(nodeState, 0o700); err != nil {
		t.Fatal(err)
	}
	staged, err := localapi.StageRunAttachment(nodeState,
		bytes.NewReader(bytes.Repeat([]byte{0xa7}, 48)))
	if err != nil {
		t.Fatal(err)
	}
	attachment, err := staged.Publish(run.ID())
	if err != nil {
		t.Fatal(err)
	}
	return PreparedWake{status: store.AgentClaimActionable, run: run,
		attachment: attachment, nodeState: nodeState}
}

func wakeWorkerJSON(t *testing.T, text string) model.JSON {
	t.Helper()
	value, err := model.NewJSON([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	return value
}
