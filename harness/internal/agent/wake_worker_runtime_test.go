package agent

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestNewWakeWorkerAcceptsClaudeProfile(t *testing.T) {
	at := time.Date(2026, 7, 18, 8, 55, 0, 0, time.UTC)
	profileSpec := wakeWorkerTestProfile(t, at).Spec()
	profileSpec.Host, profileSpec.Runtime = model.HostClaudeCode, model.RuntimeClaudeCLI
	profile, err := model.NewProfile(profileSpec)
	if err != nil {
		t.Fatal(err)
	}
	worker := newWakeWorkerForTest(t, profile, newWakeWorkerTestStore(),
		wakeWorkerPreparerFunc(func(context.Context, model.Profile) (PreparedWake, error) {
			return PreparedWake{}, nil
		}), wakeWorkerAdapterFunc(func(context.Context, CodexWakeRequest) (CodexWakeResult, error) {
			return CodexWakeResult{}, nil
		}), WakeWorkerGateFunc(func(context.Context, model.Profile) error { return nil }),
		newWakeWorkerTestTimer())
	if worker.profile.Runtime() != model.RuntimeClaudeCLI {
		t.Fatalf("worker Runtime = %s", worker.profile.Runtime())
	}
}

func TestWakeWorkerRecordsCallbacksAndSettlesNormalOrFailedRuntime(t *testing.T) {
	at := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)
	for _, scenario := range []wakeWorkerCallbackScenario{
		{name: "normal", wantFinal: "finish"},
		{name: "adapter failure", adapterErr: errors.New("private /operator/path"),
			wantFinal: "fail", wantError: wakeWorkerAdapterFailure},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			fixture := newWakeWorkerCallbackFixture(t, at, scenario)
			worker := fixture.worker(t)
			if err := worker.Run(fixture.ctx); err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			fixture.assertRun(t, worker)
		})
	}
}

type wakeWorkerCallbackScenario struct {
	name       string
	adapterErr error
	wantFinal  string
	wantError  string
}

type wakeWorkerCallbackFixture struct {
	at         time.Time
	scenario   wakeWorkerCallbackScenario
	profile    model.Profile
	run        model.AgentRun
	prepared   PreparedWake
	st         *wakeWorkerTestStore
	ctx        context.Context
	cancel     context.CancelFunc
	sequenceMu sync.Mutex
	sequence   []string
	runtimeCtx context.Context
}

func newWakeWorkerCallbackFixture(t *testing.T, at time.Time,
	scenario wakeWorkerCallbackScenario,
) *wakeWorkerCallbackFixture {
	t.Helper()
	profile := wakeWorkerTestProfile(t, at)
	run := wakeWorkerTestRun(t, profile, "run-worker-"+strings.ReplaceAll(scenario.name, " ", "-"),
		at, false)
	ctx, cancel := context.WithCancel(context.Background())
	fixture := &wakeWorkerCallbackFixture{at: at, scenario: scenario, profile: profile,
		run: run, prepared: wakeWorkerPrepared(t, run), st: newWakeWorkerTestStore(run),
		ctx: ctx, cancel: cancel}
	fixture.installStoreCallbacks(t)
	return fixture
}

func (fixture *wakeWorkerCallbackFixture) installStoreCallbacks(t *testing.T) {
	fixture.st.onLaunch = func(callbackCtx context.Context, spec store.AgentRuntimeLaunchSpec) error {
		fixture.assertRuntimeCallback(t, "launch", callbackCtx, spec.RunID)
		return nil
	}
	fixture.st.onWake = func(callbackCtx context.Context, spec store.AgentWakeDeliverySpec) error {
		fixture.assertRuntimeCallback(t, "wake", callbackCtx, spec.RunID)
		return nil
	}
	fixture.st.onFinish = func(settleCtx context.Context, spec store.AgentRuntimeFinishSpec) error {
		fixture.appendSequence("finish")
		if settleCtx.Err() != nil || spec.RunID != fixture.run.ID() ||
			spec.CompletionReceipt.IsZero() {
			t.Errorf("finish settlement = %#v, ctx=%v", spec, settleCtx.Err())
		}
		return nil
	}
	fixture.st.onFail = func(settleCtx context.Context, spec store.AgentRuntimeFailureSpec) error {
		fixture.appendSequence("fail")
		if settleCtx.Err() != nil || spec.RunID != fixture.run.ID() ||
			spec.Error != fixture.scenario.wantError || strings.Contains(spec.Error, "operator") {
			t.Errorf("failure settlement = %#v, ctx=%v", spec, settleCtx.Err())
		}
		return nil
	}
}

func (fixture *wakeWorkerCallbackFixture) assertRuntimeCallback(t *testing.T,
	name string, callbackCtx context.Context, runID model.RunID,
) {
	var callbackErr error
	if callbackCtx != nil {
		callbackErr = callbackCtx.Err()
	}
	if callbackCtx == nil || fixture.runtimeCtx == nil || callbackCtx != fixture.runtimeCtx ||
		callbackErr != nil || runID != fixture.run.ID() {
		t.Errorf("%s callback authority = run %s ctx=%v runtime=%v err=%v",
			name, runID.String(), callbackCtx, fixture.runtimeCtx, callbackErr)
	}
	fixture.appendSequence(name)
}

func (fixture *wakeWorkerCallbackFixture) worker(t *testing.T) *WakeWorker {
	return newWakeWorkerForTest(t, fixture.profile, fixture.st,
		wakeWorkerPreparerFunc(func(context.Context, model.Profile) (PreparedWake, error) {
			fixture.appendSequence("prepare")
			return fixture.prepared, nil
		}), fixture.adapter(t), WakeWorkerGateFunc(func(context.Context, model.Profile) error {
			fixture.appendSequence("gate")
			return nil
		}), newWakeWorkerTestTimer())
}

func (fixture *wakeWorkerCallbackFixture) adapter(t *testing.T) WakeWorkerAdapter {
	return wakeWorkerAdapterFunc(func(adapterCtx context.Context,
		request CodexWakeRequest,
	) (CodexWakeResult, error) {
		fixture.appendSequence("adapter")
		fixture.runtimeCtx = adapterCtx
		if _, ok := adapterCtx.Deadline(); !ok {
			t.Error("adapter context has no bounded deadline")
		}
		return fixture.runAdapterCallbacks(t, adapterCtx, request)
	})
}

func (fixture *wakeWorkerCallbackFixture) runAdapterCallbacks(t *testing.T,
	adapterCtx context.Context, request CodexWakeRequest,
) (CodexWakeResult, error) {
	diagnostic := wakeWorkerJSON(t, `{"adapter":"test"}`)
	runtimeIDs := wakeWorkerJSON(t, `{"runtime":"test"}`)
	if err := request.Callbacks.RecordLaunch(adapterCtx, CodexLaunchEvidence{
		At: fixture.at.Add(time.Second), Diagnostic: diagnostic, RuntimeIDs: runtimeIDs}); err != nil {
		return CodexWakeResult{}, err
	}
	wakeAt, wakeReceipt, wakeDelivered := fixture.recordOptionalWake(t, adapterCtx, request)
	status := "failed"
	if fixture.scenario.adapterErr == nil {
		status = "completed"
	}
	completion, err := codexCompletionReceipt(status, "thread-worker", "turn-worker",
		wakeDelivered, "wait_without_signal", nil)
	if err != nil {
		t.Fatal(err)
	}
	fixture.cancel()
	return CodexWakeResult{At: fixture.at.Add(3 * time.Second), Diagnostic: diagnostic,
			RuntimeIDs: runtimeIDs, WakeAt: wakeAt, WakeReceipt: wakeReceipt,
			CompletionReceipt: completion, WakeDelivered: wakeDelivered, ProcessExited: true},
		fixture.scenario.adapterErr
}

func (fixture *wakeWorkerCallbackFixture) recordOptionalWake(t *testing.T,
	adapterCtx context.Context, request CodexWakeRequest,
) (time.Time, model.JSON, bool) {
	if fixture.scenario.adapterErr != nil {
		return time.Time{}, model.JSON{}, false
	}
	wakeAt := fixture.at.Add(2 * time.Second)
	wakeReceipt := wakeWorkerJSON(t, `{"wake":"test"}`)
	if err := request.Callbacks.RecordWake(adapterCtx, CodexWakeEvidence{
		At: wakeAt, Receipt: wakeReceipt}); err != nil {
		t.Fatal(err)
	}
	return wakeAt, wakeReceipt, true
}

func (fixture *wakeWorkerCallbackFixture) assertRun(t *testing.T, worker *WakeWorker) {
	got := fixture.sequenceSnapshot()
	want := "gate,prepare,adapter,launch"
	if fixture.scenario.adapterErr == nil {
		want += ",wake"
	}
	want += "," + fixture.scenario.wantFinal
	if strings.Join(got, ",") != want {
		t.Fatalf("Runtime sequence = %v, want %s", got, want)
	}
	if snapshot := worker.Snapshot(); !snapshot.Healthy || snapshot.Running {
		t.Fatalf("worker snapshot = %#v", snapshot)
	}
}

func (fixture *wakeWorkerCallbackFixture) appendSequence(value string) {
	fixture.sequenceMu.Lock()
	fixture.sequence = append(fixture.sequence, value)
	fixture.sequenceMu.Unlock()
}

func (fixture *wakeWorkerCallbackFixture) sequenceSnapshot() []string {
	fixture.sequenceMu.Lock()
	defer fixture.sequenceMu.Unlock()
	return append([]string(nil), fixture.sequence...)
}

func TestWakeWorkerBoundsAdapterContextByClaimLease(t *testing.T) {
	at := time.Date(2026, 7, 18, 11, 30, 0, 0, time.UTC)
	profile := wakeWorkerTestProfile(t, at)
	run := wakeWorkerTestRun(t, profile, "run-worker-runtime-deadline", at, false)
	run = wakeWorkerRunWithLease(t, run, at.Add(time.Nanosecond))
	prepared := wakeWorkerPrepared(t, run)
	st := newWakeWorkerTestStore(run)
	var failCalls atomic.Int32
	st.onFail = func(settleCtx context.Context, spec store.AgentRuntimeFailureSpec) error {
		if settleCtx.Err() != nil || spec.Error != wakeWorkerAdapterFailure ||
			!strings.Contains(spec.CompletionReceipt.String(), `"exit_method":"not_started"`) {
			t.Errorf("deadline failure spec = %#v, ctx=%v", spec, settleCtx.Err())
		}
		failCalls.Add(1)
		return nil
	}
	var adapterCalls atomic.Int32
	worker := newWakeWorkerForTest(t, profile, st,
		wakeWorkerPreparerFunc(func(context.Context, model.Profile) (PreparedWake, error) {
			return PreparedWake{}, nil
		}), wakeWorkerAdapterFunc(func(adapterCtx context.Context,
			request CodexWakeRequest,
		) (CodexWakeResult, error) {
			adapterCalls.Add(1)
			select {
			case <-adapterCtx.Done():
			default:
				t.Fatal("adapter context was not canceled at the expired lease boundary")
			}
			if !errors.Is(adapterCtx.Err(), context.DeadlineExceeded) {
				t.Fatalf("adapter context error = %v", adapterCtx.Err())
			}
			if request.RunAttachmentEnvironment != prepared.Environment() {
				t.Fatalf("attachment environment = %q", request.RunAttachmentEnvironment)
			}
			completion, err := codexCompletionReceipt("launch_failed", "", "", false,
				"not_started", nil)
			if err != nil {
				t.Fatal(err)
			}
			return CodexWakeResult{At: at.Add(2 * time.Second), CompletionReceipt: completion,
				ProcessExited: true}, adapterCtx.Err()
		}), WakeWorkerGateFunc(func(context.Context, model.Profile) error { return nil }),
		newWakeWorkerTestTimer())
	fence, _ := run.ClaimFenceHash()
	if delay, issue := worker.runPrepared(context.Background(), prepared, fence); delay != worker.backoffInterval || issue != "" {
		t.Fatalf("runPrepared = (%s, %q)", delay, issue)
	}
	if adapterCalls.Load() != 1 || failCalls.Load() != 1 {
		t.Fatalf("adapter/fail calls = %d/%d", adapterCalls.Load(), failCalls.Load())
	}
}

func TestWakeWorkerPrepareFailureUsesRuntimeAdapterEvidence(t *testing.T) {
	at := time.Date(2026, 7, 18, 13, 5, 0, 0, time.UTC)
	profileSpec := wakeWorkerTestProfile(t, at).Spec()
	profileSpec.Host, profileSpec.Runtime = model.HostClaudeCode, model.RuntimeClaudeCLI
	profile, err := model.NewProfile(profileSpec)
	if err != nil {
		t.Fatal(err)
	}
	run := wakeWorkerTestRun(t, profile, "run-worker-claude-publish", at, false)
	st := newWakeWorkerTestStore(run)
	ctx, cancel := context.WithCancel(context.Background())
	st.onFail = func(settleCtx context.Context, spec store.AgentRuntimeFailureSpec) error {
		if settleCtx.Err() != nil || spec.Error != wakeWorkerPrepareFailure ||
			!strings.Contains(spec.LauncherDiagnostic.String(), `"adapter":"claude-cli"`) ||
			strings.Contains(spec.LauncherDiagnostic.String(), `"adapter":"codex-app-server"`) ||
			!strings.Contains(spec.CompletionReceipt.String(), `"adapter":"claude-cli"`) {
			t.Errorf("Claude prepare failure spec = %#v, ctx=%v", spec, settleCtx.Err())
		}
		cancel()
		return nil
	}
	worker := newWakeWorkerForTest(t, profile, st,
		wakeWorkerPreparerFunc(func(context.Context, model.Profile) (PreparedWake, error) {
			return PreparedWake{status: store.AgentClaimActionable, run: run},
				errors.New("private-path/collision")
		}), wakeWorkerAdapterFunc(func(context.Context, CodexWakeRequest) (CodexWakeResult, error) {
			t.Fatal("adapter ran after attachment publish failure")
			return CodexWakeResult{}, nil
		}), WakeWorkerGateFunc(func(context.Context, model.Profile) error { return nil }),
		newWakeWorkerTestTimer())
	if err := worker.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

func wakeWorkerRunWithLease(t *testing.T, run model.AgentRun, lease time.Time) model.AgentRun {
	t.Helper()
	updated, err := wakeWorkerMutateRun(run, func(runSpec *model.AgentRunSpec) {
		runSpec.LeaseUntil = &lease
		runSpec.AttachmentExpiresAt = &lease
	})
	if err != nil {
		t.Fatal(err)
	}
	return updated
}
