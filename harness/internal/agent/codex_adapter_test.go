package agent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestCodexWakeAdapterRunsOneStaticManagedTurn(t *testing.T) {
	fixture := newCodexAdapterFixture(t, fakeCodexScenario{additiveFields: true})
	result, err := fixture.adapter.Run(context.Background(), fixture.request())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !result.WakeDelivered || !result.ProcessExited || result.Diagnostic.IsZero() || result.RuntimeIDs.IsZero() ||
		result.WakeReceipt.IsZero() || result.CompletionReceipt.IsZero() {
		t.Fatalf("Run() result = %#v", result)
	}
	if !strings.Contains(result.CompletionReceipt.String(), `"exit_method":"wait_without_signal"`) ||
		!strings.Contains(result.CompletionReceipt.String(), `"signals":[]`) {
		t.Fatalf("completion receipt = %s", result.CompletionReceipt.String())
	}
	if len(fixture.launches) != 1 || len(fixture.wakes) != 1 ||
		!fixture.launches[0].At.Before(fixture.wakes[0].At) ||
		!fixture.wakes[0].At.Before(result.At) {
		t.Fatalf("evidence times = launch %#v, wake %#v, completion %s",
			fixture.launches, fixture.wakes, result.At)
	}
	if strings.Contains(result.RuntimeIDs.String(), "thread-managed") ||
		strings.Contains(result.RuntimeIDs.String(), "turn-managed") ||
		!strings.Contains(result.RuntimeIDs.String(), `"pid":91`) ||
		!strings.Contains(result.WakeReceipt.String(), WakeCue) ||
		!strings.Contains(result.WakeReceipt.String(), `"thread_id":"thread-managed"`) {
		t.Fatalf("launch/wake evidence = %s / %s", result.RuntimeIDs.String(),
			result.WakeReceipt.String())
	}

	requests := fixture.starter.requests()
	if got := requestMethods(requests); strings.Join(got, ",") !=
		"initialize,initialized,hooks/list,thread/start,turn/start" {
		t.Fatalf("request methods = %v", got)
	}
	initialize := requests[0]
	client := initialize["params"].(map[string]any)["clientInfo"].(map[string]any)
	if client["name"] != codexClientName || client["version"] != codexClientVersion ||
		client["title"] != codexClientTitle {
		t.Fatalf("initialize clientInfo = %#v", client)
	}
	thread := requests[3]["params"].(map[string]any)
	if thread["cwd"] != fixture.workspace || thread["ephemeral"] != true ||
		thread["approvalPolicy"] != "never" || thread["approvalsReviewer"] != "user" ||
		thread["sandbox"] != codexSandboxMode {
		t.Fatalf("thread/start params = %#v", thread)
	}
	turn := requests[4]["params"].(map[string]any)
	input := turn["input"].([]any)
	text := input[0].(map[string]any)
	skill := input[1].(map[string]any)
	if turn["threadId"] != "thread-managed" || text["type"] != "text" ||
		text["text"] != "$mnemon-harness" || skill["type"] != "skill" ||
		skill["name"] != "mnemon-harness" || skill["path"] != filepath.Join(fixture.workspace,
		".agents", "skills", "mnemon-harness", "SKILL.md") {
		t.Fatalf("turn/start params = %#v", turn)
	}
	sandbox := turn["sandboxPolicy"].(map[string]any)
	if turn["cwd"] != fixture.workspace || turn["approvalPolicy"] != "never" ||
		turn["approvalsReviewer"] != "user" ||
		sandbox["type"] != codexSandboxPolicyType || sandbox["networkAccess"] != false ||
		sandbox["excludeTmpdirEnvVar"] != false || sandbox["excludeSlashTmp"] != false ||
		len(sandbox["writableRoots"].([]any)) != 0 {
		t.Fatalf("turn/start authority = %#v", turn)
	}
	if got := fixture.sequenceSnapshot(); indexOf(got, "callback:launch") < 0 ||
		indexOf(got, "request:initialize") < indexOf(got, "callback:launch") ||
		indexOf(got, "request:thread/start") < indexOf(got, "callback:launch") ||
		indexOf(got, "callback:wake") < indexOf(got, "emit:hook/completed") ||
		lastIndexOf(got, "clock:now") < indexOf(got, "wait:return") {
		t.Fatalf("protocol/callback sequence = %v", got)
	}
	spec := fixture.starter.spec()
	if len(spec.Environment) != 2 || spec.Environment[0] != "PATH=/usr/bin:/bin" ||
		spec.Environment[1] != fixture.attachment || strings.Join(spec.Arguments, " ") !=
		"app-server --stdio" || spec.Directory != fixture.workspace {
		t.Fatalf("process spec = %#v", spec)
	}
	if fixture.starter.process.waitCount.Load() != 1 {
		t.Fatalf("Wait count = %d", fixture.starter.process.waitCount.Load())
	}
}

func TestCodexWakeAdapterRejectsInvalidManagedHookProofs(t *testing.T) {
	tests := []struct {
		name     string
		scenario fakeCodexScenario
	}{
		{name: "missing", scenario: fakeCodexScenario{omitHook: true}},
		{name: "wrong cue", scenario: fakeCodexScenario{wrongHookCue: true}},
		{name: "wrong execution mode", scenario: fakeCodexScenario{wrongHookMode: true}},
		{name: "completion before start", scenario: fakeCodexScenario{omitHookStart: true}},
		{name: "duplicate start", scenario: fakeCodexScenario{duplicateHookStart: true}},
		{name: "duplicate completion", scenario: fakeCodexScenario{duplicateHookCompletion: true}},
		{name: "queued duplicate completion after terminal", scenario: fakeCodexScenario{
			notificationsBeforeTurnResponse: true, duplicateHookAfterTerminal: true}},
		{name: "changed start timestamp", scenario: fakeCodexScenario{changedHookStartedAt: true}},
		{name: "oversized Hook ID", scenario: fakeCodexScenario{oversizedHookID: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCodexAdapterFixture(t, test.scenario)
			result, err := fixture.adapter.Run(context.Background(), fixture.request())
			if !errors.Is(err, ErrCodexWakeAdapter) {
				t.Fatalf("Run() = (%#v, %v), want adapter error", result, err)
			}
			if len(fixture.launches) != 1 {
				t.Fatalf("launch callbacks = %d", len(fixture.launches))
			}
			if fixture.starter.process.waitCount.Load() != 1 {
				t.Fatalf("Wait count = %d", fixture.starter.process.waitCount.Load())
			}
		})
	}
}

func TestCodexWakeAdapterProcessesNotificationsThatPrecedeTurnStartResponse(t *testing.T) {
	fixture := newCodexAdapterFixture(t, fakeCodexScenario{notificationsBeforeTurnResponse: true})
	result, err := fixture.adapter.Run(context.Background(), fixture.request())
	if err != nil || !result.WakeDelivered || !result.ProcessExited || len(fixture.wakes) != 1 {
		t.Fatalf("Run() = (%#v, %v), wakes=%d", result, err, len(fixture.wakes))
	}
	if fixture.starter.process.waitCount.Load() != 1 {
		t.Fatalf("Wait count = %d", fixture.starter.process.waitCount.Load())
	}
}

func TestCodexWakeAdapterRejectsProtocolViolations(t *testing.T) {
	tests := []struct {
		name     string
		scenario fakeCodexScenario
	}{
		{name: "server request", scenario: fakeCodexScenario{serverRequest: true}},
		{name: "invalid JSON", scenario: fakeCodexScenario{invalidJSON: true}},
		{name: "oversize JSONL", scenario: fakeCodexScenario{oversizeJSON: true}},
		{name: "nested duplicate authority", scenario: fakeCodexScenario{nestedDuplicate: true}},
		{name: "wrong response ID", scenario: fakeCodexScenario{wrongResponseID: true}},
		{name: "premature exit", scenario: fakeCodexScenario{prematureExit: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCodexAdapterFixture(t, test.scenario)
			_, err := fixture.adapter.Run(context.Background(), fixture.request())
			if !errors.Is(err, ErrCodexWakeAdapter) {
				t.Fatalf("Run() error = %v", err)
			}
			if fixture.starter.process.waitCount.Load() != 1 {
				t.Fatalf("Wait count = %d", fixture.starter.process.waitCount.Load())
			}
		})
	}
}

func TestCodexWakeAdapterCancelsThroughInterruptAndClosesProcess(t *testing.T) {
	fixture := newCodexAdapterFixture(t, fakeCodexScenario{blockTurn: true})
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct {
		result CodexWakeResult
		err    error
	}, 1)
	go func() {
		result, err := fixture.adapter.Run(ctx, fixture.request())
		finished <- struct {
			result CodexWakeResult
			err    error
		}{result, err}
	}()
	select {
	case <-fixture.wakeRecorded:
	case <-time.After(time.Second):
		t.Fatal("managed Hook proof was not recorded")
	}
	cancel()
	select {
	case outcome := <-finished:
		if !errors.Is(outcome.err, ErrCodexWakeAdapter) || !outcome.result.WakeDelivered {
			t.Fatalf("Run() after cancel = (%#v, %v)", outcome.result, outcome.err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not finish after cancellation")
	}
	requests := fixture.starter.requests()
	if indexOf(requestMethods(requests), "turn/interrupt") < 0 {
		t.Fatalf("request methods = %v", requestMethods(requests))
	}
	if fixture.starter.process.waitCount.Load() != 1 {
		t.Fatalf("Wait count = %d", fixture.starter.process.waitCount.Load())
	}
}

func TestCodexWakeAdapterPreservesObservedEvidenceAcrossCallbackFailures(t *testing.T) {
	t.Run("launch callback", func(t *testing.T) {
		fixture := newCodexAdapterFixture(t, fakeCodexScenario{})
		request := fixture.request()
		request.Callbacks.RecordLaunch = func(context.Context, CodexLaunchEvidence) error {
			return errors.New("store launch unavailable")
		}
		result, err := fixture.adapter.Run(context.Background(), request)
		if !errors.Is(err, ErrCodexWakeAdapter) || strings.Contains(err.Error(), "store launch unavailable") ||
			!result.ProcessExited ||
			result.LaunchAt.IsZero() || result.Diagnostic.IsZero() ||
			result.RuntimeIDs.IsZero() || !result.WakeReceipt.IsZero() || result.WakeDelivered {
			t.Fatalf("Run() = (%#v, %v)", result, err)
		}
		if fixture.starter.process.waitCount.Load() != 1 {
			t.Fatalf("Wait count = %d", fixture.starter.process.waitCount.Load())
		}
		if requests := fixture.starter.requests(); len(requests) != 0 {
			t.Fatalf("protocol requests before durable launch = %#v", requests)
		}
	})
	t.Run("wake callback", func(t *testing.T) {
		fixture := newCodexAdapterFixture(t, fakeCodexScenario{})
		request := fixture.request()
		request.Callbacks.RecordWake = func(context.Context, CodexWakeEvidence) error {
			return errors.New("store wake unavailable")
		}
		result, err := fixture.adapter.Run(context.Background(), request)
		if !errors.Is(err, ErrCodexWakeAdapter) || strings.Contains(err.Error(), "store wake unavailable") ||
			!result.ProcessExited ||
			result.LaunchAt.IsZero() || result.WakeAt.IsZero() || result.Diagnostic.IsZero() ||
			result.RuntimeIDs.IsZero() || result.WakeReceipt.IsZero() || !result.WakeDelivered ||
			!strings.Contains(result.CompletionReceipt.String(), `"wake_delivered":true`) {
			t.Fatalf("Run() = (%#v, %v)", result, err)
		}
		if fixture.starter.process.waitCount.Load() != 1 {
			t.Fatalf("Wait count = %d", fixture.starter.process.waitCount.Load())
		}
	})
}

func TestCodexWakeAdapterFailsClosedAcrossLaunchAndRuntimeFailures(t *testing.T) {
	t.Run("starter", func(t *testing.T) {
		workspace := t.TempDir()
		adapter, err := NewCodexWakeAdapter(CodexWakeAdapterOptions{Executable: "/usr/bin/codex",
			Workspace: workspace, Environment: []string{"PATH=/usr/bin"},
			Starter: failingCodexStarter{}, Identity: fixedCodexIdentity{}, Clock: newFakeCodexClock(),
			Terminator: &fakeCodexTerminator{}, VerifyProjection: passCodexProjection,
			InterruptGrace: time.Millisecond,
			ExitGrace:      time.Millisecond, SignalGrace: time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		result, err := adapter.Run(context.Background(), CodexWakeRequest{
			RunAttachmentEnvironment: localapi.RunAttachmentEnv + "=/tmp/run.attach",
			Callbacks:                testCodexCallbacks{}.callbacks()})
		if !errors.Is(err, ErrCodexWakeAdapter) || strings.Contains(err.Error(), "start unavailable") ||
			!codexResultWasNotStarted(result) {
			t.Fatalf("Run() = (%#v, %v)", result, err)
		}
	})
	t.Run("starter returns no process", func(t *testing.T) {
		workspace := t.TempDir()
		adapter, err := NewCodexWakeAdapter(CodexWakeAdapterOptions{Executable: "/usr/bin/codex",
			Workspace: workspace, Environment: []string{"PATH=/usr/bin"},
			Starter: nilCodexStarter{}, Identity: fixedCodexIdentity{}, Clock: newFakeCodexClock(),
			Terminator: &fakeCodexTerminator{}, VerifyProjection: passCodexProjection,
			InterruptGrace: time.Millisecond,
			ExitGrace:      time.Millisecond, SignalGrace: time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		result, err := adapter.Run(context.Background(), CodexWakeRequest{
			RunAttachmentEnvironment: localapi.RunAttachmentEnv + "=/tmp/run.attach",
			Callbacks:                testCodexCallbacks{}.callbacks()})
		if !errors.Is(err, ErrCodexWakeAdapter) || !codexResultWasNotStarted(result) {
			t.Fatalf("Run() = (%#v, %v)", result, err)
		}
	})
	t.Run("starter and clock", func(t *testing.T) {
		workspace := t.TempDir()
		adapter, err := NewCodexWakeAdapter(CodexWakeAdapterOptions{Executable: "/usr/bin/codex",
			Workspace: workspace, Environment: []string{"PATH=/usr/bin"},
			Starter: failingCodexStarter{}, Identity: fixedCodexIdentity{}, Clock: invalidCodexClock{},
			Terminator: &fakeCodexTerminator{}, VerifyProjection: passCodexProjection,
			InterruptGrace: time.Millisecond,
			ExitGrace:      time.Millisecond, SignalGrace: time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		result, err := adapter.Run(context.Background(), CodexWakeRequest{
			RunAttachmentEnvironment: localapi.RunAttachmentEnv + "=/tmp/run.attach",
			Callbacks:                testCodexCallbacks{}.callbacks()})
		if !errors.Is(err, ErrCodexWakeAdapter) || !result.ProcessExited ||
			!result.At.IsZero() || !result.CompletionReceipt.IsZero() ||
			!strings.Contains(err.Error(), "completion evidence") {
			t.Fatalf("Run() = (%#v, %v)", result, err)
		}
	})
	t.Run("incomplete started process", func(t *testing.T) {
		done := make(chan struct{})
		var sequenceMu sync.Mutex
		var sequence []string
		addSequence := func(value string) {
			sequenceMu.Lock()
			defer sequenceMu.Unlock()
			sequence = append(sequence, value)
		}
		process := &fakeCodexProcess{pid: 91, stdin: discardCodexWriteCloser{},
			stderr: io.NopCloser(strings.NewReader("")), done: done, sequence: addSequence}
		terminator := &fakeCodexTerminator{signals: []string{"SIGSTOP", "SIGKILL"},
			onTerminate: process.finish, sequence: addSequence}
		workspace := t.TempDir()
		adapter, err := NewCodexWakeAdapter(CodexWakeAdapterOptions{Executable: "/usr/bin/codex",
			Workspace: workspace, Environment: []string{"PATH=/usr/bin"},
			Starter: incompleteCodexStarter{process}, Identity: fixedCodexIdentity{},
			Clock: newFakeCodexClock(), Terminator: terminator,
			VerifyProjection: passCodexProjection,
			InterruptGrace:   time.Millisecond, ExitGrace: time.Millisecond,
			SignalGrace: time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		result, err := adapter.Run(context.Background(), CodexWakeRequest{
			RunAttachmentEnvironment: localapi.RunAttachmentEnv + "=/tmp/run.attach",
			Callbacks:                testCodexCallbacks{}.callbacks()})
		if !errors.Is(err, ErrCodexWakeAdapter) || !result.ProcessExited ||
			!strings.Contains(result.CompletionReceipt.String(), `"status":"launch_contract_failed"`) ||
			!strings.Contains(result.CompletionReceipt.String(), `"exit_method":"signal_assisted"`) ||
			process.waitCount.Load() != 1 || terminator.callCount() != 1 {
			t.Fatalf("Run() = (%#v, %v), Wait=%d", result, err, process.waitCount.Load())
		}
		sequenceMu.Lock()
		gotSequence := append([]string(nil), sequence...)
		sequenceMu.Unlock()
		if indexOf(gotSequence, "terminate") < 0 ||
			indexOf(gotSequence, "wait:return") < indexOf(gotSequence, "terminate") {
			t.Fatalf("registered cleanup sequence = %v", gotSequence)
		}
		process.signalMu.Lock()
		directSignals := append([]syscall.Signal(nil), process.signals...)
		process.signalMu.Unlock()
		if len(directSignals) != 0 {
			t.Fatalf("registered process received direct signals = %v", directSignals)
		}
	})
	t.Run("initialize error is redacted", func(t *testing.T) {
		fixture := newCodexAdapterFixture(t, fakeCodexScenario{initializeError: true})
		_, err := fixture.adapter.Run(context.Background(), fixture.request())
		if !errors.Is(err, ErrCodexWakeAdapter) || strings.Contains(err.Error(), "private-server-detail") {
			t.Fatalf("Run() error = %v", err)
		}
		if len(fixture.launches) != 1 || fixture.starter.process.waitCount.Load() != 1 {
			t.Fatalf("launches/Wait = %d/%d", len(fixture.launches),
				fixture.starter.process.waitCount.Load())
		}
	})
	t.Run("identity", func(t *testing.T) {
		fixture := newCodexAdapterFixtureWithIdentity(t, fakeCodexScenario{}, failingCodexIdentity{})
		result, err := fixture.adapter.Run(context.Background(), fixture.request())
		if !errors.Is(err, ErrCodexWakeAdapter) || strings.Contains(err.Error(), "identity unavailable") ||
			!result.Diagnostic.IsZero() ||
			len(fixture.launches) != 0 || fixture.starter.process.waitCount.Load() != 1 ||
			fixture.terminator.callCount() != 0 {
			t.Fatalf("Run() = (%#v, %v), launches/Wait=%d/%d", result, err,
				len(fixture.launches), fixture.starter.process.waitCount.Load())
		}
	})
	t.Run("stderr overflow", func(t *testing.T) {
		fixture := newCodexAdapterFixture(t, fakeCodexScenario{stderrOverflow: true})
		_, err := fixture.adapter.Run(context.Background(), fixture.request())
		if !errors.Is(err, ErrCodexWakeAdapter) || fixture.starter.process.waitCount.Load() != 1 {
			t.Fatalf("Run() error/Wait = %v/%d", err, fixture.starter.process.waitCount.Load())
		}
	})
	t.Run("late stderr overflow", func(t *testing.T) {
		fixture := newCodexAdapterFixture(t, fakeCodexScenario{lateStderrOverflow: true})
		result, err := fixture.adapter.Run(context.Background(), fixture.request())
		if !errors.Is(err, ErrCodexWakeAdapter) || !result.ProcessExited || !result.WakeDelivered ||
			!strings.Contains(result.CompletionReceipt.String(), `"status":"cleanup_failed"`) ||
			fixture.starter.process.waitCount.Load() != 1 {
			t.Fatalf("Run() = (%#v, %v), Wait=%d", result, err,
				fixture.starter.process.waitCount.Load())
		}
	})
	t.Run("cleanup failure", func(t *testing.T) {
		fixture := newCodexAdapterFixture(t, fakeCodexScenario{waitError: true})
		result, err := fixture.adapter.Run(context.Background(), fixture.request())
		if !errors.Is(err, ErrCodexWakeAdapter) || !result.WakeDelivered ||
			!strings.Contains(result.CompletionReceipt.String(), `"status":"cleanup_failed"`) ||
			result.At.IsZero() || fixture.starter.process.waitCount.Load() != 1 {
			t.Fatalf("Run() = (%#v, %v), Wait=%d", result, err,
				fixture.starter.process.waitCount.Load())
		}
	})
	t.Run("SIGKILL without exit proof", func(t *testing.T) {
		fixture := newCodexAdapterFixture(t, fakeCodexScenario{stubbornExit: true})
		result, err := fixture.adapter.Run(context.Background(), fixture.request())
		if !errors.Is(err, ErrCodexWakeAdapter) || result.ProcessExited || !result.At.IsZero() ||
			!result.CompletionReceipt.IsZero() || fixture.starter.process.waitCount.Load() != 0 {
			t.Fatalf("Run() = (%#v, %v), Wait=%d", result, err,
				fixture.starter.process.waitCount.Load())
		}
		fixture.starter.process.finish()
	})
	t.Run("signal-assisted exit is not clean completion", func(t *testing.T) {
		fixture := newCodexAdapterFixture(t, fakeCodexScenario{stubbornExit: true,
			exitOnTerminate: true})
		result, err := fixture.adapter.Run(context.Background(), fixture.request())
		if !errors.Is(err, ErrCodexWakeAdapter) || !result.ProcessExited || !result.WakeDelivered ||
			!strings.Contains(result.CompletionReceipt.String(), `"status":"cleanup_failed"`) ||
			!strings.Contains(result.CompletionReceipt.String(), `"exit_method":"signal_assisted"`) ||
			!strings.Contains(result.CompletionReceipt.String(), `"signals":["SIGSTOP","SIGKILL"]`) {
			t.Fatalf("Run() = (%#v, %v)", result, err)
		}
	})
	t.Run("undrained process pipe blocks Wait", func(t *testing.T) {
		fixture := newCodexAdapterFixture(t, fakeCodexScenario{stuckPipeDrain: true})
		result, err := fixture.adapter.Run(context.Background(), fixture.request())
		if fixture.starter.stuckStdout != nil {
			t.Cleanup(fixture.starter.stuckStdout.releaseReader)
		}
		if !errors.Is(err, ErrCodexWakeAdapter) || !result.WakeDelivered || result.ProcessExited ||
			!result.At.IsZero() || !result.CompletionReceipt.IsZero() ||
			fixture.starter.process.waitCount.Load() != 0 || fixture.starter.stuckStdout == nil {
			t.Fatalf("Run() = (%#v, %v), Wait/stuck=%d/%#v", result, err,
				fixture.starter.process.waitCount.Load(), fixture.starter.stuckStdout)
		}
		fixture.starter.stuckStdout.releaseReader()
		select {
		case <-fixture.starter.stuckStdout.returned:
		case <-time.After(time.Second):
			t.Fatal("forced-close test reader did not return after release")
		}
	})
}

func TestCodexAdapterFailureKeepsPrivateCausesClosed(t *testing.T) {
	private := errors.New("private callback detail")
	err := codexAdapterError("callback", errors.Join(context.Canceled, private))
	if !errors.Is(err, ErrCodexWakeAdapter) || !errors.Is(err, context.Canceled) ||
		errors.Is(err, private) || errors.Unwrap(err) != ErrCodexWakeAdapter ||
		strings.Contains(err.Error(), private.Error()) {
		t.Fatalf("closed adapter error = %v, unwrap = %v", err, errors.Unwrap(err))
	}
}

func TestCodexWakeAdapterRejectsThreadAuthorityDrift(t *testing.T) {
	for _, test := range []struct {
		name     string
		scenario fakeCodexScenario
	}{
		{name: "approval policy", scenario: fakeCodexScenario{wrongThreadApproval: true}},
		{name: "response cwd", scenario: fakeCodexScenario{wrongThreadCWD: true}},
		{name: "thread cwd", scenario: fakeCodexScenario{wrongThreadInnerCWD: true}},
		{name: "persistent thread", scenario: fakeCodexScenario{nonEphemeralThread: true}},
		{name: "sandbox type", scenario: fakeCodexScenario{wrongThreadSandbox: true}},
		{name: "sandbox missing", scenario: fakeCodexScenario{missingThreadSandbox: true}},
		{name: "sandbox network", scenario: fakeCodexScenario{threadSandboxNetwork: true}},
		{name: "sandbox roots", scenario: fakeCodexScenario{threadSandboxRoots: true}},
		{name: "sandbox temp", scenario: fakeCodexScenario{threadSandboxTemp: true}},
		{name: "reviewer", scenario: fakeCodexScenario{wrongThreadReviewer: true}},
		{name: "required model", scenario: fakeCodexScenario{missingThreadModel: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCodexAdapterFixture(t, test.scenario)
			result, err := fixture.adapter.Run(context.Background(), fixture.request())
			if !errors.Is(err, ErrCodexWakeAdapter) || result.WakeDelivered ||
				fixture.starter.process.waitCount.Load() != 1 {
				t.Fatalf("Run() = (%#v, %v), Wait=%d", result, err,
					fixture.starter.process.waitCount.Load())
			}
			if methods := requestMethods(fixture.starter.requests()); indexOf(methods, "turn/start") >= 0 {
				t.Fatalf("requests after invalid thread authority = %v", methods)
			}
		})
	}
}

func TestCodexWakeAdapterTerminatesAfterObservationFailure(t *testing.T) {
	fixture := newCodexAdapterFixture(t, fakeCodexScenario{observeFailure: true})
	result, err := fixture.adapter.Run(context.Background(), fixture.request())
	if !errors.Is(err, ErrCodexWakeAdapter) || !result.ProcessExited || !result.WakeDelivered ||
		!strings.Contains(result.CompletionReceipt.String(), `"status":"cleanup_failed"`) ||
		fixture.terminator.callCount() != 1 || fixture.starter.process.waitCount.Load() != 1 {
		t.Fatalf("Run() = (%#v, %v), terminate/Wait=%d/%d", result, err,
			fixture.terminator.callCount(), fixture.starter.process.waitCount.Load())
	}
}

func TestCodexCompletionReceiptRejectsCrossFieldContradictions(t *testing.T) {
	valid := []struct {
		status, threadID, turnID, method string
		wake                             bool
		signals                          []string
	}{
		{"launch_failed", "", "", "not_started", false, nil},
		{"launch_identity_failed", "", "", "signal_assisted", false, []string{"SIGKILL"}},
		{"launch_contract_failed", "", "", "signal_assisted", false, []string{"SIGSTOP", "SIGKILL"}},
		{"failed", "thread", "turn", "wait_without_signal", false, nil},
		{"completed", "thread", "turn", "wait_without_signal", true, nil},
		{"cleanup_failed", "thread", "turn", "signal_assisted", true,
			[]string{"SIGSTOP", "SIGKILL"}},
	}
	for _, test := range valid {
		if _, err := codexCompletionReceipt(test.status, test.threadID, test.turnID,
			test.wake, test.method, test.signals); err != nil {
			t.Fatalf("valid completion %#v: %v", test, err)
		}
	}
	invalid := valid
	invalid = append(invalid,
		struct {
			status, threadID, turnID, method string
			wake                             bool
			signals                          []string
		}{"completed", "thread", "turn", "signal_assisted", true, []string{"SIGKILL"}},
	)
	invalid[0].wake = true
	invalid[1].threadID = "thread"
	invalid[2].method = "not_started"
	invalid[3].turnID, invalid[3].threadID = "turn", ""
	invalid[4].wake = false
	invalid[5].turnID = ""
	for _, test := range invalid {
		if _, err := codexCompletionReceipt(test.status, test.threadID, test.turnID,
			test.wake, test.method, test.signals); err == nil {
			t.Fatalf("invalid completion accepted: %#v", test)
		}
	}
}

func TestCodexWakeAdapterRejectsNonCompletedTurns(t *testing.T) {
	for _, status := range []string{"failed", "interrupted"} {
		t.Run(status, func(t *testing.T) {
			fixture := newCodexAdapterFixture(t, fakeCodexScenario{turnStatus: status})
			result, err := fixture.adapter.Run(context.Background(), fixture.request())
			if !errors.Is(err, ErrCodexWakeAdapter) || !result.WakeDelivered ||
				len(fixture.wakes) != 1 || fixture.starter.process.waitCount.Load() != 1 {
				t.Fatalf("Run() = (%#v, %v), wakes/Wait=%d/%d", result, err,
					len(fixture.wakes), fixture.starter.process.waitCount.Load())
			}
		})
	}
}

func TestCodexWakeAdapterRequiresOneExactManagedHookRegistration(t *testing.T) {
	for _, test := range []struct {
		name     string
		scenario fakeCodexScenario
	}{
		{name: "missing", scenario: fakeCodexScenario{missingHookRegistration: true}},
		{name: "duplicate", scenario: fakeCodexScenario{duplicateHookRegistration: true}},
		{name: "wrong", scenario: fakeCodexScenario{wrongHookRegistration: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCodexAdapterFixture(t, test.scenario)
			result, err := fixture.adapter.Run(context.Background(), fixture.request())
			if !errors.Is(err, ErrCodexWakeAdapter) || result.Diagnostic.IsZero() ||
				len(fixture.launches) != 1 || len(fixture.wakes) != 0 ||
				fixture.starter.process.waitCount.Load() != 1 {
				t.Fatalf("Run() = (%#v, %v), launch/wake/Wait=%d/%d/%d", result, err,
					len(fixture.launches), len(fixture.wakes), fixture.starter.process.waitCount.Load())
			}
		})
	}
}

func TestCodexWakeAdapterRejectsInvalidClockEvidence(t *testing.T) {
	workspace := t.TempDir()
	starter := newFakeCodexStarter(fakeCodexScenario{}, nil)
	adapter, err := NewCodexWakeAdapter(CodexWakeAdapterOptions{Executable: "/usr/bin/codex",
		Workspace: workspace, Environment: []string{"PATH=/usr/bin"}, Starter: starter,
		Identity: fixedCodexIdentity{}, Clock: invalidCodexClock{}, Terminator: &fakeCodexTerminator{},
		VerifyProjection: passCodexProjection,
		InterruptGrace:   time.Millisecond, ExitGrace: time.Millisecond, SignalGrace: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Run(context.Background(), CodexWakeRequest{
		RunAttachmentEnvironment: localapi.RunAttachmentEnv + "=/tmp/run.attach",
		Callbacks:                testCodexCallbacks{}.callbacks()})
	if !errors.Is(err, ErrCodexWakeAdapter) || !result.Diagnostic.IsZero() ||
		starter.process.waitCount.Load() != 1 {
		t.Fatalf("Run() = (%#v, %v), Wait=%d", result, err, starter.process.waitCount.Load())
	}
}

func TestCodexWakeAdapterValidatesClosedConstructionAndPerRunAuthority(t *testing.T) {
	workspace := t.TempDir()
	base := CodexWakeAdapterOptions{Executable: "/usr/bin/codex", Workspace: workspace,
		Environment: []string{"PATH=/usr/bin"}, Starter: &fakeCodexStarter{},
		Identity: fixedCodexIdentity{}, Clock: newFakeCodexClock(), Terminator: &fakeCodexTerminator{},
		VerifyProjection: passCodexProjection,
		InterruptGrace:   time.Millisecond, ExitGrace: time.Millisecond, SignalGrace: time.Millisecond}
	for _, mutate := range []func(*CodexWakeAdapterOptions){
		func(value *CodexWakeAdapterOptions) { value.Executable = "codex" },
		func(value *CodexWakeAdapterOptions) { value.Workspace = "." },
		func(value *CodexWakeAdapterOptions) {
			value.Environment = append(value.Environment, localapi.RunAttachmentEnv+"=/stale")
		},
		func(value *CodexWakeAdapterOptions) { value.Environment = []string{"PATH=/a", "PATH=/b"} },
		func(value *CodexWakeAdapterOptions) { value.VerifyProjection = nil },
	} {
		value := base
		mutate(&value)
		if adapter, err := NewCodexWakeAdapter(value); adapter != nil ||
			!errors.Is(err, ErrCodexWakeAdapter) {
			t.Fatalf("NewCodexWakeAdapter() = (%#v, %v)", adapter, err)
		}
	}
	withDefaultIdentity := base
	withDefaultIdentity.Identity = nil
	if adapter, err := NewCodexWakeAdapter(withDefaultIdentity); err != nil || adapter == nil {
		t.Fatalf("NewCodexWakeAdapter(default identity) = (%#v, %v)", adapter, err)
	}
	starter := newFakeCodexStarter(fakeCodexScenario{}, nil)
	base.Starter = starter
	adapter, err := NewCodexWakeAdapter(base)
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []CodexWakeRequest{
		{},
		{RunAttachmentEnvironment: "WRONG=/tmp/run", Callbacks: testCodexCallbacks{}.callbacks()},
		{RunAttachmentEnvironment: localapi.RunAttachmentEnv + "=", Callbacks: testCodexCallbacks{}.callbacks()},
	} {
		if _, err := adapter.Run(context.Background(), request); !errors.Is(err, ErrCodexWakeAdapter) {
			t.Fatalf("Run(%#v) error = %v", request, err)
		}
	}
	preCanceledStarter := newFakeCodexStarter(fakeCodexScenario{}, nil)
	preCanceled := base
	preCanceled.Starter = preCanceledStarter
	var preCanceledProjectionCalls atomic.Int32
	preCanceled.VerifyProjection = func() error {
		preCanceledProjectionCalls.Add(1)
		return nil
	}
	adapter, err = NewCodexWakeAdapter(preCanceled)
	if err != nil {
		t.Fatal(err)
	}
	preCanceledContext, cancelPreCanceled := context.WithCancel(context.Background())
	cancelPreCanceled()
	result, err := adapter.Run(preCanceledContext, CodexWakeRequest{
		RunAttachmentEnvironment: localapi.RunAttachmentEnv + "=/tmp/run.attach",
		Callbacks:                testCodexCallbacks{}.callbacks()})
	if !errors.Is(err, ErrCodexWakeAdapter) || !errors.Is(err, context.Canceled) ||
		!codexResultWasNotStarted(result) || preCanceledProjectionCalls.Load() != 0 ||
		preCanceledStarter.process != nil {
		t.Fatalf("pre-canceled Run() = (%#v, %v), projection calls/process = %d/%#v",
			result, err, preCanceledProjectionCalls.Load(), preCanceledStarter.process)
	}

	canceledAfterProjectionStarter := newFakeCodexStarter(fakeCodexScenario{}, nil)
	canceledAfterProjection := base
	canceledAfterProjection.Starter = canceledAfterProjectionStarter
	afterProjectionContext, cancelAfterProjection := context.WithCancel(context.Background())
	canceledAfterProjection.VerifyProjection = func() error {
		cancelAfterProjection()
		return nil
	}
	adapter, err = NewCodexWakeAdapter(canceledAfterProjection)
	if err != nil {
		t.Fatal(err)
	}
	result, err = adapter.Run(afterProjectionContext, CodexWakeRequest{
		RunAttachmentEnvironment: localapi.RunAttachmentEnv + "=/tmp/run.attach",
		Callbacks:                testCodexCallbacks{}.callbacks()})
	if !errors.Is(err, ErrCodexWakeAdapter) || !errors.Is(err, context.Canceled) ||
		!codexResultWasNotStarted(result) || canceledAfterProjectionStarter.process != nil {
		t.Fatalf("projection-canceled Run() = (%#v, %v), process = %#v",
			result, err, canceledAfterProjectionStarter.process)
	}

	projectionStarter := newFakeCodexStarter(fakeCodexScenario{}, nil)
	projectionFailure := base
	projectionFailure.Starter = projectionStarter
	projectionFailure.VerifyProjection = func() error { return errors.New("private projection drift") }
	adapter, err = NewCodexWakeAdapter(projectionFailure)
	if err != nil {
		t.Fatal(err)
	}
	result, err = adapter.Run(context.Background(), CodexWakeRequest{
		RunAttachmentEnvironment: localapi.RunAttachmentEnv + "=/tmp/run.attach",
		Callbacks:                testCodexCallbacks{}.callbacks()})
	if !errors.Is(err, ErrCodexWakeAdapter) || strings.Contains(err.Error(), "private projection drift") ||
		!codexResultWasNotStarted(result) || projectionStarter.process != nil {
		t.Fatalf("projection-gated Run() = (%#v, %v), process = %#v",
			result, err, projectionStarter.process)
	}
}

type codexAdapterFixture struct {
	t            *testing.T
	adapter      *CodexWakeAdapter
	starter      *fakeCodexStarter
	workspace    string
	attachment   string
	clock        *fakeCodexClock
	terminator   *fakeCodexTerminator
	mu           sync.Mutex
	launches     []CodexLaunchEvidence
	wakes        []CodexWakeEvidence
	sequence     []string
	wakeRecorded chan struct{}
}

func newCodexAdapterFixture(t *testing.T, scenario fakeCodexScenario) *codexAdapterFixture {
	return newCodexAdapterFixtureWithIdentity(t, scenario, fixedCodexIdentity{})
}

func newCodexAdapterFixtureWithIdentity(t *testing.T, scenario fakeCodexScenario,
	identity CodexProcessIdentityProbe,
) *codexAdapterFixture {
	t.Helper()
	fixture := &codexAdapterFixture{t: t, workspace: t.TempDir(), clock: newFakeCodexClock(),
		wakeRecorded: make(chan struct{}, 1)}
	fixture.clock.sequence = fixture.addSequence
	fixture.attachment = localapi.RunAttachmentEnv + "=" + filepath.Join(fixture.workspace, "run.attach")
	fixture.starter = newFakeCodexStarter(scenario, fixture.addSequence)
	fixture.terminator = &fakeCodexTerminator{}
	if scenario.observeFailure {
		fixture.terminator.observeErr = errors.New("private observation failure")
	}
	if scenario.stubbornExit {
		fixture.terminator.observeErr = ErrRuntimeProcessLive
		fixture.terminator.err = ErrRuntimeProcessLive
	}
	if scenario.exitOnTerminate {
		fixture.terminator.signals = []string{"SIGSTOP", "SIGKILL"}
		fixture.terminator.onTerminate = fixture.starter.finishProcess
		fixture.terminator.err = nil
	}
	adapter, err := NewCodexWakeAdapter(CodexWakeAdapterOptions{Executable: "/usr/bin/codex",
		Workspace: fixture.workspace, Environment: []string{"PATH=/usr/bin:/bin"},
		Starter: fixture.starter, Identity: identity, Clock: fixture.clock,
		Terminator: fixture.terminator, VerifyProjection: passCodexProjection,
		InterruptGrace: 2 * time.Millisecond,
		ExitGrace:      5 * time.Millisecond, SignalGrace: 2 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	fixture.adapter = adapter
	return fixture
}

func (fixture *codexAdapterFixture) request() CodexWakeRequest {
	return CodexWakeRequest{RunAttachmentEnvironment: fixture.attachment,
		Callbacks: CodexWakeCallbacks{
			RecordLaunch: func(_ context.Context, evidence CodexLaunchEvidence) error {
				fixture.mu.Lock()
				defer fixture.mu.Unlock()
				fixture.launches = append(fixture.launches, evidence)
				fixture.sequence = append(fixture.sequence, "callback:launch")
				return nil
			},
			RecordWake: func(_ context.Context, evidence CodexWakeEvidence) error {
				fixture.mu.Lock()
				defer fixture.mu.Unlock()
				fixture.wakes = append(fixture.wakes, evidence)
				fixture.sequence = append(fixture.sequence, "callback:wake")
				select {
				case fixture.wakeRecorded <- struct{}{}:
				default:
				}
				return nil
			},
		}}
}

func (fixture *codexAdapterFixture) addSequence(value string) {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	fixture.sequence = append(fixture.sequence, value)
}

func (fixture *codexAdapterFixture) sequenceSnapshot() []string {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	return append([]string(nil), fixture.sequence...)
}

type fakeCodexClock struct {
	mu       sync.Mutex
	now      time.Time
	sequence func(string)
}

func newFakeCodexClock() *fakeCodexClock {
	return &fakeCodexClock{now: time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)}
}

func (clock *fakeCodexClock) Now() time.Time {
	clock.mu.Lock()
	clock.now = clock.now.Add(time.Second)
	result, sequence := clock.now, clock.sequence
	clock.mu.Unlock()
	if sequence != nil {
		sequence("clock:now")
	}
	return result
}

func (*fakeCodexClock) After(duration time.Duration) <-chan time.Time {
	return time.After(duration)
}

type fixedCodexIdentity struct{}

func (fixedCodexIdentity) Identify(pid int) (CodexProcessIdentity, error) {
	token := "linux-boot"
	if runtime.GOOS == "linux" {
		token = "linux:11111111-1111-4111-8111-111111111111:12345"
	} else if runtime.GOOS == "darwin" {
		token = "darwin:11111111-1111-4111-8111-111111111111:12345:678"
	}
	runtimeIDs, err := model.JSONFrom(runtimeProcessIDs{SchemaVersion: runtimeProcessSchemaVersion,
		OS: runtime.GOOS, PID: pid, PGID: pid, SID: pid, UID: uint32(os.Geteuid()),
		StartToken: token})
	return CodexProcessIdentity{RuntimeIDs: runtimeIDs}, err
}

type failingCodexIdentity struct{}

func (failingCodexIdentity) Identify(int) (CodexProcessIdentity, error) {
	return CodexProcessIdentity{}, errors.New("identity unavailable")
}

type invalidCodexClock struct{}

func (invalidCodexClock) Now() time.Time { return time.Time{} }
func (invalidCodexClock) After(duration time.Duration) <-chan time.Time {
	return time.After(duration)
}

type failingCodexStarter struct{}

func (failingCodexStarter) Start(CodexProcessStartSpec) (CodexProcess, error) {
	return nil, errors.New("start unavailable")
}

type nilCodexStarter struct{}

func (nilCodexStarter) Start(CodexProcessStartSpec) (CodexProcess, error) { return nil, nil }

type incompleteCodexStarter struct{ process CodexProcess }

func (starter incompleteCodexStarter) Start(CodexProcessStartSpec) (CodexProcess, error) {
	return starter.process, nil
}

type discardCodexWriteCloser struct{}

func (discardCodexWriteCloser) Write(value []byte) (int, error) { return len(value), nil }
func (discardCodexWriteCloser) Close() error                    { return nil }

type stuckCodexReadCloser struct {
	delegate    io.ReadCloser
	release     chan struct{}
	returned    chan struct{}
	releaseOnce sync.Once
}

func newStuckCodexReadCloser(delegate io.ReadCloser) *stuckCodexReadCloser {
	return &stuckCodexReadCloser{delegate: delegate, release: make(chan struct{}),
		returned: make(chan struct{})}
}

func (reader *stuckCodexReadCloser) Read(value []byte) (int, error) {
	read, err := reader.delegate.Read(value)
	if err == nil {
		return read, nil
	}
	<-reader.release
	close(reader.returned)
	return read, err
}

// Close deliberately cannot wake Read. This test double proves the adapter
// never starts Wait when an injected process violates the real pipe contract.
func (*stuckCodexReadCloser) Close() error { return nil }

func (reader *stuckCodexReadCloser) releaseReader() {
	reader.releaseOnce.Do(func() {
		close(reader.release)
		_ = reader.delegate.Close()
	})
}

type fakeCodexTerminator struct {
	mu          sync.Mutex
	signals     []string
	onTerminate func()
	sequence    func(string)
	observeErr  error
	err         error
	calls       int
}

func (terminator *fakeCodexTerminator) Observe(context.Context, model.JSON) error {
	terminator.mu.Lock()
	defer terminator.mu.Unlock()
	return terminator.observeErr
}

func (terminator *fakeCodexTerminator) Terminate(context.Context, model.JSON) ([]string, error) {
	terminator.mu.Lock()
	defer terminator.mu.Unlock()
	terminator.calls++
	if terminator.sequence != nil {
		terminator.sequence("terminate")
	}
	if terminator.onTerminate != nil {
		terminator.onTerminate()
	}
	return append([]string(nil), terminator.signals...), terminator.err
}

func (terminator *fakeCodexTerminator) callCount() int {
	terminator.mu.Lock()
	defer terminator.mu.Unlock()
	return terminator.calls
}

type fakeCodexScenario struct {
	additiveFields                  bool
	omitHook                        bool
	omitHookStart                   bool
	wrongHookCue                    bool
	wrongHookMode                   bool
	duplicateHookStart              bool
	duplicateHookCompletion         bool
	changedHookStartedAt            bool
	oversizedHookID                 bool
	notificationsBeforeTurnResponse bool
	serverRequest                   bool
	invalidJSON                     bool
	oversizeJSON                    bool
	nestedDuplicate                 bool
	wrongResponseID                 bool
	prematureExit                   bool
	blockTurn                       bool
	initializeError                 bool
	stderrOverflow                  bool
	lateStderrOverflow              bool
	turnStatus                      string
	missingHookRegistration         bool
	duplicateHookRegistration       bool
	wrongHookRegistration           bool
	waitError                       bool
	stubbornExit                    bool
	exitOnTerminate                 bool
	wrongThreadApproval             bool
	wrongThreadCWD                  bool
	wrongThreadInnerCWD             bool
	nonEphemeralThread              bool
	wrongThreadSandbox              bool
	missingThreadSandbox            bool
	threadSandboxNetwork            bool
	threadSandboxRoots              bool
	threadSandboxTemp               bool
	wrongThreadReviewer             bool
	missingThreadModel              bool
	observeFailure                  bool
	duplicateHookAfterTerminal      bool
	stuckPipeDrain                  bool
}

type fakeCodexStarter struct {
	mu          sync.Mutex
	scenario    fakeCodexScenario
	startedSpec CodexProcessStartSpec
	process     *fakeCodexProcess
	received    []map[string]any
	sequence    func(string)
	turnStarted chan struct{}
	stuckStdout *stuckCodexReadCloser
}

func newFakeCodexStarter(scenario fakeCodexScenario, sequence func(string)) *fakeCodexStarter {
	return &fakeCodexStarter{scenario: scenario, sequence: sequence, turnStarted: make(chan struct{})}
}

func (starter *fakeCodexStarter) Start(spec CodexProcessStartSpec) (CodexProcess, error) {
	clientRead, serverWrite := io.Pipe()
	serverRead, clientWrite := io.Pipe()
	stderrRead, stderrWrite := io.Pipe()
	var stdout io.ReadCloser = clientRead
	if starter.scenario.stuckPipeDrain {
		starter.stuckStdout = newStuckCodexReadCloser(clientRead)
		stdout = starter.stuckStdout
	}
	process := &fakeCodexProcess{pid: 91, stdin: clientWrite, stdout: stdout,
		stderr: stderrRead, done: make(chan struct{}), sequence: starter.sequence}
	if starter.scenario.waitError {
		process.waitErr = errors.New("unexpected process wait failure")
	}
	starter.mu.Lock()
	starter.startedSpec = CodexProcessStartSpec{Executable: spec.Executable,
		Arguments: append([]string(nil), spec.Arguments...), Directory: spec.Directory,
		Environment: append([]string(nil), spec.Environment...)}
	starter.process = process
	starter.mu.Unlock()
	go starter.serve(serverRead, serverWrite, stderrWrite, process)
	return process, nil
}

func (starter *fakeCodexStarter) serve(input *io.PipeReader, output, stderr *io.PipeWriter,
	process *fakeCodexProcess,
) {
	defer func() {
		if !starter.scenario.stubbornExit {
			process.finish()
		}
	}()
	defer input.Close()
	defer output.Close()
	defer stderr.Close()
	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		var request map[string]any
		decoder := json.NewDecoder(bytes.NewReader(scanner.Bytes()))
		decoder.UseNumber()
		if decoder.Decode(&request) != nil {
			return
		}
		starter.mu.Lock()
		starter.received = append(starter.received, request)
		starter.mu.Unlock()
		method, _ := request["method"].(string)
		if starter.sequence != nil {
			starter.sequence("request:" + method)
		}
		id, hasID := request["id"]
		switch method {
		case "initialize":
			if starter.scenario.stderrOverflow {
				_, _ = io.WriteString(stderr, strings.Repeat("e", codexStderrMax+1))
			}
			if starter.scenario.invalidJSON {
				_, _ = io.WriteString(output, "{not-json}\n")
				continue
			}
			if starter.scenario.oversizeJSON {
				_, _ = io.WriteString(output, strings.Repeat("x", codexProtocolLineMax+2)+"\n")
				continue
			}
			if starter.scenario.nestedDuplicate {
				_, _ = io.WriteString(output, `{"id":1,"result":{"codexHome":"/tmp/codex-home",`+
					`"platformFamily":"unix","platformOs":"test","userAgent":"first",`+
					`"userAgent":"second"}}`+"\n")
				continue
			}
			if starter.scenario.serverRequest {
				starter.emit(output, map[string]any{"id": 99, "method": "server/request",
					"params": map[string]any{}})
				continue
			}
			if starter.scenario.initializeError {
				starter.emit(output, map[string]any{"id": id, "error": map[string]any{
					"code": -32603, "message": "private-server-detail"}})
				continue
			}
			responseID := id
			if starter.scenario.wrongResponseID {
				responseID = json.Number("77")
			}
			result := map[string]any{"codexHome": "/tmp/codex-home", "platformFamily": "unix",
				"platformOs": "test", "userAgent": "codex-test"}
			if starter.scenario.additiveFields {
				result["futureField"] = map[string]any{"accepted": true}
			}
			starter.emit(output, map[string]any{"id": responseID, "result": result})
		case "initialized":
		case "hooks/list":
			starter.emit(output, map[string]any{"id": id, "result": starter.hooksResult(specWorkspace(starter))})
		case "thread/start":
			workspace := specWorkspace(starter)
			approval, responseCWD, threadCWD, ephemeral := "never", workspace, workspace, true
			reviewer, model, sandboxType := "user", "test-model", codexSandboxPolicyType
			if starter.scenario.wrongThreadApproval {
				approval = "on-request"
			}
			if starter.scenario.wrongThreadCWD {
				responseCWD += "-other"
			}
			if starter.scenario.wrongThreadInnerCWD {
				threadCWD += "-other"
			}
			if starter.scenario.nonEphemeralThread {
				ephemeral = false
			}
			if starter.scenario.wrongThreadReviewer {
				reviewer = "auto_review"
			}
			if starter.scenario.missingThreadModel {
				model = ""
			}
			if starter.scenario.wrongThreadSandbox {
				sandboxType = "dangerFullAccess"
			}
			sandbox := map[string]any{"type": sandboxType, "writableRoots": []any{},
				"networkAccess": false, "excludeTmpdirEnvVar": false, "excludeSlashTmp": false}
			if starter.scenario.threadSandboxNetwork {
				sandbox["networkAccess"] = true
			}
			if starter.scenario.threadSandboxRoots {
				sandbox["writableRoots"] = []any{"/tmp/other"}
			}
			if starter.scenario.threadSandboxTemp {
				sandbox["excludeSlashTmp"] = true
			}
			result := map[string]any{"approvalPolicy": approval, "approvalsReviewer": reviewer,
				"cwd": responseCWD, "model": model, "modelProvider": "openai",
				"thread": map[string]any{"cliVersion": "0.144.4", "createdAt": 1000,
					"cwd": threadCWD, "ephemeral": ephemeral, "id": "thread-managed",
					"modelProvider": "openai", "preview": "", "sessionId": "thread-managed",
					"source": "vscode", "status": map[string]any{"type": "idle"},
					"turns": []any{}, "updatedAt": 1000, "future": true}, "future": true}
			if !starter.scenario.missingThreadSandbox {
				result["sandbox"] = sandbox
			}
			starter.emit(output, map[string]any{"id": id, "result": result})
		case "turn/start":
			if starter.scenario.notificationsBeforeTurnResponse {
				starter.emitManagedTurn(output)
			}
			starter.emit(output, map[string]any{"id": id, "result": map[string]any{
				"turn": map[string]any{"id": "turn-managed", "status": "inProgress", "items": []any{},
					"future": true}}})
			close(starter.turnStarted)
			if starter.scenario.prematureExit {
				return
			}
			if starter.scenario.blockTurn {
				starter.emitManagedHookProof(output)
				continue
			}
			if starter.scenario.notificationsBeforeTurnResponse {
				continue
			}
			starter.emitManagedTurn(output)
		case "turn/interrupt":
			if hasID {
				starter.emit(output, map[string]any{"id": id, "result": map[string]any{}})
			}
			starter.emit(output, map[string]any{"method": "turn/completed", "params": map[string]any{
				"threadId": "thread-managed", "turn": map[string]any{"id": "turn-managed",
					"status": "interrupted", "items": []any{}}}})
		}
	}
	if starter.scenario.lateStderrOverflow {
		_, _ = io.WriteString(stderr, strings.Repeat("e", codexStderrMax+1))
	}
}

func (starter *fakeCodexStarter) hooksResult(workspace string) map[string]any {
	hook := map[string]any{"command": filepath.Join(workspace, ".codex", "hooks", "mnemon-harness", "hook.sh"),
		"currentHash": "sha256:test", "displayOrder": 1, "enabled": true,
		"eventName": "userPromptSubmit", "handlerType": "command", "isManaged": false,
		"key": "project:userPromptSubmit:1", "source": "project",
		"sourcePath":    filepath.Join(workspace, ".codex", "hooks.json"),
		"statusMessage": codexHookStatus, "timeoutSec": 10, "trustStatus": "trusted"}
	if starter.scenario.additiveFields {
		hook["futureField"] = []any{"accepted"}
	}
	if starter.scenario.wrongHookRegistration {
		hook["command"] = filepath.Join(workspace, ".codex", "hooks", "other", "hook.sh")
	}
	hooks := []any{hook}
	if starter.scenario.missingHookRegistration {
		hooks = []any{}
	}
	if starter.scenario.duplicateHookRegistration {
		hooks = append(hooks, hook)
	}
	return map[string]any{"data": []any{map[string]any{"cwd": workspace, "errors": []any{},
		"warnings": []any{}, "hooks": hooks, "futureField": true}}, "futureField": true}
}

func (starter *fakeCodexStarter) emitManagedTurn(output *io.PipeWriter) {
	starter.emitManagedHookProof(output)
	starter.emit(output, map[string]any{"method": "item/agentMessage/delta", "params": map[string]any{
		"delta": "discard this assistant output", "threadId": "thread-managed", "turnId": "turn-managed"}})
	status := starter.scenario.turnStatus
	if status == "" {
		status = "completed"
	}
	starter.emit(output, map[string]any{"method": "turn/completed", "params": map[string]any{
		"threadId": "thread-managed", "turn": map[string]any{"id": "turn-managed",
			"status": status, "items": []any{}, "future": true}, "future": true}})
	if starter.scenario.duplicateHookAfterTerminal {
		completed := starter.hookRun("completed", []any{map[string]any{
			"kind": "context", "text": WakeCue}})
		starter.emitHook(output, "hook/completed", completed)
	}
}

func (starter *fakeCodexStarter) emitManagedHookProof(output *io.PipeWriter) {
	if starter.scenario.additiveFields {
		starter.emit(output, map[string]any{"method": "future/changed",
			"futureEnvelopeField": true})
	}
	started := starter.hookRun("running", nil)
	if !starter.scenario.omitHook && !starter.scenario.omitHookStart {
		starter.emitHook(output, "hook/started", started)
		if starter.scenario.duplicateHookStart {
			starter.emitHook(output, "hook/started", started)
		}
	}
	if !starter.scenario.omitHook {
		cue := WakeCue
		if starter.scenario.wrongHookCue {
			cue += " changed"
		}
		completed := starter.hookRun("completed", []any{map[string]any{"kind": "context", "text": cue}})
		starter.emitHook(output, "hook/completed", completed)
		if starter.scenario.duplicateHookCompletion {
			starter.emitHook(output, "hook/completed", completed)
		}
	}
}

func (starter *fakeCodexStarter) hookRun(status string, entries []any) map[string]any {
	startedAt := 1000
	if status == "completed" && starter.scenario.changedHookStartedAt {
		startedAt = 1002
	}
	run := map[string]any{"id": "hook-managed", "displayOrder": 1, "entries": entries,
		"eventName": "userPromptSubmit", "executionMode": "sync", "handlerType": "command",
		"scope": "turn", "source": "project", "sourcePath": filepath.Join(specWorkspace(starter),
			".codex", "hooks.json"), "startedAt": startedAt, "status": status,
		"statusMessage": codexHookStatus}
	if starter.scenario.oversizedHookID {
		run["id"] = strings.Repeat("h", 257)
	}
	if entries == nil {
		run["entries"] = []any{}
	}
	if status == "completed" {
		run["completedAt"] = 1003
		run["durationMs"] = 3
	}
	if starter.scenario.wrongHookMode {
		run["executionMode"] = "async"
	}
	if starter.scenario.additiveFields {
		run["futureField"] = true
	}
	return run
}

func (starter *fakeCodexStarter) emitHook(output *io.PipeWriter, method string,
	run map[string]any,
) {
	if starter.sequence != nil {
		starter.sequence("emit:" + method)
	}
	starter.emit(output, map[string]any{"method": method, "params": map[string]any{
		"threadId": "thread-managed", "turnId": "turn-managed", "run": run,
		"futureField": true}})
}

func (starter *fakeCodexStarter) emit(output *io.PipeWriter, value any) {
	if starter.scenario.additiveFields {
		if envelope, ok := value.(map[string]any); ok {
			envelope["futureEnvelopeField"] = map[string]any{"accepted": true}
		}
	}
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(value)
}

func (starter *fakeCodexStarter) requests() []map[string]any {
	starter.mu.Lock()
	defer starter.mu.Unlock()
	result := make([]map[string]any, len(starter.received))
	copy(result, starter.received)
	return result
}

func (starter *fakeCodexStarter) spec() CodexProcessStartSpec {
	starter.mu.Lock()
	defer starter.mu.Unlock()
	return starter.startedSpec
}

func (starter *fakeCodexStarter) finishProcess() {
	starter.mu.Lock()
	process := starter.process
	starter.mu.Unlock()
	if process != nil {
		process.finish()
	}
}

func specWorkspace(starter *fakeCodexStarter) string {
	starter.mu.Lock()
	defer starter.mu.Unlock()
	return starter.startedSpec.Directory
}

type fakeCodexProcess struct {
	pid       int
	stdin     io.WriteCloser
	stdout    io.ReadCloser
	stderr    io.ReadCloser
	done      chan struct{}
	doneOnce  sync.Once
	waitCount atomic.Int32
	waitErr   error
	signalErr error
	signals   []syscall.Signal
	signalMu  sync.Mutex
	sequence  func(string)
}

func (process *fakeCodexProcess) PID() int              { return process.pid }
func (process *fakeCodexProcess) Stdin() io.WriteCloser { return process.stdin }
func (process *fakeCodexProcess) Stdout() io.ReadCloser { return process.stdout }
func (process *fakeCodexProcess) Stderr() io.ReadCloser { return process.stderr }
func (process *fakeCodexProcess) Signal(signal syscall.Signal) error {
	process.signalMu.Lock()
	process.signals = append(process.signals, signal)
	err := process.signalErr
	process.signalMu.Unlock()
	select {
	case <-process.done:
		return os.ErrProcessDone
	default:
	}
	if err == nil && signal == syscall.SIGKILL {
		process.finish()
	}
	return err
}
func (process *fakeCodexProcess) Wait() error {
	process.waitCount.Add(1)
	<-process.done
	if process.sequence != nil {
		process.sequence("wait:return")
	}
	return process.waitErr
}

func (process *fakeCodexProcess) finish() {
	process.doneOnce.Do(func() { close(process.done) })
}

func requestMethods(requests []map[string]any) []string {
	result := make([]string, 0, len(requests))
	for _, request := range requests {
		if method, ok := request["method"].(string); ok {
			result = append(result, method)
		}
	}
	return result
}

func indexOf(values []string, want string) int {
	for index, value := range values {
		if value == want {
			return index
		}
	}
	return -1
}

func lastIndexOf(values []string, want string) int {
	for index := len(values) - 1; index >= 0; index-- {
		if values[index] == want {
			return index
		}
	}
	return -1
}

type testCodexCallbacks struct{}

func passCodexProjection() error { return nil }

func codexResultWasNotStarted(result CodexWakeResult) bool {
	return result.ProcessExited && !result.At.IsZero() && result.LaunchAt.IsZero() &&
		result.WakeAt.IsZero() && result.Diagnostic.IsZero() && result.RuntimeIDs.IsZero() &&
		result.WakeReceipt.IsZero() && !result.CompletionReceipt.IsZero() && !result.WakeDelivered &&
		strings.Contains(result.CompletionReceipt.String(), `"status":"launch_failed"`) &&
		strings.Contains(result.CompletionReceipt.String(), `"exit_method":"not_started"`)
}

func (testCodexCallbacks) callbacks() CodexWakeCallbacks {
	return CodexWakeCallbacks{
		RecordLaunch: func(context.Context, CodexLaunchEvidence) error { return nil },
		RecordWake:   func(context.Context, CodexWakeEvidence) error { return nil },
	}
}
