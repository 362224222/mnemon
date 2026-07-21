package agent

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestClaudeWakeAdapterRunsOneDurablyOrderedManagedTurn(t *testing.T) {
	fixture := newClaudeAdapterFixture(t)
	result := fixture.runManagedTurn(t)
	fixture.assertManagedTurnEvidence(t, result)
	fixture.assertManagedTurnProcess(t)
}

func TestClaudeWakeAdapterFailsClosedBeforePromptAndRedactsCauses(t *testing.T) {
	fixture := newClaudeAdapterFixture(t)
	fixture.verifyErr = errors.New("private projection detail")
	result, err := fixture.adapter.Run(t.Context(), fixture.request())
	if !errors.Is(err, ErrClaudeWakeAdapter) || strings.Contains(err.Error(), "private") ||
		!result.ProcessExited || result.CompletionReceipt.IsZero() ||
		!strings.Contains(result.CompletionReceipt.String(), `"status":"launch_failed"`) ||
		fixture.starter.started.Load() {
		t.Fatalf("Run() = (%#v, %v), started=%t", result, err, fixture.starter.started.Load())
	}
}

func TestClaudeWakeAdapterRejectsInvalidConstructionAndRequest(t *testing.T) {
	base := CodexWakeAdapterOptions{Executable: "/usr/local/bin/claude", Workspace: t.TempDir(),
		Environment: []string{"PATH=/usr/bin:/bin"}, Starter: &claudeTestStarter{},
		Identity: fixedCodexIdentity{}, Clock: newFakeCodexClock(),
		Terminator: &fakeCodexTerminator{}, VerifyProjection: passCodexProjection}
	for _, mutate := range []func(*CodexWakeAdapterOptions){
		func(value *CodexWakeAdapterOptions) { value.Executable = "claude" },
		func(value *CodexWakeAdapterOptions) { value.Workspace = "." },
		func(value *CodexWakeAdapterOptions) { value.VerifyProjection = nil },
		func(value *CodexWakeAdapterOptions) {
			value.Environment = append(value.Environment, RunAttachmentEnvironment+"=/stale")
		},
	} {
		options := base
		mutate(&options)
		adapter, err := NewClaudeWakeAdapter(options)
		if adapter != nil || !errors.Is(err, ErrClaudeWakeAdapter) {
			t.Fatalf("NewClaudeWakeAdapter() = (%#v, %v)", adapter, err)
		}
	}
	adapter, err := NewClaudeWakeAdapter(base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.Run(nil, CodexWakeRequest{}); !errors.Is(err, ErrClaudeWakeAdapter) {
		t.Fatalf("Run(nil) error = %v", err)
	}
}

func containsClaudePromptArgument(arguments []string) bool {
	for _, argument := range arguments {
		if strings.Contains(argument, "mnemon-harness") {
			return true
		}
	}
	return false
}

type claudeAdapterFixture struct {
	t                  *testing.T
	adapter            *ClaudeWakeAdapter
	starter            *claudeTestStarter
	workspace          string
	attachment         string
	verifyErr          error
	launchBeforePrompt atomic.Bool
	mu                 sync.Mutex
	launches           []CodexLaunchEvidence
	wakes              []CodexWakeEvidence
}

func newClaudeAdapterFixture(t *testing.T) *claudeAdapterFixture {
	t.Helper()
	fixture := &claudeAdapterFixture{t: t, workspace: t.TempDir()}
	fixture.attachment = RunAttachmentEnvironment + "=" +
		filepath.Join(fixture.workspace, "run.attach")
	fixture.starter = &claudeTestStarter{lines: claudeProofLines(fixture.workspace,
		claudeProofMutation{}), launchObserved: &fixture.launchBeforePrompt}
	terminator := &claudeTestTerminator{starter: fixture.starter}
	adapter, err := NewClaudeWakeAdapter(CodexWakeAdapterOptions{
		Executable: "/usr/local/bin/claude", Workspace: fixture.workspace,
		Environment: []string{"PATH=/usr/bin:/bin"}, Starter: fixture.starter,
		Identity: fixedCodexIdentity{}, Clock: newFakeCodexClock(), Terminator: terminator,
		VerifyProjection: func(context.Context) error { return fixture.verifyErr },
		ExitGrace:        time.Second, SignalGrace: time.Millisecond, PipeDrainGrace: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.adapter = adapter
	return fixture
}

func (fixture *claudeAdapterFixture) request() CodexWakeRequest {
	return CodexWakeRequest{RunAttachmentEnvironment: fixture.attachment,
		Callbacks: CodexWakeCallbacks{
			RecordLaunch: func(_ context.Context, evidence CodexLaunchEvidence) error {
				fixture.mu.Lock()
				fixture.launches = append(fixture.launches, evidence)
				fixture.mu.Unlock()
				fixture.launchBeforePrompt.Store(true)
				return nil
			},
			RecordWake: func(_ context.Context, evidence CodexWakeEvidence) error {
				fixture.mu.Lock()
				fixture.wakes = append(fixture.wakes, evidence)
				fixture.mu.Unlock()
				return nil
			},
		}}
}

func (fixture *claudeAdapterFixture) runManagedTurn(t *testing.T) CodexWakeResult {
	t.Helper()
	result, err := fixture.adapter.Run(t.Context(), fixture.request())
	if err != nil {
		t.Fatalf("Run() = (%#v, %v)", result, err)
	}
	return result
}

func (fixture *claudeAdapterFixture) assertManagedTurnEvidence(t *testing.T,
	result CodexWakeResult,
) {
	t.Helper()
	if !result.WakeDelivered || !result.ProcessExited || result.Diagnostic.IsZero() ||
		result.RuntimeIDs.IsZero() || result.WakeReceipt.IsZero() ||
		result.CompletionReceipt.IsZero() || len(fixture.launches) != 1 ||
		len(fixture.wakes) != 1 {
		t.Fatalf("Run() result/evidence = %#v / %d / %d", result,
			len(fixture.launches), len(fixture.wakes))
	}
	if !strings.Contains(result.Diagnostic.String(), `"adapter":"claude-cli"`) ||
		!strings.Contains(result.CompletionReceipt.String(), `"status":"completed"`) ||
		!strings.Contains(result.CompletionReceipt.String(), `"exit_method":"wait_without_signal"`) ||
		!fixture.launchBeforePrompt.Load() {
		t.Fatalf("diagnostic/completion/order = %s / %s / %t", result.Diagnostic.String(),
			result.CompletionReceipt.String(), fixture.launchBeforePrompt.Load())
	}
}

func (fixture *claudeAdapterFixture) assertManagedTurnProcess(t *testing.T) {
	t.Helper()
	if got := fixture.starter.prompt(); got != claudeManagedPrompt {
		t.Fatalf("stdin prompt = %q", got)
	}
	spec := fixture.starter.spec()
	if spec.Executable != "/usr/local/bin/claude" || spec.Directory != fixture.workspace ||
		len(spec.Environment) != 2 || spec.Environment[1] != fixture.attachment ||
		containsClaudePromptArgument(spec.Arguments) ||
		strings.Join(spec.Arguments, "\x00") != strings.Join(claudeRuntimeArguments(), "\x00") {
		t.Fatalf("process spec = %#v", spec)
	}
	if fixture.starter.process.waitCount.Load() != 1 {
		t.Fatalf("Wait count = %d", fixture.starter.process.waitCount.Load())
	}
}

type claudeTestStarter struct {
	mu             sync.Mutex
	startedSpec    CodexProcessStartSpec
	process        *claudePipeProcess
	lines          [][]byte
	launchObserved *atomic.Bool
	observedPrompt string
	started        atomic.Bool
}

func (starter *claudeTestStarter) Start(spec CodexProcessStartSpec) (CodexProcess, error) {
	process := newClaudePipeProcess(91)
	starter.mu.Lock()
	starter.startedSpec, starter.process = spec, process
	starter.mu.Unlock()
	starter.started.Store(true)
	go starter.serve(process)
	return process, nil
}

func (starter *claudeTestStarter) serve(process *claudePipeProcess) {
	prompt, _ := io.ReadAll(process.inputReader)
	starter.mu.Lock()
	starter.observedPrompt = string(prompt)
	starter.mu.Unlock()
	if starter.launchObserved == nil || !starter.launchObserved.Load() {
		process.finish()
		return
	}
	for _, line := range starter.lines {
		_, _ = process.outputWriter.Write(append(append([]byte(nil), line...), '\n'))
	}
	_ = process.outputWriter.Close()
	_ = process.errorWriter.Close()
	process.finish()
}

func (starter *claudeTestStarter) prompt() string {
	starter.mu.Lock()
	defer starter.mu.Unlock()
	return starter.observedPrompt
}

func (starter *claudeTestStarter) spec() CodexProcessStartSpec {
	starter.mu.Lock()
	defer starter.mu.Unlock()
	return starter.startedSpec
}

type claudePipeProcess struct {
	pid          int
	inputReader  *io.PipeReader
	inputWriter  *io.PipeWriter
	outputReader *io.PipeReader
	outputWriter *io.PipeWriter
	errorReader  *io.PipeReader
	errorWriter  *io.PipeWriter
	done         chan struct{}
	doneOnce     sync.Once
	waitCount    atomic.Int32
}

func newClaudePipeProcess(pid int) *claudePipeProcess {
	inputReader, inputWriter := io.Pipe()
	outputReader, outputWriter := io.Pipe()
	errorReader, errorWriter := io.Pipe()
	return &claudePipeProcess{pid: pid, inputReader: inputReader, inputWriter: inputWriter,
		outputReader: outputReader, outputWriter: outputWriter,
		errorReader: errorReader, errorWriter: errorWriter, done: make(chan struct{})}
}

func (process *claudePipeProcess) PID() int              { return process.pid }
func (process *claudePipeProcess) Stdin() io.WriteCloser { return process.inputWriter }
func (process *claudePipeProcess) Stdout() io.ReadCloser { return process.outputReader }
func (process *claudePipeProcess) Stderr() io.ReadCloser { return process.errorReader }
func (process *claudePipeProcess) Signal(signal syscall.Signal) error {
	select {
	case <-process.done:
		return os.ErrProcessDone
	default:
	}
	if signal == syscall.SIGKILL {
		process.finish()
	}
	return nil
}
func (process *claudePipeProcess) Wait() error {
	process.waitCount.Add(1)
	<-process.done
	return nil
}
func (process *claudePipeProcess) finish() {
	process.doneOnce.Do(func() { close(process.done) })
}

type claudeTestTerminator struct{ starter *claudeTestStarter }

func (*claudeTestTerminator) Observe(context.Context, model.JSON) error { return nil }
func (terminator *claudeTestTerminator) Terminate(context.Context,
	model.JSON,
) ([]string, error) {
	terminator.starter.mu.Lock()
	process := terminator.starter.process
	terminator.starter.mu.Unlock()
	select {
	case <-process.done:
		return nil, nil
	default:
		process.finish()
		return []string{"SIGKILL"}, nil
	}
}
