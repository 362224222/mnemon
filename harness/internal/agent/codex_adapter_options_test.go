package agent

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCodexWakeAdapterRejectsInvalidClockEvidence(t *testing.T) {
	starter := newFakeCodexStarter(fakeCodexScenario{}, nil)
	adapter, err := NewCodexWakeAdapter(CodexWakeAdapterOptions{Executable: "/usr/bin/codex",
		Workspace: t.TempDir(), Environment: []string{"PATH=/usr/bin"}, Starter: starter,
		Identity: fixedCodexIdentity{}, Clock: invalidCodexClock{}, Terminator: &fakeCodexTerminator{},
		VerifyProjection: passCodexProjection,
		InterruptGrace:   time.Millisecond, ExitGrace: time.Millisecond, SignalGrace: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Run(context.Background(), CodexWakeRequest{
		RunAttachmentEnvironment: RunAttachmentEnvironment + "=/tmp/run.attach",
		Callbacks:                testCodexCallbacks{}.callbacks()})
	if !errors.Is(err, ErrCodexWakeAdapter) || !result.Diagnostic.IsZero() ||
		starter.process.waitCount.Load() != 1 {
		t.Fatalf("Run() = (%#v, %v), Wait=%d", result, err, starter.process.waitCount.Load())
	}
}

func TestCodexWakeAdapterDefaultsPipeDrainGrace(t *testing.T) {
	adapter, err := NewCodexWakeAdapter(CodexWakeAdapterOptions{
		Executable: "/usr/bin/codex", Workspace: t.TempDir(), Environment: []string{"PATH=/usr/bin"},
		Starter: &fakeCodexStarter{}, Identity: fixedCodexIdentity{}, Clock: newFakeCodexClock(),
		Terminator: &fakeCodexTerminator{}, VerifyProjection: passCodexProjection,
	})
	if err != nil {
		t.Fatal(err)
	}
	if adapter.pipeDrainGrace != codexPipeDrainMax {
		t.Fatalf("default pipe drain grace = %s, want %s", adapter.pipeDrainGrace, codexPipeDrainMax)
	}
}

func TestCodexWakeAdapterRejectsInvalidPipeDrainGrace(t *testing.T) {
	adapter, err := NewCodexWakeAdapter(CodexWakeAdapterOptions{
		Executable: "/usr/bin/codex", Workspace: t.TempDir(), Environment: []string{"PATH=/usr/bin"},
		Starter: &fakeCodexStarter{}, Identity: fixedCodexIdentity{}, Clock: newFakeCodexClock(),
		Terminator: &fakeCodexTerminator{}, VerifyProjection: passCodexProjection,
		PipeDrainGrace: 31 * time.Second,
	})
	if adapter != nil || !errors.Is(err, ErrCodexWakeAdapter) {
		t.Fatalf("NewCodexWakeAdapter() = (%#v, %v)", adapter, err)
	}
}
