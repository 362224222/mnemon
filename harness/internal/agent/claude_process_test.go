package agent

import (
	"io"
	"strings"
	"testing"
	"time"
)

func TestClaudeStreamSessionSendsOnePromptAndBoundsStderr(t *testing.T) {
	process := newClaudePipeProcess(91)
	adapter := &ClaudeWakeAdapter{managedRuntimeCore: &managedRuntimeCore{
		clock: newFakeCodexClock(), exitGrace: time.Second, pipeDrainGrace: time.Second,
	}}
	session, err := newClaudeStreamSession(adapter, process)
	if err != nil {
		t.Fatal(err)
	}
	promptRead := make(chan struct {
		value []byte
		err   error
	}, 1)
	go func() {
		value, err := io.ReadAll(process.inputReader)
		promptRead <- struct {
			value []byte
			err   error
		}{value: value, err: err}
	}()
	if err := session.sendPrompt(); err != nil {
		t.Fatal(err)
	}
	prompt := <-promptRead
	if prompt.err != nil || string(prompt.value) != claudeManagedPrompt {
		t.Fatalf("prompt = %q, error = %v", prompt.value, prompt.err)
	}
	_ = process.outputWriter.Close()
	_, _ = io.WriteString(process.errorWriter, strings.Repeat("x", claudeStderrMax+1))
	_ = process.errorWriter.Close()
	for {
		_, nextErr := session.next(t.Context())
		if nextErr == io.EOF {
			break
		}
		if nextErr != nil {
			t.Fatal(nextErr)
		}
	}
	if err := session.finishOutput(t.Context()); err == nil || session.stderrWasClean() {
		t.Fatalf("finishOutput() = %v, stderr clean = %t", err, session.stderrWasClean())
	}
	if err := session.closeReaders(); err == nil {
		t.Fatal("closeReaders accepted overflowing stderr")
	}
}
