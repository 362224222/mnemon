package memory

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestHookPromptSubmitEmitsReminder(t *testing.T) {
	var out bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&out)

	if err := runHookPromptSubmit(cmd, nil); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(out.String(), "Evaluate: recall needed?") {
		t.Fatalf("unexpected output: %q", out.String())
	}
}

func TestHookSessionStopEmitsContinueFalse(t *testing.T) {
	var out, in bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&out)
	cmd.SetIn(&in) // empty payload

	if err := runHookSessionStop(cmd, nil); err != nil {
		t.Fatalf("run: %v", err)
	}
	var parsed struct {
		Continue string `json:"continue"`
		Reason   string `json:"reason"`
	}
	if err := json.Unmarshal(out.Bytes(), &parsed); err != nil {
		t.Fatalf("output not JSON: %v; raw=%q", err, out.String())
	}
	if parsed.Continue != "false" {
		t.Fatalf("continue = %q, want false", parsed.Continue)
	}
	if parsed.Reason == "" {
		t.Fatal("empty reason")
	}
}

func TestHookSessionStopSilentWhenAlreadyActive(t *testing.T) {
	var out bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&out)
	cmd.SetIn(strings.NewReader(`{"stop_hook_active": true}`))

	if err := runHookSessionStop(cmd, nil); err != nil {
		t.Fatalf("run: %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("expected no output when stop_hook_active, got %q", out.String())
	}
}

func TestHookSessionStartGracefulWithoutStore(t *testing.T) {
	old := dataDir
	dataDir = t.TempDir()
	t.Cleanup(func() { dataDir = old })

	var out bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&out)

	if err := runHookSessionStart(cmd, nil); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(out.String(), "Memory active") {
		t.Fatalf("expected Memory active line, got %q", out.String())
	}
}
