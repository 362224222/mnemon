package main

import (
	"strings"
	"testing"
)

func TestLoopValidateCommand(t *testing.T) {
	root := t.TempDir()
	restoreLoopFlags(t)
	loopRoot = root

	cmd, output := testCommand()
	if err := runLoopValidate(cmd, nil); err != nil {
		t.Fatalf("runLoopValidate returned error: %v", err)
	}
	for _, want := range []string{"standard event package agent_profile: OK", "standard event package assignment: OK"} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("expected %q in output:\n%s", want, output.String())
		}
	}
}

func restoreLoopFlags(t *testing.T) {
	t.Helper()
	oldRoot := loopRoot
	t.Cleanup(func() {
		loopRoot = oldRoot
	})
	loopRoot = "."
}
