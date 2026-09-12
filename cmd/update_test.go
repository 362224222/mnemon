package cmd

import (
	"context"
	"io"
	"strings"
	"testing"
)

func TestNativeUpdateCommandRequiresNPMLauncher(t *testing.T) {
	t.Parallel()
	command := updateCommand()
	command.SetOut(io.Discard)
	command.SetErr(io.Discard)
	command.SetArgs(nil)
	err := command.ExecuteContext(context.Background())
	if err == nil {
		t.Fatal("expected the native update stub to fail closed")
	}
	msg := err.Error()
	for _, want := range []string{"not managed by npm", "upstream", "mnemon embed --all"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q must mention %q", msg, want)
		}
	}
	// Installing the npm package would replace this fork's binary with
	// upstream's, which ignores embed.yml and silently disables vector memory.
	// The message may warn against it, but must not offer it as the fix the way
	// the pre-fork stub did.
	if strings.Contains(msg, "migrate once with") {
		t.Errorf("error %q must not offer the upstream npm package as the migration path", msg)
	}
}
