package cmd

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRootComposesMemoryAndAgency(t *testing.T) {
	root := productRoot(new(int))
	for _, name := range []string{"remember", "recall", "setup", "agency"} {
		child, _, err := root.Find([]string{name})
		if err != nil || child == root {
			t.Fatalf("root command %q is not registered", name)
		}
	}
}

func TestExecuteRoutesAgencyWithoutChangingItsExitCode(t *testing.T) {
	var stdout, stderr bytes.Buffer
	exitCode := Execute(context.Background(), []string{"agency", "version"},
		strings.NewReader(""), &stdout, &stderr)
	if exitCode != 0 || stdout.String() != "mnemon agency version dev\n" || stderr.Len() != 0 {
		t.Fatalf("agency version: exit=%d stdout=%q stderr=%q",
			exitCode, stdout.String(), stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	exitCode = Execute(context.Background(), []string{"agency", "unknown"},
		strings.NewReader(""), &stdout, &stderr)
	if exitCode != 2 || stdout.Len() != 0 ||
		stderr.String() != "mnemon agency: unknown command \"unknown\"\n" {
		t.Fatalf("agency rejection: exit=%d stdout=%q stderr=%q",
			exitCode, stdout.String(), stderr.String())
	}
}
