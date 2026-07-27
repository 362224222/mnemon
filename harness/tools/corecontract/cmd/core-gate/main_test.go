package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/tools/corecontract"
)

func TestRunPrintsOnlySuccessfulReportPath(t *testing.T) {
	var stdout, stderr bytes.Buffer
	var receivedRoot string
	var receivedMode corecontract.RunMode
	exitCode := run(context.Background(), []string{"run", "--mode", "merge"},
		&stdout, &stderr,
		func() (string, error) { return "/repo", nil },
		func(_ context.Context, root string, mode corecontract.RunMode) (string, error) {
			receivedRoot, receivedMode = root, mode
			return ".testdata/r5/core-gates/example/gate-report.json", nil
		})
	if exitCode != 0 || stderr.Len() != 0 ||
		stdout.String() != ".testdata/r5/core-gates/example/gate-report.json\n" ||
		receivedRoot != "/repo" || receivedMode != corecontract.RunModeMerge {
		t.Fatalf("run = exit %d stdout %q stderr %q root %q mode %q",
			exitCode, stdout.String(), stderr.String(), receivedRoot, receivedMode)
	}
}

func TestRunRejectsEveryNonCanonicalInvocation(t *testing.T) {
	for _, arguments := range [][]string{
		nil,
		{"run"},
		{"run", "--mode"},
		{"run", "merge"},
		{"run", "--mode", "other"},
		{"run", "--mode", "merge", "extra"},
		{"verify", "--mode", "merge"},
	} {
		var stdout, stderr bytes.Buffer
		exitCode := run(context.Background(), arguments, &stdout, &stderr,
			func() (string, error) {
				t.Fatal("invalid invocation resolved a repository")
				return "", nil
			},
			func(context.Context, string, corecontract.RunMode) (string, error) {
				t.Fatal("invalid invocation ran gates")
				return "", nil
			})
		if exitCode != 2 || stdout.Len() != 0 ||
			!strings.Contains(stderr.String(), "usage: core-gate") {
			t.Errorf("run(%v) = exit %d stdout %q stderr %q",
				arguments, exitCode, stdout.String(), stderr.String())
		}
	}
}

func TestRunDoesNotPrintAPathOnFailure(t *testing.T) {
	tests := []struct {
		name     string
		findRoot func() (string, error)
		runGates gateRunFunc
	}{
		{
			name: "root",
			findRoot: func() (string, error) {
				return "", errors.New("not a repository")
			},
			runGates: func(context.Context, string, corecontract.RunMode) (string, error) {
				t.Fatal("root failure ran gates")
				return "", nil
			},
		},
		{
			name:     "gate",
			findRoot: func() (string, error) { return "/repo", nil },
			runGates: func(context.Context, string, corecontract.RunMode) (string, error) {
				return "", errors.New("step failed")
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			exitCode := run(context.Background(), []string{"run", "--mode", "release"},
				&stdout, &stderr, test.findRoot, test.runGates)
			if exitCode != 1 || stdout.Len() != 0 ||
				!strings.Contains(stderr.String(), "core-gate:") {
				t.Fatalf("failed run = exit %d stdout %q stderr %q",
					exitCode, stdout.String(), stderr.String())
			}
		})
	}
}
