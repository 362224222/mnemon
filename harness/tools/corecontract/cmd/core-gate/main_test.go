package main

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestCLIHasOneRootOnlyInvocationAndPrintsOnlyReportPath(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"--root", ".."}, &stdout, &stderr,
		func(_ context.Context, root string) (string, error) {
			if root != ".." {
				t.Fatalf("root = %q", root)
			}
			return ".testdata/r7/core-gates/run/gate-report.json", nil
		})
	if code != 0 || stderr.Len() != 0 ||
		stdout.String() != ".testdata/r7/core-gates/run/gate-report.json\n" {
		t.Fatalf("run = code %d stdout %q stderr %q", code, stdout.String(), stderr.String())
	}
	for _, arguments := range [][]string{nil, {"run"}, {"--root"}, {"--root", "..", "merge"}} {
		stdout.Reset()
		stderr.Reset()
		code = run(context.Background(), arguments, &stdout, &stderr,
			func(context.Context, string) (string, error) {
				t.Fatal("invalid invocation ran gates")
				return "", nil
			})
		if code != 2 || stdout.Len() != 0 {
			t.Fatalf("invalid %v = code %d stdout %q", arguments, code, stdout.String())
		}
	}
}

func TestCLIFailureDoesNotPrintReportPath(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"--root", ".."}, &stdout, &stderr,
		func(context.Context, string) (string, error) { return "", errors.New("failed") })
	if code != 1 || stdout.Len() != 0 || stderr.Len() == 0 {
		t.Fatalf("failure = code %d stdout %q stderr %q", code, stdout.String(), stderr.String())
	}
}
