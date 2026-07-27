package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"

	"github.com/mnemon-dev/mnemon/harness/tools/corecontract"
)

const usage = "usage: core-gate run --mode merge|release\n"

type gateRunFunc func(context.Context, string, corecontract.RunMode) (string, error)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if exitCode := run(ctx, os.Args[1:], os.Stdout, os.Stderr,
		repositoryRoot, corecontract.RunGates); exitCode != 0 {
		os.Exit(exitCode)
	}
}

func run(ctx context.Context, arguments []string, stdout, stderr io.Writer,
	findRoot func() (string, error), runGates gateRunFunc,
) int {
	if len(arguments) != 3 || arguments[0] != "run" || arguments[1] != "--mode" {
		_, _ = io.WriteString(stderr, usage)
		return 2
	}
	mode := corecontract.RunMode(arguments[2])
	if mode != corecontract.RunModeMerge && mode != corecontract.RunModeRelease {
		fmt.Fprintf(stderr, "core-gate: invalid mode %q\n", arguments[2])
		_, _ = io.WriteString(stderr, usage)
		return 2
	}
	root, err := findRoot()
	if err != nil {
		fmt.Fprintf(stderr, "core-gate: %v\n", err)
		return 1
	}
	reportPath, err := runGates(ctx, root, mode)
	if err != nil {
		fmt.Fprintf(stderr, "core-gate: %v\n", err)
		return 1
	}
	if _, err := fmt.Fprintln(stdout, reportPath); err != nil {
		return 1
	}
	return 0
}

func repositoryRoot() (string, error) {
	command := exec.Command("git", "rev-parse", "--show-toplevel")
	output, err := command.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("resolve repository root: %w: %s",
			err, strings.TrimSpace(string(output)))
	}
	root := strings.TrimSpace(string(output))
	if root == "" {
		return "", fmt.Errorf("resolve repository root: empty Git result")
	}
	return root, nil
}
