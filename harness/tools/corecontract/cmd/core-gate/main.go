package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/mnemon-dev/mnemon/harness/tools/corecontract"
)

const usage = "usage: core-gate --root <repository-root>\n"

type runGatesFunc func(context.Context, string) (string, error)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if code := run(ctx, os.Args[1:], os.Stdout, os.Stderr, corecontract.RunGates); code != 0 {
		os.Exit(code)
	}
}

func run(ctx context.Context, arguments []string, stdout, stderr io.Writer, runGates runGatesFunc) int {
	if len(arguments) != 2 || arguments[0] != "--root" || arguments[1] == "" {
		_, _ = io.WriteString(stderr, usage)
		return 2
	}
	path, err := runGates(ctx, arguments[1])
	if err != nil {
		fmt.Fprintf(stderr, "core-gate: %v\n", err)
		return 1
	}
	if _, err := fmt.Fprintln(stdout, path); err != nil {
		return 1
	}
	return 0
}
