package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

var version = "dev"

const helpText = `mnemon-harness is the project-local client for mnemond-managed Teamwork.

Usage:
  mnemon-harness
  mnemon-harness --help
  mnemon-harness --version

This clean-cut R5 build currently exposes lifecycle probes only.
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "mnemon-harness: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	_ = ctx
	_ = stderr

	if len(args) == 0 {
		_, err := io.WriteString(stdout, helpText)
		return err
	}
	if len(args) != 1 {
		return fmt.Errorf("unsupported command %q", args[0])
	}

	switch args[0] {
	case "-h", "--help", "help":
		_, err := io.WriteString(stdout, helpText)
		return err
	case "--version", "version":
		_, err := fmt.Fprintf(stdout, "mnemon-harness version %s\n", version)
		return err
	default:
		return fmt.Errorf("unsupported command %q", args[0])
	}
}
