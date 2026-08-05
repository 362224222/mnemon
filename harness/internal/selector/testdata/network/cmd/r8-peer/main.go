// r8-peer is a test-only transport adapter inside the removable R8 selector
// testdata. It deliberately lives outside every R7 production package and
// command.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const usage = `r8-peer exercises the test-only R8 selector transport.

Usage:
  r8-peer keygen --state-dir DIR
  r8-peer window
  r8-peer install-config --state-dir DIR
  r8-peer init --state-dir DIR --config FILE --id ID --preference A|B
  r8-peer serve --state-dir DIR --config FILE --id ID --listen ADDRESS --control SOCKET
  r8-peer control --socket SOCKET status|round
  r8-peer probe --state-dir DIR --config FILE --id ID --target ID --mode no-vote|identity-mismatch
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "r8-peer: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		_, err := fmt.Fprint(os.Stdout, usage)
		return err
	}
	switch args[0] {
	case "keygen":
		return runKeygen(args[1:])
	case "window":
		return runWindow(args[1:])
	case "install-config":
		return runInstallConfig(args[1:])
	case "init":
		return runInit(ctx, args[1:])
	case "serve":
		return runServe(ctx, args[1:])
	case "control":
		return runControl(ctx, args[1:])
	case "probe":
		return runProbe(ctx, args[1:])
	case "help", "-h", "--help":
		_, err := fmt.Fprint(os.Stdout, usage)
		return err
	default:
		return fmt.Errorf("unsupported command %q", args[0])
	}
}

type commonOptions struct {
	stateDir   string
	config     string
	self       string
	listen     string
	control    string
	preference string
}

func parseCommon(name string, args []string, configure func(*flag.FlagSet, *commonOptions)) (
	commonOptions, error,
) {
	options := commonOptions{}
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	if configure != nil {
		configure(flags, &options)
	}
	if err := flags.Parse(args); err != nil {
		return commonOptions{}, err
	}
	if flags.NArg() != 0 {
		return commonOptions{}, fmt.Errorf("%s accepts no positional arguments", name)
	}
	return options, nil
}

func requireValues(values ...string) error {
	for _, value := range values {
		if value == "" {
			return errors.New("all command options are required")
		}
	}
	return nil
}

func runWindow(args []string) error {
	if len(args) != 0 {
		return errors.New("window accepts no arguments")
	}
	now := time.Now().Round(0).UTC()
	return writeJSON(os.Stdout, windowOutput{
		CreatedAt: now.Add(-time.Minute).Format(time.RFC3339Nano),
		ExpiresAt: now.Add(30 * time.Minute).Format(time.RFC3339Nano),
	})
}

type windowOutput struct {
	CreatedAt string `json:"created_at"`
	ExpiresAt string `json:"expires_at"`
}
