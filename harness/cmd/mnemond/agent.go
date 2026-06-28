package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/driver"
)

func runAgent(ctx context.Context, args []string, out, errw io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("agent requires a subcommand")
	}
	switch args[0] {
	case "run":
		return runAgentRun(ctx, args[1:], out, errw)
	default:
		return fmt.Errorf("unknown agent subcommand %q", args[0])
	}
}

func runAgentRun(ctx context.Context, args []string, out, errw io.Writer) error {
	fs := flag.NewFlagSet("mnemond agent run", flag.ContinueOnError)
	fs.SetOutput(errw)
	principal := fs.String("principal", "", "local managed agent principal")
	runtimeName := fs.String("runtime", "noop", "managed runtime adapter (noop)")
	dryRun := fs.Bool("dry-run", false, "print the managed wake query without starting a runtime turn")
	cooldown := fs.Duration("cooldown", 0, "minimum delay between managed wakes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*principal) == "" {
		return fmt.Errorf("mnemond agent run requires --principal")
	}
	if *dryRun {
		fmt.Fprintln(out, driver.ManagedWakeQuery)
		return nil
	}
	if strings.TrimSpace(*runtimeName) != "noop" {
		return fmt.Errorf("runtime %q is not supported yet; use --runtime noop for contract validation", *runtimeName)
	}
	managed := &driver.ManagedAgentDriver{
		Principal: strings.TrimSpace(*principal),
		Client:    noopManagedTurnClient{},
		Ledger:    driver.NewMemoryManagedWakeLedger(),
		Cooldown:  *cooldown,
		Now:       func() time.Time { return time.Now().UTC() },
	}
	record, err := managed.Wake(ctx, driver.ManagedWakeCandidate{
		Principal:      strings.TrimSpace(*principal),
		DerivedEventID: "manual:agent-run",
		BodyDigest:     "sha256:manual",
		Reason:         "manual",
	})
	if err != nil {
		return err
	}
	raw, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintln(out, string(raw))
	return nil
}

type noopManagedTurnClient struct{}

func (noopManagedTurnClient) StartTurn(_ context.Context, query string) (driver.ManagedTurnResult, error) {
	if query != driver.ManagedWakeQuery {
		return driver.ManagedTurnResult{}, fmt.Errorf("unexpected managed wake query %q", query)
	}
	return driver.ManagedTurnResult{TurnID: "noop-turn", Status: "completed"}, nil
}
