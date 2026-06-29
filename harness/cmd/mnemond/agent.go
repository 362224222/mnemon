package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/driver"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation"
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
	runtimeName := fs.String("runtime", "noop", "managed runtime adapter (noop or codex-appserver)")
	dryRun := fs.Bool("dry-run", false, "print the managed wake query without starting a runtime turn")
	cooldown := fs.Duration("cooldown", 0, "minimum delay between managed wakes")
	addr := fs.String("addr", "", "local mnemond HTTP address used to render wake candidates")
	tokenFile := fs.String("token-file", "", "file containing the local mnemond bearer token")
	workspace := fs.String("workspace", "", "managed agent workspace")
	command := fs.String("command", "codex", "Codex CLI command for --runtime codex-appserver")
	codexHome := fs.String("codex-home", "", "CODEX_HOME for --runtime codex-appserver")
	ledgerPath := fs.String("ledger", "", "managed wake ledger path")
	lifecycle := fs.String("lifecycle", "remind", "render lifecycle used for wake candidate derivation")
	renderIntent := fs.String("render-intent", presentation.IntentTeamworkEvents, "render intent used for wake candidate derivation")
	surface := fs.String("surface", "hook", "render surface used for wake candidate derivation")
	turnTimeout := fs.Duration("turn-timeout", 5*time.Minute, "timeout for one managed agent turn")
	fs.Usage = func() {
		fmt.Fprintln(errw, "mnemond agent run is a local drive source: it renders wake candidates and sends only the [mnemon:wake] sentinel to the selected managed runtime.")
		fmt.Fprintln(errw)
		fmt.Fprintln(errw, "Usage of mnemond agent run:")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	localPrincipal := strings.TrimSpace(*principal)
	if localPrincipal == "" {
		return fmt.Errorf("mnemond agent run requires --principal")
	}
	if *dryRun {
		fmt.Fprintln(out, driver.ManagedWakeQuery)
		return nil
	}
	candidate := driver.ManagedWakeCandidate{
		Principal:      localPrincipal,
		DerivedEventID: "manual:agent-run",
		BodyDigest:     "sha256:manual",
		Reason:         "manual",
	}
	if strings.TrimSpace(*addr) != "" {
		token, err := readAgentTokenFile(*tokenFile)
		if err != nil {
			return err
		}
		resp, err := (driver.HTTPRenderClient{
			BaseURL:   strings.TrimSpace(*addr),
			Token:     token,
			Principal: contract.ActorID(localPrincipal),
		}).Render(ctx, presentation.Request{
			SchemaVersion: 1,
			Principal:     contract.ActorID(localPrincipal),
			RenderIntent:  strings.TrimSpace(*renderIntent),
			Lifecycle:     strings.TrimSpace(*lifecycle),
			Surface:       strings.TrimSpace(*surface),
		})
		if err != nil {
			return err
		}
		candidates := driver.ManagedWakeCandidatesFromRender(localPrincipal, resp)
		if len(candidates) == 0 {
			return writeManagedAgentRecord(out, driver.ManagedWakeRecord{
				Principal:        localPrincipal,
				Status:           "skipped",
				Reason:           "no_wake_candidate",
				StartedAt:        time.Now().UTC(),
				RenderAuditID:    resp.AuditID,
				RenderBodyDigest: resp.BodyDigest,
			})
		}
		candidate = candidates[0]
	}
	client, err := managedTurnClient(localPrincipal, *runtimeName, *command, *workspace, *codexHome, *turnTimeout)
	if err != nil {
		return err
	}
	ledger := managedWakeLedger(*ledgerPath, *workspace)
	managed := &driver.ManagedAgentDriver{
		Principal: localPrincipal,
		Client:    client,
		Ledger:    ledger,
		Cooldown:  *cooldown,
		Now:       func() time.Time { return time.Now().UTC() },
	}
	record, err := managed.Wake(ctx, candidate)
	if err != nil {
		return err
	}
	return writeManagedAgentRecord(out, record)
}

func managedTurnClient(principal, runtimeName, command, workspace, codexHome string, turnTimeout time.Duration) (driver.ManagedTurnClient, error) {
	switch strings.TrimSpace(runtimeName) {
	case "", "noop":
		return noopManagedTurnClient{}, nil
	case "codex-appserver":
		env := os.Environ()
		if strings.TrimSpace(codexHome) != "" {
			env = setAgentEnv(env, "CODEX_HOME", strings.TrimSpace(codexHome))
		}
		return driver.CodexAppServerTurnClient{
			Principal:   strings.TrimSpace(principal),
			Command:     strings.TrimSpace(command),
			Workspace:   strings.TrimSpace(workspace),
			Env:         env,
			TurnTimeout: turnTimeout,
		}, nil
	default:
		return nil, fmt.Errorf("runtime %q is not supported; use noop or codex-appserver", runtimeName)
	}
}

func managedWakeLedger(explicitPath, workspace string) driver.ManagedWakeLedger {
	path := strings.TrimSpace(explicitPath)
	if path == "" {
		root := strings.TrimSpace(workspace)
		if root == "" {
			root = "."
		}
		path = filepath.Join(root, ".mnemon", "harness", "local", "managed-agent", "wake-ledger.jsonl")
	}
	return driver.NewFileManagedWakeLedger(path)
}

func readAgentTokenFile(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

func setAgentEnv(env []string, key, value string) []string {
	prefix := key + "="
	out := env[:0]
	for _, item := range env {
		if !strings.HasPrefix(item, prefix) {
			out = append(out, item)
		}
	}
	return append(out, prefix+value)
}

func writeManagedAgentRecord(out io.Writer, record driver.ManagedWakeRecord) error {
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
