// mnemond is the LOCAL governance daemon: the standalone-daemon packaging of the exact
// `mnemon-harness local run` boot path (P1 D13 — the mnemond name now belongs to the local
// trust domain; the remote hub binary builds as mnemon-hub). It is the LOCAL trust domain
// main: it imports internal/app and shares the boot face in app/localboot.go with `local run`,
// so flags, banner, T1 loopback floor, and serve behavior stay alias-identical. One daemon per
// project store (the store's single-writer flock enforces it).
//
// mnemond is a real daemon (P2 / PD8): `up` starts the serve loop as a detached background
// process (pidfile + log under .mnemon/harness/local/), `down` stops it, `status` reports it,
// `logs` shows its output. The bare/`serve` invocation is the FOREGROUND serve the daemon child
// runs — and the same foreground face `mnemon-harness local run` keeps for debugging.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/app"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "mnemond: %v\n", err)
		os.Exit(1)
	}
}

// run dispatches the daemon lifecycle verbs (up/down/status/logs) and otherwise FOREGROUND-serves
// (bare flags, or an explicit `serve` — what `up` re-execs as the detached child). Keeping bare flags
// = foreground serve preserves the `local run` alias contract and the boot/T1 smoke tests.
func run(ctx context.Context, args []string, out, errw io.Writer) error {
	if len(args) > 0 {
		switch args[0] {
		case "up":
			return daemonUp(args[1:], out, errw)
		case "down":
			return daemonDown(args[1:], out, errw)
		case "reload":
			return daemonReload(args[1:], out, errw)
		case "status":
			return daemonStatus(args[1:], out, errw)
		case "logs":
			return daemonLogs(args[1:], out, errw)
		case "agent":
			return runAgent(ctx, args[1:], out, errw)
		case "serve":
			args = args[1:]
		}
	}
	cfg, err := parseServe(args, errw)
	if err != nil {
		return err
	}
	return serveForeground(ctx, cfg, out)
}

// serveConfig is the resolved foreground-serve plan, shared by the foreground path and the `up`
// pre-flight (so `up` reports setup/T1 errors in the foreground before it detaches).
type serveConfig struct {
	projectRoot         string
	listenAddr          string
	boot                app.LocalBoot
	ignoreExternal      bool
	allowInsecureRemote bool
	syncInterval        time.Duration
}

// parseServe parses the `local run`-equivalent flag face and resolves the SAME boot chain
// (ResolveLocalBoot, endpoint-derived listen address, T1 loopback validation), returning the plan or
// the first boot/validation error — the seam both `serve` and `up` share.
func parseServe(args []string, errw io.Writer) (serveConfig, error) {
	fs := flag.NewFlagSet("mnemond", flag.ContinueOnError)
	fs.SetOutput(errw)
	root := fs.String("root", ".", "project root")
	addr := fs.String("addr", "127.0.0.1:8787", "listen address")
	syncInterval := fs.Duration("sync-interval", 0, "sync worker cadence (0 = default 30s)")
	allowNonLoopback := fs.Bool("allow-nonloopback", false, "explicitly allow listening on a non-loopback address (T1: loopback-only by default)")
	ignoreExternal := fs.Bool("ignore-external", false, "boot the embedded-only capability catalog, ignoring external packages under .mnemon/loops (each ignored package is named on stderr)")
	allowInsecureRemote := fs.Bool("allow-insecure-remote", false, "let the background sync worker use a plaintext http:// Remote Workspace endpoint with a non-loopback host (T2: fail-closed by default)")
	if err := fs.Parse(args); err != nil {
		return serveConfig{}, err
	}
	projectRoot := "."
	if *root != "" {
		projectRoot = filepath.Clean(*root)
	}
	boot, err := app.ResolveLocalBoot(projectRoot, "", "")
	if err != nil {
		return serveConfig{}, err
	}
	listenAddr := *addr
	addrChanged := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "addr" {
			addrChanged = true
		}
	})
	if !addrChanged {
		listenAddr = app.ListenAddrFromEndpoint(boot.Config.Endpoint, *addr)
	}
	if err := app.ValidateListenAddr(listenAddr, *allowNonLoopback); err != nil {
		return serveConfig{}, err
	}
	return serveConfig{
		projectRoot:         projectRoot,
		listenAddr:          listenAddr,
		boot:                boot,
		ignoreExternal:      *ignoreExternal,
		allowInsecureRemote: *allowInsecureRemote,
		syncInterval:        *syncInterval,
	}, nil
}

// serveForeground runs the governed HTTP server in the foreground until ctx cancels — the body of
// `mnemond serve` and the process the daemon child runs.
func serveForeground(ctx context.Context, cfg serveConfig, out io.Writer) error {
	fmt.Fprintln(out, "Local Mnemon: ready")
	fmt.Fprintln(out, "Remote Workspace: "+app.RemoteWorkspaceStatus(cfg.projectRoot))
	return app.RunLocalHTTPServerWithBindings(ctx, cfg.listenAddr, cfg.boot.StorePath, cfg.boot.Loaded, app.ServeOptions{
		Loops:               cfg.boot.Config.Loops,
		ProjectRoot:         cfg.projectRoot,
		IgnoreExternal:      cfg.ignoreExternal,
		AllowInsecureRemote: cfg.allowInsecureRemote,
		SyncInterval:        cfg.syncInterval,
	}, io.Discard)
}
