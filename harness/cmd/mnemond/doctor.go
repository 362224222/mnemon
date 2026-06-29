package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/app"
)

func daemonDoctor(args []string, out, errw io.Writer) error {
	fs := flag.NewFlagSet("mnemond doctor", flag.ContinueOnError)
	fs.SetOutput(errw)
	rootFlag := fs.String("root", ".", "project root")
	fs.Usage = func() {
		fmt.Fprintln(errw, "mnemond doctor checks the local event node setup, boot chain, and background process state.")
		fmt.Fprintln(errw)
		fmt.Fprintln(errw, "Usage of mnemond doctor:")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	root := strings.TrimSpace(*rootFlag)
	if root == "" {
		root = "."
	}
	root = filepath.Clean(root)
	fmt.Fprintln(out, "mnemond doctor")
	writeLocalConfigDoctor(out, root)
	writeLocalBootDoctor(out, root)
	writeBackgroundDoctor(out, root)
	fmt.Fprintf(out, "- Remote Workspace: %s\n", app.RemoteWorkspaceStatus(root))
	return nil
}

func writeLocalConfigDoctor(out io.Writer, root string) {
	cfg, err := app.ReadLocalConfig(root)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintln(out, "- Local event node config: missing")
			fmt.Fprintln(out, "- Setup remediation: mnemon-harness setup --host codex")
			return
		}
		fmt.Fprintf(out, "- Local event node config: invalid (%v)\n", err)
		return
	}
	fmt.Fprintln(out, "- Local event node config: configured")
	fmt.Fprintf(out, "- Endpoint: %s\n", cfg.Endpoint)
	fmt.Fprintf(out, "- Principal: %s\n", cfg.Principal)
}

func writeLocalBootDoctor(out io.Writer, root string) {
	boot, err := app.ResolveLocalBoot(root, "", "")
	if err != nil {
		if errors.Is(err, app.ErrLocalNotSetup) {
			fmt.Fprintln(out, "- Boot chain: not ready (setup required)")
			return
		}
		fmt.Fprintf(out, "- Boot chain: invalid (%v)\n", err)
		return
	}
	fmt.Fprintf(out, "- Boot chain: ready (bindings=%d)\n", len(boot.Loaded.Bindings))
	fmt.Fprintf(out, "- Store: %s\n", boot.StorePath)
}

func writeBackgroundDoctor(out io.Writer, root string) {
	_, pidPath, _ := daemonPaths(root)
	pid, alive := readLivePid(pidPath)
	switch {
	case alive:
		fmt.Fprintf(out, "- Background daemon: running (pid %d)\n", pid)
	case pid > 0:
		fmt.Fprintf(out, "- Background daemon: stopped (stale pidfile pid %d)\n", pid)
	default:
		fmt.Fprintln(out, "- Background daemon: stopped")
	}
}
