package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/app"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemonhub/exchange"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
	"github.com/spf13/cobra"
)

var (
	statusRoot        string
	statusProjectRoot string
	statusPrincipal   string
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show Agent Integration, Local Mnemon, and Remote Workspace status",
	RunE:  runProductStatus,
}

func init() {
	statusCmd.Flags().StringVar(&statusRoot, "root", ".", "repository root containing harness declarations")
	statusCmd.Flags().StringVar(&statusProjectRoot, "project-root", "", "project root for Agent Integration artifacts")
	statusCmd.Flags().StringVar(&statusPrincipal, "principal", "", "Agent Integration principal")
	statusCmd.GroupID = groupSpine
	rootCmd.AddCommand(statusCmd)
}

func runProductStatus(cmd *cobra.Command, args []string) error {
	root := filepath.Clean(statusRoot)
	projectRoot := statusProjectRoot
	if projectRoot == "" {
		projectRoot = root
	}
	projectRoot = filepath.Clean(projectRoot)

	if cfg, err := app.ReadLocalConfig(projectRoot); err == nil {
		principal := statusPrincipal
		if principal == "" {
			principal = cfg.Principal
		}
		if st, ok := localServiceStatus(projectRoot, cfg, principal); ok {
			printProductStatus(cmd, true, true, app.RemoteWorkspaceStatus(projectRoot), st.SyncPending, st.SyncSynced, st.SyncConflicts)
			return nil
		}
	}

	lines, err := app.New(root).SetupStatus(projectRoot, statusPrincipal)
	if err != nil {
		return err
	}
	remote := app.RemoteWorkspaceStatus(projectRoot)
	for _, l := range lines {
		if strings.HasPrefix(l, "Remote Workspace:") {
			continue
		}
		fmt.Fprintln(cmd.OutOrStdout(), l)
	}
	fmt.Fprintln(cmd.OutOrStdout(), "Remote Workspace: "+remote)
	counts := syncCounts(projectRoot)
	fmt.Fprintf(cmd.OutOrStdout(), "Sync: %d pending, %d synced, %d conflicts\n", counts.Pending, counts.Synced, counts.Conflicts)
	return nil
}

func localServiceStatus(projectRoot string, cfg app.LocalConfig, principal string) (contract.ChannelStatus, bool) {
	if strings.TrimSpace(cfg.Endpoint) == "" || strings.TrimSpace(principal) == "" {
		return contract.ChannelStatus{}, false
	}
	bindingFile := cfg.BindingFile
	if bindingFile == "" {
		bindingFile = access.DefaultBindingFile
	}
	loaded, err := access.LoadBindingFile(projectRoot, app.ResolveProjectPath(projectRoot, bindingFile))
	if err != nil {
		return contract.ChannelStatus{}, false
	}
	client := access.NewClient(cfg.Endpoint, contract.ActorID(principal))
	if tok := tokenForPrincipal(loaded.Tokens, contract.ActorID(principal)); tok != "" {
		client = access.NewClientWithToken(cfg.Endpoint, tok)
	}
	st, err := client.Status(contract.ActorID(principal))
	if err != nil {
		return contract.ChannelStatus{}, false
	}
	return st, true
}

func printProductStatus(cmd *cobra.Command, installed, ready bool, remote string, pending, synced, conflicts int) {
	if installed {
		fmt.Fprintln(cmd.OutOrStdout(), "Agent Integration: installed")
	} else {
		fmt.Fprintln(cmd.OutOrStdout(), "Agent Integration: not installed")
	}
	if ready {
		fmt.Fprintln(cmd.OutOrStdout(), "Local Mnemon: ready")
	} else {
		fmt.Fprintln(cmd.OutOrStdout(), "Local Mnemon: not configured")
	}
	fmt.Fprintln(cmd.OutOrStdout(), "Remote Workspace: "+remote)
	fmt.Fprintf(cmd.OutOrStdout(), "Sync: %d pending, %d synced, %d conflicts\n", pending, synced, conflicts)
}

func tokenForPrincipal(tokens map[string]contract.ActorID, principal contract.ActorID) string {
	for tok, owner := range tokens {
		if owner == principal {
			return tok
		}
	}
	return ""
}

func syncCounts(projectRoot string) exchange.LocalSyncCounts {
	storePath := filepath.Join(projectRoot, runtime.DefaultStorePath)
	if _, err := os.Stat(storePath); err != nil {
		return exchange.LocalSyncCounts{}
	}
	counts, err := exchange.ReadLocalSyncCounts(storePath)
	if err != nil {
		return exchange.LocalSyncCounts{}
	}
	return counts
}
