package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/app"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemonhub/exchange"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
	"github.com/spf13/cobra"
)

var (
	syncRoot            string
	syncStorePath       string
	syncRemotesPath     string
	syncRemoteID        string
	syncRemoteURL       string
	syncRemoteToken     string
	syncRemoteTokenFile string
	syncCAFile          string
	syncAllowInsecure   bool
	syncOnce            bool
	syncBackground      bool
	syncInterval        time.Duration
)

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Sync Local Mnemon with Remote Workspace",
}

var syncConnectCmd = &cobra.Command{
	Use:   "connect <workspace>",
	Short: "Connect Remote Workspace",
	Args:  cobra.ExactArgs(1),
	RunE:  runSyncConnect,
}

var syncPushCmd = &cobra.Command{
	Use:   "push --once",
	Short: "Push local accepted changes to Remote Workspace",
	RunE:  runSyncPush,
}

var syncPullCmd = &cobra.Command{
	Use:   "pull --once",
	Short: "Pull Remote Workspace changes into Local Mnemon",
	RunE:  runSyncPull,
}

var syncRunCmd = &cobra.Command{
	Use:   "run --background",
	Short: "Run Remote Workspace sync in the background",
	RunE:  runSyncBackground,
}

func init() {
	syncCmd.PersistentFlags().StringVar(&syncRoot, "root", ".", "project root")
	syncCmd.PersistentFlags().StringVar(&syncStorePath, "store", "", "Local Mnemon store path")
	syncCmd.PersistentFlags().StringVar(&syncRemotesPath, "remotes", "", "Remote Workspace config path")
	syncCmd.PersistentFlags().StringVar(&syncRemoteID, "remote", "default", "Remote Workspace id")
	syncCmd.PersistentFlags().StringVar(&syncRemoteURL, "remote-url", "", "Remote Workspace sync endpoint")
	syncCmd.PersistentFlags().StringVar(&syncRemoteToken, "token", "", "Remote Workspace sync token")
	syncCmd.PersistentFlags().StringVar(&syncRemoteTokenFile, "token-file", "", "Remote Workspace sync token file")
	syncCmd.PersistentFlags().StringVar(&syncCAFile, "ca-file", "", "PEM bundle pinning the Remote Workspace TLS root (e.g. the mnemon-hub --dev-selfsigned cert)")
	syncCmd.PersistentFlags().BoolVar(&syncAllowInsecure, "allow-insecure-remote", false, "explicitly allow a plaintext http:// Remote Workspace endpoint with a non-loopback host (T2: fail-closed by default)")
	_ = syncCmd.PersistentFlags().MarkHidden("store")
	_ = syncCmd.PersistentFlags().MarkHidden("remotes")
	_ = syncCmd.PersistentFlags().MarkHidden("token-file")
	syncPushCmd.Flags().BoolVar(&syncOnce, "once", false, "push one batch and exit")
	syncPullCmd.Flags().BoolVar(&syncOnce, "once", false, "pull one batch and exit")
	syncRunCmd.Flags().BoolVar(&syncBackground, "background", false, "run until interrupted")
	syncRunCmd.Flags().DurationVar(&syncInterval, "interval", 30*time.Second, "background sync interval")
	syncCmd.AddCommand(syncConnectCmd, syncPushCmd, syncPullCmd, syncRunCmd)
	syncCmd.GroupID = groupSpine
	rootCmd.AddCommand(syncCmd)
}

func runSyncConnect(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("sync connect requires a workspace name")
	}
	workspace := strings.TrimSpace(args[0])
	if !validRemoteWorkspaceID(workspace) {
		return fmt.Errorf("Remote Workspace name must use letters, numbers, dot, dash, or underscore")
	}
	endpoint := strings.TrimSpace(syncRemoteURL)
	if endpoint == "" {
		return fmt.Errorf("--remote-url is required")
	}
	// T2 downgrade gate at WRITE time (v1.1 #3): a plaintext non-loopback endpoint never enters
	// remotes.json unless explicitly overridden — the worker and the manual verbs then re-validate
	// at client construction.
	if err := access.ValidateSyncEndpoint(endpoint, syncAllowInsecure); err != nil {
		return err
	}
	if strings.TrimSpace(syncRemoteToken) == "" && strings.TrimSpace(syncRemoteTokenFile) == "" {
		return fmt.Errorf("--token or --token-file is required")
	}
	if err := upsertSyncRemote(resolvedSyncRemotesPath(), syncProjectRoot(), workspace, endpoint, syncRemoteToken, syncRemoteTokenFile, syncCAFile); err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Remote Workspace: connected %s\n", workspace)
	fmt.Fprintln(cmd.OutOrStdout(), "Sync: ready")
	return nil
}

// ensureSyncStoreAvailable refuses a manual sync (one-shot or background) cleanly when a co-hosted
// Local Mnemon (`local run`) holds the single-writer lock, instead of failing with a raw lock error.
// While the service runs, its in-process sync worker owns sync; the manual verbs cover the
// service-stopped path.
func ensureSyncStoreAvailable() error {
	if err := exchange.ProbeAvailable(resolvedSyncStorePath()); err != nil {
		return fmt.Errorf("the local store is busy (is `mnemon-harness local run` running?) — its in-process sync worker already syncs a connected Remote Workspace; stop it to sync manually: %w", err)
	}
	return nil
}

func runSyncPush(cmd *cobra.Command, args []string) error {
	if err := ensureSyncStoreAvailable(); err != nil {
		return err
	}
	result, err := syncPushOnce()
	if err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Sync push: %d accepted, %d rejected, %d conflicts\n", result.accepted, result.rejected, result.conflicts)
	return nil
}

func runSyncPull(cmd *cobra.Command, args []string) error {
	if err := ensureSyncStoreAvailable(); err != nil {
		return err
	}
	result, err := syncPullOnce()
	if err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Sync pull: %d events\n", result.events)
	return nil
}

func runSyncBackground(cmd *cobra.Command, args []string) error {
	if !syncBackground {
		return fmt.Errorf("sync run requires --background")
	}
	if syncInterval <= 0 {
		return fmt.Errorf("--interval must be positive")
	}
	// Background sync opens the governed store directly, so it cannot run while a co-hosted Local
	// Mnemon holds the single-writer lock. Probe once up front and refuse cleanly rather than failing
	// (with a raw lock error) every pass.
	if err := ensureSyncStoreAvailable(); err != nil {
		return err
	}
	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()
	for {
		if result, err := syncPushOnce(); err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "sync push failed: %v\n", err)
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "Sync push: %d accepted, %d rejected, %d conflicts\n", result.accepted, result.rejected, result.conflicts)
		}
		if result, err := syncPullOnce(); err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "sync pull failed: %v\n", err)
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "Sync pull: %d events\n", result.events)
		}
		select {
		case <-cmd.Context().Done():
			return cmd.Context().Err()
		case <-ticker.C:
		}
	}
}

type syncPushResult struct {
	accepted  int
	rejected  int
	conflicts int
}

type syncPullResult struct {
	events int
}

func syncPushOnce() (syncPushResult, error) {
	storePath := resolvedSyncStorePath()
	batch, err := exchange.ReadLocalSyncPushBatch(storePath)
	if err != nil {
		return syncPushResult{}, err
	}
	if len(batch.Events) == 0 {
		return syncPushResult{}, nil
	}
	remote, err := resolveSyncRemote()
	if err != nil {
		return syncPushResult{}, err
	}
	client, err := syncClientFor(remote)
	if err != nil {
		return syncPushResult{}, err
	}
	resp, err := client.SyncPush(contract.SyncPushRequest{
		ReplicaID: batch.ReplicaID,
		BatchID:   exchange.PushBatchID(batch.ReplicaID, batch.Events),
		Events:    batch.Events,
	})
	if err != nil {
		return syncPushResult{}, fmt.Errorf("sync push failed: %w", err)
	}
	if err := exchange.ApplyLocalSyncPushResponse(storePath, remote.ID, resp); err != nil {
		return syncPushResult{}, err
	}
	return syncPushResult{accepted: len(resp.Accepted), rejected: len(resp.Rejected), conflicts: len(resp.Conflicts)}, nil
}

func syncPullOnce() (syncPullResult, error) {
	remote, err := resolveSyncRemote()
	if err != nil {
		return syncPullResult{}, err
	}
	storePath := resolvedSyncStorePath()
	state, err := exchange.ReadLocalSyncPullState(storePath, remote.ID)
	if err != nil {
		return syncPullResult{}, err
	}
	client, err := syncClientFor(remote)
	if err != nil {
		return syncPullResult{}, err
	}
	resp, err := client.SyncPull(contract.SyncPullRequest{
		ReplicaID:    state.ReplicaID,
		RemoteCursor: state.RemoteCursor,
	})
	if err != nil {
		return syncPullResult{}, fmt.Errorf("sync pull failed: %w", err)
	}
	catalog := app.SyncImportCatalog(syncProjectRoot(), os.Stderr)
	if err := app.ImportLocalSyncPull(storePath, remote.ID, resp.NextCursor, resp.Events, catalog); err != nil {
		return syncPullResult{}, err
	}
	return syncPullResult{events: len(resp.Events)}, nil
}

type syncRemoteConfig struct {
	ID       string
	Endpoint string
	Token    string
	CAFile   string
}

// syncClientFor builds the bounded sync client for one resolved remote: bearer token, optional
// pinned TLS root, and the T2 downgrade gate (--allow-insecure-remote is the only override).
func syncClientFor(remote syncRemoteConfig) (*access.Client, error) {
	return access.NewSyncClient(remote.Endpoint, access.SyncClientConfig{
		Token:         remote.Token,
		CAFile:        remote.CAFile,
		AllowInsecure: syncAllowInsecure,
	})
}

func resolveSyncRemote() (syncRemoteConfig, error) {
	if strings.TrimSpace(syncRemoteURL) != "" {
		tokenFile := syncRemoteTokenFile
		if tokenFile != "" {
			tokenFile = resolveSyncPath(tokenFile)
		}
		token, err := resolveSyncToken(syncRemoteToken, tokenFile)
		if err != nil {
			return syncRemoteConfig{}, err
		}
		return syncRemoteConfig{ID: syncRemoteID, Endpoint: syncRemoteURL, Token: token, CAFile: resolvedSyncCAFile("")}, nil
	}
	entry, err := exchange.LoadRemoteEntry(resolvedSyncRemotesPath(), syncRemoteID)
	if err != nil {
		return syncRemoteConfig{}, err
	}
	if strings.TrimSpace(entry.CredentialRef) == "" && strings.TrimSpace(syncRemoteToken) == "" && strings.TrimSpace(syncRemoteTokenFile) == "" {
		return syncRemoteConfig{}, fmt.Errorf("Remote Workspace %q has no credential_ref", entry.ID)
	}
	tokenFile := ""
	if strings.TrimSpace(entry.CredentialRef) != "" {
		tokenFile = resolveSyncPath(entry.CredentialRef)
	}
	token, err := resolveSyncToken(syncRemoteToken, tokenFile)
	if err != nil {
		return syncRemoteConfig{}, err
	}
	return syncRemoteConfig{ID: entry.ID, Endpoint: entry.Endpoint, Token: token, CAFile: resolvedSyncCAFile(entry.CAFile)}, nil
}

// resolvedSyncCAFile picks the pinned-root file: the --ca-file flag overrides the remotes.json
// entry; relative paths resolve against the project root (the same resolution connect writes).
func resolvedSyncCAFile(entryCAFile string) string {
	caFile := strings.TrimSpace(syncCAFile)
	if caFile == "" {
		caFile = strings.TrimSpace(entryCAFile)
	}
	if caFile == "" {
		return ""
	}
	return resolveSyncPath(caFile)
}

func upsertSyncRemote(path, root, id, endpoint, token, tokenFile, caFile string) error {
	doc := exchange.RemotesDoc{SchemaVersion: 1}
	if raw, err := os.ReadFile(path); err == nil && len(strings.TrimSpace(string(raw))) > 0 {
		if err := json.Unmarshal(raw, &doc); err != nil {
			return fmt.Errorf("parse Remote Workspace config: %w", err)
		}
		if doc.SchemaVersion != 1 {
			return fmt.Errorf("Remote Workspace config schema_version %d unsupported (want 1)", doc.SchemaVersion)
		}
	} else if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read Remote Workspace config: %w", err)
	}
	credentialRef, err := syncCredentialRef(root, id, token, tokenFile)
	if err != nil {
		return err
	}
	entry := exchange.RemoteEntry{ID: id, Endpoint: endpoint, CredentialRef: credentialRef, CAFile: normalizeSyncFileRef(caFile)}
	replaced := false
	for i := range doc.Remotes {
		if doc.Remotes[i].ID == id {
			doc.Remotes[i] = entry
			replaced = true
			break
		}
	}
	if !replaced {
		doc.Remotes = append(doc.Remotes, entry)
	}
	doc.Current = id
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

// normalizeSyncFileRef records a file reference the way credential refs are recorded: absolute
// verbatim, relative cleaned to slash form (resolved against the project root at read time).
func normalizeSyncFileRef(ref string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" || filepath.IsAbs(ref) {
		return ref
	}
	return filepath.ToSlash(filepath.Clean(ref))
}

func syncCredentialRef(root, id, token, tokenFile string) (string, error) {
	token = strings.TrimSpace(token)
	tokenFile = strings.TrimSpace(tokenFile)
	if token != "" {
		credentialRef := filepath.ToSlash(filepath.Join(".mnemon", "harness", "sync", "credentials", id+".token"))
		path := filepath.Join(root, filepath.FromSlash(credentialRef))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return "", err
		}
		if err := os.WriteFile(path, []byte(token+"\n"), 0o600); err != nil {
			return "", err
		}
		return credentialRef, nil
	}
	if tokenFile == "" {
		return "", fmt.Errorf("--token or --token-file is required")
	}
	if filepath.IsAbs(tokenFile) {
		return tokenFile, nil
	}
	return filepath.ToSlash(filepath.Clean(tokenFile)), nil
}

func validRemoteWorkspaceID(id string) bool {
	if id == "" {
		return false
	}
	for _, r := range id {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			continue
		}
		return false
	}
	return true
}

func resolveSyncToken(token, tokenFile string) (string, error) {
	if strings.TrimSpace(tokenFile) != "" {
		raw, err := os.ReadFile(tokenFile)
		if err != nil {
			return "", fmt.Errorf("read Remote Workspace token file: %w", err)
		}
		token = strings.TrimSpace(string(raw))
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return "", fmt.Errorf("Remote Workspace sync token is required")
	}
	return token, nil
}

func resolvedSyncStorePath() string {
	if syncStorePath != "" {
		return resolveSyncPath(syncStorePath)
	}
	return filepath.Join(syncProjectRoot(), runtime.DefaultStorePath)
}

func resolvedSyncRemotesPath() string {
	if syncRemotesPath != "" {
		return resolveSyncPath(syncRemotesPath)
	}
	return filepath.Join(syncProjectRoot(), ".mnemon", "harness", "sync", "remotes.json")
}

func resolveSyncPath(path string) string {
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	return filepath.Join(syncProjectRoot(), path)
}

func syncProjectRoot() string {
	if syncRoot == "" {
		return "."
	}
	return filepath.Clean(syncRoot)
}
