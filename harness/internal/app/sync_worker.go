package app

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/capability"
	"github.com/mnemon-dev/mnemon/harness/internal/channel"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemonhub/exchange"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

// The driver sync worker (v1.1 #2): inside the SERVING process, sync operates the already-open
// runtime/store handle — push reads pending synced events and applies the hub's verdicts through the
// live handle; pull re-enters Event Intake via the runtime's trusted intake + Tick. It never opens
// the store by path (the single-writer flock would self-collide); the path-based mnemonhub exchange
// helpers remain the OFFLINE CLI verbs' tools, and ProbeAvailable keeps the two mutually exclusive.

// SyncWorkerOptions configures the worker. The zero value is safe: default cadence and transport
// timeout, fail-closed transport security.
type SyncWorkerOptions struct {
	ProjectRoot         string
	Interval            time.Duration // <= 0 defaults to defaultSyncWorkerInterval
	Timeout             time.Duration // per-call transport bound; <= 0 defaults to channel.DefaultSyncTimeout
	AllowInsecureRemote bool          // explicit T2 downgrade override (v1.1 #3)
	// Catalog is the boot-resolved capability catalog the pull import derives its kind→observation
	// mapping from (descriptor-derived, PD6). nil falls back to the embedded first-party catalog.
	Catalog map[string]capability.Capability
}

const defaultSyncWorkerInterval = 30 * time.Second

// RunSyncWorker loops one sync pass on its own cadence until ctx cancels. Every pass error is
// logged to errw and SWALLOWED — an unreachable remote degrades sync, never the serve path (I13:
// the local loop stays fully functional offline; the bounded client keeps each pass finite).
func RunSyncWorker(ctx context.Context, rt *runtime.Runtime, opts SyncWorkerOptions, errw io.Writer) {
	interval := opts.Interval
	if interval <= 0 {
		interval = defaultSyncWorkerInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := syncWorkerPass(rt, opts); err != nil {
				fmt.Fprintf(errw, "mnemon-harness: sync worker: %v\n", err)
			}
		}
	}
}

// syncWorkerPass runs ONE push+pull pass against the configured current remote. Gate: when
// remotes.json does not exist, the pass is a no-op — zero sync activity without a connected remote
// (I13), checked per pass so `sync connect` takes effect without a restart.
func syncWorkerPass(rt *runtime.Runtime, opts SyncWorkerOptions) error {
	remotesPath := filepath.Join(opts.ProjectRoot, ".mnemon", "harness", "sync", "remotes.json")
	if _, err := os.Stat(remotesPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat Remote Workspace config: %w", err)
	}
	entry, err := exchange.LoadRemoteEntry(remotesPath, "default")
	if err != nil {
		return err
	}
	client, err := syncWorkerClient(entry, opts)
	if err != nil {
		return err
	}
	if err := syncWorkerPush(rt, client, entry.ID); err != nil {
		return err
	}
	return syncWorkerPull(rt, client, entry.ID, opts.Catalog)
}

// syncWorkerClient builds the bounded sync client from the remote entry: credential_ref + ca_file
// resolve relative to the project root (the same resolution `sync connect` wrote them under), and
// the endpoint passes the T2 downgrade gate unless explicitly overridden.
func syncWorkerClient(entry exchange.RemoteEntry, opts SyncWorkerOptions) (*channel.Client, error) {
	if strings.TrimSpace(entry.CredentialRef) == "" {
		return nil, fmt.Errorf("Remote Workspace %q has no credential_ref", entry.ID)
	}
	tokPath := entry.CredentialRef
	if !filepath.IsAbs(tokPath) {
		tokPath = filepath.Join(opts.ProjectRoot, tokPath)
	}
	raw, err := os.ReadFile(tokPath)
	if err != nil {
		return nil, fmt.Errorf("read Remote Workspace token file: %w", err)
	}
	token := strings.TrimSpace(string(raw))
	if token == "" {
		return nil, fmt.Errorf("Remote Workspace token file %s is empty", entry.CredentialRef)
	}
	caFile := entry.CAFile
	if caFile != "" && !filepath.IsAbs(caFile) {
		caFile = filepath.Join(opts.ProjectRoot, caFile)
	}
	return channel.NewSyncClient(entry.Endpoint, channel.SyncClientConfig{
		Token:         token,
		Timeout:       opts.Timeout,
		CAFile:        caFile,
		AllowInsecure: opts.AllowInsecureRemote,
	})
}

// syncWorkerPush pushes the pending batch (if any) and mirrors the hub's per-event verdicts into
// the local ledger — both through the live handle.
func syncWorkerPush(rt *runtime.Runtime, client *channel.Client, remoteID string) error {
	batch, err := exchange.ReadPushBatch(rt)
	if err != nil {
		return err
	}
	if len(batch.Events) == 0 {
		return nil
	}
	resp, err := client.SyncPush(contract.SyncPushRequest{
		ReplicaID: batch.ReplicaID,
		BatchID:   exchange.PushBatchID(batch.ReplicaID, batch.Events),
		Events:    batch.Events,
	})
	if err != nil {
		return fmt.Errorf("sync push failed: %w", err)
	}
	return exchange.ApplyPushResponse(rt, remoteID, resp)
}

// syncWorkerPull pulls after the durable cursor, re-enters each event through the live runtime's
// trusted intake (importPulledEvents — the same loop the offline path uses), then advances the
// cursor.
func syncWorkerPull(rt *runtime.Runtime, client *channel.Client, remoteID string, catalog map[string]capability.Capability) error {
	state, err := exchange.ReadPullState(rt, remoteID)
	if err != nil {
		return err
	}
	resp, err := client.SyncPull(contract.SyncPullRequest{
		ReplicaID:    state.ReplicaID,
		RemoteCursor: state.RemoteCursor,
	})
	if err != nil {
		return fmt.Errorf("sync pull failed: %w", err)
	}
	if err := importPulledEvents(rt, remoteID, resp.Events, catalog); err != nil {
		return err
	}
	return exchange.SetPullCursor(rt, remoteID, resp.NextCursor)
}
