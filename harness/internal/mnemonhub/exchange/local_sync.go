// Package exchange holds the local mnemond side of mnemonhub event exchange: reading the pending
// synced-event push batch, applying a push response's per-event status, reading pull state/counts,
// and advancing the pull cursor. It imports store + contract only. The ingest-driving pull import
// (which re-enters Event Intake via a runtime) lives in app, not here, so exchange never depends
// upward into mnemond runtime wiring.
//
// Each helper exists in two forms: a LiveStore form over an ALREADY-OPEN handle (the in-process sync
// worker drives these through the live runtime — opening the store by path from inside the serving
// process would self-collide on the single-writer flock, v1.1 #2) and the original path-based form
// that opens/closes per call, kept for the OFFLINE CLI verbs (`sync push|pull --once`, background).
package exchange

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

// LiveStore is the open-handle surface the exchange passes drive: satisfied by *store.Store (the offline
// opener) and by *runtime.Runtime's passthroughs (the in-process worker). Cursor access is included
// because the pull cursor is durable sync state; callers must stay on the sync cursor names.
type LiveStore interface {
	ReplicaID() (string, error)
	PendingSyncedEvents() ([]eventmodel.EventEnvelope, error)
	MarkSyncedEventStatus(originMnemond, eventID string, subject eventmodel.EventSubject, status, remotePeerID, at, diagnostic string) error
	GetCursor(name string) int64
	SetCursor(name string, seq int64) error
}

var _ LiveStore = (*store.Store)(nil)

type LocalSyncPushBatch struct {
	ReplicaID string
	Events    []eventmodel.EventEnvelope
}

type LocalSyncPullState struct {
	ReplicaID    string
	RemoteCursor string
}

type LocalSyncCounts struct {
	Pending   int
	Synced    int
	Conflicts int
}

// ReadPushBatch reads the pending outbound synced events + the local replica identity from an open handle.
func ReadPushBatch(s LiveStore) (LocalSyncPushBatch, error) {
	events, err := s.PendingSyncedEvents()
	if err != nil {
		return LocalSyncPushBatch{}, fmt.Errorf("read pending synced events: %w", err)
	}
	if len(events) == 0 {
		return LocalSyncPushBatch{}, nil
	}
	replicaID, err := s.ReplicaID()
	if err != nil {
		return LocalSyncPushBatch{}, fmt.Errorf("read local replica id: %w", err)
	}
	return LocalSyncPushBatch{ReplicaID: replicaID, Events: events}, nil
}

func ReadLocalSyncPushBatch(storePath string) (LocalSyncPushBatch, error) {
	s, err := openLocalSyncStore(storePath)
	if err != nil {
		return LocalSyncPushBatch{}, fmt.Errorf("open Local Mnemon store: %w", err)
	}
	defer s.Close()
	return ReadPushBatch(s)
}

// ApplyPushResponse mirrors the hub's per-event verdicts into the local sync_events ledger (the
// pusher-side half of the attribution chain) through an open handle.
func ApplyPushResponse(s LiveStore, remoteID string, resp contract.SyncPushResponse) error {
	now := time.Now().UTC().Format(time.RFC3339)
	for _, item := range resp.Accepted {
		if err := s.MarkSyncedEventStatus(item.OriginMnemond, item.EventID, item.Subject, "synced", remoteID, now, ""); err != nil {
			return err
		}
	}
	for _, item := range resp.Rejected {
		if err := s.MarkSyncedEventStatus(item.OriginMnemond, item.EventID, item.Subject, "rejected", remoteID, now, item.Diagnostic); err != nil {
			return err
		}
	}
	for _, item := range resp.Conflicts {
		if err := s.MarkSyncedEventStatus(item.OriginMnemond, item.EventID, item.Subject, "conflict", remoteID, now, item.Diagnostic); err != nil {
			return err
		}
	}
	return nil
}

func ApplyLocalSyncPushResponse(storePath, remoteID string, resp contract.SyncPushResponse) error {
	s, err := openLocalSyncStore(storePath)
	if err != nil {
		return fmt.Errorf("open Local Mnemon store for sync ack: %w", err)
	}
	defer s.Close()
	return ApplyPushResponse(s, remoteID, resp)
}

// ReadPullState reads the local replica identity + the durable pull cursor for remoteID.
func ReadPullState(s LiveStore, remoteID string) (LocalSyncPullState, error) {
	replicaID, err := s.ReplicaID()
	if err != nil {
		return LocalSyncPullState{}, fmt.Errorf("read local replica id: %w", err)
	}
	cursor := s.GetCursor(syncPullCursorName(remoteID))
	return LocalSyncPullState{ReplicaID: replicaID, RemoteCursor: strconv.FormatInt(cursor, 10)}, nil
}

func ReadLocalSyncPullState(storePath, remoteID string) (LocalSyncPullState, error) {
	s, err := openLocalSyncStore(storePath)
	if err != nil {
		return LocalSyncPullState{}, fmt.Errorf("open Local Mnemon store: %w", err)
	}
	defer s.Close()
	return ReadPullState(s, remoteID)
}

func ReadLocalSyncCounts(storePath string) (LocalSyncCounts, error) {
	s, err := openLocalSyncStore(storePath)
	if err != nil {
		return LocalSyncCounts{}, err
	}
	defer s.Close()
	counts, err := s.SyncEventCounts()
	if err != nil {
		return LocalSyncCounts{}, err
	}
	return LocalSyncCounts{
		Pending:   counts.Pending,
		Synced:    counts.Synced,
		Conflicts: counts.Conflicts,
	}, nil
}

// SetPullCursor advances the durable pull cursor for remoteID through an open handle. An empty
// cursor is a no-op (nothing was served).
func SetPullCursor(s LiveStore, remoteID, cursor string) error {
	if strings.TrimSpace(cursor) == "" {
		return nil
	}
	seq, err := strconv.ParseInt(cursor, 10, 64)
	if err != nil {
		return fmt.Errorf("parse sync pull cursor: %w", err)
	}
	return s.SetCursor(syncPullCursorName(remoteID), seq)
}

// SetSyncPullCursor advances the durable pull cursor for remoteID. It is the store-side tail of the
// pull import; the import itself (re-entering Event Intake) lives in app.
func SetSyncPullCursor(storePath, remoteID, cursor string) error {
	if strings.TrimSpace(cursor) == "" {
		return nil
	}
	s, err := openLocalSyncStore(storePath)
	if err != nil {
		return fmt.Errorf("open Local Mnemon store for sync cursor: %w", err)
	}
	defer s.Close()
	return SetPullCursor(s, remoteID, cursor)
}

func syncPullCursorName(remoteID string) string {
	return "sync_pull:" + remoteID
}

// PushBatchID derives a stable batch id from the batch content (order-independent), so a retried
// identical batch carries the same id — diagnostic provenance for the hub's audit, never an
// adjudication key (per-event idempotency is the replay defense, sync-abi-v1 §3).
func PushBatchID(replicaID string, events []eventmodel.EventEnvelope) string {
	keys := make([]string, 0, len(events))
	for _, env := range events {
		origin, _ := env.Meta["origin_mnemond"].(string)
		digest, _ := env.Meta["digest"].(string)
		keys = append(keys, strings.Join([]string{
			origin,
			env.Event.ID,
			string(env.Event.Subject),
			digest,
		}, "\x00"))
	}
	sort.Strings(keys)
	sum := sha256.Sum256([]byte(replicaID + "\x00" + strings.Join(keys, "\x00")))
	return "push-" + hex.EncodeToString(sum[:12])
}

func openLocalSyncStore(path string) (*store.Store, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		// T1 floor: this is a user-reachable creation path for the PRIVATE store dir
		// (`sync pull --once` can precede setup/local run) — owner-only, like every other
		// private-dir creation site.
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, err
		}
	}
	return store.OpenStore(path)
}

// ProbeAvailable reports whether the local store can be opened for an offline sync pass. It returns an
// error when a co-hosted Local Mnemon (`local run`) already holds the single-writer lock — so the
// standalone sync can refuse cleanly up front instead of failing every pass. It is side-effect free:
// a not-yet-created store is "available" (nothing holds it) without materializing the db or its dirs;
// an existing free store is opened and immediately released. The inverse holds too: while `local run`
// serves, ITS in-process worker owns sync and the offline verbs refuse here — the documented mutual
// exclusion between the worker and the manual verbs.
func ProbeAvailable(storePath string) error {
	if _, err := os.Stat(storePath); os.IsNotExist(err) {
		return nil
	}
	s, err := store.OpenStore(storePath)
	if err != nil {
		return err
	}
	return s.Close()
}
