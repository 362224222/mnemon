package app

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

// foreignGoalMaterial simulates a NEWER hub serving a kind this replica cannot import ("goal" is a
// known kind with no remote import mapping) — seeded into the hub log directly, since the current
// hub's own push validation would refuse it.
func foreignGoalMaterial(decisionID string) contract.SyncedEventMaterial {
	fields := map[string]any{"title": "remote goal this replica cannot import"}
	return contract.SyncedEventMaterial{
		OriginReplicaID: "other-replica", LocalDecisionID: decisionID, LocalIngestSeq: 9,
		Actor: "codex@other", ResourceRef: contract.ResourceRef{Kind: "goal", ID: "project"},
		ResourceVersion: 1, FieldsDigest: workerDigest(fields), Fields: fields,
		DecidedAt: "2026-06-12T00:00:00Z", Status: "pending",
	}
}

func countSkippedDiagnostics(t *testing.T, rt *runtime.Runtime, kind string) int {
	t.Helper()
	events, err := rt.PendingEvents(0)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	n := 0
	for _, ev := range events {
		if ev.Type != "sync.diagnostic" {
			continue
		}
		if reason, _ := ev.Payload["reason"].(string); strings.Contains(reason, "no import mapping") && strings.Contains(reason, kind) {
			n++
		}
	}
	return n
}

// v1.1 #4, worker path: a pulled material whose kind has no import mapping lands ONE durable
// sync.diagnostic (via the skipped observation + deny rule), exactly-once across re-pulls; the
// importable material in the same batch is unaffected; the cursor still advances.
func TestWorkerPullSkippedKindLandsDurableDiagnosticOnce(t *testing.T) {
	root := t.TempDir()
	rt := openServingRuntime(t, root)
	progressRef := contract.ResourceRef{Kind: "progress_digest", ID: "project"}
	// The newer-hub grant includes the goal ref — otherwise the hub's pull clamp would filter the
	// foreign-kind material before it ever reached this replica's importer.
	endpoint, _, hubStore := startHub(t, map[string]contract.ActorID{"tok-local": "replica-local@team"},
		[]contract.ResourceRef{progressRef, {Kind: "goal", ID: "project"}})
	connectRemote(t, root, endpoint, "tok-local")

	// Seed the hub log directly: one importable progress event + one goal event (newer-hub shape).
	now := "2026-06-12T00:00:00Z"
	progressEnv, err := contract.SyncedEventEnvelopeFromMaterial(foreignProgressMaterial("dec-progress", "remote-progress", "progress rides alongside the skipped kind"))
	if err != nil {
		t.Fatalf("materialize progress event: %v", err)
	}
	if _, err := hubStore.RecordRemoteSyncedEvent("replica-other@team", progressEnv, now); err != nil {
		t.Fatalf("seed progress event: %v", err)
	}
	goalEnv, err := contract.SyncedEventEnvelopeFromMaterial(foreignGoalMaterial("dec-goal"))
	if err != nil {
		t.Fatalf("materialize goal event: %v", err)
	}
	if _, err := hubStore.RecordRemoteSyncedEvent("replica-other@team", goalEnv, now); err != nil {
		t.Fatalf("seed goal event: %v", err)
	}

	if err := syncWorkerPass(rt, SyncWorkerOptions{ProjectRoot: root}); err != nil {
		t.Fatalf("worker pass: %v", err)
	}
	if got := countSkippedDiagnostics(t, rt, `"goal"`); got != 1 {
		t.Fatalf("skipped kind must land exactly one durable diagnostic, got %d", got)
	}
	// The progress material in the same batch imported normally.
	_, fields, err := rt.Resource(progressRef)
	if err != nil {
		t.Fatalf("read progress: %v", err)
	}
	if content, _ := fields["content"].(string); !strings.Contains(content, "progress rides alongside the skipped kind") {
		t.Fatalf("importable kind must be unaffected by the skip:\n%s", content)
	}
	// The cursor advanced past the skipped material (the stream never wedges)...
	if cur := rt.GetCursor("sync_pull:hub"); cur < 2 {
		t.Fatalf("pull cursor must advance past the skipped material, got %d", cur)
	}

	// ...and a forced RE-PULL from cursor zero is dedupe-absorbed: no second diagnostic.
	if err := rt.SetCursor("sync_pull:hub", 0); err != nil {
		t.Fatal(err)
	}
	if err := syncWorkerPass(rt, SyncWorkerOptions{ProjectRoot: root}); err != nil {
		t.Fatalf("re-pull pass: %v", err)
	}
	if got := countSkippedDiagnostics(t, rt, `"goal"`); got != 1 {
		t.Fatalf("re-pull must not duplicate the skipped diagnostic, got %d", got)
	}
}

// v1.1 #4, offline parity: ImportLocalSyncPull (the CLI pull path) produces the same exactly-once
// diagnostic for a skipped kind, and re-importing the same batch does not duplicate it.
func TestImportLocalSyncPullSkippedKindParity(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "local.db")
	materials := []contract.SyncedEventMaterial{
		foreignProgressMaterial("dec-progress-off", "remote-progress-off", "offline progress import works"),
		foreignGoalMaterial("dec-goal-off"),
	}
	syncedEvents := testSyncedEvents(t, materials...)
	if err := ImportLocalSyncPull(storePath, "hub", "2", syncedEvents, nil); err != nil {
		t.Fatalf("offline import: %v", err)
	}
	if err := ImportLocalSyncPull(storePath, "hub", "2", syncedEvents, nil); err != nil {
		t.Fatalf("offline re-import: %v", err)
	}

	rt, err := runtime.OpenRuntime(storePath, runtime.RuntimeConfig{})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer rt.Close()
	if got := countSkippedDiagnostics(t, rt, `"goal"`); got != 1 {
		t.Fatalf("offline path must land exactly one skipped diagnostic, got %d", got)
	}
	// Attribution payload rides the skipped observation (joinable from the diagnostic's CausedBy).
	events, _ := rt.PendingEvents(0)
	var observed bool
	for _, ev := range events {
		if ev.Type == "sync.import_skipped.observed" {
			rule := eventmodel.PayloadRule(ev.Payload)
			if rule["origin_replica_id"] == "other-replica" &&
				rule["local_decision_id"] == "dec-goal-off" &&
				rule["kind"] == "goal" && rule["remote_id"] == "hub" {
				observed = true
			}
		}
	}
	if !observed {
		t.Fatalf("skipped observation must carry {kind, origin_replica_id, local_decision_id, remote_id}: %+v", events)
	}
	// The progress material still imported.
	_, fields, err := rt.Resource(contract.ResourceRef{Kind: "progress_digest", ID: "project"})
	if err != nil {
		t.Fatalf("read progress: %v", err)
	}
	if content, _ := fields["content"].(string); !strings.Contains(content, "offline progress import works") {
		t.Fatalf("progress import must be unaffected:\n%s", content)
	}
}
