package mnemonhub

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func testNow() string { return time.Now().UTC().Format(time.RFC3339) }

func openTestHub(t *testing.T, grants Grants) (*Server, *store.Store) {
	t.Helper()
	st, err := store.OpenStore(filepath.Join(t.TempDir(), "hub.db"))
	if err != nil {
		t.Fatalf("open hub store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return New(st, grants, testNow), st
}

func testMaterial(replicaID, decisionID string, ref contract.ResourceRef, fields map[string]any) contract.SyncedEventMaterial {
	return contract.SyncedEventMaterial{
		OriginReplicaID: replicaID,
		LocalDecisionID: decisionID,
		LocalIngestSeq:  1,
		Actor:           "codex@project",
		ResourceRef:     ref,
		ResourceVersion: 1,
		FieldsDigest:    testDigest(fields),
		Fields:          fields,
		DecidedAt:       "2026-06-12T00:00:00Z",
		Status:          "pending",
	}
}

func testDigest(fields map[string]any) string {
	b, _ := json.Marshal(fields)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func testSyncEvent(t *testing.T, material contract.SyncedEventMaterial) eventmodel.EventEnvelope {
	t.Helper()
	env, err := contract.SyncedEventEnvelopeFromMaterial(material)
	if err != nil {
		t.Fatalf("synced event fixture: %v", err)
	}
	return env
}

func testSyncEvents(t *testing.T, materials ...contract.SyncedEventMaterial) []eventmodel.EventEnvelope {
	t.Helper()
	events := make([]eventmodel.EventEnvelope, 0, len(materials))
	for _, material := range materials {
		events = append(events, testSyncEvent(t, material))
	}
	return events
}

// The hub adjudication semantics extracted from the runtime, pinned at the package they now live in:
// first push accepted; identical replay idempotent (same ack, zero new rows); same idempotency key
// with different content -> conflict; invalid digest / non-syncable kind -> rejected with diagnostic.
func TestPushAdjudicationSemantics(t *testing.T) {
	mem := contract.ResourceRef{Kind: "memory", ID: "project"}
	grants := GrantMap{"replica-a@team": {Principal: "replica-a@team", Scopes: []contract.ResourceRef{mem}}}
	hub, st := openTestHub(t, grants)

	material := testMaterial("local-a", "dec-1", mem, map[string]any{"content": "hub accepted memory"})
	first, err := hub.Push("replica-a@team", contract.SyncPushRequest{ReplicaID: "local-a", BatchID: "b1", Events: testSyncEvents(t, material)})
	if err != nil {
		t.Fatalf("first push: %v", err)
	}
	if len(first.Accepted) != 1 || first.Accepted[0].Status != "accepted" || first.NextCursor == "" {
		t.Fatalf("first push must accept with a cursor, got %+v", first)
	}

	replay, err := hub.Push("replica-a@team", contract.SyncPushRequest{ReplicaID: "local-a", BatchID: "b1", Events: testSyncEvents(t, material)})
	if err != nil {
		t.Fatalf("replayed push: %v", err)
	}
	if !reflect.DeepEqual(first.Accepted, replay.Accepted) || len(replay.Conflicts) != 0 || len(replay.Rejected) != 0 {
		t.Fatalf("replay must repeat the accepted ack: first=%+v replay=%+v", first, replay)
	}
	if n, _ := st.RemoteSyncedEventCount(); n != 1 {
		t.Fatalf("replay must append zero rows, got %d", n)
	}

	mutated := testMaterial("local-a", "dec-1", mem, map[string]any{"content": "same key, different body"})
	conflicted, err := hub.Push("replica-a@team", contract.SyncPushRequest{ReplicaID: "local-a", BatchID: "b2", Events: testSyncEvents(t, mutated)})
	if err != nil {
		t.Fatalf("conflicting push: %v", err)
	}
	if len(conflicted.Conflicts) != 1 || !strings.Contains(conflicted.Conflicts[0].Diagnostic, "idempotency key") {
		t.Fatalf("key reuse with different content must conflict, got %+v", conflicted)
	}
	if conflicted.Conflicts[0].OriginMnemond != "local-a" || conflicted.Conflicts[0].LocalDecisionID() != "dec-1" {
		t.Fatalf("conflict result must carry the attribution identity, got %+v", conflicted.Conflicts[0])
	}

	bad := testMaterial("local-a", "dec-bad", mem, map[string]any{"content": "bad digest"})
	bad.FieldsDigest = "wrong"
	resp, err := hub.Push("replica-a@team", contract.SyncPushRequest{ReplicaID: "local-a", BatchID: "b3", Events: testSyncEvents(t, bad)})
	if err != nil {
		t.Fatalf("bad event must reject per-event, not fail transport: %v", err)
	}
	if len(resp.Rejected) != 1 || !strings.Contains(resp.Rejected[0].Diagnostic, "fields_digest") {
		t.Fatalf("bad digest must be rejected with diagnostic, got %+v", resp)
	}

	if _, err := hub.Push("replica-a@team", contract.SyncPushRequest{ReplicaID: "forged", BatchID: "b4", Events: testSyncEvents(t, material)}); err == nil {
		t.Fatal("request replica_id mismatching event origin must reject the request")
	}
}

// Pull serves synced events after the cursor, excludes the puller's own origin, clamps scopes,
// and replaying an old cursor re-serves identically (cursor replay idempotency).
func TestPullServesScopedEventsAfterCursor(t *testing.T) {
	mem := contract.ResourceRef{Kind: "memory", ID: "project"}
	skill := contract.ResourceRef{Kind: "skill", ID: "project"}
	grants := GrantMap{
		"replica-a@team": {Principal: "replica-a@team", Scopes: []contract.ResourceRef{mem, skill}},
		"replica-b@team": {Principal: "replica-b@team", Scopes: []contract.ResourceRef{mem}},
	}
	hub, _ := openTestHub(t, grants)

	seedMem := testMaterial("local-a", "dec-mem", mem, map[string]any{"content": "memory event"})
	seedSkill := testMaterial("local-a", "dec-skill", skill, map[string]any{"name": "project", "declarations": []any{}})
	if _, err := hub.Push("replica-a@team", contract.SyncPushRequest{ReplicaID: "local-a", BatchID: "seed", Events: testSyncEvents(t, seedMem, seedSkill)}); err != nil {
		t.Fatalf("seed push: %v", err)
	}

	// B's grant is memory-only: even with empty requested scopes, only the memory event serves.
	resp, err := hub.Pull("replica-b@team", contract.SyncPullRequest{ReplicaID: "local-b"})
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if len(resp.Events) != 1 || resp.Events[0].Event.Subject != eventmodel.Subject("memory", "project") {
		t.Fatalf("memory-only grant must serve only memory events, got %+v", resp.Events)
	}
	if resp.NextCursor == "" || resp.NextCursor == "0" {
		t.Fatalf("pull must advance the cursor, got %q", resp.NextCursor)
	}

	// Replay from the same old cursor re-serves the same event (idempotent by cursor).
	again, err := hub.Pull("replica-b@team", contract.SyncPullRequest{ReplicaID: "local-b"})
	if err != nil || len(again.Events) != 1 || again.NextCursor != resp.NextCursor {
		t.Fatalf("cursor replay must re-serve identically: %+v err=%v", again, err)
	}

	// From the advanced cursor, nothing more serves.
	after, err := hub.Pull("replica-b@team", contract.SyncPullRequest{ReplicaID: "local-b", RemoteCursor: resp.NextCursor})
	if err != nil || len(after.Events) != 0 || after.NextCursor != resp.NextCursor {
		t.Fatalf("pull past the end must serve nothing and hold the cursor: %+v err=%v", after, err)
	}

	// The origin replica never sees its own events echoed.
	echo, err := hub.Pull("replica-a@team", contract.SyncPullRequest{ReplicaID: "local-a"})
	if err != nil || len(echo.Events) != 0 {
		t.Fatalf("origin must not pull its own events back: %+v err=%v", echo, err)
	}

	// An explicit out-of-grant scope is denied (the ONE clamp).
	if _, err := hub.Pull("replica-b@team", contract.SyncPullRequest{ReplicaID: "local-b", Scopes: []contract.ResourceRef{skill}}); err == nil {
		t.Fatal("an out-of-grant pull scope must be denied")
	}
}

// The status counters are the hub-side push-arrival evidence (v1.1 #5): received counts the
// append-only log, served accumulates across pulls, and each principal's last served cursor lands.
func TestStatusReportsHubCounters(t *testing.T) {
	mem := contract.ResourceRef{Kind: "memory", ID: "project"}
	grants := GrantMap{
		"replica-a@team": {Principal: "replica-a@team", Scopes: []contract.ResourceRef{mem}},
		"replica-b@team": {Principal: "replica-b@team", Scopes: []contract.ResourceRef{mem}},
	}
	hub, _ := openTestHub(t, grants)

	st0, err := hub.Status("replica-a@team")
	if err != nil {
		t.Fatalf("empty status: %v", err)
	}
	if st0.HubEventsReceived != 0 || st0.HubEventsServed != 0 || len(st0.HubReplicaCursors) != 0 {
		t.Fatalf("fresh hub must report zero counters, got %+v", st0)
	}

	material := testMaterial("local-a", "dec-1", mem, map[string]any{"content": "counted"})
	if _, err := hub.Push("replica-a@team", contract.SyncPushRequest{ReplicaID: "local-a", BatchID: "b", Events: testSyncEvents(t, material)}); err != nil {
		t.Fatalf("push: %v", err)
	}
	pulled, err := hub.Pull("replica-b@team", contract.SyncPullRequest{ReplicaID: "local-b"})
	if err != nil || len(pulled.Events) != 1 {
		t.Fatalf("pull: %+v err=%v", pulled, err)
	}

	st1, err := hub.Status("replica-a@team")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if st1.HubEventsReceived != 1 || st1.HubEventsServed != 1 {
		t.Fatalf("counters must reflect one received + one served, got %+v", st1)
	}
	if st1.HubReplicaCursors["replica-b@team"] != pulled.NextCursor {
		t.Fatalf("last cursor per replica must record b's serve position, got %+v", st1.HubReplicaCursors)
	}
	if st1.RemoteWorkspace != "connected" || st1.Principal != "replica-a@team" {
		t.Fatalf("status identity fields must hold, got %+v", st1)
	}
}

// SQLite tx serialization under one open store: two concurrent pushers both land (v1.1 #11). The
// hub's concurrency model is the single open handle + store tx — no second flock, no lost writes.
func TestConcurrentPushersBothLand(t *testing.T) {
	mem := contract.ResourceRef{Kind: "memory", ID: "project"}
	grants := GrantMap{
		"replica-a@team": {Principal: "replica-a@team", Scopes: []contract.ResourceRef{mem}},
		"replica-b@team": {Principal: "replica-b@team", Scopes: []contract.ResourceRef{mem}},
	}
	hub, st := openTestHub(t, grants)

	push := func(principal contract.ActorID, origin string, n int) error {
		for i := 0; i < n; i++ {
			material := testMaterial(origin, fmt.Sprintf("dec-%s-%d", origin, i), mem,
				map[string]any{"content": fmt.Sprintf("event %s %d", origin, i)})
			resp, err := hub.Push(principal, contract.SyncPushRequest{
				ReplicaID: origin, BatchID: fmt.Sprintf("b-%s-%d", origin, i), Events: testSyncEvents(t, material)})
			if err != nil {
				return err
			}
			if len(resp.Accepted) != 1 {
				return fmt.Errorf("push %s/%d not accepted: %+v", origin, i, resp)
			}
		}
		return nil
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(2)
	go func() { defer wg.Done(); errs <- push("replica-a@team", "local-a", 10) }()
	go func() { defer wg.Done(); errs <- push("replica-b@team", "local-b", 10) }()
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent pusher failed: %v", err)
		}
	}
	if n, _ := st.RemoteSyncedEventCount(); n != 20 {
		t.Fatalf("both concurrent pushers must land all events, got %d/20", n)
	}
}

// MED-6: two goroutines race the SAME idempotency key (origin_replica_id, local_decision_id) with
// DIFFERENT content. The UNIQUE key + adjudication must give a deterministic outcome — exactly one
// accept and one conflict, never two accepts, never a panic, and exactly one durable row.
func TestConcurrentPushSameKeyDifferentContentDeterministic(t *testing.T) {
	mem := contract.ResourceRef{Kind: "memory", ID: "project"}
	grants := GrantMap{"replica-a@team": {Principal: "replica-a@team", Scopes: []contract.ResourceRef{mem}}}
	hub, st := openTestHub(t, grants)

	push := func(content string) (contract.SyncPushResponse, error) {
		material := testMaterial("local-a", "dec-shared", mem, map[string]any{"content": content})
		return hub.Push("replica-a@team", contract.SyncPushRequest{
			ReplicaID: "local-a", BatchID: "b-" + content, Events: testSyncEvents(t, material)})
	}

	type result struct {
		resp contract.SyncPushResponse
		err  error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); r, e := push("content-one"); results <- result{r, e} }()
	go func() { defer wg.Done(); r, e := push("content-two"); results <- result{r, e} }()
	wg.Wait()
	close(results)

	accepts, conflicts := 0, 0
	for r := range results {
		if r.err != nil {
			t.Fatalf("same-key race must not fail transport: %v", r.err)
		}
		accepts += len(r.resp.Accepted)
		conflicts += len(r.resp.Conflicts)
		if len(r.resp.Rejected) != 0 {
			t.Fatalf("same-key race must not reject (valid events), got %+v", r.resp.Rejected)
		}
	}
	if accepts != 1 || conflicts != 1 {
		t.Fatalf("same-key different-content race must be exactly one accept + one conflict, got %d accept / %d conflict", accepts, conflicts)
	}
	if n, _ := st.RemoteSyncedEventCount(); n != 1 {
		t.Fatalf("exactly one durable row must persist (no partial/double row), got %d", n)
	}
}

// MED-7: Actor is documented attribution — an empty actor is rejected per-event with a diagnostic.
func TestPushRejectsEmptyActor(t *testing.T) {
	mem := contract.ResourceRef{Kind: "memory", ID: "project"}
	grants := GrantMap{"replica-a@team": {Principal: "replica-a@team", Scopes: []contract.ResourceRef{mem}}}
	hub, st := openTestHub(t, grants)

	material := testMaterial("local-a", "dec-noactor", mem, map[string]any{"content": "no actor"})
	env := testSyncEvent(t, material)
	env.Event.Actor = "   " // whitespace-only is still empty after trim
	resp, err := hub.Push("replica-a@team", contract.SyncPushRequest{ReplicaID: "local-a", BatchID: "b", Events: []eventmodel.EventEnvelope{env}})
	if err != nil {
		t.Fatalf("empty-actor event must reject per-event, not fail transport: %v", err)
	}
	if len(resp.Rejected) != 1 || !strings.Contains(resp.Rejected[0].Diagnostic, "event actor is required") {
		t.Fatalf("empty actor must be rejected with 'actor is required', got %+v", resp)
	}
	if n, _ := st.RemoteSyncedEventCount(); n != 0 {
		t.Fatalf("a rejected event must not persist, got %d rows", n)
	}
}
