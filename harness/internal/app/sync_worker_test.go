package app

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/capability"
	"github.com/mnemon-dev/mnemon/harness/internal/channel"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemonhub"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

// openServingRuntime boots the PRODUCT serving runtime (OpenLocalRuntime = assembled host policy +
// merged sync-import policy) over a memory+skill host binding — the exact runtime the worker
// operates inside `local run`.
func openServingRuntime(t *testing.T, root string) *runtime.Runtime {
	t.Helper()
	refs := []contract.ResourceRef{{Kind: "memory", ID: "project"}, {Kind: "skill", ID: "project"}}
	b := channel.HostAgentBinding("codex@project", "http://127.0.0.1:8787", refs)
	rt, err := OpenLocalRuntime(filepath.Join(root, runtime.DefaultStorePath), channel.LoadedBindings{Bindings: []channel.ChannelBinding{b}}, nil, nil)
	if err != nil {
		t.Fatalf("open serving runtime: %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })
	return rt
}

// startHub serves a mnemonhub hub over its own store and returns the endpoint + the hub handles.
func startHub(t *testing.T, principals map[string]contract.ActorID, scopes []contract.ResourceRef) (string, *mnemonhub.Server, *store.Store) {
	t.Helper()
	st, err := store.OpenStore(filepath.Join(t.TempDir(), "hub.db"))
	if err != nil {
		t.Fatalf("open hub store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	grants := mnemonhub.GrantMap{}
	tokens := map[string]contract.ActorID{}
	for token, principal := range principals {
		grants[principal] = contract.ReplicaGrant{Principal: principal, Scopes: scopes}
		tokens[token] = principal
	}
	hub := mnemonhub.New(st, grants, func() string { return time.Now().UTC().Format(time.RFC3339) })
	srv := httptest.NewServer(mnemonhub.NewHTTPHandler(hub, mnemonhub.BearerAuthenticator{Tokens: tokens}, nil))
	t.Cleanup(srv.Close)
	return srv.URL, hub, st
}

func connectRemote(t *testing.T, root, endpoint, token string) {
	t.Helper()
	credRel := filepath.Join(".mnemon", "harness", "sync", "credentials", "hub.token")
	credPath := filepath.Join(root, credRel)
	if err := os.MkdirAll(filepath.Dir(credPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(credPath, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	remotesPath := filepath.Join(root, ".mnemon", "harness", "sync", "remotes.json")
	doc := fmt.Sprintf(`{"schema_version":1,"current":"hub","remotes":[{"id":"hub","endpoint":%q,"credential_ref":%q}]}`, endpoint, filepath.ToSlash(credRel))
	if err := os.WriteFile(remotesPath, []byte(doc+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func observeMemory(t *testing.T, rt *runtime.Runtime, externalID, content string) {
	t.Helper()
	if _, _, err := rt.API().Ingest("codex@project", contract.ObservationEnvelope{
		ExternalID: externalID,
		Event: contract.Event{Type: capability.MemoryWriteCandidateObserved, Payload: map[string]any{
			"content": content, "source": "test", "confidence": "high",
		}},
	}); err != nil {
		t.Fatalf("host observe: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
}

func workerDigest(fields map[string]any) string {
	b, _ := json.Marshal(fields)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func foreignMemoryMaterial(decisionID, entryID, content string) contract.SyncedEventMaterial {
	fields := map[string]any{
		"content": "# Local Memory\n- " + content,
		"entries": []any{map[string]any{
			"id": entryID, "content": content, "source": "remote", "confidence": "high",
			"actor": "codex@other", "ingest_seq": float64(7),
		}},
	}
	return contract.SyncedEventMaterial{
		OriginReplicaID: "other-replica", LocalDecisionID: decisionID, LocalIngestSeq: 7,
		Actor: "codex@other", ResourceRef: contract.ResourceRef{Kind: "memory", ID: "project"},
		ResourceVersion: 1, FieldsDigest: workerDigest(fields), Fields: fields,
		DecidedAt: "2026-06-12T00:00:00Z", Status: "pending",
	}
}

// I13 first leg: with NO remotes.json a worker pass is a strict no-op — zero sync activity, zero
// errors, the local store untouched.
func TestSyncWorkerIdleWithoutRemoteConfig(t *testing.T) {
	root := t.TempDir()
	rt := openServingRuntime(t, root)
	observeMemory(t, rt, "m-idle", "local memory before any remote exists")

	eventsBefore, _ := rt.PendingEvents(0)
	if err := syncWorkerPass(rt, SyncWorkerOptions{ProjectRoot: root}); err != nil {
		t.Fatalf("pass without remotes.json must be a silent no-op: %v", err)
	}
	eventsAfter, _ := rt.PendingEvents(0)
	if len(eventsAfter) != len(eventsBefore) {
		t.Fatalf("no-remote pass must not touch the log: %d -> %d events", len(eventsBefore), len(eventsAfter))
	}
	pending, err := rt.PendingSyncedEvents()
	if err != nil || len(pending) != 1 {
		t.Fatalf("local pending synced event must be untouched: %+v err=%v", pending, err)
	}
}

// I13 second leg: an unreachable remote degrades sync (pass returns a bounded transport error the
// loop logs+swallows) while the local serve path stays fully functional and the material stays
// pending for the next pass.
func TestSyncWorkerSurvivesUnreachableRemote(t *testing.T) {
	root := t.TempDir()
	rt := openServingRuntime(t, root)
	observeMemory(t, rt, "m-offline", "offline memory still governed locally")
	connectRemote(t, root, "http://127.0.0.1:1", "dead-token")

	start := time.Now()
	err := syncWorkerPass(rt, SyncWorkerOptions{ProjectRoot: root, Timeout: 500 * time.Millisecond})
	if err == nil || !strings.Contains(err.Error(), "sync push failed") {
		t.Fatalf("unreachable remote must surface a push transport error, got %v", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("pass must be bounded by the client timeout, took %v", time.Since(start))
	}
	// Local loop unaffected: a further host observe is admitted, and the material stays pending.
	observeMemory(t, rt, "m-offline-2", "second offline memory")
	pending, err := rt.PendingSyncedEvents()
	if err != nil || len(pending) != 2 {
		t.Fatalf("offline pass must leave synced events pending: %+v err=%v", pending, err)
	}
}

// The worker round trip over the LIVE runtime handle: pending local materials push (acked to synced),
// a foreign material pulls and merges through the kernel, the cursor advances, and a second pass is a
// no-op (no duplicates, no echo) — all without a second store opener.
func TestSyncWorkerPushPullRoundTrip(t *testing.T) {
	root := t.TempDir()
	rt := openServingRuntime(t, root)
	memRef := contract.ResourceRef{Kind: "memory", ID: "project"}
	scopes := []contract.ResourceRef{memRef, {Kind: "skill", ID: "project"}}
	endpoint, hub, _ := startHub(t, map[string]contract.ActorID{
		"tok-local": "replica-local@team",
		"tok-other": "replica-other@team",
	}, scopes)
	connectRemote(t, root, endpoint, "tok-local")

	observeMemory(t, rt, "m-rt", "local memory that must reach the hub")
	foreign := foreignMemoryMaterial("dec-foreign-1", "remote-entry-1", "remote memory that must reach this replica")
	if resp, err := hub.Push("replica-other@team", contract.SyncPushRequest{
		ReplicaID: "other-replica", BatchID: "seed", Events: testSyncedEvents(t, foreign),
	}); err != nil || len(resp.Accepted) != 1 {
		t.Fatalf("seed foreign material: %+v err=%v", resp, err)
	}

	if err := syncWorkerPass(rt, SyncWorkerOptions{ProjectRoot: root}); err != nil {
		t.Fatalf("worker pass: %v", err)
	}

	// Push half: the local material is synced (hub verdict mirrored through the live handle).
	if pending, _ := rt.PendingSyncedEvents(); len(pending) != 0 {
		t.Fatalf("push must drain pending synced events, got %+v", pending)
	}
	hubStatus, err := hub.Status("replica-local@team")
	if err != nil || hubStatus.HubEventsReceived != 2 {
		t.Fatalf("hub must hold seed+pushed events: %+v err=%v", hubStatus, err)
	}
	// Pull half: the foreign entry merged into governed memory through the kernel.
	_, fields, err := rt.Resource(memRef)
	if err != nil {
		t.Fatalf("read memory: %v", err)
	}
	content, _ := fields["content"].(string)
	if !strings.Contains(content, "remote memory that must reach this replica") ||
		!strings.Contains(content, "local memory that must reach the hub") {
		t.Fatalf("memory must hold local + imported entries:\n%s", content)
	}

	// Second pass: cursor-idempotent, no duplicate entries, no outbound echo of the import.
	if err := syncWorkerPass(rt, SyncWorkerOptions{ProjectRoot: root}); err != nil {
		t.Fatalf("second worker pass: %v", err)
	}
	if pending, _ := rt.PendingSyncedEvents(); len(pending) != 0 {
		t.Fatalf("import must not create an outbound echo, got %+v", pending)
	}
	_, fields, _ = rt.Resource(memRef)
	content, _ = fields["content"].(string)
	if strings.Count(content, "remote memory that must reach this replica") != 1 {
		t.Fatalf("second pass duplicated the import:\n%s", content)
	}
	if st, _ := hub.Status("replica-local@team"); st.HubEventsReceived != 2 {
		t.Fatalf("second pass must not re-append at the hub: %+v", st)
	}
}

// Co-existence proof for the merged policy (v1.1 #2): the serving runtime carries host rules AND
// sync-import rules; host-agent flow is unaffected (admission + secret-deny behave exactly as
// before), foreign events pass through the principal gates, and the import path works in-process.
func TestServingRuntimeMergesSyncImportWithoutDisturbingHostFlow(t *testing.T) {
	root := t.TempDir()
	rt := openServingRuntime(t, root)
	memRef := contract.ResourceRef{Kind: "memory", ID: "project"}

	// Host flow: a good candidate is admitted...
	observeMemory(t, rt, "m-good", "host fact survives the merged policy")
	v1, fields, err := rt.Resource(memRef)
	if err != nil || v1 == 0 {
		t.Fatalf("host candidate must be admitted: v=%d err=%v", v1, err)
	}
	// ...and the secret-like candidate is still denied (host rule teeth intact under the merge).
	observeMemory(t, rt, "m-secret", "password=hunter2")
	v2, _, _ := rt.Resource(memRef)
	if v2 != v1 {
		t.Fatalf("secret-like candidate must stay denied under the merged policy: v %d -> %d", v1, v2)
	}

	// Import flow on the SAME runtime: a foreign material merges under sync@local.
	if err := importPulledEvents(rt, "hub", testSyncedEvents(t,
		foreignMemoryMaterial("dec-coexist", "remote-coexist", "imported entry coexists"),
	), nil); err != nil {
		t.Fatalf("in-process import: %v", err)
	}
	_, fields, err = rt.Resource(memRef)
	if err != nil {
		t.Fatalf("read memory: %v", err)
	}
	content, _ := fields["content"].(string)
	if !strings.Contains(content, "imported entry coexists") || !strings.Contains(content, "host fact survives the merged policy") {
		t.Fatalf("host + imported entries must coexist:\n%s", content)
	}

	// Host flow still live AFTER an import (no policy poisoning either direction).
	observeMemory(t, rt, "m-after", "host flow still works after import")
	_, fields, _ = rt.Resource(memRef)
	content, _ = fields["content"].(string)
	if !strings.Contains(content, "host flow still works after import") {
		t.Fatalf("host flow must keep working after an import:\n%s", content)
	}
}
