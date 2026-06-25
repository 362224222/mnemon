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

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/policy"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/state"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemonhub"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

const noteImportablePackageSpec = `{"schema_version":1,"name":"note","observed_type":"note.write_candidate.observed",
"proposed_type":"note.write.proposed","resource_kind":"note","items_field":"items",
"fields":[{"name":"text","validators":[{"id":"required","params":{"missing_style":"empty"}},{"id":"safety:unsafe"}]}],
"render":{"content":{"member":"bullet-list","params":{"title":"# Notes","field":"text"}}},
"sync":{"importable":true,"merge":"item-dedup"}}`

// openServingRuntime boots the PRODUCT serving runtime (OpenLocalRuntime = assembled host policy +
// merged sync-import policy) over a standard event host binding — the exact runtime the worker
// operates inside `local run`.
func openServingRuntime(t *testing.T, root string) *runtime.Runtime {
	t.Helper()
	refs := []contract.ResourceRef{{Kind: "progress_digest", ID: "project"}, {Kind: "assignment", ID: "project"}}
	b := access.HostAgentBinding("codex@project", "http://127.0.0.1:8787", refs)
	rt, err := OpenLocalRuntime(filepath.Join(root, runtime.DefaultStorePath), access.LoadedBindings{Bindings: []access.ChannelBinding{b}}, nil, nil)
	if err != nil {
		t.Fatalf("open serving runtime: %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })
	return rt
}

// startHub serves a mnemonhub hub over its own store and returns the endpoint + the hub handles.
func startHub(t *testing.T, principals map[string]contract.ActorID, scopes []contract.ResourceRef) (string, *mnemonhub.Server, *state.Store) {
	t.Helper()
	st, err := state.OpenStore(filepath.Join(t.TempDir(), "hub.db"))
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
	connectRemoteWithDirection(t, root, endpoint, token, "")
}

func connectRemoteWithDirection(t *testing.T, root, endpoint, token, direction string) {
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
	directionField := ""
	if strings.TrimSpace(direction) != "" {
		directionField = fmt.Sprintf(`,"direction":%q`, direction)
	}
	doc := fmt.Sprintf(`{"schema_version":1,"current":"hub","remotes":[{"id":"hub"%s,"endpoint":%q,"credential_ref":%q}]}`, directionField, endpoint, filepath.ToSlash(credRel))
	if err := os.WriteFile(remotesPath, []byte(doc+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func observeProgress(t *testing.T, rt *runtime.Runtime, externalID, content string) {
	t.Helper()
	if _, _, err := rt.API().Ingest("codex@project", contract.ObservationEnvelope{
		ExternalID: externalID,
		Event: contract.Event{Type: "progress_digest.write_candidate.observed", Payload: map[string]any{
			"summary": content,
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

func foreignProgressMaterial(decisionID, itemID, summary string) contract.SyncedEventMaterial {
	fields := map[string]any{
		"content": "# Progress\n- " + summary,
		"items": []any{map[string]any{
			"id": itemID, "summary": summary,
			"actor": "codex@other", "ingest_seq": float64(7),
		}},
	}
	return contract.SyncedEventMaterial{
		OriginReplicaID: "other-replica", LocalDecisionID: decisionID, LocalIngestSeq: 7,
		Actor: "codex@other", ResourceRef: contract.ResourceRef{Kind: "progress_digest", ID: "project"},
		ResourceVersion: 1, FieldsDigest: workerDigest(fields), Fields: fields,
		DecidedAt: "2026-06-12T00:00:00Z", Status: "pending",
	}
}

func foreignNoteMaterial(decisionID, itemID, text string) contract.SyncedEventMaterial {
	fields := map[string]any{
		"content": "# Notes\n- " + text,
		"items": []any{map[string]any{
			"id": itemID, "text": text,
			"actor": "codex@other", "ingest_seq": float64(8),
		}},
	}
	return contract.SyncedEventMaterial{
		OriginReplicaID: "other-replica", LocalDecisionID: decisionID, LocalIngestSeq: 8,
		Actor: "codex@other", ResourceRef: contract.ResourceRef{Kind: "note", ID: "project"},
		ResourceVersion: 1, FieldsDigest: workerDigest(fields), Fields: fields,
		DecidedAt: "2026-06-12T00:00:00Z", Status: "pending",
	}
}

// I13 first leg: with NO remotes.json a worker pass is a strict no-op — zero sync activity, zero
// errors, the local store untouched.
func TestSyncWorkerIdleWithoutRemoteConfig(t *testing.T) {
	root := t.TempDir()
	rt := openServingRuntime(t, root)
	observeProgress(t, rt, "m-idle", "local progress before any remote exists")

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
	observeProgress(t, rt, "m-offline", "offline progress still governed locally")
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
	observeProgress(t, rt, "m-offline-2", "second offline progress")
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
	progressRef := contract.ResourceRef{Kind: "progress_digest", ID: "project"}
	scopes := []contract.ResourceRef{progressRef, {Kind: "assignment", ID: "project"}}
	endpoint, hub, _ := startHub(t, map[string]contract.ActorID{
		"tok-local": "replica-local@team",
		"tok-other": "replica-other@team",
	}, scopes)
	connectRemote(t, root, endpoint, "tok-local")

	observeProgress(t, rt, "m-rt", "local progress that must reach the hub")
	foreign := foreignProgressMaterial("dec-foreign-1", "remote-entry-1", "remote progress that must reach this replica")
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
	// Pull half: the foreign entry merged into governed event state.
	_, fields, err := rt.Resource(progressRef)
	if err != nil {
		t.Fatalf("read progress: %v", err)
	}
	content, _ := fields["content"].(string)
	if !strings.Contains(content, "remote progress that must reach this replica") ||
		!strings.Contains(content, "local progress that must reach the hub") {
		t.Fatalf("progress must hold local + imported entries:\n%s", content)
	}

	// Second pass: cursor-idempotent, no duplicate entries, no outbound echo of the import.
	if err := syncWorkerPass(rt, SyncWorkerOptions{ProjectRoot: root}); err != nil {
		t.Fatalf("second worker pass: %v", err)
	}
	if pending, _ := rt.PendingSyncedEvents(); len(pending) != 0 {
		t.Fatalf("import must not create an outbound echo, got %+v", pending)
	}
	_, fields, _ = rt.Resource(progressRef)
	content, _ = fields["content"].(string)
	if strings.Count(content, "remote progress that must reach this replica") != 1 {
		t.Fatalf("second pass duplicated the import:\n%s", content)
	}
	if st, _ := hub.Status("replica-local@team"); st.HubEventsReceived != 2 {
		t.Fatalf("second pass must not re-append at the hub: %+v", st)
	}
}

func TestSyncWorkerPublishOnlyDoesNotPull(t *testing.T) {
	root := t.TempDir()
	rt := openServingRuntime(t, root)
	progressRef := contract.ResourceRef{Kind: "progress_digest", ID: "project"}
	endpoint, hub, _ := startHub(t, map[string]contract.ActorID{
		"tok-local": "replica-local@team",
		"tok-other": "replica-other@team",
	}, []contract.ResourceRef{progressRef})
	connectRemoteWithDirection(t, root, endpoint, "tok-local", "publish")

	observeProgress(t, rt, "m-publish-only", "publish-only local progress reaches the hub")
	foreign := foreignProgressMaterial("dec-publish-only-foreign", "remote-publish-only", "publish-only must not import this")
	if resp, err := hub.Push("replica-other@team", contract.SyncPushRequest{
		ReplicaID: "other-replica", BatchID: "seed-publish-only", Events: testSyncedEvents(t, foreign),
	}); err != nil || len(resp.Accepted) != 1 {
		t.Fatalf("seed foreign material: %+v err=%v", resp, err)
	}

	if err := syncWorkerPass(rt, SyncWorkerOptions{ProjectRoot: root}); err != nil {
		t.Fatalf("publish-only worker pass: %v", err)
	}
	if pending, _ := rt.PendingSyncedEvents(); len(pending) != 0 {
		t.Fatalf("publish-only pass must push local synced events, got %+v", pending)
	}
	if st, _ := hub.Status("replica-local@team"); st.HubEventsReceived != 2 {
		t.Fatalf("publish-only pass must append local event to hub without duplicate work: %+v", st)
	}
	_, fields, err := rt.Resource(progressRef)
	if err != nil {
		t.Fatalf("read progress: %v", err)
	}
	content, _ := fields["content"].(string)
	if strings.Contains(content, "publish-only must not import this") {
		t.Fatalf("publish-only pass must not pull remote content:\n%s", content)
	}
}

func TestSyncWorkerSubscribeOnlyDoesNotPush(t *testing.T) {
	root := t.TempDir()
	rt := openServingRuntime(t, root)
	progressRef := contract.ResourceRef{Kind: "progress_digest", ID: "project"}
	endpoint, hub, _ := startHub(t, map[string]contract.ActorID{
		"tok-local": "replica-local@team",
		"tok-other": "replica-other@team",
	}, []contract.ResourceRef{progressRef})
	connectRemoteWithDirection(t, root, endpoint, "tok-local", "subscribe")

	observeProgress(t, rt, "m-subscribe-only", "subscribe-only local progress stays pending")
	foreign := foreignProgressMaterial("dec-subscribe-only-foreign", "remote-subscribe-only", "subscribe-only imports this")
	if resp, err := hub.Push("replica-other@team", contract.SyncPushRequest{
		ReplicaID: "other-replica", BatchID: "seed-subscribe-only", Events: testSyncedEvents(t, foreign),
	}); err != nil || len(resp.Accepted) != 1 {
		t.Fatalf("seed foreign material: %+v err=%v", resp, err)
	}

	if err := syncWorkerPass(rt, SyncWorkerOptions{ProjectRoot: root}); err != nil {
		t.Fatalf("subscribe-only worker pass: %v", err)
	}
	if pending, _ := rt.PendingSyncedEvents(); len(pending) != 1 {
		t.Fatalf("subscribe-only pass must not push local synced events, got %+v", pending)
	}
	if st, _ := hub.Status("replica-local@team"); st.HubEventsReceived != 1 {
		t.Fatalf("subscribe-only pass must not append local event to hub: %+v", st)
	}
	_, fields, err := rt.Resource(progressRef)
	if err != nil {
		t.Fatalf("read progress: %v", err)
	}
	content, _ := fields["content"].(string)
	if !strings.Contains(content, "subscribe-only imports this") {
		t.Fatalf("subscribe-only pass must pull remote content:\n%s", content)
	}
}

// Co-existence proof for the merged policy (v1.1 #2): the serving runtime carries host rules AND
// sync-import rules; host-agent flow is unaffected (admission + secret-deny behave exactly as
// before), foreign events pass through the principal gates, and the import path works in-process.
func TestServingRuntimeMergesSyncImportWithoutDisturbingHostFlow(t *testing.T) {
	root := t.TempDir()
	rt := openServingRuntime(t, root)
	progressRef := contract.ResourceRef{Kind: "progress_digest", ID: "project"}

	// Host flow: a good candidate is admitted...
	observeProgress(t, rt, "m-good", "host fact survives the merged policy")
	v1, fields, err := rt.Resource(progressRef)
	if err != nil || v1 == 0 {
		t.Fatalf("host candidate must be admitted: v=%d err=%v", v1, err)
	}
	// ...and the secret-like candidate is still denied (host rule teeth intact under the merge).
	observeProgress(t, rt, "m-secret", "password=hunter2")
	v2, _, _ := rt.Resource(progressRef)
	if v2 != v1 {
		t.Fatalf("secret-like candidate must stay denied under the merged policy: v %d -> %d", v1, v2)
	}

	// Import flow on the SAME runtime: a foreign material merges under sync@local.
	if err := importPulledEvents(rt, "hub", testSyncedEvents(t,
		foreignProgressMaterial("dec-coexist", "remote-coexist", "imported entry coexists"),
	), nil); err != nil {
		t.Fatalf("in-process import: %v", err)
	}
	_, fields, err = rt.Resource(progressRef)
	if err != nil {
		t.Fatalf("read progress: %v", err)
	}
	content, _ := fields["content"].(string)
	if !strings.Contains(content, "imported entry coexists") || !strings.Contains(content, "host fact survives the merged policy") {
		t.Fatalf("host + imported entries must coexist:\n%s", content)
	}

	// Host flow still live AFTER an import (no policy poisoning either direction).
	observeProgress(t, rt, "m-after", "host flow still works after import")
	_, fields, _ = rt.Resource(progressRef)
	content, _ = fields["content"].(string)
	if !strings.Contains(content, "host flow still works after import") {
		t.Fatalf("host flow must keep working after an import:\n%s", content)
	}
}

func TestServingRuntimeImportsExternalKindWithoutLocalLoopEnabled(t *testing.T) {
	root := t.TempDir()
	writeExternalGoalPackage(t, root, "note", noteImportablePackageSpec)
	catalog, err := policy.ResolveRegistry(root, state.DefaultSchemaGuard().Required)
	if err != nil {
		t.Fatalf("resolve catalog: %v", err)
	}
	progressRef := contract.ResourceRef{Kind: "progress_digest", ID: "project"}
	noteRef := contract.ResourceRef{Kind: "note", ID: "project"}
	binding := access.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{progressRef, noteRef})
	binding.AllowedObservedTypes = []string{"progress_digest.write_candidate.observed"}
	rt, err := OpenLocalRuntime(filepath.Join(root, runtime.DefaultStorePath),
		access.LoadedBindings{Bindings: []access.ChannelBinding{binding}},
		[]string{"progress_digest"}, catalog)
	if err != nil {
		t.Fatalf("open serving runtime: %v", err)
	}
	defer rt.Close()

	if err := importPulledEvents(rt, "hub", testSyncedEvents(t,
		foreignNoteMaterial("dec-note", "remote-note", "external note import works"),
	), catalog); err != nil {
		t.Fatalf("in-process external import: %v", err)
	}
	_, fields, err := rt.Resource(noteRef)
	if err != nil {
		t.Fatalf("read note: %v", err)
	}
	if content, _ := fields["content"].(string); !strings.Contains(content, "external note import works") {
		t.Fatalf("external import missing note content:\n%s", content)
	}
}
