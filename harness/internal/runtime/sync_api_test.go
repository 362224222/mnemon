package runtime

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
)

func TestRemoteSyncPushIsIdempotentAndAuthenticated(t *testing.T) {
	ref := contract.ResourceRef{Kind: "memory", ID: "project"}
	host := access.HostAgentBinding("codex@project", "http://localhost:8787", []contract.ResourceRef{ref})
	replica := access.ReplicaAgentBinding("replica@project", "http://localhost:8787", []contract.ResourceRef{ref})
	rt, err := OpenRuntime(filepath.Join(t.TempDir(), "remote.db"), RuntimeConfig{
		Bindings: []access.ChannelBinding{host, replica},
		Subs:     access.SubsFromBindings([]access.ChannelBinding{host, replica}),
	})
	if err != nil {
		t.Fatalf("open remote runtime: %v", err)
	}
	defer rt.Close()
	srv := newTokenRuntimeServer(t, rt, map[string]contract.ActorID{
		"host-token":    "codex@project",
		"replica-token": "replica@project",
	})
	defer srv.Close()

	material := syncAPITestMaterial("local-a", "dec-1", ref, map[string]any{"content": "remote accepted memory"})
	replicaClient := access.NewClientWithToken(srv.URL, "replica-token")
	first, err := replicaClient.SyncPush(contract.SyncPushRequest{
		ReplicaID: "local-a",
		BatchID:   "batch-1",
		Events:    syncAPITestEvents(t, material),
	})
	if err != nil {
		t.Fatalf("first sync push: %v", err)
	}
	if len(first.Accepted) != 1 || first.Accepted[0].Status != "accepted" {
		t.Fatalf("first push must accept the material, got %+v", first)
	}

	duplicate, err := replicaClient.SyncPush(contract.SyncPushRequest{
		ReplicaID: "local-a",
		BatchID:   "batch-1",
		Events:    syncAPITestEvents(t, material),
	})
	if err != nil {
		t.Fatalf("duplicate sync push: %v", err)
	}
	if !reflect.DeepEqual(first.Accepted, duplicate.Accepted) || len(duplicate.Conflicts) != 0 || len(duplicate.Rejected) != 0 {
		t.Fatalf("duplicate push must return the same ack without conflicts: first=%+v duplicate=%+v", first, duplicate)
	}

	mutated := syncAPITestMaterial("local-a", "dec-1", ref, map[string]any{"content": "same idempotency key, different body"})
	conflicted, err := replicaClient.SyncPush(contract.SyncPushRequest{
		ReplicaID: "local-a",
		BatchID:   "batch-2",
		Events:    syncAPITestEvents(t, mutated),
	})
	if err != nil {
		t.Fatalf("conflicting duplicate sync push: %v", err)
	}
	if len(conflicted.Conflicts) != 1 || !strings.Contains(conflicted.Conflicts[0].Diagnostic, "idempotency key") {
		t.Fatalf("changed duplicate must be a protocol conflict, got %+v", conflicted)
	}

	if _, err := replicaClient.SyncPush(contract.SyncPushRequest{
		ReplicaID: "forged-local-id",
		BatchID:   "batch-forged",
		Events:    syncAPITestEvents(t, material),
	}); err == nil {
		t.Fatalf("forged request replica_id must be rejected instead of trusted")
	}

	hostClient := access.NewClientWithToken(srv.URL, "host-token")
	if _, err := hostClient.SyncPush(contract.SyncPushRequest{
		ReplicaID: "local-a",
		BatchID:   "host-batch",
		Events:    syncAPITestEvents(t, material),
	}); err == nil {
		t.Fatalf("host-agent credential must not call sync endpoints")
	}
	if _, _, err := replicaClient.Ingest("replica@project", contract.ObservationEnvelope{
		ExternalID: "replica-observe",
		Event: contract.Event{
			Type:    "progress_digest.write_candidate.observed",
			Payload: runtimeR2Progress("replica should not be able to submit host observations"),
		},
	}); err == nil {
		t.Fatalf("replica-agent credential must not call Agent Integration observe endpoints")
	}
}

func TestRemoteSyncPushRejectsBadCommitsWithDiagnostics(t *testing.T) {
	ref := contract.ResourceRef{Kind: "memory", ID: "project"}
	replica := access.ReplicaAgentBinding("replica@project", "http://localhost:8787", []contract.ResourceRef{ref})
	rt, err := OpenRuntime(filepath.Join(t.TempDir(), "remote.db"), RuntimeConfig{
		Bindings: []access.ChannelBinding{replica},
		Subs:     access.SubsFromBindings([]access.ChannelBinding{replica}),
	})
	if err != nil {
		t.Fatalf("open remote runtime: %v", err)
	}
	defer rt.Close()
	srv := newTokenRuntimeServer(t, rt, map[string]contract.ActorID{"replica-token": "replica@project"})
	defer srv.Close()

	bad := syncAPITestMaterial("local-a", "dec-bad", ref, map[string]any{"content": "bad digest"})
	bad.FieldsDigest = "wrong"
	resp, err := access.NewClientWithToken(srv.URL, "replica-token").SyncPush(contract.SyncPushRequest{
		ReplicaID: "local-a",
		BatchID:   "batch-bad",
		Events:    syncAPITestEvents(t, bad),
	})
	if err != nil {
		t.Fatalf("bad material should return diagnostics, not transport failure: %v", err)
	}
	if len(resp.Rejected) != 1 || !strings.Contains(resp.Rejected[0].Diagnostic, "fields_digest") {
		t.Fatalf("bad material must be rejected with a diagnostic, got %+v", resp)
	}
}

func newTokenRuntimeServer(t *testing.T, rt *Runtime, tokens map[string]contract.ActorID) *httptest.Server {
	t.Helper()
	return httptest.NewServer(NewRuntimeHandler(rt, access.TokenAuthenticator{Tokens: tokens}))
}

func syncAPITestMaterial(replicaID, decisionID string, ref contract.ResourceRef, fields map[string]any) contract.SyncedEventMaterial {
	return contract.SyncedEventMaterial{
		OriginReplicaID: replicaID,
		LocalDecisionID: decisionID,
		LocalIngestSeq:  1,
		Actor:           "codex@project",
		ResourceRef:     ref,
		ResourceVersion: 1,
		FieldsDigest:    syncAPITestDigest(fields),
		Fields:          fields,
		DecidedAt:       "2026-06-06T00:00:00Z",
		Status:          "pending",
	}
}

func syncAPITestDigest(fields map[string]any) string {
	b, _ := json.Marshal(fields)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func syncAPITestEvents(t *testing.T, materials ...contract.SyncedEventMaterial) []eventmodel.EventEnvelope {
	t.Helper()
	out := make([]eventmodel.EventEnvelope, 0, len(materials))
	for _, material := range materials {
		env, err := contract.SyncedEventEnvelopeFromMaterial(material)
		if err != nil {
			t.Fatalf("synced event fixture: %v", err)
		}
		out = append(out, env)
	}
	return out
}

// The sync path's scope clamp had ZERO test coverage while being the only pre-SQL enforcement on
// that path. Pin the ClampRefs semantics here: in-scope narrowing passes, out-of-scope is denied,
// empty-requested defaults to the binding scope, and — the deliberate tightening — an empty-scope
// replica binding now denies explicit refs instead of passing them through.
func TestClampSyncScopesEnforcesBindingScope(t *testing.T) {
	mem := contract.ResourceRef{Kind: "memory", ID: "project"}
	skill := contract.ResourceRef{Kind: "skill", ID: "project"}
	b := access.ReplicaAgentBinding("replica@peer", "http://x", []contract.ResourceRef{mem, skill})

	if got, err := clampSyncScopes(b, []contract.ResourceRef{mem}); err != nil || len(got) != 1 || got[0] != mem {
		t.Fatalf("in-scope narrowing must pass: %v err=%v", got, err)
	}
	if _, err := clampSyncScopes(b, []contract.ResourceRef{{Kind: "note", ID: "project"}}); err == nil {
		t.Fatal("an out-of-scope sync ref must be denied")
	}
	if got, err := clampSyncScopes(b, nil); err != nil || len(got) != 2 {
		t.Fatalf("empty requested must default to the binding scope: %v err=%v", got, err)
	}
	unscoped := access.ReplicaAgentBinding("replica@peer", "http://x", nil)
	if _, err := clampSyncScopes(unscoped, []contract.ResourceRef{mem}); err == nil {
		t.Fatal("an empty-scope replica binding must deny explicit refs (fail closed)")
	}
}

// HIGH-1 / LOW-9: an EMPTY-scope replica binding must be fully fail-closed on the co-hosted runtime
// hub — Pull, Push, AND Status are all rejected (no grant), because an empty grant scope would reach
// RemoteSyncedEventsAfter's serve-all SQL and bypass scope authorization (the standalone mnemon-hub
// path fail-closes empty scope at replicas.json load; this pins the dual-form parity on the runtime
// side). A scoped replica binding works; an in-scope push is accepted; an empty-scope push is fully
// rejected (the push==subscribe-scope contract).
func TestEmptyScopeReplicaBindingFailsClosed(t *testing.T) {
	mem := contract.ResourceRef{Kind: "memory", ID: "project"}

	scoped := access.ReplicaAgentBinding("scoped@peer", "http://x", []contract.ResourceRef{mem})
	empty := access.ReplicaAgentBinding("empty@peer", "http://x", nil)
	rt, err := OpenRuntime(filepath.Join(t.TempDir(), "remote.db"), RuntimeConfig{
		Bindings: []access.ChannelBinding{scoped, empty},
		Subs:     access.SubsFromBindings([]access.ChannelBinding{scoped, empty}),
	})
	if err != nil {
		t.Fatalf("open remote runtime: %v", err)
	}
	defer rt.Close()

	// The empty-scope replica is rejected on every sync verb (fail closed before any serve-all SQL).
	if _, err := rt.SyncPull("empty@peer", contract.SyncPullRequest{ReplicaID: "local-empty"}); err == nil {
		t.Fatal("empty-scope replica Pull must be rejected (would otherwise serve the entire hub log)")
	}
	if _, err := rt.SyncStatus("empty@peer"); err == nil {
		t.Fatal("empty-scope replica Status must be rejected")
	}
	emptyCommit := syncAPITestMaterial("local-empty", "dec-e", mem, map[string]any{"content": "empty-scope push"})
	if _, err := rt.SyncPush("empty@peer", contract.SyncPushRequest{
		ReplicaID: "local-empty", BatchID: "be", Events: syncAPITestEvents(t, emptyCommit),
	}); err == nil {
		t.Fatal("empty-scope replica Push must be fully rejected (push == subscribe scope)")
	}

	// The scoped replica still works end to end: an in-scope material is accepted, then served.
	inScope := syncAPITestMaterial("local-scoped", "dec-s", mem, map[string]any{"content": "in-scope push"})
	pushResp, err := rt.SyncPush("scoped@peer", contract.SyncPushRequest{
		ReplicaID: "local-scoped", BatchID: "bs", Events: syncAPITestEvents(t, inScope),
	})
	if err != nil || len(pushResp.Accepted) != 1 {
		t.Fatalf("scoped replica in-scope push must be accepted: %+v err=%v", pushResp, err)
	}
	if _, err := rt.SyncStatus("scoped@peer"); err != nil {
		t.Fatalf("scoped replica Status must work: %v", err)
	}
}
