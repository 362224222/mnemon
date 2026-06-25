package githubbackend

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemonhub/exchange"
)

var progressRef = contract.ResourceRef{Kind: "progress_digest", ID: "project"}

func TestGitHubBackendFakePushPublishesSyncedEnvelope(t *testing.T) {
	store, backend := newFakeBackend(t, "mnemon/agent-a", progressRef)
	env := githubBackendTestEnvelope(t, "replica-a", "dec-a", progressRef, map[string]any{"content": "published progress"})

	resp, err := backend.SyncPush(contract.SyncPushRequest{ReplicaID: "replica-a", BatchID: "batch-a", Events: []eventmodel.EventEnvelope{env}})
	if err != nil {
		t.Fatalf("sync push: %v", err)
	}
	if len(resp.Accepted) != 1 || len(resp.Rejected) != 0 || len(resp.Conflicts) != 0 {
		t.Fatalf("push resp = %+v, want one accepted", resp)
	}
	list, err := store.ListEvents(context.Background(), "mnemon/agent-a", exchange.PublicationEventRoot, "")
	if err != nil {
		t.Fatalf("list publication events: %v", err)
	}
	if len(list.Events) != 1 {
		t.Fatalf("publication store events = %+v, want one", list.Events)
	}

	replayed, err := backend.SyncPush(contract.SyncPushRequest{ReplicaID: "replica-a", BatchID: "batch-a", Events: []eventmodel.EventEnvelope{env}})
	if err != nil {
		t.Fatalf("repeat sync push: %v", err)
	}
	if len(replayed.Accepted) != 1 || len(replayed.Conflicts) != 0 {
		t.Fatalf("repeat push must be idempotent accepted, got %+v", replayed)
	}
}

func TestGitHubBackendFakePushRejectsInvalidPhase(t *testing.T) {
	_, backend := newFakeBackend(t, "mnemon/agent-a", progressRef)
	env := githubBackendTestEnvelope(t, "replica-a", "dec-a", progressRef, map[string]any{"content": "bad phase"})
	env.Phase = eventmodel.PhaseAccepted

	resp, err := backend.SyncPush(contract.SyncPushRequest{ReplicaID: "replica-a", BatchID: "batch-a", Events: []eventmodel.EventEnvelope{env}})
	if err != nil {
		t.Fatalf("sync push invalid phase: %v", err)
	}
	if len(resp.Rejected) != 1 || !strings.Contains(resp.Rejected[0].Diagnostic, "phase") {
		t.Fatalf("invalid phase must be rejected with diagnostic, got %+v", resp)
	}
}

func TestGitHubBackendFakePushDetectsSameKeyDifferentBody(t *testing.T) {
	_, backend := newFakeBackend(t, "mnemon/agent-a", progressRef)
	env := githubBackendTestEnvelope(t, "replica-a", "dec-a", progressRef, map[string]any{"content": "same key"})
	if resp, err := backend.SyncPush(contract.SyncPushRequest{ReplicaID: "replica-a", BatchID: "batch-a", Events: []eventmodel.EventEnvelope{env}}); err != nil || len(resp.Accepted) != 1 {
		t.Fatalf("seed push: resp=%+v err=%v", resp, err)
	}
	changedBody := env
	changedBody.Event.CorrelationID = "changed-body"
	resp, err := backend.SyncPush(contract.SyncPushRequest{ReplicaID: "replica-a", BatchID: "batch-b", Events: []eventmodel.EventEnvelope{changedBody}})
	if err != nil {
		t.Fatalf("conflict push: %v", err)
	}
	if len(resp.Conflicts) != 1 || !strings.Contains(resp.Conflicts[0].Diagnostic, "different content") {
		t.Fatalf("same key different body must conflict, got %+v", resp)
	}
}

func TestGitHubBackendFakePullReturnsValidEventsAndSkipsOwnOrigin(t *testing.T) {
	store, backend := newFakeBackend(t, "mnemon/agent-b", progressRef)
	foreign := githubBackendTestEnvelope(t, "replica-b", "dec-foreign", progressRef, map[string]any{"content": "foreign progress"})
	own := githubBackendTestEnvelope(t, "replica-a", "dec-own", progressRef, map[string]any{"content": "own progress"})
	putStoredEvent(t, store, "mnemon/agent-b", foreign)
	putStoredEvent(t, store, "mnemon/agent-b", own)

	resp, err := backend.SyncPull(contract.SyncPullRequest{ReplicaID: "replica-a"})
	if err != nil {
		t.Fatalf("sync pull: %v", err)
	}
	if len(resp.Events) != 1 || resp.Events[0].Event.ID != foreign.Event.ID || len(resp.Diagnostics) != 0 || resp.NextCursor != "2" {
		t.Fatalf("pull resp = %+v, want only foreign event and cursor 2", resp)
	}
	again, err := backend.SyncPull(contract.SyncPullRequest{ReplicaID: "replica-a", RemoteCursor: resp.NextCursor})
	if err != nil {
		t.Fatalf("sync pull after cursor: %v", err)
	}
	if len(again.Events) != 0 || len(again.Diagnostics) != 0 || again.NextCursor != "2" {
		t.Fatalf("cursor pull must be empty at cursor 2, got %+v", again)
	}
}

func TestGitHubBackendFakePullReturnsDiagnostics(t *testing.T) {
	store, backend := newFakeBackend(t, "mnemon/agent-b", progressRef)
	invalidPhase := githubBackendTestEnvelope(t, "replica-b", "dec-invalid-phase", progressRef, map[string]any{"content": "invalid phase"})
	invalidPhase.Phase = eventmodel.PhaseAccepted
	badDigest := githubBackendTestEnvelope(t, "replica-b", "dec-bad-digest", progressRef, map[string]any{"content": "bad digest"})
	badDigest.Meta["digest"] = "wrong"
	outOfScope := githubBackendTestEnvelope(t, "replica-b", "dec-out-scope", contract.ResourceRef{Kind: "assignment", ID: "project"}, map[string]any{"content": "assignment"})
	putStoredEvent(t, store, "mnemon/agent-b", invalidPhase)
	putStoredEvent(t, store, "mnemon/agent-b", badDigest)
	putStoredEvent(t, store, "mnemon/agent-b", outOfScope)

	resp, err := backend.SyncPull(contract.SyncPullRequest{ReplicaID: "replica-a"})
	if err != nil {
		t.Fatalf("sync pull diagnostics: %v", err)
	}
	if len(resp.Events) != 0 || len(resp.Diagnostics) != 3 {
		t.Fatalf("pull diagnostics resp = %+v, want three diagnostics", resp)
	}
	joined := diagnosticsText(resp.Diagnostics)
	for _, want := range []string{"phase", "fields_digest", "outside configured publication scope"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("diagnostics missing %q in %s", want, joined)
		}
	}
}

func newFakeBackend(t *testing.T, branch string, scopes ...contract.ResourceRef) (*exchange.MemoryPublicationStore, *Backend) {
	t.Helper()
	store, err := exchange.NewMemoryPublicationStore(branch)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := New(Config{Store: store, Repo: "mnemon-dev/mnemon-teamwork-example", Branch: branch, Scopes: scopes})
	if err != nil {
		t.Fatal(err)
	}
	return store, backend
}

func putStoredEvent(t *testing.T, store *exchange.MemoryPublicationStore, branch string, env eventmodel.EventEnvelope) {
	t.Helper()
	path, err := exchange.PublicationEventPath(githubBackendPathEnvelope(env))
	if err != nil {
		t.Fatalf("publication path: %v", err)
	}
	body, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutEvent(context.Background(), branch, path, body); err != nil {
		t.Fatalf("put stored event: %v", err)
	}
}

func githubBackendPathEnvelope(env eventmodel.EventEnvelope) eventmodel.EventEnvelope {
	if env.Phase == eventmodel.PhaseSynced {
		return env
	}
	out := env
	out.Phase = eventmodel.PhaseSynced
	return out
}

func githubBackendTestEnvelope(t *testing.T, origin, decisionID string, ref contract.ResourceRef, fields map[string]any) eventmodel.EventEnvelope {
	t.Helper()
	env, err := contract.SyncedEventEnvelopeFromMaterial(contract.SyncedEventMaterial{
		OriginReplicaID: origin,
		LocalDecisionID: decisionID,
		LocalIngestSeq:  7,
		Actor:           "codex@project",
		ResourceRef:     ref,
		ResourceVersion: 1,
		FieldsDigest:    syncedFieldsDigest(fields),
		Fields:          fields,
		DecidedAt:       "2026-06-12T00:00:00Z",
		Status:          "pending",
	})
	if err != nil {
		t.Fatalf("synced event envelope: %v", err)
	}
	return env
}

func diagnosticsText(items []contract.EventExchangeResult) string {
	var b strings.Builder
	for _, item := range items {
		b.WriteString(item.Diagnostic)
		b.WriteByte('\n')
	}
	return b.String()
}
