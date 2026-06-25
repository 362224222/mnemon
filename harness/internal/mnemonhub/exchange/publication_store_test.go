package exchange

import (
	"context"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
)

func TestPublicationStoreEventPathDeterministic(t *testing.T) {
	env := publicationTestEnvelope(t, "dec-a", "remote-entry-a", "same event")
	path1, err := PublicationEventPath(env)
	if err != nil {
		t.Fatalf("event path: %v", err)
	}
	path2, err := PublicationEventPath(env)
	if err != nil {
		t.Fatalf("event path again: %v", err)
	}
	if path1 != path2 {
		t.Fatalf("event path must be deterministic: %q != %q", path1, path2)
	}
	if !strings.HasPrefix(path1, PublicationEventRoot+"/replica-a/") || !strings.HasSuffix(path1, ".json") {
		t.Fatalf("event path must stay under publication event root, got %q", path1)
	}

	changed := publicationTestEnvelope(t, "dec-b", "remote-entry-b", "different event")
	changedPath, err := PublicationEventPath(changed)
	if err != nil {
		t.Fatalf("changed event path: %v", err)
	}
	if changedPath == path1 {
		t.Fatalf("different event material must produce a different event path: %q", changedPath)
	}
}

func TestPublicationStorePutEventIsIdempotentAndConflictAware(t *testing.T) {
	ctx := context.Background()
	store, err := NewMemoryPublicationStore("mnemon/agent-a")
	if err != nil {
		t.Fatal(err)
	}
	path := PublicationEventRoot + "/replica-a/event-a.json"

	first, err := store.PutEvent(ctx, "mnemon/agent-a", path, []byte(`{"id":"a"}`))
	if err != nil {
		t.Fatalf("first put: %v", err)
	}
	if !first.Created || first.ExistsSame || first.Conflict {
		t.Fatalf("first put result = %+v, want created", first)
	}
	same, err := store.PutEvent(ctx, "mnemon/agent-a", path, []byte(`{"id":"a"}`))
	if err != nil {
		t.Fatalf("same put: %v", err)
	}
	if !same.ExistsSame || same.Created || same.Conflict {
		t.Fatalf("same put result = %+v, want exists_same", same)
	}
	conflict, err := store.PutEvent(ctx, "mnemon/agent-a", path, []byte(`{"id":"different"}`))
	if err != nil {
		t.Fatalf("conflict put: %v", err)
	}
	if !conflict.Conflict || conflict.Created || conflict.ExistsSame {
		t.Fatalf("conflict put result = %+v, want conflict", conflict)
	}
	body, err := store.ReadFile(ctx, "mnemon/agent-a", path)
	if err != nil {
		t.Fatalf("read event file: %v", err)
	}
	if string(body) != `{"id":"a"}` {
		t.Fatalf("conflict put must not overwrite existing body: %s", body)
	}
}

func TestPublicationStoreListEventsAfterCursor(t *testing.T) {
	ctx := context.Background()
	store, err := NewMemoryPublicationStore("mnemon/agent-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutEvent(ctx, "mnemon/agent-a", PublicationEventRoot+"/replica-a/event-a.json", []byte("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutEvent(ctx, "mnemon/agent-a", PublicationEventRoot+"/replica-a/event-b.json", []byte("b")); err != nil {
		t.Fatal(err)
	}

	all, err := store.ListEvents(ctx, "mnemon/agent-a", PublicationEventRoot, "")
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(all.Events) != 2 || all.Events[0].Path > all.Events[1].Path || all.NextCursor != "2" {
		t.Fatalf("list all = %+v, want two ordered events with cursor 2", all)
	}
	afterFirst, err := store.ListEvents(ctx, "mnemon/agent-a", PublicationEventRoot, all.Events[0].Cursor)
	if err != nil {
		t.Fatalf("list after first: %v", err)
	}
	if len(afterFirst.Events) != 1 || afterFirst.Events[0].Path != PublicationEventRoot+"/replica-a/event-b.json" || afterFirst.NextCursor != "2" {
		t.Fatalf("list after first = %+v, want event-b only", afterFirst)
	}
	empty, err := store.ListEvents(ctx, "mnemon/agent-a", PublicationEventRoot, afterFirst.NextCursor)
	if err != nil {
		t.Fatalf("list after next cursor: %v", err)
	}
	if len(empty.Events) != 0 || empty.NextCursor != "2" {
		t.Fatalf("list after next cursor = %+v, want no events cursor 2", empty)
	}
}

func TestPublicationStoreRejectsUnsupportedBranchAndPath(t *testing.T) {
	ctx := context.Background()
	store, err := NewMemoryPublicationStore("mnemon/agent-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutEvent(ctx, "mnemon/agent-b", PublicationEventRoot+"/replica-a/event-a.json", []byte("a")); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("unsupported branch must fail closed, got %v", err)
	}
	if _, err := store.PutEvent(ctx, "refs/heads/main", PublicationEventRoot+"/replica-a/event-a.json", []byte("a")); err == nil || !strings.Contains(err.Error(), "outside the mnemon namespace") {
		t.Fatalf("non-publication branch must fail closed, got %v", err)
	}
	if _, err := store.PutEvent(ctx, "mnemon/agent-a", ".mnemonhub/v1/../events/event-a.json", []byte("a")); err == nil || !strings.Contains(err.Error(), "invalid") {
		t.Fatalf("escaping event path must fail closed, got %v", err)
	}
	if _, err := store.PutEvent(ctx, "mnemon/agent-a", ".mnemon/reports/run.json", []byte("a")); err == nil || !strings.Contains(err.Error(), "must be under") {
		t.Fatalf("non-event PutEvent path must fail closed, got %v", err)
	}
	if err := store.WriteFile(ctx, "mnemon/agent-a", PublicationEventRoot+"/replica-a/event-a.json", []byte("a")); err == nil || !strings.Contains(err.Error(), "PutEvent") {
		t.Fatalf("WriteFile must not write event paths, got %v", err)
	}
}

func TestPublicationStoreWriteReadFile(t *testing.T) {
	ctx := context.Background()
	store, err := NewMemoryPublicationStore("mnemon/team")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteFile(ctx, "mnemon/team", ".mnemon/team.json", []byte(`{"schema_version":1}`)); err != nil {
		t.Fatalf("write team manifest: %v", err)
	}
	body, err := store.ReadFile(ctx, "mnemon/team", ".mnemon/team.json")
	if err != nil {
		t.Fatalf("read team manifest: %v", err)
	}
	if string(body) != `{"schema_version":1}` {
		t.Fatalf("read body = %s", body)
	}
}

func publicationTestEnvelope(t *testing.T, decisionID, entryID, summary string) eventmodel.EventEnvelope {
	t.Helper()
	fields := map[string]any{
		"content": "# Progress\n- " + summary,
		"items": []any{map[string]any{
			"id":         entryID,
			"summary":    summary,
			"actor":      "codex@other",
			"ingest_seq": float64(7),
		}},
	}
	env, err := contract.SyncedEventEnvelopeFromMaterial(contract.SyncedEventMaterial{
		OriginReplicaID: "replica-a",
		LocalDecisionID: decisionID,
		LocalIngestSeq:  7,
		Actor:           "codex@other",
		ResourceRef:     contract.ResourceRef{Kind: "progress_digest", ID: "project"},
		ResourceVersion: 1,
		FieldsDigest:    syncTestDigestForPublication(fields),
		Fields:          fields,
		DecidedAt:       "2026-06-12T00:00:00Z",
		Status:          "pending",
	})
	if err != nil {
		t.Fatalf("synced event envelope: %v", err)
	}
	return env
}

func syncTestDigestForPublication(fields map[string]any) string {
	return "digest-" + fields["content"].(string)
}
