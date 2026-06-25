package runtime

import (
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
)

func TestLocalProgressCandidateCreatesSyncPendingEvent(t *testing.T) {
	ref := contract.ResourceRef{Kind: "progress_digest", ID: "project"}
	binding := access.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	binding.AllowedObservedTypes = []string{"progress_digest.write_candidate.observed"}
	rt, err := OpenRuntime(filepath.Join(t.TempDir(), "local.db"), localRuntimeConfigT([]access.ChannelBinding{binding}))
	if err != nil {
		t.Fatalf("open local runtime: %v", err)
	}
	defer rt.Close()
	srv := httptest.NewServer(NewRuntimeHandler(rt, access.HeaderAuthenticator{}))
	defer srv.Close()

	client := access.NewClient(srv.URL, "codex@project")
	if _, err := client.IngestObserve("codex@project", contract.ObservationEnvelope{
		ExternalID: "progress-release-checklist",
		Event: contract.Event{Type: "progress_digest.write_candidate.observed", Payload: map[string]any{
			"summary": "Check tests, build, and release notes before shipping.",
		}},
	}); err != nil {
		t.Fatalf("observe progress candidate: %v", err)
	}

	proj, err := client.PullPresentationView("codex@project", contract.Subscription{Actor: "codex@project"})
	if err != nil {
		t.Fatalf("pull progress projection: %v", err)
	}
	if len(proj.Content) != 1 {
		t.Fatalf("expected progress content, got %+v", proj.Content)
	}
	fields := proj.Content[0].Fields
	if content, _ := fields["content"].(string); !strings.Contains(content, "Check tests") {
		t.Fatalf("progress resource must render content, got %+v", fields)
	}
	items, ok := fields["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected one progress item, got %+v", fields["items"])
	}
	pending, err := rt.store.PendingSyncedEvents()
	if err != nil {
		t.Fatalf("pending synced events: %v", err)
	}
	if len(pending) != 1 || pending[0].Event.Subject != "progress_digest/project" {
		t.Fatalf("progress event must become pending synced event, got %+v", pending)
	}
}

func TestLocalProgressChangesAppendItems(t *testing.T) {
	ref := contract.ResourceRef{Kind: "progress_digest", ID: "project"}
	binding := access.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	binding.AllowedObservedTypes = []string{"progress_digest.write_candidate.observed"}
	rt, err := OpenRuntime(filepath.Join(t.TempDir(), "local.db"), localRuntimeConfigT([]access.ChannelBinding{binding}))
	if err != nil {
		t.Fatalf("open local runtime: %v", err)
	}
	defer rt.Close()
	srv := httptest.NewServer(NewRuntimeHandler(rt, access.HeaderAuthenticator{}))
	defer srv.Close()
	client := access.NewClient(srv.URL, "codex@project")

	for _, item := range []struct {
		externalID string
		summary    string
	}{
		{"progress-release-active", "Initial active manifest."},
		{"progress-release-stale", "Approved lifecycle change to stale."},
	} {
		if _, err := client.IngestObserve("codex@project", contract.ObservationEnvelope{
			ExternalID: item.externalID,
			Event: contract.Event{Type: "progress_digest.write_candidate.observed", Payload: map[string]any{
				"summary": item.summary,
			}},
		}); err != nil {
			t.Fatalf("observe %s: %v", item.externalID, err)
		}
	}

	proj, err := client.PullPresentationView("codex@project", contract.Subscription{Actor: "codex@project"})
	if err != nil {
		t.Fatalf("pull progress projection: %v", err)
	}
	items, ok := proj.Content[0].Fields["items"].([]any)
	if !ok || len(items) != 2 {
		t.Fatalf("progress changes must append two items, got %+v", proj.Content[0].Fields)
	}
	first := items[0].(map[string]any)
	second := items[1].(map[string]any)
	if first["summary"] != "Initial active manifest." || second["summary"] != "Approved lifecycle change to stale." {
		t.Fatalf("items must preserve progress history, got %+v", items)
	}
}
