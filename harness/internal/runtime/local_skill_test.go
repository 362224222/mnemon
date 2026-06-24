package runtime

import (
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/capability"
	"github.com/mnemon-dev/mnemon/harness/internal/channel"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

func TestLocalSkillCandidateCreatesSyncPendingDeclaration(t *testing.T) {
	ref := contract.ResourceRef{Kind: "skill", ID: "project"}
	binding := channel.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	binding.AllowedObservedTypes = []string{capability.SkillWriteCandidateObserved}
	rt, err := OpenRuntime(filepath.Join(t.TempDir(), "local.db"), localRuntimeConfigT([]channel.ChannelBinding{binding}))
	if err != nil {
		t.Fatalf("open local runtime: %v", err)
	}
	defer rt.Close()
	srv := httptest.NewServer(NewRuntimeHandler(rt, channel.HeaderAuthenticator{}))
	defer srv.Close()

	client := channel.NewClient(srv.URL, "codex@project")
	if _, err := client.IngestObserve("codex@project", contract.ObservationEnvelope{
		ExternalID: "skill-declare-release-checklist",
		Event: contract.Event{Type: capability.SkillWriteCandidateObserved, Payload: map[string]any{
			"skill_id":   "release-checklist",
			"name":       "release-checklist",
			"status":     "active",
			"content":    "Check tests, build, and release notes before shipping.",
			"source":     "test",
			"confidence": "high",
		}},
	}); err != nil {
		t.Fatalf("observe skill candidate: %v", err)
	}

	proj, err := client.PullEventView("codex@project", contract.Subscription{Actor: "codex@project"})
	if err != nil {
		t.Fatalf("pull skill projection: %v", err)
	}
	if len(proj.Content) != 1 {
		t.Fatalf("expected skill content, got %+v", proj.Content)
	}
	fields := proj.Content[0].Fields
	if fields["name"] != "project" {
		t.Fatalf("skill resource must carry project aggregate name, got %+v", fields)
	}
	decls, ok := fields["declarations"].([]any)
	if !ok || len(decls) != 1 {
		t.Fatalf("expected one skill declaration, got %+v", fields["declarations"])
	}
	decl, ok := decls[0].(map[string]any)
	if !ok || decl["skill_id"] != "release-checklist" || decl["status"] != "active" {
		t.Fatalf("unexpected declaration: %+v", decls[0])
	}
	pending, err := rt.store.PendingSyncedEvents()
	if err != nil {
		t.Fatalf("pending synced events: %v", err)
	}
	if len(pending) != 1 || pending[0].Event.Subject != "skill/project" {
		t.Fatalf("skill declaration must become pending synced event, got %+v", pending)
	}
}

func TestLocalSkillLifecycleChangesAppendDeclarations(t *testing.T) {
	ref := contract.ResourceRef{Kind: "skill", ID: "project"}
	binding := channel.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	binding.AllowedObservedTypes = []string{capability.SkillWriteCandidateObserved}
	rt, err := OpenRuntime(filepath.Join(t.TempDir(), "local.db"), localRuntimeConfigT([]channel.ChannelBinding{binding}))
	if err != nil {
		t.Fatalf("open local runtime: %v", err)
	}
	defer rt.Close()
	srv := httptest.NewServer(NewRuntimeHandler(rt, channel.HeaderAuthenticator{}))
	defer srv.Close()
	client := channel.NewClient(srv.URL, "codex@project")

	for _, item := range []struct {
		externalID string
		status     string
		content    string
	}{
		{"skill-release-active", "active", "Initial active manifest."},
		{"skill-release-stale", "stale", "Approved lifecycle change to stale."},
	} {
		if _, err := client.IngestObserve("codex@project", contract.ObservationEnvelope{
			ExternalID: item.externalID,
			Event: contract.Event{Type: capability.SkillWriteCandidateObserved, Payload: map[string]any{
				"skill_id":   "release-checklist",
				"name":       "release-checklist",
				"status":     item.status,
				"content":    item.content,
				"source":     "test",
				"confidence": "high",
			}},
		}); err != nil {
			t.Fatalf("observe %s: %v", item.status, err)
		}
	}

	proj, err := client.PullEventView("codex@project", contract.Subscription{Actor: "codex@project"})
	if err != nil {
		t.Fatalf("pull skill projection: %v", err)
	}
	decls, ok := proj.Content[0].Fields["declarations"].([]any)
	if !ok || len(decls) != 2 {
		t.Fatalf("skill lifecycle changes must append two declarations, got %+v", proj.Content[0].Fields)
	}
	first := decls[0].(map[string]any)
	second := decls[1].(map[string]any)
	if first["status"] != "active" || second["status"] != "stale" {
		t.Fatalf("declarations must preserve lifecycle history, got %+v", decls)
	}
}
