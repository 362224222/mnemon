package app

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemonhub/exchange"
	githubbackend "github.com/mnemon-dev/mnemon/harness/internal/mnemonhub/exchange/backend/github"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

func TestSyncGitHubFakeFiveMnemondPublicationMesh(t *testing.T) {
	ids := []string{"agent-a", "agent-b", "agent-c", "agent-d", "agent-e"}
	branches := make([]string, 0, len(ids))
	for _, id := range ids {
		branches = append(branches, "mnemon/"+id)
	}
	store, err := exchange.NewMemoryPublicationStore(branches...)
	if err != nil {
		t.Fatal(err)
	}
	type node struct {
		id        string
		branch    string
		principal string
		rt        *runtime.Runtime
	}
	nodes := make([]node, 0, len(ids))
	for i, id := range ids {
		root := t.TempDir()
		principal := "codex-" + id + "@project"
		rt := openMeshServingRuntime(t, root, principal)
		observeMeshAssignment(t, rt, principal, id)
		nodes = append(nodes, node{id: id, branch: branches[i], principal: principal, rt: rt})
	}

	for _, n := range nodes {
		remote := githubFakeRemote(t, store, n.branch)
		if err := syncWorkerPush(n.rt, remote, "publish-"+n.id); err != nil {
			t.Fatalf("%s publish: %v", n.id, err)
		}
		if pending, err := n.rt.PendingSyncedEvents(); err != nil || len(pending) != 0 {
			t.Fatalf("%s publish must drain pending events, pending=%+v err=%v", n.id, pending, err)
		}
	}

	for _, n := range nodes {
		for _, source := range nodes {
			if source.id == n.id {
				continue
			}
			remote := githubFakeRemote(t, store, source.branch)
			state, err := exchange.ReadPullState(n.rt, "subscribe-"+source.id)
			if err != nil {
				t.Fatalf("%s read pull state %s: %v", n.id, source.id, err)
			}
			probe, err := remote.SyncPull(contract.SyncPullRequest{ReplicaID: state.ReplicaID, RemoteCursor: state.RemoteCursor})
			if err != nil {
				t.Fatalf("%s probe %s: %v", n.id, source.id, err)
			}
			if len(probe.Diagnostics) > 0 || len(probe.Events) == 0 {
				t.Fatalf("%s probe %s returned events=%d diagnostics=%+v", n.id, source.id, len(probe.Events), probe.Diagnostics)
			}
			if err := syncWorkerPull(n.rt, remote, "subscribe-"+source.id, nil); err != nil {
				t.Fatalf("%s subscribe %s: %v", n.id, source.id, err)
			}
		}
	}

	assignmentRef := contract.ResourceRef{Kind: "assignment", ID: "project"}
	for _, n := range nodes {
		_, fields, err := n.rt.Resource(assignmentRef)
		if err != nil {
			t.Fatalf("%s read assignments: %v", n.id, err)
		}
		items, _ := fields["items"].([]any)
		scopes := map[string]bool{}
		for _, item := range items {
			m, _ := item.(map[string]any)
			scope, _ := m["scope"].(string)
			scopes[scope] = true
		}
		for _, source := range nodes {
			want := meshAssignmentScope(source.id)
			if !scopes[want] {
				t.Fatalf("%s assignments missing %q in %+v", n.id, want, items)
			}
		}
	}
}

func openMeshServingRuntime(t *testing.T, root, principal string) *runtime.Runtime {
	t.Helper()
	refs := []contract.ResourceRef{{Kind: "progress_digest", ID: "project"}, {Kind: "assignment", ID: "project"}}
	b := access.HostAgentBinding(contract.ActorID(principal), "http://127.0.0.1:8787", refs)
	rt, err := OpenLocalRuntime(filepath.Join(root, runtime.DefaultStorePath), access.LoadedBindings{Bindings: []access.ChannelBinding{b}}, nil, nil)
	if err != nil {
		t.Fatalf("open serving runtime: %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })
	return rt
}

func observeMeshAssignment(t *testing.T, rt *runtime.Runtime, principal, id string) {
	t.Helper()
	if _, _, err := rt.API().Ingest(contract.ActorID(principal), contract.ObservationEnvelope{
		ExternalID: "github-mesh-assignment-" + id,
		Event: contract.Event{Type: "assignment.write_candidate.observed", Payload: map[string]any{
			"assignment_id":     "mesh-" + id,
			"scope":             meshAssignmentScope(id),
			"ttl":               "2h",
			"assignee":          "codex@" + id,
			"expected_work":     "complete deterministic publication mesh validation for " + id,
			"expected_feedback": "progress_digest",
			"evidence":          "deterministic fake GitHub publication mesh test",
		}},
	}); err != nil {
		t.Fatalf("observe assignment: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick assignment: %v", err)
	}
}

func meshAssignmentScope(id string) string {
	return fmt.Sprintf("%s publication mesh assignment", id)
}

func githubFakeRemote(t *testing.T, store exchange.PublicationStore, branch string) exchange.RemoteWorkspace {
	t.Helper()
	remote, err := githubbackend.New(githubbackend.Config{
		Store:  store,
		Repo:   "mnemon-dev/mnemon-teamwork-example",
		Branch: branch,
	})
	if err != nil {
		t.Fatal(err)
	}
	return remote
}
