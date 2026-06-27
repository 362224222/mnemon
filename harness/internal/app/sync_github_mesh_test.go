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

func TestSyncGitHubFakePublicationMeshJoinLeaveReassignment(t *testing.T) {
	ids := []string{"agent-a", "agent-b", "agent-c", "agent-d", "agent-e", "agent-f", "agent-g"}
	branches := make([]string, 0, len(ids))
	for _, id := range ids {
		branches = append(branches, "mnemon/"+id)
	}
	store, err := exchange.NewMemoryPublicationStore(branches...)
	if err != nil {
		t.Fatal(err)
	}

	nodes := make([]meshTestNode, 0, len(ids))
	for _, id := range ids[:5] {
		node := newMeshTestNode(t, id)
		observeMeshAssignment(t, node.rt, node.principal, id)
		publishMeshNode(t, store, node)
		nodes = append(nodes, node)
	}
	pullMeshAllSources(t, store, nodes[:5], nodes[:5])

	for _, id := range ids[5:] {
		node := newMeshTestNode(t, id)
		observeMeshAssignment(t, node.rt, node.principal, id)
		publishMeshNode(t, store, node)
		nodes = append(nodes, node)
	}
	offline := nodes[3]
	active := append([]meshTestNode{}, nodes[:3]...)
	active = append(active, nodes[4:]...)
	reassignScope := "agent-d down; reassign delayed work to agent-f"
	observeMeshAssignmentWithScope(t, nodes[0].rt, nodes[0].principal, "reassign-agent-d", reassignScope, "codex@agent-f")
	publishMeshNode(t, store, nodes[0])

	pullMeshAllSources(t, store, active, nodes)
	assertMeshScopes(t, active, []string{
		meshAssignmentScope("agent-a"),
		meshAssignmentScope("agent-b"),
		meshAssignmentScope("agent-c"),
		meshAssignmentScope("agent-d"),
		meshAssignmentScope("agent-e"),
		meshAssignmentScope("agent-f"),
		meshAssignmentScope("agent-g"),
		reassignScope,
	})
	assertMeshScopesAbsent(t, []meshTestNode{offline}, []string{meshAssignmentScope("agent-f"), meshAssignmentScope("agent-g"), reassignScope})

	pullMeshAllSources(t, store, []meshTestNode{offline}, nodes)
	assertMeshScopes(t, []meshTestNode{offline}, []string{meshAssignmentScope("agent-f"), meshAssignmentScope("agent-g"), reassignScope})
}

type meshTestNode struct {
	id        string
	branch    string
	principal string
	rt        *runtime.Runtime
}

func newMeshTestNode(t *testing.T, id string) meshTestNode {
	t.Helper()
	root := t.TempDir()
	principal := "codex-" + id + "@project"
	return meshTestNode{
		id:        id,
		branch:    "mnemon/" + id,
		principal: principal,
		rt:        openMeshServingRuntime(t, root, principal),
	}
}

func publishMeshNode(t *testing.T, store exchange.PublicationStore, node meshTestNode) {
	t.Helper()
	remote := githubFakeRemote(t, store, node.branch)
	if err := syncWorkerPush(node.rt, remote, "publish-"+node.id); err != nil {
		t.Fatalf("%s publish: %v", node.id, err)
	}
	if pending, err := node.rt.PendingSyncedEvents(); err != nil || len(pending) != 0 {
		t.Fatalf("%s publish must drain pending events, pending=%+v err=%v", node.id, pending, err)
	}
}

func pullMeshAllSources(t *testing.T, store exchange.PublicationStore, targets, sources []meshTestNode) {
	t.Helper()
	for _, target := range targets {
		for _, source := range sources {
			if source.id == target.id {
				continue
			}
			remote := githubFakeRemote(t, store, source.branch)
			if err := syncWorkerPull(target.rt, remote, "subscribe-"+source.id, nil); err != nil {
				t.Fatalf("%s subscribe %s: %v", target.id, source.id, err)
			}
		}
	}
}

func assertMeshScopes(t *testing.T, nodes []meshTestNode, scopes []string) {
	t.Helper()
	for _, node := range nodes {
		got := meshScopes(t, node)
		for _, want := range scopes {
			if !got[want] {
				t.Fatalf("%s assignments missing %q in %+v", node.id, want, got)
			}
		}
	}
}

func assertMeshScopesAbsent(t *testing.T, nodes []meshTestNode, scopes []string) {
	t.Helper()
	for _, node := range nodes {
		got := meshScopes(t, node)
		for _, want := range scopes {
			if got[want] {
				t.Fatalf("%s assignments unexpectedly contain %q in %+v", node.id, want, got)
			}
		}
	}
}

func meshScopes(t *testing.T, node meshTestNode) map[string]bool {
	t.Helper()
	assignmentRef := contract.ResourceRef{Kind: "assignment", ID: "project"}
	_, fields, err := node.rt.Resource(assignmentRef)
	if err != nil {
		t.Fatalf("%s read assignments: %v", node.id, err)
	}
	items, _ := fields["items"].([]any)
	scopes := map[string]bool{}
	for _, item := range items {
		m, _ := item.(map[string]any)
		scope, _ := m["scope"].(string)
		scopes[scope] = true
	}
	return scopes
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
	observeMeshAssignmentWithScope(t, rt, principal, id, meshAssignmentScope(id), "codex@"+id)
}

func observeMeshAssignmentWithScope(t *testing.T, rt *runtime.Runtime, principal, id, scope, assignee string) {
	t.Helper()
	if _, _, err := rt.API().Ingest(contract.ActorID(principal), contract.ObservationEnvelope{
		ExternalID: "github-mesh-assignment-" + id,
		Event: contract.Event{Type: "assignment.write_candidate.observed", Payload: map[string]any{
			"assignment_id":     "mesh-" + id,
			"scope":             scope,
			"ttl":               "2h",
			"assignee":          assignee,
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
