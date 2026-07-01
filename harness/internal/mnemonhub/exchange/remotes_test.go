package exchange

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadRemoteEntryDefaultsBackendToHTTP(t *testing.T) {
	path := filepath.Join(t.TempDir(), "remotes.json")
	if err := os.WriteFile(path, []byte(`{
	  "schema_version": 1,
	  "current": "hub",
	  "remotes": [{
	    "id": "hub",
	    "endpoint": "http://127.0.0.1:9787",
	    "credential_ref": ".mnemon/harness/sync/credentials/hub.token"
	  }]
	}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	remote, err := LoadRemoteEntry(path, "default")
	if err != nil {
		t.Fatalf("load legacy remote: %v", err)
	}
	if remote.Backend != RemoteBackendHTTP || remote.NormalizedBackend() != RemoteBackendHTTP {
		t.Fatalf("legacy remote backend = %q, want %q", remote.Backend, RemoteBackendHTTP)
	}
	if remote.Direction != RemoteDirectionBidirectional || remote.NormalizedDirection() != RemoteDirectionBidirectional {
		t.Fatalf("legacy remote direction = %q, want %q", remote.Direction, RemoteDirectionBidirectional)
	}
}

func TestLoadRemoteEntryRejectsUnsupportedBackend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "remotes.json")
	if err := os.WriteFile(path, []byte(`{
	  "schema_version": 1,
	  "remotes": [{
	    "id": "mesh",
	    "backend": "bogus",
	    "credential_ref": ".mnemon/harness/sync/credentials/mesh.token"
	  }]
	}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := LoadRemoteEntry(path, "mesh")
	if err == nil || !strings.Contains(err.Error(), "unsupported Remote Workspace backend") {
		t.Fatalf("unsupported backend must fail closed, got %v", err)
	}
}

func TestLoadRemoteEntryRejectsGitHubBackend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "remotes.json")
	if err := os.WriteFile(path, []byte(`{
	  "schema_version": 1,
	  "current": "self",
	  "remotes": [{
	    "id": "self",
	    "backend": "github",
	    "direction": "publish",
	    "repo": "mnemon-dev/mnemon-teamwork-example",
	    "branch": "mnemon/agent-a",
	    "credential_ref": ".mnemon/harness/sync/credentials/self.token"
	  }]
	}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := LoadRemoteEntry(path, "default")
	if err == nil || !strings.Contains(err.Error(), "unsupported Remote Workspace backend") {
		t.Fatalf("github backend must fail closed, got %v", err)
	}
}

func TestLoadRemotePlanLegacyCurrentIsBidirectional(t *testing.T) {
	path := filepath.Join(t.TempDir(), "remotes.json")
	if err := os.WriteFile(path, []byte(`{
	  "schema_version": 1,
	  "current": "hub",
	  "remotes": [{
	    "id": "old",
	    "endpoint": "http://127.0.0.1:9786",
	    "credential_ref": ".mnemon/harness/sync/credentials/old.token"
	  }, {
	    "id": "hub",
	    "endpoint": "http://127.0.0.1:9787",
	    "credential_ref": ".mnemon/harness/sync/credentials/hub.token"
	  }]
	}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	plan, err := LoadRemotePlan(path, "default")
	if err != nil {
		t.Fatalf("load legacy plan: %v", err)
	}
	if len(plan.PushTargets) != 1 || plan.PushTargets[0].ID != "hub" {
		t.Fatalf("legacy push targets = %+v, want hub only", plan.PushTargets)
	}
	if len(plan.PullSources) != 1 || plan.PullSources[0].ID != "hub" {
		t.Fatalf("legacy pull sources = %+v, want hub only", plan.PullSources)
	}
}

func TestLoadRemotePlanHonorsDirections(t *testing.T) {
	path := filepath.Join(t.TempDir(), "remotes.json")
	if err := os.WriteFile(path, []byte(`{
	  "schema_version": 1,
	  "remotes": [{
	    "id": "pub",
	    "direction": "publish",
	    "endpoint": "http://127.0.0.1:9787",
	    "credential_ref": ".mnemon/harness/sync/credentials/pub.token"
	  }, {
	    "id": "sub-a",
	    "direction": "subscribe",
	    "endpoint": "http://127.0.0.1:9788",
	    "credential_ref": ".mnemon/harness/sync/credentials/sub-a.token"
	  }, {
	    "id": "sub-b",
	    "direction": "subscribe",
	    "endpoint": "http://127.0.0.1:9789",
	    "credential_ref": ".mnemon/harness/sync/credentials/sub-b.token"
	  }]
	}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	plan, err := LoadRemotePlan(path, "default")
	if err != nil {
		t.Fatalf("load directional plan: %v", err)
	}
	if len(plan.PushTargets) != 1 || plan.PushTargets[0].ID != "pub" {
		t.Fatalf("directional push targets = %+v, want pub only", plan.PushTargets)
	}
	if got := remotePlanIDs(plan.PullSources); strings.Join(got, ",") != "sub-a,sub-b" {
		t.Fatalf("directional pull sources = %v, want sub-a,sub-b", got)
	}
}

func TestLoadRemotePlanRejectsUnknownDirection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "remotes.json")
	if err := os.WriteFile(path, []byte(`{
	  "schema_version": 1,
	  "remotes": [{
	    "id": "mesh",
	    "direction": "gossip",
	    "endpoint": "http://127.0.0.1:9787",
	    "credential_ref": ".mnemon/harness/sync/credentials/mesh.token"
	  }]
	}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := LoadRemotePlan(path, "default")
	if err == nil || !strings.Contains(err.Error(), "unsupported Remote Workspace direction") {
		t.Fatalf("unsupported direction must fail closed, got %v", err)
	}
}

func TestLoadRemotePlanRejectsMultiplePublishTargets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "remotes.json")
	if err := os.WriteFile(path, []byte(`{
	  "schema_version": 1,
	  "remotes": [{
	    "id": "hub-a",
	    "direction": "publish",
	    "endpoint": "http://127.0.0.1:9787",
	    "credential_ref": ".mnemon/harness/sync/credentials/hub-a.token"
	  }, {
	    "id": "hub-b",
	    "direction": "bidirectional",
	    "endpoint": "http://127.0.0.1:9788",
	    "credential_ref": ".mnemon/harness/sync/credentials/hub-b.token"
	  }]
	}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := LoadRemotePlan(path, "default")
	if err == nil || !strings.Contains(err.Error(), "multiple Remote Workspace publish targets unsupported") {
		t.Fatalf("multiple publish targets must fail closed, got %v", err)
	}
}

func remotePlanIDs(remotes []RemoteEntry) []string {
	ids := make([]string, 0, len(remotes))
	for _, remote := range remotes {
		ids = append(ids, remote.ID)
	}
	return ids
}
