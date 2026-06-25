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
