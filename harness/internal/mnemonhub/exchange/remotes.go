package exchange

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// remotes.json is the LOCAL side's remote registry (written by `sync connect`, read by the manual
// sync verbs AND the in-process worker — one loader so the two can never drift). Paths inside it
// (credential_ref, ca_file) resolve relative to the PROJECT root, matching connect's write side.

type RemotesDoc struct {
	SchemaVersion int           `json:"schema_version"`
	Current       string        `json:"current,omitempty"`
	Remotes       []RemoteEntry `json:"remotes"`
}

const RemoteBackendHTTP = "http"

type RemoteEntry struct {
	// Backend selects the Remote Workspace implementation. Empty is the v1
	// compatibility default: the first-party HTTP mnemon-hub wire.
	Backend       string `json:"backend,omitempty"`
	ID            string `json:"id"`
	Endpoint      string `json:"endpoint,omitempty"`
	CredentialRef string `json:"credential_ref"`
	// CAFile optionally pins the remote's TLS root (PEM bundle) — the client trusts exactly it
	// (sync-abi-v1 §8). Empty = the system roots.
	CAFile string `json:"ca_file,omitempty"`
}

func (r RemoteEntry) NormalizedBackend() string {
	backend := strings.TrimSpace(r.Backend)
	if backend == "" {
		return RemoteBackendHTTP
	}
	return backend
}

func validateRemoteBackend(backend string) error {
	switch backend {
	case RemoteBackendHTTP:
		return nil
	default:
		return fmt.Errorf("unsupported Remote Workspace backend %q (supported: %s)", backend, RemoteBackendHTTP)
	}
}

// LoadRemoteEntry resolves one remote from the registry at path: id "default" follows the doc's
// `current` pointer. It validates schema version and the selected backend; credential presence is
// the caller's concern (the CLI may inject a --token override).
func LoadRemoteEntry(path, id string) (RemoteEntry, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return RemoteEntry{}, fmt.Errorf("read Remote Workspace config: %w", err)
	}
	var doc RemotesDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		return RemoteEntry{}, fmt.Errorf("parse Remote Workspace config: %w", err)
	}
	if doc.SchemaVersion != 1 {
		return RemoteEntry{}, fmt.Errorf("Remote Workspace config schema_version %d unsupported (want 1)", doc.SchemaVersion)
	}
	if id == "default" && strings.TrimSpace(doc.Current) != "" {
		id = strings.TrimSpace(doc.Current)
	}
	for _, remote := range doc.Remotes {
		if remote.ID == id {
			remote.Backend = remote.NormalizedBackend()
			if err := validateRemoteBackend(remote.Backend); err != nil {
				return RemoteEntry{}, fmt.Errorf("Remote Workspace %q: %w", id, err)
			}
			if remote.Backend == RemoteBackendHTTP && strings.TrimSpace(remote.Endpoint) == "" {
				return RemoteEntry{}, fmt.Errorf("Remote Workspace %q has no endpoint", id)
			}
			return remote, nil
		}
	}
	return RemoteEntry{}, fmt.Errorf("Remote Workspace %q not found in %s", id, path)
}
