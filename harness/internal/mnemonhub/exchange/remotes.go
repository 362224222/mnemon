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

const (
	RemoteBackendHTTP   = "http"
	RemoteBackendGitHub = "github"
)

const (
	RemoteDirectionBidirectional = "bidirectional"
	RemoteDirectionPublish       = "publish"
	RemoteDirectionSubscribe     = "subscribe"
)

type RemoteEntry struct {
	// Backend selects the Remote Workspace implementation. Empty is the v1
	// compatibility default: the first-party HTTP mnemon-hub wire.
	Backend string `json:"backend,omitempty"`
	// Direction scopes how this local replica uses the remote. Empty is the v1
	// compatibility default: bidirectional push+pull.
	Direction     string `json:"direction,omitempty"`
	ID            string `json:"id"`
	Endpoint      string `json:"endpoint,omitempty"`
	Repo          string `json:"repo,omitempty"`
	Branch        string `json:"branch,omitempty"`
	CredentialRef string `json:"credential_ref"`
	// CAFile optionally pins the remote's TLS root (PEM bundle) — the client trusts exactly it
	// (sync-abi-v1 §8). Empty = the system roots.
	CAFile string `json:"ca_file,omitempty"`
}

type RemotePlan struct {
	PushTargets []RemoteEntry
	PullSources []RemoteEntry
}

func (r RemoteEntry) NormalizedBackend() string {
	backend, _ := NormalizeRemoteBackend(r.Backend)
	return backend
}

func (r RemoteEntry) NormalizedDirection() string {
	direction, _ := NormalizeRemoteDirection(r.Direction)
	return direction
}

func NormalizeRemoteBackend(backend string) (string, error) {
	backend = strings.TrimSpace(backend)
	if backend == "" {
		backend = RemoteBackendHTTP
	}
	if err := validateRemoteBackend(backend); err != nil {
		return "", err
	}
	return backend, nil
}

func NormalizeRemoteDirection(direction string) (string, error) {
	direction = strings.TrimSpace(direction)
	if direction == "" {
		direction = RemoteDirectionBidirectional
	}
	if err := validateRemoteDirection(direction); err != nil {
		return "", err
	}
	return direction, nil
}

func validateRemoteBackend(backend string) error {
	switch backend {
	case RemoteBackendHTTP, RemoteBackendGitHub:
		return nil
	default:
		return fmt.Errorf("unsupported Remote Workspace backend %q (supported: %s, %s)", backend, RemoteBackendHTTP, RemoteBackendGitHub)
	}
}

func validateRemoteDirection(direction string) error {
	switch direction {
	case RemoteDirectionBidirectional, RemoteDirectionPublish, RemoteDirectionSubscribe:
		return nil
	default:
		return fmt.Errorf("unsupported Remote Workspace direction %q (supported: %s, %s, %s)", direction, RemoteDirectionBidirectional, RemoteDirectionPublish, RemoteDirectionSubscribe)
	}
}

// LoadRemoteEntry resolves one remote from the registry at path: id "default" follows the doc's
// `current` pointer. It validates schema version, backend, and direction; credential presence is the
// caller's concern (the CLI may inject a --token override).
func LoadRemoteEntry(path, id string) (RemoteEntry, error) {
	doc, err := loadRemotesDoc(path)
	if err != nil {
		return RemoteEntry{}, err
	}
	if id == "default" && strings.TrimSpace(doc.Current) != "" {
		id = strings.TrimSpace(doc.Current)
	}
	remote, ok := findRemoteEntry(doc, id)
	if !ok {
		return RemoteEntry{}, fmt.Errorf("Remote Workspace %q not found in %s", id, path)
	}
	remote, err = normalizeRemoteEntry(remote)
	if err != nil {
		return RemoteEntry{}, fmt.Errorf("Remote Workspace %q: %w", id, err)
	}
	return remote, nil
}

// LoadRemotePlan resolves the active Remote Workspace plan. Legacy configs with `current` and no
// explicit directions keep selecting exactly the current remote. Directional configs select all
// remotes by default so a local replica can publish to one workspace and subscribe to many sources.
func LoadRemotePlan(path, id string) (RemotePlan, error) {
	doc, err := loadRemotesDoc(path)
	if err != nil {
		return RemotePlan{}, err
	}
	entries, err := selectRemotePlanEntries(doc, id)
	if err != nil {
		return RemotePlan{}, fmt.Errorf("%w in %s", err, path)
	}
	plan := RemotePlan{}
	for _, entry := range entries {
		entry, err = normalizeRemoteEntry(entry)
		if err != nil {
			return RemotePlan{}, fmt.Errorf("Remote Workspace %q: %w", entry.ID, err)
		}
		switch entry.Direction {
		case RemoteDirectionBidirectional:
			plan.PushTargets = append(plan.PushTargets, entry)
			plan.PullSources = append(plan.PullSources, entry)
		case RemoteDirectionPublish:
			plan.PushTargets = append(plan.PushTargets, entry)
		case RemoteDirectionSubscribe:
			plan.PullSources = append(plan.PullSources, entry)
		}
	}
	if len(plan.PushTargets) > 1 {
		return RemotePlan{}, fmt.Errorf("multiple Remote Workspace publish targets unsupported: %s", remoteIDs(plan.PushTargets))
	}
	return plan, nil
}

func loadRemotesDoc(path string) (RemotesDoc, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return RemotesDoc{}, fmt.Errorf("read Remote Workspace config: %w", err)
	}
	var doc RemotesDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		return RemotesDoc{}, fmt.Errorf("parse Remote Workspace config: %w", err)
	}
	if doc.SchemaVersion != 1 {
		return RemotesDoc{}, fmt.Errorf("Remote Workspace config schema_version %d unsupported (want 1)", doc.SchemaVersion)
	}
	return doc, nil
}

func normalizeRemoteEntry(remote RemoteEntry) (RemoteEntry, error) {
	var err error
	remote.Backend, err = NormalizeRemoteBackend(remote.Backend)
	if err != nil {
		return RemoteEntry{}, err
	}
	remote.Direction, err = NormalizeRemoteDirection(remote.Direction)
	if err != nil {
		return RemoteEntry{}, err
	}
	switch remote.Backend {
	case RemoteBackendHTTP:
		if strings.TrimSpace(remote.Endpoint) == "" {
			return RemoteEntry{}, fmt.Errorf("has no endpoint")
		}
	case RemoteBackendGitHub:
		remote.Repo, err = NormalizeGitHubRepo(remote.Repo)
		if err != nil {
			return RemoteEntry{}, err
		}
		remote.Branch, err = NormalizePublicationBranch(remote.Branch)
		if err != nil {
			return RemoteEntry{}, err
		}
	}
	return remote, nil
}

func NormalizeGitHubRepo(repo string) (string, error) {
	repo = strings.TrimSpace(repo)
	if repo == "" {
		return "", fmt.Errorf("github repo is required")
	}
	parts := strings.Split(repo, "/")
	if len(parts) != 2 {
		return "", fmt.Errorf("github repo %q must be owner/name", repo)
	}
	for _, part := range parts {
		if !validGitHubRepoSegment(part) {
			return "", fmt.Errorf("github repo %q is invalid", repo)
		}
	}
	return repo, nil
}

func validGitHubRepoSegment(segment string) bool {
	if segment == "" || segment == "." || segment == ".." {
		return false
	}
	for _, r := range segment {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			continue
		}
		return false
	}
	return true
}

func findRemoteEntry(doc RemotesDoc, id string) (RemoteEntry, bool) {
	for _, remote := range doc.Remotes {
		if remote.ID == id {
			return remote, true
		}
	}
	return RemoteEntry{}, false
}

func selectRemotePlanEntries(doc RemotesDoc, id string) ([]RemoteEntry, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		id = "default"
	}
	if id != "default" {
		remote, ok := findRemoteEntry(doc, id)
		if !ok {
			return nil, fmt.Errorf("Remote Workspace %q not found", id)
		}
		return []RemoteEntry{remote}, nil
	}
	current := strings.TrimSpace(doc.Current)
	if current != "" && !hasExplicitRemoteDirection(doc) {
		remote, ok := findRemoteEntry(doc, current)
		if !ok {
			return nil, fmt.Errorf("Remote Workspace %q not found", current)
		}
		return []RemoteEntry{remote}, nil
	}
	if len(doc.Remotes) == 0 {
		return nil, fmt.Errorf("Remote Workspace config has no remotes")
	}
	return append([]RemoteEntry(nil), doc.Remotes...), nil
}

func hasExplicitRemoteDirection(doc RemotesDoc) bool {
	for _, remote := range doc.Remotes {
		if strings.TrimSpace(remote.Direction) != "" {
			return true
		}
	}
	return false
}

func remoteIDs(remotes []RemoteEntry) string {
	ids := make([]string, 0, len(remotes))
	for _, remote := range remotes {
		ids = append(ids, remote.ID)
	}
	return strings.Join(ids, ", ")
}
