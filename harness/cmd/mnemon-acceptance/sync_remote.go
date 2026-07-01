package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/mnemonhub/exchange"
)

func upsertSyncRemote(path, root, id, backend, direction, endpoint, token, tokenFile, caFile string) error {
	doc := exchange.RemotesDoc{SchemaVersion: 1}
	if raw, err := os.ReadFile(path); err == nil && len(strings.TrimSpace(string(raw))) > 0 {
		if err := json.Unmarshal(raw, &doc); err != nil {
			return fmt.Errorf("parse Remote Workspace config: %w", err)
		}
		if doc.SchemaVersion != 1 {
			return fmt.Errorf("Remote Workspace config schema_version %d unsupported (want 1)", doc.SchemaVersion)
		}
	} else if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read Remote Workspace config: %w", err)
	}
	credentialRef, err := syncCredentialRef(root, id, token, tokenFile)
	if err != nil {
		return err
	}
	entry := exchange.RemoteEntry{
		Backend:       backend,
		Direction:     direction,
		ID:            id,
		Endpoint:      endpoint,
		CredentialRef: credentialRef,
		CAFile:        normalizeSyncFileRef(caFile),
	}
	replaced := false
	for i := range doc.Remotes {
		if doc.Remotes[i].ID == id {
			doc.Remotes[i] = entry
			replaced = true
			break
		}
	}
	if !replaced {
		doc.Remotes = append(doc.Remotes, entry)
	}
	doc.Current = id
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

func normalizeSyncFileRef(ref string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" || filepath.IsAbs(ref) {
		return ref
	}
	return filepath.ToSlash(filepath.Clean(ref))
}

func syncCredentialRef(root, id, token, tokenFile string) (string, error) {
	token = strings.TrimSpace(token)
	tokenFile = strings.TrimSpace(tokenFile)
	if token != "" {
		credentialRef := filepath.ToSlash(filepath.Join(".mnemon", "harness", "sync", "credentials", id+".token"))
		path := filepath.Join(root, filepath.FromSlash(credentialRef))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return "", err
		}
		if err := os.WriteFile(path, []byte(token+"\n"), 0o600); err != nil {
			return "", err
		}
		return credentialRef, nil
	}
	if tokenFile == "" {
		return "", fmt.Errorf("--token or --token-file is required")
	}
	if filepath.IsAbs(tokenFile) {
		return tokenFile, nil
	}
	return filepath.ToSlash(filepath.Clean(tokenFile)), nil
}
