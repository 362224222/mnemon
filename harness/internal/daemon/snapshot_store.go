package daemon

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const DefaultStatusSnapshotRelPath = ".mnemon/harness/daemon/status.json"

type FileSnapshotStore struct {
	path string
}

func StatusSnapshotPath(root, explicit string) string {
	if strings.TrimSpace(explicit) != "" {
		return strings.TrimSpace(explicit)
	}
	root = strings.TrimSpace(root)
	if root == "" {
		root = "."
	}
	return filepath.Join(root, DefaultStatusSnapshotRelPath)
}

func NewFileSnapshotStore(path string) *FileSnapshotStore {
	return &FileSnapshotStore{path: strings.TrimSpace(path)}
}

func (s *FileSnapshotStore) Load() (Snapshot, bool, error) {
	if s == nil || strings.TrimSpace(s.path) == "" {
		return Snapshot{}, false, fmt.Errorf("daemon status snapshot path is required")
	}
	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return Snapshot{}, false, nil
	}
	if err != nil {
		return Snapshot{}, false, err
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return Snapshot{}, false, nil
	}
	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return Snapshot{}, false, fmt.Errorf("parse daemon status snapshot: %w", err)
	}
	if snapshot.Workers == nil {
		snapshot.Workers = map[string]WorkerSnapshot{}
	}
	return snapshot, true, nil
}

func (s *FileSnapshotStore) Save(snapshot Snapshot) error {
	if s == nil || strings.TrimSpace(s.path) == "" {
		return fmt.Errorf("daemon status snapshot path is required")
	}
	if snapshot.Workers == nil {
		snapshot.Workers = map[string]WorkerSnapshot{}
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
