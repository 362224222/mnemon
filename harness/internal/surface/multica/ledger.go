package multica

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const MulticaDefaultProjectionLedgerRelPath = ".mnemon/multica/projection-ledger.jsonl"

type ProjectionLedgerRecord struct {
	EventRef          string      `json:"event_ref"`
	ResourceRef       string      `json:"resource_ref,omitempty"`
	ProjectionRef     string      `json:"projection_ref,omitempty"`
	SourceArtifactRef string      `json:"source_artifact_ref,omitempty"`
	SurfaceRole       SurfaceRole `json:"surface_role"`
	TargetKind        string      `json:"target_kind"`
	TargetID          string      `json:"target_id"`
	Status            string      `json:"status"`
}

type ProjectionLedgerTarget struct {
	Kind string
	ID   string
}

type FileProjectionLedger struct {
	mu      sync.Mutex
	path    string
	records []ProjectionLedgerRecord
	loaded  bool
}

func ProjectionLedgerPath(root, explicit string) string {
	if strings.TrimSpace(explicit) != "" {
		return filepath.Clean(explicit)
	}
	root = strings.TrimSpace(root)
	if root == "" {
		root = "."
	}
	return filepath.Join(root, MulticaDefaultProjectionLedgerRelPath)
}

func NewFileProjectionLedger(path string) *FileProjectionLedger {
	return &FileProjectionLedger{path: strings.TrimSpace(path)}
}

func (l *FileProjectionLedger) Records() ([]ProjectionLedgerRecord, error) {
	if err := l.withLockedLoad(func() error { return nil }); err != nil {
		return nil, err
	}
	out := append([]ProjectionLedgerRecord(nil), l.records...)
	return out, nil
}

func (l *FileProjectionLedger) Find(eventRef string, role SurfaceRole, target ProjectionLedgerTarget) (ProjectionLedgerRecord, bool, error) {
	eventRef = strings.TrimSpace(eventRef)
	target.Kind = strings.TrimSpace(target.Kind)
	target.ID = strings.TrimSpace(target.ID)
	if eventRef == "" || role == "" || target.Kind == "" || target.ID == "" {
		return ProjectionLedgerRecord{}, false, nil
	}
	var found ProjectionLedgerRecord
	err := l.withLockedLoad(func() error {
		for _, record := range l.records {
			if projectionLedgerRecordMatches(record, eventRef, role, target) {
				found = record
			}
		}
		return nil
	})
	if err != nil {
		return ProjectionLedgerRecord{}, false, err
	}
	return found, strings.TrimSpace(found.EventRef) != "", nil
}

func (l *FileProjectionLedger) Reserve(record ProjectionLedgerRecord) (ProjectionLedgerRecord, bool, error) {
	record = normalizeProjectionLedgerRecord(record)
	if err := validateProjectionLedgerRecord(record); err != nil {
		return ProjectionLedgerRecord{}, false, err
	}
	var reserved ProjectionLedgerRecord
	ok := false
	err := l.withLockedLoad(func() error {
		for _, existing := range l.records {
			if projectionLedgerRecordMatches(existing, record.EventRef, record.SurfaceRole, ProjectionLedgerTarget{Kind: record.TargetKind, ID: record.TargetID}) {
				reserved = existing
				return nil
			}
		}
		if record.Status == "" {
			record.Status = "reserved"
		}
		l.records = append(l.records, record)
		reserved = record
		ok = true
		return l.appendRecord(record)
	})
	return reserved, ok, err
}

func (l *FileProjectionLedger) Record(record ProjectionLedgerRecord) error {
	record = normalizeProjectionLedgerRecord(record)
	if err := validateProjectionLedgerRecord(record); err != nil {
		return err
	}
	return l.withLockedLoad(func() error {
		replaced := false
		for i, existing := range l.records {
			if projectionLedgerRecordMatches(existing, record.EventRef, record.SurfaceRole, ProjectionLedgerTarget{Kind: record.TargetKind, ID: record.TargetID}) {
				l.records[i] = record
				replaced = true
			}
		}
		if !replaced {
			l.records = append(l.records, record)
		}
		return l.rewrite()
	})
}

func (l *FileProjectionLedger) withLockedLoad(fn func() error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.loaded {
		if err := l.load(); err != nil {
			return err
		}
		l.loaded = true
	}
	return fn()
}

func (l *FileProjectionLedger) load() error {
	if l.path == "" {
		return nil
	}
	file, err := os.Open(l.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	var records []ProjectionLedgerRecord
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var record ProjectionLedgerRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return fmt.Errorf("decode projection ledger %s: %w", l.path, err)
		}
		records = upsertProjectionLedgerRecord(records, normalizeProjectionLedgerRecord(record))
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	l.records = records
	return nil
}

func (l *FileProjectionLedger) appendRecord(record ProjectionLedgerRecord) error {
	if l.path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(l.path), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(l.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if _, err := file.Write(append(encoded, '\n')); err != nil {
		return err
	}
	return nil
}

func (l *FileProjectionLedger) rewrite() error {
	if l.path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(l.path), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(l.path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	for _, record := range l.records {
		encoded, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if _, err := file.Write(append(encoded, '\n')); err != nil {
			return err
		}
	}
	return nil
}

func normalizeProjectionLedgerRecord(record ProjectionLedgerRecord) ProjectionLedgerRecord {
	record.EventRef = strings.TrimSpace(record.EventRef)
	record.ResourceRef = strings.TrimSpace(record.ResourceRef)
	record.ProjectionRef = strings.TrimSpace(record.ProjectionRef)
	record.SourceArtifactRef = strings.TrimSpace(record.SourceArtifactRef)
	record.TargetKind = strings.TrimSpace(record.TargetKind)
	record.TargetID = strings.TrimSpace(record.TargetID)
	record.Status = strings.TrimSpace(record.Status)
	return record
}

func validateProjectionLedgerRecord(record ProjectionLedgerRecord) error {
	if strings.TrimSpace(record.EventRef) == "" {
		return fmt.Errorf("event_ref is required")
	}
	if record.SurfaceRole == "" {
		return fmt.Errorf("surface_role is required")
	}
	if strings.TrimSpace(record.TargetKind) == "" || strings.TrimSpace(record.TargetID) == "" {
		return fmt.Errorf("projection ledger target is required")
	}
	return nil
}

func projectionLedgerRecordMatches(record ProjectionLedgerRecord, eventRef string, role SurfaceRole, target ProjectionLedgerTarget) bool {
	return record.EventRef == strings.TrimSpace(eventRef) &&
		record.SurfaceRole == role &&
		record.TargetKind == strings.TrimSpace(target.Kind) &&
		record.TargetID == strings.TrimSpace(target.ID)
}

func upsertProjectionLedgerRecord(records []ProjectionLedgerRecord, next ProjectionLedgerRecord) []ProjectionLedgerRecord {
	for i, record := range records {
		if projectionLedgerRecordMatches(record, next.EventRef, next.SurfaceRole, ProjectionLedgerTarget{Kind: next.TargetKind, ID: next.TargetID}) {
			records[i] = next
			return records
		}
	}
	return append(records, next)
}
