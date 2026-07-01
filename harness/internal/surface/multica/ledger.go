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

const MulticaDefaultSurfaceWriteLedgerRelPath = ".mnemon/multica/surface-write-ledger.jsonl"

type SurfaceWriteLedgerRecord struct {
	EventRef          string      `json:"event_ref"`
	ResourceRef       string      `json:"resource_ref,omitempty"`
	SurfaceRef        string      `json:"surface_ref,omitempty"`
	SourceArtifactRef string      `json:"source_artifact_ref,omitempty"`
	SurfaceRole       SurfaceRole `json:"surface_role"`
	TargetKind        string      `json:"target_kind"`
	TargetID          string      `json:"target_id"`
	Status            string      `json:"status"`
}

type SurfaceWriteLedgerTarget struct {
	Kind string
	ID   string
}

type FileSurfaceWriteLedger struct {
	mu      sync.Mutex
	path    string
	records []SurfaceWriteLedgerRecord
	loaded  bool
}

func SurfaceWriteLedgerPath(root, explicit string) string {
	if strings.TrimSpace(explicit) != "" {
		return filepath.Clean(explicit)
	}
	root = strings.TrimSpace(root)
	if root == "" {
		root = "."
	}
	return filepath.Join(root, MulticaDefaultSurfaceWriteLedgerRelPath)
}

func NewFileSurfaceWriteLedger(path string) *FileSurfaceWriteLedger {
	return &FileSurfaceWriteLedger{path: strings.TrimSpace(path)}
}

func (l *FileSurfaceWriteLedger) Records() ([]SurfaceWriteLedgerRecord, error) {
	if err := l.withLockedLoad(func() error { return nil }); err != nil {
		return nil, err
	}
	out := append([]SurfaceWriteLedgerRecord(nil), l.records...)
	return out, nil
}

func (l *FileSurfaceWriteLedger) Find(eventRef string, role SurfaceRole, target SurfaceWriteLedgerTarget) (SurfaceWriteLedgerRecord, bool, error) {
	eventRef = strings.TrimSpace(eventRef)
	target.Kind = strings.TrimSpace(target.Kind)
	target.ID = strings.TrimSpace(target.ID)
	if eventRef == "" || role == "" || target.Kind == "" || target.ID == "" {
		return SurfaceWriteLedgerRecord{}, false, nil
	}
	var found SurfaceWriteLedgerRecord
	err := l.withLockedLoad(func() error {
		for _, record := range l.records {
			if surfaceWriteLedgerRecordMatches(record, eventRef, role, target) {
				found = record
			}
		}
		return nil
	})
	if err != nil {
		return SurfaceWriteLedgerRecord{}, false, err
	}
	return found, strings.TrimSpace(found.EventRef) != "", nil
}

func (l *FileSurfaceWriteLedger) Reserve(record SurfaceWriteLedgerRecord) (SurfaceWriteLedgerRecord, bool, error) {
	record = normalizeSurfaceWriteLedgerRecord(record)
	if err := validateSurfaceWriteLedgerRecord(record); err != nil {
		return SurfaceWriteLedgerRecord{}, false, err
	}
	var reserved SurfaceWriteLedgerRecord
	ok := false
	err := l.withLockedLoad(func() error {
		for _, existing := range l.records {
			if surfaceWriteLedgerRecordMatches(existing, record.EventRef, record.SurfaceRole, SurfaceWriteLedgerTarget{Kind: record.TargetKind, ID: record.TargetID}) {
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

func (l *FileSurfaceWriteLedger) Record(record SurfaceWriteLedgerRecord) error {
	record = normalizeSurfaceWriteLedgerRecord(record)
	if err := validateSurfaceWriteLedgerRecord(record); err != nil {
		return err
	}
	return l.withLockedLoad(func() error {
		replaced := false
		for i, existing := range l.records {
			if surfaceWriteLedgerRecordMatches(existing, record.EventRef, record.SurfaceRole, SurfaceWriteLedgerTarget{Kind: record.TargetKind, ID: record.TargetID}) {
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

func (l *FileSurfaceWriteLedger) withLockedLoad(fn func() error) error {
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

func (l *FileSurfaceWriteLedger) load() error {
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
	var records []SurfaceWriteLedgerRecord
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var record SurfaceWriteLedgerRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return fmt.Errorf("decode surface write ledger %s: %w", l.path, err)
		}
		records = upsertSurfaceWriteLedgerRecord(records, normalizeSurfaceWriteLedgerRecord(record))
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	l.records = records
	return nil
}

func (l *FileSurfaceWriteLedger) appendRecord(record SurfaceWriteLedgerRecord) error {
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

func (l *FileSurfaceWriteLedger) rewrite() error {
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

func normalizeSurfaceWriteLedgerRecord(record SurfaceWriteLedgerRecord) SurfaceWriteLedgerRecord {
	record.EventRef = strings.TrimSpace(record.EventRef)
	record.ResourceRef = strings.TrimSpace(record.ResourceRef)
	record.SurfaceRef = strings.TrimSpace(record.SurfaceRef)
	record.SourceArtifactRef = strings.TrimSpace(record.SourceArtifactRef)
	record.TargetKind = strings.TrimSpace(record.TargetKind)
	record.TargetID = strings.TrimSpace(record.TargetID)
	record.Status = strings.TrimSpace(record.Status)
	return record
}

func validateSurfaceWriteLedgerRecord(record SurfaceWriteLedgerRecord) error {
	if strings.TrimSpace(record.EventRef) == "" {
		return fmt.Errorf("event_ref is required")
	}
	if record.SurfaceRole == "" {
		return fmt.Errorf("surface_role is required")
	}
	if strings.TrimSpace(record.TargetKind) == "" || strings.TrimSpace(record.TargetID) == "" {
		return fmt.Errorf("surface write ledger target is required")
	}
	return nil
}

func surfaceWriteLedgerRecordMatches(record SurfaceWriteLedgerRecord, eventRef string, role SurfaceRole, target SurfaceWriteLedgerTarget) bool {
	return record.EventRef == strings.TrimSpace(eventRef) &&
		record.SurfaceRole == role &&
		record.TargetKind == strings.TrimSpace(target.Kind) &&
		record.TargetID == strings.TrimSpace(target.ID)
}

func upsertSurfaceWriteLedgerRecord(records []SurfaceWriteLedgerRecord, next SurfaceWriteLedgerRecord) []SurfaceWriteLedgerRecord {
	for i, record := range records {
		if surfaceWriteLedgerRecordMatches(record, next.EventRef, next.SurfaceRole, SurfaceWriteLedgerTarget{Kind: next.TargetKind, ID: next.TargetID}) {
			records[i] = next
			return records
		}
	}
	return append(records, next)
}
