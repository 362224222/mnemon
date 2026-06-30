package daemon

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	WorkKindSurface = "surface"
	WorkKindWake    = "wake"
)

type WorkLedgerRecord struct {
	Kind      string    `json:"kind"`
	Key       string    `json:"key"`
	Status    string    `json:"status"`
	Attempts  int       `json:"attempts,omitempty"`
	Message   string    `json:"message,omitempty"`
	Error     string    `json:"error,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type FileWorkLedger struct {
	path string
	now  Clock

	mu      sync.Mutex
	loaded  bool
	records map[string]WorkLedgerRecord
}

func NewFileWorkLedger(path string, now Clock) *FileWorkLedger {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &FileWorkLedger{path: strings.TrimSpace(path), now: now, records: map[string]WorkLedgerRecord{}}
}

func (l *FileWorkLedger) Seen(kind, key string) (bool, error) {
	_, ok, err := l.Load(kind, key)
	return ok, err
}

func (l *FileWorkLedger) Load(kind, key string) (WorkLedgerRecord, bool, error) {
	kind, key, err := cleanWorkLedgerKey(kind, key)
	if err != nil {
		return WorkLedgerRecord{}, false, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.loadLocked(); err != nil {
		return WorkLedgerRecord{}, false, err
	}
	record, ok := l.records[workLedgerMapKey(kind, key)]
	return record, ok, nil
}

func (l *FileWorkLedger) Records(kind string) ([]WorkLedgerRecord, error) {
	kind = strings.TrimSpace(kind)
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.loadLocked(); err != nil {
		return nil, err
	}
	out := make([]WorkLedgerRecord, 0, len(l.records))
	for _, record := range l.records {
		if kind == "" || record.Kind == kind {
			out = append(out, record)
		}
	}
	sortWorkLedgerRecords(out)
	return out, nil
}

func (l *FileWorkLedger) Record(record WorkLedgerRecord) error {
	kind, key, err := cleanWorkLedgerKey(record.Kind, record.Key)
	if err != nil {
		return err
	}
	now := l.now()
	createdAtProvided := !record.CreatedAt.IsZero()
	record.Kind = kind
	record.Key = key
	record.Status = strings.TrimSpace(record.Status)
	if record.Status == "" {
		record.Status = "recorded"
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	if record.UpdatedAt.IsZero() {
		record.UpdatedAt = now
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.loadLocked(); err != nil {
		return err
	}
	mapKey := workLedgerMapKey(kind, key)
	if existing, ok := l.records[mapKey]; ok && !createdAtProvided {
		record.CreatedAt = existing.CreatedAt
	}
	l.records[mapKey] = record
	return l.writeLocked()
}

func (l *FileWorkLedger) loadLocked() error {
	if l.loaded {
		return nil
	}
	l.loaded = true
	if strings.TrimSpace(l.path) == "" {
		return fmt.Errorf("daemon work ledger path is required")
	}
	data, err := os.ReadFile(l.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil
	}
	var records []WorkLedgerRecord
	if err := json.Unmarshal(data, &records); err != nil {
		return fmt.Errorf("parse daemon work ledger: %w", err)
	}
	for _, record := range records {
		kind, key, err := cleanWorkLedgerKey(record.Kind, record.Key)
		if err != nil {
			continue
		}
		record.Kind = kind
		record.Key = key
		l.records[workLedgerMapKey(kind, key)] = record
	}
	return nil
}

func (l *FileWorkLedger) writeLocked() error {
	if strings.TrimSpace(l.path) == "" {
		return fmt.Errorf("daemon work ledger path is required")
	}
	if err := os.MkdirAll(filepath.Dir(l.path), 0o755); err != nil {
		return err
	}
	records := make([]WorkLedgerRecord, 0, len(l.records))
	for _, record := range l.records {
		records = append(records, record)
	}
	sortWorkLedgerRecords(records)
	data, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	tmp := l.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, l.path)
}

func cleanWorkLedgerKey(kind, key string) (string, string, error) {
	kind = strings.TrimSpace(kind)
	key = strings.TrimSpace(key)
	if kind == "" {
		return "", "", fmt.Errorf("daemon work ledger kind is required")
	}
	if key == "" {
		return "", "", fmt.Errorf("daemon work ledger key is required")
	}
	return kind, key, nil
}

func workLedgerMapKey(kind, key string) string {
	return kind + "\x00" + key
}

func sortWorkLedgerRecords(records []WorkLedgerRecord) {
	sort.Slice(records, func(i, j int) bool {
		if records[i].Kind == records[j].Kind {
			return records[i].Key < records[j].Key
		}
		return records[i].Kind < records[j].Kind
	})
}
