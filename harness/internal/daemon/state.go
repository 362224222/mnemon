package daemon

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type CursorRecord struct {
	Cursor    string    `json:"cursor"`
	UpdatedAt time.Time `json:"updated_at"`
}

type FileCursorStore struct {
	path string
	now  Clock

	mu      sync.Mutex
	loaded  bool
	records map[string]CursorRecord
}

func NewFileCursorStore(path string, now Clock) *FileCursorStore {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &FileCursorStore{path: strings.TrimSpace(path), now: now, records: map[string]CursorRecord{}}
}

func (s *FileCursorStore) Load(worker string) (CursorRecord, bool, error) {
	worker = strings.TrimSpace(worker)
	if worker == "" {
		return CursorRecord{}, false, fmt.Errorf("daemon cursor worker name is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(); err != nil {
		return CursorRecord{}, false, err
	}
	record, ok := s.records[worker]
	return record, ok, nil
}

func (s *FileCursorStore) Save(worker, cursor string) error {
	worker = strings.TrimSpace(worker)
	if worker == "" {
		return fmt.Errorf("daemon cursor worker name is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(); err != nil {
		return err
	}
	s.records[worker] = CursorRecord{Cursor: strings.TrimSpace(cursor), UpdatedAt: s.now()}
	return s.writeLocked()
}

func (s *FileCursorStore) loadLocked() error {
	if s.loaded {
		return nil
	}
	s.loaded = true
	if s.path == "" {
		return fmt.Errorf("daemon cursor store path is required")
	}
	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, &s.records); err != nil {
		return fmt.Errorf("parse daemon cursor store: %w", err)
	}
	if s.records == nil {
		s.records = map[string]CursorRecord{}
	}
	return nil
}

func (s *FileCursorStore) writeLocked() error {
	if s.path == "" {
		return fmt.Errorf("daemon cursor store path is required")
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s.records, "", "  ")
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

type BackoffPolicy struct {
	Base time.Duration
	Max  time.Duration
}

func (p BackoffPolicy) Duration(attempt int) time.Duration {
	base := p.Base
	if base <= 0 {
		base = time.Second
	}
	max := p.Max
	if max <= 0 {
		max = 30 * time.Second
	}
	if max < base {
		max = base
	}
	if attempt <= 0 {
		return base
	}
	backoff := base
	for i := 0; i < attempt; i++ {
		if backoff >= max/2 {
			return max
		}
		backoff *= 2
	}
	if backoff > max {
		return max
	}
	return backoff
}

type InFlightGuard struct {
	mu   sync.Mutex
	keys map[string]bool
}

func NewInFlightGuard() *InFlightGuard {
	return &InFlightGuard{keys: map[string]bool{}}
}

func (g *InFlightGuard) TryStart(key string) bool {
	key = strings.TrimSpace(key)
	if key == "" {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.keys == nil {
		g.keys = map[string]bool{}
	}
	if g.keys[key] {
		return false
	}
	g.keys[key] = true
	return true
}

func (g *InFlightGuard) Done(key string) {
	key = strings.TrimSpace(key)
	if key == "" {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.keys, key)
}
