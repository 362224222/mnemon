package exchange

import (
	"bytes"
	"context"
	"fmt"
	pathpkg "path"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
)

const PublicationEventRoot = "mnemon-publications/v1/events"

// PublicationStore is the storage seam under a Remote Workspace publication backend. It is
// intentionally repository-shaped but not tied to any GitHub client, so exchange semantics can be
// tested against a deterministic fake before a real API adapter exists.
type PublicationStore interface {
	PutEvent(ctx context.Context, branch string, path string, body []byte) (PublicationPutResult, error)
	ListEvents(ctx context.Context, branch string, prefix string, cursor string) (PublicationListResult, error)
	ReadFile(ctx context.Context, branch string, path string) ([]byte, error)
	WriteFile(ctx context.Context, branch string, path string, body []byte) error
}

type PublicationPutResult struct {
	Created    bool
	ExistsSame bool
	Conflict   bool
}

type PublicationStoredEvent struct {
	Path   string
	Body   []byte
	Cursor string
}

type PublicationListResult struct {
	Events     []PublicationStoredEvent
	NextCursor string
}

func PublicationEventPath(env eventmodel.EventEnvelope) (string, error) {
	material, err := contract.SyncedEventMaterialFromEnvelope(env)
	if err != nil {
		return "", err
	}
	origin, err := normalizePublicationPathSegment(material.OriginReplicaID)
	if err != nil {
		return "", fmt.Errorf("publication origin: %w", err)
	}
	kind, err := normalizePublicationPathSegment(string(material.ResourceRef.Kind))
	if err != nil {
		return "", fmt.Errorf("publication resource kind: %w", err)
	}
	resourceID, err := normalizePublicationPathSegment(string(material.ResourceRef.ID))
	if err != nil {
		return "", fmt.Errorf("publication resource id: %w", err)
	}
	stem, err := PublicationEventFileStem(env)
	if err != nil {
		return "", err
	}
	return PublicationEventRoot + "/" + origin + "/" + kind + "/" + resourceID + "/" + stem + ".json", nil
}

func PublicationEventFileStem(env eventmodel.EventEnvelope) (string, error) {
	material, err := contract.SyncedEventMaterialFromEnvelope(env)
	if err != nil {
		return "", err
	}
	if material.LocalIngestSeq < 0 {
		return "", fmt.Errorf("local_ingest_seq must be non-negative")
	}
	decisionID, err := normalizePublicationPathSegment(material.LocalDecisionID)
	if err != nil {
		return "", fmt.Errorf("publication decision id: %w", err)
	}
	return fmt.Sprintf("%012d-%s", material.LocalIngestSeq, decisionID), nil
}

func NormalizePublicationBranch(branch string) (string, error) {
	branch = strings.TrimSpace(branch)
	if branch == "" {
		return "", fmt.Errorf("publication branch is required")
	}
	if strings.Contains(branch, "\\") || strings.HasPrefix(branch, "/") {
		return "", fmt.Errorf("publication branch %q is invalid", branch)
	}
	for _, part := range strings.Split(branch, "/") {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("publication branch %q is invalid", branch)
		}
	}
	if pathpkg.Clean(branch) != branch {
		return "", fmt.Errorf("publication branch %q is invalid", branch)
	}
	if branch != "mnemon/team" && !strings.HasPrefix(branch, "mnemon/") {
		return "", fmt.Errorf("publication branch %q is outside the mnemon namespace", branch)
	}
	return branch, nil
}

func NormalizePublicationPath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("publication path is required")
	}
	if strings.Contains(path, "\\") || strings.HasPrefix(path, "/") {
		return "", fmt.Errorf("publication path %q is invalid", path)
	}
	for _, part := range strings.Split(path, "/") {
		if part == ".." {
			return "", fmt.Errorf("publication path %q is invalid", path)
		}
	}
	clean := pathpkg.Clean(path)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("publication path %q is invalid", path)
	}
	return clean, nil
}

type MemoryPublicationStore struct {
	mu       sync.Mutex
	branches map[string]*memoryPublicationBranch
}

type memoryPublicationBranch struct {
	seq   int64
	files map[string]memoryPublicationFile
}

type memoryPublicationFile struct {
	body  []byte
	seq   int64
	event bool
}

func NewMemoryPublicationStore(branches ...string) (*MemoryPublicationStore, error) {
	store := &MemoryPublicationStore{branches: map[string]*memoryPublicationBranch{}}
	for _, branch := range branches {
		branch, err := NormalizePublicationBranch(branch)
		if err != nil {
			return nil, err
		}
		if _, ok := store.branches[branch]; !ok {
			store.branches[branch] = &memoryPublicationBranch{files: map[string]memoryPublicationFile{}}
		}
	}
	return store, nil
}

func (s *MemoryPublicationStore) PutEvent(ctx context.Context, branch string, path string, body []byte) (PublicationPutResult, error) {
	if err := ctx.Err(); err != nil {
		return PublicationPutResult{}, err
	}
	branch, path, err := s.normalizeEventRef(branch, path)
	if err != nil {
		return PublicationPutResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.branch(branch)
	if err != nil {
		return PublicationPutResult{}, err
	}
	if existing, ok := b.files[path]; ok {
		if bytes.Equal(existing.body, body) {
			return PublicationPutResult{ExistsSame: true}, nil
		}
		return PublicationPutResult{Conflict: true}, nil
	}
	b.seq++
	b.files[path] = memoryPublicationFile{body: append([]byte(nil), body...), seq: b.seq, event: true}
	return PublicationPutResult{Created: true}, nil
}

func (s *MemoryPublicationStore) ListEvents(ctx context.Context, branch string, prefix string, cursor string) (PublicationListResult, error) {
	if err := ctx.Err(); err != nil {
		return PublicationListResult{}, err
	}
	branch, err := NormalizePublicationBranch(branch)
	if err != nil {
		return PublicationListResult{}, err
	}
	prefix, err = normalizePublicationEventPrefix(prefix)
	if err != nil {
		return PublicationListResult{}, err
	}
	after, err := publicationCursor(cursor)
	if err != nil {
		return PublicationListResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.branch(branch)
	if err != nil {
		return PublicationListResult{}, err
	}
	events := make([]PublicationStoredEvent, 0)
	for path, file := range b.files {
		if !file.event || file.seq <= after || !strings.HasPrefix(path, prefix) {
			continue
		}
		events = append(events, PublicationStoredEvent{
			Path:   path,
			Body:   append([]byte(nil), file.body...),
			Cursor: strconv.FormatInt(file.seq, 10),
		})
	}
	sort.Slice(events, func(i, j int) bool {
		left, _ := strconv.ParseInt(events[i].Cursor, 10, 64)
		right, _ := strconv.ParseInt(events[j].Cursor, 10, 64)
		if left == right {
			return events[i].Path < events[j].Path
		}
		return left < right
	})
	return PublicationListResult{Events: events, NextCursor: strconv.FormatInt(b.seq, 10)}, nil
}

func (s *MemoryPublicationStore) ReadFile(ctx context.Context, branch string, path string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	branch, path, err := normalizePublicationFileRef(branch, path)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.branch(branch)
	if err != nil {
		return nil, err
	}
	file, ok := b.files[path]
	if !ok {
		return nil, fmt.Errorf("publication file %s:%s not found", branch, path)
	}
	return append([]byte(nil), file.body...), nil
}

func (s *MemoryPublicationStore) WriteFile(ctx context.Context, branch string, path string, body []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	branch, path, err := normalizePublicationFileRef(branch, path)
	if err != nil {
		return err
	}
	if isPublicationEventPath(path) {
		return fmt.Errorf("publication event path %q must be written with PutEvent", path)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.branch(branch)
	if err != nil {
		return err
	}
	b.seq++
	b.files[path] = memoryPublicationFile{body: append([]byte(nil), body...), seq: b.seq}
	return nil
}

func (s *MemoryPublicationStore) normalizeEventRef(branch, path string) (string, string, error) {
	branch, path, err := normalizePublicationFileRef(branch, path)
	if err != nil {
		return "", "", err
	}
	if !isPublicationEventPath(path) {
		return "", "", fmt.Errorf("publication event path %q must be under %s", path, PublicationEventRoot)
	}
	return branch, path, nil
}

func normalizePublicationFileRef(branch, path string) (string, string, error) {
	branch, err := NormalizePublicationBranch(branch)
	if err != nil {
		return "", "", err
	}
	path, err = NormalizePublicationPath(path)
	if err != nil {
		return "", "", err
	}
	return branch, path, nil
}

func (s *MemoryPublicationStore) branch(branch string) (*memoryPublicationBranch, error) {
	b, ok := s.branches[branch]
	if !ok {
		return nil, fmt.Errorf("publication branch %q is not configured", branch)
	}
	return b, nil
}

func normalizePublicationEventPrefix(prefix string) (string, error) {
	prefix, err := NormalizePublicationPath(prefix)
	if err != nil {
		return "", err
	}
	if prefix == PublicationEventRoot {
		return prefix + "/", nil
	}
	if !isPublicationEventPath(prefix) {
		return "", fmt.Errorf("publication event prefix %q must be under %s", prefix, PublicationEventRoot)
	}
	return prefix, nil
}

func isPublicationEventPath(path string) bool {
	return strings.HasPrefix(path, PublicationEventRoot+"/")
}

func publicationCursor(cursor string) (int64, error) {
	cursor = strings.TrimSpace(cursor)
	if cursor == "" {
		return 0, nil
	}
	seq, err := strconv.ParseInt(cursor, 10, 64)
	if err != nil || seq < 0 {
		return 0, fmt.Errorf("publication cursor %q is invalid", cursor)
	}
	return seq, nil
}

func normalizePublicationPathSegment(segment string) (string, error) {
	segment = strings.TrimSpace(segment)
	if segment == "" || strings.Contains(segment, "/") || strings.Contains(segment, "\\") || segment == "." || segment == ".." {
		return "", fmt.Errorf("path segment %q is invalid", segment)
	}
	return segment, nil
}
