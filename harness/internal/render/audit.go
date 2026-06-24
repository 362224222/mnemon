package render

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type AuditSink interface {
	WriteRenderAudit(context.Context, AuditRecord) error
}

type AuditRecord struct {
	SchemaVersion    int
	AuditID          string
	Principal        string
	Host             string
	Lifecycle        string
	RenderIntent     string
	ProjectionDigest string
	BodyDigest       string
	CatalogDigest    string
	DecisionHead     string
	Status           Status
	BodyChars        int
	CueCounts        map[string]int
	EventCounts      map[string]int
	CreatedAt        string
}

func AuditRecordFrom(req Request, resp Response, cueCounts map[string]int, eventCounts map[string]int) AuditRecord {
	return AuditRecord{
		SchemaVersion:    1,
		AuditID:          resp.AuditID,
		Principal:        string(req.Principal),
		Host:             req.Host,
		Lifecycle:        req.Lifecycle,
		RenderIntent:     req.RenderIntent,
		ProjectionDigest: resp.ProjectionDigest,
		BodyDigest:       resp.BodyDigest,
		CatalogDigest:    resp.Provenance.CatalogDigest,
		DecisionHead:     resp.Provenance.DecisionHead,
		Status:           resp.Status,
		BodyChars:        len(resp.Body),
		CueCounts:        cueCounts,
		EventCounts:      eventCounts,
		CreatedAt:        resp.Provenance.RenderedAt,
	}
}

type MemoryAuditSink struct {
	Records []AuditRecord
}

func (s *MemoryAuditSink) WriteRenderAudit(_ context.Context, r AuditRecord) error {
	s.Records = append(s.Records, r)
	return nil
}

type JSONLAuditSink struct {
	Path string
	mu   sync.Mutex
}

func (s *JSONLAuditSink) WriteRenderAudit(_ context.Context, r AuditRecord) error {
	if strings.TrimSpace(s.Path) == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(s.Path), 0o700); err != nil {
		return err
	}
	f, err := os.OpenFile(s.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := json.NewEncoder(f).Encode(r); err != nil {
		return err
	}
	return nil
}

func cueCounts(body string) map[string]int {
	counts := map[string]int{}
	for _, kind := range []string{"profile", "signal", "work", "feedback", "integrate", "expired"} {
		counts[kind] = strings.Count(body, "[mnemon:"+kind+"]")
	}
	return counts
}
