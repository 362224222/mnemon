package drive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
)

const ManagedWakeQuery = "[mnemon:wake]"

type ManagedTurnClient interface {
	StartTurn(context.Context, string) (ManagedTurnResult, error)
}

type ManagedTurnResult struct {
	TurnID      string
	Status      string
	FinalAnswer string
}

type ManagedWakeCandidate struct {
	Principal        string
	DerivedEventID   string
	BodyDigest       string
	Reason           string
	RenderAuditID    string
	RenderBodyDigest string
}

type ManagedWakeRecord struct {
	Principal        string    `json:"principal"`
	DerivedEventID   string    `json:"derived_event_id"`
	BodyDigest       string    `json:"body_digest"`
	Reason           string    `json:"reason"`
	Query            string    `json:"query"`
	TurnID           string    `json:"turn_id,omitempty"`
	Status           string    `json:"status"`
	StartedAt        time.Time `json:"started_at"`
	RenderAuditID    string    `json:"render_audit_id,omitempty"`
	RenderBodyDigest string    `json:"render_body_digest,omitempty"`
	FinalAnswer      string    `json:"final_answer,omitempty"`
	Error            string    `json:"error,omitempty"`
}

type ManagedWakeLedger interface {
	Seen(ManagedWakeCandidate) bool
	Record(ManagedWakeRecord) error
}

type MemoryManagedWakeLedger struct {
	mu   sync.Mutex
	seen map[string]ManagedWakeRecord
}

func NewMemoryManagedWakeLedger() *MemoryManagedWakeLedger {
	return &MemoryManagedWakeLedger{seen: map[string]ManagedWakeRecord{}}
}

func (l *MemoryManagedWakeLedger) Seen(candidate ManagedWakeCandidate) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	record, ok := l.seen[managedWakeKey(candidate)]
	return ok && managedWakeRecordHandled(record)
}

func (l *MemoryManagedWakeLedger) Record(record ManagedWakeRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seen[managedWakeKey(ManagedWakeCandidate{
		Principal:      record.Principal,
		DerivedEventID: record.DerivedEventID,
		BodyDigest:     record.BodyDigest,
	})] = record
	return nil
}

func managedWakeRecordHandled(record ManagedWakeRecord) bool {
	return strings.EqualFold(strings.TrimSpace(record.Status), "completed")
}

type ManagedAgentDriver struct {
	Principal string
	Client    ManagedTurnClient
	Ledger    ManagedWakeLedger
	TraceSink ManagedTurnTraceSink
	Cooldown  time.Duration
	Now       func() time.Time

	mu       sync.Mutex
	inFlight bool
	lastWake time.Time
}

func (d *ManagedAgentDriver) Wake(ctx context.Context, candidate ManagedWakeCandidate) (ManagedWakeRecord, error) {
	if d.Principal == "" {
		return ManagedWakeRecord{}, fmt.Errorf("managed agent driver requires a principal")
	}
	if candidate.Principal != d.Principal {
		return ManagedWakeRecord{}, fmt.Errorf("wake candidate principal %q does not match local principal %q", candidate.Principal, d.Principal)
	}
	if candidate.DerivedEventID == "" || candidate.BodyDigest == "" {
		return ManagedWakeRecord{}, fmt.Errorf("wake candidate requires derived event id and body digest")
	}
	if d.Client == nil {
		return ManagedWakeRecord{}, fmt.Errorf("managed agent driver requires a turn client")
	}
	ledger := d.Ledger
	if ledger == nil {
		ledger = NewMemoryManagedWakeLedger()
		d.Ledger = ledger
	}
	now := d.now()
	d.mu.Lock()
	if d.inFlight {
		d.mu.Unlock()
		return ManagedWakeRecord{}, fmt.Errorf("managed wake already in flight")
	}
	if d.Cooldown > 0 && !d.lastWake.IsZero() && now.Sub(d.lastWake) < d.Cooldown {
		d.mu.Unlock()
		return ManagedWakeRecord{}, fmt.Errorf("managed wake cooldown active")
	}
	if ledger.Seen(candidate) {
		d.mu.Unlock()
		return ManagedWakeRecord{}, fmt.Errorf("managed wake candidate already handled")
	}
	d.inFlight = true
	d.mu.Unlock()

	record := ManagedWakeRecord{
		Principal:        candidate.Principal,
		DerivedEventID:   candidate.DerivedEventID,
		BodyDigest:       candidate.BodyDigest,
		Reason:           candidate.Reason,
		Query:            ManagedWakeQuery,
		StartedAt:        now,
		Status:           "started",
		RenderAuditID:    candidate.RenderAuditID,
		RenderBodyDigest: candidate.RenderBodyDigest,
	}
	var result ManagedTurnResult
	var err error
	if traced, ok := d.Client.(ManagedTurnClientWithTrace); ok && d.TraceSink != nil {
		result, err = traced.StartTurnWithTrace(ctx, ManagedWakeQuery, d.TraceSink)
	} else {
		result, err = d.Client.StartTurn(ctx, ManagedWakeQuery)
	}
	if err != nil {
		record.Status = "failed"
		record.Error = err.Error()
		_ = ledger.Record(record)
		d.clearInFlight()
		return record, err
	}
	record.TurnID = result.TurnID
	record.FinalAnswer = result.FinalAnswer
	record.Status = result.Status
	if record.Status == "" {
		record.Status = "completed"
	}
	if err := ledger.Record(record); err != nil {
		d.clearInFlight()
		return record, err
	}
	d.mu.Lock()
	d.lastWake = now
	d.inFlight = false
	d.mu.Unlock()
	return record, nil
}

func (d *ManagedAgentDriver) clearInFlight() {
	d.mu.Lock()
	d.inFlight = false
	d.mu.Unlock()
}

func (d *ManagedAgentDriver) now() time.Time {
	if d.Now != nil {
		return d.Now().UTC()
	}
	return time.Now().UTC()
}

func ManagedWakeCandidatesFromEvents(principal string, events []eventmodel.EventEnvelope) []ManagedWakeCandidate {
	var out []ManagedWakeCandidate
	for _, env := range events {
		if string(env.Event.Audience) != principal {
			continue
		}
		reason, _ := env.Meta["presentation_hint"].(string)
		if reason == "" {
			continue
		}
		body, _ := eventmodel.PayloadNarrative(env.Event.Payload)["body"].(string)
		out = append(out, ManagedWakeCandidate{
			Principal:      principal,
			DerivedEventID: env.Event.ID,
			BodyDigest:     managedBodyDigest(body),
			Reason:         reason,
		})
	}
	return out
}

func managedBodyDigest(body string) string {
	sum := sha256.Sum256([]byte(body))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func managedWakeKey(candidate ManagedWakeCandidate) string {
	return candidate.Principal + "|" + candidate.DerivedEventID + "|" + candidate.BodyDigest
}
