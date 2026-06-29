package drive

import (
	"context"
	"errors"
	"testing"
	"time"

	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
)

type turnClientFunc func(context.Context, string) (ManagedTurnResult, error)

func (f turnClientFunc) StartTurn(ctx context.Context, query string) (ManagedTurnResult, error) {
	return f(ctx, query)
}

func TestManagedAgentDriverUsesWakeSentinelOnly(t *testing.T) {
	var gotQuery string
	driver := &ManagedAgentDriver{
		Principal: "codex-a@project",
		Client: turnClientFunc(func(_ context.Context, query string) (ManagedTurnResult, error) {
			gotQuery = query
			return ManagedTurnResult{TurnID: "turn-1", Status: "completed"}, nil
		}),
		Ledger: NewMemoryManagedWakeLedger(),
		Now:    func() time.Time { return time.Unix(100, 0).UTC() },
	}
	record, err := driver.Wake(context.Background(), ManagedWakeCandidate{
		Principal:      "codex-a@project",
		DerivedEventID: "derived:assignment.work_available:assignment/asg1:codex-a@project",
		BodyDigest:     "sha256:body",
		Reason:         "work",
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotQuery != ManagedWakeQuery || record.Query != ManagedWakeQuery {
		t.Fatalf("managed wake query = %q / %q, want %q", gotQuery, record.Query, ManagedWakeQuery)
	}
}

func TestManagedAgentDriverRejectsRemotePrincipal(t *testing.T) {
	driver := &ManagedAgentDriver{
		Principal: "codex-a@project",
		Client:    turnClientFunc(func(context.Context, string) (ManagedTurnResult, error) { return ManagedTurnResult{}, nil }),
		Ledger:    NewMemoryManagedWakeLedger(),
	}
	if _, err := driver.Wake(context.Background(), ManagedWakeCandidate{Principal: "codex-b@project", DerivedEventID: "d1", BodyDigest: "sha256:x"}); err == nil {
		t.Fatal("managed driver must reject candidates for non-local principals")
	}
}

func TestManagedAgentDriverDedupesAndCoolsDown(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	driver := &ManagedAgentDriver{
		Principal: "codex-a@project",
		Client:    turnClientFunc(func(context.Context, string) (ManagedTurnResult, error) { return ManagedTurnResult{}, nil }),
		Ledger:    NewMemoryManagedWakeLedger(),
		Cooldown:  time.Minute,
		Now:       func() time.Time { return now },
	}
	candidate := ManagedWakeCandidate{Principal: "codex-a@project", DerivedEventID: "d1", BodyDigest: "sha256:x"}
	if _, err := driver.Wake(context.Background(), candidate); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Wake(context.Background(), candidate); err == nil {
		t.Fatal("duplicate candidate should not wake twice")
	}
	now = now.Add(10 * time.Second)
	if _, err := driver.Wake(context.Background(), ManagedWakeCandidate{Principal: "codex-a@project", DerivedEventID: "d2", BodyDigest: "sha256:y"}); err == nil {
		t.Fatal("cooldown should block a different rapid wake")
	}
}

func TestManagedAgentDriverRecordsFailureWithoutChangingQuery(t *testing.T) {
	ledger := NewMemoryManagedWakeLedger()
	driver := &ManagedAgentDriver{
		Principal: "codex-a@project",
		Client: turnClientFunc(func(_ context.Context, query string) (ManagedTurnResult, error) {
			if query != ManagedWakeQuery {
				t.Fatalf("query = %q, want %q", query, ManagedWakeQuery)
			}
			return ManagedTurnResult{}, errors.New("runtime unavailable")
		}),
		Ledger: ledger,
	}
	candidate := ManagedWakeCandidate{Principal: "codex-a@project", DerivedEventID: "d1", BodyDigest: "sha256:x"}
	record, err := driver.Wake(context.Background(), candidate)
	if err == nil {
		t.Fatal("wake should surface runtime failure")
	}
	if record.Status != "failed" || record.Query != ManagedWakeQuery {
		t.Fatalf("failed record mismatch: %+v", record)
	}
	if !ledger.Seen(candidate) {
		t.Fatal("failure should be recorded locally for audit/idempotence")
	}
}

func TestManagedWakeCandidatesFromEventsUseNarrativeDigest(t *testing.T) {
	env := eventmodel.DerivedEnvelope(eventmodel.Event{
		SchemaVersion: eventmodel.SchemaVersion,
		ID:            "derived:work",
		Type:          "assignment.work_available",
		Subject:       "assignment/asg1",
		Actor:         "mnemond@local",
		Audience:      "codex-a@project",
		Payload:       eventmodel.BuildPayload(nil, map[string]any{"body": "Assignment asg1 is yours."}, nil),
	}, "2026-06-24T10:00:00Z", "2026-06-24T10:05:00Z", "work", nil)
	candidates := ManagedWakeCandidatesFromEvents("codex-a@project", []eventmodel.EventEnvelope{env})
	if len(candidates) != 1 {
		t.Fatalf("candidates = %+v, want one", candidates)
	}
	if candidates[0].Reason != "work" || candidates[0].BodyDigest == "" {
		t.Fatalf("candidate should carry reason and body digest: %+v", candidates[0])
	}
}
