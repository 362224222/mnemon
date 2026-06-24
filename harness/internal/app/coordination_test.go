package app

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/channel"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

// P3a: the AgentTeam coordination kinds (project_intent/assignment/progress_digest) are ordinary
// first-party declared kinds — they govern through the SAME assembler/appendItemRule path as
// memory/skill, with no per-kind code. This pins one (assignment, which carries the required `scope`)
// through observe → admit → resource read, plus the negative: a candidate missing the required scope
// is rejected, never written.
func TestCoordinationAssignmentGoverns(t *testing.T) {
	ref := contract.ResourceRef{Kind: "assignment", ID: "project"}
	binding := channel.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	binding.AllowedObservedTypes = []string{"assignment.write_candidate.observed"}

	// nil catalog → EmbeddedCatalog, which now carries the three coordination kinds (P3a).
	rc, err := LocalRuntimeConfigFromBindings([]channel.ChannelBinding{binding}, nil)
	if err != nil {
		t.Fatalf("boot config: %v", err)
	}
	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "coord.db"), rc)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()

	// positive: a well-formed assignment candidate is admitted.
	if _, _, err := rt.API().Ingest("codex@project", contract.ObservationEnvelope{
		ExternalID: "a1",
		Event: contract.Event{Type: "assignment.write_candidate.observed", Payload: map[string]any{
			"scope": "fix projection", "ttl": "2h", "assignee": "codex@impl", "evidence": "ticket-123",
			"expected_work": "fix the projection path", "expected_feedback": "summary and blockers",
		}},
	}); err != nil {
		t.Fatalf("ingest assignment: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	v, fields, err := rt.Resource(ref)
	if err != nil || v == 0 {
		t.Fatalf("assignment must admit (v=%d err=%v)", v, err)
	}
	if content, _ := fields["content"].(string); !strings.Contains(content, "fix projection") {
		t.Fatalf("assignment content missing the candidate scope: %q", content)
	}

	// negative: scope is required (§569) — a candidate WITH evidence but no scope is rejected, version
	// unchanged (evidence present so the only failure is the missing required scope).
	if _, _, err := rt.API().Ingest("codex@project", contract.ObservationEnvelope{
		ExternalID: "a2",
		Event: contract.Event{Type: "assignment.write_candidate.observed", Payload: map[string]any{
			"ttl": "1h", "assignee": "codex@impl", "evidence": "ticket-123",
			"expected_work": "fix the projection path", "expected_feedback": "summary and blockers",
		}},
	}); err != nil {
		t.Fatalf("ingest scopeless assignment: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	v2, _, _ := rt.Resource(ref)
	if v2 != v {
		t.Fatalf("a scopeless assignment must be rejected (required scope), version moved %d -> %d", v, v2)
	}
}

// P3c risk-tier: assignment is mid-risk, so a complete candidate that lacks `evidence` is DENIED by
// the risk gate (the gate's deny outranks the admission propose), never written.
func TestCoordinationMidRiskRequiresEvidence(t *testing.T) {
	ref := contract.ResourceRef{Kind: "assignment", ID: "project"}
	binding := channel.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	binding.AllowedObservedTypes = []string{"assignment.write_candidate.observed"}
	rc, err := LocalRuntimeConfigFromBindings([]channel.ChannelBinding{binding}, nil)
	if err != nil {
		t.Fatalf("boot config: %v", err)
	}
	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "risk.db"), rc)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()

	// complete assignment but NO evidence → mid-risk gate denies.
	if _, _, err := rt.API().Ingest("codex@project", contract.ObservationEnvelope{
		ExternalID: "r1",
		Event: contract.Event{Type: "assignment.write_candidate.observed", Payload: map[string]any{
			"scope": "evidence-less work", "ttl": "2h", "assignee": "codex@impl",
			"expected_work": "review evidence-less path", "expected_feedback": "short result",
		}},
	}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if v, _, _ := rt.Resource(ref); v != 0 {
		t.Fatalf("a mid-risk assignment without evidence must be denied, but it admitted (v=%d)", v)
	}

	// the same candidate WITH evidence is admitted.
	if _, _, err := rt.API().Ingest("codex@project", contract.ObservationEnvelope{
		ExternalID: "r2",
		Event: contract.Event{Type: "assignment.write_candidate.observed", Payload: map[string]any{
			"scope": "evidence-backed work", "ttl": "2h", "assignee": "codex@impl", "evidence": "PR-42",
			"expected_work": "review evidence-backed path", "expected_feedback": "short result",
		}},
	}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if v, _, _ := rt.Resource(ref); v == 0 {
		t.Fatal("a mid-risk assignment WITH evidence must admit")
	}
}

func TestAssignmentItemsCarryCreatedAtFromEventTimestamp(t *testing.T) {
	ref := contract.ResourceRef{Kind: "assignment", ID: "project"}
	binding := channel.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	binding.AllowedObservedTypes = []string{"assignment.write_candidate.observed"}
	rc, err := LocalRuntimeConfigFromBindings([]channel.ChannelBinding{binding}, nil)
	if err != nil {
		t.Fatalf("boot config: %v", err)
	}
	const ts = "2026-06-24T09:45:00Z"
	rc.Now = func() string { return ts }
	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "assignment-created-at.db"), rc)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()

	if _, _, err := rt.API().Ingest("codex@project", contract.ObservationEnvelope{
		ExternalID: "created-at-1",
		Event: contract.Event{TS: "client-forged", Type: "assignment.write_candidate.observed", Payload: map[string]any{
			"scope": "timestamped work", "ttl": "30m", "assignee": "codex@impl", "evidence": "ticket-10",
			"expected_work": "check timestamp propagation", "expected_feedback": "short result",
		}},
	}); err != nil {
		t.Fatalf("ingest timestamped assignment: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	v, fields, err := rt.Resource(ref)
	if err != nil || v == 0 {
		t.Fatalf("assignment must admit (v=%d err=%v)", v, err)
	}
	items, ok := fields["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("assignment items must be stored in canonical []any shape, got %#v", fields["items"])
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("assignment item must be a map, got %#v", items[0])
	}
	if got, _ := item["created_at"].(string); got != ts {
		t.Fatalf("created_at = %q, want server-stamped event timestamp %q (item=%#v)", got, ts, item)
	}
}

// P3b default-enablement: a host whose binding enables ONLY memory (explicit allow-list + scope, as
// setup writes) STILL governs the coordination kinds — the boot grants them to every host-agent
// principal without an explicit --loop. This pins the "coordination package is on out of the box".
func TestCoordinationDefaultEnabled(t *testing.T) {
	memRef := contract.ResourceRef{Kind: "memory", ID: "project"}
	binding := channel.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{memRef})
	// explicit allow-list (like setup): memory only — coordination is NOT named here.
	binding.AllowedObservedTypes = []string{"session.observed", "memory.write_candidate.observed"}

	rc, err := LocalRuntimeConfigFromBindings([]channel.ChannelBinding{binding}, nil)
	if err != nil {
		t.Fatalf("boot config: %v", err)
	}
	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "de.db"), rc)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()

	// an assignment candidate — never named in the binding's --loop scope — is admitted, because the
	// boot default-enabled it.
	assignRef := contract.ResourceRef{Kind: "assignment", ID: "project"}
	if _, _, err := rt.API().Ingest("codex@project", contract.ObservationEnvelope{
		ExternalID: "de1",
		Event: contract.Event{Type: "assignment.write_candidate.observed", Payload: map[string]any{
			"scope": "default-enabled work", "ttl": "2h", "assignee": "codex@impl", "evidence": "ticket-9",
			"expected_work": "handle default-enabled assignment", "expected_feedback": "short result",
		}},
	}); err != nil {
		t.Fatalf("default-enabled assignment observe must be authorized: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	v, _, err := rt.Resource(assignRef)
	if err != nil || v == 0 {
		t.Fatalf("default-enabled assignment must admit without an explicit --loop (v=%d err=%v)", v, err)
	}
	// memory still governs (default-enablement did not disturb the explicit grant).
	if _, _, err := rt.API().Ingest("codex@project", contract.ObservationEnvelope{
		ExternalID: "de2",
		Event: contract.Event{Type: "memory.write_candidate.observed", Payload: map[string]any{
			"content": "still works", "source": "user", "confidence": "high",
		}},
	}); err != nil {
		t.Fatalf("memory must still be observable alongside default-enabled coordination: %v", err)
	}
}

// project_intent governs through the same path — a quick admit pin so all three coordination kinds
// are exercised (assignment above carries the required-field negative).
func TestCoordinationProjectIntentGoverns(t *testing.T) {
	ref := contract.ResourceRef{Kind: "project_intent", ID: "project"}
	binding := channel.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	binding.AllowedObservedTypes = []string{"project_intent.write_candidate.observed"}

	rc, err := LocalRuntimeConfigFromBindings([]channel.ChannelBinding{binding}, nil)
	if err != nil {
		t.Fatalf("boot config: %v", err)
	}
	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "pi.db"), rc)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()
	if _, _, err := rt.API().Ingest("codex@project", contract.ObservationEnvelope{
		ExternalID: "p1",
		Event: contract.Event{Type: "project_intent.write_candidate.observed", Payload: map[string]any{
			"statement": "ship the AgentTeam beta", "evidence": "roadmap-q3",
		}},
	}); err != nil {
		t.Fatalf("ingest project_intent: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	v, fields, err := rt.Resource(ref)
	if err != nil || v == 0 {
		t.Fatalf("project_intent must admit (v=%d err=%v)", v, err)
	}
	if content, _ := fields["content"].(string); !strings.Contains(content, "ship the AgentTeam beta") {
		t.Fatalf("project_intent content missing the statement: %q", content)
	}
}

// R1 Event presentation schema: agent_profile and teamwork_signal are first-party governed resources too,
// not role packages or hostsurface-only hints.
func TestCoordinationProfileAndTeamworkSignalGovern(t *testing.T) {
	profileRef := contract.ResourceRef{Kind: "agent_profile", ID: "project"}
	signalRef := contract.ResourceRef{Kind: "teamwork_signal", ID: "project"}
	binding := channel.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{profileRef, signalRef})
	binding.AllowedObservedTypes = []string{"agent_profile.write_candidate.observed", "teamwork_signal.write_candidate.observed"}

	rc, err := LocalRuntimeConfigFromBindings([]channel.ChannelBinding{binding}, nil)
	if err != nil {
		t.Fatalf("boot config: %v", err)
	}
	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "r1-teamwork.db"), rc)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()

	if _, _, err := rt.API().Ingest("codex@project", contract.ObservationEnvelope{
		ExternalID: "profile-1",
		Event: contract.Event{Type: "agent_profile.write_candidate.observed", Payload: map[string]any{
			"actor": "codex@project", "focus": "harness R1 schema",
			"context_advantages": []any{"read Event presentation plan", "knows capability package"},
			"availability":       "available", "ttl": "30m", "summary": "Working on schema phase.",
		}},
	}); err != nil {
		t.Fatalf("ingest profile: %v", err)
	}
	decisions, err := rt.Tick()
	if err != nil {
		t.Fatalf("tick profile: %v", err)
	}
	if v, fields, err := rt.Resource(profileRef); err != nil || v == 0 || !strings.Contains(fmt.Sprint(fields["content"]), "Working on schema phase.") {
		t.Fatalf("agent_profile must admit and render summary (v=%d err=%v fields=%+v decisions=%+v)", v, err, fields, decisions)
	}

	if _, _, err := rt.API().Ingest("codex@project", contract.ObservationEnvelope{
		ExternalID: "signal-1",
		Event: contract.Event{Type: "teamwork_signal.write_candidate.observed", Payload: map[string]any{
			"scope": "harness/r1", "statement": "Need a second review of render/presentation schema.",
			"why_teamwork": "another agent has fresher render context", "ttl": "1h", "evidence": "profile roster",
		}},
	}); err != nil {
		t.Fatalf("ingest teamwork signal: %v", err)
	}
	decisions, err = rt.Tick()
	if err != nil {
		t.Fatalf("tick teamwork signal: %v", err)
	}
	if v, fields, err := rt.Resource(signalRef); err != nil || v == 0 || !strings.Contains(fmt.Sprint(fields["content"]), "Need a second review") {
		t.Fatalf("teamwork_signal must admit and render statement (v=%d err=%v fields=%+v decisions=%+v)", v, err, fields, decisions)
	}
}
