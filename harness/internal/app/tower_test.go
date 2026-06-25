package app

import (
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

// P6a-2: the Tower facade assembles GOAL (project_intent statements) and LEDGER (accepted decisions
// with attribution) read-only from the runtime. An admitted project_intent write shows up on both: the
// goal statement on GOAL, the accepted decision (attributed to the proposer) on LEDGER.
func TestBuildTowerViewGoalAndLedger(t *testing.T) {
	piRef := contract.ResourceRef{Kind: "project_intent", ID: "project"}
	binding := access.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{piRef})
	binding.AllowedObservedTypes = []string{"project_intent.write_candidate.observed"}
	rc, err := LocalRuntimeConfigFromBindings([]access.ChannelBinding{binding}, nil)
	if err != nil {
		t.Fatalf("boot config: %v", err)
	}
	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "tower.db"), rc)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()

	if _, _, err := rt.API().Ingest("codex@project", contract.ObservationEnvelope{
		ExternalID: "pi1",
		Event: contract.Event{Type: "project_intent.write_candidate.observed", Payload: map[string]any{
			"statement": "ship the AgentTeam beta", "evidence": "roadmap-q3"}},
	}); err != nil {
		t.Fatalf("ingest project_intent: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}

	v, err := BuildTowerView(rt, []access.ChannelBinding{binding})
	if err != nil {
		t.Fatalf("build tower view: %v", err)
	}

	// GOAL: the goal statement is surfaced.
	if len(v.Goal.Statements) != 1 || v.Goal.Statements[0] != "ship the AgentTeam beta" {
		t.Fatalf("GOAL statements wrong: %+v", v.Goal.Statements)
	}

	// LEDGER: the accepted project_intent decision, attributed to the proposer, with the changed ref.
	var found bool
	for _, d := range v.Ledger.Decisions {
		if d.Actor != "codex@project" {
			continue
		}
		for _, r := range d.Refs {
			if r.Kind == "project_intent" {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("LEDGER must carry the accepted project_intent decision with attribution: %+v", v.Ledger.Decisions)
	}
}

// P6a-3: FIELD enumerates agents from the BindingSet + live assignments from the assignment resource;
// INBOX surfaces open escalations from the durable .diagnostic events. A valid assignment lands on
// FIELD; a denied one (missing the required scope) surfaces as an INBOX escalation, never silently lost.
func TestBuildTowerViewFieldAndInbox(t *testing.T) {
	asgRef := contract.ResourceRef{Kind: "assignment", ID: "project"}
	binding := access.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{asgRef})
	binding.AllowedObservedTypes = []string{"assignment.write_candidate.observed"}
	rc, err := LocalRuntimeConfigFromBindings([]access.ChannelBinding{binding}, nil)
	if err != nil {
		t.Fatalf("boot config: %v", err)
	}
	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "field.db"), rc)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()

	// valid assignment -> admitted (FIELD)
	if _, _, err := rt.API().Ingest("codex@project", contract.ObservationEnvelope{
		ExternalID: "asg1",
		Event: contract.Event{Type: "assignment.write_candidate.observed", Payload: map[string]any{
			"scope": "fix projection", "ttl": "2h", "assignee": "codex@impl", "evidence": "ticket-1",
			"expected_work": "fix projection", "expected_feedback": "summary and blockers"}},
	}); err != nil {
		t.Fatalf("ingest valid assignment: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	// invalid assignment (missing the required scope) -> denied -> diagnostic (INBOX)
	if _, _, err := rt.API().Ingest("codex@project", contract.ObservationEnvelope{
		ExternalID: "asg2",
		Event: contract.Event{Type: "assignment.write_candidate.observed", Payload: map[string]any{
			"ttl": "1h", "assignee": "codex@impl", "evidence": "ticket-2",
			"expected_work": "fix projection", "expected_feedback": "summary and blockers"}},
	}); err != nil {
		t.Fatalf("ingest invalid assignment: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}

	v, err := BuildTowerView(rt, []access.ChannelBinding{binding})
	if err != nil {
		t.Fatalf("build tower view: %v", err)
	}

	// FIELD agents from the BindingSet.
	if len(v.Field.Agents) != 1 || v.Field.Agents[0].Principal != "codex@project" {
		t.Fatalf("FIELD must enumerate the bound agent: %+v", v.Field.Agents)
	}
	// FIELD assignment (only the admitted one).
	if len(v.Field.Assignments) != 1 || v.Field.Assignments[0].Scope != "fix projection" || v.Field.Assignments[0].TTL != "2h" {
		t.Fatalf("FIELD assignment wrong: %+v", v.Field.Assignments)
	}
	// INBOX: the denied assignment surfaces as an escalation.
	var inboxedAssignment bool
	for _, e := range v.Inbox.Escalations {
		if e.Domain == "assignment" {
			inboxedAssignment = true
		}
	}
	if !inboxedAssignment {
		t.Fatalf("INBOX must surface the denied assignment escalation: %+v", v.Inbox.Escalations)
	}
	if v.Field.Diagnostics != len(v.Inbox.Escalations) {
		t.Fatalf("FIELD diagnostic count (%d) must equal INBOX escalations (%d)", v.Field.Diagnostics, len(v.Inbox.Escalations))
	}
}

// An empty runtime yields empty pages (no panic, no fabricated data).
func TestBuildTowerViewEmpty(t *testing.T) {
	binding := access.HostAgentBinding("codex@project", "http://127.0.0.1:8787",
		[]contract.ResourceRef{{Kind: "progress_digest", ID: "project"}})
	binding.AllowedObservedTypes = []string{"progress_digest.write_candidate.observed"}
	rc, err := LocalRuntimeConfigFromBindings([]access.ChannelBinding{binding}, nil)
	if err != nil {
		t.Fatalf("boot config: %v", err)
	}
	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "empty.db"), rc)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()

	v, err := BuildTowerView(rt, []access.ChannelBinding{binding})
	if err != nil {
		t.Fatalf("build tower view: %v", err)
	}
	if len(v.Goal.Statements) != 0 || len(v.Ledger.Decisions) != 0 {
		t.Fatalf("empty runtime must yield empty pages, got goal=%+v ledger=%+v", v.Goal, v.Ledger)
	}
}
