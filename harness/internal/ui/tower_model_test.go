package ui

import (
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/app"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

func sampleView() app.TowerView {
	return app.TowerView{
		Goal:  app.GoalPage{Statements: []string{"ship the beta"}, Progress: []string{"80% done"}},
		Field: app.FieldPage{Agents: []app.AgentRow{{Principal: "codex@project", Kind: "host-agent"}}, Diagnostics: 2},
		Inbox: app.InboxPage{Escalations: []app.InboxRow{
			{Domain: "approval", Actor: "codex@project", Stage: "rule", Reason: "needs operator", CausedBy: "ev-1"},
			{Domain: "approval", Actor: "codex@project", Stage: "rule", Reason: "needs operator", CausedBy: "ev-2"},
		}},
		Ledger: app.LedgerPage{Decisions: []app.LedgerRow{
			{DecisionID: "d-1", Actor: "codex@project", Refs: []contract.ResourceRef{{Kind: "memory"}}}}},
	}
}

// P6d: page navigation is bounded; the tab bar always names all four pages (the --dump acceptance).
func TestTowerModelPageNavAndRender(t *testing.T) {
	m := NewTowerModel(sampleView())
	if m.Page() != PageGoal {
		t.Fatalf("model must start on GOAL, got %v", m.Page())
	}
	// the tab bar names every page regardless of the active one
	r := m.Render()
	for _, title := range []string{"GOAL", "FIELD", "INBOX", "LEDGER"} {
		if !strings.Contains(r, title) {
			t.Fatalf("render must name page %q:\n%s", title, r)
		}
	}
	// next-page walks GOAL->FIELD->INBOX->LEDGER and clamps at the end
	m, _ = m.Update(ActionNextPage)
	m, _ = m.Update(ActionNextPage)
	if m.Page() != PageInbox {
		t.Fatalf("two next-pages from GOAL must land on INBOX, got %v", m.Page())
	}
	m, _ = m.Update(ActionNextPage) // LEDGER
	m, _ = m.Update(ActionNextPage) // clamp at LEDGER
	if m.Page() != PageLedger {
		t.Fatalf("next-page must clamp at LEDGER, got %v", m.Page())
	}
	// RenderAll carries every page body
	all := m.RenderAll()
	for _, want := range []string{"# GOAL", "ship the beta", "# FIELD", "codex@project", "# INBOX", "approval", "# LEDGER", "d-1"} {
		if !strings.Contains(all, want) {
			t.Fatalf("RenderAll missing %q:\n%s", want, all)
		}
	}
}

// P6d: on INBOX, the cursor selects an escalation; Reobserve returns an intent for the selection;
// Dismiss read-side-acks it (it leaves the open list, no kernel write).
func TestTowerModelInboxActions(t *testing.T) {
	m := NewTowerModel(sampleView())
	m, _ = m.Update(ActionNextPage)
	m, _ = m.Update(ActionNextPage) // INBOX

	// Reobserve on the first escalation returns its intent
	_, intent := m.Update(ActionReobserve)
	if intent == nil || intent.Escalation.CausedBy != "ev-1" {
		t.Fatalf("Reobserve must intent the selected escalation (ev-1), got %+v", intent)
	}
	// move cursor down, Reobserve targets ev-2
	m2, _ := m.Update(ActionCursorDown)
	_, intent = m2.Update(ActionReobserve)
	if intent == nil || intent.Escalation.CausedBy != "ev-2" {
		t.Fatalf("cursor-down then Reobserve must target ev-2, got %+v", intent)
	}
	// Dismiss the first escalation -> it leaves the open list; render no longer shows it
	m3, _ := m.Update(ActionDismiss)
	if got := m3.Render(); strings.Contains(got, "ev-1") {
		t.Fatalf("a dismissed escalation must leave the INBOX:\n%s", got)
	}
	// the dismissal is read-side only — the underlying view is unchanged (still 2 escalations)
	if len(m3.view.Inbox.Escalations) != 2 {
		t.Fatal("dismiss must be a read-side ack, never mutating the underlying view")
	}
}

// P6d / T4: an action illegal in the current state is a pure no-op — Reobserve off INBOX yields no intent.
func TestTowerModelLegality(t *testing.T) {
	m := NewTowerModel(sampleView()) // GOAL page
	if _, intent := m.Update(ActionReobserve); intent != nil {
		t.Fatalf("Reobserve off the INBOX page must be a no-op, got intent %+v", intent)
	}
	// cursor moves off INBOX do nothing
	m2, _ := m.Update(ActionCursorDown)
	if m2.inbox != 0 {
		t.Fatal("cursor move off INBOX must be a no-op")
	}
	// an empty INBOX: Reobserve is a no-op (no panic, no intent)
	empty := NewTowerModel(app.TowerView{})
	empty, _ = empty.Update(ActionNextPage)
	empty, _ = empty.Update(ActionNextPage) // INBOX (empty)
	if _, intent := empty.Update(ActionReobserve); intent != nil {
		t.Fatalf("Reobserve on an empty INBOX must yield no intent, got %+v", intent)
	}
}
