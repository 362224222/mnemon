// Package ui is the Control Tower's presentation layer (P6). It renders the read-only app.TowerView
// and drives a pure state machine over it. Per the ui↛store law (T2), this package imports ONLY the
// app facade (the TowerView data + the re-observe action) and contract — never store/kernel/runtime.
// The Model is pure: it holds no *Runtime and performs no writes; a re-observe surfaces as an INTENT
// that the command layer executes (against a fresh view — the concurrency re-check).
package ui

import (
	"fmt"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/app"
)

// TowerPage is one of the Control Tower's four pages (§3.3 IA).
type TowerPage int

const (
	PageGoal TowerPage = iota
	PageField
	PageInbox
	PageLedger
)

var pageTitles = [...]string{"GOAL", "FIELD", "INBOX", "LEDGER"}

// Title is the page's sanctioned name (the vocabulary the lint enforces).
func (p TowerPage) Title() string { return pageTitles[p] }

// TowerAction is a state-machine input. The Tower offers ONLY these; the Model enforces legality (T4)
// — an action illegal in the current state is a pure no-op, never an out-of-bounds or a forbidden write.
type TowerAction int

const (
	ActionNextPage TowerAction = iota
	ActionPrevPage
	ActionCursorDown
	ActionCursorUp
	ActionReobserve // INBOX only: re-observe the selected escalation (returns an intent)
	ActionDismiss   // INBOX only: read-side acknowledge the selected escalation (no kernel write)
)

// ReobserveIntent is the Model's request for the COMMAND layer to perform the side-effecting write.
// The Model never writes; the command executes app.ReobserveCandidate against a freshly-built view.
type ReobserveIntent struct {
	Escalation app.InboxRow
}

// TowerModel is the pure state machine over a TowerView snapshot: the active page, the INBOX cursor,
// and the read-side-dismissed escalations. All transitions are pure (no shared mutation, no I/O).
type TowerModel struct {
	view  app.TowerView
	page  TowerPage
	inbox int             // cursor over the OPEN (un-dismissed) INBOX escalations
	acked map[string]bool // dismissed escalations, keyed by CausedBy (read-side ack)
}

// NewTowerModel seeds a model on the GOAL page from a freshly-built view.
func NewTowerModel(view app.TowerView) TowerModel {
	return TowerModel{view: view, page: PageGoal, acked: map[string]bool{}}
}

// Page reports the active page (for the command/tests).
func (m TowerModel) Page() TowerPage { return m.page }

// openEscalations is the INBOX list MINUS the read-side-dismissed ones.
func (m TowerModel) openEscalations() []app.InboxRow {
	var out []app.InboxRow
	for _, e := range m.view.Inbox.Escalations {
		if !m.acked[e.CausedBy] {
			out = append(out, e)
		}
	}
	return out
}

// Update applies an action and returns the next model plus an optional re-observe intent (only when a
// legal ActionReobserve fires on a selectable, re-observable escalation). Illegal actions are pure
// no-ops (T4): a cursor move off an empty/non-INBOX page does nothing; a re-observe off INBOX yields
// no intent.
func (m TowerModel) Update(a TowerAction) (TowerModel, *ReobserveIntent) {
	switch a {
	case ActionNextPage:
		m.page = TowerPage(clamp(int(m.page)+1, 0, len(pageTitles)-1))
	case ActionPrevPage:
		m.page = TowerPage(clamp(int(m.page)-1, 0, len(pageTitles)-1))
	case ActionCursorDown:
		if m.page == PageInbox {
			m.inbox = clamp(m.inbox+1, 0, len(m.openEscalations())-1)
		}
	case ActionCursorUp:
		if m.page == PageInbox {
			m.inbox = clamp(m.inbox-1, 0, len(m.openEscalations())-1)
		}
	case ActionReobserve:
		if open := m.openEscalations(); m.page == PageInbox && m.inbox >= 0 && m.inbox < len(open) {
			if esc := open[m.inbox]; esc.CausedBy != "" {
				return m, &ReobserveIntent{Escalation: esc}
			}
		}
	case ActionDismiss:
		if open := m.openEscalations(); m.page == PageInbox && m.inbox >= 0 && m.inbox < len(open) {
			// copy-on-write so a "previous" model retains its own ack set (keeps Update pure).
			acked := make(map[string]bool, len(m.acked)+1)
			for k, v := range m.acked {
				acked[k] = v
			}
			acked[open[m.inbox].CausedBy] = true
			m.acked = acked
			m.inbox = clamp(m.inbox, 0, len(m.openEscalations())-1)
		}
	}
	return m, nil
}

// Render draws the active page beneath a tab bar naming all four pages (so every page title is always
// present — the headless --dump acceptance). It is a pure read of the snapshot.
func (m TowerModel) Render() string {
	var b strings.Builder
	tabs := make([]string, len(pageTitles))
	for i, t := range pageTitles {
		if TowerPage(i) == m.page {
			tabs[i] = "[" + t + "]"
		} else {
			tabs[i] = " " + t + " "
		}
	}
	b.WriteString(strings.Join(tabs, " ") + "\n\n")
	b.WriteString(m.renderPage(m.page))
	return b.String()
}

// RenderAll draws every page body (the full --dump snapshot — four pages, all titles + content).
func (m TowerModel) RenderAll() string {
	var b strings.Builder
	for i := range pageTitles {
		b.WriteString(m.renderPage(TowerPage(i)))
		b.WriteString("\n")
	}
	return b.String()
}

func (m TowerModel) renderPage(p TowerPage) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# %s\n", p.Title())
	switch p {
	case PageGoal:
		if len(m.view.Goal.Statements) == 0 {
			b.WriteString("  (no project intent)\n")
		}
		for _, s := range m.view.Goal.Statements {
			fmt.Fprintf(&b, "  intent: %s\n", s)
		}
		for _, s := range m.view.Goal.Progress {
			fmt.Fprintf(&b, "  progress: %s\n", s)
		}
	case PageField:
		for _, a := range m.view.Field.Agents {
			fmt.Fprintf(&b, "  agent: %s (%s)\n", a.Principal, a.Kind)
		}
		for _, as := range m.view.Field.Assignments {
			fmt.Fprintf(&b, "  assignment: %s -> %s (lease %s)\n", as.Scope, as.Assignee, as.TTL)
		}
		fmt.Fprintf(&b, "  escalations: %d\n", m.view.Field.Diagnostics)
	case PageInbox:
		open := m.openEscalations()
		if len(open) == 0 {
			b.WriteString("  (inbox clear)\n")
		}
		for i, e := range open {
			cursor := "  "
			if i == m.inbox && m.page == PageInbox {
				cursor = "> "
			}
			fmt.Fprintf(&b, "%s%s: %s [%s] %s\n", cursor, e.Domain, e.Actor, e.Stage, e.Reason)
		}
	case PageLedger:
		if len(m.view.Ledger.Decisions) == 0 {
			b.WriteString("  (no decisions)\n")
		}
		for _, d := range m.view.Ledger.Decisions {
			refs := make([]string, len(d.Refs))
			for i, r := range d.Refs {
				refs[i] = string(r.Kind)
			}
			fmt.Fprintf(&b, "  %s by %s -> %s\n", d.DecisionID, d.Actor, strings.Join(refs, ","))
		}
	}
	return b.String()
}

// clamp bounds v to [lo,hi]; an empty list (hi<lo) clamps to lo.
func clamp(v, lo, hi int) int {
	if hi < lo {
		return lo
	}
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
