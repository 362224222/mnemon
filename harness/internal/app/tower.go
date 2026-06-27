package app

import (
	"fmt"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

// TowerView is the read-only, operator-wide projection that backs the Agent Control Tower's four pages
// (P6). The app-layer facade assembles it from the *Runtime's read surfaces; the ui package renders a
// TowerView and never touches the store/kernel (the ui↛store law). Zero new kernel concepts — every
// field maps to an existing protocol object (§3.1). READ-ONLY: building a view never writes or Ticks.
type TowerView struct {
	Goal   GoalPage
	Field  FieldPage
	Inbox  InboxPage
	Ledger LedgerPage
}

// FieldPage answers "场上谁在干什么": the agents on the field (enumerated from the BindingSet — the only
// existing "who's on the field" source), the live assignments (scope/assignee/lease TTL), and the
// open-escalation count. There is deliberately NO "● active / ○ idle" liveness or "evt/h" rate: the
// data model has no heartbeat/last-seen concept, and inventing one would be a new kernel concept
// (T1 veto, folded from the adversarial review).
type FieldPage struct {
	Agents      []AgentRow
	Assignments []AssignmentRow
	Diagnostics int // open escalations (the INBOX count, surfaced on FIELD too)
}

// AgentRow is one bound principal on the field.
type AgentRow struct {
	Principal contract.ActorID
	Kind      contract.ActorKind
}

// AssignmentRow is one live assignment (who's doing what, with its lease TTL).
type AssignmentRow struct {
	Scope    string
	Assignee string
	TTL      string
}

// InboxPage answers "什么需要我": the open escalations — high-risk/denied candidates surfaced as
// diagnostics. The operator acts by RE-OBSERVING the underlying candidate as a control-agent (P6b),
// not by "approving a proposal" (no such kernel verb). CausedBy links each escalation to its
// triggering candidate event.
type InboxPage struct {
	Escalations []InboxRow
}

// InboxRow is one escalation (a durable diagnostic) awaiting operator attention.
type InboxRow struct {
	Domain   string // the kind domain (e.g. "approval", "assignment")
	Actor    contract.ActorID
	Stage    string
	Reason   string
	CausedBy string // the triggering candidate event ID (the re-observation target, P6b)
}

// GoalPage answers "目标怎么样了": the project_intent statements (the goal) and the progress_digest
// summaries. "readiness" is shown as the ACTUAL progress entries — a fabricated percentage would need
// a KR data model that does not exist, and inventing one would be a new kernel concept (T1 veto).
type GoalPage struct {
	Statements []string // project_intent items' statements
	Progress   []string // progress_digest items' summaries
}

// LedgerRow is one accepted decision with its attribution (the proposer + what it changed).
type LedgerRow struct {
	DecisionID string
	Actor      contract.ActorID
	AppliedAt  string
	Refs       []contract.ResourceRef
}

// LedgerPage answers "什么已经定了": the accepted decisions, newest last (append order).
type LedgerPage struct {
	Decisions []LedgerRow
}

// towerScopeID is the default coordination scope every coordination kind is bound at ("project").
const towerScopeID = contract.ResourceID("project")

// BuildTowerView assembles the read-only Tower projection from the runtime + the BindingSet. It
// performs only resource reads, the read-only DecisionLedger, and an event-log scan — never a write or
// a Tick (G10/T5). The bindings supply the FIELD "who's on the field" enumeration (the only existing
// source); the ui package renders the result and never touches the store (ui↛store).
func BuildTowerView(rt *runtime.Runtime, bindings []access.ChannelBinding) (TowerView, error) {
	var v TowerView
	// GOAL: project_intent statements + progress_digest summaries (read-only resource reads; an
	// absent resource — version 0 — simply yields no entries).
	if ver, fields, err := rt.Resource(contract.ResourceRef{Kind: "project_intent", ID: towerScopeID}); err == nil && ver > 0 {
		v.Goal.Statements = towerItemStrings(fields, "items", "statement")
	}
	if ver, fields, err := rt.Resource(contract.ResourceRef{Kind: "progress_digest", ID: towerScopeID}); err == nil && ver > 0 {
		v.Goal.Progress = towerItemStrings(fields, "items", "summary")
	}

	// FIELD: agents from the BindingSet; live assignments from the assignment resource.
	for _, b := range bindings {
		v.Field.Agents = append(v.Field.Agents, AgentRow{Principal: b.Principal, Kind: b.ActorKind})
	}
	if ver, fields, err := rt.Resource(contract.ResourceRef{Kind: "assignment", ID: towerScopeID}); err == nil && ver > 0 {
		if raw, ok := fields["items"].([]any); ok {
			for _, r := range raw {
				if m, ok := r.(map[string]any); ok {
					scope := towerItemString(m, "scope")
					assignee := towerItemString(m, "assignee")
					ttl := towerItemString(m, "ttl")
					v.Field.Assignments = append(v.Field.Assignments, AssignmentRow{Scope: scope, Assignee: assignee, TTL: ttl})
				}
			}
		}
	}

	// INBOX: open escalations from the durable .diagnostic events (a denied/high-risk candidate
	// surfaces as a diagnostic, never silently dropped). CausedBy links to the re-observation target.
	events, err := rt.PendingEvents(0)
	if err != nil {
		return v, err
	}
	for _, ev := range events {
		if !strings.HasSuffix(ev.Type, ".diagnostic") {
			continue
		}
		stage, _ := ev.Payload["stage"].(string)
		reason, _ := ev.Payload["reason"].(string)
		v.Inbox.Escalations = append(v.Inbox.Escalations, InboxRow{
			Domain:   strings.TrimSuffix(ev.Type, ".diagnostic"),
			Actor:    ev.Actor,
			Stage:    stage,
			Reason:   reason,
			CausedBy: ev.CausedBy,
		})
	}
	v.Field.Diagnostics = len(v.Inbox.Escalations)

	// LEDGER: accepted decisions with attribution.
	decisions, err := rt.DecisionLedger()
	if err != nil {
		return v, err
	}
	for _, d := range decisions {
		if d.Status != contract.Accepted {
			continue
		}
		refs := make([]contract.ResourceRef, 0, len(d.NewVersions))
		for _, nv := range d.NewVersions {
			refs = append(refs, nv.Ref)
		}
		v.Ledger.Decisions = append(v.Ledger.Decisions, LedgerRow{
			DecisionID: d.DecisionID, Actor: d.Actor, AppliedAt: d.AppliedAt, Refs: refs,
		})
	}
	return v, nil
}

// towerItemStrings extracts a string field from each item in fields[itemsField] (the canonical []any
// of map[string]any shape). Absent/typeless items yield an empty slice (no panic).
func towerItemStrings(fields map[string]any, itemsField, field string) []string {
	raw, ok := fields[itemsField].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		if m, ok := r.(map[string]any); ok {
			if s := towerItemString(m, field); s != "" {
				out = append(out, s)
			}
		}
	}
	return out
}

func towerItemString(item map[string]any, key string) string {
	if s, ok := item[key].(string); ok && s != "" {
		return s
	}
	for _, section := range []string{eventmodel.PayloadRuleKey, eventmodel.PayloadNarrativeKey, eventmodel.PayloadRefsKey} {
		if m, ok := item[section].(map[string]any); ok {
			if s, ok := m[key].(string); ok && s != "" {
				return s
			}
		}
	}
	return ""
}

// ReobserveCandidate is the Tower's ONLY write action (P6b): it resolves an INBOX escalation by
// RE-OBSERVING the underlying candidate as the operator (a control-agent principal) — the action that
// clears a high-risk operator-gate denial (RiskOperatorGate exempts the control-agent). It is NOT
// "approve a proposal": no such kernel verb exists, and the wire rejects *.proposed/*.diagnostic. The
// original observed candidate is recovered from the durable event log by the escalation's CausedBy id
// and re-emitted through the SAME Ingest path under the operator (so the operator's binding governs
// it, G9). It fails loud if CausedBy does not name an OBSERVED candidate — the Tower never even
// attempts to ingest a trusted internal event. Idempotent per escalation (the re-observe ExternalID
// derives from CausedBy, so re-observing the same escalation twice dedups).
func ReobserveCandidate(rt *runtime.Runtime, operator contract.ActorID, escalation InboxRow) error {
	if escalation.CausedBy == "" {
		return fmt.Errorf("tower: escalation %q has no causing candidate to re-observe", escalation.Domain)
	}
	events, err := rt.PendingEvents(0)
	if err != nil {
		return err
	}
	var candidate *contract.Event
	for i := range events {
		if events[i].ID == escalation.CausedBy {
			candidate = &events[i]
			break
		}
	}
	if candidate == nil {
		return fmt.Errorf("tower: candidate event %q not found in the log", escalation.CausedBy)
	}
	// T3: re-observe ONLY an observed candidate — never a trusted internal event. A *.proposed or
	// *.diagnostic does not end in ".observed"; fail loud here so the Tower never even tries (the wire
	// would reject it anyway), keeping the "no backdoor ingest" guarantee explicit at the facade.
	if !strings.HasSuffix(candidate.Type, ".observed") {
		return fmt.Errorf("tower: refuse to re-observe non-candidate event type %q", candidate.Type)
	}
	_, _, err = rt.API().Ingest(operator, contract.ObservationEnvelope{
		ExternalID: "tower-reobserve:" + escalation.CausedBy,
		Event:      contract.Event{Type: candidate.Type, Payload: candidate.Payload, CorrelationID: candidate.CorrelationID},
	})
	return err
}
