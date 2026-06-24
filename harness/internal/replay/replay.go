// Package replay re-derives decisions from the canonical event log on a throwaway kernel (event-sourcing
// purity, S8): replay reads the log only, never advances a live cursor or writes a live store, and its
// determinism is established by FIELD-MASKING the dynamic decision fields (DecisionID/AppliedAt) before any
// diff (D1) — production decisions keep their real uuid/time. replay imports rule (one-way, D11).
package replay

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/kernel"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation/view"
	"github.com/mnemon-dev/mnemon/harness/internal/reconcile"
	"github.com/mnemon-dev/mnemon/harness/internal/rule"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

// canonicalModes is the fixed policy replay reconciles under. It SHARES contract.DefaultModes()
// with the live server's zero-config default (one source of truth, pinned by
// TestReplayModesMatchServerDefault) — replay under different conflict semantics could accept
// what live rejected, silently breaking I6.
var canonicalModes = contract.DefaultModes()

func isProposal(ev contract.Event) bool { return strings.HasSuffix(ev.Type, ".proposed") }

// ordered returns a defensive copy of events stable-sorted by IngestSeq — the contract's ONE
// ordering key (decision-contract-v1.md). Replay and Shadow must not depend on the caller's slice
// order: an unordered log fed to Shadow could apply a later proposal before an earlier observed
// event, corrupting the dispatch-time view into a false-clean/false-dirty.
func ordered(events []contract.Event) []contract.Event {
	out := append([]contract.Event(nil), events...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].IngestSeq < out[j].IngestSeq })
	return out
}

// logWriteKinds is the set of resource kinds the log's proposals write — the kinds replay's kernel
// must both KNOW (schema) and AUTHORIZE so it never adds a rejection the live run did not have.
// Live only ever proposes a kind its enabled capabilities registered, so this is exactly the kinds
// live accepted writes for; sourcing them from the log (not a compiled catalog) keeps replay
// deterministic however the LIVE run's user kinds are sourced — a graduated memory/skill or an
// external declared kind replays identically (I6, D3/PD5). The write extraction mirrors
// reconcile.opFromEvent's JSON round-trip (Payload["writes"] decodes to []any after AppendEvent).
func logWriteKinds(events []contract.Event) []contract.ResourceKind {
	seen := map[contract.ResourceKind]bool{}
	var kinds []contract.ResourceKind
	for _, ev := range events {
		raw, ok := ev.Payload["writes"]
		if !ok {
			continue
		}
		b, err := json.Marshal(raw)
		if err != nil {
			continue
		}
		var writes []contract.ResourceWrite
		if json.Unmarshal(b, &writes) != nil {
			continue
		}
		for _, w := range writes {
			if !seen[w.Ref.Kind] {
				seen[w.Ref.Kind] = true
				kinds = append(kinds, w.Ref.Kind)
			}
		}
	}
	return kinds
}

// logSchemaGuard registers every log-write kind with NO required fields: replay re-derives and
// never re-polices, so a kind whose writes were live-accepted (carrying their fields) must validate.
func logSchemaGuard(events []contract.Event) kernel.SchemaGuard {
	required := map[contract.ResourceKind][]string{}
	for _, k := range logWriteKinds(events) {
		required[k] = nil
	}
	return kernel.SchemaGuard{Required: required}
}

// permissiveAuthority lets every actor that appears in the events write every log-write kind, so replay does
// not introduce authz rejections the live run did not have (the live authority is reproduced by the events
// themselves having been accepted; replay only re-derives, it does not re-police).
func permissiveAuthority(events []contract.Event) kernel.AuthorityRules {
	kinds := logWriteKinds(events)
	allow := map[contract.ActorID][]contract.ResourceKind{}
	for _, ev := range events {
		if _, ok := allow[ev.Actor]; !ok {
			allow[ev.Actor] = kinds
		}
	}
	return kernel.AuthorityRules{Allow: allow}
}

// Replay re-derives the decisions by reconciling the *.proposed events of the log over a FRESH :memory:
// kernel. It is a pure function of the events (no live store), reproducing the live decisions up to the
// masked dynamic fields. The candidate ruleset is retained for signature symmetry with Shadow — pure replay
// needs no policy because the logged proposals are authoritative (event-sourcing).
func Replay(events []contract.Event, candidate rule.RuleSet) []contract.Decision {
	return drive(ordered(events))
}

// PROOF-ONLY: S7/S8 shadow-promotion spec proof; no production caller yet — see .insight
// Shadow asks the governance question "would promoting this candidate rule set change behavior?" by RE-RUNNING
// both policies' rules over the OBSERVED events of the log and diffing their rule results (S8). This is the
// faithful model: a rule handles OBSERVED events and EMITS proposals/denies/jobs — so the candidate's behavior
// change lives in observed->decision, NOT in re-reconciling the already-minted *.proposed events (the prior
// model never ran the candidate's rules at all, so every real rule change passed Clean).
//
// View TIMING matches the server, which evaluates rules at DISPATCH time — BEFORE that tick's reconcile. Shadow
// walks the log in IngestSeq order on a throwaway kernel: a logged *.proposed event is applied (reconciled) to
// evolve canonical state; an OBSERVED event is evaluated against the state BUILT FROM THE PROPOSALS THAT PRECEDE
// IT in the log — i.e. the dispatch-time state, not the final state (evaluating against final state yields a
// false-clean for any version-sensitive rule that diverges at @1 but agrees at @2). Only the logged proposals
// mutate the kernel; the rule evaluations are read-only (S8).
//
// The comparison covers verdict + proposal (type+payload) + job + trusted origin actor + the decision REASONS
// + the rule DIAGNOSTICS. Reasons are NOT advisory: the server writes them verbatim into durable *.diagnostic
// events for deny/warn (the auditable S7 trail), so a Reasons-only reword (e.g. blanking a security deny
// reason) IS a behavior change. Diagnostics likewise are durable: a candidate that errors or returns a
// borrowed-emit proposal reduces to Verdict allow but emits one. It reports diffs, never pass/fail (the
// operator gates promotion on Clean).
func Shadow(events []contract.Event, subs map[contract.ActorID]contract.Subscription, live, candidate rule.RuleSet) rule.ShadowReport {
	s, err := store.OpenStore(":memory:")
	if err != nil {
		return rule.ShadowReport{}
	}
	defer s.Close()
	k := kernel.NewKernel(s, logSchemaGuard(events), permissiveAuthority(events))
	r := reconcile.NewReconciler(s, k)

	diffs := 0
	for _, ev := range ordered(events) {
		if isProposal(ev) {
			// evolve the canonical state at dispatch granularity: apply this proposal so observed events LATER in
			// the log see it, while observed events BEFORE it (already compared) saw the pre-proposal state.
			if _, err := s.AppendEvent(ev); err == nil {
				r.RunOnce(canonicalModes)
			}
			continue
		}
		if strings.HasSuffix(ev.Type, ".diagnostic") {
			continue
		}
		// OBSERVED event: evaluate BOTH policies against the current (dispatch-time) scoped view.
		view := view.ScopedView(s, subs[ev.Actor])
		in := rule.RuleInput{Event: ev, View: view}
		ld, ldiag := live.Evaluate(in)
		cd, cdiag := candidate.Evaluate(in)
		if canonicalRuleResult(ld, ldiag) != canonicalRuleResult(cd, cdiag) {
			diffs++
		}
	}
	return rule.ShadowReport{Clean: diffs == 0, Diffs: diffs}
}

// canonicalRuleResult serializes the behaviorally-meaningful output of a rule evaluation — the decision
// (verdict, proposal type+payload, trusted origin actor), the REASONS (the server writes these verbatim
// into durable deny/warn *.diagnostic events, so they are auditable state, not advisory), AND the durable
// diagnostics — to a stable string for comparison. json.Marshal sorts map keys, so equal payloads compare equal.
func canonicalRuleResult(d contract.RuleDecision, diags []contract.Diagnostic) string {
	v := struct {
		Verdict     contract.RuleVerdict
		Proposal    *contract.ProposedEvent
		Actor       contract.ActorID
		Reasons     []string
		Diagnostics []contract.Diagnostic
	}{d.Verdict, d.Proposal, d.ProposalActor, d.Reasons, diags}
	b, err := json.Marshal(v)
	if err == nil {
		return string(b)
	}
	// A non-finite float (NaN/Inf — legal in a Proposal payload, settable by a native
	// rule) makes json.Marshal fail. Do NOT collapse to "" (the zero value when the error is dropped): two
	// DIVERGENT decisions would both render "" and compare equal, masking a reason/verdict change as Clean.
	// Fall back to a Go-syntax rendering — but FLATTEN the Proposal pointer to a value first, because %#v
	// prints a NESTED pointer field as a heap ADDRESS (non-deterministic) rather than its dereferenced value.
	// On the flattened, pointer-free struct, fmt renders NaN/Inf as "NaN"/"+Inf" and sorts map keys, so the
	// canonical form handles every float value AND distinguishes every field deterministically.
	flat := struct {
		Verdict     contract.RuleVerdict
		Proposal    contract.ProposedEvent
		HasProposal bool
		Actor       contract.ActorID
		Reasons     []string
		Diagnostics []contract.Diagnostic
	}{Verdict: d.Verdict, Actor: d.ProposalActor, Reasons: d.Reasons, Diagnostics: diags}
	if d.Proposal != nil {
		flat.Proposal, flat.HasProposal = *d.Proposal, true
	}
	return "nonjson:" + fmt.Sprintf("%#v", flat)
}

// drive replays the events on a throwaway kernel and returns the reconciler's decisions (event-sourcing
// reproduce-from-log: the logged proposals are authoritative). It never touches a live store/cursor.
func drive(events []contract.Event) []contract.Decision {
	s, err := store.OpenStore(":memory:")
	if err != nil {
		return nil
	}
	defer s.Close()
	k := kernel.NewKernel(s, logSchemaGuard(events), permissiveAuthority(events))
	r := reconcile.NewReconciler(s, k)
	for _, ev := range events {
		if _, err := s.AppendEvent(ev); err != nil {
			continue
		}
	}
	return r.RunOnce(canonicalModes)
}

// maskDynamic zeros the per-run dynamic fields and sorts the order-insensitive slices so two decisions for
// the same logical outcome compare equal regardless of uuid/time/ordering (D1).
func maskDynamic(d contract.Decision) contract.Decision {
	d.DecisionID = ""
	d.AppliedAt = ""
	sort.Slice(d.Conflicts, func(i, j int) bool {
		return string(d.Conflicts[i].Ref.Kind)+string(d.Conflicts[i].Ref.ID) < string(d.Conflicts[j].Ref.Kind)+string(d.Conflicts[j].Ref.ID)
	})
	sort.Slice(d.NewVersions, func(i, j int) bool {
		return string(d.NewVersions[i].Ref.Kind)+string(d.NewVersions[i].Ref.ID) < string(d.NewVersions[j].Ref.Kind)+string(d.NewVersions[j].Ref.ID)
	})
	return d
}
