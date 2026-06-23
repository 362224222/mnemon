package main

import (
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/autopilot"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// Roles used by the scripted-brain tests. They are ordinary host-agent principals from
// codexTeamBindings; "leader/POC" is a stance (a routing brain), never a privileged kind.
const (
	loopWorker   = contract.ActorID("codex-01@appserver")
	loopPOC      = contract.ActorID("codex-02@appserver")
	loopReviewer = contract.ActorID("codex-03@appserver")
	loopOperator = contract.ActorID("human@owner")
)

// newLoopTestHarness builds a real in-process runtime (3 host-agents + operator, wide
// project-level scope) and the scripted brains for the one-hop chain. The POC brain is the
// ONLY place a routing decision (an assignment) is made — exactly as the model requires.
func newLoopTestHarness(t *testing.T, withPOC bool) (*codexTeamRuntimeHandle, *autopilot.Loop) {
	t.Helper()
	dir := t.TempDir()
	bindings, tokens, err := codexTeamBindings(3, "http://127.0.0.1:0")
	if err != nil {
		t.Fatalf("bindings: %v", err)
	}
	handle, err := newCodexTeamRuntimeHandle(filepath.Join(dir, "governed.db"), filepath.Join(dir, "dynamic"), bindings, tokens)
	if err != nil {
		t.Fatalf("runtime handle: %v", err)
	}
	t.Cleanup(func() { _ = handle.Close() })

	// worker: once it sees the goal (project_intent), it reports progress ONCE (idempotent ExternalID).
	worker := autopilot.Scripted(loopWorker, func(pkt autopilot.TurnPacket) []contract.ObservationEnvelope {
		if !autopilot.ProjectionHasKind(pkt.Projection, "project_intent") {
			return nil
		}
		return []contract.ObservationEnvelope{autopilot.Observe("progress_digest.write_candidate.observed", "worker-report-1",
			map[string]any{"summary": "worker: built feature X", "evidence": "compiled and ran"})}
	})

	// POC: the routing brain. For every worker progress item, it emits a GOVERNED assignment
	// routing a review to the reviewer. THIS is the "who acts next" decision — in a governed event.
	poc := autopilot.Scripted(loopPOC, func(pkt autopilot.TurnPacket) []contract.ObservationEnvelope {
		var out []contract.ObservationEnvelope
		for _, item := range autopilot.ProjectionItems(pkt.Projection, "progress_digest") {
			if autopilot.ItemStr(item, "actor") != string(loopWorker) {
				continue
			}
			id := autopilot.ItemStr(item, "id")
			out = append(out, autopilot.Observe("assignment.write_candidate.observed", "route-"+id,
				map[string]any{"scope": "review: " + autopilot.ItemStr(item, "summary"), "ttl": "30m",
					"assignee": string(loopReviewer), "expected_work": "review: " + autopilot.ItemStr(item, "summary"),
					"expected_feedback": "progress_digest with review result", "evidence": "routed by poc from " + id}))
		}
		return out
	})

	// reviewer: acts ONLY on an assignment addressed to it, then reports the review.
	reviewer := autopilot.Scripted(loopReviewer, func(pkt autopilot.TurnPacket) []contract.ObservationEnvelope {
		var out []contract.ObservationEnvelope
		for _, item := range autopilot.ProjectionItems(pkt.Projection, "assignment") {
			if autopilot.ItemStr(item, "assignee") != string(loopReviewer) {
				continue
			}
			id := autopilot.ItemStr(item, "id")
			out = append(out, autopilot.Observe("progress_digest.write_candidate.observed", "review-"+id,
				map[string]any{"summary": "reviewer: reviewed " + autopilot.ItemStr(item, "scope"), "evidence": "checked claim " + id}))
		}
		return out
	})

	brains := []autopilot.Agent{worker, reviewer}
	if withPOC {
		brains = []autopilot.Agent{worker, poc, reviewer}
	}
	loop := autopilot.NewLoop(handle, bindings, brains...)
	return handle, loop
}

// kickoff seeds ONE project_intent under the operator — the human handing the cluster a goal.
func kickoff(t *testing.T, handle *codexTeamRuntimeHandle) {
	t.Helper()
	_, _, _, err := handle.Submit(loopOperator, autopilot.Observe("project_intent.write_candidate.observed", "kickoff",
		map[string]any{"statement": "ship feature X", "evidence": "goal from human"}))
	if err != nil {
		t.Fatalf("seed project_intent: %v", err)
	}
}

// TestGovernedLoopSelfContinues is the core acceptance test: from ONE seeded goal, the
// cluster self-continues — worker report -> POC routes via assignment -> reviewer acts —
// and the whole chain is reconstructable from the decision ledger, with the routing
// assignment authored by the POC (not the engine).
func TestGovernedLoopSelfContinues(t *testing.T) {
	handle, loop := newLoopTestHarness(t, true)
	kickoff(t, handle)

	if _, err := loop.Run(50); err != nil {
		t.Fatalf("loop run: %v", err)
	}

	ledger, err := handle.DecisionLedger()
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}

	intent, ok := acceptedWrite(ledger, loopOperator, "project_intent")
	if !ok {
		t.Fatalf("missing accepted project_intent kickoff; ledger=%s", ledgerDump(ledger))
	}
	report, ok := acceptedWrite(ledger, loopWorker, "progress_digest")
	if !ok {
		t.Fatalf("missing accepted worker report; ledger=%s", ledgerDump(ledger))
	}
	route, ok := acceptedWrite(ledger, loopPOC, "assignment")
	if !ok {
		t.Fatalf("missing accepted POC routing assignment; ledger=%s", ledgerDump(ledger))
	}
	review, ok := acceptedWrite(ledger, loopReviewer, "progress_digest")
	if !ok {
		t.Fatalf("missing accepted reviewer review; ledger=%s", ledgerDump(ledger))
	}

	// The chain must be causally ordered: goal < report < routing < review (IngestSeq is the clock).
	if !(intent.IngestSeq < report.IngestSeq && report.IngestSeq < route.IngestSeq && route.IngestSeq < review.IngestSeq) {
		t.Fatalf("chain not ordered by IngestSeq: intent=%d report=%d route=%d review=%d",
			intent.IngestSeq, report.IngestSeq, route.IngestSeq, review.IngestSeq)
	}

	// The routing decision is authored by the POC principal — proving the "who acts next"
	// decision is a governed event from a peer agent, not engine orchestration.
	if route.Actor != loopPOC {
		t.Fatalf("routing assignment author = %q, want POC %q", route.Actor, loopPOC)
	}
}

// TestGovernedLoopRoutingLivesInBrain proves the routing decision lives in the POC brain,
// not the engine: with the POC brain removed, the SAME engine produces no assignment and no
// review — the chain breaks. (If the engine routed, the chain would survive.)
func TestGovernedLoopRoutingLivesInBrain(t *testing.T) {
	handle, loop := newLoopTestHarness(t, false) // no POC brain
	kickoff(t, handle)

	if _, err := loop.Run(50); err != nil {
		t.Fatalf("loop run: %v", err)
	}
	ledger, err := handle.DecisionLedger()
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}

	// Worker still reports (it self-continues off the goal)...
	if _, ok := acceptedWrite(ledger, loopWorker, "progress_digest"); !ok {
		t.Fatalf("worker should still report; ledger=%s", ledgerDump(ledger))
	}
	// ...but with no POC routing brain, no assignment is ever authored...
	if _, ok := acceptedWrite(ledger, loopPOC, "assignment"); ok {
		t.Fatalf("no POC brain, yet an assignment was authored — routing leaked into the engine")
	}
	// ...so the reviewer is never nudged into action.
	if _, ok := acceptedWrite(ledger, loopReviewer, "progress_digest"); ok {
		t.Fatalf("reviewer acted with no routing assignment — chain should have broken")
	}
}

// acceptedWrite finds an Accepted decision authored by actor that wrote a resource of kind.
func acceptedWrite(ledger []contract.Decision, actor contract.ActorID, kind contract.ResourceKind) (contract.Decision, bool) {
	for _, d := range ledger {
		if d.Status != contract.Accepted || d.Actor != actor {
			continue
		}
		for _, nv := range d.NewVersions {
			if nv.Ref.Kind == kind {
				return d, true
			}
		}
	}
	return contract.Decision{}, false
}

func ledgerDump(ledger []contract.Decision) string {
	out := ""
	for _, d := range ledger {
		kinds := ""
		for _, nv := range d.NewVersions {
			kinds += string(nv.Ref.Kind) + " "
		}
		out += "\n  seq=" + itoa(d.IngestSeq) + " actor=" + string(d.Actor) + " status=" + string(d.Status) + " wrote=[" + kinds + "]"
	}
	return out
}

// avoid importing strconv just for the dump helper
func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// TestGovernedLoopDemoScenario runs the shipped 5-agent / 2-POC demo brains end to end and
// asserts the full multi-hop self-continuation chain, then validates the human-facing snapshot.
func TestGovernedLoopDemoScenario(t *testing.T) {
	dir := t.TempDir()
	bindings, tokens, err := codexTeamBindings(5, "http://127.0.0.1:0")
	if err != nil {
		t.Fatalf("bindings: %v", err)
	}
	handle, err := newCodexTeamRuntimeHandle(filepath.Join(dir, "governed.db"), filepath.Join(dir, "dynamic"), bindings, tokens)
	if err != nil {
		t.Fatalf("runtime handle: %v", err)
	}
	t.Cleanup(func() { _ = handle.Close() })

	cfg := defaultLoopDemoConfig()
	loop := autopilot.NewLoop(handle, bindings, codexLoopDemoBrains(cfg)...)
	if _, _, _, err := handle.Submit(cfg.Operator, autopilot.Observe("project_intent.write_candidate.observed", "goal",
		map[string]any{"statement": "ship feature X", "evidence": "goal"})); err != nil {
		t.Fatalf("seed goal: %v", err)
	}
	if _, err := loop.Run(50); err != nil {
		t.Fatalf("loop run: %v", err)
	}

	ledger, err := handle.DecisionLedger()
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	// The multi-hop chain: planner reports, poc-build routes to builder, builder reports,
	// poc-review routes to reviewer, reviewer reports.
	for _, want := range []struct {
		actor contract.ActorID
		kind  contract.ResourceKind
		desc  string
	}{
		{cfg.Planner, "progress_digest", "planner report"},
		{cfg.PocBuild, "assignment", "poc-build routing"},
		{cfg.Builder, "progress_digest", "builder report"},
		{cfg.PocReview, "assignment", "poc-review routing"},
		{cfg.Reviewer, "progress_digest", "reviewer report"},
	} {
		if _, ok := acceptedWrite(ledger, want.actor, want.kind); !ok {
			t.Fatalf("missing %s (%s by %s); ledger=%s", want.desc, want.kind, want.actor, ledgerDump(ledger))
		}
	}

	// Snapshot must reflect the chain with exactly two POC routing assignments and quiescence.
	snap, err := buildLoopSnapshot(handle, loop, cfg, "ship feature X")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snap.Routes != 2 {
		t.Fatalf("snapshot routes = %d, want 2 (one per POC); chain=%+v", snap.Routes, snap.Chain)
	}
	if !snap.Quiescent {
		t.Fatalf("snapshot should be quiescent after Run returns")
	}
	if len(snap.Agents) != 5 {
		t.Fatalf("snapshot agents = %d, want 5", len(snap.Agents))
	}
	// Chain must be ordered by IngestSeq (it is the clock).
	for i := 1; i < len(snap.Chain); i++ {
		if snap.Chain[i].Seq < snap.Chain[i-1].Seq {
			t.Fatalf("chain not ordered by seq at %d: %+v", i, snap.Chain)
		}
	}
}
