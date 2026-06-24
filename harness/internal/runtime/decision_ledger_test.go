package runtime

import (
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/policy"
)

// P6a-1: DecisionLedger is the operator-wide, READ-ONLY decision-log read that backs the Control
// Tower's LEDGER (accepted) page — the cross-actor history the per-actor PullEventView cannot serve.
// An accepted memory write lands as one Accepted decision attributed to the proposing principal; the
// ledger surfaces it. Reading is idempotent (no write path).
func TestDecisionLedgerSurfacesAcceptedDecisions(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "governed.db")
	ref := contract.ResourceRef{Kind: "memory", ID: "project"}
	binding := access.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	binding.AllowedObservedTypes = []string{policy.MemoryWriteCandidateObserved}
	rt, err := OpenRuntime(storePath, localRuntimeConfigT([]access.ChannelBinding{binding}))
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()

	if _, _, err := rt.API().Ingest("codex@project", contract.ObservationEnvelope{
		ExternalID: "led1",
		Event: contract.Event{Type: policy.MemoryWriteCandidateObserved, Payload: map[string]any{
			"content": "a governed ledger entry", "source": "user", "confidence": "high"}},
	}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}

	ledger, err := rt.DecisionLedger()
	if err != nil {
		t.Fatalf("decision ledger: %v", err)
	}
	var accepted *contract.Decision
	for i := range ledger {
		if ledger[i].Status == contract.Accepted {
			accepted = &ledger[i]
		}
	}
	if accepted == nil {
		t.Fatalf("ledger must surface the accepted decision (LEDGER source), got %d decisions, none accepted", len(ledger))
	}
	if accepted.Actor != "codex@project" {
		t.Fatalf("accepted decision attribution wrong: got %q, want codex@project", accepted.Actor)
	}
	if len(accepted.NewVersions) == 0 || accepted.NewVersions[0].Ref.Kind != "memory" {
		t.Fatalf("accepted decision must record the memory write it applied: %+v", accepted.NewVersions)
	}

	// READ-ONLY: a second read returns the same log (no write path, no mutation).
	again, err := rt.DecisionLedger()
	if err != nil || len(again) != len(ledger) {
		t.Fatalf("DecisionLedger must be a pure read: first=%d second=%d err=%v", len(ledger), len(again), err)
	}
}
