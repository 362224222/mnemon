package app

import (
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/policy"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/state"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

const approvalHighRiskSpec = `{"schema_version":2,"name":"approval","observed_type":"approval.write_candidate.observed",
"proposed_type":"approval.write.proposed","resource_kind":"approval","items_field":"items",
"fields":[{"section":"narrative","name":"text","validators":[{"id":"required","params":{"missing_style":"empty"}}]}],
"render":{"content":{"member":"bullet-list","params":{"title":"# Approvals","field":"text"}}},
"risk":"high"}`

// A high-risk kind's candidate from an AGENT (host-agent) is DENIED — the operator-only gate
// (the deny outranks the admission propose) — while the same candidate from an OPERATOR
// (control-agent) is ADMITTED. This proves the generic high-risk path with a static policy.
func TestHighRiskOperatorGate(t *testing.T) {
	root := t.TempDir()
	writeExternalGoalPackage(t, root, "approval", approvalHighRiskSpec)
	catalog, err := policy.ResolveRegistry(root, state.DefaultSchemaGuard().Required)
	if err != nil {
		t.Fatalf("resolve catalog: %v", err)
	}
	ref := contract.ResourceRef{Kind: "approval", ID: "project"}
	host := access.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	host.AllowedObservedTypes = []string{"approval.write_candidate.observed"}
	operator := access.ControlAgentBinding("human@owner", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	operator.AllowedObservedTypes = []string{"approval.write_candidate.observed"}

	rc, err := LocalRuntimeConfigFromBindings([]access.ChannelBinding{host, operator}, catalog)
	if err != nil {
		t.Fatalf("boot config: %v", err)
	}
	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "hr.db"), rc)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()

	// agent (host-agent) candidate → denied by the operator gate, never written.
	if _, _, err := rt.API().Ingest("codex@project", contract.ObservationEnvelope{
		ExternalID: "h1",
		Event:      contract.Event{Type: "approval.write_candidate.observed", Payload: r2Text("agent tries a high-risk write")},
	}); err != nil {
		t.Fatalf("ingest as agent: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if v, _, _ := rt.Resource(ref); v != 0 {
		t.Fatalf("a high-risk candidate from a host-agent must be denied, but it admitted (v=%d)", v)
	}

	// operator (control-agent) candidate → admitted (the operator is exempt from the gate).
	if _, _, err := rt.API().Ingest("human@owner", contract.ObservationEnvelope{
		ExternalID: "o1",
		Event:      contract.Event{Type: "approval.write_candidate.observed", Payload: r2Text("operator approves")},
	}); err != nil {
		t.Fatalf("ingest as operator: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if v, _, _ := rt.Resource(ref); v == 0 {
		t.Fatal("a high-risk candidate from a control-agent (operator) must be admitted")
	}
}
