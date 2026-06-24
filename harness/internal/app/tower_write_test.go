package app

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/kernel"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/policy"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

// P6b: the Tower's only write action — the operator (a control-agent) resolves an INBOX escalation by
// RE-OBSERVING the underlying candidate, NOT by "approving a proposal" (no such kernel verb). A
// high-risk candidate denied from a host-agent surfaces on INBOX; ReobserveCandidate re-emits it under
// the operator, whom the operator-gate exempts, so it admits — carrying the ORIGINAL candidate content.
// The facade refuses to re-observe anything that is not an observed candidate (no backdoor ingest).
func TestReobserveCandidateAdmitsViaOperator(t *testing.T) {
	root := t.TempDir()
	writeExternalGoalPackage(t, root, "approval", approvalHighRiskSpec)
	catalog, err := policy.ResolveCatalog(root, kernel.DefaultSchemaGuard().Required)
	if err != nil {
		t.Fatalf("resolve catalog: %v", err)
	}
	ref := contract.ResourceRef{Kind: "approval", ID: "project"}
	host := access.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	host.AllowedObservedTypes = []string{"approval.write_candidate.observed"}
	operator := access.ControlAgentBinding("human@owner", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	operator.AllowedObservedTypes = []string{"approval.write_candidate.observed"}
	bindings := []access.ChannelBinding{host, operator}
	rc, err := LocalRuntimeConfigFromBindings(bindings, catalog)
	if err != nil {
		t.Fatalf("boot config: %v", err)
	}
	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "reobs.db"), rc)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()

	// host candidate -> denied by the operator gate -> diagnostic (never written)
	if _, _, err := rt.API().Ingest("codex@project", contract.ObservationEnvelope{
		ExternalID: "h1",
		Event:      contract.Event{Type: "approval.write_candidate.observed", Payload: map[string]any{"text": "needs operator approval"}},
	}); err != nil {
		t.Fatalf("ingest host candidate: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if v, _, _ := rt.Resource(ref); v != 0 {
		t.Fatalf("a high-risk host candidate must be denied first (v=%d)", v)
	}

	// the escalation appears on the Tower INBOX
	view, err := BuildTowerView(rt, bindings)
	if err != nil {
		t.Fatalf("build view: %v", err)
	}
	var esc *InboxRow
	for i := range view.Inbox.Escalations {
		if view.Inbox.Escalations[i].Domain == "approval" {
			esc = &view.Inbox.Escalations[i]
		}
	}
	if esc == nil {
		t.Fatalf("INBOX must carry the approval escalation: %+v", view.Inbox.Escalations)
	}

	// operator re-observes -> admitted, carrying the original candidate content
	if err := ReobserveCandidate(rt, "human@owner", *esc); err != nil {
		t.Fatalf("re-observe: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick after re-observe: %v", err)
	}
	v, fields, _ := rt.Resource(ref)
	if v == 0 {
		t.Fatal("re-observe by the operator must admit the high-risk candidate")
	}
	if content, _ := fields["content"].(string); !strings.Contains(content, "needs operator approval") {
		t.Fatalf("re-observed resource must carry the ORIGINAL candidate content: %q", content)
	}

	// negative: a missing candidate errors (never a silent no-op)
	if err := ReobserveCandidate(rt, "human@owner", InboxRow{Domain: "x", CausedBy: "does-not-exist"}); err == nil {
		t.Fatal("re-observe must error when the candidate event is not in the log")
	}

	// negative (T3 guard): the Tower refuses to re-observe a trusted internal event (a .diagnostic),
	// never a backdoor ingest of a non-candidate.
	evs, _ := rt.PendingEvents(0)
	var diagID string
	for _, e := range evs {
		if strings.HasSuffix(e.Type, ".diagnostic") {
			diagID = e.ID
		}
	}
	if diagID == "" {
		t.Fatal("expected a .diagnostic event in the log to exercise the guard")
	}
	if err := ReobserveCandidate(rt, "human@owner", InboxRow{Domain: "approval", CausedBy: diagID}); err == nil {
		t.Fatal("re-observe must refuse a non-candidate (.diagnostic) event type")
	}
}
