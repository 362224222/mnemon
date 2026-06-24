package policy

import (
	"io/fs"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/rule"
)

func TestR1DeferredCapabilityAssetsRemainDeferred(t *testing.T) {
	entries, err := fs.ReadDir(assets.FS, "capabilities")
	if err != nil {
		t.Fatalf("read embedded capabilities: %v", err)
	}
	present := map[string]bool{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		present[strings.TrimSuffix(entry.Name(), ".json")] = true
	}

	for _, name := range []string{"assignment_status", "assignment_expired", "poc_role", "ic_role"} {
		if present[name] {
			t.Fatalf("%s must remain deferred in R1; model it as a render presentation or later capability, not a built-in asset", name)
		}
	}
}

func TestR1TeamworkCapabilitySchema(t *testing.T) {
	catalog := EmbeddedCatalog()
	cases := []struct {
		name         string
		risk         string
		requiredMiss string
		valid        map[string]any
		invalid      map[string]any
	}{
		{
			name:         "agent_profile",
			risk:         "low",
			requiredMiss: "empty context_advantages",
			valid: map[string]any{
				"actor": "codex@project", "focus": "render presentation implementation",
				"context_advantages": []any{"read r1 docs", "inspected hostagent setup"},
				"availability":       "available", "ttl": "30m", "summary": "Working on R1 render/presentation.",
			},
			invalid: map[string]any{
				"actor": "codex@project", "focus": "render presentation implementation",
				"availability": "available", "ttl": "30m", "summary": "Missing advantages.",
			},
		},
		{
			name:         "teamwork_signal",
			risk:         "mid",
			requiredMiss: "empty why_teamwork",
			valid: map[string]any{
				"scope": "harness/r1", "statement": "Need teammate review",
				"why_teamwork": "another agent has fresher sync context",
				"ttl":          "2h", "evidence": "profile roster says sync context is elsewhere",
			},
			invalid: map[string]any{"scope": "harness/r1", "statement": "Need teammate review", "ttl": "2h", "evidence": "x"},
		},
		{
			name:         "assignment",
			risk:         "mid",
			requiredMiss: "empty expected_feedback",
			valid: map[string]any{
				"assignee": "codex-b@project", "scope": "harness/r1/render",
				"expected_work": "review render audit fields", "expected_feedback": "short blockers list",
				"ttl": "45m", "evidence": "assigned from accepted profile",
			},
			invalid: map[string]any{
				"assignee": "codex-b@project", "scope": "harness/r1/render",
				"expected_work": "review render audit fields", "ttl": "45m", "evidence": "x",
			},
		},
		{
			name:         "progress_digest",
			risk:         "low",
			requiredMiss: "empty summary",
			valid:        map[string]any{"summary": "Rendered work presentation and tests pass.", "assignment_ref": "asg-1"},
			invalid:      map[string]any{"assignment_ref": "asg-1"},
		},
	}

	for _, tc := range cases {
		cap, ok := catalog[tc.name]
		if !ok {
			t.Fatalf("%s must be embedded", tc.name)
		}
		if !cap.DefaultEnabled {
			t.Fatalf("%s must be default-enabled for the standard hook+skill surface", tc.name)
		}
		if !cap.Sync.Importable || cap.Sync.Merge != "item-dedup" {
			t.Fatalf("%s sync = %+v, want importable item-dedup", tc.name, cap.Sync)
		}
		if cap.Risk != tc.risk {
			t.Fatalf("%s risk = %q, want %q", tc.name, cap.Risk, tc.risk)
		}

		if dec := evaluateR1Capability(t, cap, tc.valid); dec.Verdict != contract.VerdictPropose {
			t.Fatalf("%s valid payload verdict = %+v, want propose", tc.name, dec)
		}
		dec := evaluateR1Capability(t, cap, tc.invalid)
		if dec.Verdict != contract.VerdictDeny || len(dec.Reasons) == 0 || !strings.Contains(dec.Reasons[0], tc.requiredMiss) {
			t.Fatalf("%s invalid payload verdict = %+v, want deny containing %q", tc.name, dec, tc.requiredMiss)
		}
	}
}

func evaluateR1Capability(t *testing.T, cap Capability, payload map[string]any) contract.RuleDecision {
	t.Helper()
	ref := contract.ResourceRef{Kind: cap.ResourceKind, ID: "project"}
	dec, err := cap.Rule("codex@project", ref, Limits{}).Evaluate(rule.RuleInput{Event: contract.Event{
		Type: cap.ObservedType, Actor: "codex@project", IngestSeq: 7, Payload: payload,
	}})
	if err != nil {
		t.Fatalf("%s evaluate: %v", cap.Name, err)
	}
	if dec.Verdict == contract.VerdictPropose && (dec.Proposal == nil || dec.Proposal.Type != cap.ProposedType) {
		t.Fatalf("%s proposed bad event: %+v", cap.Name, dec.Proposal)
	}
	return dec
}
