package policy

import (
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/admission"
)

func TestR1DeferredEventPackagesRemainDeferred(t *testing.T) {
	present := StandardRegistry()
	for _, name := range []string{"assignment_status", "assignment_expired", "poc_role", "ic_role"} {
		if _, ok := present[name]; ok {
			t.Fatalf("%s must remain deferred in R1; model it as presentation or a later event package, not a standard package", name)
		}
	}
}

func TestR1TeamworkEventPackageSchema(t *testing.T) {
	catalog := StandardRegistry()
	payload := eventmodel.BuildPayload
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
			valid: payload(map[string]any{"actor": "codex@project", "availability": "available", "ttl": "30m"},
				map[string]any{"focus": "render presentation implementation",
					"context_advantages": []any{"read r1 docs", "inspected hostagent setup"},
					"summary":            "Working on R1 render/presentation."}, nil),
			invalid: payload(map[string]any{"actor": "codex@project", "availability": "available", "ttl": "30m"},
				map[string]any{"focus": "render presentation implementation", "summary": "Missing advantages."}, nil),
		},
		{
			name:         "teamwork_signal",
			risk:         "mid",
			requiredMiss: "empty why_teamwork",
			valid: payload(map[string]any{"scope": "harness/r1", "ttl": "2h"},
				map[string]any{"statement": "Need teammate review",
					"why_teamwork": "another agent has fresher sync context",
				}, map[string]any{"evidence_refs": []any{"profile:sync-context"}}),
			invalid: payload(map[string]any{"scope": "harness/r1", "ttl": "2h"},
				map[string]any{"statement": "Need teammate review"}, map[string]any{"evidence_refs": []any{"x"}}),
		},
		{
			name:         "assignment",
			risk:         "mid",
			requiredMiss: "empty expected_feedback",
			valid: payload(map[string]any{"assignee": "codex-b@project", "scope": "harness/r1/render", "ttl": "45m"},
				map[string]any{"expected_work": "review render audit fields", "expected_feedback": "short blockers list"},
				map[string]any{"evidence_refs": []any{"profile:codex-b"}}),
			invalid: payload(map[string]any{"assignee": "codex-b@project", "scope": "harness/r1/render", "ttl": "45m"},
				map[string]any{"expected_work": "review render audit fields"}, map[string]any{"evidence_refs": []any{"x"}}),
		},
		{
			name:         "progress_digest",
			risk:         "low",
			requiredMiss: "empty summary",
			valid:        payload(map[string]any{"assignment_ref": "asg-1", "feedback_kind": "progress"}, map[string]any{"summary": "Rendered work presentation and tests pass."}, nil),
			invalid:      payload(map[string]any{"assignment_ref": "asg-1", "feedback_kind": "progress"}, nil, nil),
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

		if dec := evaluateR1EventPackage(t, cap, tc.valid); dec.Verdict != contract.VerdictPropose {
			t.Fatalf("%s valid payload verdict = %+v, want propose", tc.name, dec)
		}
		dec := evaluateR1EventPackage(t, cap, tc.invalid)
		if dec.Verdict != contract.VerdictDeny || len(dec.Reasons) == 0 || !strings.Contains(dec.Reasons[0], tc.requiredMiss) {
			t.Fatalf("%s invalid payload verdict = %+v, want deny containing %q", tc.name, dec, tc.requiredMiss)
		}
	}
}

func evaluateR1EventPackage(t *testing.T, cap EventPackage, payload map[string]any) contract.RuleDecision {
	t.Helper()
	ref := contract.ResourceRef{Kind: cap.ResourceKind, ID: "project"}
	dec, err := cap.Rule("codex@project", ref, Limits{}).Evaluate(admission.RuleInput{Event: contract.Event{
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
