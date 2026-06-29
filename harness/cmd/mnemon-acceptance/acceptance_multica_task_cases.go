package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	multicasurface "github.com/mnemon-dev/mnemon/harness/internal/surface/multica"
)

const (
	multicaAcceptanceTaskCaseR2Readiness      = "r2-readiness"
	multicaAcceptanceTaskCaseProtocolReAct    = "protocol-react-drill"
	multicaAcceptanceTaskCaseParallelPoc      = "parallel-poc-overlap"
	multicaAcceptanceTaskCaseReleaseReadiness = "release-readiness"
	multicaAcceptanceTaskCaseIncidentTriage   = "incident-triage"
	multicaAcceptanceTaskCaseRunbookReview    = "runbook-review"
)

type multicaAcceptanceTaskCaseMaterial struct {
	ID                 string
	Title              string
	Description        string
	Expectations       multicaAcceptanceTaskCaseExpectations
	Workstreams        []multicaAcceptanceWorkstream
	Roles              []multicaAcceptanceRolePlan
	SharedContexts     []multicaAcceptanceSharedContext
	RoleOverlaps       []string
	ContextReuseChecks []string
}

type multicaAcceptanceTaskCaseExpectations struct {
	MinActiveAgents     int      `json:"min_active_agents,omitempty"`
	MinChildMailboxes   int      `json:"min_child_mailboxes,omitempty"`
	MinFeedbackComments int      `json:"min_feedback_comments,omitempty"`
	TeamworkRounds      []string `json:"teamwork_rounds,omitempty"`
}

type multicaAcceptanceExecutionPlan struct {
	RunRoot            string                           `json:"run_root,omitempty"`
	CaseRoot           string                           `json:"case_root,omitempty"`
	SharedContextDir   string                           `json:"shared_context_dir,omitempty"`
	EvidenceDir        string                           `json:"evidence_dir,omitempty"`
	Workstreams        []multicaAcceptanceWorkstream    `json:"workstreams,omitempty"`
	Roles              []multicaAcceptanceRolePlan      `json:"roles,omitempty"`
	SharedContexts     []multicaAcceptanceSharedContext `json:"shared_contexts,omitempty"`
	RoleOverlaps       []string                         `json:"role_overlaps,omitempty"`
	ContextReuseChecks []string                         `json:"context_reuse_checks,omitempty"`
}

type multicaAcceptanceWorkstream struct {
	ID                string   `json:"id,omitempty"`
	Title             string   `json:"title,omitempty"`
	Directory         string   `json:"directory,omitempty"`
	PrimaryRoles      []string `json:"primary_roles,omitempty"`
	SharedContextRefs []string `json:"shared_context_refs,omitempty"`
	ExpectedArtifacts []string `json:"expected_artifacts,omitempty"`
}

type multicaAcceptanceRolePlan struct {
	Role             string   `json:"role,omitempty"`
	Principal        string   `json:"principal,omitempty"`
	Directory        string   `json:"directory,omitempty"`
	Primary          []string `json:"primary,omitempty"`
	Overlaps         []string `json:"overlaps,omitempty"`
	Responsibilities []string `json:"responsibilities,omitempty"`
}

type multicaAcceptanceSharedContext struct {
	ID        string   `json:"id,omitempty"`
	Directory string   `json:"directory,omitempty"`
	UsedBy    []string `json:"used_by,omitempty"`
	Purpose   string   `json:"purpose,omitempty"`
}

func multicaAcceptanceTaskCaseNames() []string {
	names := make([]string, 0, len(multicaAcceptanceTaskCases))
	for name := range multicaAcceptanceTaskCases {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func multicaAcceptanceTaskCase(id string, started time.Time) (multicaAcceptanceTaskCaseMaterial, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		id = multicaAcceptanceTaskCaseR2Readiness
	}
	build, ok := multicaAcceptanceTaskCases[id]
	if !ok {
		return multicaAcceptanceTaskCaseMaterial{}, fmt.Errorf("unknown Multica task case %q (available: %s)", id, strings.Join(multicaAcceptanceTaskCaseNames(), ", "))
	}
	material := build(started)
	material.ID = id
	return material, nil
}

func materializeMulticaAcceptanceExecutionPlan(runRoot string, taskCase multicaAcceptanceTaskCaseMaterial) (multicaAcceptanceExecutionPlan, error) {
	runRoot = strings.TrimSpace(runRoot)
	if runRoot == "" {
		return multicaAcceptanceExecutionPlan{}, fmt.Errorf("Multica task case execution plan requires a run root")
	}
	caseID := multicaAcceptancePathSegment(taskCase.ID, "task-case")
	caseRoot := filepath.Join(runRoot, "taskcase", caseID)
	sharedDir := filepath.Join(caseRoot, "shared-context")
	evidenceDir := filepath.Join(caseRoot, "evidence")
	plan := multicaAcceptanceExecutionPlan{
		RunRoot:            runRoot,
		CaseRoot:           caseRoot,
		SharedContextDir:   sharedDir,
		EvidenceDir:        evidenceDir,
		RoleOverlaps:       multicaAcceptanceCleanStrings(taskCase.RoleOverlaps),
		ContextReuseChecks: multicaAcceptanceCleanStrings(taskCase.ContextReuseChecks),
	}
	dirs := []string{caseRoot, sharedDir, evidenceDir}
	for _, stream := range taskCase.Workstreams {
		stream.ID = strings.TrimSpace(stream.ID)
		if stream.ID == "" {
			continue
		}
		stream.Directory = filepath.Join(caseRoot, "workstreams", multicaAcceptancePathSegment(stream.ID, "workstream"))
		stream.PrimaryRoles = multicaAcceptanceCleanStrings(stream.PrimaryRoles)
		stream.SharedContextRefs = multicaAcceptanceCleanStrings(stream.SharedContextRefs)
		stream.ExpectedArtifacts = multicaAcceptanceCleanStrings(stream.ExpectedArtifacts)
		plan.Workstreams = append(plan.Workstreams, stream)
		dirs = append(dirs, stream.Directory)
	}
	for _, role := range taskCase.Roles {
		role.Role = strings.TrimSpace(role.Role)
		role.Principal = strings.TrimSpace(role.Principal)
		if role.Role == "" && role.Principal == "" {
			continue
		}
		role.Directory = filepath.Join(caseRoot, "roles", multicaAcceptancePathSegment(multicaAcceptanceFirstNonEmpty(role.Role, role.Principal), "role"))
		role.Primary = multicaAcceptanceCleanStrings(role.Primary)
		role.Overlaps = multicaAcceptanceCleanStrings(role.Overlaps)
		role.Responsibilities = multicaAcceptanceCleanStrings(role.Responsibilities)
		plan.Roles = append(plan.Roles, role)
		dirs = append(dirs, role.Directory)
	}
	for _, shared := range taskCase.SharedContexts {
		shared.ID = strings.TrimSpace(shared.ID)
		if shared.ID == "" {
			continue
		}
		shared.Directory = filepath.Join(sharedDir, multicaAcceptancePathSegment(shared.ID, "context"))
		shared.UsedBy = multicaAcceptanceCleanStrings(shared.UsedBy)
		shared.Purpose = strings.TrimSpace(shared.Purpose)
		plan.SharedContexts = append(plan.SharedContexts, shared)
		dirs = append(dirs, shared.Directory)
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return plan, err
		}
	}
	return plan, nil
}

func renderMulticaAcceptanceExecutionPlan(plan multicaAcceptanceExecutionPlan) string {
	if strings.TrimSpace(plan.CaseRoot) == "" {
		return ""
	}
	var b strings.Builder
	b.WriteString("## Execution Plan\n\n")
	writeMulticaAcceptancePlanBullet(&b, "Case root", plan.CaseRoot)
	writeMulticaAcceptancePlanBullet(&b, "Shared context", plan.SharedContextDir)
	writeMulticaAcceptancePlanBullet(&b, "Evidence", plan.EvidenceDir)
	if len(plan.Workstreams) > 0 {
		b.WriteString("\n## Parallel PoCs\n\n")
		for _, stream := range plan.Workstreams {
			line := "- " + multicaAcceptanceMarkdownCode(stream.ID)
			if title := strings.TrimSpace(stream.Title); title != "" {
				line += ": " + title
			}
			if len(stream.PrimaryRoles) > 0 {
				line += "; roles " + multicaAcceptanceInlineCodes(stream.PrimaryRoles)
			}
			if len(stream.SharedContextRefs) > 0 {
				line += "; shared context " + multicaAcceptanceInlineCodes(stream.SharedContextRefs)
			}
			if strings.TrimSpace(stream.Directory) != "" {
				line += "; dir " + multicaAcceptanceMarkdownCode(stream.Directory)
			}
			b.WriteString(line)
			b.WriteString("\n")
			if len(stream.ExpectedArtifacts) > 0 {
				b.WriteString("  - expected artifacts: ")
				b.WriteString(strings.Join(stream.ExpectedArtifacts, ", "))
				b.WriteString("\n")
			}
		}
	}
	if len(plan.SharedContexts) > 0 {
		b.WriteString("\n## Shared Contexts\n\n")
		for _, shared := range plan.SharedContexts {
			line := "- " + multicaAcceptanceMarkdownCode(shared.ID)
			if purpose := strings.TrimSpace(shared.Purpose); purpose != "" {
				line += ": " + purpose
			}
			if len(shared.UsedBy) > 0 {
				line += "; used by " + multicaAcceptanceInlineCodes(shared.UsedBy)
			}
			if strings.TrimSpace(shared.Directory) != "" {
				line += "; dir " + multicaAcceptanceMarkdownCode(shared.Directory)
			}
			b.WriteString(line)
			b.WriteString("\n")
		}
	}
	if len(plan.Roles) > 0 {
		b.WriteString("\n## Role Matrix\n\n")
		for _, role := range plan.Roles {
			label := multicaAcceptanceFirstNonEmpty(role.Principal, role.Role)
			line := "- " + multicaAcceptanceMarkdownCode(label)
			if role.Role != "" && role.Principal != "" && role.Role != role.Principal {
				line += " (" + role.Role + ")"
			}
			if len(role.Primary) > 0 {
				line += "; primary " + multicaAcceptanceInlineCodes(role.Primary)
			}
			if len(role.Overlaps) > 0 {
				line += "; overlap " + multicaAcceptanceInlineCodes(role.Overlaps)
			}
			b.WriteString(line)
			b.WriteString("\n")
			for _, responsibility := range role.Responsibilities {
				b.WriteString("  - ")
				b.WriteString(responsibility)
				b.WriteString("\n")
			}
		}
	}
	if len(plan.RoleOverlaps) > 0 {
		b.WriteString("\n## Role Overlap\n\n")
		for _, overlap := range plan.RoleOverlaps {
			b.WriteString("- ")
			b.WriteString(overlap)
			b.WriteString("\n")
		}
	}
	if len(plan.ContextReuseChecks) > 0 {
		b.WriteString("\n## Context Reuse Checks\n\n")
		for _, check := range plan.ContextReuseChecks {
			b.WriteString("- ")
			b.WriteString(check)
			b.WriteString("\n")
		}
	}
	return strings.TrimSpace(b.String())
}

func writeMulticaAcceptancePlanBullet(b *strings.Builder, label, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	b.WriteString("- ")
	b.WriteString(label)
	b.WriteString(": ")
	b.WriteString(multicaAcceptanceMarkdownCode(value))
	b.WriteString("\n")
}

func multicaAcceptanceInlineCodes(values []string) string {
	values = multicaAcceptanceCleanStrings(values)
	if len(values) == 0 {
		return ""
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, multicaAcceptanceMarkdownCode(value))
	}
	return strings.Join(out, ", ")
}

func multicaAcceptanceMarkdownCode(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "``"
	}
	return "`" + strings.ReplaceAll(value, "`", "'") + "`"
}

func multicaAcceptanceCleanStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func multicaAcceptancePathSegment(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		value = fallback
	}
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == '.' {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), ".-")
	if out == "" {
		return fallback
	}
	return out
}

var multicaAcceptanceTaskCases = map[string]func(time.Time) multicaAcceptanceTaskCaseMaterial{
	multicaAcceptanceTaskCaseR2Readiness: func(started time.Time) multicaAcceptanceTaskCaseMaterial {
		return multicaAcceptanceTaskCaseMaterial{
			Title: "Mnemon Multica runtime prod-sim " + started.Format("150405"),
			Description: multicasurface.RootSessionDescription(multicasurface.RootSessionMaterial{
				Request:  "Run a small Mnemon R2 Multica readiness drill.",
				WorkMode: "Use Mnemon teamwork; Multica shows issues, runs, comments, and statuses.",
				Handoffs: []string{
					"Route root session visibility and child issue routing checks to separate teammates.",
					"After teammate feedback is visible, route a final integration check.",
				},
				Validation: []string{
					"Root issue carries session metadata and shows run activity.",
					"Accepted assignments become child issue mailboxes assigned to target agents.",
					"Feedback comments and statuses are projected back to Multica.",
					"Stale or cross-session assignment material is ignored.",
					"Final root status reflects completion.",
				},
				Completion: "Finish when child feedback comments are visible and the root issue reaches a terminal status.",
			}),
			Expectations: multicaAcceptanceTaskCaseExpectations{
				MinActiveAgents:     3,
				MinChildMailboxes:   2,
				MinFeedbackComments: 2,
				TeamworkRounds: []string{
					"Round 1: root issue intake and first assignment split",
					"Round 2: child mailbox feedback",
					"Round 3: integration and final status projection",
				},
			},
		}
	},
	multicaAcceptanceTaskCaseProtocolReAct: func(started time.Time) multicaAcceptanceTaskCaseMaterial {
		return multicaAcceptanceTaskCaseMaterial{
			Title: "Protocol ReAct collaboration drill " + started.Format("150405"),
			Description: multicasurface.RootSessionDescription(multicasurface.RootSessionMaterial{
				Request:  "Run a complex Mnemon-on-Multica collaboration drill that forces observe-act-reflect cycles across assignment routing, mailbox correlation, feedback projection, and integration. Treat the task as an operator-facing acceptance investigation, not a simple happy-path flow check.",
				WorkMode: "Use Multica as the visible teamwork hub. Mnemon should create child assignment mailboxes, collect teammate feedback, integrate results, and create at least one follow-up slice when the first round leaves an unresolved risk.",
				Handoffs: []string{
					"Round 1 - Observe: assign separate teammates to inspect root session metadata, child mailbox routing, and runtime run visibility. Each teammate must report concrete issue IDs, agent IDs, statuses, and evidence.",
					"Round 2 - Act: the planner/integrator must read first-round feedback, identify the highest-risk gap, and create a follow-up assignment for a different teammate to verify or falsify that gap.",
					"Round 3 - Reflect: after the follow-up result appears, the integrator must reconcile all feedback into a final decision that names residual risks, owner, and next validation step.",
				},
				Validation: []string{
					"At least three child assignment mailboxes are created across the initial and follow-up rounds.",
					"At least four distinct Multica agents participate through root or child runs.",
					"Feedback comments include evidence from both the initial observation round and the follow-up action round.",
					"The final root result distinguishes observed facts, actions taken, reflection, and any remaining risk.",
					"Machine-only protocol fields remain in metadata or markers, not in visible child issue text.",
				},
				Completion: "Finish only after the follow-up assignment has feedback and the root issue contains an integrated ReAct-style conclusion: observations, actions, reflection, decision, and next validation step.",
			}),
			Expectations: multicaAcceptanceTaskCaseExpectations{
				MinActiveAgents:     4,
				MinChildMailboxes:   3,
				MinFeedbackComments: 3,
				TeamworkRounds: []string{
					"Round 1 - Observe: split root metadata, routing, and run-visibility checks",
					"Round 2 - Act: create a follow-up assignment for the highest-risk gap",
					"Round 3 - Reflect: integrate first-round and follow-up feedback into a decision",
				},
			},
		}
	},
	multicaAcceptanceTaskCaseParallelPoc: func(started time.Time) multicaAcceptanceTaskCaseMaterial {
		return multicaAcceptanceTaskCaseMaterial{
			Title: "Parallel PoC overlap drill " + started.Format("150405"),
			Description: multicasurface.RootSessionDescription(multicasurface.RootSessionMaterial{
				Request:  "Run a production-like Mnemon-on-Multica collaboration case with three overlapping PoCs running in parallel. The goal is to validate Multica as the primary hub backend for assignment activation, feedback projection, and context reuse across related workstreams.",
				WorkMode: "Use Multica root and child issues as the visible teamwork hub. The planner should split the case into parallel child assignment mailboxes, require shared context references in every feedback item, and create at least one follow-up assignment after reading first-round results.",
				Handoffs: []string{
					"Round 1 - Observe: launch three parallel PoCs for runtime routing, operator runbook readiness, and release risk. Each PoC must name the shared context it consumed and the evidence it produced.",
					"Round 2 - Act: create a follow-up assignment for the highest disagreement or missing evidence across PoCs. The follow-up owner must reuse at least two shared contexts and cite prior child feedback.",
					"Round 3 - Reflect: the integrator reconciles all PoC outputs into one operator-facing decision with residual risks, context reuse evidence, and the next validation signal.",
				},
				Validation: []string{
					"At least three initial child assignment mailboxes are created, followed by at least one follow-up mailbox after first-round feedback is visible.",
					"At least five distinct Multica agents participate through root or child runs when a five-agent registry is available.",
					"Every PoC feedback comment references a shared context and an evidence artifact, not only a local conclusion.",
					"At least one shared context is reused by two or more PoCs, and the final integration comment names that reuse explicitly.",
					"Machine routing, dedupe, session, and assignment fields remain in Multica metadata or stable markers rather than visible issue prose.",
				},
				Completion: "Finish only after the follow-up assignment feedback is visible, all three PoCs have result or blocker comments, and the root issue records an integrated decision that explains context reuse.",
			}),
			Expectations: multicaAcceptanceTaskCaseExpectations{
				MinActiveAgents:     5,
				MinChildMailboxes:   4,
				MinFeedbackComments: 4,
				TeamworkRounds: []string{
					"Round 1 - Observe: three parallel PoCs split runtime, runbook, and release risk",
					"Round 2 - Act: follow up on disagreement or missing evidence across PoCs",
					"Round 3 - Reflect: integrate reused context, final decision, and residual risks",
				},
			},
			Workstreams: []multicaAcceptanceWorkstream{
				{
					ID:                "poc-runtime-routing",
					Title:             "Runtime routing and assignment mailbox correlation",
					PrimaryRoles:      []string{"planner@team", "researcher@team", "implementer@team"},
					SharedContextRefs: []string{"session-map", "mailbox-contract", "evidence-ledger"},
					ExpectedArtifacts: []string{"routing-evidence.md", "assignment-mailbox-map.json", "runtime-run-summary.md"},
				},
				{
					ID:                "poc-operator-runbook",
					Title:             "Operator runbook and rollback readiness",
					PrimaryRoles:      []string{"implementer@team", "reviewer@team"},
					SharedContextRefs: []string{"mailbox-contract", "risk-register", "evidence-ledger"},
					ExpectedArtifacts: []string{"runbook-gap-list.md", "rollback-checklist.md", "operator-risk-notes.md"},
				},
				{
					ID:                "poc-release-risk",
					Title:             "Release decision and product status projection",
					PrimaryRoles:      []string{"researcher@team", "reviewer@team", "integrator@team"},
					SharedContextRefs: []string{"session-map", "risk-register", "evidence-ledger"},
					ExpectedArtifacts: []string{"release-risk-matrix.md", "status-projection-evidence.md", "ship-hold-decision.md"},
				},
			},
			Roles: []multicaAcceptanceRolePlan{
				{
					Role:      "planner",
					Principal: "planner@team",
					Primary:   []string{"poc-runtime-routing"},
					Overlaps:  []string{"poc-release-risk"},
					Responsibilities: []string{
						"Seed the root teamwork signal and create the three first-round assignment mailboxes.",
						"Read first-round feedback before creating the follow-up assignment.",
					},
				},
				{
					Role:      "researcher",
					Principal: "researcher@team",
					Primary:   []string{"poc-runtime-routing"},
					Overlaps:  []string{"poc-release-risk"},
					Responsibilities: []string{
						"Trace runtime routing evidence and reuse the session map when judging release risk.",
						"Report concrete issue IDs, agent IDs, run status, and evidence refs.",
					},
				},
				{
					Role:      "implementer",
					Principal: "implementer@team",
					Primary:   []string{"poc-operator-runbook"},
					Overlaps:  []string{"poc-runtime-routing"},
					Responsibilities: []string{
						"Verify the mailbox contract against the operator runbook and runtime behavior.",
						"Name the smallest code or runbook change that would reduce operator ambiguity.",
					},
				},
				{
					Role:      "reviewer",
					Principal: "reviewer@team",
					Primary:   []string{"poc-release-risk"},
					Overlaps:  []string{"poc-operator-runbook"},
					Responsibilities: []string{
						"Challenge release readiness with rollback and status-projection evidence.",
						"Identify disagreement between PoC outputs before final integration.",
					},
				},
				{
					Role:      "integrator",
					Principal: "integrator@team",
					Primary:   []string{"poc-release-risk"},
					Overlaps:  []string{"poc-runtime-routing", "poc-operator-runbook"},
					Responsibilities: []string{
						"Consume all PoC outputs and the follow-up result.",
						"Write the final root decision with context reuse evidence and residual risk.",
					},
				},
			},
			SharedContexts: []multicaAcceptanceSharedContext{
				{
					ID:      "session-map",
					UsedBy:  []string{"poc-runtime-routing", "poc-release-risk"},
					Purpose: "Root issue, session mailbox, child issue, and agent run map used to correlate visible Multica artifacts.",
				},
				{
					ID:      "mailbox-contract",
					UsedBy:  []string{"poc-runtime-routing", "poc-operator-runbook"},
					Purpose: "Human-readable assignment mailbox contract plus hidden metadata boundary for routing, dedupe, and correlation.",
				},
				{
					ID:      "risk-register",
					UsedBy:  []string{"poc-operator-runbook", "poc-release-risk"},
					Purpose: "Shared release and operator risks with owner, severity, mitigation, and validation signal.",
				},
				{
					ID:      "evidence-ledger",
					UsedBy:  []string{"poc-runtime-routing", "poc-operator-runbook", "poc-release-risk"},
					Purpose: "Cross-PoC evidence index for issue IDs, run IDs, comments, statuses, and artifacts.",
				},
			},
			RoleOverlaps: []string{
				"researcher@team carries runtime routing evidence into poc-release-risk through session-map.",
				"implementer@team reuses mailbox-contract across poc-runtime-routing and poc-operator-runbook.",
				"reviewer@team connects rollback concerns from poc-operator-runbook to poc-release-risk.",
				"integrator@team consumes all PoCs but must wait for follow-up feedback before closing the root issue.",
			},
			ContextReuseChecks: []string{
				"Every first-round feedback comment names at least one shared context and one evidence artifact.",
				"The follow-up assignment cites two prior child comments or artifacts before adding new work.",
				"The final root comment names which shared contexts were reused and where disagreement was resolved.",
				"No visible issue text should expose session ids, assignment ids, assignment fingerprints, or projection-owner keys.",
			},
		}
	},
	multicaAcceptanceTaskCaseReleaseReadiness: func(started time.Time) multicaAcceptanceTaskCaseMaterial {
		return multicaAcceptanceTaskCaseMaterial{
			Title: "Release readiness handoff " + started.Format("150405"),
			Description: multicasurface.RootSessionDescription(multicasurface.RootSessionMaterial{
				Request:  "Prepare a release readiness decision for a Mnemon Multica runtime update that affects assignment routing, mailbox metadata, and visible status projection.",
				WorkMode: "Use Multica as the visible coordination hub while Mnemon keeps the canonical event state.",
				Handoffs: []string{
					"Ask one teammate to verify release risk: registry coverage, runtime activation evidence, and stale-session isolation.",
					"Ask another teammate to verify the operator path: rollback notes, status transitions, and what should be checked before enabling the update.",
					"Have the integrator decide ship, hold, or ship-with-follow-up based on teammate feedback.",
				},
				Validation: []string{
					"Release decision references concrete root and child issue evidence.",
					"Assignment mailboxes route to the intended Multica agents.",
					"Feedback comments distinguish release blockers from ordinary progress.",
					"Final status makes the release decision visible without exposing machine-only protocol fields.",
				},
				Completion: "Finish when the root issue contains an explicit release decision and every child mailbox has result or blocker feedback.",
			}),
			Expectations: multicaAcceptanceTaskCaseExpectations{
				MinActiveAgents:     4,
				MinChildMailboxes:   3,
				MinFeedbackComments: 3,
				TeamworkRounds: []string{
					"Round 1: risk and operator-path review",
					"Round 2: release decision follow-up for any hold risk",
					"Round 3: final ship/hold integration",
				},
			},
		}
	},
	multicaAcceptanceTaskCaseIncidentTriage: func(started time.Time) multicaAcceptanceTaskCaseMaterial {
		return multicaAcceptanceTaskCaseMaterial{
			Title: "Runtime regression triage " + started.Format("150405"),
			Description: multicasurface.RootSessionDescription(multicasurface.RootSessionMaterial{
				Request:  "Triage a production-style regression report where Multica child assignment mailboxes may be created but feedback comments or completion statuses appear late.",
				WorkMode: "Use Mnemon teamwork to split diagnosis, mitigation, and verification while Multica remains the operator-facing hub.",
				Handoffs: []string{
					"Assign one teammate to inspect routing evidence and identify whether assignment metadata is complete enough for correlation.",
					"Assign one teammate to inspect feedback projection and status mapping for delayed or missing updates.",
					"Ask the integrator to propose the smallest mitigation and the follow-up acceptance signal needed before closing the incident.",
				},
				Validation: []string{
					"The triage identifies whether the problem is intake, routing, wake, feedback projection, or Multica run visibility.",
					"Each child mailbox reports evidence with issue IDs, agent IDs, and observed status.",
					"The root issue records a mitigation decision rather than only stating that the flow completed.",
				},
				Completion: "Finish when the root issue has a triage decision, mitigation summary, and clear next verification step.",
			}),
			Expectations: multicaAcceptanceTaskCaseExpectations{
				MinActiveAgents:     4,
				MinChildMailboxes:   3,
				MinFeedbackComments: 3,
				TeamworkRounds: []string{
					"Round 1: diagnose intake, routing, and feedback projection",
					"Round 2: act on the leading failure mode with a mitigation check",
					"Round 3: reflect into incident decision and next verification",
				},
			},
		}
	},
	multicaAcceptanceTaskCaseRunbookReview: func(started time.Time) multicaAcceptanceTaskCaseMaterial {
		return multicaAcceptanceTaskCaseMaterial{
			Title: "Operator runbook review " + started.Format("150405"),
			Description: multicasurface.RootSessionDescription(multicasurface.RootSessionMaterial{
				Request:  "Review the operator runbook for installing, provisioning, and validating the Mnemon Multica runtime in a workspace with multiple participant agents.",
				WorkMode: "Use Multica child issues to split documentation review, command validation, and operator risk notes.",
				Handoffs: []string{
					"Ask one teammate to verify the install/provisioning commands and identify missing prerequisites.",
					"Ask another teammate to verify the validation section covers root metadata, child mailbox routing, feedback comments, and final statuses.",
					"Have the integrator produce a concise runbook change list with priority and owner.",
				},
				Validation: []string{
					"Runbook findings are tied to concrete operator actions, not generic flow checks.",
					"The review covers both successful operation and recovery from missing metadata or delayed run messages.",
					"Final feedback identifies documentation changes that can be applied without changing runtime semantics.",
				},
				Completion: "Finish when each review slice has feedback and the root issue contains a prioritized runbook update list.",
			}),
			Expectations: multicaAcceptanceTaskCaseExpectations{
				MinActiveAgents:     4,
				MinChildMailboxes:   3,
				MinFeedbackComments: 3,
				TeamworkRounds: []string{
					"Round 1: install, validation, and risk review",
					"Round 2: follow-up on the most ambiguous runbook step",
					"Round 3: integrate prioritized documentation changes",
				},
			},
		}
	},
}
