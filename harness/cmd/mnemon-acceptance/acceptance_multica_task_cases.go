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
	multicaAcceptanceTaskCaseR3Surface        = "r3-surface-readiness"
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
	MinActiveAgents      int      `json:"min_active_agents,omitempty"`
	InitialChildSurfaces int      `json:"initial_child_surfaces,omitempty"`
	MinChildSurfaces     int      `json:"min_child_surfaces,omitempty"`
	MinFeedbackComments  int      `json:"min_feedback_comments,omitempty"`
	TeamworkRounds       []string `json:"teamwork_rounds,omitempty"`
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
		id = multicaAcceptanceTaskCaseR3Surface
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
	multicaAcceptanceTaskCaseR3Surface: func(started time.Time) multicaAcceptanceTaskCaseMaterial {
		return multicaAcceptanceTaskCaseMaterial{
			Title: "Mnemon R3 Multica surface readiness " + started.Format("150405"),
			Description: multicasurface.IssueSurfaceDescription(multicasurface.IssueSurfaceDescriptionMaterial{
				Request:  "运行一次小型 Mnemon R3 Multica surface readiness 演练，验证 Multica 作为 OA/runtime 体验层时，provider wrapper、surface ingest、显式 report 写回和 activation carrier 的边界是否清晰。",
				WorkMode: "Mnemon EventEnvelope 和 mnemond accepted state 保持 canonical；Multica 只展示 issue、run、comment、status，并通过明确的 surface-report 或 activation-carrier 命令参与交互。",
				Handoffs: []string{
					"第一轮让两个 teammate 分别检查 provider wrapper run visibility 和 root surface metadata。",
					"在第一轮反馈可见后，由 integrator 创建或要求一个 activation carrier 来验证下一轮调度入口。",
				},
				Validation: []string{
					"Root issue shows Multica run activity and readable R3 surface metadata, without legacy hub/mailbox keys.",
					"Accepted Mnemon events are exposed through display-only report writeback without triggering provider work.",
					"需要执行时使用 activation carrier，并携带 event_ref/resource_ref，而不是靠 display comment 触发。",
					"显式写回的 comment/status 对运营人员可读，machine-only 字段只在 metadata 或 stable marker 中出现。",
					"Final root status reflects the accepted integration decision rather than raw Multica task state.",
				},
				Completion: "当第一轮反馈、activation carrier 证据、最终 surface-report comment 和 root terminal status 都可见时完成。",
			}),
			Expectations: multicaAcceptanceTaskCaseExpectations{
				MinActiveAgents:     3,
				MinChildSurfaces:    2,
				MinFeedbackComments: 2,
				TeamworkRounds: []string{
					"Round 1: root surface intake and first provider-wrapper visibility split",
					"Round 2: activation carrier check for an accepted event",
					"Round 3: integration and display-only surface-report status",
				},
			},
		}
	},
	multicaAcceptanceTaskCaseProtocolReAct: func(started time.Time) multicaAcceptanceTaskCaseMaterial {
		return multicaAcceptanceTaskCaseMaterial{
			Title: "Protocol ReAct collaboration drill " + started.Format("150405"),
			Description: multicasurface.IssueSurfaceDescription(multicasurface.IssueSurfaceDescriptionMaterial{
				Request:  "运行一个复杂的 Mnemon-on-Multica 协作演练，强制经历 observe-act-reflect 循环，覆盖事件接入、provider wrapper 调度、OA 状态回写和最终集成。把它当作面向运营人员的验收调查，而不是简单 happy path。",
				WorkMode: "Multica 作为可见 OA 协作界面；Mnemon EventEnvelope 和 mnemond accepted state 保持 canonical。协作者通过 Mnemon 事件拆分工作，通过显式命令把必要状态写回 Multica，并在第一轮发现风险后创建至少一个 follow-up slice。",
				Handoffs: []string{
					"Round 1 - Observe: 分别让 teammate 检查根 issue 的 surface metadata、provider wrapper run visibility、以及 OA 状态可读性。每个 teammate 必须报告具体 issue ID、agent ID、状态和证据。",
					"Round 2 - Act: the planner/integrator must read first-round feedback, identify the highest-risk gap, and create a follow-up assignment for a different teammate to verify or falsify that gap.",
					"Round 3 - Reflect: after the follow-up result appears, the integrator must reconcile all feedback into a final decision that names residual risks, owner, and next validation step.",
				},
				Validation: []string{
					"至少三个独立工作切片通过 Mnemon assignment event 表达，并在 Multica 中留下可追踪的 surface/评论/状态证据。",
					"At least four distinct Multica agents participate through root or child runs.",
					"Feedback comments include evidence from both the initial observation round and the follow-up action round.",
					"The final root result distinguishes observed facts, actions taken, reflection, and any remaining risk.",
					"Machine-only protocol fields remain in metadata or markers, not in visible child issue text.",
				},
				Completion: "Finish only after the follow-up assignment has feedback and the root issue contains an integrated ReAct-style conclusion: observations, actions, reflection, decision, and next validation step.",
			}),
			Expectations: multicaAcceptanceTaskCaseExpectations{
				MinActiveAgents:     4,
				MinChildSurfaces:    3,
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
			Description: multicasurface.IssueSurfaceDescription(multicasurface.IssueSurfaceDescriptionMaterial{
				Request:  "运行一个接近真实业务的 Mnemon-on-Multica 协作 case：三个有重叠上下文的 PoC 并发推进。目标是验证 Multica 作为 OA/runtime 体验层时，provider wrapper 调度、显式状态回写、以及跨 workstream 上下文复用是否清晰可靠。",
				WorkMode: "Multica 只承担可见 issue、run、comment、status 的 OA 体验；工作拆分必须先进入 Mnemon assignment/progress/integration 事件。不要把 Multica 当 canonical state store，也不要用普通 display issue 替代 activation carrier。planner 需要把 case 拆成三个并行 PoC，要求每个反馈引用 shared context，并在第一轮后创建至少一个 follow-up event。",
				Handoffs: []string{
					"Round 1 - Observe: launch three parallel PoCs for runtime routing, operator runbook readiness, and release risk. Each PoC must name the shared context it consumed and the evidence it produced.",
					"Round 2 - Act: create a follow-up assignment for the highest disagreement or missing evidence across PoCs. The follow-up owner must reuse at least two shared contexts and cite prior child feedback.",
					"Round 3 - Reflect: the integrator reconciles all PoC outputs into one operator-facing decision with residual risks, context reuse evidence, and the next validation signal.",
				},
				Validation: []string{
					"至少三个初始 PoC 切片被表达为 Mnemon assignment event，并在第一轮反馈后追加至少一个 follow-up event。",
					"At least five distinct Multica agents participate through root or child runs when a five-agent registry is available.",
					"Every PoC feedback comment references a shared context and an evidence artifact, not only a local conclusion.",
					"At least one shared context is reused by two or more PoCs, and the final integration comment names that reuse explicitly.",
					"Machine routing, dedupe, session, and assignment fields remain in Multica metadata or stable markers rather than visible issue prose.",
				},
				Completion: "Finish only after the follow-up assignment feedback is visible, all three PoCs have result or blocker comments, and the root issue records an integrated decision that explains context reuse.",
			}),
			Expectations: multicaAcceptanceTaskCaseExpectations{
				MinActiveAgents:      5,
				InitialChildSurfaces: 3,
				MinChildSurfaces:     4,
				MinFeedbackComments:  4,
				TeamworkRounds: []string{
					"Round 1 - Observe: three parallel PoCs split runtime, runbook, and release risk",
					"Round 2 - Act: follow up on disagreement or missing evidence across PoCs",
					"Round 3 - Reflect: integrate reused context, final decision, and residual risks",
				},
			},
			Workstreams: []multicaAcceptanceWorkstream{
				{
					ID:                "poc-runtime-routing",
					Title:             "Provider wrapper routing and surface correlation",
					PrimaryRoles:      []string{"planner@team", "researcher@team", "implementer@team"},
					SharedContextRefs: []string{"surface-map", "provider-contract", "evidence-ledger"},
					ExpectedArtifacts: []string{"routing-evidence.md", "surface-correlation-map.json", "runtime-run-summary.md"},
				},
				{
					ID:                "poc-operator-runbook",
					Title:             "Operator runbook and rollback readiness",
					PrimaryRoles:      []string{"implementer@team", "reviewer@team"},
					SharedContextRefs: []string{"provider-contract", "risk-register", "evidence-ledger"},
					ExpectedArtifacts: []string{"runbook-gap-list.md", "rollback-checklist.md", "operator-risk-notes.md"},
				},
				{
					ID:                "poc-release-risk",
					Title:             "Release decision and product status writeback",
					PrimaryRoles:      []string{"researcher@team", "reviewer@team", "integrator@team"},
					SharedContextRefs: []string{"session-map", "risk-register", "evidence-ledger"},
					ExpectedArtifacts: []string{"release-risk-matrix.md", "status-writeback-evidence.md", "ship-hold-decision.md"},
				},
			},
			Roles: []multicaAcceptanceRolePlan{
				{
					Role:      "planner",
					Principal: "planner@team",
					Primary:   []string{"poc-runtime-routing"},
					Overlaps:  []string{"poc-release-risk"},
					Responsibilities: []string{
						"Seed root teamwork signal，并创建三个第一轮 Mnemon assignment event。",
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
						"Verify provider wrapper contract against the operator runbook and runtime behavior.",
						"Name the smallest code or runbook change that would reduce operator ambiguity.",
					},
				},
				{
					Role:      "reviewer",
					Principal: "reviewer@team",
					Primary:   []string{"poc-release-risk"},
					Overlaps:  []string{"poc-operator-runbook"},
					Responsibilities: []string{
						"Challenge release readiness with rollback and status-writeback evidence.",
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
					Purpose: "Root issue、surface metadata、runtime run 和 agent trace 的对应表，用于关联 Multica 可见产物。",
				},
				{
					ID:      "provider-contract",
					UsedBy:  []string{"poc-runtime-routing", "poc-operator-runbook"},
					Purpose: "Provider wrapper 输入输出、Mnemon event emission channel、以及 Multica surface metadata 的边界约定。",
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
				"implementer@team reuses provider-contract across poc-runtime-routing and poc-operator-runbook.",
				"reviewer@team connects rollback concerns from poc-operator-runbook to poc-release-risk.",
				"integrator@team consumes all PoCs but must wait for follow-up feedback before closing the root issue.",
			},
			ContextReuseChecks: []string{
				"Every first-round feedback comment names at least one shared context and one evidence artifact.",
				"The follow-up assignment cites two prior child comments or artifacts before adding new work.",
				"The final root comment names which shared contexts were reused and where disagreement was resolved.",
				"Ordinary Multica display issues must not trigger execution; execution carriers must originate from accepted Mnemon events and explicit activation-carrier commands.",
				"No visible issue text should expose session ids, assignment ids, assignment fingerprints, or surface owner keys.",
			},
		}
	},
	multicaAcceptanceTaskCaseReleaseReadiness: func(started time.Time) multicaAcceptanceTaskCaseMaterial {
		return multicaAcceptanceTaskCaseMaterial{
			Title: "Release readiness handoff " + started.Format("150405"),
			Description: multicasurface.IssueSurfaceDescription(multicasurface.IssueSurfaceDescriptionMaterial{
				Request:  "Prepare a release readiness decision for a Mnemon Multica runtime update that affects provider wrapper routing, R3 surface metadata, and visible status writeback.",
				WorkMode: "Use Multica as the visible coordination hub while Mnemon keeps the canonical event state.",
				Handoffs: []string{
					"Ask one teammate to verify release risk: registry coverage, runtime activation evidence, and stale-session isolation.",
					"Ask another teammate to verify the operator path: rollback notes, status transitions, and what should be checked before enabling the update.",
					"Have the integrator decide ship, hold, or ship-with-follow-up based on teammate feedback.",
				},
				Validation: []string{
					"Release decision references concrete root and child issue evidence.",
					"Provider wrapper turns route to the intended Multica-visible agents.",
					"Feedback comments distinguish release blockers from ordinary progress.",
					"Final status makes the release decision visible without exposing machine-only protocol fields.",
				},
				Completion: "Finish when the root issue contains an explicit release decision and every work slice has result or blocker feedback recorded through Mnemon events.",
			}),
			Expectations: multicaAcceptanceTaskCaseExpectations{
				MinActiveAgents:     4,
				MinChildSurfaces:    3,
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
			Description: multicasurface.IssueSurfaceDescription(multicasurface.IssueSurfaceDescriptionMaterial{
				Request:  "Triage a production-style regression report where Multica run activity appears, but Mnemon event observation or explicit surface writeback appears late.",
				WorkMode: "Use Mnemon teamwork to split diagnosis, mitigation, and verification while Multica remains the operator-facing hub.",
				Handoffs: []string{
					"Assign one teammate to inspect routing evidence and identify whether surface metadata is complete enough for correlation.",
					"Assign one teammate to inspect display report and status-writeback mapping for delayed or missing updates.",
					"Ask the integrator to propose the smallest mitigation and the follow-up acceptance signal needed before closing the incident.",
				},
				Validation: []string{
					"The triage identifies whether the problem is intake, provider-wrapper routing, activation carrier, display report, status writeback, or Multica run visibility.",
					"Each work slice reports evidence with issue IDs, agent IDs, run IDs, and observed status.",
					"The root issue records a mitigation decision rather than only stating that the flow completed.",
				},
				Completion: "Finish when the root issue has a triage decision, mitigation summary, and clear next verification step.",
			}),
			Expectations: multicaAcceptanceTaskCaseExpectations{
				MinActiveAgents:     4,
				MinChildSurfaces:    3,
				MinFeedbackComments: 3,
				TeamworkRounds: []string{
					"Round 1: diagnose intake, provider routing, and display report writeback",
					"Round 2: act on the leading failure mode with a mitigation check",
					"Round 3: reflect into incident decision and next verification",
				},
			},
		}
	},
	multicaAcceptanceTaskCaseRunbookReview: func(started time.Time) multicaAcceptanceTaskCaseMaterial {
		return multicaAcceptanceTaskCaseMaterial{
			Title: "Operator runbook review " + started.Format("150405"),
			Description: multicasurface.IssueSurfaceDescription(multicasurface.IssueSurfaceDescriptionMaterial{
				Request:  "Review the operator runbook for installing, provisioning, and validating the Mnemon Multica runtime in a workspace with multiple participant agents.",
				WorkMode: "Use Mnemon assignments to split documentation review, command validation, and operator risk notes; use Multica to show issue/run/comment/status evidence.",
				Handoffs: []string{
					"Ask one teammate to verify the install/provisioning commands and identify missing prerequisites.",
					"Ask another teammate to verify the validation section covers R3 surface metadata, provider wrapper routing, feedback comments, and final statuses.",
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
				MinChildSurfaces:    3,
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
