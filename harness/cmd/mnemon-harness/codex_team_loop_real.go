package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/autopilot"
	"github.com/mnemon-dev/mnemon/harness/internal/codexapp"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/projection"
)

// ============================================================================
// realCodexBrain: an autopilot.Agent whose understanding/routing is a REAL Codex turn.
//
// It is a drop-in for autopilot.Scripted — same interface, same engine. When the engine nudges it,
// it first does a CHEAP, Go-level relevance pre-check (is there genuinely new work for me?) so
// it never burns a Codex turn on an unrelated scope change. Only when there is new work does it
// run one real Codex turn, then PARSE the model's output into a governed observation:
//   - a worker emits a progress_digest from its MNEMON_REPORT line;
//   - a POC emits a governed assignment from its MNEMON_ASSIGN / MNEMON_SCOPE lines — the LLM,
//     not the Go, decides who acts next. The Go only translates the model's words into an
//     envelope. The "who acts next" decision still lives in the (now LLM-backed) brain.
// ============================================================================

type realCodexBrain struct {
	principal   contract.ActorID
	role        string
	poc         bool
	teammates   []contract.ActorID // routing choices offered to a POC
	workDir     string
	codexCmd    string
	sandbox     string
	turnTimeout time.Duration
	log         func(string)

	server   *codexapp.AppServer
	threadID string
	handled  map[string]bool // work-item ids already acted on (idempotency + turn-frugality)
}

func newRealCodexBrain(principal contract.ActorID, role string, poc bool, teammates []contract.ActorID, workDir, codexCmd, sandbox string, turnTimeout time.Duration, log func(string)) *realCodexBrain {
	if log == nil {
		log = func(string) {}
	}
	return &realCodexBrain{
		principal: principal, role: role, poc: poc, teammates: teammates,
		workDir: workDir, codexCmd: codexCmd, sandbox: sandbox, turnTimeout: turnTimeout,
		log: log, handled: map[string]bool{},
	}
}

func (b *realCodexBrain) Principal() contract.ActorID { return b.principal }

// realWorkItem is one unit of pending work surfaced by the relevance pre-check.
type realWorkItem struct {
	id      string // stable id (for idempotency) — the source item's id, or "plan"
	context string // what to tell the model this turn
}

// Act runs at most one real Codex turn per pending work item, then translates the output.
func (b *realCodexBrain) Act(pkt autopilot.TurnPacket) []contract.ObservationEnvelope {
	work := b.pendingWork(pkt.Projection)
	if len(work) == 0 {
		return nil // nothing new — no turn (content-blind nudge, brain-frugal)
	}
	if err := b.ensureStarted(); err != nil {
		b.log(fmt.Sprintf("[%s] codex app-server start failed: %v", b.principal, err))
		return nil
	}
	field := realFieldRender(pkt.Projection)
	var out []contract.ObservationEnvelope
	for _, w := range work {
		if b.handled[w.id] {
			continue
		}
		b.log(fmt.Sprintf("[%s] running real Codex turn for %q", b.principal, w.id))
		finalText, err := b.runTurn(field, w.context)
		if err != nil {
			b.log(fmt.Sprintf("[%s] turn failed: %v", b.principal, err))
			continue
		}
		b.handled[w.id] = true
		if b.poc {
			assignee, scope, ok := parseRealAssign(finalText)
			if !ok {
				b.log(fmt.Sprintf("[%s] model declined to route %q", b.principal, w.id))
				continue
			}
			out = append(out, autopilot.Observe("assignment.write_candidate.observed", "real-route-"+w.id,
				map[string]any{"scope": scope, "ttl": "30m", "assignee": assignee,
					"expected_work": scope, "expected_feedback": "progress_digest with result or blocker",
					"evidence": "real Codex POC routed from " + w.id}))
		} else {
			summary := parseRealReport(finalText)
			out = append(out, autopilot.Observe("progress_digest.write_candidate.observed", "real-"+b.role+"-"+w.id,
				map[string]any{"summary": b.role + ": " + summary, "evidence": "real Codex turn by " + string(b.principal)}))
		}
	}
	return out
}

// pendingWork is the cheap relevance filter: WHAT, if anything, is newly mine to act on. It never
// makes a routing decision — for a POC it only surfaces unrouted reports; the model decides routing.
func (b *realCodexBrain) pendingWork(pkt projection.Projection) []realWorkItem {
	var work []realWorkItem
	switch {
	case b.poc:
		for _, item := range autopilot.ProjectionItems(pkt, "progress_digest") {
			if autopilot.ItemStr(item, "actor") == string(b.principal) {
				continue // don't route my own reports
			}
			id := autopilot.ItemStr(item, "id")
			if id == "" || b.handled[id] {
				continue
			}
			work = append(work, realWorkItem{id: id, context: "A teammate reported: " + autopilot.ItemStr(item, "summary") + " (progress id " + id + "). Decide who should act on it next, if anyone."})
		}
	case b.role == "planner":
		if autopilot.ProjectionHasKind(pkt, "project_intent") && !b.handled["plan"] {
			work = append(work, realWorkItem{id: "plan", context: "The team has an intent (see the field). Produce a brief plan to achieve it."})
		}
	default: // builder / reviewer: act on assignments addressed to me
		for _, item := range autopilot.ProjectionItems(pkt, "assignment") {
			if autopilot.ItemStr(item, "assignee") != string(b.principal) {
				continue
			}
			id := autopilot.ItemStr(item, "id")
			if id == "" || b.handled[id] {
				continue
			}
			work = append(work, realWorkItem{id: id, context: "You were assigned: " + autopilot.ItemStr(item, "scope") + " (assignment id " + id + "). Do it and report what you accomplished."})
		}
	}
	return work
}

func (b *realCodexBrain) ensureStarted() error {
	if b.server != nil {
		return nil
	}
	server := codexapp.New(b.codexCmd, b.workDir)
	if err := server.Start(); err != nil {
		return err
	}
	if _, err := server.Request("initialize", map[string]any{"clientInfo": map[string]any{"name": "mnemon-codex-team-loop", "version": "0.1.0"}}, 30*time.Second); err != nil {
		server.Close()
		return err
	}
	thread, err := server.Request("thread/start", map[string]any{
		"cwd":                   b.workDir,
		"approvalPolicy":        "never",
		"ephemeral":             true,
		"developerInstructions": b.developerInstructions(),
	}, 30*time.Second)
	if err != nil {
		server.Close()
		return err
	}
	threadID := codexapp.ThreadID(thread)
	if threadID == "" {
		server.Close()
		return fmt.Errorf("thread/start returned no thread id")
	}
	b.server = server
	b.threadID = threadID
	return nil
}

func (b *realCodexBrain) runTurn(field, task string) (string, error) {
	prompt := strings.Join([]string{
		"You are a governed member of a Mnemon agent team. The shared field (governed state) is:",
		field,
		"",
		"Your task this turn: " + task,
		"",
		b.outputContract(),
	}, "\n")
	before := b.server.NotificationCount()
	if _, err := b.server.Request("turn/start", map[string]any{
		"threadId":       b.threadID,
		"input":          []map[string]any{{"type": "text", "text": prompt}},
		"cwd":            b.workDir,
		"approvalPolicy": "never",
		"sandboxPolicy":  map[string]any{"type": b.sandbox},
	}, 30*time.Second); err != nil {
		return "", err
	}
	if _, err := b.server.WaitNotification("turn/completed", b.turnTimeout, before); err != nil {
		return "", err
	}
	notes := b.server.NotificationsSince(before)
	final := codexapp.FinalAnswer(notes)
	if final == "" {
		final = codexTeamTrimOutput(codexapp.CombinedText(notes), 1500)
	}
	return final, nil
}

func (b *realCodexBrain) Close() {
	if b.server != nil {
		b.server.Close()
		b.server = nil
	}
}

func (b *realCodexBrain) developerInstructions() string {
	if b.poc {
		mates := make([]string, 0, len(b.teammates))
		for _, m := range b.teammates {
			mates = append(mates, string(m))
		}
		return strings.Join([]string{
			"You are " + string(b.principal) + ", a POC (point-of-contact / coordinator) in a Mnemon-governed agent team.",
			"You do not do the work yourself. You read the field and decide WHICH teammate should act next.",
			"Your teammates are: " + strings.Join(mates, ", ") + ".",
			"Every decision you make becomes a governed event — keep it crisp and accountable.",
			b.outputContract(),
		}, "\n")
	}
	return strings.Join([]string{
		"You are " + string(b.principal) + ", the " + b.role + " in a Mnemon-governed agent team.",
		"Do the task you are given and report a concise, factual result. " + sandboxGuidance(b.sandbox),
		b.outputContract(),
	}, "\n")
}

// sandboxGuidance states the file-write posture that matches the ACTUAL sandbox policy passed to
// turn/start, so the developer instruction never contradicts the sandbox (a read-only instruction
// under a writable sandbox silently blocks all work).
func sandboxGuidance(sandbox string) string {
	if sandbox == "readOnly" {
		return "Read-only sandbox: do not modify files; inspect and report."
	}
	return "You may create, modify, and run files in the current working directory to complete the task."
}

func (b *realCodexBrain) outputContract() string {
	if b.poc {
		return "OUTPUT CONTRACT: end your reply with exactly two lines:\nMNEMON_ASSIGN: <one teammate principal, or 'none'>\nMNEMON_SCOPE: <one concise sentence of what they should do>"
	}
	return "OUTPUT CONTRACT: end your reply with exactly one line:\nMNEMON_REPORT: <one concise sentence describing what you accomplished>"
}

// ---- output parsing (unit-tested without quota) ----

// parseRealReport extracts a worker's one-line report. Falls back to a trimmed one-liner of the
// whole answer if the model forgot the contract line.
func parseRealReport(finalText string) string {
	if v, ok := lastTaggedLine(finalText, "MNEMON_REPORT:"); ok && v != "" {
		return v
	}
	return codexTeamOneLine(codexTeamTrimOutput(finalText, 400))
}

// parseRealAssign extracts a POC's routing decision. ok=false when the model declined to route.
func parseRealAssign(finalText string) (assignee, scope string, ok bool) {
	a, hasA := lastTaggedLine(finalText, "MNEMON_ASSIGN:")
	if !hasA {
		return "", "", false
	}
	a = strings.TrimSpace(a)
	if a == "" || strings.EqualFold(a, "none") {
		return "", "", false
	}
	s, _ := lastTaggedLine(finalText, "MNEMON_SCOPE:")
	s = strings.TrimSpace(s)
	if s == "" {
		s = "act on the routed work"
	}
	return a, s, true
}

// lastTaggedLine returns the value after the LAST line beginning with tag (case-insensitive).
func lastTaggedLine(text, tag string) (string, bool) {
	var val string
	var found bool
	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if len(trimmed) >= len(tag) && strings.EqualFold(trimmed[:len(tag)], tag) {
			val = strings.TrimSpace(trimmed[len(tag):])
			found = true
		}
	}
	return val, found
}

// realFieldRender renders the projection as a compact, human/LLM-legible field summary.
func realFieldRender(pkt projection.Projection) string {
	var lines []string
	for _, it := range autopilot.ProjectionItems(pkt, "project_intent") {
		if s := autopilot.ItemStr(it, "statement"); s != "" {
			lines = append(lines, "INTENT: "+s)
		}
	}
	for _, it := range autopilot.ProjectionItems(pkt, "assignment") {
		lines = append(lines, fmt.Sprintf("ASSIGNMENT -> %s: %s", autopilot.ItemStr(it, "assignee"), autopilot.ItemStr(it, "scope")))
	}
	for _, it := range autopilot.ProjectionItems(pkt, "progress_digest") {
		lines = append(lines, "PROGRESS: "+autopilot.ItemStr(it, "summary"))
	}
	if len(lines) == 0 {
		return "(the field is empty)"
	}
	return strings.Join(lines, "\n")
}
