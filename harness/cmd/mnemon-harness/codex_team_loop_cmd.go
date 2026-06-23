package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"sort"
	"strings"
	"text/template"
	"time"

	"github.com/spf13/cobra"

	"github.com/mnemon-dev/mnemon/harness/internal/autopilot"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// ============================================================================
// `codex-team-loop`: a runnable demonstration of governed self-continuation.
//
// This command hands the cluster ONE intent and then steps back. The cluster drives ITSELF
// through governed events: workers report, POC agents route via governed `assignment` writes,
// and the optional autopilot (internal/autopilot) wakes whichever agent's scope changed. The
// "who acts next" decision is never in Go — it is a POC's governed assignment, replayable from
// the ledger. The Web UI shows the chain growing live.
//
// Roles not in --real-roles use deterministic scripted Agents (autopilot.Scripted): this proves
// the PLUMBING without a real Codex turn. A real-Codex Agent (realCodexBrain, driving a Codex
// turn via internal/codexapp) is a drop-in with the same autopilot.Agent interface — swapping
// one for the other is an Agent change, never an autopilot change.
// ============================================================================

var (
	codexLoopAddr        string
	codexLoopStorePath   string
	codexLoopIntent      string
	codexLoopMaxSteps    int
	codexLoopStepDelay   time.Duration
	codexLoopSimulate    bool
	codexLoopRealRoles   string
	codexLoopTurnTimeout time.Duration
	codexLoopCodexCmd    string
	codexLoopSandbox     string
	codexLoopOnce        bool
)

var codexTeamLoopCmd = &cobra.Command{
	Use:   "codex-team-loop",
	Short: "Demonstrate governed self-continuation: one intent, a self-driving agent cluster, live UI",
	Long: "Hand a local agent cluster ONE intent and watch it self-continue through governed events. " +
		"Workers report; two POC agents route via governed assignments; a content-blind nudge engine " +
		"wakes whichever agent's scope changed. The routing decision is never in code — it is a POC's " +
		"governed assignment, replayable from the decision ledger. The Web UI renders the chain live.",
	RunE: runCodexTeamLoop,
}

func init() {
	codexTeamLoopCmd.Flags().StringVar(&codexLoopAddr, "addr", "127.0.0.1:8796", "Web UI listen address")
	codexTeamLoopCmd.Flags().StringVar(&codexLoopStorePath, "store", "", "governed.db path (default: temp demo store)")
	codexTeamLoopCmd.Flags().StringVar(&codexLoopIntent, "intent", "ship feature X with a reviewed, governed handoff", "the single intent handed to the cluster")
	codexTeamLoopCmd.Flags().IntVar(&codexLoopMaxSteps, "max-steps", 200, "runaway guard: maximum nudge passes")
	codexTeamLoopCmd.Flags().DurationVar(&codexLoopStepDelay, "step-delay", 700*time.Millisecond, "pacing between nudge passes (so the UI shows it self-continue)")
	codexTeamLoopCmd.Flags().BoolVar(&codexLoopSimulate, "simulate", true, "use deterministic scripted brains (no real Codex turns) for roles not in --real-roles")
	codexTeamLoopCmd.Flags().StringVar(&codexLoopRealRoles, "real-roles", "", "comma-separated roles backed by REAL Codex turns (planner,poc-build,builder,poc-review,reviewer); uses quota")
	codexTeamLoopCmd.Flags().DurationVar(&codexLoopTurnTimeout, "turn-timeout", 4*time.Minute, "timeout for each real Codex turn")
	codexTeamLoopCmd.Flags().StringVar(&codexLoopCodexCmd, "codex-command", "codex", "Codex CLI command used to start real app-servers")
	codexTeamLoopCmd.Flags().StringVar(&codexLoopSandbox, "codex-sandbox", "readOnly", "Codex turn sandbox policy: readOnly, workspaceWrite, or dangerFullAccess")
	codexTeamLoopCmd.Flags().BoolVar(&codexLoopOnce, "once", false, "headless: run the loop to quiescence, print the chain as JSON, and exit (no Web UI)")
	codexTeamLoopCmd.GroupID = groupAdvanced
	rootCmd.AddCommand(codexTeamLoopCmd)
}

// loopDemoConfig names which principal plays which role. POC agents are ordinary host-agents
// with a routing lane — "leader" is a stance, never a privileged kind.
type loopDemoConfig struct {
	Operator  contract.ActorID
	Planner   contract.ActorID // worker
	PocBuild  contract.ActorID // POC: routes plan -> build
	Builder   contract.ActorID // worker
	PocReview contract.ActorID // POC: routes build -> review
	Reviewer  contract.ActorID // worker
}

func defaultLoopDemoConfig() loopDemoConfig {
	return loopDemoConfig{
		Operator:  "human@owner",
		Planner:   "codex-01@appserver",
		PocBuild:  "codex-02@appserver",
		Builder:   "codex-03@appserver",
		PocReview: "codex-04@appserver",
		Reviewer:  "codex-05@appserver",
	}
}

func (c loopDemoConfig) roleOf(actor contract.ActorID) (string, bool) {
	switch actor {
	case c.Operator:
		return "operator", false
	case c.Planner:
		return "planner", false
	case c.PocBuild:
		return "poc-build", true
	case c.Builder:
		return "builder", false
	case c.PocReview:
		return "poc-review", true
	case c.Reviewer:
		return "reviewer", false
	}
	return "agent", false
}

// codexLoopDemoBrains builds the deterministic brains for the demo chain:
//
//	intent -> planner plans -> [poc-build routes] -> builder builds -> [poc-review routes] -> reviewer reviews
//
// Each worker emits idempotently (fixed/derived ExternalIDs) so re-nudges on unrelated scope
// changes re-emit harmlessly and the loop reaches quiescence. Each POC's routing is a GOVERNED
// assignment — the only place a "who acts next" decision is made.
func codexLoopDemoBrains(cfg loopDemoConfig) []autopilot.Agent {
	brains, _ := codexLoopBrains(cfg, nil, "", "", "", 0, nil)
	return brains
}

// loopRoleOrder is the fixed agent order: 3 workers + 2 POCs.
func loopRoleOrder(cfg loopDemoConfig) []struct {
	role      string
	principal contract.ActorID
	poc       bool
	teammates []contract.ActorID
} {
	workers := []contract.ActorID{cfg.Planner, cfg.Builder, cfg.Reviewer}
	return []struct {
		role      string
		principal contract.ActorID
		poc       bool
		teammates []contract.ActorID
	}{
		{"planner", cfg.Planner, false, nil},
		{"poc-build", cfg.PocBuild, true, workers},
		{"builder", cfg.Builder, false, nil},
		{"poc-review", cfg.PocReview, true, workers},
		{"reviewer", cfg.Reviewer, false, nil},
	}
}

// codexLoopBrains assembles the agent brains, substituting a real-Codex brain for any role named
// in realRoles and a deterministic scripted brain otherwise. Returns the brains plus the real
// brains (so the caller can Close their app-servers). With realRoles nil/empty it is all scripted.
func codexLoopBrains(cfg loopDemoConfig, realRoles map[string]bool, workDir, codexCmd, sandbox string, turnTimeout time.Duration, log func(string)) ([]autopilot.Agent, []*realCodexBrain) {
	var brains []autopilot.Agent
	var reals []*realCodexBrain
	for _, o := range loopRoleOrder(cfg) {
		if realRoles[o.role] {
			rb := newRealCodexBrain(o.principal, o.role, o.poc, o.teammates, workDir, codexCmd, sandbox, turnTimeout, log)
			brains = append(brains, rb)
			reals = append(reals, rb)
			continue
		}
		brains = append(brains, scriptedBrainForRole(cfg, o.role))
	}
	return brains, reals
}

// scriptedBrainForRole returns the deterministic brain for a role (the --simulate path).
func scriptedBrainForRole(cfg loopDemoConfig, role string) autopilot.Agent {
	switch role {
	case "planner":
		return autopilot.Scripted(cfg.Planner, func(pkt autopilot.TurnPacket) []contract.ObservationEnvelope {
			if !autopilot.ProjectionHasKind(pkt.Projection, "project_intent") {
				return nil
			}
			return []contract.ObservationEnvelope{autopilot.Observe("progress_digest.write_candidate.observed", "plan",
				map[string]any{"summary": "planner: drafted a plan for the intent", "evidence": "broke the intent into build + review lanes"})}
		})
	case "poc-build":
		return autopilot.Scripted(cfg.PocBuild, func(pkt autopilot.TurnPacket) []contract.ObservationEnvelope {
			return routeProgress(pkt, "planner:", "build: ", cfg.Builder, "route-build-")
		})
	case "builder":
		return autopilot.Scripted(cfg.Builder, func(pkt autopilot.TurnPacket) []contract.ObservationEnvelope {
			return actOnAssignment(pkt, cfg.Builder, "builder: built ", "build-")
		})
	case "poc-review":
		return autopilot.Scripted(cfg.PocReview, func(pkt autopilot.TurnPacket) []contract.ObservationEnvelope {
			return routeProgress(pkt, "builder:", "review: ", cfg.Reviewer, "route-review-")
		})
	case "reviewer":
		return autopilot.Scripted(cfg.Reviewer, func(pkt autopilot.TurnPacket) []contract.ObservationEnvelope {
			return actOnAssignment(pkt, cfg.Reviewer, "reviewer: reviewed ", "review-")
		})
	}
	return autopilot.Scripted("unknown", nil)
}

// routeProgress is the POC routing primitive: for every progress item whose summary begins with
// wantPrefix (agent-side relevance filtering over a wide scope), emit a governed assignment
// addressing assignee. Idempotent via idPrefix+itemID.
func routeProgress(pkt autopilot.TurnPacket, wantPrefix, scopePrefix string, assignee contract.ActorID, idPrefix string) []contract.ObservationEnvelope {
	var out []contract.ObservationEnvelope
	for _, item := range autopilot.ProjectionItems(pkt.Projection, "progress_digest") {
		summary := autopilot.ItemStr(item, "summary")
		if len(summary) < len(wantPrefix) || summary[:len(wantPrefix)] != wantPrefix {
			continue
		}
		id := autopilot.ItemStr(item, "id")
		out = append(out, autopilot.Observe("assignment.write_candidate.observed", idPrefix+id,
			map[string]any{
				"scope":             scopePrefix + summary,
				"ttl":               "30m",
				"assignee":          string(assignee),
				"expected_work":     scopePrefix + summary,
				"expected_feedback": "progress_digest with result or blocker",
				"evidence":          "routed by POC from progress " + id,
			}))
	}
	return out
}

// actOnAssignment is the worker primitive: for every assignment addressed to me, report the work.
// Idempotent via idPrefix+itemID.
func actOnAssignment(pkt autopilot.TurnPacket, me contract.ActorID, summaryPrefix, idPrefix string) []contract.ObservationEnvelope {
	var out []contract.ObservationEnvelope
	for _, item := range autopilot.ProjectionItems(pkt.Projection, "assignment") {
		if autopilot.ItemStr(item, "assignee") != string(me) {
			continue
		}
		id := autopilot.ItemStr(item, "id")
		out = append(out, autopilot.Observe("progress_digest.write_candidate.observed", idPrefix+id,
			map[string]any{"summary": summaryPrefix + autopilot.ItemStr(item, "scope"), "evidence": "acted on assignment " + id}))
	}
	return out
}

// brainKindLabel describes the brain mix for startup/headless output.
func brainKindLabel(realRoles map[string]bool) string {
	if len(realRoles) == 0 {
		return "all scripted (deterministic)"
	}
	return "real Codex turns for: " + codexLoopRealRoles + " (rest scripted)"
}

// parseLoopRealRoles parses the comma-separated --real-roles flag into a validated set.
func parseLoopRealRoles(s string) (map[string]bool, error) {
	valid := map[string]bool{"planner": true, "poc-build": true, "builder": true, "poc-review": true, "reviewer": true}
	out := map[string]bool{}
	for _, raw := range strings.Split(s, ",") {
		role := strings.TrimSpace(raw)
		if role == "" {
			continue
		}
		if !valid[role] {
			return nil, fmt.Errorf("unknown role %q in --real-roles (valid: planner, poc-build, builder, poc-review, reviewer)", role)
		}
		out[role] = true
	}
	return out, nil
}

func runCodexTeamLoop(cmd *cobra.Command, args []string) error {
	if codexLoopMaxSteps < 1 {
		return fmt.Errorf("--max-steps must be at least 1")
	}
	realRoles, err := parseLoopRealRoles(codexLoopRealRoles)
	if err != nil {
		return err
	}
	if len(realRoles) > 0 {
		if _, lerr := exec.LookPath(codexLoopCodexCmd); lerr != nil {
			return fmt.Errorf("--real-roles requested but %q not found on PATH: %w", codexLoopCodexCmd, lerr)
		}
	}

	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
	defer stop()

	storePath := codexLoopStorePath
	if storePath == "" {
		tmp, err := os.MkdirTemp("", "mnemon-codex-loop-*")
		if err != nil {
			return err
		}
		defer os.RemoveAll(tmp)
		storePath = tmp + "/governed.db"
	}
	dynamicRoot, err := os.MkdirTemp("", "mnemon-codex-loop-dynamic-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dynamicRoot)

	cfg := defaultLoopDemoConfig()
	bindings, tokens, err := codexTeamBindings(5, "http://127.0.0.1:0")
	if err != nil {
		return err
	}
	handle, err := newCodexTeamRuntimeHandle(storePath, dynamicRoot, bindings, tokens)
	if err != nil {
		return err
	}
	defer handle.Close()

	workDir, err := os.Getwd()
	if err != nil {
		return err
	}
	brainLog := func(msg string) { fmt.Fprintln(cmd.OutOrStdout(), "  "+msg) }
	brains, realBrains := codexLoopBrains(cfg, realRoles, workDir, codexLoopCodexCmd, codexLoopSandbox, codexLoopTurnTimeout, brainLog)
	defer func() {
		for _, rb := range realBrains {
			rb.Close()
		}
	}()

	loop := autopilot.NewLoop(handle, bindings, brains...)
	loop.Delay = codexLoopStepDelay

	// Kickoff: the human hands the cluster ONE intent. Everything after is self-continuation.
	if _, _, _, err := handle.Submit(cfg.Operator, autopilot.Observe("project_intent.write_candidate.observed", "intent",
		map[string]any{"statement": codexLoopIntent, "evidence": "intent handed to the cluster by the operator"})); err != nil {
		return fmt.Errorf("seed intent: %w", err)
	}

	// Headless one-shot: run the loop to quiescence, print the chain, exit. Best for a real-Codex
	// run you want to verify without a browser — the real turns happen during Run.
	if codexLoopOnce {
		loop.Delay = 0
		accepted, runErr := loop.RunContext(ctx, codexLoopMaxSteps)
		snap, serr := buildLoopSnapshot(handle, loop, cfg, codexLoopIntent)
		if serr != nil {
			return serr
		}
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		fmt.Fprintf(cmd.OutOrStdout(), "intent: %s\nbrains: %s\naccepted decisions: %d\n", codexLoopIntent, brainKindLabel(realRoles), accepted)
		_ = enc.Encode(snap.Chain)
		return runErr
	}

	go func() { _, _ = loop.RunContext(ctx, codexLoopMaxSteps) }()

	uiLn, err := net.Listen("tcp", codexLoopAddr)
	if err != nil {
		return fmt.Errorf("listen Web UI: %w", err)
	}
	uiURL := listenerURL(uiLn)
	srv := &http.Server{Handler: codexLoopMux(handle, loop, cfg, codexLoopIntent)}

	errc := make(chan error, 1)
	go func() {
		if err := srv.Serve(uiLn); err != nil && err != http.ErrServerClosed {
			errc <- err
		}
	}()

	brainKind := brainKindLabel(realRoles)
	fmt.Fprintf(cmd.OutOrStdout(), "Governed self-continuation UI: %s\n", uiURL)
	fmt.Fprintf(cmd.OutOrStdout(), "Intent: %s\n", codexLoopIntent)
	fmt.Fprintf(cmd.OutOrStdout(), "Cluster: 3 workers + 2 POCs; brains: %s; engine makes 0 routing decisions\n", brainKind)
	fmt.Fprintf(cmd.OutOrStdout(), "Store: %s\n", storePath)

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-errc:
	}
	shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
	return runErr
}

// ---- snapshot (the human-facing, ledger-authoritative view) ----

type loopChainStep struct {
	Seq     int64  `json:"seq"`
	Actor   string `json:"actor"`
	Role    string `json:"role"`
	Kind    string `json:"kind"`
	Summary string `json:"summary"`
	Routing bool   `json:"routing"` // true = a POC's governed routing assignment
}

type loopAgentView struct {
	Principal  string `json:"principal"`
	Role       string `json:"role"`
	POC        bool   `json:"poc"`
	Nudges     int    `json:"nudges"`
	LastDigest string `json:"last_digest"`
}

type loopNudgeView struct {
	Step      int    `json:"step"`
	Principal string `json:"principal"`
	Role      string `json:"role"`
	Emitted   int    `json:"emitted"`
	Accepted  int    `json:"accepted"`
}

type loopSnapshot struct {
	Intent    string          `json:"intent"`
	Quiescent bool            `json:"quiescent"`
	Steps     int             `json:"steps"`
	Accepted  int             `json:"accepted"`
	Routes    int             `json:"routes"`
	Chain     []loopChainStep `json:"chain"`
	Agents    []loopAgentView `json:"agents"`
	Nudges    []loopNudgeView `json:"nudges"`
}

func buildLoopSnapshot(handle *codexTeamRuntimeHandle, loop *autopilot.Loop, cfg loopDemoConfig, intent string) (loopSnapshot, error) {
	ledger, err := handle.DecisionLedger()
	if err != nil {
		return loopSnapshot{}, err
	}
	snap := loopSnapshot{Intent: intent, Quiescent: loop.Done()}

	accepted := make([]contract.Decision, 0, len(ledger))
	for _, d := range ledger {
		if d.Status == contract.Accepted {
			accepted = append(accepted, d)
		}
	}
	sort.Slice(accepted, func(i, j int) bool { return accepted[i].IngestSeq < accepted[j].IngestSeq })
	for _, d := range accepted {
		role, _ := cfg.roleOf(d.Actor)
		kind, summary := lastWrite(d)
		step := loopChainStep{Seq: d.IngestSeq, Actor: string(d.Actor), Role: role, Kind: kind, Summary: summary, Routing: kind == "assignment"}
		if step.Routing {
			snap.Routes++
		}
		snap.Chain = append(snap.Chain, step)
	}
	snap.Accepted = len(accepted)

	nudges := loop.Nudges()
	snap.Steps = 0
	last := map[contract.ActorID]string{}
	count := map[contract.ActorID]int{}
	for _, n := range nudges {
		if n.Step > snap.Steps {
			snap.Steps = n.Step
		}
		role, _ := cfg.roleOf(n.Principal)
		snap.Nudges = append(snap.Nudges, loopNudgeView{Step: n.Step, Principal: string(n.Principal), Role: role, Emitted: n.Emitted, Accepted: n.Accepted})
		last[n.Principal] = n.Digest
		count[n.Principal]++
	}

	for _, p := range []contract.ActorID{cfg.Planner, cfg.PocBuild, cfg.Builder, cfg.PocReview, cfg.Reviewer} {
		role, poc := cfg.roleOf(p)
		snap.Agents = append(snap.Agents, loopAgentView{
			Principal: string(p), Role: role, POC: poc, Nudges: count[p], LastDigest: shortDigest(last[p]),
		})
	}
	return snap, nil
}

// lastWrite returns the kind and a short summary for the resource this decision wrote, taken
// from the LAST item it appended (the decision's own contribution). Read from the ledger's
// NewResources — the engine never inspects payloads.
func lastWrite(d contract.Decision) (string, string) {
	for _, rs := range d.NewResources {
		kind := string(rs.Ref.Kind)
		items, _ := rs.Fields["items"].([]any)
		if len(items) == 0 {
			return kind, ""
		}
		last, _ := items[len(items)-1].(map[string]any)
		for _, key := range []string{"summary", "scope", "statement"} {
			if s, ok := last[key].(string); ok && s != "" {
				return kind, s
			}
		}
		return kind, ""
	}
	if len(d.NewVersions) > 0 {
		return string(d.NewVersions[0].Ref.Kind), ""
	}
	return "", ""
}

func shortDigest(d string) string {
	if len(d) > 10 {
		return d[:10]
	}
	return d
}

func codexLoopMux(handle *codexTeamRuntimeHandle, loop *autopilot.Loop, cfg loopDemoConfig, intent string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/snapshot", func(w http.ResponseWriter, r *http.Request) {
		snap, err := buildLoopSnapshot(handle, loop, cfg, intent)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(snap)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = codexLoopHTML.Execute(w, nil)
	})
	return mux
}

var codexLoopHTML = template.Must(template.New("codex-loop").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Mnemon — governed self-continuation</title>
<style>
 :root{color-scheme:light;--bg:#f6f8fb;--ink:#16202b;--muted:#64748b;--line:#dce3ee;--acc:#2563eb;--poc:#7c3aed;--ok:#16a34a}
 *{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.5 -apple-system,Segoe UI,Roboto,sans-serif}
 .wrap{max-width:1060px;margin:0 auto;padding:24px}
 h1{font-size:20px;margin:0 0 2px}.sub{color:var(--muted);margin:0 0 18px}
 .intent{background:#fff;border:1px solid var(--line);border-radius:12px;padding:14px 16px;margin-bottom:16px}
 .intent b{color:var(--acc)}
 .stat{display:inline-block;background:#eef2ff;border-radius:999px;padding:3px 10px;margin-right:8px;font-size:13px}
 .grid{display:grid;grid-template-columns:2fr 1fr;gap:16px}
 .card{background:#fff;border:1px solid var(--line);border-radius:12px;padding:16px}
 .card h2{font-size:14px;text-transform:uppercase;letter-spacing:.04em;color:var(--muted);margin:0 0 12px}
 .step{display:flex;gap:10px;padding:8px 0;border-bottom:1px dashed var(--line)}
 .step:last-child{border-bottom:0}
 .seq{color:var(--muted);font-variant-numeric:tabular-nums;min-width:34px}
 .badge{font-size:11px;font-weight:600;border-radius:6px;padding:1px 7px;white-space:nowrap}
 .b-worker{background:#e0e7ff;color:#3730a3}.b-poc{background:#f3e8ff;color:var(--poc)}.b-op{background:#dcfce7;color:#166534}
 .route{color:var(--poc);font-weight:600}
 .agent{display:flex;justify-content:space-between;padding:7px 0;border-bottom:1px dashed var(--line)}
 .agent:last-child{border-bottom:0}.dim{color:var(--muted);font-size:13px}
 .callout{background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;padding:12px 14px;margin-top:16px;font-size:14px}
 .callout b{color:var(--ok)}
 .nudge{font-size:13px;color:var(--muted);padding:3px 0}
 code{background:#f1f5f9;border-radius:5px;padding:1px 5px;font-size:12px}
</style></head><body><div class="wrap">
 <h1>Mnemon · governed self-continuation</h1>
 <p class="sub">One intent in. The cluster drives itself through governed events. The engine makes <b>zero</b> routing decisions.</p>
 <div class="intent">Intent: <b id="intent">—</b> &nbsp; <span id="stats"></span></div>
 <div class="grid">
  <div class="card"><h2>Self-continuation chain (replayable from the ledger)</h2><div id="chain"></div>
   <div class="callout">Every <span class="route">routing assignment</span> above is authored by a <b>POC agent</b> as a governed event — not by the engine. Remove the POC brain and the chain breaks. That is the line between a governed cluster and an orchestrator.</div>
  </div>
  <div><div class="card"><h2>Agents</h2><div id="agents"></div></div>
   <div class="card" style="margin-top:16px"><h2>Nudge timeline</h2><div id="nudges"></div></div></div>
 </div>
</div>
<script>
 function badge(role,poc){var c=poc?'b-poc':(role==='operator'?'b-op':'b-worker');return '<span class="badge '+c+'">'+role+'</span>'}
 async function tick(){
  try{
   const s=await (await fetch('/api/snapshot',{cache:'no-store'})).json();
   document.getElementById('intent').textContent=s.intent;
   document.getElementById('stats').innerHTML='<span class="stat">'+s.accepted+' governed decisions</span><span class="stat">'+s.routes+' POC routes</span><span class="stat">'+s.steps+' nudge passes</span><span class="stat">'+(s.quiescent?'quiescent ✓':'running…')+'</span>';
   document.getElementById('chain').innerHTML=(s.chain||[]).map(function(c){
     return '<div class="step"><span class="seq">#'+c.seq+'</span>'+badge(c.role,c.role.indexOf('poc')===0)+'<span>'+(c.routing?'<span class="route">routes → </span>':'')+(c.summary||c.kind)+'</span></div>'}).join('')||'<div class="dim">waiting for the intent to land…</div>';
   document.getElementById('agents').innerHTML=(s.agents||[]).map(function(a){
     return '<div class="agent"><span>'+badge(a.role,a.poc)+' <span class="dim">'+a.principal+'</span></span><span class="dim">'+a.nudges+' nudges · <code>'+(a.last_digest||'—')+'</code></span></div>'}).join('');
   document.getElementById('nudges').innerHTML=(s.nudges||[]).slice(-14).reverse().map(function(n){
     return '<div class="nudge">pass '+n.step+' · '+n.role+' · emitted '+n.emitted+' → +'+n.accepted+' governed</div>'}).join('')||'<div class="dim">—</div>';
  }catch(e){}
 }
 tick();setInterval(tick,1000);
</script>
</body></html>`))
