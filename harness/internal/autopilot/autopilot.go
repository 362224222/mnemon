// Package autopilot is the OPTIONAL auto-drive layer over the governed collaboration channel.
//
// Base mnemon-harness integrates the channel into host agents and the human drives each agent
// by hand (prompting it). Engage the autopilot and that manual pacing is automated: it watches
// each participant's governed projection scope and, when a participant's scope changes, NUDGES
// it to take a turn — looping until the cluster is quiescent. Disengage and you are back to
// manual. Base never depends on this package; delete it and the channel still runs.
//
// Like an aircraft autopilot, it flies the plane but does NOT navigate: the flight plan —
// who acts next / what to do — is decided elsewhere (a POC's governed assignment events,
// surfaced by the Control Tower). The autopilot is deliberately CONTENT-BLIND: it cannot tell
// a worker report from a routing assignment from a review; it only sees "this participant's
// scope changed, nudge it". That is the line that keeps this a governed cluster, not an
// orchestrator. Routing lives in the Agents, never here.
package autopilot

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/channel"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/eventview"
)

// Runtime is the autopilot's only seam to the governed channel: pull a participant's scoped
// projection, Submit (ingest + tick) the observations it emits, and read the decision ledger.
// The in-process runtime handle satisfies it; autopilot never imports the runtime package, and
// the channel core never imports autopilot — so the autopilot stays a deletable optional ring.
type Runtime interface {
	PullEventView(principal contract.ActorID, sub contract.Subscription) (eventview.EventView, error)
	Submit(principal contract.ActorID, env contract.ObservationEnvelope) (seq int64, dup bool, decisions []contract.Decision, err error)
	DecisionLedger() ([]contract.Decision, error)
}

// TurnPacket is what a nudged participant receives: its scoped projection and why it was woken.
// Reason is always "scope-changed" — the only nudge cause (content-blind).
type TurnPacket struct {
	Principal contract.ActorID
	Reason    string
	EventView eventview.EventView
}

// Agent is a participant the autopilot drives. When nudged it returns the observations it
// chooses to emit; it owns ALL understanding and routing, the autopilot owns none. A scripted
// agent (deterministic) and a real-LLM agent are both just Agents — swapping one for the other
// is an Agent change, never an autopilot change. Emissions MUST be idempotent via ExternalID so
// re-nudges on an unrelated scope change re-emit harmlessly and the loop reaches quiescence.
type Agent interface {
	Principal() contract.ActorID
	Act(pkt TurnPacket) []contract.ObservationEnvelope
}

// Scripted wraps a closure as an Agent: deterministic understanding/routing instead of an LLM.
// It proves the plumbing of governed self-continuation without spending a real turn.
func Scripted(principal contract.ActorID, act func(pkt TurnPacket) []contract.ObservationEnvelope) Agent {
	return scriptedAgent{principal: principal, act: act}
}

type scriptedAgent struct {
	principal contract.ActorID
	act       func(pkt TurnPacket) []contract.ObservationEnvelope
}

func (a scriptedAgent) Principal() contract.ActorID { return a.principal }
func (a scriptedAgent) Act(pkt TurnPacket) []contract.ObservationEnvelope {
	if a.act == nil {
		return nil
	}
	return a.act(pkt)
}

// Nudge records one nudge for the human-facing UI: which participant woke, on what digest, how
// much it emitted, and how many governed decisions that produced — the observability surface
// that makes the self-continuation legible.
type Nudge struct {
	Step      int
	Principal contract.ActorID
	Digest    string
	Emitted   int
	Accepted  int
}

// Loop is the engaged autopilot. It drives the cluster to quiescence by nudging on scope change.
type Loop struct {
	rt     Runtime
	agents []Agent
	subs   map[contract.ActorID]contract.Subscription

	// Delay, when > 0, paces the loop one step at a time so a human can watch the cluster
	// self-continue in the UI. Zero (the test/CI default) runs at full speed.
	Delay time.Duration

	mu     sync.Mutex
	seen   map[contract.ActorID]string
	nudges []Nudge
	done   bool
}

// NewLoop engages the autopilot over rt for the given agents. Each participant's subscription
// scope comes straight from its channel binding (the auditable ceiling); the autopilot never
// widens or narrows it — scope is the communication graph, configured at binding time, not here.
func NewLoop(rt Runtime, bindings []channel.ChannelBinding, agents ...Agent) *Loop {
	subs := make(map[contract.ActorID]contract.Subscription, len(bindings))
	for _, b := range bindings {
		subs[b.Principal] = contract.Subscription{Actor: b.Principal, Refs: b.SubscriptionScope}
	}
	return &Loop{rt: rt, agents: agents, subs: subs, seen: make(map[contract.ActorID]string)}
}

// Run drives passes until quiescence (a full pass that produces no new accepted decision) or
// maxSteps (a runaway guard). It returns the total accepted decisions produced. Quiescence —
// not a fixed round count — is what "the cluster finished" means.
func (l *Loop) Run(maxSteps int) (int, error) {
	return l.RunContext(context.Background(), maxSteps)
}

// RunContext is Run with cancellation and optional per-step pacing (Delay) for the live UI.
func (l *Loop) RunContext(ctx context.Context, maxSteps int) (int, error) {
	defer l.markDone()
	total := 0
	for step := 1; step <= maxSteps; step++ {
		if l.Delay > 0 {
			select {
			case <-ctx.Done():
				return total, ctx.Err()
			case <-time.After(l.Delay):
			}
		} else if ctx.Err() != nil {
			return total, ctx.Err()
		}
		n, err := l.step(step)
		if err != nil {
			return total, err
		}
		total += n
		if n == 0 {
			return total, nil
		}
	}
	return total, nil
}

func (l *Loop) markDone() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.done = true
}

// Done reports whether the autopilot has reached quiescence (or stopped).
func (l *Loop) Done() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.done
}

// step is one nudge pass over the agents: nudge each whose scope changed, ingest+govern its
// output. Returns the number of NEW accepted decisions produced this pass.
func (l *Loop) step(step int) (int, error) {
	accepted := 0
	for _, agent := range l.agents {
		p := agent.Principal()
		proj, err := l.rt.PullEventView(p, l.subs[p])
		if err != nil {
			return accepted, fmt.Errorf("pull projection for %s: %w", p, err)
		}
		if proj.Digest == l.lastDigest(p) {
			continue // scope unchanged for this participant — no nudge (content-blind trigger)
		}
		l.setDigest(p, proj.Digest)

		emitted := agent.Act(TurnPacket{Principal: p, Reason: "scope-changed", EventView: proj})
		nudgeAccepted := 0
		for _, env := range emitted {
			_, dup, decisions, serr := l.rt.Submit(p, env)
			if serr != nil {
				return accepted, fmt.Errorf("submit %s observation for %s: %w", env.Event.Type, p, serr)
			}
			if dup {
				continue
			}
			for _, d := range decisions {
				if d.Status == contract.Accepted {
					nudgeAccepted++
				}
			}
		}
		accepted += nudgeAccepted
		l.recordNudge(Nudge{Step: step, Principal: p, Digest: proj.Digest, Emitted: len(emitted), Accepted: nudgeAccepted})
	}
	return accepted, nil
}

func (l *Loop) lastDigest(p contract.ActorID) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.seen[p]
}

func (l *Loop) setDigest(p contract.ActorID, digest string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seen[p] = digest
}

func (l *Loop) recordNudge(ev Nudge) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.nudges = append(l.nudges, ev)
}

// Nudges returns a copy of the nudge timeline for the UI/observability surface.
func (l *Loop) Nudges() []Nudge {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Nudge(nil), l.nudges...)
}

// ---- observation + projection helpers (shared by Agent implementations) ----

// Observe builds an observation envelope. Source is left empty: the server stamps the
// authenticated principal as Event.Actor on Ingest — a client never names its own identity.
func Observe(eventType, externalID string, payload map[string]any) contract.ObservationEnvelope {
	return contract.ObservationEnvelope{
		ExternalID: externalID,
		Event:      contract.Event{Type: eventType, Payload: payload},
	}
}

// EventViewHasKind reports whether a resource of kind is present (materialized) in the view.
func EventViewHasKind(proj eventview.EventView, kind contract.ResourceKind) bool {
	for _, c := range proj.Content {
		if c.Ref.Kind == kind {
			return true
		}
	}
	return false
}

// EventViewItems returns the item list of the first resource of kind in the view. Coordination
// kinds (assignment, progress_digest, project_intent) carry their records under the "items" field.
func EventViewItems(proj eventview.EventView, kind contract.ResourceKind) []map[string]any {
	for _, c := range proj.Content {
		if c.Ref.Kind != kind {
			continue
		}
		raw, ok := c.Fields["items"].([]any)
		if !ok {
			return nil
		}
		out := make([]map[string]any, 0, len(raw))
		for _, r := range raw {
			if m, ok := r.(map[string]any); ok {
				out = append(out, m)
			}
		}
		return out
	}
	return nil
}

// ItemStr reads a string field from a coordination item.
func ItemStr(item map[string]any, key string) string {
	if s, ok := item[key].(string); ok {
		return s
	}
	return ""
}
