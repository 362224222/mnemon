// Package runtime is the governed control loop: a ControlServer ingests observations exactly-once, runs
// them through the rule pre-gate, bridges proposals into trusted *.proposed events, reconciles them
// through the single-writer kernel, and emits outbox invalidations + durable diagnostics. The Runtime
// owns the store + the single Tick driver; hosts reach it over the access.ServerAPI port (D5). The
// kernel stays minimal; the rich admission semantics live here (D4).
package runtime

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/kernel"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/admission"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation/view"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

const (
	serverDispatchCursor = "server_dispatch"
	decisionSinkCursor   = "decision_sink" // tracks decisions whose S2/S7 side-effects are produced (recoverable)
)

var _ access.ServerAPI = (*ControlServer)(nil)

// ControlServer is the one single-writer governed loop. Tick is its deterministic, restart-safe driver.
type ControlServer struct {
	tickMu     sync.Mutex // serializes Tick: closes the GetCursor->dispatch TOCTOU + the reconciler-cursor race
	store      *store.Store
	kernel     *kernel.Kernel
	reconciler *admission.Reconciler
	bridge     *Bridge
	rules      admission.RuleSet
	subs       map[contract.ActorID]contract.Subscription
	modes      contract.Modes
	newID      func() string
	now        func() string
	// syncableKinds is the produce surface: the resource kinds a host decision becomes a pending synced
	// event for (sync-abi-v2 §4). Descriptor-derived and injected by OpenRuntime from
	// RuntimeConfig.SyncableKinds; nil = produce no synced events.
	syncableKinds map[contract.ResourceKind]bool
}

// kindSet builds the produce-surface lookup from the configured syncable kinds.
func kindSet(kinds []contract.ResourceKind) map[contract.ResourceKind]bool {
	if len(kinds) == 0 {
		return nil
	}
	set := make(map[contract.ResourceKind]bool, len(kinds))
	for _, k := range kinds {
		set[k] = true
	}
	return set
}

func New(s *store.Store, k *kernel.Kernel, rules admission.RuleSet, subs map[contract.ActorID]contract.Subscription, modes contract.Modes, newID, now func() string) *ControlServer {
	return &ControlServer{
		store:      s,
		kernel:     k,
		reconciler: admission.NewReconciler(s, k),
		bridge:     NewBridge(newID, now),
		rules:      rules,
		subs:       subs,
		modes:      modes,
		newID:      newID,
		now:        now,
	}
}

// Ingest records an observation exactly-once (S1). Source and Event.Actor are stamped from the AUTHENTICATED
// principal — the client's payload claim is overwritten, never trusted (D7/S9).
//
// Trust boundary (R11/S9/S10): the wire admits ONLY observations. A *.proposed / *.diagnostic is an INTERNAL,
// trusted event class — a *.proposed is minted EXCLUSIVELY by the bridge after the rule pre-gate + write-scope
// check, a *.diagnostic only by the server. The reconciler trusts every *.proposed in the log, and dispatchOne
// SKIPS reserved types (so they bypass the rule pre-gate, bridge write-scope, and readback). Admitting a
// client-supplied one would let an edge write any resource of an authorized KIND outside its dispatched scope
// (a within-kind cross-resource / cross-principal escalation) and dodge the S10 content digest. Reject it at
// the door, before it can enter the canonical log.
func (cs *ControlServer) Ingest(principal contract.ActorID, env contract.ObservationEnvelope) (int64, bool, error) {
	if t := env.Event.Type; strings.HasSuffix(t, ".proposed") || strings.HasSuffix(t, ".diagnostic") {
		return 0, false, fmt.Errorf("ingest: event type %q is internal-only; the wire admits observations, never proposals/diagnostics (R11/S9)", t)
	}
	if err := cs.normalizeObservedEvent(principal, &env.Event); err != nil {
		return 0, false, err
	}
	env.Source = principal
	return cs.store.IngestObservation(env)
}

func (cs *ControlServer) IngestObservedEnvelope(principal contract.ActorID, env eventmodel.EventEnvelope) (int64, bool, error) {
	obs, err := observationFromObservedEnvelope(env)
	if err != nil {
		return 0, false, err
	}
	return cs.Ingest(principal, obs)
}

func observationFromObservedEnvelope(env eventmodel.EventEnvelope) (contract.ObservationEnvelope, error) {
	if env.Phase != eventmodel.PhaseObserved {
		return contract.ObservationEnvelope{}, fmt.Errorf("ingest envelope: phase must be %q, got %q", eventmodel.PhaseObserved, env.Phase)
	}
	if err := env.Validate(); err != nil {
		return contract.ObservationEnvelope{}, err
	}
	externalID, _ := env.Meta["external_id"].(string)
	externalID = strings.TrimSpace(externalID)
	if externalID == "" {
		return contract.ObservationEnvelope{}, fmt.Errorf("ingest envelope: observed meta external_id is required")
	}
	refs, err := resourceRefsFromEventSubject(env.Event.Subject)
	if err != nil {
		return contract.ObservationEnvelope{}, err
	}
	payload := copyEventPayload(env.Event.Payload)
	if env.Event.TTL != "" {
		if _, ok := payload["ttl"]; !ok {
			payload["ttl"] = env.Event.TTL
		}
	}
	return contract.ObservationEnvelope{
		ExternalID: externalID,
		Event: contract.Event{
			SchemaVersion: env.Event.SchemaVersion,
			ID:            env.Event.ID,
			TS:            env.Event.CreatedAt,
			Type:          env.Event.Type,
			Actor:         contract.ActorID(env.Event.Actor),
			ResourceRefs:  refs,
			CorrelationID: env.Event.CorrelationID,
			CausedBy:      firstCause(env.Event.CausedBy),
			Payload:       payload,
		},
	}, nil
}

func resourceRefsFromEventSubject(subject eventmodel.EventSubject) ([]contract.ResourceRef, error) {
	parts := strings.SplitN(string(subject), "/", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return nil, fmt.Errorf("ingest envelope: event subject %q must be kind/id", subject)
	}
	return []contract.ResourceRef{{Kind: contract.ResourceKind(parts[0]), ID: contract.ResourceID(parts[1])}}, nil
}

func copyEventPayload(payload map[string]any) map[string]any {
	if payload == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(payload))
	for key, value := range payload {
		out[key] = value
	}
	return out
}

func firstCause(causes []string) string {
	if len(causes) == 0 {
		return ""
	}
	return strings.TrimSpace(causes[0])
}

// normalizeObservedEvent turns a client EventDraft into a server-stamped observed Event (the Event
// Intake duty): it STAMPS the server-authoritative fields from the AUTHENTICATED principal (id, ts,
// schema version, actor) and ZEROES the client-forgeable provenance (read-set, event-view ref, ingest
// seq). The payload, resource refs, correlation/lineage, and context digest are preserved — a client
// can never forge identity or a read-set on the wire (D7/S9).
func (cs *ControlServer) normalizeObservedEvent(principal contract.ActorID, ev *contract.Event) error {
	if err := validateObservedType(ev.Type); err != nil {
		return err
	}
	ev.SchemaVersion, ev.ID, ev.TS, ev.Actor = 1, cs.newID(), cs.now(), principal // STAMP
	ev.BasedOn, ev.EventViewRef, ev.IngestSeq = nil, "", 0                        // ZERO forgeable
	return nil
}

// validateObservedType requires a lowercase, dot-segmented observed event type (e.g.
// "memory.write_candidate.observed"; the legacy underscore form is still lowercase). The reserved
// *.proposed / *.diagnostic suffixes are rejected earlier, at the trust boundary.
func validateObservedType(t string) error {
	if t == "" {
		return fmt.Errorf("intake: event type is required")
	}
	if t != strings.ToLower(t) {
		return fmt.Errorf("intake: event type %q must be lowercase", t)
	}
	if !strings.Contains(t, ".") {
		return fmt.Errorf("intake: event type %q must be dot-segmented", t)
	}
	for _, r := range t {
		if !(r >= 'a' && r <= 'z') && !(r >= '0' && r <= '9') && r != '.' && r != '_' {
			return fmt.Errorf("intake: event type %q has an invalid character %q", t, string(r))
		}
	}
	return nil
}

// PullEventView serves an actor's scoped, server-built view. The subscription's actor MUST equal the
// authenticated principal (S9/D7): a client can never name another actor's scope on the wire.
func (cs *ControlServer) PullEventView(principal contract.ActorID, sub contract.Subscription) (view.View, error) {
	if sub.Actor != principal {
		return view.View{}, fmt.Errorf("subscription actor %q does not match authenticated principal %q", sub.Actor, principal)
	}
	// S9: serve ONLY the actor's server-CONFIGURED scope. The client may NARROW (request a subset) but never
	// widen — requested refs are intersected with the configured scope, so a client-named out-of-scope ref is
	// never materialized. An empty request defaults to the whole configured scope.
	configured := cs.subs[principal]
	allowed := make(map[contract.ResourceRef]bool, len(configured.Refs))
	for _, r := range configured.Refs {
		allowed[r] = true
	}
	want := sub.Refs
	if len(want) == 0 {
		want = configured.Refs
	}
	var refs []contract.ResourceRef
	for _, r := range want {
		if allowed[r] {
			refs = append(refs, r)
		}
	}
	return view.ScopedView(cs.store, contract.Subscription{Actor: principal, Refs: refs, PrivacyTier: configured.PrivacyTier}), nil
}

// Tick runs one governed cycle:
//  1. DISPATCH: scan events past the durable dispatch cursor; for each OBSERVED event, build its actor's
//     scoped view, run the rule pre-gate, turn the verdict into trusted events — a propose -> bridged
//     *.proposed event; a deny / rule-error -> a *.diagnostic event (S7, no silent drop). The proposed +
//     diagnostic events AND the cursor advance are ONE atomic DispatchTx (S2).
//  2. RECONCILE: the kernel decides the pending *.proposed events (the kernel is the only writer).
//  3. INVALIDATE: each Accepted decision enqueues an outbox invalidation (downstream projections are stale).
func (cs *ControlServer) Tick() ([]contract.Decision, error) {
	cs.tickMu.Lock() // single-writer: Tick is serialized (the in-memory reconciler cursor is not concurrency-safe)
	defer cs.tickMu.Unlock()
	cur := cs.store.GetCursor(serverDispatchCursor)
	evs, err := cs.store.PendingEvents(cur)
	if err != nil {
		return nil, err // fail-stop on a corrupt log (consistent with RunOnce)
	}
	for _, ev := range evs {
		stamped, derr := cs.dispatchOne(ev)
		if derr != nil {
			return nil, derr
		}
		// S2: this observed event's produced events + the cursor advance are ONE tx.
		if err := cs.store.WithTx(func(tx *store.Tx) error {
			for _, e := range stamped {
				if err := tx.AppendEvent(e); err != nil {
					return err
				}
			}
			return tx.SetCursor(serverDispatchCursor, ev.IngestSeq)
		}); err != nil {
			return nil, err
		}
	}
	// 1) decide the rule proposals.
	decisions := cs.reconciler.RunOnce(cs.modes)
	// 2) produce each decision's side-effects (S2 invalidation / S7 diagnostic) from the durable decision log,
	//    advancing a sink cursor — so a crash between a decision commit and its side-effects is RECOVERABLE on
	//    the next Tick (the reconciler cursor alone would skip past the un-effected decision).
	if err := cs.processDecisionSideEffects(); err != nil {
		return nil, err
	}
	return decisions, nil
}

// dispatchOne runs the rule pre-gate for one event and returns the trusted events to append (proposals +
// diagnostics). Events no rule handles (proposals, diagnostics, other domains) produce nothing — the cursor
// still advances past them, so each event is consumed exactly once.
func (cs *ControlServer) dispatchOne(ev contract.Event) ([]contract.Event, error) {
	// Only OBSERVED events go through the readback check + rule pre-gate. Internal events — a *.proposed event
	// (decided by the reconciler) carries a PROVENANCE digest, a *.diagnostic carries none — must NOT be
	// readback-checked: re-scanning a proposal on a later Tick (its stamped digest now stale vs the current
	// view) would otherwise spuriously trip the readback diagnostic.
	if strings.HasSuffix(ev.Type, ".proposed") || strings.HasSuffix(ev.Type, ".diagnostic") {
		return nil, nil
	}
	view := cs.scopedView(ev.Actor)
	// S10/D8 readback: if the edge echoed the digest it claims to have read, it MUST match the current
	// canonical content digest. A mismatch means the edge acted on tampered/stale content — block the
	// dependent proposal (no write) and surface a stage:readback diagnostic.
	if ev.ContextDigest != "" && ev.ContextDigest != view.Digest {
		return []contract.Event{cs.diagnosticEvent(ev, contract.Diagnostic{
			Stage: "readback", Reason: fmt.Sprintf("echoed digest %q != current %q", ev.ContextDigest, view.Digest), Ref: string(ev.Actor)})}, nil
	}
	dec, diags := cs.rules.Evaluate(admission.RuleInput{Event: ev, View: view})
	var stamped []contract.Event
	for _, dg := range diags { // S7: every rule error is a durable diagnostic.
		stamped = append(stamped, cs.diagnosticEvent(ev, dg))
	}
	switch dec.Verdict {
	case contract.VerdictPropose:
		if dec.Proposal == nil {
			// S7: a propose verdict that carries no proposal is diagnosed, never silently dropped — symmetric
			// with the deny nil-reasons handling.
			stamped = append(stamped, cs.diagnosticEvent(ev, contract.Diagnostic{Stage: "rule", Reason: "verdict propose carried no proposal", Ref: ev.Type}))
			break
		}
		// The bridge write identity comes from the TRUSTED origin the reducer carried (the producing rule's
		// Actor) + the proposal's type (which the reducer enforced == that rule's Emits) — NOT a guess by
		// scanning for any rule with a matching Handles/Emits, which could stamp a different rule's actor.
		if dec.ProposalActor == "" {
			stamped = append(stamped, cs.diagnosticEvent(ev, contract.Diagnostic{Stage: "bridge", Reason: "no rule owns the proposal", Ref: dec.Proposal.Type}))
			break
		}
		b := ResolvedBinding{Actor: dec.ProposalActor, Emits: dec.Proposal.Type}
		e, serr := cs.bridge.Stamp(b, view, ev, *dec.Proposal)
		if serr != nil {
			stamped = append(stamped, cs.diagnosticEvent(ev, contract.Diagnostic{Stage: "bridge", Reason: serr.Error(), Ref: string(b.Actor)}))
			break
		}
		stamped = append(stamped, e)
	case contract.VerdictWarn:
		// S7: a warn surfaces its reasons as a diagnostic — never a silent warn. (The action, if any, still
		// rides whatever verdict won; a standalone warn produces no proposal.)
		stamped = append(stamped, cs.diagnosticEvent(ev, contract.Diagnostic{Stage: "rule", Reason: "warn: " + strings.Join(dec.Reasons, "; "), Ref: ev.Type}))
	case contract.VerdictDeny:
		stamped = append(stamped, cs.diagnosticEvent(ev, contract.Diagnostic{Stage: "rule", Reason: strings.Join(dec.Reasons, "; "), Ref: ev.Type}))
	case contract.VerdictAllow:
		// allow: no proposal, no diagnostic — the event is consumed and the cursor advances.
	default:
		// S7 fail-closed: an UNKNOWN verdict (e.g. a stale rule still emitting a retired verdict string) must
		// never fall through silently — it is diagnosed like any other dropped intent.
		stamped = append(stamped, cs.diagnosticEvent(ev, contract.Diagnostic{Stage: "rule", Reason: fmt.Sprintf("unknown verdict %q dropped", string(dec.Verdict)), Ref: ev.Type}))
	}
	// S7: surface accumulated advisory reasons (e.g. a co-firing warn rule) for the verdicts whose branch does
	// not already emit them (Deny and standalone-Warn do). A warning is never silently dropped, even when a
	// higher-ranked verdict wins.
	if len(dec.Reasons) > 0 && dec.Verdict == contract.VerdictPropose {
		stamped = append(stamped, cs.diagnosticEvent(ev, contract.Diagnostic{Stage: "warn", Reason: "warn: " + strings.Join(dec.Reasons, "; "), Ref: ev.Type}))
	}
	return stamped, nil
}

// scopedView builds the actor's scoped view. (P2 strengthens the scoping + digest behind this seam;
// the call site stays stable.)
func (cs *ControlServer) scopedView(actor contract.ActorID) view.View {
	return view.ScopedView(cs.store, cs.subs[actor])
}

// diagnosticEvent builds a durable "*.diagnostic" event in the trigger's domain (S7). Domain = the prefix of
// the trigger type before the first dot (memory.observed -> memory.diagnostic).
func (cs *ControlServer) diagnosticEvent(trigger contract.Event, dg contract.Diagnostic) contract.Event {
	domain := trigger.Type
	if i := strings.IndexByte(domain, '.'); i >= 0 {
		domain = domain[:i]
	}
	return contract.Event{
		SchemaVersion: 1,
		ID:            cs.newID(),
		TS:            cs.now(),
		Type:          domain + ".diagnostic",
		Actor:         trigger.Actor,
		CorrelationID: trigger.CorrelationID,
		CausedBy:      trigger.ID,
		Payload:       map[string]any{"stage": dg.Stage, "reason": dg.Reason, "ref": dg.Ref},
	}
}

// processDecisionSideEffects produces every not-yet-effected decision's side-effects from the DURABLE decision
// log: an Accepted decision enqueues an outbox invalidation (S2 downstream propagation); a non-Accepted one
// appends a diagnostic naming WHY (S7 — no silent drop). Each decision's side-effect AND the sink-cursor
// advance are ONE tx, so a crash leaves the sink exactly at what committed: on restart the gap decisions are
// re-derived (the invalidation is idempotent via its UNIQUE key; the diagnostic, being atomic with the sink
// advance, is appended exactly once). Direct (non-reconciler) applies carry IngestSeq 0 and produce no
// side-effect — the sink just advances past them.
func (cs *ControlServer) processDecisionSideEffects() error {
	cur := cs.store.GetCursor(decisionSinkCursor)
	decs, err := cs.store.DecisionsAfter(cur)
	if err != nil {
		return err
	}
	for _, dr := range decs {
		d := dr.Decision
		rid := dr.Rowid
		if e := cs.store.WithTx(func(tx *store.Tx) error {
			if d.IngestSeq > 0 {
				if d.Status == contract.Accepted {
					payload, _ := json.Marshal(d.NewVersions)
					key := "inv_" + d.DecisionID
					if err := tx.EnqueueOutbox(store.OutboxRow{ID: key, Kind: "invalidation", EventSeq: d.IngestSeq, Target: "projection", Payload: string(payload), IdempotencyKey: key}); err != nil {
						return err
					}
					// cs.syncableKinds is the produce surface, descriptor-derived from the replica's
					// capability catalog (sync-abi-v2 §4): a host decision on a syncable kind becomes a
					// pending synced event. The hub's accept surface is its per-replica grant scope; the
					// two align by configuration (a mismatch surfaces as a per-event rejection, never a
					// silent drop). The sync-import principal is excluded — imported writes never re-emit.
					if d.Actor != contract.SyncImportActor {
						if err := tx.RecordSyncEventsTx(d, cs.syncableKinds); err != nil {
							return err
						}
					}
				} else if err := tx.AppendEvent(cs.rejectDiagnostic(d)); err != nil {
					return err
				}
			}
			return tx.SetCursor(decisionSinkCursor, rid)
		}); e != nil {
			return e
		}
	}
	return nil
}

// rejectDiagnostic turns a kernel reject/defer into a durable "*.diagnostic" event (S7). A CAS/read-stale
// conflict names the raced ResourceVersion (kind/id@actual); a schema/authz reject carries the kernel's
// reason, which already names actor×kind/field. The domain is the conflict's resource kind when present.
func (cs *ControlServer) rejectDiagnostic(d contract.Decision) contract.Event {
	stage, reason, ref, domain := "kernel", d.Reason, string(d.Actor), "control"
	if len(d.Conflicts) > 0 {
		c := d.Conflicts[0]
		reason = fmt.Sprintf("conflict on %s/%s: expected v%d, actual v%d (%s)", c.Ref.Kind, c.Ref.ID, c.ExpectedVersion, c.ActualVersion, c.Kind)
		ref = fmt.Sprintf("%s/%s@%d", c.Ref.Kind, c.Ref.ID, c.ActualVersion)
		domain = string(c.Ref.Kind)
	}
	return contract.Event{
		SchemaVersion: 1, ID: cs.newID(), TS: cs.now(),
		Type: domain + ".diagnostic", Actor: d.Actor, CorrelationID: d.CorrelationID, CausedBy: d.OpID,
		Payload: map[string]any{"stage": stage, "reason": reason, "ref": ref, "decision": string(d.Status)},
	}
}
