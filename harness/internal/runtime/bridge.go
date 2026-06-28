package runtime

import (
	"encoding/json"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation/view"
)

// ResolvedBinding carries the trusted write identity (Actor) and authorized emit type for a
// proposal: the server builds it at dispatch time from the rule (Actor()/Emits()),
// and Bridge.Stamp reads only these two fields. It is runtime's own stamping DTO — trusted write
// identity at stamp time has nothing to do with file config (where it lived before the assembler
// cutover).
type ResolvedBinding struct {
	Actor contract.ActorID
	Emits string
}

// Bridge is the single chokepoint where a callback's INTENT becomes a TRUSTED *.proposed event. newID
// mints unique event ids; now stamps the (provenance-only) ts. Both are injected for deterministic tests.
type Bridge struct {
	newID func() string
	now   func() string
}

func NewBridge(newID, now func() string) *Bridge { return &Bridge{newID: newID, now: now} }

// Stamp turns intent into a trusted *.proposed event, OR returns an error if any proposed write targets a
// ref outside the actor's DISPATCHED SCOPE (write-scope, R11 — the kernel's authz is actor/kind only).
// Trusted fields come from the binding (write identity), the dispatched presentation view (scope + provenance),
// and the trigger (correlation + lineage) — NEVER from the intent payload, even if a hostile callback stuffs
// "actor"/"based_on" into it (R1/R2). The bridge uses the full dispatched scope for ref-level authorization,
// but stamps the proposal read-set from the written refs only; generic append-item rules read the target
// resource they update, not every resource visible to the actor. Only Payload (the write set) rides through
// proposer-controlled; the kernel validates it. An empty/undecodable write set PASSES the bridge (the kernel
// rejects it as a malformed/empty op, preserving the audit trail); only a DECODED, out-of-scope write is
// blocked here.
func (br *Bridge) Stamp(b ResolvedBinding, dispatchedOn view.View, trigger contract.Event, intent contract.ProposedEvent) (contract.Event, error) {
	scope := make(map[contract.ResourceRef]bool, len(dispatchedOn.Resources))
	versions := make(map[contract.ResourceRef]contract.Version, len(dispatchedOn.Resources))
	refs := make([]contract.ResourceRef, 0, len(dispatchedOn.Resources))
	for _, rv := range dispatchedOn.Resources {
		scope[rv.Ref] = true
		versions[rv.Ref] = rv.Version
		refs = append(refs, rv.Ref)
	}
	writes := decodeWrites(intent.Payload)
	readSet := make([]contract.ResourceVersion, 0, len(writes))
	for _, w := range writes {
		if !scope[w.Ref] {
			return contract.Event{}, fmt.Errorf("proposal writes %s/%s outside actor %q dispatched scope", w.Ref.Kind, w.Ref.ID, b.Actor)
		}
		readSet = append(readSet, contract.ResourceVersion{Ref: w.Ref, Version: versions[w.Ref]})
	}
	corr := trigger.CorrelationID
	if corr == "" {
		corr = br.newID() // escalation requires a non-empty correlation (R3)
	}
	return contract.Event{
		SchemaVersion:       1,
		ID:                  br.newID(),
		TS:                  br.now(),
		Type:                b.Emits, // authorized type from the binding, not the intent's claim
		Actor:               b.Actor, // TRUSTED write identity
		ResourceRefs:        refs,
		BasedOn:             readSet,             // TRUSTED rule read-set, narrowed to written refs
		PresentationViewRef: dispatchedOn.Ref,    // provenance
		ContextDigest:       dispatchedOn.Digest, // provenance
		CorrelationID:       corr,                // TRUSTED: inherited or minted
		CausedBy:            trigger.ID,          // lineage
		Payload:             intent.Payload,      // proposer-controlled write set (kernel-validated)
	}, nil
}

// decodeWrites mirrors admission's robust event-to-kernel decode (round-trip-safe). Undecodable/absent -> nil
// (no scope violation; the kernel rejects the empty/malformed op downstream).
func decodeWrites(payload map[string]any) []contract.ResourceWrite {
	raw, ok := payload["writes"]
	if !ok {
		return nil
	}
	b, _ := json.Marshal(raw)
	var writes []contract.ResourceWrite
	if err := json.Unmarshal(b, &writes); err != nil {
		return nil
	}
	return writes
}
