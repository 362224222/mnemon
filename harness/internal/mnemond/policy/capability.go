package policy

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/admission"
)

// Item is one decoded, validated candidate as a field map. The generic kind stamps id/actor/ingest_seq
// onto it and appends it to a resource's item list.
type Item map[string]any

type Limits struct {
	MaxPayloadBytes int
}

// Capability is the built-in descriptor that turns config selection into one compiled rule kind. ALL
// built-in capabilities admit a candidate through the SAME generic append-item-to-resource rule
// (appendItemRule); they differ only by DATA — the observed/proposed types, the resource kind, the
// item-list field, how a payload decodes to an Item, and the resource "header" fields a write carries
// (e.g. memory's rendered content, skill's name). A new capability is a new descriptor + config, not
// new rule code.
type Capability struct {
	Name         string
	ObservedType string
	ProposedType string
	ResourceKind contract.ResourceKind
	ItemsField   string // resource field holding the item list
	Decode       func(payload map[string]any) (Item, error)
	Header       func(items []Item) map[string]any // resource fields besides the item list + updated_by
	// RequiredHeader is the kind's kernel-required header fields, derived from the spec (the
	// render-produced keys, or the declared `required` subset). The assembler reads it to build the
	// assembly-time SchemaGuard so a declared kind's required set has ONE source — the policy.
	RequiredHeader []string
	// Sync, when Importable, opts the kind into Remote Workspace import under the named (closed-set)
	// merge strategy. The sync-import path derives its rules and syncable-kind set from this, so the
	// importable kinds are no longer a hardcoded list (PD6).
	Sync SyncOptions
	// DefaultEnabled opts the kind into governance on every local boot without an explicit --loop
	// (P3 coordination package). The app boot grants it to every host-agent principal.
	DefaultEnabled bool
	// Risk is the kind's governance risk tier ("low"|"mid"|"high"; P3). The assembler builds the
	// matching risk-gate rule per binding (mid = evidence required; high = operator-only).
	Risk   string
	Limits Limits
}

type SyncOptions struct {
	Importable bool
	Merge      string
}

// RemoteSyncedEventObserved is the event type the platform mints for a pulled remote material of this kind
// (the system-derived sync-import observation form, capability-spec v2 grammar). The import rule
// observes it; the puller emits it.
func (c Capability) RemoteSyncedEventObserved() string {
	return string(c.ResourceKind) + ".remote_synced_event.observed"
}

// Rule builds the capability's admission rule for one principal + resource ref. limits bounds the
// capability (MaxPayloadBytes; 0 = unbounded — the 1 MiB channel body cap still applies upstream)
// without changing the compiled kind.
//
// Deviation from the locked Phase-2 signature Rule(..., cfg config.CapabilityConfig)
// (plan-control-plane.md:241): the same plan locks capability as a rule/presentation-view/contract-only
// leaf (:51,:61); the leaf wins, and the assembler maps config.CapabilityConfig -> Limits.
func (c Capability) Rule(principal contract.ActorID, ref contract.ResourceRef, limits Limits) admission.Rule {
	return appendItemRule(c, principal, ref, limits)
}

// appendItemRule is the ONE generic kind: decode the candidate to an Item, stamp trusted id/actor/seq,
// append it to the resource's item list, and propose a write carrying the item list + the capability's
// header fields + updated_by. It only acts on events from its own principal.
func appendItemRule(c Capability, principal contract.ActorID, ref contract.ResourceRef, limits Limits) admission.Rule {
	return admission.NewNativeRule("local-"+c.Name+"-admission:"+string(principal), principal, c.ProposedType, []string{c.ObservedType},
		func(in admission.RuleInput) (contract.RuleDecision, error) {
			if in.Event.Actor != principal {
				return contract.RuleDecision{Verdict: contract.VerdictAllow}, nil
			}
			if limits.MaxPayloadBytes > 0 {
				raw, merr := json.Marshal(in.Event.Payload)
				if merr != nil || len(raw) > limits.MaxPayloadBytes {
					return contract.RuleDecision{Verdict: contract.VerdictDeny, Reasons: []string{fmt.Sprintf(
						"%s candidate denied: payload exceeds max_payload_bytes %d", c.Name, limits.MaxPayloadBytes)}}, nil
				}
			}
			item, err := c.Decode(in.Event.Payload)
			if err != nil {
				return contract.RuleDecision{Verdict: contract.VerdictDeny, Reasons: []string{err.Error()}}, nil
			}
			item["id"] = itemID(in.Event.Actor, in.Event.IngestSeq)
			item["actor"] = string(in.Event.Actor)
			item["ingest_seq"] = in.Event.IngestSeq
			if in.Event.TS != "" {
				item["created_at"] = in.Event.TS
			}
			version, fields := resourceFromPresentationView(in.View, ref)
			items := append(itemsFromFields(fields, c.ItemsField), item)
			newFields := map[string]any{c.ItemsField: items, "updated_by": string(in.Event.Actor)}
			for k, v := range c.Header(items) {
				newFields[k] = v
			}
			write := contract.ResourceWrite{Ref: ref, Kind: contract.OpCreate, Fields: newFields}
			if version > 0 {
				write.Kind = contract.OpUpdate
				write.BasedOn = version
			}
			return contract.RuleDecision{Verdict: contract.VerdictPropose, Proposal: &contract.ProposedEvent{
				Type:    c.ProposedType,
				Payload: map[string]any{"writes": []contract.ResourceWrite{write}},
			}}, nil
		})
}

func itemsFromFields(fields map[string]any, field string) []Item {
	if fields == nil {
		return nil
	}
	raw, ok := fields[field].([]any)
	if !ok {
		return nil
	}
	items := make([]Item, 0, len(raw))
	for _, r := range raw {
		m, ok := r.(map[string]any)
		if !ok {
			continue
		}
		if id, _ := m["id"].(string); id != "" {
			items = append(items, Item(m))
		}
	}
	return items
}

func itemID(actor contract.ActorID, ingestSeq int64) string {
	return memoryEntryID(actor, ingestSeq)
}

// ---- memory descriptor data ----

func renderMemoryItems(items []Item) string {
	lines := []string{"# Local Memory"}
	for _, it := range items {
		meta := []string{"id: " + itemString(it, "id"), "source: " + itemString(it, "source"), "confidence: " + itemString(it, "confidence")}
		if tags := itemStrings(it, "tags"); len(tags) > 0 {
			meta = append(meta, "tags: "+strings.Join(tags, ","))
		}
		lines = append(lines, "- "+itemString(it, "content")+" ("+strings.Join(meta, "; ")+")")
	}
	return strings.Join(lines, "\n")
}

// ---- skill descriptor data ----

func itemString(it Item, key string) string {
	if s, ok := it[key].(string); ok {
		return s
	}
	return ""
}

func itemStrings(it Item, key string) []string {
	switch raw := it[key].(type) {
	case []string:
		return raw
	case []any:
		out := make([]string, 0, len(raw))
		for _, v := range raw {
			if s, ok := v.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}
