package policy

import (
	"fmt"
	"sort"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/rule"
)

// RemoteImportRule builds the remote-import admission rule for one importable capability and the sync
// import principal: it observes the capability's system-derived <kind>.remote_synced_event.observed event
// and dispatches to the capability's declared (closed-set) merge strategy. Returns ok=false when the
// capability is not importable (the caller skips it).
func RemoteImportRule(cap Capability, principal contract.ActorID) (rule.Rule, bool) {
	if !cap.Sync.Importable {
		return nil, false
	}
	strategy := importStrategy(cap.Sync.Merge)
	if strategy == nil {
		return nil, false
	}
	return rule.NewNativeRule("remote-import:"+cap.Name+":"+string(principal), principal, cap.ProposedType, []string{cap.RemoteSyncedEventObserved()},
		func(in rule.RuleInput) (contract.RuleDecision, error) {
			if in.Event.Actor != principal {
				return contract.RuleDecision{Verdict: contract.VerdictAllow}, nil
			}
			return strategy(cap, in)
		}), true
}

// importStrategy maps a (FromSpec-validated) merge-strategy name to its closed-set implementation.
func importStrategy(merge string) func(Capability, rule.RuleInput) (contract.RuleDecision, error) {
	switch merge {
	case "entry-dedup":
		return entryDedupImport
	case "declaration-dedup":
		return declarationDedupImport
	case "item-dedup":
		return itemDedupImport
	default:
		return nil
	}
}

// RemoteImportRules builds the remote-import rules for every importable capability in the catalog,
// sorted by kind for determinism — the descriptor-derived replacement for the hardcoded
// memory/skill import-rule list (PD6).
func RemoteImportRules(catalog map[string]Capability, principal contract.ActorID) []rule.Rule {
	var rules []rule.Rule
	for _, cap := range sortedImportable(catalog) {
		if r, ok := RemoteImportRule(cap, principal); ok {
			rules = append(rules, r)
		}
	}
	return rules
}

// ImportableKinds returns the resource kinds the catalog imports from Remote Workspace pulls, sorted
// — the descriptor-derived syncable-kind set (PD6).
func ImportableKinds(catalog map[string]Capability) []contract.ResourceKind {
	var kinds []contract.ResourceKind
	for _, cap := range sortedImportable(catalog) {
		kinds = append(kinds, cap.ResourceKind)
	}
	return kinds
}

// RemoteSyncedEventType returns the import observation event type for a pulled material kind when the
// catalog imports that kind — the descriptor-derived replacement for the hardcoded kind→type switch.
func RemoteSyncedEventType(catalog map[string]Capability, kind contract.ResourceKind) (string, bool) {
	for _, cap := range catalog {
		if cap.Sync.Importable && cap.ResourceKind == kind {
			return cap.RemoteSyncedEventObserved(), true
		}
	}
	return "", false
}

func sortedImportable(catalog map[string]Capability) []Capability {
	var caps []Capability
	for _, cap := range catalog {
		if cap.Sync.Importable {
			caps = append(caps, cap)
		}
	}
	sort.Slice(caps, func(i, j int) bool { return caps[i].ResourceKind < caps[j].ResourceKind })
	return caps
}

// SyncImportSkippedObserved is the observation a sync puller ingests for a pulled material whose
// resource kind has no import mapping (v1.1 #4): instead of a silent continue, the skip enters the
// canonical log exactly-once (ExternalID = the six-part pull key + ":skipped") and the deny rule
// below turns it into a durable sync.diagnostic via the existing pre-gate. Payload: {kind,
// origin_replica_id, local_decision_id, remote_id}.
const SyncImportSkippedObserved = "sync.import_skipped.observed"

// SyncImportSkippedRule is the legal diagnostic mechanism for skipped kinds: it Handles ONLY the
// skipped observation, gates on the sync import principal (foreign events pass through), and always
// denies with a reason naming the kind — the deny is what produces the durable *.diagnostic (S7);
// no write, no proposal.
func SyncImportSkippedRule(principal contract.ActorID) rule.Rule {
	return rule.NewNativeRule("sync-import-skipped:"+string(principal), principal, "", []string{SyncImportSkippedObserved},
		func(in rule.RuleInput) (contract.RuleDecision, error) {
			if in.Event.Actor != principal {
				return contract.RuleDecision{Verdict: contract.VerdictAllow}, nil
			}
			kind, _ := in.Event.Payload["kind"].(string)
			if kind == "" {
				kind = "unknown"
			}
			return contract.RuleDecision{
				Verdict: contract.VerdictDeny,
				Reasons: []string{fmt.Sprintf("sync import skipped: resource kind %q has no import mapping on this replica", kind)},
			}, nil
		})
}
