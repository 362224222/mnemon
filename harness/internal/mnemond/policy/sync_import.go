package policy

import (
	"fmt"
	"sort"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/admission"
)

// RemoteImportRule builds the remote-import admission rule for one importable event package and the
// sync import principal: it observes the package's system-derived <kind>.remote_synced_event.observed
// event and dispatches to the package's declared merge strategy. Returns ok=false when the package is
// not importable.
func RemoteImportRule(cap EventPackage, principal contract.ActorID) (admission.Rule, bool) {
	if !cap.Sync.Importable {
		return nil, false
	}
	strategy := importStrategy(cap.Sync.Merge)
	if strategy == nil {
		return nil, false
	}
	return admission.NewNativeRule("remote-import:"+cap.Name+":"+string(principal), principal, cap.ProposedType, []string{cap.RemoteSyncedEventObserved()},
		func(in admission.RuleInput) (contract.RuleDecision, error) {
			if in.Event.Actor != principal {
				return contract.RuleDecision{Verdict: contract.VerdictAllow}, nil
			}
			return strategy(cap, in)
		}), true
}

// importStrategy maps a (CompileExternalSpec-validated) merge-strategy name to its closed-set implementation.
func importStrategy(merge string) func(EventPackage, admission.RuleInput) (contract.RuleDecision, error) {
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

// RemoteImportRules builds the remote-import rules for every importable event package in the catalog,
// sorted by kind for determinism.
func RemoteImportRules(catalog Registry, principal contract.ActorID) []admission.Rule {
	var rules []admission.Rule
	for _, cap := range sortedImportable(catalog) {
		if r, ok := RemoteImportRule(cap, principal); ok {
			rules = append(rules, r)
		}
	}
	return rules
}

// ImportableKinds returns the resource kinds the catalog imports from Remote Workspace pulls, sorted
// — the descriptor-derived syncable-kind set (PD6).
func ImportableKinds(catalog Registry) []contract.ResourceKind {
	var kinds []contract.ResourceKind
	for _, cap := range sortedImportable(catalog) {
		kinds = append(kinds, cap.ResourceKind)
	}
	return kinds
}

// RemoteSyncedEventType returns the import observation event type for a pulled material kind when the
// catalog imports that kind — the descriptor-derived replacement for the hardcoded kind→type switch.
func RemoteSyncedEventType(catalog Registry, kind contract.ResourceKind) (string, bool) {
	for _, cap := range catalog {
		if cap.Sync.Importable && cap.ResourceKind == kind {
			return cap.RemoteSyncedEventObserved(), true
		}
	}
	return "", false
}

func sortedImportable(catalog Registry) []EventPackage {
	var caps []EventPackage
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
// below turns it into a durable sync.diagnostic via the existing pre-gate. Payload rule:
// {kind, origin_replica_id, local_decision_id, remote_id}.
const SyncImportSkippedObserved = "sync.import_skipped.observed"

// SyncRemoteDiagnosticObserved is the observation a sync puller ingests when a Remote Workspace
// returns a pull-side diagnostic for an invalid/rejected/conflicting publication entry. Payload rule:
// {remote_id, origin_mnemond, event_id, subject, status}; payload narrative: {diagnostic}.
const SyncRemoteDiagnosticObserved = "sync.remote_diagnostic.observed"

// SyncImportSkippedRule is the legal diagnostic mechanism for skipped kinds: it Handles ONLY the
// skipped observation, gates on the sync import principal (foreign events pass through), and always
// denies with a reason naming the kind — the deny is what produces the durable *.diagnostic (S7);
// no write, no proposal.
func SyncImportSkippedRule(principal contract.ActorID) admission.Rule {
	return admission.NewNativeRule("sync-import-skipped:"+string(principal), principal, "", []string{SyncImportSkippedObserved},
		func(in admission.RuleInput) (contract.RuleDecision, error) {
			if in.Event.Actor != principal {
				return contract.RuleDecision{Verdict: contract.VerdictAllow}, nil
			}
			rule := payloadSection(in.Event.Payload, FieldSectionRule)
			kind, _ := rule["kind"].(string)
			if kind == "" {
				kind = "unknown"
			}
			return contract.RuleDecision{
				Verdict: contract.VerdictDeny,
				Reasons: []string{fmt.Sprintf("sync import skipped: resource kind %q has no import mapping on this replica", kind)},
			}, nil
		})
}

// SyncRemoteDiagnosticRule is the legal diagnostic mechanism for pull-side Remote Workspace
// diagnostics. Like skipped-kind import, it denies a sync.* observation so the kernel emits one
// durable sync.diagnostic with lineage to the original remote diagnostic observation.
func SyncRemoteDiagnosticRule(principal contract.ActorID) admission.Rule {
	return admission.NewNativeRule("sync-remote-diagnostic:"+string(principal), principal, "", []string{SyncRemoteDiagnosticObserved},
		func(in admission.RuleInput) (contract.RuleDecision, error) {
			if in.Event.Actor != principal {
				return contract.RuleDecision{Verdict: contract.VerdictAllow}, nil
			}
			rule := payloadSection(in.Event.Payload, FieldSectionRule)
			narrative := payloadSection(in.Event.Payload, FieldSectionNarrative)
			remoteID, _ := rule["remote_id"].(string)
			status, _ := rule["status"].(string)
			origin, _ := rule["origin_mnemond"].(string)
			eventID, _ := rule["event_id"].(string)
			subject, _ := rule["subject"].(string)
			diagnostic, _ := narrative["diagnostic"].(string)
			return contract.RuleDecision{
				Verdict: contract.VerdictDeny,
				Reasons: []string{fmt.Sprintf("remote workspace diagnostic: remote_id=%q status=%q origin_mnemond=%q event_id=%q subject=%q: %s",
					remoteID, status, origin, eventID, subject, diagnostic)},
			}, nil
		})
}
