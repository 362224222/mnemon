package policy

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/admission"
)

// itemDedupImport is the "item-dedup" remote-import strategy: the GENERIC
// append-merge for a directory-of-items kind (§577). It merges a remote material's items into the
// resource's item list BY ID, preserving EVERY item field. It makes no assumption about
// the item's domain fields, so an arbitrary declared kind syncs without losing fields such as
// assignment scope/ttl/assignee. Item ids are replica-specific
// (actor+ingest_seq stamped at admission), so cross-replica items never collide; a
// same-id/different-content divergence is rejected (I15, defensive). The merged resource header is
// re-derived from the event package's OWN render, never hardcoded.
func itemDedupImport(cap EventPackage, in admission.RuleInput) (contract.RuleDecision, error) {
	material, err := decodeRemoteSyncedEventMaterial(in.Event.Payload)
	if err != nil {
		return contract.RuleDecision{Verdict: contract.VerdictDeny, Reasons: []string{err.Error()}}, nil
	}
	if material.ResourceRef.Kind != cap.ResourceKind {
		return contract.RuleDecision{Verdict: contract.VerdictDeny, Reasons: []string{"remote import denied: resource kind does not match the importing event package"}}, nil
	}
	incoming := itemsFromFields(material.Fields, cap.ItemsField)
	if len(incoming) == 0 {
		return contract.RuleDecision{Verdict: contract.VerdictDeny, Reasons: []string{"remote import denied: no items"}}, nil
	}
	version, fields := resourceFromPresentationView(in.View, material.ResourceRef)
	existing := itemsFromFields(fields, cap.ItemsField)
	byID := make(map[string]Item, len(existing))
	for _, it := range existing {
		byID[stringMapField(it, "id")] = it
	}
	var additions []Item
	for _, it := range incoming {
		id := stringMapField(it, "id")
		if cur, ok := byID[id]; ok {
			if !reflect.DeepEqual(cur, it) {
				return contract.RuleDecision{Verdict: contract.VerdictDeny, Reasons: []string{"remote import conflict: item " + id + " already exists with different content"}}, nil
			}
			continue
		}
		additions = append(additions, it)
	}
	if len(additions) == 0 {
		return contract.RuleDecision{Verdict: contract.VerdictAllow}, nil
	}
	items := append(append([]Item(nil), existing...), additions...)
	newFields := map[string]any{cap.ItemsField: items, "updated_by": string(in.Event.Actor)}
	for k, v := range cap.Header(items) {
		newFields[k] = v
	}
	write := contract.ResourceWrite{Ref: material.ResourceRef, Kind: contract.OpCreate, Fields: newFields}
	if version > 0 {
		write.Kind = contract.OpUpdate
		write.BasedOn = version
	}
	return contract.RuleDecision{Verdict: contract.VerdictPropose, Proposal: &contract.ProposedEvent{
		Type:    cap.ProposedType,
		Payload: map[string]any{"writes": []contract.ResourceWrite{write}},
	}}, nil
}

// decodeRemoteSyncedEventMaterial decodes a remote SyncedEventMaterial from an import event payload.
func decodeRemoteSyncedEventMaterial(payload map[string]any) (contract.SyncedEventMaterial, error) {
	raw, ok := payload["material"]
	if !ok {
		return contract.SyncedEventMaterial{}, fmt.Errorf("remote import denied: missing material")
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return contract.SyncedEventMaterial{}, fmt.Errorf("remote import denied: encode material: %w", err)
	}
	var material contract.SyncedEventMaterial
	if err := json.Unmarshal(data, &material); err != nil {
		return contract.SyncedEventMaterial{}, fmt.Errorf("remote import denied: decode material: %w", err)
	}
	if strings.TrimSpace(material.OriginReplicaID) == "" || strings.TrimSpace(material.LocalDecisionID) == "" {
		return contract.SyncedEventMaterial{}, fmt.Errorf("remote import denied: missing provenance")
	}
	return material, nil
}
