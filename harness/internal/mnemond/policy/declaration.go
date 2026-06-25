package policy

import (
	"reflect"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/admission"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation/view"
)

// declarationDedupImport is the "declaration-dedup" remote-import strategy. It merges
// non-conflicting declarations from a remote material into the resource's declaration list,
// validates each imported declaration, and rejects same-id/different-content conflicts. The
// strategy is parameterized by the event package descriptor, so it carries no product-kind literal.
func declarationDedupImport(cap EventPackage, in admission.RuleInput) (contract.RuleDecision, error) {
	material, err := decodeRemoteSyncedEventMaterial(in.Event.Payload)
	if err != nil {
		return contract.RuleDecision{Verdict: contract.VerdictDeny, Reasons: []string{err.Error()}}, nil
	}
	if material.ResourceRef.Kind != cap.ResourceKind {
		return contract.RuleDecision{Verdict: contract.VerdictDeny, Reasons: []string{"remote import denied: resource kind does not match the importing event package"}}, nil
	}
	incoming := declarationsFromFields(material.Fields)
	if len(incoming) == 0 {
		return contract.RuleDecision{Verdict: contract.VerdictDeny, Reasons: []string{"remote import denied: no declarations"}}, nil
	}
	for _, decl := range incoming {
		if reason := validateRemoteDeclaration(decl); reason != "" {
			return contract.RuleDecision{Verdict: contract.VerdictDeny, Reasons: []string{reason}}, nil
		}
	}
	version, fields := declarationResourceFromPresentationView(in.View, material.ResourceRef)
	existing := declarationsFromFields(fields)
	byID := make(map[string]declarationItem, len(existing))
	for _, decl := range existing {
		byID[decl.ID] = decl
	}
	var additions []declarationItem
	for _, decl := range incoming {
		if current, ok := byID[decl.ID]; ok {
			if !sameDeclaration(current, decl) {
				return contract.RuleDecision{Verdict: contract.VerdictDeny, Reasons: []string{"remote import conflict: declaration " + decl.ID + " already exists with different content"}}, nil
			}
			continue
		}
		additions = append(additions, decl)
	}
	if len(additions) == 0 {
		return contract.RuleDecision{Verdict: contract.VerdictAllow}, nil
	}
	declarations := append(append([]declarationItem(nil), existing...), additions...)
	newFields := map[string]any{
		"name":         "project",
		"declarations": declarations,
		"updated_by":   string(in.Event.Actor),
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

type declarationItem struct {
	ID            string `json:"id"`
	DeclarationID string `json:"declaration_id"`
	Name          string `json:"name"`
	Status        string `json:"status"`
	Content       string `json:"content,omitempty"`
	Source        string `json:"source"`
	Confidence    string `json:"confidence"`
	Actor         string `json:"actor"`
	IngestSeq     int64  `json:"ingest_seq"`
}

func validateRemoteDeclaration(decl declarationItem) string {
	if !validIdentifier(decl.DeclarationID) {
		return "remote declaration import denied: invalid declaration_id"
	}
	if decl.Status != "active" && decl.Status != "stale" && decl.Status != "archived" {
		return "remote declaration import denied: invalid status"
	}
	if containsSecretLikeContent(decl.Content) || containsPromptInjectionShape(decl.Content) {
		return "remote declaration import denied: unsafe content"
	}
	return ""
}

func sameDeclaration(a, b declarationItem) bool {
	return reflect.DeepEqual(a, b)
}

func validIdentifier(s string) bool {
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			continue
		}
		return false
	}
	return s != ""
}

func declarationResourceFromPresentationView(view view.View, ref contract.ResourceRef) (contract.Version, map[string]any) {
	return resourceFromPresentationView(view, ref)
}

func declarationsFromFields(fields map[string]any) []declarationItem {
	if fields == nil {
		return nil
	}
	raw, ok := fields["declarations"].([]any)
	if !ok {
		return nil
	}
	declarations := make([]declarationItem, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		decl := declarationItem{
			ID:            stringMapField(m, "id"),
			DeclarationID: stringMapField(m, "declaration_id"),
			Name:          stringMapField(m, "name"),
			Status:        stringMapField(m, "status"),
			Content:       stringMapField(m, "content"),
			Source:        stringMapField(m, "source"),
			Confidence:    stringMapField(m, "confidence"),
			Actor:         stringMapField(m, "actor"),
			IngestSeq:     int64MapField(m, "ingest_seq"),
		}
		if decl.ID != "" && decl.DeclarationID != "" && decl.Name != "" {
			declarations = append(declarations, decl)
		}
	}
	return declarations
}
