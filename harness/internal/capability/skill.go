package capability

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/eventview"
	"github.com/mnemon-dev/mnemon/harness/internal/rule"
)

const (
	SkillWriteCandidateObserved = "skill.write_candidate.observed"
	SkillWriteProposed          = "skill.write.proposed"
)

// declarationDedupImport is the "declaration-dedup" remote-import strategy (capability-spec v2
// §Sync): merge non-conflicting DECLARATIONS from a remote material into the resource's declaration
// list, VALIDATING each imported declaration (id format, status enum, secret/injection scan — I15,
// receiving-side admission is not relaxed) and rejecting a same-id-different-content conflict.
// Parameterized by cap (kind/proposed type); skill selects it.
func declarationDedupImport(cap Capability, in rule.RuleInput) (contract.RuleDecision, error) {
	material, err := decodeRemoteSkillSyncedEventMaterial(in.Event.Payload)
	if err != nil {
		return contract.RuleDecision{Verdict: contract.VerdictDeny, Reasons: []string{err.Error()}}, nil
	}
	if material.ResourceRef.Kind != cap.ResourceKind {
		return contract.RuleDecision{Verdict: contract.VerdictDeny, Reasons: []string{"remote import denied: resource kind does not match the importing capability"}}, nil
	}
	incoming := skillDeclarationsFromFields(material.Fields)
	if len(incoming) == 0 {
		return contract.RuleDecision{Verdict: contract.VerdictDeny, Reasons: []string{"remote import denied: no declarations"}}, nil
	}
	for _, decl := range incoming {
		if reason := validateRemoteSkillDeclaration(decl); reason != "" {
			return contract.RuleDecision{Verdict: contract.VerdictDeny, Reasons: []string{reason}}, nil
		}
	}
	version, fields := skillResourceFromEventView(in.View, material.ResourceRef)
	existing := skillDeclarationsFromFields(fields)
	byID := make(map[string]skillDeclaration, len(existing))
	for _, decl := range existing {
		byID[decl.ID] = decl
	}
	var additions []skillDeclaration
	for _, decl := range incoming {
		if current, ok := byID[decl.ID]; ok {
			if !sameSkillDeclaration(current, decl) {
				return contract.RuleDecision{Verdict: contract.VerdictDeny, Reasons: []string{"remote import conflict: declaration " + decl.ID + " already exists with different content"}}, nil
			}
			continue
		}
		additions = append(additions, decl)
	}
	if len(additions) == 0 {
		return contract.RuleDecision{Verdict: contract.VerdictAllow}, nil
	}
	declarations := append(append([]skillDeclaration(nil), existing...), additions...)
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

type skillDeclaration struct {
	ID         string `json:"id"`
	SkillID    string `json:"skill_id"`
	Name       string `json:"name"`
	Status     string `json:"status"`
	Content    string `json:"content,omitempty"`
	Source     string `json:"source"`
	Confidence string `json:"confidence"`
	Actor      string `json:"actor"`
	IngestSeq  int64  `json:"ingest_seq"`
}

func decodeRemoteSkillSyncedEventMaterial(payload map[string]any) (contract.SyncedEventMaterial, error) {
	raw, ok := payload["material"]
	if !ok {
		return contract.SyncedEventMaterial{}, fmt.Errorf("remote skill import denied: missing material")
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return contract.SyncedEventMaterial{}, fmt.Errorf("remote skill import denied: encode material: %w", err)
	}
	var material contract.SyncedEventMaterial
	if err := json.Unmarshal(data, &material); err != nil {
		return contract.SyncedEventMaterial{}, fmt.Errorf("remote skill import denied: decode material: %w", err)
	}
	if strings.TrimSpace(material.OriginReplicaID) == "" || strings.TrimSpace(material.LocalDecisionID) == "" {
		return contract.SyncedEventMaterial{}, fmt.Errorf("remote skill import denied: missing provenance")
	}
	return material, nil
}

func validateRemoteSkillDeclaration(decl skillDeclaration) string {
	if !validSkillID(decl.SkillID) {
		return "remote skill import denied: invalid skill_id"
	}
	if decl.Status != "active" && decl.Status != "stale" && decl.Status != "archived" {
		return "remote skill import denied: invalid status"
	}
	if containsSecretLikeContent(decl.Content) || containsPromptInjectionShape(decl.Content) {
		return "remote skill import denied: unsafe content"
	}
	return ""
}

func sameSkillDeclaration(a, b skillDeclaration) bool {
	return reflect.DeepEqual(a, b)
}

func validSkillID(s string) bool {
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			continue
		}
		return false
	}
	return true
}

func skillResourceFromEventView(view eventview.EventView, ref contract.ResourceRef) (contract.Version, map[string]any) {
	return resourceFromEventView(view, ref)
}

func skillDeclarationsFromFields(fields map[string]any) []skillDeclaration {
	if fields == nil {
		return nil
	}
	raw, ok := fields["declarations"].([]any)
	if !ok {
		return nil
	}
	declarations := make([]skillDeclaration, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		decl := skillDeclaration{
			ID:         stringMapField(m, "id"),
			SkillID:    stringMapField(m, "skill_id"),
			Name:       stringMapField(m, "name"),
			Status:     stringMapField(m, "status"),
			Content:    stringMapField(m, "content"),
			Source:     stringMapField(m, "source"),
			Confidence: stringMapField(m, "confidence"),
			Actor:      stringMapField(m, "actor"),
			IngestSeq:  int64MapField(m, "ingest_seq"),
		}
		if decl.ID != "" && decl.SkillID != "" && decl.Name != "" {
			declarations = append(declarations, decl)
		}
	}
	return declarations
}
