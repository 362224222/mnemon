// Package capability holds the built-in admission rules (the pure leaf): given an Event + View
// it returns a RuleDecision, never writing. It imports rule/eventview/contract only — binding->rule
// translation and runtime wiring live in app. Memory + skill are the two P0 capabilities.
package policy

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/admission"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation/view"
)

const (
	MemoryWriteCandidateObserved = "memory.write_candidate.observed"
	MemoryWriteProposed          = "memory.write.proposed"
)

// entryDedupImport is the "entry-dedup" remote-import strategy (capability-spec v2 §Sync, the
// closed-set merge a kind selects): merge non-conflicting ENTRIES from a remote material into the
// resource's entry list, synthesizing one entry from a bare `content` field when the material carries
// none, and rejecting a same-id-different-content conflict (I15 — receiving-side admission is not
// relaxed). Parameterized by cap (kind/proposed type), so it carries no kind literal; memory selects
// it.
func entryDedupImport(cap Capability, in admission.RuleInput) (contract.RuleDecision, error) {
	material, err := decodeRemoteMemorySyncedEventMaterial(in.Event.Payload)
	if err != nil {
		return contract.RuleDecision{Verdict: contract.VerdictDeny, Reasons: []string{err.Error()}}, nil
	}
	if material.ResourceRef.Kind != cap.ResourceKind {
		return contract.RuleDecision{Verdict: contract.VerdictDeny, Reasons: []string{"remote import denied: resource kind does not match the importing capability"}}, nil
	}
	incoming := memoryEntriesFromFields(material.Fields)
	if len(incoming) == 0 {
		if content := strings.TrimSpace(stringField(material.Fields, "content")); content != "" {
			incoming = []memoryEntry{{
				ID:         remoteMemoryEntryID(material),
				Content:    content,
				Source:     "remote",
				Confidence: "remote",
				Actor:      string(material.Actor),
				IngestSeq:  material.LocalIngestSeq,
			}}
		}
	}
	if len(incoming) == 0 {
		return contract.RuleDecision{Verdict: contract.VerdictDeny, Reasons: []string{"remote import denied: no entries"}}, nil
	}
	version, fields := resourceFromEventView(in.View, material.ResourceRef)
	existing := memoryEntriesFromFields(fields)
	byID := make(map[string]memoryEntry, len(existing))
	for _, entry := range existing {
		byID[entry.ID] = entry
	}
	var additions []memoryEntry
	for _, entry := range incoming {
		if current, ok := byID[entry.ID]; ok {
			if current.Content != entry.Content {
				return contract.RuleDecision{Verdict: contract.VerdictDeny, Reasons: []string{"remote import conflict: entry " + entry.ID + " already exists with different content"}}, nil
			}
			continue
		}
		additions = append(additions, entry)
	}
	if len(additions) == 0 {
		return contract.RuleDecision{Verdict: contract.VerdictAllow}, nil
	}
	entries := append(append([]memoryEntry(nil), existing...), additions...)
	newFields := map[string]any{
		"content":    renderMemoryContent(entries),
		"entries":    entries,
		"updated_by": string(in.Event.Actor),
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

func decodeRemoteMemorySyncedEventMaterial(payload map[string]any) (contract.SyncedEventMaterial, error) {
	raw, ok := payload["material"]
	if !ok {
		return contract.SyncedEventMaterial{}, fmt.Errorf("remote memory import denied: missing material")
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return contract.SyncedEventMaterial{}, fmt.Errorf("remote memory import denied: encode material: %w", err)
	}
	var material contract.SyncedEventMaterial
	if err := json.Unmarshal(data, &material); err != nil {
		return contract.SyncedEventMaterial{}, fmt.Errorf("remote memory import denied: decode material: %w", err)
	}
	if strings.TrimSpace(material.OriginReplicaID) == "" || strings.TrimSpace(material.LocalDecisionID) == "" {
		return contract.SyncedEventMaterial{}, fmt.Errorf("remote memory import denied: missing provenance")
	}
	return material, nil
}

func remoteMemoryEntryID(material contract.SyncedEventMaterial) string {
	return "remote/" + sanitizeEntryIDPart(material.OriginReplicaID) + "/" + sanitizeEntryIDPart(material.LocalDecisionID)
}

type memoryEntry struct {
	ID         string   `json:"id"`
	Content    string   `json:"content"`
	Source     string   `json:"source"`
	Confidence string   `json:"confidence"`
	Tags       []string `json:"tags,omitempty"`
	Actor      string   `json:"actor"`
	IngestSeq  int64    `json:"ingest_seq"`
}

func stringField(payload map[string]any, key string) string {
	if v, ok := payload[key].(string); ok {
		return v
	}
	return ""
}

func stringSliceField(payload map[string]any, key string) []string {
	switch raw := payload[key].(type) {
	case []string:
		return compactStrings(raw)
	case []any:
		out := make([]string, 0, len(raw))
		for _, v := range raw {
			if s, ok := v.(string); ok {
				out = append(out, s)
			}
		}
		return compactStrings(out)
	case string:
		return compactStrings(strings.Split(raw, ","))
	default:
		return nil
	}
}

func compactStrings(in []string) []string {
	var out []string
	for _, s := range in {
		if trimmed := strings.TrimSpace(s); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func containsSecretLikeContent(content string) bool {
	lower := strings.ToLower(content)
	for _, marker := range []string{
		"password=", "password:", "api_key", "api key", "secret=", "secret:",
		"token=", "token:", "bearer ", "private key", "-----begin",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return regexp.MustCompile(`sk-[a-zA-Z0-9]{12,}`).FindString(content) != ""
}

func containsPromptInjectionShape(content string) bool {
	lower := strings.ToLower(content)
	for _, marker := range []string{
		"ignore previous instructions",
		"disregard previous instructions",
		"reveal the system prompt",
		"show the system prompt",
		"developer message",
		"act as system",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func resourceFromEventView(view view.View, ref contract.ResourceRef) (contract.Version, map[string]any) {
	var version contract.Version
	for _, rv := range view.Resources {
		if rv.Ref == ref {
			version = rv.Version
			break
		}
	}
	for _, item := range view.Content {
		if item.Ref == ref {
			return item.Version, item.Fields
		}
	}
	return version, nil
}

func memoryEntriesFromFields(fields map[string]any) []memoryEntry {
	if fields == nil {
		return nil
	}
	raw, ok := fields["entries"].([]any)
	if !ok {
		return nil
	}
	entries := make([]memoryEntry, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		entry := memoryEntry{
			ID:         stringMapField(m, "id"),
			Content:    stringMapField(m, "content"),
			Source:     stringMapField(m, "source"),
			Confidence: stringMapField(m, "confidence"),
			Tags:       stringSliceMapField(m, "tags"),
			Actor:      stringMapField(m, "actor"),
			IngestSeq:  int64MapField(m, "ingest_seq"),
		}
		if entry.ID != "" && entry.Content != "" {
			entries = append(entries, entry)
		}
	}
	return entries
}

func stringMapField(m map[string]any, key string) string {
	if s, ok := m[key].(string); ok {
		return s
	}
	return ""
}

func stringSliceMapField(m map[string]any, key string) []string {
	if raw, ok := m[key].([]any); ok {
		out := make([]string, 0, len(raw))
		for _, v := range raw {
			if s, ok := v.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

func int64MapField(m map[string]any, key string) int64 {
	switch v := m[key].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	default:
		return 0
	}
}

func renderMemoryContent(entries []memoryEntry) string {
	var lines []string
	lines = append(lines, "# Local Memory")
	for _, entry := range entries {
		meta := []string{"id: " + entry.ID, "source: " + entry.Source, "confidence: " + entry.Confidence}
		if len(entry.Tags) > 0 {
			meta = append(meta, "tags: "+strings.Join(entry.Tags, ","))
		}
		lines = append(lines, "- "+entry.Content+" ("+strings.Join(meta, "; ")+")")
	}
	return strings.Join(lines, "\n")
}

func memoryEntryID(actor contract.ActorID, ingestSeq int64) string {
	return "local/" + sanitizeEntryIDPart(string(actor)) + "/" + strconv.FormatInt(ingestSeq, 10)
}

func sanitizeEntryIDPart(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	if b.Len() == 0 {
		return "unknown"
	}
	return b.String()
}
