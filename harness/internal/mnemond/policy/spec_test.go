package policy

import (
	"strings"
	"testing"
)

func minimalSpec() CapabilitySpec {
	return CapabilitySpec{
		SchemaVersion: 1,
		Name:          "note", ObservedType: "note.write_candidate.observed",
		ProposedType: "note.write.proposed", ResourceKind: "note", ItemsField: "items",
		Fields: []FieldSpec{{Name: "text", Validators: []ValidatorRef{
			{ID: "required", Params: map[string]string{"missing_style": "empty"}},
			{ID: "safety:unsafe"},
		}}},
		Render: RenderSpec{Content: &ContentRender{Member: "bullet-list",
			Params: map[string]string{"title": "# Notes", "field": "text"}}},
	}
}

func TestFromSpecCompilesMinimal(t *testing.T) {
	if _, err := FromSpec(minimalSpec()); err != nil {
		t.Fatalf("a well-formed spec must compile: %v", err)
	}
}

// Required-derivation rule (capability-spec v2): a kind's kernel-required header fields are the
// spec's render-produced keys when `required` is omitted, else exactly the declared subset.
func TestFromSpecRequiredDerivation(t *testing.T) {
	// Default: render produces "content" (bullet-list), no `required` → RequiredHeader = ["content"].
	cap, err := FromSpec(minimalSpec())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if got := cap.RequiredHeader; len(got) != 1 || got[0] != "content" {
		t.Fatalf("default RequiredHeader = render-produced keys, want [content], got %v", got)
	}
	// Subset selection: render produces {content, statement}; required selects only statement.
	s := minimalSpec()
	s.Render.Static = map[string]string{"statement": "project"}
	s.Required = []string{"statement"}
	cap, err = FromSpec(s)
	if err != nil {
		t.Fatalf("compile with required subset: %v", err)
	}
	if got := cap.RequiredHeader; len(got) != 1 || got[0] != "statement" {
		t.Fatalf("declared required selects the subset, want [statement], got %v", got)
	}
}

// 每条 fail-closed 路径一例:unknown 成员、参数缺失/未知、schema_version、重复字段、
// 前向 default-from、list 独占、render 键冲突、kind 不在 KindCatalog。
func TestFromSpecFailsClosed(t *testing.T) {
	mutate := func(name string, fn func(*CapabilitySpec), wantErr string) {
		t.Helper()
		s := minimalSpec()
		fn(&s)
		_, err := FromSpec(s)
		if err == nil || !strings.Contains(err.Error(), wantErr) {
			t.Fatalf("%s: want error containing %q, got %v", name, wantErr, err)
		}
	}
	mutate("unknown validator", func(s *CapabilitySpec) { s.Fields[0].Validators[0].ID = "regex" }, "unknown validator")
	mutate("unknown render", func(s *CapabilitySpec) { s.Render.Content.Member = "html" }, "unknown render")
	mutate("missing resource kind", func(s *CapabilitySpec) { s.ResourceKind = "" }, "missing resource_kind")
	// G8 reservation (capability-spec v2): a spec declares its OWN kind (a non-reserved kind like
	// "phantom" now compiles — that is the declarative-kind feature), but may not claim a governance
	// kind, the mnemon namespace, or a reserved first-party event family.
	mutate("governance kind reserved", func(s *CapabilitySpec) { s.ResourceKind = "lease" }, "kernel-internal governance kind")
	mutate("mnemon namespace reserved", func(s *CapabilitySpec) { s.ResourceKind = "mnemon" }, "reserved mnemon namespace")
	mutate("reserved first-party family", func(s *CapabilitySpec) { s.ResourceKind = "sync" }, "reserved first-party event family")
	mutate("dashed name", func(s *CapabilitySpec) { s.Name = "my-loop" }, "event-family segment")
	mutate("foreign observed family", func(s *CapabilitySpec) {
		s.ObservedType = "other.write_candidate.observed"
	}, "frozen type grammar")
	// Bijection pin (capability-spec v2): the event family is the spec's OWN kind, never an open
	// parameter — a well-formed-but-mismatched-prefix observed_type is rejected, not just free text.
	mutate("mismatched observed prefix", func(s *CapabilitySpec) {
		s.ObservedType = "bar.write_candidate.observed"
	}, "frozen type grammar")
	// System-derived forms (capability-spec v2 grammar table): the platform mints
	// <kind>.remote_synced_event.observed (the sync-import observation); a spec may NEVER declare it.
	mutate("system-derived observed form", func(s *CapabilitySpec) {
		s.ObservedType = "note.remote_synced_event.observed"
	}, "system-derived")
	mutate("system-derived proposed form", func(s *CapabilitySpec) {
		s.ProposedType = "note.remote_synced_event.observed"
	}, "system-derived")
	mutate("free-form proposed type", func(s *CapabilitySpec) {
		s.ProposedType = "note.write.done"
	}, "reconciler consumes only *.proposed")
	mutate("bad schema version", func(s *CapabilitySpec) { s.SchemaVersion = 2 }, "schema_version 2 unsupported")
	mutate("missing validator param", func(s *CapabilitySpec) { s.Fields[0].Validators[0].Params = nil }, "missing param")
	mutate("unknown validator param", func(s *CapabilitySpec) {
		s.Fields[0].Validators[0].Params["typo"] = "x"
	}, "unknown param")
	mutate("bad missing_style", func(s *CapabilitySpec) {
		s.Fields[0].Validators[0].Params["missing_style"] = "loud"
	}, "must be empty|missing")
	mutate("duplicate field", func(s *CapabilitySpec) {
		s.Fields = append(s.Fields, FieldSpec{Name: "text"})
	}, "duplicate field")
	mutate("forward default-from", func(s *CapabilitySpec) {
		s.Fields = append(s.Fields, FieldSpec{Name: "alias", Validators: []ValidatorRef{
			{ID: "default-from", Params: map[string]string{"field": "later"}},
		}}, FieldSpec{Name: "later"})
	}, "previously declared")
	mutate("list not exclusive", func(s *CapabilitySpec) {
		s.Fields = append(s.Fields, FieldSpec{Name: "tags", Validators: []ValidatorRef{
			{ID: "list:strings"}, {ID: "safety:unsafe"},
		}})
	}, "only validator")
	mutate("render field undeclared", func(s *CapabilitySpec) {
		s.Render.Content.Params["field"] = "ghost"
	}, "not declared")
	mutate("render collides with items_field", func(s *CapabilitySpec) {
		s.Render.Static = map[string]string{"items": "x"}
	}, "reserved resource key")
	mutate("render collides with updated_by", func(s *CapabilitySpec) {
		s.Render.Static = map[string]string{"updated_by": "x"}
	}, "reserved resource key")
	mutate("static and content both produce content", func(s *CapabilitySpec) {
		s.Render.Static = map[string]string{"content": "x"}
	}, "both produce")
	mutate("missing render param", func(s *CapabilitySpec) {
		delete(s.Render.Content.Params, "title")
	}, "missing param")
	mutate("required names unproduced key", func(s *CapabilitySpec) {
		s.Required = []string{"ghost"}
	}, "not one the render produces")
}
