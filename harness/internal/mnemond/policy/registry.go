package policy

import "fmt"

var standardRegistry = mustStandardRegistry()

// StandardRegistry returns the first-party standard event package registry. These packages are
// defined in Go, not JSON assets; external JSON packages are merged by ResolveRegistry.
func StandardRegistry() Registry { return cloneRegistry(standardRegistry) }

func mustStandardRegistry() Registry {
	packages := []EventPackage{
		StandardAgentProfile(),
		StandardProjectIntent(),
		StandardTeamworkSignal(),
		StandardAssignment(),
		StandardProgressDigest(),
	}
	out := Registry{}
	claims := newRegistryClaims()
	for _, pkg := range packages {
		if err := claims.claim("standard event package "+pkg.Name, pkg); err != nil {
			panic(fmt.Sprintf("standard event package registry must be internally consistent: %v", err))
		}
		out[pkg.Name] = pkg
	}
	return out
}

func cloneRegistry(in Registry) Registry {
	out := make(Registry, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func StandardAgentProfile() EventPackage {
	return mustStandardEventPackage(eventPackageDefinition{
		Name:           "agent_profile",
		ObservedType:   observedEventType("agent_profile"),
		ProposedType:   proposedEventType("agent_profile"),
		ResourceKind:   "agent_profile",
		ItemsField:     "items",
		Fields:         agentProfileFields(),
		Render:         bulletListRender("# Agent Profiles", "summary"),
		DefaultEnabled: true,
		Risk:           "low",
		Sync:           SyncOptions{Importable: true, Merge: "item-dedup"},
		SyncSet:        true,
		Source:         "standard event package",
	})
}

func StandardProjectIntent() EventPackage {
	return mustStandardEventPackage(eventPackageDefinition{
		Name:           "project_intent",
		ObservedType:   observedEventType("project_intent"),
		ProposedType:   proposedEventType("project_intent"),
		ResourceKind:   "project_intent",
		ItemsField:     "items",
		Fields:         projectIntentFields(),
		Render:         bulletListRender("# Project Intent", "statement"),
		DefaultEnabled: true,
		Risk:           "mid",
		Sync:           SyncOptions{Importable: true, Merge: "item-dedup"},
		SyncSet:        true,
		Source:         "standard event package",
	})
}

func StandardTeamworkSignal() EventPackage {
	return mustStandardEventPackage(eventPackageDefinition{
		Name:           "teamwork_signal",
		ObservedType:   observedEventType("teamwork_signal"),
		ProposedType:   proposedEventType("teamwork_signal"),
		ResourceKind:   "teamwork_signal",
		ItemsField:     "items",
		Fields:         teamworkSignalFields(),
		Render:         bulletListRender("# Teamwork Signals", "statement"),
		DefaultEnabled: true,
		Risk:           "mid",
		Sync:           SyncOptions{Importable: true, Merge: "item-dedup"},
		SyncSet:        true,
		Source:         "standard event package",
	})
}

func StandardAssignment() EventPackage {
	return mustStandardEventPackage(eventPackageDefinition{
		Name:           "assignment",
		ObservedType:   observedEventType("assignment"),
		ProposedType:   proposedEventType("assignment"),
		ResourceKind:   "assignment",
		ItemsField:     "items",
		Fields:         assignmentFields(),
		Render:         bulletListRender("# Assignments", "scope"),
		DefaultEnabled: true,
		Risk:           "mid",
		Sync:           SyncOptions{Importable: true, Merge: "item-dedup"},
		SyncSet:        true,
		Source:         "standard event package",
	})
}

func StandardProgressDigest() EventPackage {
	return mustStandardEventPackage(eventPackageDefinition{
		Name:           "progress_digest",
		ObservedType:   observedEventType("progress_digest"),
		ProposedType:   proposedEventType("progress_digest"),
		ResourceKind:   "progress_digest",
		ItemsField:     "items",
		Fields:         progressDigestFields(),
		Render:         bulletListRender("# Progress", "summary"),
		DefaultEnabled: true,
		Risk:           "low",
		Sync:           SyncOptions{Importable: true, Merge: "item-dedup"},
		SyncSet:        true,
		Source:         "standard event package",
	})
}

func mustStandardEventPackage(def eventPackageDefinition) EventPackage {
	pkg, err := compileEventPackage(def)
	if err != nil {
		panic(fmt.Sprintf("%s %q must compile: %v", def.Source, def.Name, err))
	}
	return pkg
}

func bulletListRender(title, field string) RenderSpec {
	return RenderSpec{Content: &ContentRender{Member: "bullet-list", Params: map[string]string{
		"title": title,
		"field": field,
	}}}
}

func agentProfileFields() []FieldSpec {
	return []FieldSpec{
		ruleField("actor", required("missing")),
		ruleField("availability", required("missing"), enum("available|busy|blocked|unknown", "invalid availability")),
		ruleField("freshness", enum("|fresh|stale", "invalid freshness")),
		ruleField("ttl", required("missing")),
		narrativeField("focus", required("empty"), unsafeText()),
		narrativeField("context_advantages", listStringsRequired()),
		narrativeField("constraints", listStrings()),
		narrativeField("summary", required("empty"), unsafeText()),
		refsField("active_scopes", listStrings()),
		refsField("recent_evidence", listStrings()),
	}
}

func projectIntentFields() []FieldSpec {
	return []FieldSpec{
		ruleField("intent_id", unsafeText()),
		ruleField("scope", unsafeText()),
		ruleField("ttl"),
		narrativeField("statement", required("empty"), unsafeText()),
		narrativeField("evidence_summary", unsafeText()),
		refsField("evidence_refs", listStrings()),
		refsField("context_refs", listStrings()),
	}
}

func teamworkSignalFields() []FieldSpec {
	return []FieldSpec{
		ruleField("signal_id", unsafeText()),
		ruleField("scope", required("empty"), unsafeText()),
		ruleField("urgency", enum("|low|normal|high", "invalid urgency")),
		ruleField("ttl", required("missing")),
		narrativeField("statement", required("empty"), unsafeText()),
		narrativeField("why_teamwork", required("empty"), unsafeText()),
		narrativeField("needed_context", listStrings()),
		refsField("evidence_refs", listStrings()),
		refsField("context_refs", listStrings()),
	}
}

func assignmentFields() []FieldSpec {
	return []FieldSpec{
		ruleField("assignment_id", unsafeText()),
		ruleField("signal_ref", unsafeText()),
		ruleField("assignee", required("missing")),
		ruleField("scope", required("empty"), unsafeText()),
		ruleField("ttl", required("missing")),
		ruleField("report_on", listStrings()),
		narrativeField("expected_work", required("empty"), unsafeText()),
		narrativeField("expected_feedback", required("empty"), unsafeText()),
		narrativeField("rationale", unsafeText()),
		refsField("evidence_refs", listStrings()),
		refsField("context_refs", listStrings()),
	}
}

func progressDigestFields() []FieldSpec {
	return []FieldSpec{
		ruleField("assignment_ref", unsafeText()),
		ruleField("scope", unsafeText()),
		ruleField("feedback_kind", required("missing"), enum("progress|result|blocker", "invalid feedback_kind")),
		narrativeField("summary", required("empty"), unsafeText()),
		narrativeField("blocker", unsafeText()),
		narrativeField("result", unsafeText()),
		narrativeField("changed_context", listStrings()),
		narrativeField("suggested_next", unsafeText()),
		refsField("evidence_refs", listStrings()),
		refsField("artifact_refs", listStrings()),
	}
}

func ruleField(name string, validators ...ValidatorRef) FieldSpec {
	return field(FieldSectionRule, name, validators...)
}

func narrativeField(name string, validators ...ValidatorRef) FieldSpec {
	return field(FieldSectionNarrative, name, validators...)
}

func refsField(name string, validators ...ValidatorRef) FieldSpec {
	return field(FieldSectionRefs, name, validators...)
}

func field(section, name string, validators ...ValidatorRef) FieldSpec {
	return FieldSpec{Section: section, Name: name, Validators: validators}
}

func required(style string) ValidatorRef {
	return ValidatorRef{ID: "required", Params: map[string]string{"missing_style": style}}
}

func unsafeText() ValidatorRef {
	return ValidatorRef{ID: "safety:unsafe"}
}

func listStrings() ValidatorRef {
	return ValidatorRef{ID: "list:strings"}
}

func listStringsRequired() ValidatorRef {
	return ValidatorRef{ID: "list:strings-required"}
}

func enum(values, message string) ValidatorRef {
	return ValidatorRef{ID: "enum", Params: map[string]string{"values": values, "message": message}}
}

// registryClaims enforces uniqueness on the event-family axes every registry path must hold: no two
// packages may claim the same name, observed type, or proposed type. ResolveRegistry adds the
// resource-kind axis when external packages are merged.
type registryClaims struct {
	names    map[string]bool
	observed map[string]string
	proposed map[string]string
}

func newRegistryClaims() *registryClaims {
	return &registryClaims{names: map[string]bool{}, observed: map[string]string{}, proposed: map[string]string{}}
}

func (r *registryClaims) claim(source string, c EventPackage) error {
	if r.names[c.Name] {
		return fmt.Errorf("%s: duplicate event package name %q", source, c.Name)
	}
	if prev, dup := r.observed[c.ObservedType]; dup {
		return fmt.Errorf("%s: observed type %q already claimed by %q", source, c.ObservedType, prev)
	}
	if prev, dup := r.proposed[c.ProposedType]; dup {
		return fmt.Errorf("%s: proposed type %q already claimed by %q", source, c.ProposedType, prev)
	}
	r.names[c.Name] = true
	r.observed[c.ObservedType], r.proposed[c.ProposedType] = c.Name, c.Name
	return nil
}
