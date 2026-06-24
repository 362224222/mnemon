package assembler

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/channel"
	"github.com/mnemon-dev/mnemon/harness/internal/config"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/kernel"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/policy"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

// fixtureCatalog is EmbeddedCatalog() plus the DEMOTED note/decision policies, compiled from their
// canonical fixture specs (mnemond/policy/testdata/capabilities/*.json — formerly embedded, now
// supplied the way an external package would supply them). Mirrors the shape the boot path gets
// from policy.ResolveCatalog when the operator lays the packages under .mnemon/loops.
func fixtureCatalog(t *testing.T, names ...string) map[string]policy.Capability {
	t.Helper()
	catalog := map[string]policy.Capability{}
	for id, c := range policy.EmbeddedCatalog() {
		catalog[id] = c
	}
	fixtures := os.DirFS(filepath.Join("..", "mnemond", "policy", "testdata"))
	for _, name := range names {
		spec, err := policy.LoadSpec(fixtures, name)
		if err != nil {
			t.Fatalf("load fixture spec %s: %v", name, err)
		}
		cap, err := policy.FromSpec(spec)
		if err != nil {
			t.Fatalf("compile fixture spec %s: %v", name, err)
		}
		catalog[cap.Name] = cap
	}
	return catalog
}

// A 3rd capability (note) stands up end-to-end through config + the generic kind alone — no new rule
// code: Assemble selects the note rule from the provided catalog (note is a fixture/external-package
// capability since the P1 demotion, not a builtin) and admits a note candidate through the
// channel -> tick -> kernel -> eventview.
func TestAssembleAdmitsConfiguredNoteCapabilityEndToEnd(t *testing.T) {
	ref := contract.ResourceRef{Kind: "note", ID: "project"}
	binding := channel.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	binding.AllowedObservedTypes = []string{"note.write_candidate.observed"}

	cfg := config.File{Capabilities: map[string]config.CapabilityConfig{
		"note": {Enabled: true, ResourceRef: "note/project", RuleRef: "native:note"},
	}}
	rc, err := Assemble(cfg, []channel.ChannelBinding{binding}, fixtureCatalog(t, "note"))
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}

	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "g.db"), rc)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()

	if _, _, err := rt.API().Ingest("codex@project", contract.ObservationEnvelope{
		ExternalID: "n1",
		Event:      contract.Event{Type: "note.write_candidate.observed", Payload: map[string]any{"text": "remember the assembler"}},
	}); err != nil {
		t.Fatalf("ingest note: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	v, fields, err := rt.Resource(ref)
	if err != nil {
		t.Fatalf("read note: %v", err)
	}
	if v == 0 {
		t.Fatal("the configured note capability must admit a candidate (resource not created)")
	}
	if content, _ := fields["content"].(string); !strings.Contains(content, "remember the assembler") {
		t.Fatalf("note content missing the candidate: %q", content)
	}
}

// PD2 declared kinds: a capability whose resource kind is NOT in the compiled
// kernel.DefaultSchemaGuard (a genuinely declared user kind) boots end-to-end — Assemble registers
// its required header in the RuntimeConfig.SchemaGuard, and the live kernel admits its candidate.
// This is the assembly-time declared kind set: the live known-kind set is governance ∪ enabled caps.
func TestAssembleRegistersDeclaredKindNotInDefaultGuard(t *testing.T) {
	if _, compiled := kernel.DefaultSchemaGuard().Required["widget"]; compiled {
		t.Fatal("precondition: widget must NOT be a compiled kind for this test to prove declared-kind registration")
	}
	widgetSpec := policy.CapabilitySpec{
		SchemaVersion: 1, Name: "widget",
		ObservedType: "widget.write_candidate.observed", ProposedType: "widget.write.proposed",
		ResourceKind: "widget", ItemsField: "items",
		Fields: []policy.FieldSpec{{Name: "text", Validators: []policy.ValidatorRef{
			{ID: "required", Params: map[string]string{"missing_style": "empty"}},
		}}},
		Render: policy.RenderSpec{Content: &policy.ContentRender{
			Member: "bullet-list", Params: map[string]string{"title": "# Widgets", "field": "text"}}},
	}
	widgetCap, err := policy.FromSpec(widgetSpec)
	if err != nil {
		t.Fatalf("a declared (non-reserved) kind must compile: %v", err)
	}
	ref := contract.ResourceRef{Kind: "widget", ID: "project"}
	binding := channel.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	binding.AllowedObservedTypes = []string{"widget.write_candidate.observed"}
	cfg := config.File{Capabilities: map[string]config.CapabilityConfig{
		"widget": {Enabled: true, ResourceRef: "widget/project", RuleRef: "native:widget"},
	}}
	rc, err := Assemble(cfg, []channel.ChannelBinding{binding}, map[string]policy.Capability{"widget": widgetCap})
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if _, known := rc.SchemaGuard.Required["widget"]; !known {
		t.Fatal("Assemble must register the declared kind's schema guard entry from the capability")
	}
	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "g.db"), rc)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()
	if _, _, err := rt.API().Ingest("codex@project", contract.ObservationEnvelope{
		ExternalID: "w1",
		Event:      contract.Event{Type: "widget.write_candidate.observed", Payload: map[string]any{"text": "a declared kind"}},
	}); err != nil {
		t.Fatalf("ingest widget: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if v, _, err := rt.Resource(ref); err != nil || v == 0 {
		t.Fatalf("the live kernel must admit the declared kind (v=%d err=%v)", v, err)
	}
}

// Stage-5: Assemble selects from the PROVIDED catalog — a capability that exists only in an
// external package (goal) resolves when the resolved catalog is passed, and fails closed when the
// caller passes nil (nil = policy.EmbeddedCatalog(), the backward-compatible seam).
func TestAssembleResolvesFromProvidedCatalog(t *testing.T) {
	goalSpec := policy.CapabilitySpec{
		SchemaVersion: 1, Name: "goal",
		ObservedType: "goal.write_candidate.observed", ProposedType: "goal.write.proposed",
		ResourceKind: "goal", ItemsField: "items",
		Fields: []policy.FieldSpec{{Name: "statement", Validators: []policy.ValidatorRef{
			{ID: "required", Params: map[string]string{"missing_style": "empty"}},
		}}},
		Render: policy.RenderSpec{
			Content: &policy.ContentRender{Member: "bullet-list", Params: map[string]string{"title": "# Goals", "field": "statement"}},
			Static:  map[string]string{"statement": "project"},
		},
	}
	goalCap, err := policy.FromSpec(goalSpec)
	if err != nil {
		t.Fatalf("compile goal spec: %v", err)
	}
	catalog := map[string]policy.Capability{"goal": goalCap}
	for id, c := range policy.EmbeddedCatalog() {
		catalog[id] = c
	}

	ref := contract.ResourceRef{Kind: "goal", ID: "project"}
	binding := channel.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	binding.AllowedObservedTypes = []string{"goal.write_candidate.observed"}
	cfg := config.File{Capabilities: map[string]config.CapabilityConfig{
		"goal": {Enabled: true, ResourceRef: "goal/project", RuleRef: "native:goal"},
	}}

	if _, err := Assemble(cfg, []channel.ChannelBinding{binding}, nil); err == nil {
		t.Fatal("native:goal must fail closed against the nil (EmbeddedCatalog()) catalog")
	}
	rc, err := Assemble(cfg, []channel.ChannelBinding{binding}, catalog)
	if err != nil {
		t.Fatalf("assemble with external-merged catalog: %v", err)
	}

	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "g.db"), rc)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()
	if _, _, err := rt.API().Ingest("codex@project", contract.ObservationEnvelope{
		ExternalID: "g1",
		Event:      contract.Event{Type: "goal.write_candidate.observed", Payload: map[string]any{"statement": "ship stage five"}},
	}); err != nil {
		t.Fatalf("ingest goal: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	v, fields, err := rt.Resource(ref)
	if err != nil || v == 0 {
		t.Fatalf("the catalog-selected goal capability must admit (v=%d err=%v)", v, err)
	}
	if content, _ := fields["content"].(string); !strings.Contains(content, "ship stage five") {
		t.Fatalf("goal content missing the candidate: %q", content)
	}
}

func TestAssembleFailsClosedOnUnknownCapability(t *testing.T) {
	cfg := config.File{Capabilities: map[string]config.CapabilityConfig{
		"bogus": {Enabled: true, ResourceRef: "bogus/project", RuleRef: "native:bogus"},
	}}
	if _, err := Assemble(cfg, nil, nil); err == nil {
		t.Fatal("an unknown capability rule_ref must fail closed")
	}
}

// The P1 demotion nail: config enables note but NO external package supplies its spec (nil
// catalog = EmbeddedCatalog(), which is exactly {memory, skill} now) — Assemble must land on the
// 'unknown rule_ref' fail-closed path, never a silent no-op or a builtin fallback.
func TestAssembleFailsClosedOnNoteWithoutExternalPackage(t *testing.T) {
	cfg := config.File{Capabilities: map[string]config.CapabilityConfig{
		"note": {Enabled: true, ResourceRef: "note/project", RuleRef: "native:note"},
	}}
	_, err := Assemble(cfg, nil, nil)
	if err == nil {
		t.Fatal("native:note without an external package must fail closed against the EmbeddedCatalog() catalog")
	}
	if !strings.Contains(err.Error(), `unknown rule_ref "native:note"`) {
		t.Fatalf("want the 'unknown rule_ref' fail-closed diagnostic, got %v", err)
	}
}

// A binding scoped to a non-default ref of the capability's kind must get a rule targeting ITS ref
// (parity with the production memoryRefForBinding fallback), not the config-pinned default.
func TestAssembleDerivesRefFromBindingScope(t *testing.T) {
	teamRef := contract.ResourceRef{Kind: "memory", ID: "team"}
	binding := channel.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{teamRef})
	binding.AllowedObservedTypes = []string{"memory.write_candidate.observed"}

	cfg := config.File{Capabilities: map[string]config.CapabilityConfig{
		"memory": {Enabled: true, ResourceRef: "memory/project", RuleRef: "native:memory"},
	}}
	rc, err := Assemble(cfg, []channel.ChannelBinding{binding}, nil)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}

	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "g.db"), rc)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()

	if _, _, err := rt.API().Ingest("codex@project", contract.ObservationEnvelope{
		ExternalID: "m1",
		Event:      contract.Event{Type: "memory.write_candidate.observed", Payload: map[string]any{"content": "team fact", "source": "s", "confidence": "high"}},
	}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if v, _, err := rt.Resource(teamRef); err != nil || v == 0 {
		t.Fatalf("write must land on the binding's scoped ref memory/team (v=%d err=%v)", v, err)
	}
	if v, _, _ := rt.Resource(contract.ResourceRef{Kind: "memory", ID: "project"}); v != 0 {
		t.Fatal("the config default memory/project must NOT be written for a team-scoped binding")
	}
}

// A host-agent binding with observe + observed-type but EMPTY SubscriptionScope must produce no rule
// and no kernel authority (parity with the app builders' skip; an unscoped binding could never pull
// what it writes).
func TestAssembleSkipsUnscopedBinding(t *testing.T) {
	binding := channel.HostAgentBinding("codex@project", "http://127.0.0.1:8787", nil)
	binding.AllowedObservedTypes = []string{"memory.write_candidate.observed"}

	cfg := config.File{Capabilities: map[string]config.CapabilityConfig{
		"memory": {Enabled: true, ResourceRef: "memory/project", RuleRef: "native:memory"},
	}}
	rc, err := Assemble(cfg, []channel.ChannelBinding{binding}, nil)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if got := len(rc.Authority.Allow["codex@project"]); got != 0 {
		t.Fatalf("unscoped binding must get no kernel authority, got %d kinds", got)
	}

	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "g.db"), rc)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()
	if _, _, err := rt.API().Ingest("codex@project", contract.ObservationEnvelope{
		ExternalID: "m1",
		Event:      contract.Event{Type: "memory.write_candidate.observed", Payload: map[string]any{"content": "x", "source": "s", "confidence": "high"}},
	}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if v, _, _ := rt.Resource(contract.ResourceRef{Kind: "memory", ID: "project"}); v != 0 {
		t.Fatal("an unscoped binding must not produce a write")
	}
}

// rule_ref 必须携带命名空间前缀:裸 id(如 "memory")在 Assemble 这道生产 seam
// 上 fail-closed —— 为未来的 wasm: 等命名空间立规,与 config.Load 的校验双门一致。
func TestAssembleRejectsBareRuleRef(t *testing.T) {
	cfg := config.File{Capabilities: map[string]config.CapabilityConfig{
		"memory": {Enabled: true, ResourceRef: "memory/project", RuleRef: "memory"}, // 缺 native: 前缀
	}}
	if _, err := Assemble(cfg, nil, nil); err == nil {
		t.Fatal("a bare rule_ref without the native: namespace prefix must fail closed")
	}
}

// 阶段二验收(P1 降级后):第四能力 decision 的全部 Go 足迹 = KindCatalog/SchemaGuard 各一行;
// 行为完全来自 spec 文件(mnemond/policy/testdata/capabilities/decision.json,经 P1 降级为
// fixture/外部包供给——曾内嵌于 assets)。端到端与 note 同构。
func TestAssembleAdmitsDecisionCapabilityEndToEnd(t *testing.T) {
	ref := contract.ResourceRef{Kind: "decision", ID: "project"}
	binding := channel.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	binding.AllowedObservedTypes = []string{"decision.write_candidate.observed"}

	cfg := config.File{Capabilities: map[string]config.CapabilityConfig{
		"decision": {Enabled: true, ResourceRef: "decision/project", RuleRef: "native:decision"},
	}}
	rc, err := Assemble(cfg, []channel.ChannelBinding{binding}, fixtureCatalog(t, "decision"))
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "g.db"), rc)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()
	if _, _, err := rt.API().Ingest("codex@project", contract.ObservationEnvelope{
		ExternalID: "d1",
		Event:      contract.Event{Type: "decision.write_candidate.observed", Payload: map[string]any{"text": "adopt the spec catalogs"}},
	}); err != nil {
		t.Fatalf("ingest decision: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	v, fields, err := rt.Resource(ref)
	if err != nil || v == 0 {
		t.Fatalf("decision capability must admit (v=%d err=%v)", v, err)
	}
	if content, _ := fields["content"].(string); !strings.Contains(content, "adopt the spec catalogs") {
		t.Fatalf("decision content missing the candidate: %q", content)
	}
}

// Header⊇SchemaGuard 锁步:每个内置能力的渲染产物必须覆盖其 kind 的全部必填字段——
// 否则 spec 文件能声明一个 kernel 永远拒绝的能力(装配期可发现的缺陷不留到运行期)。
func TestBuiltinHeadersSatisfySchemaGuard(t *testing.T) {
	// Post-graduation, a kind's required header IS the capability's RequiredHeader (the assembler
	// registers it). Build the guard from the caps and assert each cap's rendered fields satisfy its
	// own kind's required — the render⊇required lockstep, now derived from the spec.
	extra := map[contract.ResourceKind][]string{}
	for _, cap := range policy.EmbeddedCatalog() {
		extra[cap.ResourceKind] = cap.RequiredHeader
	}
	guard := kernel.SchemaGuardWith(extra)
	for id, cap := range policy.EmbeddedCatalog() {
		item, err := cap.Decode(minimalAcceptPayload(id))
		if err != nil {
			t.Fatalf("%s: decode minimal accept: %v", id, err)
		}
		fields := map[string]any{cap.ItemsField: []policy.Item{item}, "updated_by": "x"}
		for k, v := range cap.Header([]policy.Item{item}) {
			fields[k] = v
		}
		if err := guard.Validate(cap.ResourceKind, fields); err != nil {
			t.Fatalf("%s: rendered fields must satisfy SchemaGuard: %v", id, err)
		}
	}
}

func minimalAcceptPayload(id string) map[string]any {
	switch id {
	case "memory":
		return map[string]any{"content": "x", "source": "s", "confidence": "high"}
	case "skill":
		return map[string]any{"skill_id": "x-skill", "source": "s", "confidence": "high"}
	case "project_intent":
		return map[string]any{"statement": "ship the thing"}
	case "agent_profile":
		return map[string]any{
			"actor": "codex@impl", "focus": "projection", "context_advantages": []any{"read projection code"},
			"availability": "available", "ttl": "30m", "summary": "projection context",
		}
	case "teamwork_signal":
		return map[string]any{"scope": "projection", "statement": "needs review", "why_teamwork": "another agent has context", "ttl": "2h", "evidence": "profile roster"}
	case "assignment":
		return map[string]any{
			"scope": "projection", "ttl": "2h", "assignee": "codex@impl",
			"expected_work": "review projection", "expected_feedback": "short result", "evidence": "profile roster",
		}
	case "progress_digest":
		return map[string]any{"summary": "projection 80% done"}
	default:
		return map[string]any{"text": "x"}
	}
}
