package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/policy"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/state"
)

const widgetPackageSpec = `{"schema_version":1,"name":"widget","observed_type":"widget.write_candidate.observed",
"proposed_type":"widget.write.proposed","resource_kind":"widget","items_field":"items",
"fields":[{"name":"text","validators":[{"id":"required","params":{"missing_style":"empty"}}]}],
"render":{"content":{"member":"bullet-list","params":{"title":"# Widgets","field":"text"}}}}`

// loop add places a package under its canonical name and validates it through the boot resolution;
// the registered package then resolves in the project catalog.
func TestLoopAddRegistersAndValidates(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src", "widget")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "capability.json"), []byte(widgetPackageSpec), 0o644); err != nil {
		t.Fatal(err)
	}

	name, err := New(root).LoopAdd(src)
	if err != nil {
		t.Fatalf("loop add: %v", err)
	}
	if name != "widget" {
		t.Fatalf("registered name = %q, want widget", name)
	}
	if _, err := os.Stat(filepath.Join(root, ".mnemon", "loops", "widget", "capability.json")); err != nil {
		t.Fatalf("package not placed under .mnemon/loops/widget: %v", err)
	}
	catalog, err := policy.ResolveRegistry(root, state.DefaultSchemaGuard().Required)
	if err != nil {
		t.Fatalf("resolve after add: %v", err)
	}
	if _, ok := catalog["widget"]; !ok {
		t.Fatalf("added loop must resolve in the catalog: %v", catalog)
	}
}

// A package that would refuse boot is rejected AND rolled back — no half-added directory lingers.
func TestLoopAddRejectsAndRollsBack(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src", "broken")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	// resource_kind "assignment" is an embedded kind an external package may not claim (shadowing) —
	// ResolveRegistry refuses it, so loop add must too.
	bad := `{"schema_version":1,"name":"broken","observed_type":"broken.write_candidate.observed",
"proposed_type":"broken.write.proposed","resource_kind":"assignment","items_field":"items",
"fields":[{"name":"text","validators":[{"id":"required","params":{"missing_style":"empty"}}]}],
"render":{"content":{"member":"bullet-list","params":{"title":"# B","field":"text"}}}}`
	if err := os.WriteFile(filepath.Join(src, "capability.json"), []byte(bad), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := New(root).LoopAdd(src); err == nil {
		t.Fatal("loop add must reject a package that fails boot resolution")
	}
	if _, err := os.Stat(filepath.Join(root, ".mnemon", "loops", "broken")); !os.IsNotExist(err) {
		t.Fatalf("a rejected package must be rolled back, but .mnemon/loops/broken survives (err=%v)", err)
	}
}

// An existing target is not overwritten — the user removes it first to replace.
func TestLoopAddRefusesExistingTarget(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src", "widget")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "capability.json"), []byte(widgetPackageSpec), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := New(root).LoopAdd(src); err != nil {
		t.Fatalf("first add: %v", err)
	}
	if _, err := New(root).LoopAdd(src); err == nil {
		t.Fatal("a second add of an existing target must refuse, not overwrite")
	}
}

// loop packages resolves standard + external kinds; loop schema returns one kind and errors on
// an unknown one.
func TestLoopEventPackagesAndSchema(t *testing.T) {
	root := t.TempDir()
	writeExternalGoalPackage(t, root, "widget", widgetPackageSpec)

	infos, err := New(root).LoopEventPackages()
	if err != nil {
		t.Fatalf("loop packages: %v", err)
	}
	byKind := map[string]EventPackageInfo{}
	for _, info := range infos {
		byKind[info.Kind] = info
	}
	if byKind["assignment"].Source != "standard" || !byKind["assignment"].Importable || byKind["assignment"].Merge != "item-dedup" {
		t.Fatalf("assignment must be standard + importable item-dedup: %+v", byKind["assignment"])
	}
	if w, ok := byKind["widget"]; !ok || w.Source != "external" || w.ObservedType != "widget.write_candidate.observed" {
		t.Fatalf("external widget must appear with its descriptor: %+v", w)
	}

	info, err := New(root).LoopSchema("assignment")
	if err != nil || info.Merge != "item-dedup" {
		t.Fatalf("loop schema assignment: info=%+v err=%v", info, err)
	}
	if _, err := New(root).LoopSchema("nope"); err == nil {
		t.Fatal("loop schema must error on an unknown kind, not return an empty success")
	}
}

// The generic observe skill renders its mechanism from the live catalog (every enabled kind's
// observe event type) and carries the hand-written judgment + discovery pointers.
func TestRenderObserveSkill(t *testing.T) {
	root := t.TempDir()
	writeExternalGoalPackage(t, root, "widget", widgetPackageSpec)

	skill, err := New(root).RenderObserveSkill()
	if err != nil {
		t.Fatalf("render observe skill: %v", err)
	}
	for _, want := range []string{
		"# mnemon-observe",
		"When to record",                      // judgment (hand-written)
		"assignment.write_candidate.observed", // embedded mechanism (catalog-rendered)
		"widget.write_candidate.observed",     // external mechanism (catalog-rendered)
		"mnemon-harness loop schema --type",   // discovery pointer, not hardcoded fields
		"mnemon-harness control observe",      // submit shape
	} {
		if !strings.Contains(skill, want) {
			t.Fatalf("observe skill missing %q:\n%s", want, skill)
		}
	}
}
