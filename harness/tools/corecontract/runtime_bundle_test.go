package corecontract

import (
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
)

func TestScenarioManifestAnchorAloneCannotCloseRequirement(t *testing.T) {
	fixture := newScenarioFixture(t, "G-DOCKER", false,
		"payment-review/system/system-proof")
	fixture.seal(t)
	fixture.attachBundle(t, bundleFixture{
		runtime: "scripted", runID: "hermetic", image: fixtureImageDigest,
	})
	fixture.addStep(t, "docker", "{}\n")

	closure, err := fixture.evaluate()
	if err != nil {
		t.Fatal(err)
	}
	result := closure.Requirements[0]
	if result.Status != RequirementPending ||
		!strings.Contains(result.Reason, "assertion is absent") {
		t.Fatalf("scenario declaration-only result = %+v", result)
	}
}

func TestRuntimeBundleRejectsAdditionalScenario(t *testing.T) {
	fixture := newScenarioFixture(t, "G-DOCKER", false,
		"payment-review/system/system-proof")
	fixture.seal(t)
	fixture.attachBundle(t, bundleFixture{
		runtime: "scripted", runID: "hermetic", image: fixtureImageDigest,
		assertion: true, additionalScenario: true,
	})
	fixture.addStep(t, "docker", "{}\n")
	if _, err := fixture.evaluate(); err == nil ||
		!strings.Contains(err.Error(), "identity or verdict") {
		t.Fatalf("additional scenario error = %v", err)
	}
}

func TestRuntimeBundleRejectsUnknownAndTrailingJSON(t *testing.T) {
	for _, test := range []struct {
		name, shadow, trailing string
	}{
		{name: "suite-unknown", shadow: "suite"},
		{name: "manifest-unknown", shadow: "manifest"},
		{name: "case-unknown", shadow: "case"},
		{name: "suite-trailing", trailing: "suite"},
		{name: "manifest-trailing", trailing: "manifest"},
		{name: "case-trailing", trailing: "case"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newScenarioFixture(t, "G-DOCKER", false,
				"payment-review/system/system-proof")
			fixture.seal(t)
			fixture.attachBundle(t, bundleFixture{
				runtime: "scripted", runID: "hermetic", image: fixtureImageDigest,
				assertion: true, shadowLayer: test.shadow, trailingLayer: test.trailing,
			})
			fixture.addStep(t, "docker", "{}\n")
			if _, err := fixture.evaluate(); err == nil ||
				(!strings.Contains(err.Error(), "unknown field") &&
					!strings.Contains(err.Error(), "trailing data")) {
				t.Fatalf("strict runtime JSON error = %v", err)
			}
		})
	}
}

func TestFaultClosureRequiresPublicOrderedSequence(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*faultFixture)
		wantErr string
		want    string
	}{
		{"missing-precondition", func(f *faultFixture) { f.omitPre = true }, "", RequirementPending},
		{"missing-action", func(f *faultFixture) { f.omitAction = true }, "", RequirementPending},
		{"missing-postcondition", func(f *faultFixture) { f.omitPost = true }, "", RequirementPending},
		{"false-precondition", func(f *faultFixture) { f.prePassed = false }, "", RequirementPending},
		{"false-action", func(f *faultFixture) { f.actionApplied = false }, "", RequirementPending},
		{"false-postcondition", func(f *faultFixture) { f.postPassed = false }, "", RequirementPending},
		{"missing-observation-receipt", func(f *faultFixture) {
			f.preReceipt.path = ""
		}, "", RequirementPending},
		{"wrong-observation-id", func(f *faultFixture) {
			f.preReceipt.faultID = "another-fault"
		}, "", RequirementPending},
		{"wrong-observation-phase", func(f *faultFixture) {
			f.preReceipt.phase = "postcondition"
		}, "", RequirementPending},
		{"wrong-observation-time", func(f *faultFixture) {
			f.preReceipt.at = fixturePostAt
		}, "", RequirementPending},
		{"observation-receipt-shadow", func(f *faultFixture) {
			f.preReceipt.shadow = true
		}, "", RequirementPending},
		{"observation-receipt-trailing", func(f *faultFixture) {
			f.preReceipt.trailing = true
		}, "", RequirementPending},
		{"same-observation-receipt", func(f *faultFixture) {
			f.postReceipt.path = f.preReceipt.path
		}, "", RequirementPending},
		{"missing-receipt", func(f *faultFixture) { f.receipt = "" }, "", RequirementPending},
		{"invalid-receipt", func(f *faultFixture) { f.receiptApplied = false }, "", RequirementPending},
		{"wrong-receipt-id", func(f *faultFixture) {
			f.receiptFaultID = "another-fault"
		}, "", RequirementPending},
		{"wrong-receipt-time", func(f *faultFixture) {
			f.receiptAt = fixturePreAt
		}, "", RequirementPending},
		{"empty-receipt-action", func(f *faultFixture) {
			f.receiptAction = ""
		}, "", RequirementPending},
		{"action-receipt-shadow", func(f *faultFixture) {
			f.receiptShadow = true
		}, "", RequirementPending},
		{"action-receipt-trailing", func(f *faultFixture) {
			f.receiptTrailing = true
		}, "", RequirementPending},
		{"out-of-order", func(f *faultFixture) { f.actionAt = f.preAt }, "", RequirementPending},
		{"legacy-shadow-fields", func(f *faultFixture) { f.legacy = true }, "unknown field", ""},
		{"complete", func(*faultFixture) {}, "", RequirementVerified},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			fault := completeFaultFixture()
			test.mutate(&fault)
			fixture := newScenarioFixture(t, "G-DOCKER", false,
				"payment-review/fault/review-receipt-loss")
			fixture.seal(t)
			fixture.attachBundle(t, bundleFixture{
				runtime: "scripted", runID: "hermetic", image: fixtureImageDigest,
				assertion: true, fault: &fault,
			})
			fixture.addStep(t, "docker", "{}\n")
			closure, err := fixture.evaluate()
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("fault error = %v, want %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := closure.Requirements[0].Status; got != test.want {
				t.Fatalf("fault status = %s, want %s; result=%+v",
					got, test.want, closure.Requirements[0])
			}
		})
	}
}

func TestLiveClosureRequiresExactHermeticPair(t *testing.T) {
	cases := []struct {
		name      string
		pair      string
		liveImage string
		wantError bool
	}{
		{"wrong-run", "another-run", fixtureImageDigest, true},
		{"wrong-image", "hermetic", "sha256:" + strings.Repeat("2", 64), true},
		{"exact", "hermetic", fixtureImageDigest, false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			fixture := newScenarioFixture(t, "G-LIVE", true,
				"payment-review/system/system-proof")
			fixture.seal(t)
			fixture.attachBundle(t, bundleFixture{
				runtime: "scripted", runID: "hermetic", image: fixtureImageDigest,
				assertion: true,
			})
			fixture.attachBundle(t, bundleFixture{
				runtime: "codex", runID: "live", paired: test.pair,
				image: test.liveImage, assertion: true,
			})
			fixture.addStep(t, "live", "{}\n")
			fixture.addStep(t, "evidence-live", "{}\n")
			closure, err := fixture.evaluate()
			if test.wantError {
				if err == nil || !strings.Contains(err.Error(), "exact Hermetic") {
					t.Fatalf("pair error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if closure.Requirements[0].Status != RequirementVerified {
				t.Fatalf("exact pair result = %+v", closure.Requirements[0])
			}
		})
	}
}

const fixtureImageDigest = "sha256:1111111111111111111111111111111111111111111111111111111111111111"

const (
	fixturePreAt       = "2026-07-27T10:01:00Z"
	fixtureActionAt    = "2026-07-27T10:02:00Z"
	fixturePostAt      = "2026-07-27T10:03:00Z"
	fixtureReceipt     = "faults/review-receipt-loss-action.json"
	fixturePreReceipt  = "faults/review-receipt-loss-precondition.json"
	fixturePostReceipt = "faults/review-receipt-loss-postcondition.json"
)

type bundleFixture struct {
	runtime, runID, paired, image string
	assertion                     bool
	additionalScenario            bool
	shadowLayer, trailingLayer    string
	fault                         *faultFixture
}

type faultFixture struct {
	omitPre, omitAction, omitPost            bool
	prePassed, actionApplied, postPassed     bool
	preAt, actionAt, postAt, receipt         string
	receiptApplied, legacy                   bool
	receiptFaultID, receiptAction, receiptAt string
	receiptShadow, receiptTrailing           bool
	preReceipt, postReceipt                  observationFixture
}

type observationFixture struct {
	path, faultID, phase, at string
	public, shadow, trailing bool
}

func completeFaultFixture() faultFixture {
	return faultFixture{
		prePassed: true, actionApplied: true, postPassed: true,
		preAt: fixturePreAt, actionAt: fixtureActionAt, postAt: fixturePostAt,
		receipt: fixtureReceipt, receiptApplied: true,
		receiptFaultID: "review-receipt-loss", receiptAction: "drop-review-receipt",
		receiptAt: fixtureActionAt,
		preReceipt: observationFixture{
			path: fixturePreReceipt, faultID: "review-receipt-loss",
			phase: "precondition", at: fixturePreAt, public: true,
		},
		postReceipt: observationFixture{
			path: fixturePostReceipt, faultID: "review-receipt-loss",
			phase: "postcondition", at: fixturePostAt, public: true,
		},
	}
}

func newScenarioFixture(t *testing.T, gate string, live bool,
	key string,
) *closureFixture {
	t.Helper()
	fixture := newClosureFixture(t, gate)
	writeTestFile(t, fixture.root,
		"harness/test/e2e/scenarios/payment-review/manifest.json", `{
  "schema_version": 1,
  "name": "payment-review",
  "faults": [{"id": "review-receipt-loss"}],
  "oracles": {
    "system": ["system-proof"],
    "task": [{"id": "task-proof"}],
    "experience": ["experience-proof"]
  }
}`)
	if live {
		fixture.registry.Requirements[0].LiveScenarioKeys = []string{key}
	} else {
		fixture.registry.Requirements[0].ScenarioKeys = []string{key}
	}
	return fixture
}

func (fixture *closureFixture) attachBundle(t *testing.T, spec bundleFixture) {
	t.Helper()
	base := ".testdata/r5/" + spec.runID
	casePath := "cases/" + canonicalScenarioName
	proofRef := "evidence/proof.json"
	writeJSONTestFile(t, fixture.root, base+"/"+casePath+"/"+proofRef,
		map[string]any{"public_observation": true})
	writeFaultReceipts(t, fixture.root, base, casePath, spec.fault)
	fixture.writeBundleReports(t, base, casePath, proofRef, spec)
	fixture.writeBundleManifest(t, base, casePath, proofRef, spec)
	fixture.report.Bundles = append(fixture.report.Bundles, GateBundleRef{
		Runtime: spec.runtime, RunID: spec.runID,
		ReportPath: base + "/report.json", ReportSHA256: mustFileDigest(
			t, fixture.root, base+"/report.json"),
		ManifestPath: base + "/manifest.json", ManifestSHA256: mustFileDigest(
			t, fixture.root, base+"/manifest.json"),
	})
}

func (fixture *closureFixture) writeBundleReports(t *testing.T, base, casePath,
	proofRef string, spec bundleFixture,
) {
	t.Helper()
	assertions := []map[string]any{}
	if spec.assertion {
		id := "system-proof"
		if spec.fault != nil {
			id = "fault-review-receipt-loss"
		}
		assertions = append(assertions, map[string]any{
			"id": id, "category": "system", "required": true, "passed": true,
			"evidence": []string{proofRef},
		})
	}
	faults := []map[string]any{}
	if spec.fault != nil {
		faults = append(faults, faultDocument(*spec.fault, proofRef))
	}
	caseReport := map[string]any{
		"schema_version": 1, "run_id": spec.runID + "-" + canonicalScenarioName,
		"scenario": canonicalScenarioName, "runtime": spec.runtime, "status": "passed",
		"git_sha": fixture.report.Source.Commit, "image_digest": spec.image,
		"assertions": assertions, "faults": faults,
		"oracle": map[string]any{"task": map[string]any{"results": []any{}}},
	}
	addShadowField(caseReport, spec.shadowLayer == "case")
	caseReportPath := base + "/" + casePath + "/report.json"
	writeJSONTestFile(t, fixture.root, caseReportPath, caseReport)
	addTrailingObject(t, fixture.root, caseReportPath, spec.trailingLayer == "case")

	names := []string{canonicalScenarioName}
	cases := []map[string]any{suiteCase(spec.runID, canonicalScenarioName)}
	if spec.additionalScenario {
		names = append(names, "api-sdk-contract")
		cases = append(cases, suiteCase(spec.runID, "api-sdk-contract"))
	}
	var paired any
	if spec.runtime == "codex" {
		paired = spec.paired
	}
	suiteReport := map[string]any{
		"schema_version": 1, "run_id": spec.runID, "runtime": spec.runtime,
		"status": "passed", "git_sha": fixture.report.Source.Commit,
		"image": map[string]any{
			"digest": spec.image, "revision": fixture.report.Source.Commit,
			"source_tree": fixture.report.Source.Tree,
		},
		"case_names": names, "cases": cases, "paired_hermetic_run": paired,
	}
	addShadowField(suiteReport, spec.shadowLayer == "suite")
	suiteReportPath := base + "/report.json"
	writeJSONTestFile(t, fixture.root, suiteReportPath, suiteReport)
	addTrailingObject(t, fixture.root, suiteReportPath, spec.trailingLayer == "suite")
}

func (fixture *closureFixture) writeBundleManifest(t *testing.T, base, casePath,
	proofRef string, spec bundleFixture,
) {
	t.Helper()
	paths := []string{casePath + "/" + proofRef, casePath + "/report.json", "report.json"}
	if spec.fault != nil {
		for _, relative := range []string{
			spec.fault.preReceipt.path, spec.fault.postReceipt.path, spec.fault.receipt,
		} {
			path := casePath + "/" + relative
			if relative != "" && !slices.Contains(paths, path) {
				paths = append(paths, path)
			}
		}
	}
	slices.Sort(paths)
	files := make([]map[string]any, 0, len(paths))
	for _, relative := range paths {
		data, err := readRuntimeFile(fixture.root, base+"/"+relative,
			mustFileDigest(t, fixture.root, base+"/"+relative))
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, map[string]any{
			"path": relative, "sha256": bytesDigest(data), "bytes": len(data),
		})
	}
	manifest := map[string]any{
		"schema_version": 1, "run_id": spec.runID, "files": files,
	}
	addShadowField(manifest, spec.shadowLayer == "manifest")
	manifestPath := base + "/manifest.json"
	writeJSONTestFile(t, fixture.root, manifestPath, manifest)
	addTrailingObject(t, fixture.root, manifestPath, spec.trailingLayer == "manifest")
}

func writeFaultReceipts(t *testing.T, root, base, casePath string, fault *faultFixture) {
	t.Helper()
	if fault == nil {
		return
	}
	written := []string{}
	for _, receipt := range []observationFixture{fault.preReceipt, fault.postReceipt} {
		if receipt.path == "" || slices.Contains(written, receipt.path) {
			continue
		}
		document := map[string]any{
			"fault_id": receipt.faultID, "phase": receipt.phase,
			"at": receipt.at, "public_observation": receipt.public,
		}
		addShadowField(document, receipt.shadow)
		path := base + "/" + casePath + "/" + receipt.path
		writeJSONTestFile(t, root, path, document)
		addTrailingObject(t, root, path, receipt.trailing)
		written = append(written, receipt.path)
	}
	if fault.receipt == "" {
		return
	}
	document := map[string]any{
		"external_action_applied": fault.receiptApplied,
		"fault_id":                fault.receiptFaultID,
		"external_action":         fault.receiptAction,
		"at":                      fault.receiptAt,
	}
	addShadowField(document, fault.receiptShadow)
	path := base + "/" + casePath + "/" + fault.receipt
	writeJSONTestFile(t, root, path, document)
	addTrailingObject(t, root, path, fault.receiptTrailing)
}

func addShadowField(document map[string]any, enabled bool) {
	if enabled {
		document["legacy_shadow"] = true
	}
}

func addTrailingObject(t *testing.T, root, relative string, enabled bool) {
	t.Helper()
	if !enabled {
		return
	}
	data, err := os.ReadFile(root + "/" + relative)
	if err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, root, relative, string(data)+"{}\n")
}

func faultDocument(fault faultFixture, proofRef string) map[string]any {
	if fault.legacy {
		return map[string]any{
			"id": "review-receipt-loss", "injected": true,
			"observation_passed": true, "evidence_refs": []string{proofRef},
		}
	}
	document := map[string]any{"id": "review-receipt-loss"}
	if !fault.omitPre {
		refs := []string{}
		if fault.preReceipt.path != "" {
			refs = append(refs, fault.preReceipt.path)
		}
		document["public_precondition"] = map[string]any{
			"passed": fault.prePassed, "at": fault.preAt,
			"observation_receipt": fault.preReceipt.path, "evidence_refs": refs,
		}
	}
	if !fault.omitAction {
		refs := []string{}
		if fault.receipt != "" {
			refs = append(refs, fault.receipt)
		}
		document["external_action"] = map[string]any{
			"applied": fault.actionApplied, "at": fault.actionAt,
			"action_receipt": fault.receipt, "evidence_refs": refs,
		}
	}
	if !fault.omitPost {
		refs := []string{}
		if fault.postReceipt.path != "" {
			refs = append(refs, fault.postReceipt.path)
		}
		document["public_postcondition"] = map[string]any{
			"passed": fault.postPassed, "at": fault.postAt,
			"observation_receipt": fault.postReceipt.path, "evidence_refs": refs,
		}
	}
	return document
}

func suiteCase(runID, name string) map[string]any {
	return map[string]any{
		"name": name, "path": "cases/" + name, "run_id": runID + "-" + name,
		"status": "passed", "exit_code": 0, "task_oracles_passed": true,
		"system_invariants_passed": true,
	}
}

func mustFileDigest(t *testing.T, root, relative string) string {
	t.Helper()
	digest, err := fileDigest(root + "/" + relative)
	if err != nil {
		t.Fatal(fmt.Errorf("digest fixture file %s: %w", relative, err))
	}
	return digest
}
