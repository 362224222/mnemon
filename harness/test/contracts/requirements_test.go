package contracts_test

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/tools/corecontract"
)

const coreReleaseClosureEnvironment = "R5_CORE_RELEASE_CLOSURE"
const coreGateReportEnvironment = "R5_CORE_GATE_REPORT"

func TestCoreRequirementsRegistry(t *testing.T) {
	t.Parallel()
	root, contract, registry := loadCoreRequirements(t)
	if err := corecontract.ValidateBindings(root, contract, registry); err != nil {
		t.Fatal(err)
	}
	wantGates := []string{
		"G-CONTRACT", "G-DOCKER", "G-EVIDENCE", "G-LIVE", "G-PROCESS", "G-ROOT", "G-UNIT",
	}
	if got := contract.GateIDs(); !slices.Equal(got, wantGates) {
		t.Fatalf("tracked Core gates = %v, want %v", got, wantGates)
	}
}

func TestCoreRequirementsReleaseClosure(t *testing.T) {
	if os.Getenv(coreReleaseClosureEnvironment) != "1" {
		t.Skip("strict Core release closure is an explicit target")
	}
	root, contract, registry := loadCoreRequirements(t)
	reportPath := os.Getenv(coreGateReportEnvironment)
	if reportPath == "" {
		t.Fatalf("%s is required for runtime release closure", coreGateReportEnvironment)
	}
	if !filepath.IsAbs(reportPath) {
		reportPath = filepath.Join(root, filepath.FromSlash(reportPath))
	}
	relative, err := filepath.Rel(root, reportPath)
	if err != nil || strings.HasPrefix(relative, "..") ||
		!strings.HasPrefix(filepath.ToSlash(relative), ".testdata/r5/") {
		t.Fatalf("%s must name an ignored .testdata/r5 report under the repository",
			coreGateReportEnvironment)
	}
	report, err := corecontract.LoadGateReport(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := coreReleaseClosureError(root, contract, registry, &report); err != nil {
		t.Fatal(err)
	}
}

func loadCoreRequirements(t *testing.T) (string, corecontract.Contract, corecontract.Registry) {
	t.Helper()
	root := repositoryRoot(t)
	contract, err := corecontract.Load(root)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := corecontract.LoadRegistry(filepath.Join(
		root, "harness", "test", "contracts", "requirements.json"))
	if err != nil {
		t.Fatal(err)
	}
	return root, contract, registry
}

func coreReleaseClosureError(root string, contract corecontract.Contract,
	registry corecontract.Registry, report *corecontract.GateReport,
) error {
	if report == nil {
		return fmt.Errorf("Core release closure requires a runtime gate report")
	}
	closure, err := corecontract.EvaluateClosure(root, contract, registry, *report)
	if err != nil {
		return fmt.Errorf("validate Core runtime evidence: %w", err)
	}
	unresolved := corecontract.UnresolvedMust(closure)
	if len(unresolved) == 0 {
		return nil
	}
	return fmt.Errorf(
		"Core release closure has %d unresolved MUST requirements (%s); open gates: %s",
		len(unresolved), strings.Join(unresolved, ","), strings.Join(
			corecontract.UnresolvedGates(closure), ","),
	)
}
