package corecontract

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

type runtimeSuiteReport struct {
	SchemaVersion int    `json:"schema_version"`
	RunID         string `json:"run_id"`
	BundleKind    string `json:"bundle_kind"`
	Runtime       string `json:"runtime"`
	Status        string `json:"status"`
	GeneratedAt   string `json:"generated_at"`
	GitSHA        string `json:"git_sha"`
	Image         struct {
		Reference      string  `json:"reference"`
		Digest         string  `json:"digest"`
		Revision       string  `json:"revision"`
		SourceTree     string  `json:"source_tree"`
		CodexVersion   *string `json:"codex_version"`
		CodexIntegrity *string `json:"codex_integrity"`
	} `json:"image"`
	CaseNames         []string           `json:"case_names"`
	Cases             []runtimeSuiteCase `json:"cases"`
	PairedHermeticRun *string            `json:"paired_hermetic_run"`
}

type runtimeSuiteCase struct {
	Name                   string `json:"name"`
	Path                   string `json:"path"`
	RunID                  string `json:"run_id"`
	Status                 string `json:"status"`
	ExitCode               int    `json:"exit_code"`
	TaskOraclesPassed      bool   `json:"task_oracles_passed"`
	SystemInvariantsPassed bool   `json:"system_invariants_passed"`
}

type runtimeManifest struct {
	SchemaVersion int                    `json:"schema_version"`
	RunID         string                 `json:"run_id"`
	Files         []runtimeManifestEntry `json:"files"`
}

type runtimeManifestEntry struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

type runtimeCaseReport struct {
	SchemaVersion  int             `json:"schema_version"`
	RunID          string          `json:"run_id"`
	Scenario       string          `json:"scenario"`
	Runtime        string          `json:"runtime"`
	Status         string          `json:"status"`
	GitSHA         string          `json:"git_sha"`
	ImageDigest    string          `json:"image_digest"`
	ScenarioDigest string          `json:"scenario_digest"`
	Versions       json.RawMessage `json:"versions"`
	Counts         json.RawMessage `json:"counts"`
	Commands       json.RawMessage `json:"commands"`
	Assertions     []struct {
		ID       string   `json:"id"`
		Category string   `json:"category"`
		Required bool     `json:"required"`
		Passed   bool     `json:"passed"`
		Evidence []string `json:"evidence"`
		Message  string   `json:"message"`
	} `json:"assertions"`
	Faults  []json.RawMessage `json:"faults"`
	Latency json.RawMessage   `json:"latency"`
	Oracle  struct {
		System struct {
			Passed bool `json:"passed"`
		} `json:"system"`
		Task struct {
			Passed  bool `json:"passed"`
			Results []struct {
				ID       string   `json:"id"`
				Passed   bool     `json:"passed"`
				ExitCode int      `json:"exit_code"`
				Evidence []string `json:"evidence"`
			} `json:"results"`
		} `json:"task"`
		Experience struct {
			Passed bool `json:"passed"`
		} `json:"experience"`
	} `json:"oracle"`
}

type loadedRuntimeBundle struct {
	root      string
	suite     runtimeSuiteReport
	files     map[string]runtimeManifestEntry
	casePath  string
	caseRunID string
}

const canonicalScenarioName = "payment-review"

func validateRuntimeBundles(root string, report GateReport) error {
	loaded := make(map[string]loadedRuntimeBundle, len(report.Bundles))
	for _, ref := range report.Bundles {
		bundle, err := loadRuntimeBundle(root, report, ref)
		if err != nil {
			return fmt.Errorf("%s bundle: %w", ref.Runtime, err)
		}
		loaded[ref.Runtime] = bundle
	}
	live, hasLive := loaded["codex"]
	if !hasLive {
		return nil
	}
	hermetic, hasHermetic := loaded["scripted"]
	if !hasHermetic || live.suite.PairedHermeticRun == nil ||
		*live.suite.PairedHermeticRun != hermetic.suite.RunID ||
		live.suite.Image.Digest != hermetic.suite.Image.Digest {
		return fmt.Errorf("Live bundle lacks an exact Hermetic commit/tree/image pair")
	}
	return nil
}

func loadRuntimeBundle(root string, report GateReport,
	ref GateBundleRef,
) (loadedRuntimeBundle, error) {
	reportData, err := readRuntimeFile(root, ref.ReportPath, ref.ReportSHA256)
	if err != nil {
		return loadedRuntimeBundle{}, err
	}
	var suite runtimeSuiteReport
	if err := decodeStrictJSON(reportData, &suite); err != nil {
		return loadedRuntimeBundle{}, fmt.Errorf("decode suite report: %w", err)
	}
	if !validRuntimeSuite(suite, ref, report.Source) {
		return loadedRuntimeBundle{}, fmt.Errorf("suite report identity or verdict is invalid")
	}
	manifestData, err := readRuntimeFile(root, ref.ManifestPath, ref.ManifestSHA256)
	if err != nil {
		return loadedRuntimeBundle{}, err
	}
	var manifest runtimeManifest
	if err := decodeStrictJSON(manifestData, &manifest); err != nil {
		return loadedRuntimeBundle{}, fmt.Errorf("decode evidence manifest: %w", err)
	}
	if manifest.SchemaVersion != 1 || manifest.RunID != ref.RunID || manifest.Files == nil {
		return loadedRuntimeBundle{}, fmt.Errorf("evidence manifest identity is invalid")
	}
	bundleRoot := filepath.Dir(filepath.Join(root, filepath.FromSlash(ref.ManifestPath)))
	files := make(map[string]runtimeManifestEntry, len(manifest.Files))
	previous := ""
	for _, file := range manifest.Files {
		if !validManifestEntry(file, previous) {
			return loadedRuntimeBundle{}, fmt.Errorf("invalid evidence manifest entry %q", file.Path)
		}
		previous = file.Path
		files[file.Path] = file
	}
	relativeReport, err := filepath.Rel(bundleRoot,
		filepath.Join(root, filepath.FromSlash(ref.ReportPath)))
	if err != nil || strings.HasPrefix(relativeReport, "..") {
		return loadedRuntimeBundle{}, fmt.Errorf("suite report is outside its bundle")
	}
	loaded := loadedRuntimeBundle{root: bundleRoot, suite: suite, files: files}
	if err := loaded.verifyFile(filepath.ToSlash(relativeReport)); err != nil {
		return loadedRuntimeBundle{}, fmt.Errorf("suite report manifest binding: %w", err)
	}
	if len(suite.Cases) != 1 ||
		!validRuntimeCase(suite.Cases[0], canonicalScenarioName, suite.RunID) {
		return loadedRuntimeBundle{}, fmt.Errorf("suite must contain only %s",
			canonicalScenarioName)
	}
	item := suite.Cases[0]
	loaded.casePath, loaded.caseRunID = item.Path, item.RunID
	return loaded, nil
}

func validRuntimeSuite(suite runtimeSuiteReport, ref GateBundleRef, source GateSource) bool {
	return suite.SchemaVersion == 1 && suite.RunID == ref.RunID &&
		suite.Runtime == ref.Runtime && suite.Status == "passed" &&
		suite.GitSHA == source.Commit && suite.Image.Revision == source.Commit &&
		suite.Image.SourceTree == source.Tree && digestPattern.MatchString(suite.Image.Digest) &&
		slices.Equal(suite.CaseNames, []string{canonicalScenarioName}) &&
		(ref.Runtime == "scripted") == (suite.PairedHermeticRun == nil)
}

func validManifestEntry(file runtimeManifestEntry, previous string) bool {
	return validateEvidencePath(file.Path) == nil && digestPattern.MatchString(file.SHA256) &&
		file.Bytes >= 0 && (previous == "" || file.Path > previous)
}

func validRuntimeCase(item runtimeSuiteCase, name, runID string) bool {
	return item.Name == name && item.Status == "passed" && item.RunID == runID+"-"+name &&
		item.Path == "cases/"+name && item.ExitCode == 0 && item.TaskOraclesPassed &&
		item.SystemInvariantsPassed && validateEvidencePath(item.Path) == nil
}

func validateScenarioRuntimeEvidence(root string, report GateReport, key ScenarioKey,
	runtimeName string,
) (bool, string, error) {
	var ref *GateBundleRef
	for index := range report.Bundles {
		if report.Bundles[index].Runtime == runtimeName {
			ref = &report.Bundles[index]
			break
		}
	}
	if ref == nil {
		return false, runtimeName + " evidence bundle was not attached", nil
	}
	bundle, err := loadRuntimeBundle(root, report, *ref)
	if err != nil {
		return false, "", err
	}
	if key.Scenario != canonicalScenarioName {
		return false, "scenario is absent from the runtime bundle: " + key.Scenario, nil
	}
	casePath := bundle.casePath
	caseRelative := filepath.ToSlash(filepath.Join(casePath, "report.json"))
	data, err := bundle.readVerifiedFile(caseRelative)
	if err != nil {
		return false, "", fmt.Errorf("scenario report: %w", err)
	}
	var caseReport runtimeCaseReport
	if err := decodeStrictJSON(data, &caseReport); err != nil {
		return false, "", fmt.Errorf("decode scenario report: %w", err)
	}
	if caseReport.SchemaVersion != 1 || caseReport.RunID != bundle.caseRunID ||
		caseReport.Scenario != key.Scenario ||
		caseReport.Runtime != runtimeName || caseReport.Status != "passed" ||
		caseReport.GitSHA != report.Source.Commit ||
		caseReport.ImageDigest != bundle.suite.Image.Digest {
		return false, "scenario runtime identity or verdict is invalid", nil
	}
	switch key.Kind {
	case "system", "experience":
		return bundle.validateAssertion(casePath, caseReport, key.Anchor, key.Kind)
	case "task":
		return bundle.validateTask(casePath, caseReport, key.Anchor)
	case "fault":
		return bundle.validateFault(casePath, caseReport, key.Anchor)
	default:
		return false, "", fmt.Errorf("unsupported scenario anchor kind %s", key.Kind)
	}
}

func (bundle loadedRuntimeBundle) validateAssertion(casePath string,
	report runtimeCaseReport, id, category string,
) (bool, string, error) {
	for _, assertion := range report.Assertions {
		if assertion.ID != id {
			continue
		}
		if assertion.Category != category || !assertion.Required || !assertion.Passed ||
			len(assertion.Evidence) == 0 {
			return false, "scenario assertion is not required, passing, and evidenced: " + id, nil
		}
		if err := bundle.verifyEvidenceRefs(casePath, assertion.Evidence); err != nil {
			return false, "", err
		}
		return true, "", nil
	}
	return false, "scenario assertion is absent: " + id, nil
}

func (bundle loadedRuntimeBundle) validateTask(casePath string, report runtimeCaseReport,
	id string,
) (bool, string, error) {
	ok, reason, err := bundle.validateAssertion(casePath, report, id, "task")
	if err != nil || !ok {
		return ok, reason, err
	}
	for _, result := range report.Oracle.Task.Results {
		if result.ID == id && result.Passed && result.ExitCode == 0 && len(result.Evidence) > 0 {
			if err := bundle.verifyEvidenceRefs(casePath, result.Evidence); err != nil {
				return false, "", err
			}
			return true, "", nil
		}
	}
	return false, "task oracle is absent or failed: " + id, nil
}

func (bundle loadedRuntimeBundle) verifyEvidenceRefs(casePath string, refs []string) error {
	for _, reference := range refs {
		if err := validateEvidencePath(reference); err != nil {
			return err
		}
		if err := bundle.verifyFile(
			filepath.ToSlash(filepath.Join(casePath, reference))); err != nil {
			return err
		}
	}
	return nil
}

func (bundle loadedRuntimeBundle) verifyFile(relative string) error {
	_, err := bundle.readVerifiedFile(relative)
	return err
}

func (bundle loadedRuntimeBundle) readVerifiedFile(relative string) ([]byte, error) {
	entry, exists := bundle.files[relative]
	if !exists {
		return nil, fmt.Errorf("evidence manifest does not bind %s", relative)
	}
	data, err := os.ReadFile(filepath.Join(bundle.root, filepath.FromSlash(relative)))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != entry.Bytes || bytesDigest(data) != entry.SHA256 {
		return nil, fmt.Errorf("evidence file %s differs from its manifest", relative)
	}
	return data, nil
}

func testEventPassed(data []byte, importPath, symbol string) (bool, error) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		var event struct {
			Action  string `json:"Action"`
			Package string `json:"Package"`
			Test    string `json:"Test"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return false, err
		}
		if event.Action == "pass" && event.Package == importPath && event.Test == symbol {
			return true, nil
		}
	}
	return false, scanner.Err()
}
