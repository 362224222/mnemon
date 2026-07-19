package main

import (
	"fmt"
	"path/filepath"
	"strings"
)

const (
	baselinePath     = "harness/test/contracts/go_quality_baseline.json"
	exceptionsPath   = "harness/test/contracts/go_quality_exceptions.json"
	architecturePath = "harness/test/contracts/go_architecture_debt.json"
	expectedPath     = "harness/test/contracts/expected_requirements.json"
	requirementsPath = "harness/test/contracts/requirements.json"
)

type contractBundle struct {
	baseline     baselineManifest
	exceptions   exceptionManifest
	architecture architectureManifest
	expected     expectedManifest
	requirements requirementsManifest
}

func checkRepository(root, baseReference string) error {
	root, err := filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("resolve root: %w", err)
	}
	exclusions, err := loadQualityExclusions(root, true)
	if err != nil {
		return err
	}
	if err := validateExclusionEvidence(root, exclusions); err != nil {
		return err
	}
	contracts, err := loadContractBundle(root)
	if err != nil {
		return err
	}
	if err := validateContractBundle(root, contracts); err != nil {
		return err
	}
	return runRepositoryChecks(root, baseReference, contracts)
}

func loadContractBundle(root string) (contractBundle, error) {
	var bundle contractBundle
	loads := []func() error{
		func() error {
			var err error
			bundle.baseline, err = readExactJSON[baselineManifest](filepath.Join(root, baselinePath))
			return err
		},
		func() error {
			var err error
			bundle.exceptions, err = readExactJSON[exceptionManifest](filepath.Join(root, exceptionsPath))
			return err
		},
		func() error {
			var err error
			bundle.architecture, err = readExactJSON[architectureManifest](filepath.Join(root, architecturePath))
			return err
		},
		func() error {
			var err error
			bundle.expected, err = readExactJSON[expectedManifest](filepath.Join(root, expectedPath))
			return err
		},
		func() error {
			var err error
			bundle.requirements, err = readExactJSON[requirementsManifest](filepath.Join(root, requirementsPath))
			return err
		},
	}
	for _, load := range loads {
		if err := load(); err != nil {
			return contractBundle{}, err
		}
	}
	return bundle, nil
}

func validateContractBundle(root string, bundle contractBundle) error {
	if err := validateAllManifests(bundle.baseline, bundle.exceptions, bundle.architecture, bundle.expected, bundle.requirements); err != nil {
		return err
	}
	if bundle.baseline.SourceCommit != bundle.architecture.SourceCommit {
		return fmt.Errorf("quality baseline and architecture debt must use the same source_commit")
	}
	if _, err := runGit(root, "cat-file", "-e", bundle.baseline.SourceCommit+"^{commit}"); err != nil {
		return fmt.Errorf("baseline source_commit does not exist: %w", err)
	}
	if _, err := runGit(root, "merge-base", "--is-ancestor", bundle.baseline.SourceCommit, "HEAD"); err != nil {
		return fmt.Errorf("baseline source_commit is not an ancestor of HEAD")
	}
	return nil
}

func runRepositoryChecks(root, baseReference string, bundle contractBundle) error {
	files, err := loadHarnessSources(root)
	if err != nil {
		return err
	}
	drift, err := gofmtDrift(files)
	if err != nil {
		return err
	}
	if len(drift) > 0 {
		return fmt.Errorf("gofmt drift: %s", strings.Join(drift, ", "))
	}
	if directives := nolintDiagnostics(files); len(directives) > 0 {
		return fmt.Errorf("bare, wildcard, or unexplained //nolint directives: %s", strings.Join(directives, ", "))
	}
	measured, err := measureTree(root, bundle.baseline.SourceCommit)
	if err != nil {
		return err
	}
	if err := compareMeasurement(bundle.baseline, bundle.exceptions, measured); err != nil {
		return err
	}
	findings, err := dependencyFindings(root)
	if err != nil {
		return err
	}
	if err := validateArchitectureEvidence(root, bundle.architecture, findings); err != nil {
		return err
	}
	if err := validateRequirementEvidence(root, bundle.expected, bundle.requirements); err != nil {
		return err
	}
	if baseReference != "" {
		if err := compareManifestHistory(root, baseReference, bundle.baseline, bundle.exceptions, bundle.architecture); err != nil {
			return err
		}
	}
	return nil
}

func validateAllManifests(baseline baselineManifest, exceptions exceptionManifest, architecture architectureManifest, expected expectedManifest, requirements requirementsManifest) error {
	validators := []func() error{
		func() error { return validateBaseline(baseline) },
		func() error { return validateExceptions(exceptions) },
		func() error { return validateArchitectureManifest(architecture) },
		func() error { return validateExpectedManifest(expected) },
		func() error { return validateRequirementsManifest(requirements) },
	}
	for _, validate := range validators {
		if err := validate(); err != nil {
			return err
		}
	}
	return nil
}

func compareManifestHistory(root, baseReference string, baseline baselineManifest, exceptions exceptionManifest, architecture architectureManifest) error {
	if _, err := runGit(root, "rev-parse", "--verify", baseReference+"^{commit}"); err != nil {
		return fmt.Errorf("invalid base-ref %q: %w", baseReference, err)
	}
	mergeBaseBytes, err := runGit(root, "merge-base", baseReference, "HEAD")
	if err != nil {
		return fmt.Errorf("base-ref %q has no merge base with HEAD: %w", baseReference, err)
	}
	deltaAnchor := strings.TrimSpace(string(mergeBaseBytes))
	if err := validateFullCommit(deltaAnchor, "base-ref merge base"); err != nil {
		return err
	}
	if err := validateCommittedManifestChain(root, deltaAnchor); err != nil {
		return err
	}
	lifetimeLedger, err := committedManifestLedger(root, baseline.SourceCommit)
	if err != nil {
		return err
	}
	prior, found, err := loadContractBundleAtRef(root, "HEAD")
	if err != nil {
		return err
	}
	current := contractBundle{baseline: baseline, exceptions: exceptions, architecture: architecture}
	if !found {
		if err := validateBootstrapSource(root, baseline.SourceCommit, architecture.SourceCommit); err != nil {
			return err
		}
		if err := validateRatchetManifests(current); err != nil {
			return err
		}
	} else {
		if err := compareContractBundles(prior, current); err != nil {
			return err
		}
	}
	if err := lifetimeLedger.observe(current); err != nil {
		return err
	}
	base, baseFound, err := loadContractBundleAtRef(root, baseReference)
	if err != nil {
		return err
	}
	if !baseFound {
		return nil
	}
	if err := compareContractBundles(base, current); err != nil {
		return fmt.Errorf("candidate is not monotone relative to base-ref %s: %w", baseReference, err)
	}
	baseLedger, err := committedManifestLedgerTo(root, baseline.SourceCommit, baseReference)
	if err != nil {
		return err
	}
	merged, err := mergeRatchetLedgers([]manifestLineage{{ledger: lifetimeLedger}, {ledger: baseLedger}})
	if err != nil {
		return err
	}
	return merged.observe(current)
}

func validateBootstrapSource(root, baselineSource, architectureSource string) error {
	head, err := currentCommit(root)
	if err != nil {
		return err
	}
	parentBytes, parentErr := runGit(root, "rev-parse", "HEAD^")
	parent := strings.TrimSpace(string(parentBytes))
	valid := baselineSource == head || (parentErr == nil && baselineSource == parent)
	if !valid || architectureSource != baselineSource {
		return fmt.Errorf("base-ref has no quality manifests and no deterministic ratchet anchor; bootstrap source_commit must equal HEAD or its first parent")
	}
	return nil
}
