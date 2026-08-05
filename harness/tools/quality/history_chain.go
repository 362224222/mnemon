package main

import (
	"fmt"
	"strings"
)

type ratchetLedger struct {
	baselineIdentities  map[string]struct{}
	exceptions          map[string]exceptionEntry
	activeExceptions    map[string]struct{}
	exceptionTombstones map[string]struct{}
}

func newRatchetLedger() *ratchetLedger {
	return &ratchetLedger{
		baselineIdentities:  make(map[string]struct{}),
		exceptions:          make(map[string]exceptionEntry),
		activeExceptions:    make(map[string]struct{}),
		exceptionTombstones: make(map[string]struct{}),
	}
}

type manifestCommit struct {
	hash    string
	parents []string
}

type manifestLineage struct {
	bundle contractBundle
	found  bool
	ledger *ratchetLedger
}

func validateCommittedManifestChain(root, baseReference string) error {
	_, err := committedManifestLedgerTo(root, baseReference, "HEAD")
	return err
}

func committedManifestLedger(root, anchorReference string) (*ratchetLedger, error) {
	return committedManifestLedgerTo(root, anchorReference, "HEAD")
}

func committedManifestLedgerTo(root, anchorReference, targetReference string) (*ratchetLedger, error) {
	anchorHash, err := resolveCommitHash(root, anchorReference)
	if err != nil {
		return nil, err
	}
	commits, err := manifestHistoryCommits(root, anchorReference, targetReference)
	if err != nil {
		return nil, err
	}
	anchor, err := loadBoundaryLineage(root, anchorHash)
	if err != nil {
		return nil, err
	}
	states := map[string]manifestLineage{anchorHash: anchor}
	boundaries := make(map[string]manifestLineage)
	for _, commit := range commits {
		state, err := buildManifestLineage(root, commit, states, boundaries)
		if err != nil {
			return nil, err
		}
		states[commit.hash] = state
	}
	targetHash, err := resolveCommitHash(root, targetReference)
	if err != nil {
		return nil, err
	}
	if targetHash == anchorHash {
		return anchor.ledger, nil
	}
	state, exists := states[targetHash]
	if !exists {
		state, err = loadBoundaryLineage(root, targetHash)
		if err != nil {
			return nil, err
		}
	}
	return state.ledger, nil
}

func resolveCommitHash(root, reference string) (string, error) {
	data, err := runGit(root, "rev-parse", reference+"^{commit}")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

func buildManifestLineage(
	root string,
	commit manifestCommit,
	states map[string]manifestLineage,
	boundaries map[string]manifestLineage,
) (manifestLineage, error) {
	parents, err := loadParentLineages(root, commit.parents, states, boundaries)
	if err != nil {
		return manifestLineage{}, err
	}
	ledger, err := mergeRatchetLedgers(parents)
	if err != nil {
		return manifestLineage{}, err
	}
	current, found, err := loadContractBundleAtRef(root, commit.hash)
	if err != nil {
		return manifestLineage{}, err
	}
	foundParents, err := validateManifestHistoryEdges(commit.hash, current, found, parents)
	if err != nil {
		return manifestLineage{}, err
	}
	if found && foundParents == 0 {
		if err := validateBootstrapCommit(root, commit.hash, current); err != nil {
			return manifestLineage{}, err
		}
	}
	if found {
		if err := ledger.observe(current); err != nil {
			return manifestLineage{}, fmt.Errorf("manifest history at %s: %w", commit.hash, err)
		}
	}
	return manifestLineage{bundle: current, found: found, ledger: ledger}, nil
}

func loadParentLineages(
	root string,
	parentHashes []string,
	states map[string]manifestLineage,
	boundaries map[string]manifestLineage,
) ([]manifestLineage, error) {
	parents := make([]manifestLineage, 0, len(parentHashes))
	for _, parentHash := range parentHashes {
		state, exists := states[parentHash]
		if !exists {
			state, exists = boundaries[parentHash]
		}
		if !exists {
			var err error
			state, err = loadBoundaryLineage(root, parentHash)
			if err != nil {
				return nil, err
			}
			boundaries[parentHash] = state
		}
		parents = append(parents, state)
	}
	return parents, nil
}

func validateManifestHistoryEdges(
	commitHash string,
	current contractBundle,
	found bool,
	parents []manifestLineage,
) (int, error) {
	foundParents := 0
	for _, parent := range parents {
		if !parent.found {
			continue
		}
		foundParents++
		if !found {
			return 0, fmt.Errorf("manifest history at %s removes the complete contract bundle", commitHash)
		}
		if err := compareContractBundles(parent.bundle, current); err != nil {
			return 0, fmt.Errorf("manifest history edge into %s: %w", commitHash, err)
		}
	}
	return foundParents, nil
}

func (ledger *ratchetLedger) observe(bundle contractBundle) error {
	for _, entry := range bundle.baseline.Entries {
		ledger.baselineIdentities[entry.Identity] = struct{}{}
	}
	current := make(map[string]struct{}, len(bundle.exceptions.Entries))
	for _, entry := range bundle.exceptions.Entries {
		current[entry.Identity] = struct{}{}
	}
	for identity := range ledger.activeExceptions {
		if _, remains := current[identity]; !remains {
			ledger.exceptionTombstones[identity] = struct{}{}
		}
	}
	nextActive := make(map[string]struct{}, len(current))
	for _, entry := range bundle.exceptions.Entries {
		if _, wasBaseline := ledger.baselineIdentities[entry.Identity]; wasBaseline {
			return fmt.Errorf("historical baseline identity %s cannot move to exceptions", entry.Identity)
		}
		if _, removed := ledger.exceptionTombstones[entry.Identity]; removed {
			return fmt.Errorf("historical quality exception %s cannot be resurrected after removal", entry.Identity)
		}
		prior, seen := ledger.exceptions[entry.Identity]
		if seen {
			if entry.Rule != prior.Rule || entry.Path != prior.Path || entry.Symbol != prior.Symbol || entry.Component != prior.Component {
				return fmt.Errorf("historical quality exception %s cannot rebind its scope", entry.Identity)
			}
			if entry.Ceiling > prior.Ceiling {
				return fmt.Errorf("historical quality exception %s raises ceiling from %d to %d", entry.Identity, prior.Ceiling, entry.Ceiling)
			}
		}
		ledger.exceptions[entry.Identity] = entry
		nextActive[entry.Identity] = struct{}{}
	}
	ledger.activeExceptions = nextActive
	return nil
}

func loadBoundaryLineage(root, reference string) (manifestLineage, error) {
	bundle, found, err := loadContractBundleAtRef(root, reference)
	if err != nil {
		return manifestLineage{}, err
	}
	ledger := newRatchetLedger()
	if found {
		if err := validateRatchetManifests(bundle); err != nil {
			return manifestLineage{}, fmt.Errorf("manifest history at %s: %w", reference, err)
		}
		if err := ledger.observe(bundle); err != nil {
			return manifestLineage{}, fmt.Errorf("manifest history at %s: %w", reference, err)
		}
	}
	return manifestLineage{bundle: bundle, found: found, ledger: ledger}, nil
}

func mergeRatchetLedgers(parents []manifestLineage) (*ratchetLedger, error) {
	merged := newRatchetLedger()
	for _, parent := range parents {
		for identity := range parent.ledger.baselineIdentities {
			merged.baselineIdentities[identity] = struct{}{}
		}
		for identity := range parent.ledger.activeExceptions {
			merged.activeExceptions[identity] = struct{}{}
		}
		for identity := range parent.ledger.exceptionTombstones {
			merged.exceptionTombstones[identity] = struct{}{}
		}
		for identity, entry := range parent.ledger.exceptions {
			prior, exists := merged.exceptions[identity]
			if exists {
				if entry.Rule != prior.Rule || entry.Path != prior.Path || entry.Symbol != prior.Symbol || entry.Component != prior.Component {
					return nil, fmt.Errorf("historical quality exception %s has conflicting parent scopes", identity)
				}
				if entry.Ceiling >= prior.Ceiling {
					continue
				}
			}
			merged.exceptions[identity] = entry
		}
	}
	for identity := range merged.exceptionTombstones {
		delete(merged.activeExceptions, identity)
	}
	return merged, nil
}

func manifestHistoryCommits(root, anchorReference, targetReference string) ([]manifestCommit, error) {
	arguments := []string{"rev-list", "--reverse", "--topo-order", "--parents", "--full-history", anchorReference + ".." + targetReference}
	data, err := runGit(root, arguments...)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	commits := make([]manifestCommit, 0, len(lines))
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		commits = append(commits, manifestCommit{hash: fields[0], parents: append([]string(nil), fields[1:]...)})
	}
	return commits, nil
}

func loadContractBundleAtRef(root, reference string) (contractBundle, bool, error) {
	baseline, baselineFound, err := gitManifest[baselineManifest](root, reference, baselinePath)
	if err != nil {
		return contractBundle{}, false, err
	}
	exceptions, exceptionsFound, err := gitManifest[exceptionManifest](root, reference, exceptionsPath)
	if err != nil {
		return contractBundle{}, false, err
	}
	architecture, architectureFound, err := gitManifest[architectureManifest](root, reference, architecturePath)
	if err != nil {
		return contractBundle{}, false, err
	}
	foundCount := boolCount(baselineFound) + boolCount(exceptionsFound) + boolCount(architectureFound)
	if foundCount == 0 {
		return contractBundle{}, false, nil
	}
	if foundCount != 3 {
		return contractBundle{}, false, fmt.Errorf("%s contains a partial quality manifest set", reference)
	}
	return contractBundle{baseline: baseline, exceptions: exceptions, architecture: architecture}, true, nil
}

func validateBootstrapCommit(root, commit string, bundle contractBundle) error {
	if err := validateRatchetManifests(bundle); err != nil {
		return err
	}
	parents, err := runGit(root, "rev-list", "--parents", "-n", "1", commit)
	if err != nil {
		return err
	}
	fields := strings.Fields(string(parents))
	for _, parent := range fields[1:] {
		if bundle.baseline.SourceCommit == parent && bundle.architecture.SourceCommit == parent {
			return nil
		}
	}
	return fmt.Errorf("bootstrap manifest commit %s source_commit is not an exact parent", commit)
}

func compareContractBundles(prior, current contractBundle) error {
	if err := validateRatchetManifests(current); err != nil {
		return err
	}
	if err := forbidBaselineExceptionConversion(prior.baseline, current.exceptions); err != nil {
		return err
	}
	if err := compareBaseBaseline(prior.baseline, current.baseline); err != nil {
		return err
	}
	if err := compareBaseExceptions(prior.exceptions, current.exceptions); err != nil {
		return err
	}
	return compareBaseArchitecture(prior.architecture, current.architecture)
}

func validateRatchetManifests(bundle contractBundle) error {
	if err := validateBaseline(bundle.baseline); err != nil {
		return err
	}
	if err := validateExceptions(bundle.exceptions); err != nil {
		return err
	}
	return validateArchitectureManifest(bundle.architecture)
}

func forbidBaselineExceptionConversion(prior baselineManifest, current exceptionManifest) error {
	baselineIdentities := make(map[string]struct{}, len(prior.Entries))
	for _, entry := range prior.Entries {
		baselineIdentities[entry.Identity] = struct{}{}
	}
	for _, exception := range current.Entries {
		if _, converted := baselineIdentities[exception.Identity]; converted {
			return fmt.Errorf("historical baseline identity %s cannot move to exceptions", exception.Identity)
		}
	}
	return nil
}

func boolCount(value bool) int {
	if value {
		return 1
	}
	return 0
}
