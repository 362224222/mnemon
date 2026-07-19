package main

import (
	"fmt"
	"os/exec"
	"reflect"
)

func runGit(root string, arguments ...string) ([]byte, error) {
	command := exec.Command("git", append([]string{"-C", root}, arguments...)...)
	output, err := command.Output()
	if err == nil {
		return output, nil
	}
	if exit, ok := err.(*exec.ExitError); ok {
		return nil, fmt.Errorf("git %v: %s", arguments, exit.Stderr)
	}
	return nil, fmt.Errorf("run git %v: %w", arguments, err)
}

func gitManifest[T any](root, reference, path string) (T, bool, error) {
	var zero T
	command := exec.Command("git", "-C", root, "show", reference+":"+path)
	data, err := command.Output()
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok && exit.ExitCode() != 0 {
			return zero, false, nil
		}
		return zero, false, fmt.Errorf("read %s at %s: %w", path, reference, err)
	}
	manifest, err := decodeExactJSON[T](data, reference+":"+path)
	if err != nil {
		return zero, false, err
	}
	canonical, err := canonicalJSON(manifest)
	if err != nil {
		return zero, false, err
	}
	if !reflect.DeepEqual(data, canonical) {
		return zero, false, fmt.Errorf("%s at %s is not canonical JSON", path, reference)
	}
	return manifest, true, nil
}

func compareBaseBaseline(base, candidate baselineManifest) error {
	if candidate.SourceCommit != base.SourceCommit {
		return fmt.Errorf("quality baseline source_commit changed from %s to %s", base.SourceCommit, candidate.SourceCommit)
	}
	baseEntries := make(map[string]baselineEntry, len(base.Entries))
	for _, entry := range base.Entries {
		baseEntries[entry.Identity] = entry
	}
	for _, entry := range candidate.Entries {
		prior, exists := baseEntries[entry.Identity]
		if !exists {
			return fmt.Errorf("quality baseline adds identity %s relative to base", entry.Identity)
		}
		if entry.Rule != prior.Rule || entry.Symbol != prior.Symbol || entry.DebtID != prior.DebtID {
			return fmt.Errorf("quality baseline rebinds identity %s relative to base", entry.Identity)
		}
		if entry.Rule == ruleDuplicate {
			if err := validateDuplicateEvolution(prior, entry); err != nil {
				return err
			}
		} else if entry.Path != prior.Path {
			return fmt.Errorf("quality baseline rebinds identity %s relative to base", entry.Identity)
		}
		if entry.Ceiling > prior.Ceiling {
			return fmt.Errorf("quality baseline raises %s from %d to %d", entry.Identity, prior.Ceiling, entry.Ceiling)
		}
	}
	return nil
}

func validateDuplicateEvolution(prior, current baselineEntry) error {
	ownersChanged := !reflect.DeepEqual(prior.Owners, current.Owners)
	if prior.Fingerprint != current.Fingerprint {
		return fmt.Errorf("duplicate debt %s cannot rebind fingerprint", current.Identity)
	}
	if !ownersChanged {
		if current.Path != prior.Path {
			return fmt.Errorf("duplicate debt %s rebinds its derived path without owner cleanup", current.Identity)
		}
		return nil
	}
	priorOwners := make(map[string]struct{}, len(prior.Owners))
	for _, owner := range prior.Owners {
		priorOwners[owner] = struct{}{}
	}
	for _, owner := range current.Owners {
		if _, existed := priorOwners[owner]; !existed {
			return fmt.Errorf("duplicate debt %s adds or rebinds owner %s", current.Identity, owner)
		}
	}
	if len(current.Owners) >= len(prior.Owners) {
		return fmt.Errorf("duplicate debt %s owner change is not a strict cleanup", current.Identity)
	}
	if len(current.Owners) < 2 {
		return fmt.Errorf("duplicate debt %s owner cleanup leaves fewer than two owners", current.Identity)
	}
	path, _, err := parsePathSymbol(current.Owners[0])
	if err != nil || current.Path != path {
		return fmt.Errorf("duplicate debt %s path does not follow its first remaining owner", current.Identity)
	}
	return nil
}

func compareBaseArchitecture(base, candidate architectureManifest) error {
	if candidate.SourceCommit != base.SourceCommit {
		return fmt.Errorf("architecture debt source_commit changed from %s to %s", base.SourceCommit, candidate.SourceCommit)
	}
	baseEntries := make(map[string]architectureEntry, len(base.Entries))
	for _, entry := range base.Entries {
		baseEntries[entry.Identity] = entry
	}
	for _, entry := range candidate.Entries {
		prior, exists := baseEntries[entry.Identity]
		if !exists {
			return fmt.Errorf("architecture debt adds identity %s relative to base", entry.Identity)
		}
		if entry.Rule != prior.Rule || entry.Path != prior.Path || entry.Symbol != prior.Symbol || entry.Component != prior.Component {
			return fmt.Errorf("architecture debt rebinds identity %s relative to base", entry.Identity)
		}
		if riskRank(entry.Risk) > riskRank(prior.Risk) {
			return fmt.Errorf("architecture debt upgrades %s risk from %s to %s", entry.Identity, prior.Risk, entry.Risk)
		}
	}
	return nil
}

func compareBaseExceptions(base, candidate exceptionManifest) error {
	baseEntries := make(map[string]exceptionEntry, len(base.Entries))
	for _, entry := range base.Entries {
		baseEntries[entry.Identity] = entry
	}
	for _, entry := range candidate.Entries {
		prior, exists := baseEntries[entry.Identity]
		if !exists {
			continue
		}
		if entry.Rule != prior.Rule || entry.Path != prior.Path || entry.Symbol != prior.Symbol || entry.Component != prior.Component {
			return fmt.Errorf("quality exception %s broadens or rebinds its scope", entry.Identity)
		}
		if entry.Ceiling > prior.Ceiling {
			return fmt.Errorf("quality exception %s raises ceiling from %d to %d", entry.Identity, prior.Ceiling, entry.Ceiling)
		}
	}
	return nil
}

func riskRank(risk string) int {
	switch risk {
	case "critical":
		return 3
	case "high":
		return 2
	case "medium":
		return 1
	default:
		return 0
	}
}
