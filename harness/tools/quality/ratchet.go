package main

import (
	"fmt"
	"reflect"
)

type measurementRatchet struct {
	regularBaseline   map[string]baselineEntry
	duplicateBaseline []baselineEntry
	exceptions        map[string]exceptionEntry
	usedBaseline      map[string]struct{}
	usedExceptions    map[string]struct{}
}

func compareMeasurement(baseline baselineManifest, exceptions exceptionManifest, measured baselineManifest) error {
	ratchet := newMeasurementRatchet(baseline, exceptions)
	for _, current := range measured.Entries {
		if err := ratchet.check(current); err != nil {
			return err
		}
	}
	return ratchet.checkStale(baseline, exceptions)
}

func newMeasurementRatchet(baseline baselineManifest, exceptions exceptionManifest) *measurementRatchet {
	ratchet := &measurementRatchet{
		regularBaseline: make(map[string]baselineEntry), exceptions: make(map[string]exceptionEntry),
		usedBaseline: make(map[string]struct{}), usedExceptions: make(map[string]struct{}),
	}
	for _, entry := range baseline.Entries {
		if entry.Rule == ruleDuplicate {
			ratchet.duplicateBaseline = append(ratchet.duplicateBaseline, entry)
		} else {
			ratchet.regularBaseline[entry.Identity] = entry
		}
	}
	for _, entry := range exceptions.Entries {
		ratchet.exceptions[entry.Identity] = entry
	}
	return ratchet
}

func (ratchet *measurementRatchet) check(current baselineEntry) error {
	tracked, err := ratchet.trackedEntry(current)
	if err != nil {
		return err
	}
	if tracked != nil {
		return ratchet.checkTracked(*tracked, current)
	}
	return ratchet.checkException(current)
}

func (ratchet *measurementRatchet) trackedEntry(current baselineEntry) (*baselineEntry, error) {
	if current.Rule == ruleDuplicate {
		return matchDuplicateBaseline(current, ratchet.duplicateBaseline, ratchet.usedBaseline)
	}
	prior, exists := ratchet.regularBaseline[current.Identity]
	if !exists {
		return nil, nil
	}
	return &prior, nil
}

func (ratchet *measurementRatchet) checkTracked(prior, current baselineEntry) error {
	if prior.Rule == ruleDuplicate && prior.Fingerprint != current.Fingerprint {
		return fmt.Errorf("duplicate debt %s matched by owners but its baseline fingerprint is stale", prior.Identity)
	}
	if err := compareCeiling(prior, current); err != nil {
		return err
	}
	ratchet.usedBaseline[prior.Identity] = struct{}{}
	return nil
}

func (ratchet *measurementRatchet) checkException(current baselineEntry) error {
	exception, excepted := ratchet.exceptions[current.Identity]
	if !excepted {
		return fmt.Errorf("new untracked quality violation %s measured at %d", current.Identity, current.Ceiling)
	}
	if exception.Rule != current.Rule || exception.Path != current.Path || exception.Symbol != current.Symbol {
		return fmt.Errorf("quality exception %s does not exactly match the measured violation", exception.Identity)
	}
	if current.Rule == ruleCyclomatic && current.Ceiling > 30 {
		return fmt.Errorf("new cyclomatic complexity %s is %d and cannot be waived above 30", current.Identity, current.Ceiling)
	}
	if current.Ceiling != exception.Ceiling {
		return compareExceptionCeiling(exception, current.Ceiling)
	}
	ratchet.usedExceptions[exception.Identity] = struct{}{}
	return nil
}

func compareExceptionCeiling(exception exceptionEntry, current int) error {
	if current > exception.Ceiling {
		return fmt.Errorf("quality exception %s increased from %d to %d", exception.Identity, exception.Ceiling, current)
	}
	return fmt.Errorf("quality exception %s improved from %d to %d; lower its ceiling in the same change", exception.Identity, exception.Ceiling, current)
}

func (ratchet *measurementRatchet) checkStale(baseline baselineManifest, exceptions exceptionManifest) error {
	for _, entry := range baseline.Entries {
		if _, used := ratchet.usedBaseline[entry.Identity]; !used {
			return fmt.Errorf("stale quality baseline entry %s has no current violation", entry.Identity)
		}
	}
	for _, entry := range exceptions.Entries {
		if _, used := ratchet.usedExceptions[entry.Identity]; !used {
			return fmt.Errorf("stale quality exception %s has no current violation", entry.Identity)
		}
	}
	return nil
}

func compareCeiling(baseline, current baselineEntry) error {
	if current.Ceiling > baseline.Ceiling {
		return fmt.Errorf("quality violation %s increased from %d to %d", baseline.Identity, baseline.Ceiling, current.Ceiling)
	}
	if current.Ceiling < baseline.Ceiling {
		return fmt.Errorf("quality violation %s improved from %d to %d; lower its baseline ceiling in the same change", baseline.Identity, baseline.Ceiling, current.Ceiling)
	}
	return nil
}

func matchDuplicateBaseline(current baselineEntry, candidates []baselineEntry, used map[string]struct{}) (*baselineEntry, error) {
	var exact []baselineEntry
	for _, candidate := range candidates {
		if _, alreadyUsed := used[candidate.Identity]; alreadyUsed || !reflect.DeepEqual(candidate.Owners, current.Owners) {
			continue
		}
		if candidate.Fingerprint == current.Fingerprint {
			exact = append(exact, candidate)
		}
	}
	if len(exact) == 1 {
		return &exact[0], nil
	}
	if len(exact) > 1 {
		return nil, fmt.Errorf("duplicate violation with owners %v has ambiguous exact baseline matches", current.Owners)
	}
	return nil, nil
}
