package integration

import (
	"errors"
	"fmt"
	"os"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
)

// HostProjectionUpgradePreflight records the closed old and new facts observed
// before an active Host projection upgrade. It is an observation, not an
// authorization to mutate the projection; InstallHostProjection performs its
// own exact checks before publishing the upgrade journal.
type HostProjectionUpgradePreflight struct {
	ConfigPath       string
	Host             assets.Host
	OwnershipPath    string
	PreviousRevision string
	Revision         string
}

// PreflightHostProjectionUpgrade proves that one complete previous applied
// projection is still exact and that the desired canonical Node bundle is
// installed and valid. It performs no repair, staging, journaling or other
// filesystem mutation.
func PreflightHostProjectionUpgrade(workspace, nodeState string, host assets.Host,
	expectedActiveRevision string, bundle assets.Bundle,
) (HostProjectionUpgradePreflight, error) {
	if !validProjectionDigest(expectedActiveRevision) {
		return HostProjectionUpgradePreflight{}, fmt.Errorf(
			"%w: expected active asset revision is invalid", ErrUnsafeProjection)
	}
	plan, err := prepareProjection(workspace, nodeState, host, bundle)
	if err != nil {
		return HostProjectionUpgradePreflight{}, err
	}
	if expectedActiveRevision == plan.applied.AssetRevision {
		return HostProjectionUpgradePreflight{}, projectionConflict(
			"Host projection upgrade requires distinct active and desired revisions", nil)
	}

	ownership, err := readProjectionOwnership(plan)
	if err != nil {
		return HostProjectionUpgradePreflight{}, err
	}
	if ownership.state != projectionPrevious || ownership.previous == nil {
		return HostProjectionUpgradePreflight{}, projectionConflict(
			"Host projection upgrade requires one complete previous applied ownership", nil)
	}
	if ownership.previous.AssetRevision != expectedActiveRevision {
		return HostProjectionUpgradePreflight{}, projectionConflict(
			"previous applied ownership revision differs from active authority", nil)
	}
	if err := verifyCompletePreviousProjection(plan, *ownership.previous); err != nil {
		return HostProjectionUpgradePreflight{}, err
	}

	return HostProjectionUpgradePreflight{
		ConfigPath:       plan.configPath,
		Host:             host,
		OwnershipPath:    plan.ownershipPath,
		PreviousRevision: ownership.previous.AssetRevision,
		Revision:         plan.applied.AssetRevision,
	}, nil
}

func verifyCompletePreviousProjection(plan projectionPlan, previous ownershipPrevious) error {
	if err := validateExistingProjectionParents(plan); err != nil {
		return err
	}
	for _, file := range plan.files {
		record := previousFileRecord(&previous, file.record.Path)
		if record == nil {
			return projectionConflict("previous ownership omits a frozen managed file", nil)
		}
		content, _, err := readSafeRegular(file.path, file.mode, maxHostConfigBytes)
		if err != nil || digest(content) != record.InstalledDigest {
			if errors.Is(err, os.ErrNotExist) {
				err = errors.New("managed Host file is missing")
			}
			return projectionConflict("managed Host file differs from previous applied ownership", err)
		}
	}

	document, _, err := readSharedJSON(plan.configPath)
	if err != nil {
		return err
	}
	entries, err := registrationEntries(document, plan.registration.Value.Event, true)
	if err != nil {
		return err
	}
	previousDigest := previous.Registrations[0].InstalledDigest
	digestMatches, commandMatches, index, err := registrationDigestAndCommandCounts(
		entries, previousDigest, plan.entry.Hooks[0].Command)
	if err != nil {
		return err
	}
	if digestMatches != 1 || commandMatches != 1 || index < 0 ||
		!entryUsesExactCommand(entries[index], plan.entry.Hooks[0].Command) {
		return projectionConflict(fmt.Sprintf(
			"previous managed Host registration digest/command counts are %d/%d, want 1/1",
			digestMatches, commandMatches), nil)
	}
	return nil
}
