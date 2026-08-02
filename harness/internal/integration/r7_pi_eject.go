package integration

import (
	"bytes"
	"errors"
	"os"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
)

// EjectR7PiProjectionReceipt reports removal of the exact owned files. Empty
// parent directories and every adjacent Pi asset remain workspace-owned.
type EjectR7PiProjectionReceipt struct {
	RemovedFiles int
	Replayed     bool
	Revision     string
}

// EjectR7PiProjection removes only an exact R7 Pi projection. Drift fails
// closed before deletion. Missing files under a valid journal are resumable
// state from an interrupted prior eject.
func EjectR7PiProjection(workspace string,
	projection assets.R7Projection,
) (EjectR7PiProjectionReceipt, error) {
	plan, err := prepareR7PiProjection(workspace, projection)
	if err != nil {
		return EjectR7PiProjectionReceipt{}, err
	}
	receipt := EjectR7PiProjectionReceipt{Revision: plan.applied.Revision}

	if _, err := os.Lstat(plan.ownershipPath); errors.Is(err, os.ErrNotExist) {
		if err := verifyR7PiProjectionAbsent(plan); err != nil {
			return EjectR7PiProjectionReceipt{}, err
		}
		receipt.Replayed = true
		return receipt, nil
	} else if err != nil {
		return EjectR7PiProjectionReceipt{}, projectionConflict(
			"inspect R7 Pi ownership journal", err)
	}

	_, journal, err := readR7PiOwnership(plan)
	if err != nil {
		return EjectR7PiProjectionReceipt{}, err
	}
	present := make([]bool, len(plan.files))
	for index, file := range plan.files {
		content, _, readErr := readSafeRegular(file.path, file.mode, maxHostConfigBytes)
		if errors.Is(readErr, os.ErrNotExist) {
			continue
		}
		if readErr != nil || !bytes.Equal(content, file.content) {
			return EjectR7PiProjectionReceipt{}, projectionConflict(
				"R7 Pi file differs from eject ownership", readErr)
		}
		present[index] = true
	}
	for index, file := range plan.files {
		if !present[index] {
			continue
		}
		if err := removeExactProjectedFile(file.path, file.mode, file.record.Digest); err != nil {
			return EjectR7PiProjectionReceipt{}, err
		}
		receipt.RemovedFiles++
	}
	if err := verifyR7PiProjectionAbsent(plan); err != nil {
		return EjectR7PiProjectionReceipt{}, err
	}
	if err := removeExactOwnership(plan.ownershipPath, journal); err != nil {
		return EjectR7PiProjectionReceipt{}, err
	}
	return receipt, nil
}

func verifyR7PiProjectionAbsent(plan r7PiProjectionPlan) error {
	if err := validateExistingR7PiParents(plan); err != nil {
		return err
	}
	for _, file := range plan.files {
		if _, err := os.Lstat(file.path); err == nil {
			return projectionConflict("R7 Pi projected file remains", nil)
		} else if !errors.Is(err, os.ErrNotExist) {
			return projectionConflict("inspect absent R7 Pi projected file", err)
		}
	}
	return nil
}
