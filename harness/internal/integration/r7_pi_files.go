package integration

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
)

func createR7PiProjectionDirectories(plan r7PiProjectionPlan) error {
	for _, file := range plan.files {
		relative, err := filepath.Rel(plan.workspace, filepath.Dir(file.path))
		if err != nil {
			return projectionConflict("resolve R7 Pi projection directory", err)
		}
		if err := ensureOwnedDirectoryChain(plan.workspace, relative, 0o755); err != nil {
			return err
		}
	}
	return nil
}

func convergeR7PiFiles(plan r7PiProjectionPlan, boundary r7PiProjectionBoundary) error {
	if err := createR7PiProjectionDirectories(plan); err != nil {
		return err
	}
	for _, file := range plan.files {
		if err := convergeR7PiFile(plan, file); err != nil {
			return err
		}
		if err := runR7PiBoundary(boundary, "after_file:"+file.record.Path); err != nil {
			return err
		}
	}
	return verifyR7PiProjectedState(plan)
}

func convergeR7PiFile(plan r7PiProjectionPlan, file r7PiProjectedFile) error {
	content, _, err := readSafeRegular(file.path, file.mode, maxHostConfigBytes)
	if err == nil {
		if !bytes.Equal(content, file.content) {
			return projectionConflict("R7 Pi file drift under ownership journal", nil)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return projectionConflict("R7 Pi file drift under ownership journal", err)
	}
	publishErr := publishExclusiveFile(filepath.Dir(plan.ownershipPath), file.path,
		file.content, file.mode)
	if publishErr == nil {
		return nil
	}
	current, _, readErr := readSafeRegular(file.path, file.mode, maxHostConfigBytes)
	if readErr != nil || !bytes.Equal(current, file.content) {
		return projectionConflict("publish R7 Pi file", publishErr)
	}
	return nil
}

func verifyR7PiProjectedState(plan r7PiProjectionPlan) error {
	for _, file := range plan.files {
		relative, err := filepath.Rel(plan.workspace, filepath.Dir(file.path))
		if err != nil {
			return projectionConflict("resolve R7 Pi projection directory", err)
		}
		if err := requireOwnedDirectoryChain(plan.workspace, relative); err != nil {
			return err
		}
		content, info, err := readSafeRegular(file.path, file.mode, maxHostConfigBytes)
		if err != nil || !ownedByCurrentUser(info) || !bytes.Equal(content, file.content) {
			return projectionConflict("R7 Pi projected file drift", err)
		}
	}
	return nil
}

func validateExistingR7PiParents(plan r7PiProjectionPlan) error {
	paths := []string{filepath.Dir(plan.ownershipPath)}
	for _, file := range plan.files {
		paths = append(paths, filepath.Dir(file.path))
	}
	for _, path := range paths {
		relative, err := filepath.Rel(plan.workspace, path)
		if err != nil {
			return projectionConflict("resolve R7 Pi projection parent", err)
		}
		if err := requireExistingOwnedDirectoryChain(plan.workspace, relative); err != nil {
			return err
		}
	}
	return nil
}
