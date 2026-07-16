package integration

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
)

type EjectHostProjectionReceipt struct {
	Host                assets.Host
	Revision            string
	RemovedFiles        int
	RegistrationRemoved bool
	Replayed            bool
}

// VerifyHostProjectionAbsent proves that one Host has no remaining managed
// projection. It accepts unrelated Host configuration, but rejects any
// ownership journal, frozen managed file, exact registration, or registration
// that retains the managed command or status. The proof is strictly read-only.
func VerifyHostProjectionAbsent(workspace, nodeState string, host assets.Host,
	bundle assets.Bundle,
) error {
	plan, err := prepareProjection(workspace, nodeState, host, bundle)
	if err != nil {
		return err
	}
	return verifyHostProjectionAbsent(plan)
}

// EjectHostProjection removes one fully authenticated managed projection. It
// never removes the immutable Node bundle or shared Host configuration. Any
// user/content drift fails before deletion; missing managed entries are safe
// replay state left by an interrupted earlier eject.
func EjectHostProjection(workspace, nodeState string, host assets.Host,
	bundle assets.Bundle,
) (EjectHostProjectionReceipt, error) {
	plan, err := prepareProjection(workspace, nodeState, host, bundle)
	if err != nil {
		return EjectHostProjectionReceipt{}, err
	}
	receipt := EjectHostProjectionReceipt{Host: host, Revision: bundle.Manifest().AssetRevision}
	if _, err := os.Lstat(plan.ownershipPath); errors.Is(err, os.ErrNotExist) {
		if err := verifyHostProjectionAbsent(plan); err != nil {
			return EjectHostProjectionReceipt{}, err
		}
		receipt.Replayed = true
		return receipt, nil
	} else if err != nil {
		return EjectHostProjectionReceipt{}, projectionConflict("inspect ownership journal", err)
	}

	ownership, err := readProjectionOwnership(plan)
	if err != nil {
		return EjectHostProjectionReceipt{}, err
	}
	files, registration, revision, err := ejectRecords(plan, ownership)
	if err != nil {
		return EjectHostProjectionReceipt{}, err
	}
	receipt.Revision = revision
	fileStates, err := preflightEjectFiles(plan, files)
	if err != nil {
		return EjectHostProjectionReceipt{}, err
	}
	config, configSnapshot, registrationIndex, registrationPresent, err := preflightEjectRegistration(plan, registration)
	if err != nil {
		return EjectHostProjectionReceipt{}, err
	}

	if registrationPresent {
		hooks := config["hooks"].(map[string]any)
		entries := hooks[plan.registration.Value.Event].([]any)
		updated := append([]any(nil), entries[:registrationIndex]...)
		updated = append(updated, entries[registrationIndex+1:]...)
		if len(updated) == 0 {
			delete(hooks, plan.registration.Value.Event)
		} else {
			hooks[plan.registration.Value.Event] = updated
		}
		if err := replaceSharedJSON(filepath.Dir(plan.ownershipPath), plan.configPath,
			configSnapshot, config); err != nil {
			return EjectHostProjectionReceipt{}, err
		}
		receipt.RegistrationRemoved = true
	}
	for index, file := range plan.files {
		if !fileStates[file.record.Path] {
			continue
		}
		record := files[index]
		mode, _ := projectionRecordMode(record.Mode)
		if err := removeExactProjectedFile(file.path, mode, record.InstalledDigest); err != nil {
			return EjectHostProjectionReceipt{}, err
		}
		receipt.RemovedFiles++
	}
	if err := verifyEjectedProjection(plan, registration); err != nil {
		return EjectHostProjectionReceipt{}, err
	}
	if err := removeExactOwnership(plan.ownershipPath, ownership.journalBytes); err != nil {
		return EjectHostProjectionReceipt{}, err
	}
	return receipt, nil
}

func verifyHostProjectionAbsent(plan projectionPlan) error {
	if err := validateExistingProjectionParents(plan); err != nil {
		return err
	}
	relative, err := filepath.Rel(plan.workspace, filepath.Dir(plan.ownershipPath))
	if err != nil {
		return projectionConflict("resolve ownership journal directory", err)
	}
	if err := requireExistingOwnedDirectoryChain(plan.workspace, relative); err != nil {
		return err
	}
	if _, err := os.Lstat(plan.ownershipPath); err == nil {
		return projectionConflict("managed Host ownership journal remains", nil)
	} else if !errors.Is(err, os.ErrNotExist) {
		return projectionConflict("inspect ownership journal", err)
	}
	_, _, err = inspectFreshProjection(plan)
	return err
}

func ejectRecords(plan projectionPlan, ownership projectionOwnership) ([]ownershipFile,
	ownershipRegistration, string, error,
) {
	switch ownership.state {
	case projectionApplied:
		return append([]ownershipFile(nil), plan.applied.Files...), plan.applied.Registrations[0],
			plan.applied.AssetRevision, nil
	case projectionPrevious:
		if ownership.previous == nil {
			break
		}
		return append([]ownershipFile(nil), ownership.previous.Files...),
			ownership.previous.Registrations[0], ownership.previous.AssetRevision, nil
	default:
		return nil, ownershipRegistration{}, "",
			projectionConflict("install or upgrade must converge before eject", nil)
	}
	return nil, ownershipRegistration{}, "", projectionConflict("ownership journal cannot be ejected", nil)
}

func preflightEjectFiles(plan projectionPlan, records []ownershipFile) (map[string]bool, error) {
	if len(records) != len(plan.files) {
		return nil, projectionConflict("eject ownership has the wrong file count", nil)
	}
	states := make(map[string]bool, len(records))
	for index, file := range plan.files {
		record := records[index]
		if record.Path != file.record.Path || record.Source != file.record.Source {
			return nil, projectionConflict("eject ownership escapes frozen managed file paths", nil)
		}
		mode, err := projectionRecordMode(record.Mode)
		if err != nil {
			return nil, err
		}
		content, _, err := readSafeRegular(file.path, mode, maxHostConfigBytes)
		if errors.Is(err, os.ErrNotExist) {
			states[record.Path] = false
			continue
		}
		if err != nil || digest(content) != record.InstalledDigest {
			return nil, projectionConflict("managed Host file differs from eject ownership", err)
		}
		states[record.Path] = true
	}
	return states, nil
}

func preflightEjectRegistration(plan projectionPlan, record ownershipRegistration) (map[string]any,
	fileSnapshot, int, bool, error,
) {
	if record.Path != plan.applied.Registrations[0].Path ||
		record.ManagedKey != plan.applied.Registrations[0].ManagedKey ||
		!validProjectionDigest(record.InstalledDigest) {
		return nil, fileSnapshot{}, 0, false,
			projectionConflict("eject ownership escapes the frozen managed registration", nil)
	}
	document, snapshot, err := readSharedJSON(plan.configPath)
	if err != nil {
		return nil, fileSnapshot{}, 0, false, err
	}
	entries, err := registrationEntries(document, plan.registration.Value.Event, false)
	if err != nil {
		return nil, fileSnapshot{}, 0, false, err
	}
	digestMatches, commandMatches, index, err := registrationDigestAndCommandCounts(entries,
		record.InstalledDigest, plan.entry.Hooks[0].Command)
	if err != nil {
		return nil, fileSnapshot{}, 0, false, err
	}
	statusMatches := managedStatusCount(entries, plan.entry.Hooks[0].StatusMessage)
	if digestMatches == 0 && commandMatches == 0 && statusMatches == 0 {
		return document, snapshot, 0, false, nil
	}
	if digestMatches != 1 || commandMatches != 1 || index < 0 ||
		statusMatches != 1 ||
		!entryUsesExactCommand(entries[index], plan.entry.Hooks[0].Command) {
		return nil, fileSnapshot{}, 0, false,
			projectionConflict("managed Host registration differs from eject ownership", nil)
	}
	return document, snapshot, index, true, nil
}

func projectionRecordMode(value string) (os.FileMode, error) {
	parsed, err := strconv.ParseUint(value, 8, 32)
	mode := os.FileMode(parsed)
	if err != nil || (mode != 0o644 && mode != 0o755) || fmt.Sprintf("%04o", mode) != value {
		return 0, projectionConflict("managed Host file ownership has an invalid mode", err)
	}
	return mode, nil
}

func removeExactProjectedFile(path string, mode os.FileMode, wantedDigest string) error {
	content, before, err := readSafeRegular(path, mode, maxHostConfigBytes)
	if err != nil || digest(content) != wantedDigest {
		return projectionConflict("managed Host file changed before eject", err)
	}
	after, err := os.Lstat(path)
	if err != nil || !os.SameFile(before, after) {
		return projectionConflict("managed Host file identity changed before eject", err)
	}
	if err := os.Remove(path); err != nil {
		return projectionConflict("remove managed Host file", err)
	}
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		return projectionConflict("persist managed Host file removal", err)
	}
	return nil
}

func verifyEjectedProjection(plan projectionPlan, registration ownershipRegistration) error {
	for _, file := range plan.files {
		if _, err := os.Lstat(file.path); !errors.Is(err, os.ErrNotExist) {
			return projectionConflict("managed Host file remains after eject", err)
		}
	}
	document, _, err := readSharedJSON(plan.configPath)
	if err != nil {
		return err
	}
	entries, err := registrationEntries(document, plan.registration.Value.Event, false)
	if err != nil {
		return err
	}
	digests, commands, _, err := registrationDigestAndCommandCounts(entries,
		registration.InstalledDigest, plan.entry.Hooks[0].Command)
	statuses := managedStatusCount(entries, plan.entry.Hooks[0].StatusMessage)
	if err != nil || digests != 0 || commands != 0 || statuses != 0 {
		return projectionConflict("managed Host registration remains after eject", err)
	}
	return nil
}

func removeExactOwnership(path string, expected []byte) error {
	content, before, err := readSafeRegular(path, 0o600, 64<<10)
	if err != nil || !bytes.Equal(content, expected) {
		return projectionConflict("ownership journal changed before eject completion", err)
	}
	after, err := os.Lstat(path)
	if err != nil || !os.SameFile(before, after) {
		return projectionConflict("ownership journal identity changed before eject completion", err)
	}
	if err := os.Remove(path); err != nil {
		return projectionConflict("remove ownership journal", err)
	}
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		return projectionConflict("persist ownership journal removal", err)
	}
	return nil
}
