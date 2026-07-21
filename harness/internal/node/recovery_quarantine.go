package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

var ErrRecoveryQuarantine = errors.New("quarantine mnemond Node")

type RecoveryQuarantineResult struct {
	NodePath  string
	PeerID    string
	RenamedAt time.Time
}

type recoveryQuarantinePlan struct {
	destination  string
	peerID       string
	recoveryRoot string
	renamedAt    time.Time
	targetRoot   string
}

// Quarantine atomically renames the complete Node tree after Quiesce proved
// the exact durable authority offline. No state is interpreted, migrated, or
// deleted; the setup and ensure descriptors remain held across the rename.
func (lease *DaemonLifecycleLease) Quarantine(ctx context.Context,
	expected AuthorityResponse, at time.Time,
) (RecoveryQuarantineResult, error) {
	if lease == nil {
		return RecoveryQuarantineResult{}, recoveryQuarantineError("prepare",
			errors.New("lifecycle lease is unavailable"))
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	plan, err := lease.prepareRecoveryQuarantine(ctx, expected, at)
	if err != nil {
		return RecoveryQuarantineResult{}, err
	}
	return lease.executeRecoveryQuarantine(ctx, plan)
}

func (lease *DaemonLifecycleLease) prepareRecoveryQuarantine(ctx context.Context,
	expected AuthorityResponse, at time.Time,
) (recoveryQuarantinePlan, error) {
	snapshot, renamedAt, err := lease.validateRecoveryQuarantine(ctx, expected, at)
	if err != nil {
		return recoveryQuarantinePlan{}, err
	}
	if err := lease.confirmControlSocketAbsent(); err != nil {
		return recoveryQuarantinePlan{}, recoveryQuarantineError("confirm control socket", err)
	}
	if lease.lock.state == nil || lease.lock.state.dir == nil {
		return recoveryQuarantinePlan{}, recoveryQuarantineError("confirm lease",
			errors.New("pinned Node directory is unavailable"))
	}
	if err := lease.lock.state.dir.Sync(); err != nil {
		return recoveryQuarantinePlan{}, recoveryQuarantineError("persist source Node", err)
	}
	return lease.prepareRecoveryQuarantineTarget(snapshot.PeerID.String(), renamedAt)
}

func (lease *DaemonLifecycleLease) validateRecoveryQuarantine(ctx context.Context,
	expected AuthorityResponse, at time.Time,
) (AuthoritySnapshot, time.Time, error) {
	if ctx == nil {
		return AuthoritySnapshot{}, time.Time{}, recoveryQuarantineError("prepare",
			errors.New("context is unavailable"))
	}
	if err := ctx.Err(); err != nil {
		return AuthoritySnapshot{}, time.Time{}, recoveryQuarantineError("prepare", err)
	}
	if !lease.recovery || lease.quarantined || lease.quiesced == nil || *lease.quiesced != expected {
		return AuthoritySnapshot{}, time.Time{}, recoveryQuarantineError("prepare",
			errors.New("exact recovery quiescence was not proved"))
	}
	if err := lease.validateHeld(); err != nil {
		return AuthoritySnapshot{}, time.Time{}, recoveryQuarantineError("confirm lease", err)
	}
	snapshot, err := authorityResponseSnapshot(expected)
	if err != nil || snapshot.PeerID != lease.peerID {
		if err == nil {
			err = errors.New("recovery authority belongs to another Node")
		}
		return AuthoritySnapshot{}, time.Time{}, recoveryQuarantineError("confirm authority", err)
	}
	renamedAt, ok := canonicalAuthorityTime(at)
	if !ok {
		return AuthoritySnapshot{}, time.Time{}, recoveryQuarantineError("prepare",
			errors.New("rename time is invalid"))
	}
	return snapshot, renamedAt, nil
}

func (lease *DaemonLifecycleLease) prepareRecoveryQuarantineTarget(peerID string,
	renamedAt time.Time,
) (recoveryQuarantinePlan, error) {
	recoveryRoot := filepath.Join(lease.workspace, ".mnemon", "harness", "recovery")
	if err := ensureProvisionDirectory(recoveryRoot, 0o700, true); err != nil {
		return recoveryQuarantinePlan{}, recoveryQuarantineError("prepare recovery root", err)
	}
	stamp := renamedAt.Format("20060102T150405.000000000Z")
	targetRoot := filepath.Join(recoveryRoot, stamp+"-"+peerID)
	if _, err := os.Lstat(targetRoot); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			err = errors.New("recovery target already exists")
		}
		return recoveryQuarantinePlan{}, recoveryQuarantineError("prepare recovery target", err)
	}
	if err := ensureProvisionDirectory(targetRoot, 0o700, true); err != nil {
		return recoveryQuarantinePlan{}, recoveryQuarantineError("prepare recovery target", err)
	}
	return recoveryQuarantinePlan{destination: filepath.Join(targetRoot, "node"), peerID: peerID,
		recoveryRoot: recoveryRoot, renamedAt: renamedAt, targetRoot: targetRoot}, nil
}

func (lease *DaemonLifecycleLease) executeRecoveryQuarantine(ctx context.Context,
	plan recoveryQuarantinePlan,
) (RecoveryQuarantineResult, error) {
	if err := ctx.Err(); err != nil {
		return RecoveryQuarantineResult{}, errors.Join(recoveryQuarantineError("rename", err),
			removeEmptyRecoveryTarget(plan.targetRoot))
	}
	if err := lease.validateHeld(); err != nil {
		return RecoveryQuarantineResult{}, errors.Join(recoveryQuarantineError("confirm rename fence", err),
			removeEmptyRecoveryTarget(plan.targetRoot))
	}
	if err := os.Rename(lease.nodeState, plan.destination); err != nil {
		return RecoveryQuarantineResult{}, errors.Join(recoveryQuarantineError("rename", err),
			removeEmptyRecoveryTarget(plan.targetRoot))
	}
	lease.quarantined = true
	result := RecoveryQuarantineResult{NodePath: plan.destination,
		PeerID: plan.peerID, RenamedAt: plan.renamedAt}
	if err := confirmRenamedNode(lease, plan.destination, plan.targetRoot, plan.recoveryRoot); err != nil {
		return result, recoveryQuarantineError("persist renamed Node", err)
	}
	return result, nil
}

func confirmRenamedNode(lease *DaemonLifecycleLease, destination, targetRoot,
	recoveryRoot string,
) error {
	moved, err := os.Lstat(destination)
	if err != nil || lease == nil || lease.lock == nil || lease.lock.state == nil ||
		!os.SameFile(lease.lock.state.identity, moved) {
		if err == nil {
			err = errors.New("renamed Node identity differs from pinned source")
		}
		return err
	}
	if _, err := validateIdentityOwnerPath(moved, identityDirectoryMode, true); err != nil {
		return err
	}
	for _, path := range []string{targetRoot, recoveryRoot,
		filepath.Join(lease.workspace, ".mnemon", "harness")} {
		directory, err := os.Open(path)
		if err != nil {
			return err
		}
		syncErr := directory.Sync()
		closeErr := directory.Close()
		if syncErr != nil || closeErr != nil {
			return errors.Join(syncErr, closeErr)
		}
	}
	return nil
}

func removeEmptyRecoveryTarget(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return recoveryQuarantineError("remove unused recovery target", err)
	}
	return nil
}

func recoveryQuarantineError(operation string, err error) error {
	if err == nil {
		err = errors.New("unknown recovery quarantine failure")
	}
	return fmt.Errorf("%w: %s: %w", ErrRecoveryQuarantine, operation, err)
}
