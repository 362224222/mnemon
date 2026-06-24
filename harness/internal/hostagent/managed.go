package hostagent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
)

// managedState tracks the no-clobber projection of one host's managed definition files: the hashes we
// last wrote (prior, loaded from the host manifest), the hashes we write this pass (next, persisted
// back), the user-modified / pre-existing files we preserved this pass (conflicts), and the set of
// paths a prior pass recorded as preserved (preservedPrior) so uninstall does not delete them as
// generated residue.
type managedState struct {
	prior          map[string]string
	next           map[string]string
	conflicts      []string
	preservedPrior map[string]bool
}

func newManagedState() *managedState {
	return &managedState{prior: map[string]string{}, next: map[string]string{}, preservedPrior: map[string]bool{}}
}

// beginManaged resets the per-loop managed hashes and loads the prior recorded hashes for loopName
// from the existing host manifest (absent manifest -> no prior).
func (c projectorCore) beginManaged(loopName string) {
	c.managed.prior = map[string]string{}
	c.managed.next = map[string]string{}
	c.managed.preservedPrior = map[string]bool{}
	data, err := os.ReadFile(c.resolve(c.hostManifestPath()))
	if err != nil {
		return
	}
	var m hostProjectionManifest
	if json.Unmarshal(data, &m) != nil {
		return
	}
	lp, ok := m.Loops[loopName]
	if !ok {
		return
	}
	// A prior pass recorded these as preserved (a user/pre-existing file we declined to write); carry
	// them forward so uninstall preserves them rather than deleting them as generated residue.
	for _, p := range lp.Ownership.Preserved {
		c.managed.preservedPrior[p] = true
	}
	// Trust recorded hashes only when the marker scheme matches. A future scheme change leaves prior
	// empty -> classifyManaged preserves (never clobbers) on install and removeManaged* preserve on
	// uninstall: fail safe toward keeping the user's files, never toward deleting them.
	if lp.Ownership.MarkerVersion == managedMarkerVersion && lp.Ownership.Hashes != nil {
		c.managed.prior = lp.Ownership.Hashes
	}
}

// projectManagedBytes writes already-rendered static shim content under the no-clobber policy.
func (c projectorCore) projectManagedBytes(desired []byte, dstDisplay string, mode os.FileMode) error {
	dst := c.resolve(dstDisplay)
	if classifyManaged(dst, desired, c.managed.prior[dstDisplay]) == classConflict {
		c.managed.conflicts = append(c.managed.conflicts, dstDisplay)
		if c.dryRun {
			c.printf("would preserve user-modified %s\n", dstDisplay)
		} else {
			c.printf("preserved user-modified %s\n", dstDisplay)
		}
		return nil
	}
	if err := c.writeFile(dstDisplay, desired, mode); err != nil {
		return err
	}
	c.managed.next[dstDisplay] = hashBytes(desired)
	return nil
}

// removeManagedTree removes a Mnemon-owned static shim directory safely on uninstall: each recorded
// managed file is removed only if its on-disk hash still matches what we wrote, and a user-edited one
// is preserved + reported. The directory itself is removed only once empty, so a preserved edit keeps
// its directory. Call beginManaged(loop) first to load the recorded hashes.
func (c projectorCore) removeManagedTree(dirDisplay string) error {
	abs := c.resolve(dirDisplay)
	entries, err := os.ReadDir(abs)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		childDisplay := pathJoin(dirDisplay, e.Name())
		if e.IsDir() {
			if err := c.removeManagedTree(childDisplay); err != nil {
				return err
			}
			continue
		}
		// A path a prior pass recorded as preserved (a user/pre-existing file we never wrote) is not ours
		// to delete, even though it has no recorded hash.
		if c.managed.preservedPrior[childDisplay] {
			c.managed.conflicts = append(c.managed.conflicts, childDisplay)
			c.printf("preserved %s\n", childDisplay)
			continue
		}
		if hash, ok := c.managed.prior[childDisplay]; ok {
			current, err := os.ReadFile(c.resolve(childDisplay))
			if err != nil {
				return err
			}
			if hashBytes(current) != hash {
				c.managed.conflicts = append(c.managed.conflicts, childDisplay)
				c.printf("preserved user-modified %s\n", childDisplay)
				continue
			}
		}
		if err := os.Remove(c.resolve(childDisplay)); err != nil {
			return err
		}
	}
	if remaining, err := os.ReadDir(abs); err == nil && len(remaining) == 0 {
		return os.Remove(abs)
	}
	return nil
}

// Report is the outcome of a host shim install/uninstall: the managed files preserved because the
// user edited them.
type Report struct {
	Conflicts []string
}

// managedClass is the no-clobber decision for one managed definition file.
type managedClass int

const (
	classWrite    managedClass = iota // safe to (over)write: absent, equals desired, or ours-unmodified
	classConflict                     // preserve: the user edited a managed file, or a pre-existing unknown file
)

// managedMarkerVersion stamps the ownership-hash scheme so a future host shim can detect an older
// marker layout and re-adopt rather than mis-preserve.
const managedMarkerVersion = 1

// classifyManaged decides whether a managed definition file at dst may be written with desired
// content, given the hash we last recorded for it (prior, empty if none). We NEVER overwrite a file we
// did not write — on install or reinstall:
//
//   - absent on disk                               -> classWrite (nothing to clobber)
//   - on-disk content already equals desired       -> classWrite (idempotent; re-install is safe)
//   - prior recorded AND on-disk matches prior      -> classWrite (still ours; safe to update)
//   - prior recorded AND on-disk differs from prior -> classConflict (user edited a managed file)
//   - no prior AND on-disk differs from desired     -> classConflict (a pre-existing unknown file —
//     the user's own — never clobbered, not even on the first install)
func classifyManaged(dst string, desired []byte, prior string) managedClass {
	current, err := os.ReadFile(dst)
	if err != nil {
		return classWrite
	}
	currentHash := hashBytes(current)
	if currentHash == hashBytes(desired) {
		return classWrite
	}
	if prior != "" && currentHash == prior {
		return classWrite
	}
	if prior == "" && knownLegacyManagedHashes[currentHash] {
		return classWrite // pre-ownership workspace holding our own legacy bytes: adopt and upgrade
	}
	return classConflict
}

func hashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
