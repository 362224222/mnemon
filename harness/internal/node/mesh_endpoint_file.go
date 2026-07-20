package node

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"golang.org/x/sys/unix"
)

const (
	meshEndpointPendingName = "mesh-endpoint.pending.json"
	meshEndpointName        = "mesh-endpoint.json"
	meshEndpointFileMode    = os.FileMode(0o600)
	meshEndpointTempTries   = 32
)

func publishMeshEndpointPending(nodeState string, pending meshEndpointPending) (bool, error) {
	if !validMeshEndpointValue(pending.value, false) {
		return false, meshEndpointError("publish pending", errors.New("pending value is invalid"))
	}
	return mutateMeshEndpoint(nodeState, pending.value.peerID, func(state *identityNodeState,
		current meshEndpointState,
	) (bool, error) {
		if current.kind == meshEndpointStatePending &&
			bytes.Equal(current.pending.value.canonical, pending.value.canonical) {
			return false, nil
		}
		if current.kind != meshEndpointStateAbsent {
			return false, meshEndpointError("publish pending", errMeshEndpointConflict)
		}
		return meshEndpointPublishOutcome(
			publishMeshEndpointFile(state, meshEndpointPendingName, pending.value.canonical, false))
	})
}

func publishMeshEndpointFinal(nodeState string, expected meshEndpointPending,
	endpoint meshEndpoint,
) (bool, error) {
	if !validMeshEndpointValue(expected.value, false) || !validMeshEndpointValue(endpoint.value, true) ||
		!meshEndpointAdvances(expected.value, endpoint.value) {
		return false, meshEndpointError("publish final", errors.New("endpoint transition is invalid"))
	}
	return mutateMeshEndpoint(nodeState, endpoint.value.peerID, func(state *identityNodeState,
		current meshEndpointState,
	) (bool, error) {
		if final, ok := current.finalAuthority(); ok && bytes.Equal(final.value.canonical, endpoint.value.canonical) {
			if pending, present := current.pendingAuthority(); present &&
				!bytes.Equal(pending.value.canonical, expected.value.canonical) {
				return false, meshEndpointError("publish final", errMeshEndpointConflict)
			}
			return false, nil
		}
		pending, ok := current.pendingAuthority()
		if !ok || current.kind != meshEndpointStatePending ||
			!bytes.Equal(pending.value.canonical, expected.value.canonical) {
			return false, meshEndpointError("publish final", errMeshEndpointConflict)
		}
		return meshEndpointPublishOutcome(
			publishMeshEndpointFile(state, meshEndpointName, endpoint.value.canonical, true))
	})
}

func retireMeshEndpointPending(nodeState string, expected meshEndpointPending,
	endpoint meshEndpoint,
) error {
	if !validMeshEndpointValue(expected.value, false) || !validMeshEndpointValue(endpoint.value, true) ||
		!meshEndpointAdvances(expected.value, endpoint.value) {
		return meshEndpointError("retire pending", errors.New("expected authority is invalid"))
	}
	_, err := mutateMeshEndpoint(nodeState, endpoint.value.peerID, func(state *identityNodeState,
		current meshEndpointState,
	) (bool, error) {
		final, finalOK := current.finalAuthority()
		if !finalOK || !bytes.Equal(final.value.canonical, endpoint.value.canonical) {
			return false, meshEndpointError("retire pending", errMeshEndpointConflict)
		}
		pending, pendingOK := current.pendingAuthority()
		if !pendingOK {
			return false, nil
		}
		if !bytes.Equal(pending.value.canonical, expected.value.canonical) {
			return false, meshEndpointError("retire pending", errMeshEndpointConflict)
		}
		if err := state.root.Remove(meshEndpointPendingName); err != nil {
			return false, meshEndpointError("retire pending", err)
		}
		return true, meshEndpointSync(state, "persist pending retirement")
	})
	return err
}

// mutateMeshEndpoint accepts only the fixed synchronous call sites in this file;
// mutation runs while the Node-state flock is held. It may perform only bounded
// same-directory filesystem work and must not reenter or call Store, peer,
// channel, or other blocking work.
func mutateMeshEndpoint(nodeState string, expected model.PeerID,
	mutation func(*identityNodeState, meshEndpointState) (bool, error),
) (bool, error) {
	state, err := openIdentityNodeState(nodeState)
	if err != nil {
		return false, meshEndpointError("open Node state", err)
	}
	defer state.close()
	if err := state.lock(); err != nil {
		return false, meshEndpointError("lock Node state", err)
	}
	defer state.unlock()
	current, err := inspectMeshEndpointStateLocked(state, expected)
	if err != nil {
		return false, err
	}
	return mutation(state, current)
}

func readMeshEndpointFile(state *identityNodeState, name string,
	final bool,
) (meshEndpointValue, bool, error) {
	if err := state.validateLive(); err != nil {
		return meshEndpointValue{}, false, meshEndpointError("read "+name, err)
	}
	before, err := state.root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return meshEndpointValue{}, false, nil
	}
	if err != nil {
		return meshEndpointValue{}, false, meshEndpointError("inspect "+name, err)
	}
	if validateErr := validateMeshEndpointFile(state, before); validateErr != nil {
		return meshEndpointValue{}, false, meshEndpointError("inspect "+name, errors.Join(err, validateErr))
	}
	raw, err := readMeshEndpointBytes(state, name, before)
	if err != nil {
		return meshEndpointValue{}, false, err
	}
	value, err := decodeMeshEndpointValue(raw, final)
	if err != nil {
		return meshEndpointValue{}, false, meshEndpointError("decode "+name, err)
	}
	return value, true, nil
}

func readMeshEndpointBytes(state *identityNodeState, name string, before os.FileInfo) ([]byte, error) {
	file, err := state.root.Open(name)
	if err != nil {
		return nil, meshEndpointError("open "+name, err)
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, maxMeshEndpointBytes+1))
	opened, statErr := file.Stat()
	if err := validateMeshEndpointRead(state, before, opened, raw, readErr, statErr); err != nil {
		_ = file.Close()
		return nil, meshEndpointError("read "+name, err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, meshEndpointError("verify "+name, err)
	}
	confirmed, confirmErr := io.ReadAll(io.LimitReader(file, maxMeshEndpointBytes+1))
	afterOpen, afterStatErr := file.Stat()
	closeErr := file.Close()
	after, pathErr := state.root.Lstat(name)
	if err := validateMeshEndpointConfirmation(state, before, afterOpen, after, raw, confirmed,
		confirmErr, afterStatErr, closeErr, pathErr); err != nil {
		return nil, meshEndpointError("verify "+name, err)
	}
	return raw, nil
}

func validateMeshEndpointRead(state *identityNodeState, before, opened os.FileInfo, raw []byte,
	readErr, statErr error,
) error {
	if readErr != nil || statErr != nil {
		return errors.Join(readErr, statErr)
	}
	if !os.SameFile(before, opened) || validateMeshEndpointFile(state, opened) != nil {
		return errors.New("endpoint file identity or mode changed")
	}
	if len(raw) == 0 || int64(len(raw)) > maxMeshEndpointBytes || int64(len(raw)) != opened.Size() {
		return errors.New("endpoint file size is invalid")
	}
	return nil
}

func validateMeshEndpointConfirmation(state *identityNodeState, before, opened, after os.FileInfo,
	raw, confirmed []byte, errs ...error,
) error {
	if err := errors.Join(errs...); err != nil {
		return err
	}
	if !bytes.Equal(raw, confirmed) || !os.SameFile(before, opened) || !os.SameFile(before, after) {
		return errors.New("endpoint file changed while reading")
	}
	if validateMeshEndpointFile(state, opened) != nil || validateMeshEndpointFile(state, after) != nil {
		return errors.New("endpoint file mode or links changed")
	}
	return state.validateLive()
}

func validateMeshEndpointFile(state *identityNodeState, info os.FileInfo) error {
	return validateMeshEndpointFileLinks(state, info, 1)
}

func validateMeshEndpointFileLinks(state *identityNodeState, info os.FileInfo, links uint64) error {
	owner, err := validateIdentityOwnerPath(info, meshEndpointFileMode, false)
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if owner != state.ownerUID || !ok || uint64(stat.Nlink) != links {
		if err == nil {
			err = errors.New("endpoint file owner or link count is invalid")
		}
		return err
	}
	return nil
}

func publishMeshEndpointFile(state *identityNodeState, name string, raw []byte, final bool) error {
	stageName, stage, err := createMeshEndpointStage(state)
	if err != nil {
		return err
	}
	stagePresent := true
	defer func() {
		_ = stage.Close()
		if stagePresent {
			_ = state.root.Remove(stageName)
		}
	}()
	if err := writeMeshEndpointBytes(stage, raw); err != nil {
		return meshEndpointError("write staged endpoint", err)
	}
	if err := stage.Sync(); err != nil {
		return meshEndpointError("sync staged endpoint", err)
	}
	staged, err := stage.Stat()
	liveStage, liveErr := state.root.Lstat(stageName)
	if err != nil || liveErr != nil || !os.SameFile(staged, liveStage) ||
		validateMeshEndpointFile(state, staged) != nil || validateMeshEndpointFile(state, liveStage) != nil ||
		staged.Size() != int64(len(raw)) || state.validateLive() != nil {
		return meshEndpointError("verify staged endpoint", errors.Join(err, liveErr))
	}
	dirFD := int(state.dir.Fd())
	if err := unix.Linkat(dirFD, stageName, dirFD, name, 0); err != nil {
		if errors.Is(err, syscall.EEXIST) {
			return meshEndpointError("publish "+name, errMeshEndpointConflict)
		}
		return meshEndpointError("publish "+name, err)
	}
	if err := meshEndpointSync(state, "persist "+name); err != nil {
		return err
	}
	if err := state.root.Remove(stageName); err != nil {
		return meshEndpointError("remove staged endpoint", err)
	}
	stagePresent = false
	if err := meshEndpointSync(state, "persist staged endpoint cleanup"); err != nil {
		return err
	}
	confirmed, present, err := readMeshEndpointFile(state, name, final)
	if err != nil || !present || !bytes.Equal(confirmed.canonical, raw) {
		return meshEndpointError("confirm "+name, errors.Join(err, errors.New("published endpoint differs")))
	}
	return nil
}

func createMeshEndpointStage(state *identityNodeState) (string, *os.File, error) {
	for range meshEndpointTempTries {
		var random [16]byte
		if _, err := io.ReadFull(rand.Reader, random[:]); err != nil {
			return "", nil, meshEndpointError("allocate staged endpoint", err)
		}
		name := ".mesh-endpoint-" + hex.EncodeToString(random[:]) + ".tmp"
		file, err := state.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, meshEndpointFileMode)
		if err == nil {
			if err := file.Chmod(meshEndpointFileMode); err != nil {
				_ = file.Close()
				_ = state.root.Remove(name)
				return "", nil, meshEndpointError("secure staged endpoint", err)
			}
			return name, file, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return "", nil, meshEndpointError("create staged endpoint", err)
		}
	}
	return "", nil, meshEndpointError("create staged endpoint", errors.New("temporary namespace exhausted"))
}

func cleanupMeshEndpointStages(state *identityNodeState) error {
	directory, err := state.root.Open(".")
	if err != nil {
		return meshEndpointError("scan staged endpoints", err)
	}
	entries, readErr := directory.ReadDir(-1)
	closeErr := directory.Close()
	if readErr != nil || closeErr != nil {
		return meshEndpointError("scan staged endpoints", errors.Join(readErr, closeErr))
	}
	removed := false
	for _, entry := range entries {
		if !isMeshEndpointStageName(entry.Name()) {
			continue
		}
		info, err := state.root.Lstat(entry.Name())
		if err != nil || validateMeshEndpointStage(state, info) != nil {
			return meshEndpointError("inspect staged endpoint", errors.Join(err, errors.New("unsafe staged endpoint")))
		}
		if err := state.root.Remove(entry.Name()); err != nil {
			return meshEndpointError("remove staged endpoint", err)
		}
		removed = true
	}
	if removed {
		return meshEndpointSync(state, "persist staged endpoint cleanup")
	}
	return state.validateLive()
}

func validateMeshEndpointStage(state *identityNodeState, stage os.FileInfo) error {
	stat, ok := stage.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink < 1 || stat.Nlink > 2 {
		return errors.New("staged endpoint link count is invalid")
	}
	if err := validateMeshEndpointFileLinks(state, stage, uint64(stat.Nlink)); err != nil || stat.Nlink == 1 {
		return err
	}
	linked := false
	for _, name := range []string{meshEndpointPendingName, meshEndpointName} {
		live, err := state.root.Lstat(name)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if os.SameFile(stage, live) {
			if linked || validateMeshEndpointFileLinks(state, live, 2) != nil {
				return errors.New("staged endpoint has ambiguous live authority")
			}
			linked = true
		}
	}
	if !linked {
		return errors.New("staged endpoint is linked outside its authority path")
	}
	return nil
}

func isMeshEndpointStageName(name string) bool {
	const prefix, suffix = ".mesh-endpoint-", ".tmp"
	if len(name) != len(prefix)+32+len(suffix) || name[:len(prefix)] != prefix ||
		name[len(name)-len(suffix):] != suffix {
		return false
	}
	value := name[len(prefix) : len(name)-len(suffix)]
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 16 && hex.EncodeToString(decoded) == value
}

func writeMeshEndpointBytes(file *os.File, raw []byte) error {
	if len(raw) == 0 || int64(len(raw)) > maxMeshEndpointBytes {
		return meshEndpointError("write endpoint", errors.New("endpoint size is invalid"))
	}
	for len(raw) > 0 {
		written, err := file.Write(raw)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		raw = raw[written:]
	}
	return nil
}

func meshEndpointSync(state *identityNodeState, operation string) error {
	if err := state.validateLive(); err != nil {
		return meshEndpointError(operation, err)
	}
	if err := state.dir.Sync(); err != nil {
		return meshEndpointError(operation, err)
	}
	return state.validateLive()
}

func meshEndpointError(operation string, err error) error {
	if err == nil {
		err = errors.New("operation failed")
	}
	return fmt.Errorf("%w: %s: %w", errMeshEndpointAuthority, operation, err)
}
