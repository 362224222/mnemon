package localapi

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"golang.org/x/sys/unix"
)

const (
	profileCredentialDirectory    = "profiles"
	profileCredentialTempPrefix   = ".teamwork-profile-credential-"
	profileCredentialTempSuffix   = ".tmp"
	profileCredentialTempAttempts = 32
)

var profileCredentialProcessMu sync.Mutex

// EnsureProfileCredential loads the fixed Teamwork Profile credential or
// atomically publishes a freshly generated one. The raw capability never
// leaves this package: callers receive only the digest bound into durable
// Profile authority.
//
// Publication is no-clobber. Concurrent setup processes converge on the first
// fully persisted credential, while an existing malformed or unsafe path is
// reported and preserved for operator inspection.
func EnsureProfileCredential(nodeState string) (model.Digest, bool, error) {
	state, err := openProfileCredentialState(nodeState)
	if err != nil {
		return model.Digest{}, false, err
	}
	defer state.close()
	if err := state.lock(); err != nil {
		return model.Digest{}, false, err
	}
	defer state.unlock()
	if err := state.openProfiles(); err != nil {
		return model.Digest{}, false, err
	}
	if err := state.cleanupStaging(); err != nil {
		return model.Digest{}, false, err
	}

	digest, err := state.load()
	if err == nil {
		return digest, false, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return model.Digest{}, false, err
	}
	return state.create()
}

type profileCredentialState struct {
	nodePath     string
	nodeIdentity os.FileInfo
	nodeRoot     *os.Root
	nodeDir      *os.File
	profiles     os.FileInfo
	profilesRoot *os.Root
	profilesDir  *os.File
	ownerUID     uint32
	locked       bool
}

func openProfileCredentialState(path string) (*profileCredentialState, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, unsafeClientState("Node state path must be absolute and canonical")
	}
	before, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("%w: inspect Node state directory: %w", ErrUnsafeClientState, err)
	}
	ownerUID := uint32(os.Geteuid())
	if err := validateOwnerDirectory(before, ownerUID); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, fmt.Errorf("%w: open Node state Root: %w", ErrUnsafeClientState, err)
	}
	dir, err := os.Open(path)
	if err != nil {
		_ = root.Close()
		return nil, fmt.Errorf("%w: open Node state directory: %w", ErrUnsafeClientState, err)
	}
	fail := func(detail string) (*profileCredentialState, error) {
		_ = dir.Close()
		_ = root.Close()
		return nil, unsafeClientState(detail)
	}
	opened, openedErr := dir.Stat()
	rooted, rootedErr := root.Lstat(".")
	live, liveErr := os.Lstat(path)
	if openedErr != nil || rootedErr != nil || liveErr != nil ||
		!os.SameFile(before, opened) || !os.SameFile(before, rooted) || !os.SameFile(before, live) {
		return fail("Node state directory changed while opening")
	}
	if validateOwnerDirectory(opened, ownerUID) != nil ||
		validateOwnerDirectory(rooted, ownerUID) != nil ||
		validateOwnerDirectory(live, ownerUID) != nil {
		return fail("Node state directory is not owner-only")
	}
	return &profileCredentialState{
		nodePath: path, nodeIdentity: before, nodeRoot: root, nodeDir: dir, ownerUID: ownerUID,
	}, nil
}

func (state *profileCredentialState) close() {
	if state == nil {
		return
	}
	if state.profilesDir != nil {
		_ = state.profilesDir.Close()
	}
	if state.profilesRoot != nil {
		_ = state.profilesRoot.Close()
	}
	if state.nodeDir != nil {
		_ = state.nodeDir.Close()
	}
	if state.nodeRoot != nil {
		_ = state.nodeRoot.Close()
	}
}

func (state *profileCredentialState) lock() error {
	if state == nil || state.nodeDir == nil {
		return unsafeClientState("Node state directory is unavailable")
	}
	profileCredentialProcessMu.Lock()
	if err := unix.Flock(int(state.nodeDir.Fd()), unix.LOCK_EX); err != nil {
		profileCredentialProcessMu.Unlock()
		return fmt.Errorf("lock Profile credential Node state: %w", err)
	}
	if err := state.validateNodeLive(); err != nil {
		_ = unix.Flock(int(state.nodeDir.Fd()), unix.LOCK_UN)
		profileCredentialProcessMu.Unlock()
		return err
	}
	state.locked = true
	return nil
}

func (state *profileCredentialState) unlock() {
	if state != nil && state.nodeDir != nil && state.locked {
		state.locked = false
		_ = unix.Flock(int(state.nodeDir.Fd()), unix.LOCK_UN)
		profileCredentialProcessMu.Unlock()
	}
}

func (state *profileCredentialState) openProfiles() error {
	if err := state.validateNodeLive(); err != nil {
		return err
	}
	info, err := state.nodeRoot.Lstat(profileCredentialDirectory)
	if errors.Is(err, os.ErrNotExist) {
		if err := state.nodeRoot.Mkdir(profileCredentialDirectory, ownerDirectoryMode); err != nil {
			return fmt.Errorf("create owner-only Profile directory: %w", err)
		}
		if err := state.nodeDir.Sync(); err != nil {
			return fmt.Errorf("persist owner-only Profile directory: %w", err)
		}
		info, err = state.nodeRoot.Lstat(profileCredentialDirectory)
	}
	if err != nil {
		return fmt.Errorf("%w: inspect Profile directory: %w", ErrUnsafeClientState, err)
	}
	if err := validateOwnerDirectory(info, state.ownerUID); err != nil {
		return err
	}
	root, err := state.nodeRoot.OpenRoot(profileCredentialDirectory)
	if err != nil {
		return fmt.Errorf("%w: open Profile Root: %w", ErrUnsafeClientState, err)
	}
	dir, err := state.nodeRoot.Open(profileCredentialDirectory)
	if err != nil {
		_ = root.Close()
		return fmt.Errorf("%w: open Profile directory: %w", ErrUnsafeClientState, err)
	}
	fail := func(detail string) error {
		_ = dir.Close()
		_ = root.Close()
		return unsafeClientState(detail)
	}
	opened, openedErr := dir.Stat()
	rooted, rootedErr := root.Lstat(".")
	live, liveErr := state.nodeRoot.Lstat(profileCredentialDirectory)
	if openedErr != nil || rootedErr != nil || liveErr != nil ||
		!os.SameFile(info, opened) || !os.SameFile(info, rooted) || !os.SameFile(info, live) {
		return fail("Profile directory changed while opening")
	}
	if validateOwnerDirectory(opened, state.ownerUID) != nil ||
		validateOwnerDirectory(rooted, state.ownerUID) != nil ||
		validateOwnerDirectory(live, state.ownerUID) != nil {
		return fail("Profile directory is not owner-only")
	}
	state.profiles, state.profilesRoot, state.profilesDir = info, root, dir
	if err := state.validateProfilesLive(); err != nil {
		state.profiles, state.profilesRoot, state.profilesDir = nil, nil, nil
		return fail("Profile directory changed after opening")
	}
	return nil
}

func (state *profileCredentialState) validateNodeLive() error {
	if state == nil || state.nodeIdentity == nil || state.nodeRoot == nil || state.nodeDir == nil {
		return unsafeClientState("Node state identity is unavailable")
	}
	live, liveErr := os.Lstat(state.nodePath)
	rooted, rootedErr := state.nodeRoot.Lstat(".")
	opened, openedErr := state.nodeDir.Stat()
	if liveErr != nil || rootedErr != nil || openedErr != nil ||
		!os.SameFile(state.nodeIdentity, live) || !os.SameFile(state.nodeIdentity, rooted) ||
		!os.SameFile(state.nodeIdentity, opened) {
		return unsafeClientState("Node state directory identity changed")
	}
	if validateOwnerDirectory(live, state.ownerUID) != nil ||
		validateOwnerDirectory(rooted, state.ownerUID) != nil ||
		validateOwnerDirectory(opened, state.ownerUID) != nil {
		return unsafeClientState("Node state directory authority changed")
	}
	return nil
}

func (state *profileCredentialState) validateProfilesLive() error {
	if err := state.validateNodeLive(); err != nil {
		return err
	}
	if state.profiles == nil || state.profilesRoot == nil || state.profilesDir == nil {
		return unsafeClientState("Profile directory identity is unavailable")
	}
	live, liveErr := state.nodeRoot.Lstat(profileCredentialDirectory)
	rooted, rootedErr := state.profilesRoot.Lstat(".")
	opened, openedErr := state.profilesDir.Stat()
	if liveErr != nil || rootedErr != nil || openedErr != nil ||
		!os.SameFile(state.profiles, live) || !os.SameFile(state.profiles, rooted) ||
		!os.SameFile(state.profiles, opened) {
		return unsafeClientState("Profile directory identity changed")
	}
	if validateOwnerDirectory(live, state.ownerUID) != nil ||
		validateOwnerDirectory(rooted, state.ownerUID) != nil ||
		validateOwnerDirectory(opened, state.ownerUID) != nil {
		return unsafeClientState("Profile directory authority changed")
	}
	return nil
}

func (state *profileCredentialState) cleanupStaging() error {
	if err := state.validateProfilesLive(); err != nil {
		return err
	}
	directory, err := state.profilesRoot.Open(".")
	if err != nil {
		return fmt.Errorf("scan staged Profile credentials: %w", err)
	}
	entries, readErr := directory.ReadDir(-1)
	closeErr := directory.Close()
	if readErr != nil {
		return fmt.Errorf("scan staged Profile credentials: %w", readErr)
	}
	if closeErr != nil {
		return fmt.Errorf("scan staged Profile credentials: %w", closeErr)
	}
	removed := false
	for _, entry := range entries {
		if !isProfileCredentialTempName(entry.Name()) {
			continue
		}
		info, err := state.profilesRoot.Lstat(entry.Name())
		if err != nil {
			return fmt.Errorf("%w: inspect staged Profile credential: %w", ErrUnsafeClientState, err)
		}
		if err := validateOwnerRegularFile(info, state.ownerUID); err != nil {
			return unsafeClientState("staged Profile credential is not an owner-only regular file")
		}
		if err := state.profilesRoot.Remove(entry.Name()); err != nil {
			return fmt.Errorf("remove staged Profile credential: %w", err)
		}
		removed = true
	}
	if removed {
		if err := state.profilesDir.Sync(); err != nil {
			return fmt.Errorf("persist staged Profile credential cleanup: %w", err)
		}
	}
	return state.validateProfilesLive()
}

func isProfileCredentialTempName(name string) bool {
	if len(name) != len(profileCredentialTempPrefix)+32+len(profileCredentialTempSuffix) ||
		name[:len(profileCredentialTempPrefix)] != profileCredentialTempPrefix ||
		name[len(name)-len(profileCredentialTempSuffix):] != profileCredentialTempSuffix {
		return false
	}
	encoded := name[len(profileCredentialTempPrefix) : len(name)-len(profileCredentialTempSuffix)]
	decoded, err := hex.DecodeString(encoded)
	return err == nil && len(decoded) == 16 && hex.EncodeToString(decoded) == encoded
}

func (state *profileCredentialState) load() (model.Digest, error) {
	if err := state.validateProfilesLive(); err != nil {
		return model.Digest{}, err
	}
	name := profileCredentialFilename()
	before, err := state.profilesRoot.Lstat(name)
	if err != nil {
		return model.Digest{}, err
	}
	if err := validateOwnerRegularFile(before, state.ownerUID); err != nil {
		return model.Digest{}, err
	}
	file, err := state.profilesRoot.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		return model.Digest{}, fmt.Errorf("%w: open Profile credential", ErrUnsafeClientState)
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(before, opened) || validateOwnerRegularFile(opened, state.ownerUID) != nil {
		return model.Digest{}, unsafeClientState("Profile credential changed while opening")
	}
	if opened.Size() != profileTokenBytes {
		return model.Digest{}, unsafeClientState("Profile credential has noncanonical bytes")
	}
	raw, err := io.ReadAll(io.LimitReader(file, profileTokenBytes+1))
	if err != nil || len(raw) != profileTokenBytes {
		clear(raw)
		return model.Digest{}, unsafeClientState("Profile credential changed while reading")
	}
	defer clear(raw)
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return model.Digest{}, unsafeClientState("Profile credential cannot be reverified")
	}
	confirmed, err := io.ReadAll(io.LimitReader(file, profileTokenBytes+1))
	if err != nil || !bytes.Equal(raw, confirmed) {
		clear(confirmed)
		return model.Digest{}, unsafeClientState("Profile credential changed while confirming")
	}
	clear(confirmed)
	after, err := state.profilesRoot.Lstat(name)
	if err != nil || !os.SameFile(before, after) || validateOwnerRegularFile(after, state.ownerUID) != nil {
		return model.Digest{}, unsafeClientState("Profile credential identity changed")
	}
	if err := state.validateProfilesLive(); err != nil {
		return model.Digest{}, err
	}
	if raw[len(raw)-1] != '\n' {
		return model.Digest{}, unsafeClientState("Profile credential has noncanonical bytes")
	}
	encoded := string(raw[:len(raw)-1])
	decoded, err := decodeOpaqueSecret(encoded)
	if err != nil || base64.RawURLEncoding.EncodeToString(decoded) != encoded {
		clear(decoded)
		return model.Digest{}, unsafeClientState("Profile credential has noncanonical bytes")
	}
	digest := model.Sum(decoded)
	clear(decoded)
	return digest, nil
}

func (state *profileCredentialState) create() (model.Digest, bool, error) {
	var credential [opaqueSecretBytes]byte
	if _, err := io.ReadFull(rand.Reader, credential[:]); err != nil {
		return model.Digest{}, false, fmt.Errorf("generate Profile credential: %w", err)
	}
	defer clear(credential[:])
	encoded := base64.RawURLEncoding.EncodeToString(credential[:])
	if len(encoded) != profileTokenBytes-1 {
		return model.Digest{}, false, errors.New("encode Profile credential: unexpected closed size")
	}
	payload := append([]byte(encoded), '\n')
	defer clear(payload)
	want := model.Sum(credential[:])

	tempName, temp, err := state.createTemp()
	if err != nil {
		return model.Digest{}, false, err
	}
	keepTemp := true
	var tempIdentity os.FileInfo
	defer func() {
		_ = temp.Close()
		if keepTemp && tempIdentity != nil {
			current, statErr := state.profilesRoot.Lstat(tempName)
			if statErr == nil && os.SameFile(current, tempIdentity) {
				_ = state.profilesRoot.Remove(tempName)
			}
		}
	}()
	tempIdentity, err = temp.Stat()
	if err != nil {
		return model.Digest{}, false, fmt.Errorf("inspect staged Profile credential: %w", err)
	}
	if err := temp.Chmod(ownerRegularFileMode); err != nil {
		return model.Digest{}, false, fmt.Errorf("protect staged Profile credential: %w", err)
	}
	if err := writeProfileCredential(temp, payload); err != nil {
		return model.Digest{}, false, fmt.Errorf("write staged Profile credential: %w", err)
	}
	if err := temp.Sync(); err != nil {
		return model.Digest{}, false, fmt.Errorf("persist staged Profile credential: %w", err)
	}
	tempIdentity, err = temp.Stat()
	if err != nil || tempIdentity.Size() != profileTokenBytes ||
		validateOwnerRegularFile(tempIdentity, state.ownerUID) != nil {
		return model.Digest{}, false, unsafeClientState("staged Profile credential is not canonical")
	}
	staged, err := state.profilesRoot.Lstat(tempName)
	if err != nil || !os.SameFile(tempIdentity, staged) ||
		validateOwnerRegularFile(staged, state.ownerUID) != nil {
		return model.Digest{}, false, unsafeClientState("staged Profile credential identity changed")
	}
	if err := state.validateProfilesLive(); err != nil {
		return model.Digest{}, false, err
	}

	destination := profileCredentialFilename()
	linkErr := unix.Linkat(int(state.profilesDir.Fd()), tempName,
		int(state.profilesDir.Fd()), destination, 0)
	if linkErr != nil && !errors.Is(linkErr, syscall.EEXIST) {
		return model.Digest{}, false, fmt.Errorf("publish Profile credential: %w", linkErr)
	}
	if linkErr == nil {
		published, statErr := state.profilesRoot.Lstat(destination)
		if statErr != nil || !os.SameFile(tempIdentity, published) ||
			validateOwnerRegularFile(published, state.ownerUID) != nil {
			return model.Digest{}, false, unsafeClientState("published Profile credential identity changed")
		}
		if err := state.profilesDir.Sync(); err != nil {
			return model.Digest{}, false, fmt.Errorf("persist Profile credential publication: %w", err)
		}
	}
	if err := state.removeTemp(tempName, tempIdentity); err != nil {
		return model.Digest{}, false, err
	}
	keepTemp = false
	if err := state.profilesDir.Sync(); err != nil {
		return model.Digest{}, false, fmt.Errorf("persist Profile credential staging cleanup: %w", err)
	}
	if err := temp.Close(); err != nil {
		return model.Digest{}, false, fmt.Errorf("close staged Profile credential: %w", err)
	}

	actual, err := state.load()
	if err != nil {
		return model.Digest{}, false, err
	}
	if linkErr == nil && actual != want {
		return model.Digest{}, false, unsafeClientState("published Profile credential differs from generated authority")
	}
	return actual, linkErr == nil, nil
}

func (state *profileCredentialState) createTemp() (string, *os.File, error) {
	for attempt := 0; attempt < profileCredentialTempAttempts; attempt++ {
		var random [16]byte
		if _, err := io.ReadFull(rand.Reader, random[:]); err != nil {
			return "", nil, fmt.Errorf("allocate staged Profile credential: %w", err)
		}
		name := profileCredentialTempPrefix + hex.EncodeToString(random[:]) + profileCredentialTempSuffix
		file, err := state.profilesRoot.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL,
			ownerRegularFileMode)
		if err == nil {
			return name, file, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return "", nil, fmt.Errorf("create staged Profile credential: %w", err)
		}
	}
	return "", nil, errors.New("create staged Profile credential: temporary namespace exhausted")
}

func (state *profileCredentialState) removeTemp(name string, expected os.FileInfo) error {
	current, err := state.profilesRoot.Lstat(name)
	if err != nil || expected == nil || !os.SameFile(current, expected) ||
		validateOwnerRegularFile(current, state.ownerUID) != nil {
		return unsafeClientState("staged Profile credential changed before cleanup")
	}
	if err := state.profilesRoot.Remove(name); err != nil {
		return fmt.Errorf("remove staged Profile credential: %w", err)
	}
	return nil
}

func profileCredentialFilename() string {
	return model.TeamworkProfileID().String() + profileTokenSuffix
}

func writeProfileCredential(file *os.File, payload []byte) error {
	for len(payload) > 0 {
		written, err := file.Write(payload)
		if err != nil {
			return err
		}
		if written <= 0 {
			return io.ErrShortWrite
		}
		payload = payload[written:]
	}
	return nil
}
