package node

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"golang.org/x/sys/unix"
)

const (
	identityKeyName       = "identity.key"
	identityDirectoryMode = os.FileMode(0o700)
	identityKeyMode       = os.FileMode(0o600)
	maxIdentityKeyBytes   = int64(256)
	identityTempAttempts  = 32
	identityLockPoll      = 10 * time.Millisecond
)

var ErrIdentity = errors.New("mnemond Node identity")

var identityProcessMu sync.Mutex

// Identity is the one persistent cryptographic identity of an R5 Node. The
// libp2p PeerID and Event publication signer are deliberately derived from the
// same Ed25519 private key.
type Identity struct {
	peerID     model.PeerID
	privateKey libp2pcrypto.PrivKey
	publicKey  ed25519.PublicKey
	signer     *event.Ed25519Signer
}

func (identity *Identity) PeerID() model.PeerID {
	if identity == nil {
		return model.PeerID{}
	}
	return identity.peerID
}

func (identity *Identity) PrivateKey() libp2pcrypto.PrivKey {
	if identity == nil {
		return nil
	}
	return identity.privateKey
}

func (identity *Identity) PublicKey() ed25519.PublicKey {
	if identity == nil {
		return nil
	}
	return append(ed25519.PublicKey(nil), identity.publicKey...)
}

func (identity *Identity) PublicationSigner() event.PublicationSigner {
	if identity == nil {
		return nil
	}
	return identity.signer
}

// EnsureIdentity loads the canonical Node identity or atomically publishes a
// fresh Ed25519 identity when none exists. It never replaces an existing path:
// concurrent creators converge on the first completely written key.
func EnsureIdentity(nodeState string) (*Identity, error) {
	handle, err := openIdentityNodeState(nodeState)
	if err != nil {
		return nil, err
	}
	defer handle.close()
	if err := handle.lock(); err != nil {
		return nil, err
	}
	defer handle.unlock()
	if err := handle.cleanupStaging(); err != nil {
		return nil, err
	}

	identity, err := handle.load()
	if err == nil {
		return identity, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	privateKey, _, err := libp2pcrypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, identityError("generate", err)
	}
	encoded, err := libp2pcrypto.MarshalPrivateKey(privateKey)
	if err != nil {
		return nil, identityError("encode", err)
	}
	if err := handle.publish(encoded); err != nil {
		return nil, err
	}
	return handle.load()
}

// LoadIdentity loads an existing canonical identity. Daemon restart uses this
// strict path so a missing key is a Node identity disaster, not an invitation
// to silently create a different PeerID.
func LoadIdentity(nodeState string) (*Identity, error) {
	handle, err := openIdentityNodeState(nodeState)
	if err != nil {
		return nil, err
	}
	defer handle.close()
	if err := handle.lock(); err != nil {
		return nil, err
	}
	defer handle.unlock()
	if err := handle.cleanupStaging(); err != nil {
		return nil, err
	}
	return handle.load()
}

type identityNodeState struct {
	path     string
	identity os.FileInfo
	root     *os.Root
	dir      *os.File
	ownerUID uint32
	locked   bool
}

func openIdentityNodeState(path string) (*identityNodeState, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, identityError("open Node state", errors.New("path must be absolute and clean"))
	}
	before, err := os.Lstat(path)
	if err != nil {
		return nil, identityError("inspect Node state", err)
	}
	ownerUID, err := validateIdentityOwnerPath(before, identityDirectoryMode, true)
	if err != nil {
		return nil, identityError("inspect Node state", err)
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, identityError("open Node state", err)
	}
	dir, err := os.Open(path)
	if err != nil {
		_ = root.Close()
		return nil, identityError("open Node state", err)
	}
	opened, statErr := dir.Stat()
	live, liveErr := os.Lstat(path)
	if statErr != nil || liveErr != nil || !os.SameFile(before, opened) || !os.SameFile(before, live) {
		_ = dir.Close()
		_ = root.Close()
		return nil, identityError("open Node state", errors.New("directory identity changed"))
	}
	if _, err := validateIdentityOwnerPath(opened, identityDirectoryMode, true); err != nil {
		_ = dir.Close()
		_ = root.Close()
		return nil, identityError("open Node state", err)
	}
	return &identityNodeState{path: path, identity: before, root: root, dir: dir, ownerUID: ownerUID}, nil
}

func (state *identityNodeState) close() {
	if state == nil {
		return
	}
	if state.dir != nil {
		_ = state.dir.Close()
	}
	if state.root != nil {
		_ = state.root.Close()
	}
}

func (state *identityNodeState) lock() error {
	return state.lockContext(context.Background())
}

// lockContext preserves the identity module's process-plus-filesystem lock
// while allowing bounded daemon startup paths to honor cancellation.
func (state *identityNodeState) lockContext(ctx context.Context) error {
	if state == nil || state.dir == nil {
		return identityError("lock Node state", errors.New("directory is unavailable"))
	}
	if ctx == nil {
		return identityError("lock Node state", errors.New("context is unavailable"))
	}
	for {
		if err := ctx.Err(); err != nil {
			return identityError("lock Node state", err)
		}
		if identityProcessMu.TryLock() {
			err := unix.Flock(int(state.dir.Fd()), unix.LOCK_EX|unix.LOCK_NB)
			if err == nil {
				if err := ctx.Err(); err != nil {
					_ = unix.Flock(int(state.dir.Fd()), unix.LOCK_UN)
					identityProcessMu.Unlock()
					return identityError("lock Node state", err)
				}
				if err := state.validateLive(); err != nil {
					_ = unix.Flock(int(state.dir.Fd()), unix.LOCK_UN)
					identityProcessMu.Unlock()
					return err
				}
				state.locked = true
				return nil
			}
			identityProcessMu.Unlock()
			if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
				return identityError("lock Node state", err)
			}
		}
		timer := time.NewTimer(identityLockPoll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return identityError("lock Node state", ctx.Err())
		case <-timer.C:
		}
	}
}

func (state *identityNodeState) unlock() {
	if state != nil && state.dir != nil && state.locked {
		state.locked = false
		_ = unix.Flock(int(state.dir.Fd()), unix.LOCK_UN)
		identityProcessMu.Unlock()
	}
}

// cleanupStaging makes identity publication crash-convergent. The staging
// namespace is private to this module; the directory lock prevents removing a
// live concurrent creator's file. Unsafe entries fail closed instead of being
// followed or silently discarded.
func (state *identityNodeState) cleanupStaging() error {
	if err := state.validateLive(); err != nil {
		return err
	}
	directory, err := state.root.Open(".")
	if err != nil {
		return identityError("scan staged keys", err)
	}
	entries, err := directory.ReadDir(-1)
	closeErr := directory.Close()
	if err != nil {
		return identityError("scan staged keys", err)
	}
	if closeErr != nil {
		return identityError("scan staged keys", closeErr)
	}
	removed := false
	for _, entry := range entries {
		if !isIdentityTempName(entry.Name()) {
			continue
		}
		info, err := state.root.Lstat(entry.Name())
		if err != nil {
			return identityError("inspect staged key", err)
		}
		ownerUID, err := validateIdentityOwnerPath(info, identityKeyMode, false)
		if err != nil || ownerUID != state.ownerUID {
			if err == nil {
				err = errors.New("staged key owner differs from Node state owner")
			}
			return identityError("inspect staged key", err)
		}
		if err := state.root.Remove(entry.Name()); err != nil {
			return identityError("remove staged key", err)
		}
		removed = true
	}
	if removed {
		if err := state.dir.Sync(); err != nil {
			return identityError("sync staged key cleanup", err)
		}
	}
	return state.validateLive()
}

func isIdentityTempName(name string) bool {
	const prefix = ".identity-"
	const suffix = ".tmp"
	if len(name) != len(prefix)+32+len(suffix) || name[:len(prefix)] != prefix ||
		name[len(name)-len(suffix):] != suffix {
		return false
	}
	hexValue := name[len(prefix) : len(name)-len(suffix)]
	decoded, err := hex.DecodeString(hexValue)
	return err == nil && hex.EncodeToString(decoded) == hexValue
}

func (state *identityNodeState) load() (*Identity, error) {
	if err := state.validateLive(); err != nil {
		return nil, err
	}
	before, err := state.root.Lstat(identityKeyName)
	if err != nil {
		return nil, identityError("inspect key", err)
	}
	if _, err := validateIdentityOwnerPath(before, identityKeyMode, false); err != nil {
		return nil, identityError("inspect key", err)
	}
	file, err := state.root.OpenFile(identityKeyName, os.O_RDONLY, 0)
	if err != nil {
		return nil, identityError("open key", err)
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return nil, identityError("open key", errors.New("key identity changed"))
	}
	ownerUID, err := validateIdentityOwnerPath(opened, identityKeyMode, false)
	if err != nil || ownerUID != state.ownerUID {
		if err == nil {
			err = errors.New("key owner differs from Node state owner")
		}
		return nil, identityError("open key", err)
	}
	if opened.Size() <= 0 || opened.Size() > maxIdentityKeyBytes {
		return nil, identityError("read key", errors.New("invalid encoded key size"))
	}
	encoded, err := io.ReadAll(io.LimitReader(file, maxIdentityKeyBytes+1))
	if err != nil {
		return nil, identityError("read key", err)
	}
	if int64(len(encoded)) != opened.Size() || int64(len(encoded)) > maxIdentityKeyBytes {
		return nil, identityError("read key", errors.New("key size changed"))
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, identityError("verify key", err)
	}
	confirmed, err := io.ReadAll(io.LimitReader(file, maxIdentityKeyBytes+1))
	if err != nil || !bytes.Equal(encoded, confirmed) {
		if err == nil {
			err = errors.New("key content changed")
		}
		return nil, identityError("verify key", err)
	}
	confirmedInfo, err := file.Stat()
	if err != nil || !os.SameFile(opened, confirmedInfo) || confirmedInfo.Size() != opened.Size() {
		return nil, identityError("verify key", errors.New("key identity or size changed"))
	}
	if _, err := validateIdentityOwnerPath(confirmedInfo, identityKeyMode, false); err != nil {
		return nil, identityError("verify key", err)
	}
	after, err := state.root.Lstat(identityKeyName)
	if err != nil || !os.SameFile(before, after) {
		return nil, identityError("read key", errors.New("key identity changed"))
	}
	if _, err := validateIdentityOwnerPath(after, identityKeyMode, false); err != nil {
		return nil, identityError("read key", err)
	}
	if err := state.validateLive(); err != nil {
		return nil, err
	}
	return decodeIdentity(encoded)
}

func (state *identityNodeState) publish(encoded []byte) error {
	if len(encoded) == 0 || int64(len(encoded)) > maxIdentityKeyBytes {
		return identityError("publish key", errors.New("invalid encoded key size"))
	}
	tempName, tempFile, err := state.createTemp()
	if err != nil {
		return err
	}
	tempPublished := false
	defer func() {
		_ = tempFile.Close()
		if !tempPublished {
			_ = state.root.Remove(tempName)
		}
	}()
	if err := tempFile.Chmod(identityKeyMode); err != nil {
		return identityError("prepare key", err)
	}
	if err := writeIdentityBytes(tempFile, encoded); err != nil {
		return identityError("write key", err)
	}
	if err := tempFile.Sync(); err != nil {
		return identityError("sync key", err)
	}
	tempInfo, err := tempFile.Stat()
	if err != nil {
		return identityError("inspect staged key", err)
	}
	ownerUID, err := validateIdentityOwnerPath(tempInfo, identityKeyMode, false)
	if err != nil || ownerUID != state.ownerUID {
		if err == nil {
			err = errors.New("staged key owner differs from Node state owner")
		}
		return identityError("inspect staged key", err)
	}
	stagedPathInfo, err := state.root.Lstat(tempName)
	if err != nil || !os.SameFile(tempInfo, stagedPathInfo) {
		return identityError("inspect staged key", errors.New("staged key identity changed"))
	}
	if err := state.validateLive(); err != nil {
		return err
	}

	dirFD := int(state.dir.Fd())
	err = unix.Linkat(dirFD, tempName, dirFD, identityKeyName, 0)
	if err != nil && !errors.Is(err, syscall.EEXIST) {
		return identityError("publish key", err)
	}
	if err == nil {
		if syncErr := state.dir.Sync(); syncErr != nil {
			return identityError("sync published key", syncErr)
		}
	}
	if removeErr := state.root.Remove(tempName); removeErr != nil {
		return identityError("remove staged key", removeErr)
	}
	tempPublished = true
	if syncErr := state.dir.Sync(); syncErr != nil {
		return identityError("sync Node state", syncErr)
	}
	if err := state.validateLive(); err != nil {
		return err
	}
	return nil
}

func (state *identityNodeState) createTemp() (string, *os.File, error) {
	for attempt := 0; attempt < identityTempAttempts; attempt++ {
		var random [16]byte
		if _, err := io.ReadFull(rand.Reader, random[:]); err != nil {
			return "", nil, identityError("allocate staged key", err)
		}
		name := ".identity-" + hex.EncodeToString(random[:]) + ".tmp"
		file, err := state.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, identityKeyMode)
		if err == nil {
			return name, file, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return "", nil, identityError("create staged key", err)
		}
	}
	return "", nil, identityError("create staged key", errors.New("temporary name space exhausted"))
}

func (state *identityNodeState) validateLive() error {
	live, err := os.Lstat(state.path)
	if err != nil || !os.SameFile(state.identity, live) {
		return identityError("validate Node state", errors.New("directory identity changed"))
	}
	ownerUID, err := validateIdentityOwnerPath(live, identityDirectoryMode, true)
	if err != nil || ownerUID != state.ownerUID {
		if err == nil {
			err = errors.New("directory owner changed")
		}
		return identityError("validate Node state", err)
	}
	return nil
}

func validateIdentityOwnerPath(info os.FileInfo, mode os.FileMode, directory bool) (uint32, error) {
	if info == nil || info.Mode()&os.ModeSymlink != 0 {
		return 0, errors.New("path must not be a symlink")
	}
	if directory {
		if !info.IsDir() {
			return 0, errors.New("path must be a directory")
		}
	} else if !info.Mode().IsRegular() {
		return 0, errors.New("path must be a regular file")
	}
	if info.Mode().Perm() != mode {
		return 0, fmt.Errorf("path mode is %04o, want %04o", info.Mode().Perm(), mode)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, errors.New("path owner is unavailable")
	}
	ownerUID := uint32(stat.Uid)
	if ownerUID != uint32(os.Geteuid()) {
		return 0, errors.New("path is not owned by the current effective user")
	}
	return ownerUID, nil
}

func decodeIdentity(encoded []byte) (*Identity, error) {
	privateKey, err := libp2pcrypto.UnmarshalPrivateKey(encoded)
	if err != nil {
		return nil, identityError("decode key", err)
	}
	if privateKey.Type() != libp2pcrypto.Ed25519 {
		return nil, identityError("decode key", errors.New("only Ed25519 identities are supported"))
	}
	canonical, err := libp2pcrypto.MarshalPrivateKey(privateKey)
	if err != nil || !bytes.Equal(encoded, canonical) {
		return nil, identityError("decode key", errors.New("key encoding is not canonical"))
	}
	rawPrivate, err := privateKey.Raw()
	if err != nil || len(rawPrivate) != ed25519.PrivateKeySize {
		return nil, identityError("decode key", errors.New("invalid Ed25519 private key"))
	}
	wantPrivate := ed25519.NewKeyFromSeed(rawPrivate[:ed25519.SeedSize])
	if !bytes.Equal(rawPrivate, wantPrivate) {
		return nil, identityError("decode key", errors.New("Ed25519 public and private key material disagree"))
	}
	rawPublic, err := privateKey.GetPublic().Raw()
	if err != nil || len(rawPublic) != ed25519.PublicKeySize ||
		!bytes.Equal(rawPublic, rawPrivate[ed25519.SeedSize:]) {
		return nil, identityError("derive public key", errors.New("invalid Ed25519 public key"))
	}
	libp2pID, err := libp2ppeer.IDFromPrivateKey(privateKey)
	if err != nil {
		return nil, identityError("derive PeerID", err)
	}
	peerID, err := model.ParsePeerID(libp2pID.String())
	if err != nil {
		return nil, identityError("derive PeerID", err)
	}
	signer, err := event.NewEd25519Signer(ed25519.PrivateKey(rawPrivate))
	if err != nil {
		return nil, identityError("derive publication signer", err)
	}
	return &Identity{peerID: peerID, privateKey: privateKey,
		publicKey: append(ed25519.PublicKey(nil), rawPublic...), signer: signer}, nil
}

func writeIdentityBytes(file *os.File, encoded []byte) error {
	for len(encoded) > 0 {
		written, err := file.Write(encoded)
		if err != nil {
			return err
		}
		if written <= 0 {
			return io.ErrShortWrite
		}
		encoded = encoded[written:]
	}
	return nil
}

func identityError(operation string, err error) error {
	return fmt.Errorf("%w: %s: %w", ErrIdentity, operation, err)
}
