package localapi

import (
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"golang.org/x/sys/unix"
)

const (
	ownerRegularFileMode os.FileMode = 0o600
	contextFileSuffix                = ".context"
	contextFileBytes                 = 43 + 1 // raw-base64url 32-byte token plus newline
)

// ErrUnsafeClientState marks a client-side security or replay handle that
// cannot be trusted. The error deliberately never includes capability bytes.
var ErrUnsafeClientState = errors.New("local API: unsafe client state")

// ContextFile is a verified owner-fenced claim capability. It intentionally
// exposes the secret only through HeaderValue; callers must not serialize the
// value into a request body, prompt, log, or evidence.
type ContextFile struct {
	path     string
	runID    model.RunID
	token    [opaqueSecretBytes]byte
	digest   model.Digest
	identity os.FileInfo
}

func (f ContextFile) Path() string         { return f.path }
func (f ContextFile) RunID() model.RunID   { return f.runID }
func (f ContextFile) Digest() model.Digest { return f.digest }

// HeaderValue returns the opaque value for Mnemon-Claim-Context. It must not
// be placed in argv or any Agent-visible representation.
func (f ContextFile) HeaderValue() string {
	return base64.RawURLEncoding.EncodeToString(f.token[:])
}

// WriteContextFile atomically publishes exactly
// <node-state>/runs/<run-id>.context. Repeating the write with the same token
// is idempotent; an existing different capability is never replaced.
func WriteContextFile(nodeState string, runID model.RunID, encodedToken string) (ContextFile, error) {
	runsDir, ownerUID, err := ensureOwnerSubdirectory(nodeState, "runs")
	if err != nil {
		return ContextFile{}, err
	}
	path, err := contextPathForRun(runsDir, runID)
	if err != nil {
		return ContextFile{}, err
	}
	token, err := decodeOpaqueSecret(encodedToken)
	if err != nil {
		return ContextFile{}, unsafeClientState("claim token is not canonical")
	}
	defer clear(token)
	payload := make([]byte, 0, contextFileBytes)
	payload = append(payload, encodedToken...)
	payload = append(payload, '\n')

	var result ContextFile
	err = withOwnerDirectoryLock(runsDir, ownerUID, func() error {
		if _, statErr := os.Lstat(path); statErr == nil {
			existing, readErr := readContextFileFromDir(runsDir, path, ownerUID)
			if readErr != nil {
				return readErr
			}
			if subtle.ConstantTimeCompare(existing.token[:], token) != 1 {
				return unsafeClientState("a different context already exists for the Run")
			}
			result = existing
			return nil
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return fmt.Errorf("%w: inspect context destination", ErrUnsafeClientState)
		}
		if err := atomicWriteOwnerFile(runsDir, path, payload, ownerUID); err != nil {
			return err
		}
		created, err := readContextFileFromDir(runsDir, path, ownerUID)
		if err != nil {
			return err
		}
		if subtle.ConstantTimeCompare(created.token[:], token) != 1 {
			return unsafeClientState("published context does not match its claim")
		}
		result = created
		return nil
	})
	if err != nil {
		return ContextFile{}, err
	}
	return result, nil
}

// ReadContextFile verifies that path is the exact canonical path of one Run
// beneath this Node's runs directory before opening it.
func ReadContextFile(nodeState, path string) (ContextFile, error) {
	runsDir, ownerUID, err := requireOwnerSubdirectory(nodeState, "runs")
	if err != nil {
		return ContextFile{}, err
	}
	if _, err := parseContextPath(runsDir, path); err != nil {
		return ContextFile{}, err
	}
	var result ContextFile
	err = withOwnerDirectoryLock(runsDir, ownerUID, func() error {
		var readErr error
		result, readErr = readContextFileFromDir(runsDir, path, ownerUID)
		return readErr
	})
	if err != nil {
		return ContextFile{}, err
	}
	return result, nil
}

// RemoveContextFile removes a terminal claim handle only when the current
// path is still the exact inode and canonical capability previously verified.
// A replacement, even one with byte-identical content, is preserved.
func RemoveContextFile(nodeState string, expected ContextFile) error {
	runsDir, ownerUID, err := requireOwnerSubdirectory(nodeState, "runs")
	if err != nil {
		return err
	}
	if expected.identity == nil || expected.digest.IsZero() || expected.runID.IsZero() {
		return unsafeClientState("context removal lacks an expected identity")
	}
	if _, err := parseContextPath(runsDir, expected.path); err != nil {
		return err
	}
	return withOwnerDirectoryLock(runsDir, ownerUID, func() error {
		current, err := readContextFileFromDir(runsDir, expected.path, ownerUID)
		if err != nil {
			return err
		}
		if !os.SameFile(current.identity, expected.identity) || current.runID != expected.runID ||
			current.digest != expected.digest || subtle.ConstantTimeCompare(current.token[:], expected.token[:]) != 1 {
			return unsafeClientState("context identity changed before removal")
		}
		if err := os.Remove(expected.path); err != nil {
			return fmt.Errorf("remove context file: %w", err)
		}
		if err := syncOwnerDirectory(runsDir); err != nil {
			return fmt.Errorf("persist context removal: %w", err)
		}
		return nil
	})
}

func contextPathForRun(runsDir string, runID model.RunID) (string, error) {
	if runID.IsZero() || !safeManagedFilename(runID.String()) {
		return "", unsafeClientState("Run ID is not safe for a managed context path")
	}
	return filepath.Join(runsDir, runID.String()+contextFileSuffix), nil
}

func parseContextPath(runsDir, path string) (model.RunID, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || filepath.Dir(path) != runsDir {
		return model.RunID{}, unsafeClientState("context path is outside the expected Run directory")
	}
	base := filepath.Base(path)
	if !strings.HasSuffix(base, contextFileSuffix) {
		return model.RunID{}, unsafeClientState("context path does not name a context file")
	}
	name := strings.TrimSuffix(base, contextFileSuffix)
	if !safeManagedFilename(name) {
		return model.RunID{}, unsafeClientState("context path has an invalid Run name")
	}
	runID, err := model.ParseRunID(name)
	if err != nil {
		return model.RunID{}, unsafeClientState("context path has an invalid Run ID")
	}
	expected, err := contextPathForRun(runsDir, runID)
	if err != nil || expected != path {
		return model.RunID{}, unsafeClientState("context path is not canonical")
	}
	return runID, nil
}

func readContextFileFromDir(runsDir, path string, ownerUID uint32) (ContextFile, error) {
	runID, err := parseContextPath(runsDir, path)
	if err != nil {
		return ContextFile{}, err
	}
	raw, identity, err := readOwnerRegularFile(path, contextFileBytes, ownerUID)
	if err != nil {
		return ContextFile{}, err
	}
	if len(raw) != contextFileBytes || raw[len(raw)-1] != '\n' {
		return ContextFile{}, unsafeClientState("context file has noncanonical bytes")
	}
	encoded := string(raw[:len(raw)-1])
	token, err := decodeOpaqueSecret(encoded)
	if err != nil {
		return ContextFile{}, unsafeClientState("context file has noncanonical bytes")
	}
	defer clear(token)
	if base64.RawURLEncoding.EncodeToString(token) != encoded {
		return ContextFile{}, unsafeClientState("context file has noncanonical bytes")
	}
	result := ContextFile{path: path, runID: runID, digest: model.Sum(raw), identity: identity}
	copy(result.token[:], token)
	return result, nil
}

func ensureOwnerSubdirectory(nodeState, name string) (string, uint32, error) {
	ownerUID, err := validateNodeStateDirectory(nodeState)
	if err != nil {
		return "", 0, err
	}
	path := filepath.Join(nodeState, name)
	created := false
	if err := os.Mkdir(path, ownerDirectoryMode); err == nil {
		created = true
	} else if !errors.Is(err, os.ErrExist) {
		return "", 0, fmt.Errorf("create owner-only %s directory: %w", name, err)
	}
	if _, err := validateOwnerDirectoryPath(path, ownerUID); err != nil {
		return "", 0, err
	}
	if created {
		if err := syncOwnerDirectory(nodeState); err != nil {
			return "", 0, fmt.Errorf("persist owner-only %s directory: %w", name, err)
		}
	}
	return path, ownerUID, nil
}

func requireOwnerSubdirectory(nodeState, name string) (string, uint32, error) {
	ownerUID, err := validateNodeStateDirectory(nodeState)
	if err != nil {
		return "", 0, err
	}
	path := filepath.Join(nodeState, name)
	if _, err := validateOwnerDirectoryPath(path, ownerUID); err != nil {
		return "", 0, err
	}
	return path, ownerUID, nil
}

func validateNodeStateDirectory(path string) (uint32, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return 0, unsafeClientState("Node state path must be absolute and canonical")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return 0, fmt.Errorf("%w: inspect Node state directory", ErrUnsafeClientState)
	}
	ownerUID := uint32(os.Geteuid())
	if err := validateOwnerDirectory(info, ownerUID); err != nil {
		return 0, err
	}
	return ownerUID, nil
}

func validateOwnerDirectoryPath(path string, ownerUID uint32) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("%w: inspect managed directory", ErrUnsafeClientState)
	}
	if err := validateOwnerDirectory(info, ownerUID); err != nil {
		return nil, err
	}
	return info, nil
}

func validateOwnerDirectory(info os.FileInfo, ownerUID uint32) error {
	if info == nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != ownerDirectoryMode {
		return unsafeClientState("managed directory is not an owner-only real directory")
	}
	actual, err := fileOwnerUID(info)
	if err != nil || actual != ownerUID {
		return unsafeClientState("managed directory has the wrong owner")
	}
	return nil
}

func validateOwnerRegularFile(info os.FileInfo, ownerUID uint32) error {
	if info == nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm() != ownerRegularFileMode {
		return unsafeClientState("managed file is not an owner-only regular file")
	}
	actual, err := fileOwnerUID(info)
	if err != nil || actual != ownerUID {
		return unsafeClientState("managed file has the wrong owner")
	}
	return nil
}

func readOwnerRegularFile(path string, maxBytes int64, ownerUID uint32) ([]byte, os.FileInfo, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: inspect managed file", ErrUnsafeClientState)
	}
	if err := validateOwnerRegularFile(before, ownerUID); err != nil {
		return nil, nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: open managed file", ErrUnsafeClientState)
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return nil, nil, unsafeClientState("managed file changed while opening")
	}
	if err := validateOwnerRegularFile(opened, ownerUID); err != nil {
		return nil, nil, err
	}
	if opened.Size() < 0 || opened.Size() > maxBytes {
		return nil, nil, unsafeClientState("managed file exceeds its closed size")
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil || int64(len(raw)) != opened.Size() || int64(len(raw)) > maxBytes {
		return nil, nil, unsafeClientState("managed file changed while reading")
	}
	return raw, opened, nil
}

func atomicWriteOwnerFile(dir, destination string, payload []byte, ownerUID uint32) error {
	if filepath.Dir(destination) != dir {
		return unsafeClientState("managed destination escaped its owner directory")
	}
	if _, err := os.Lstat(destination); err == nil {
		return unsafeClientState("managed destination already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: inspect managed destination", ErrUnsafeClientState)
	}
	temporary, err := os.CreateTemp(dir, ".mnemon-write-*.tmp")
	if err != nil {
		return fmt.Errorf("create managed temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	keepTemporary := true
	defer func() {
		_ = temporary.Close()
		if keepTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(ownerRegularFileMode); err != nil {
		return fmt.Errorf("protect managed temporary file: %w", err)
	}
	identity, err := temporary.Stat()
	if err != nil || validateOwnerRegularFile(identity, ownerUID) != nil {
		return unsafeClientState("managed temporary file is not owner-only")
	}
	if _, err := temporary.Write(payload); err != nil {
		return fmt.Errorf("write managed temporary file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("persist managed temporary file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close managed temporary file: %w", err)
	}
	if _, err := os.Lstat(destination); err == nil {
		return unsafeClientState("managed destination appeared during publication")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: reinspect managed destination", ErrUnsafeClientState)
	}
	if err := os.Rename(temporaryPath, destination); err != nil {
		return fmt.Errorf("publish managed file: %w", err)
	}
	keepTemporary = false
	if err := syncOwnerDirectory(dir); err != nil {
		return fmt.Errorf("persist managed publication: %w", err)
	}
	return nil
}

func withOwnerDirectoryLock(dir string, ownerUID uint32, action func() error) error {
	before, err := validateOwnerDirectoryPath(dir, ownerUID)
	if err != nil {
		return err
	}
	file, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("%w: open managed directory", ErrUnsafeClientState)
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(before, opened) || validateOwnerDirectory(opened, ownerUID) != nil {
		return unsafeClientState("managed directory changed while opening")
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX); err != nil {
		return fmt.Errorf("lock managed directory: %w", err)
	}
	defer unix.Flock(int(file.Fd()), unix.LOCK_UN) //nolint:errcheck -- lock is released again on close
	current, err := os.Lstat(dir)
	if err != nil || !os.SameFile(opened, current) || validateOwnerDirectory(current, ownerUID) != nil {
		return unsafeClientState("managed directory changed while locking")
	}
	return action()
}

func syncOwnerDirectory(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

func safeManagedFilename(value string) bool {
	return value != "" && value != "." && value != ".." && filepath.Base(value) == value &&
		!strings.ContainsAny(value, "/\\\x00")
}

func unsafeClientState(detail string) error {
	return fmt.Errorf("%w: %s", ErrUnsafeClientState, detail)
}
