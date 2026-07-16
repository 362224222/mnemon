package integration

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
)

var (
	ErrProjectionConflict = errors.New("managed projection conflicts with existing state")
	ErrUnsafeProjection   = errors.New("managed projection path is unsafe")
)

type NodeBundleReceipt struct {
	Path     string
	Revision string
	Replayed bool
}

// InstallNodeBundle materializes the validated embedded asset tree below one
// owner-only Node state. The revision directory is immutable: an existing
// exact tree is a replay, while any missing, changed or additional entry is a
// conflict and is never repaired in place.
func InstallNodeBundle(nodeState string, bundle assets.Bundle) (NodeBundleReceipt, error) {
	manifest := bundle.Manifest()
	manifestRaw := bundle.ManifestBytes()
	if err := validateBundleInputs(nodeState, manifest, manifestRaw); err != nil {
		return NodeBundleReceipt{}, err
	}
	if err := requireOwnerDirectory(nodeState, 0o700); err != nil {
		return NodeBundleReceipt{}, err
	}
	assetsPath := filepath.Join(nodeState, "assets")
	if err := ensureOwnerDirectory(assetsPath, 0o700); err != nil {
		return NodeBundleReceipt{}, err
	}
	target := filepath.Join(assetsPath, manifest.AssetRevision)
	if _, err := os.Lstat(target); err == nil {
		if err := verifyNodeBundle(target, bundle); err != nil {
			return NodeBundleReceipt{}, err
		}
		return NodeBundleReceipt{Path: target, Revision: manifest.AssetRevision, Replayed: true}, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return NodeBundleReceipt{}, fmt.Errorf("inspect managed Node bundle: %w", err)
	}

	stage, err := os.MkdirTemp(assetsPath, ".bundle-")
	if err != nil {
		return NodeBundleReceipt{}, fmt.Errorf("stage managed Node bundle: %w", err)
	}
	stageLive := true
	defer func() {
		if stageLive {
			_ = os.RemoveAll(stage)
		}
	}()
	if err := os.Chmod(stage, 0o700); err != nil {
		return NodeBundleReceipt{}, fmt.Errorf("protect managed Node bundle stage: %w", err)
	}
	if err := writeBundleTree(stage, bundle); err != nil {
		return NodeBundleReceipt{}, err
	}
	if err := syncDirectoryTree(stage); err != nil {
		return NodeBundleReceipt{}, err
	}
	if err := os.Rename(stage, target); err != nil {
		// A concurrent identical installer may have published first. It is a
		// replay only after full verification; otherwise preserve the conflict.
		if verifyErr := verifyNodeBundle(target, bundle); verifyErr == nil {
			return NodeBundleReceipt{Path: target, Revision: manifest.AssetRevision, Replayed: true}, nil
		}
		return NodeBundleReceipt{}, fmt.Errorf("publish managed Node bundle: %w", err)
	}
	stageLive = false
	if err := syncDirectory(assetsPath); err != nil {
		return NodeBundleReceipt{}, fmt.Errorf("sync managed Node bundle parent: %w", err)
	}
	if err := verifyNodeBundle(target, bundle); err != nil {
		return NodeBundleReceipt{}, err
	}
	return NodeBundleReceipt{Path: target, Revision: manifest.AssetRevision}, nil
}

// VerifyNodeBundle revalidates an installed revision without changing it.
func VerifyNodeBundle(nodeState string, bundle assets.Bundle) error {
	manifest := bundle.Manifest()
	if err := validateBundleInputs(nodeState, manifest, bundle.ManifestBytes()); err != nil {
		return err
	}
	if err := requireOwnerDirectory(nodeState, 0o700); err != nil {
		return err
	}
	if err := requireOwnerDirectory(filepath.Join(nodeState, "assets"), 0o700); err != nil {
		return err
	}
	return verifyNodeBundle(filepath.Join(nodeState, "assets", manifest.AssetRevision), bundle)
}

func validateBundleInputs(nodeState string, manifest assets.Manifest, manifestRaw []byte) error {
	if nodeState == "" || !filepath.IsAbs(nodeState) || filepath.Clean(nodeState) != nodeState ||
		manifest.AssetRevision == "" || filepath.Base(manifest.AssetRevision) != manifest.AssetRevision ||
		len(manifest.Files) == 0 || len(manifestRaw) == 0 {
		return fmt.Errorf("%w: Node state or canonical bundle is incomplete", ErrUnsafeProjection)
	}
	return nil
}

func writeBundleTree(root string, bundle assets.Bundle) error {
	manifest := bundle.Manifest()
	for _, record := range manifest.Files {
		content, err := bundle.Read(record.Path)
		if err != nil {
			return fmt.Errorf("read canonical asset %s: %w", record.Path, err)
		}
		mode, err := parseAssetMode(record.Mode)
		if err != nil {
			return err
		}
		path := filepath.Join(root, filepath.FromSlash(record.Path))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return fmt.Errorf("create canonical asset directory: %w", err)
		}
		if err := writeSyncedExclusive(path, content, mode); err != nil {
			return fmt.Errorf("write canonical asset %s: %w", record.Path, err)
		}
	}
	if err := writeSyncedExclusive(filepath.Join(root, "manifest.json"), bundle.ManifestBytes(), 0o644); err != nil {
		return fmt.Errorf("write canonical asset manifest: %w", err)
	}
	return nil
}

func verifyNodeBundle(root string, bundle assets.Bundle) error {
	if err := requireOwnerDirectory(root, 0o700); err != nil {
		return fmt.Errorf("%w: %v", ErrProjectionConflict, err)
	}
	expectedFiles := make(map[string]struct {
		content []byte
		mode    os.FileMode
	})
	expectedDirs := map[string]struct{}{":root": {}}
	for _, record := range bundle.Manifest().Files {
		content, err := bundle.Read(record.Path)
		if err != nil {
			return err
		}
		mode, err := parseAssetMode(record.Mode)
		if err != nil {
			return err
		}
		expectedFiles[record.Path] = struct {
			content []byte
			mode    os.FileMode
		}{content: content, mode: mode}
		for parent := filepath.ToSlash(filepath.Dir(record.Path)); parent != "."; parent = filepath.ToSlash(filepath.Dir(parent)) {
			expectedDirs[parent] = struct{}{}
		}
	}
	expectedFiles["manifest.json"] = struct {
		content []byte
		mode    os.FileMode
	}{content: bundle.ManifestBytes(), mode: 0o644}

	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		rel = filepath.ToSlash(rel)
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || !ownedByCurrentUser(info) {
			return fmt.Errorf("entry %s has unsafe type or owner", rel)
		}
		if entry.IsDir() {
			if _, ok := expectedDirs[rel]; !ok || info.Mode().Perm() != 0o700 {
				return fmt.Errorf("directory %s is unexpected or has wrong mode", rel)
			}
			delete(expectedDirs, rel)
			return nil
		}
		expected, ok := expectedFiles[rel]
		if !ok || !info.Mode().IsRegular() || info.Mode().Perm() != expected.mode {
			return fmt.Errorf("file %s is unexpected or has wrong type/mode", rel)
		}
		content, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(content, expected.content) {
			return fmt.Errorf("file %s differs from the canonical asset", rel)
		}
		delete(expectedFiles, rel)
		return nil
	})
	if err != nil || len(expectedFiles) != 0 || len(expectedDirs) != 1 {
		if err == nil {
			missing := make([]string, 0, len(expectedFiles)+len(expectedDirs))
			for path := range expectedFiles {
				missing = append(missing, path)
			}
			for path := range expectedDirs {
				if path != ":root" {
					missing = append(missing, path+"/")
				}
			}
			sort.Strings(missing)
			err = fmt.Errorf("missing entries: %s", strings.Join(missing, ", "))
		}
		return fmt.Errorf("%w: installed Node bundle is not exact: %v", ErrProjectionConflict, err)
	}
	return nil
}

func ensureOwnerDirectory(path string, mode os.FileMode) error {
	if err := os.Mkdir(path, mode); err != nil && !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("create owner directory %s: %w", path, err)
	}
	return requireOwnerDirectory(path, mode)
}

func requireOwnerDirectory(path string, mode os.FileMode) error {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != mode ||
		!ownedByCurrentUser(info) {
		return fmt.Errorf("%w: %s must be a real current-owner directory with mode %04o", ErrUnsafeProjection, path, mode)
	}
	return nil
}

func ownedByCurrentUser(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}

func parseAssetMode(text string) (os.FileMode, error) {
	parsed, err := strconv.ParseUint(text, 8, 32)
	if err != nil || parsed != 0o644 && parsed != 0o755 {
		return 0, errors.New("canonical asset has unsupported mode")
	}
	return os.FileMode(parsed), nil
}

func writeSyncedExclusive(path string, content []byte, mode os.FileMode) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	fail := func(cause error) error {
		_ = file.Close()
		return cause
	}
	if _, err := file.Write(content); err != nil {
		return fail(err)
	}
	if err := file.Chmod(mode); err != nil {
		return fail(err)
	}
	if err := file.Sync(); err != nil {
		return fail(err)
	}
	return file.Close()
}

func syncDirectoryTree(root string) error {
	var directories []string
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			directories = append(directories, path)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("enumerate managed Node bundle directories: %w", err)
	}
	sort.Slice(directories, func(i, j int) bool { return len(directories[i]) > len(directories[j]) })
	for _, directory := range directories {
		if err := syncDirectory(directory); err != nil {
			return fmt.Errorf("sync managed Node bundle directory: %w", err)
		}
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return err
	}
	return directory.Close()
}
