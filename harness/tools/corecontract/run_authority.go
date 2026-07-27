package corecontract

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func canonicalRepositoryRoot(root string) (string, error) {
	if root == "" {
		return "", errors.New("Core gate repository root is required")
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve Core gate repository root: %w", err)
	}
	canonical, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", fmt.Errorf("resolve Core gate repository root symlinks: %w", err)
	}
	info, err := os.Stat(canonical)
	if err != nil || !info.IsDir() {
		return "", errors.New("Core gate repository root is not a directory")
	}
	return canonical, nil
}

func readGateAuthority(root string) (gateAuthority, error) {
	if err := validateGateRuntimePath(root); err != nil {
		return gateAuthority{}, err
	}
	status, err := runGit(root, "status", "--porcelain=v1", "--untracked-files=all")
	if err != nil || len(bytes.TrimSpace(status)) != 0 {
		return gateAuthority{}, errors.New("Core gates require a clean source worktree")
	}
	commit, err := gateGitObject(root, "HEAD")
	if err != nil {
		return gateAuthority{}, err
	}
	tree, err := gateGitObject(root, "HEAD^{tree}")
	if err != nil {
		return gateAuthority{}, err
	}
	contractDigest, err := fileDigest(filepath.Join(root, filepath.FromSlash(DocumentPath)))
	if err != nil {
		return gateAuthority{}, fmt.Errorf("digest Core contract: %w", err)
	}
	registryDigest, err := fileDigest(filepath.Join(root, filepath.FromSlash(RegistryPath)))
	if err != nil {
		return gateAuthority{}, fmt.Errorf("digest Core registry: %w", err)
	}
	return gateAuthority{
		commit: commit, tree: tree, contractDigest: contractDigest,
		registryDigest: registryDigest,
	}, nil
}

func validateGateRuntimePath(root string) error {
	probe := ".testdata/r5/core-gates/ignore-probe"
	if _, err := runGit(root, "check-ignore", "--no-index", "--", probe); err != nil {
		return errors.New("Core gate runtime path is not ignored")
	}
	tracked, err := runGit(root, "ls-files", "--", ".testdata")
	if err != nil || len(bytes.TrimSpace(tracked)) != 0 {
		return errors.New("Core gate runtime path contains tracked files")
	}
	return nil
}

func gateGitObject(root, revision string) (string, error) {
	output, err := runGit(root, "rev-parse", revision)
	value := strings.TrimSpace(string(output))
	if err != nil || !gitObjectPattern.MatchString(value) {
		return "", fmt.Errorf("resolve Core gate Git object %s", revision)
	}
	return value, nil
}

func makePrivateDirectory(root, relative string) (string, error) {
	if relative == "" || filepath.IsAbs(relative) || filepath.Clean(relative) != relative {
		return "", fmt.Errorf("Core gate runtime directory %q is invalid", relative)
	}
	current := root
	for _, component := range strings.Split(filepath.ToSlash(relative), "/") {
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			if err := os.Mkdir(current, 0o700); err != nil {
				return "", fmt.Errorf("create Core gate runtime directory: %w", err)
			}
			continue
		}
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("Core gate runtime path %s is not a real directory", current)
		}
		if err := os.Chmod(current, 0o700); err != nil {
			return "", fmt.Errorf("protect Core gate runtime directory: %w", err)
		}
	}
	return current, nil
}

func openExclusivePrivate(filename string) (*os.File, error) {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create private Core gate file: %w", err)
	}
	return file, nil
}

func writeExclusivePrivate(filename string, data []byte) error {
	file, err := openExclusivePrivate(filename)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(data)
	closeErr := file.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		_ = os.Remove(filename)
		return fmt.Errorf("write private Core gate file: %w", err)
	}
	return nil
}
