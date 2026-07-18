package cli

import (
	"errors"
	"os"
	"path/filepath"
)

// resolveManagedWorkspace walks only physical directory ancestors. A local
// projection directory identifies the workspace; authority, modes, and
// credentials are still validated by the closed client that follows.
func resolveManagedWorkspace(workingDirectory func() (string, error)) (string, string, error) {
	if workingDirectory == nil {
		return "", "", errors.New("working directory dependency is unavailable")
	}
	cwd, err := workingDirectory()
	if err != nil {
		return "", "", err
	}
	resolved, err := filepath.EvalSymlinks(cwd)
	if err != nil {
		return "", "", err
	}
	current, err := filepath.Abs(resolved)
	if err != nil {
		return "", "", err
	}
	for {
		nodeState := filepath.Join(current, ".mnemon", "harness", "node")
		info, statErr := os.Lstat(nodeState)
		if statErr == nil && info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
			return current, nodeState, nil
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", "", errors.New("no configured Mnemon Harness workspace")
		}
		current = parent
	}
}
