package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type serveOptions struct {
	stateDirectory string
}

func parseServeOptions(args []string) (serveOptions, error) {
	if len(args) != 2 {
		return serveOptions{}, errors.New("serve requires exactly --state-dir DIR")
	}
	if args[0] != "--state-dir" || strings.TrimSpace(args[1]) == "" {
		return serveOptions{}, errors.New("serve accepts only --state-dir DIR")
	}
	stateDirectory, err := resolveStateDirectory(args[1])
	if err != nil {
		return serveOptions{}, err
	}
	return serveOptions{stateDirectory: stateDirectory}, nil
}

func resolveStateDirectory(requested string) (string, error) {
	if strings.TrimSpace(requested) == "" {
		return "", errors.New("serve state directory is empty")
	}
	absolute, err := filepath.Abs(requested)
	if err != nil {
		return "", fmt.Errorf("resolve state directory: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", fmt.Errorf("resolve state directory: %w", err)
	}
	info, err := os.Lstat(resolved)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("serve state directory must be a real directory")
	}
	return filepath.Clean(resolved), nil
}
