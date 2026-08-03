package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

type serveOptions struct {
	stateDirectory string
	principal      agency.AgentPrincipalID
}

func parseServeOptions(args []string) (serveOptions, error) {
	if len(args) != 4 {
		return serveOptions{}, errors.New("serve requires exactly --state-dir DIR --principal ID")
	}
	values := make(map[string]string, 2)
	for len(args) != 0 {
		if args[0] != "--state-dir" && args[0] != "--principal" {
			return serveOptions{}, errors.New("serve accepts only --state-dir DIR --principal ID")
		}
		if _, duplicate := values[args[0]]; duplicate || strings.TrimSpace(args[1]) == "" {
			return serveOptions{}, errors.New("serve options must occur exactly once with nonempty values")
		}
		values[args[0]] = args[1]
		args = args[2:]
	}
	stateDirectory, err := resolveStateDirectory(values["--state-dir"])
	if err != nil {
		return serveOptions{}, err
	}
	principal, err := agency.NewAgentPrincipalID(values["--principal"])
	if err != nil {
		return serveOptions{}, errors.New("serve Principal is invalid")
	}
	return serveOptions{stateDirectory: stateDirectory, principal: principal}, nil
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
