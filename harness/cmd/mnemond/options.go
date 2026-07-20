package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

func parseInspectProjectRoot(args []string) (string, error) {
	if len(args) != 2 || args[0] != "--project-root" || strings.TrimSpace(args[1]) == "" {
		return "", errors.New("inspect requires exactly --project-root DIR")
	}
	return resolveProjectRoot(args[1])
}

func parseConfirmOfflineOptions(args []string) (string, model.Digest, error) {
	expectedWire, remaining, err := takeRequiredManagedOption("confirm-offline",
		"--expected-authority-digest", args)
	if err != nil {
		return "", model.Digest{}, err
	}
	workspace, err := parseInspectProjectRoot(remaining)
	if err != nil {
		return "", model.Digest{}, errors.New(
			"confirm-offline requires --project-root and --expected-authority-digest")
	}
	expected, err := model.ParseDigest(expectedWire)
	if err != nil || expected.IsZero() || expected.String() != expectedWire {
		return "", model.Digest{}, errors.New(
			"confirm-offline expected authority digest must be canonical sha256")
	}
	return workspace, expected, nil
}

func parseInitializeOptions(args []string) (node.ProvisionOptions, error) {
	workspace, host, revision, err := parseManagedAuthorityOptions("initialize", args)
	if err != nil {
		return node.ProvisionOptions{}, err
	}
	return node.ProvisionOptions{Workspace: workspace, Host: host, AssetRevision: revision}, nil
}

func parseActivateOptions(args []string) (node.ActivateOptions, error) {
	expected, workspace, host, revision, err := parseExpectedManagedAuthorityOptions("activate", args)
	if err != nil {
		return node.ActivateOptions{}, err
	}
	return node.ActivateOptions{Workspace: workspace, Host: host, AssetRevision: revision,
		ExpectedUpdatedAt: expected}, nil
}

func parseDeactivateOptions(args []string) (node.DeactivateOptions, error) {
	expected, workspace, host, revision, err := parseExpectedManagedAuthorityOptions("deactivate", args)
	if err != nil {
		return node.DeactivateOptions{}, err
	}
	return node.DeactivateOptions{Workspace: workspace, Host: host, AssetRevision: revision,
		ExpectedUpdatedAt: expected}, nil
}

func parseExpectedManagedAuthorityOptions(command string,
	args []string,
) (time.Time, string, model.HostKind, string, error) {
	expectedWire, remaining, err := takeRequiredManagedOption(command, "--expected-updated-at", args)
	if err != nil {
		return time.Time{}, "", "", "", err
	}
	workspace, host, revision, err := parseManagedAuthorityOptions(command, remaining)
	if err != nil {
		return time.Time{}, "", "", "", err
	}
	expected, err := time.Parse(time.RFC3339Nano, expectedWire)
	if err != nil || expected.UnixNano() <= 0 || expected.UTC().Format(time.RFC3339Nano) != expectedWire {
		return time.Time{}, "", "", "", fmt.Errorf("%s expected update time must be canonical RFC3339Nano", command)
	}
	if !time.Unix(0, expected.UnixNano()).UTC().Equal(expected.UTC()) {
		return time.Time{}, "", "", "", fmt.Errorf("%s expected update time is outside the supported range", command)
	}
	return expected, workspace, host, revision, nil
}

func takeRequiredManagedOption(command, option string, args []string) (string, []string, error) {
	remaining := make([]string, 0, len(args))
	value := ""
	for index := 0; index < len(args); {
		if args[index] != option {
			remaining = append(remaining, args[index])
			index++
			continue
		}
		if value != "" || index+1 >= len(args) || strings.TrimSpace(args[index+1]) == "" {
			return "", nil, fmt.Errorf("%s %s must occur exactly once with a nonempty value", command, option)
		}
		value = args[index+1]
		index += 2
	}
	if value == "" {
		return "", nil, fmt.Errorf("%s requires %s", command, option)
	}
	return value, remaining, nil
}

func parseManagedAuthorityOptions(command string, args []string) (string, model.HostKind, string, error) {
	values := make(map[string]string, 3)
	for len(args) != 0 {
		if len(args) < 2 || (args[0] != "--project-root" && args[0] != "--host" && args[0] != "--asset-revision") {
			return "", "", "", fmt.Errorf("%s requires --project-root, --host and --asset-revision", command)
		}
		if _, duplicate := values[args[0]]; duplicate || strings.TrimSpace(args[1]) == "" {
			return "", "", "", fmt.Errorf("%s options must occur exactly once with nonempty values", command)
		}
		values[args[0]] = args[1]
		args = args[2:]
	}
	if len(values) != 3 {
		return "", "", "", fmt.Errorf("%s requires --project-root, --host and --asset-revision", command)
	}
	workspace, err := resolveProjectRoot(values["--project-root"])
	if err != nil {
		return "", "", "", err
	}
	host := model.HostKind(values["--host"])
	if _, ok := model.RuntimeForHost(host); !ok {
		return "", "", "", fmt.Errorf("%s Host must be codex or claude-code", command)
	}
	if _, err := model.ParseDigest(values["--asset-revision"]); err != nil {
		return "", "", "", fmt.Errorf("%s asset revision must be a sha256 digest", command)
	}
	return workspace, host, values["--asset-revision"], nil
}

func parseServeProjectRoot(args []string) (string, error) {
	var requested string
	switch {
	case len(args) == 0:
		current, err := os.Getwd()
		if err != nil {
			return "", fmt.Errorf("resolve project root: %w", err)
		}
		requested = current
	case len(args) == 2 && args[0] == "--project-root":
		requested = args[1]
	case len(args) == 1 && strings.HasPrefix(args[0], "--project-root="):
		requested = strings.TrimPrefix(args[0], "--project-root=")
	default:
		return "", errors.New("serve accepts only --project-root DIR")
	}
	return resolveProjectRoot(requested)
}

func resolveProjectRoot(requested string) (string, error) {
	if strings.TrimSpace(requested) == "" {
		return "", errors.New("project root is empty")
	}
	absolute, err := filepath.Abs(requested)
	if err != nil {
		return "", fmt.Errorf("resolve project root: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", fmt.Errorf("resolve project root: %w", err)
	}
	info, err := os.Lstat(resolved)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("serve project root must be a real directory")
	}
	return filepath.Clean(resolved), nil
}
