package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
)

const (
	hostProbeTimeout   = 2 * time.Second
	hostProbeOutputMax = 8 << 10
	hostVersionMax     = 256
)

var ErrHostUnavailable = errors.New("managed Host adapter is unavailable")

type HostObservation struct {
	Host       assets.Host
	Executable string
	Version    string
}

type hostLookup func(string) (string, error)
type hostRun func(context.Context, string, ...string) ([]byte, error)

// DetectHost resolves an explicit Host or chooses the first fully verifiable
// T0 adapter. Auto selection prefers Codex and falls back to Claude Code only
// when the complete Codex binary/version/app-server preflight is unavailable.
func DetectHost(ctx context.Context, selection string) (HostObservation, error) {
	return detectHost(ctx, selection, exec.LookPath, runHostProbe)
}

// InspectHost repeats the same bounded binary and adapter preflight for an
// already frozen Profile Host. Registration observation remains a separate
// VerifyHostProjection gate because it is workspace-specific.
func InspectHost(ctx context.Context, host assets.Host) (HostObservation, error) {
	if !host.Valid() {
		return HostObservation{}, fmt.Errorf("%w: unknown Host", ErrHostUnavailable)
	}
	return inspectHost(ctx, host, exec.LookPath, runHostProbe)
}

func detectHost(ctx context.Context, selection string, lookup hostLookup,
	run hostRun,
) (HostObservation, error) {
	if ctx == nil || lookup == nil || run == nil {
		return HostObservation{}, fmt.Errorf("%w: detection dependencies are unavailable", ErrHostUnavailable)
	}
	selection = strings.TrimSpace(selection)
	if selection != "auto" && selection != string(assets.HostCodex) &&
		selection != string(assets.HostClaudeCode) {
		return HostObservation{}, fmt.Errorf("%w: Host must be auto, codex or claude-code", ErrHostUnavailable)
	}
	if selection != "auto" {
		return inspectHost(ctx, assets.Host(selection), lookup, run)
	}
	var failures []error
	for _, host := range []assets.Host{assets.HostCodex, assets.HostClaudeCode} {
		observation, err := inspectHost(ctx, host, lookup, run)
		if err == nil {
			return observation, nil
		}
		failures = append(failures, err)
		if ctx.Err() != nil {
			return HostObservation{}, fmt.Errorf("%w: %v", ErrHostUnavailable, ctx.Err())
		}
	}
	return HostObservation{}, fmt.Errorf("%w: no supported Host passed preflight: %v",
		ErrHostUnavailable, errors.Join(failures...))
}

func inspectHost(ctx context.Context, host assets.Host, lookup hostLookup,
	run hostRun,
) (HostObservation, error) {
	if ctx == nil || lookup == nil || run == nil || !host.Valid() {
		return HostObservation{}, fmt.Errorf("%w: inspection input is invalid", ErrHostUnavailable)
	}
	binary := map[assets.Host]string{assets.HostCodex: "codex", assets.HostClaudeCode: "claude"}[host]
	path, err := lookup(binary)
	if err != nil {
		return HostObservation{}, fmt.Errorf("%w: %s binary was not found", ErrHostUnavailable, host)
	}
	path, err = verifyHostExecutable(path)
	if err != nil {
		return HostObservation{}, fmt.Errorf("%w: %s: %v", ErrHostUnavailable, host, err)
	}
	versionRaw, err := run(ctx, path, "--version")
	if err != nil {
		return HostObservation{}, fmt.Errorf("%w: %s version probe failed: %v", ErrHostUnavailable, host, err)
	}
	version, err := canonicalHostVersion(versionRaw)
	if err != nil {
		return HostObservation{}, fmt.Errorf("%w: %s version is invalid", ErrHostUnavailable, host)
	}
	helpArgs := []string{"--help"}
	wantUsage := "Usage: claude"
	if host == assets.HostCodex {
		helpArgs, wantUsage = []string{"app-server", "--help"}, "Usage: codex app-server"
	}
	help, err := run(ctx, path, helpArgs...)
	if err != nil || len(help) == 0 || len(help) > hostProbeOutputMax || !utf8.Valid(help) ||
		!bytes.Contains(help, []byte(wantUsage)) {
		return HostObservation{}, fmt.Errorf("%w: %s adapter surface did not pass preflight",
			ErrHostUnavailable, host)
	}
	return HostObservation{Host: host, Executable: path, Version: version}, nil
}

func verifyHostExecutable(path string) (string, error) {
	if path == "" {
		return "", errors.New("executable path is empty")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", errors.New("executable path cannot be resolved")
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil || !filepath.IsAbs(resolved) || filepath.Clean(resolved) != resolved {
		return "", errors.New("executable path is unavailable")
	}
	info, err := os.Lstat(resolved)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return "", errors.New("executable is not a runnable regular file")
	}
	if info.Mode().Perm()&0o022 != 0 {
		return "", errors.New("executable is writable by group or other")
	}
	return resolved, nil
}

func canonicalHostVersion(raw []byte) (string, error) {
	if len(raw) == 0 || len(raw) > hostVersionMax || !utf8.Valid(raw) || bytes.IndexByte(raw, 0) >= 0 {
		return "", errors.New("Host version has noncanonical bytes")
	}
	version := strings.TrimSpace(string(raw))
	if version == "" || strings.ContainsAny(version, "\r\n") || len(version) > hostVersionMax {
		return "", errors.New("Host version must be one bounded line")
	}
	return version, nil
}

func runHostProbe(ctx context.Context, executable string, args ...string) ([]byte, error) {
	if ctx == nil || executable == "" {
		return nil, errors.New("Host probe is unavailable")
	}
	probeCtx, cancel := context.WithTimeout(ctx, hostProbeTimeout)
	defer cancel()
	var output boundedProbeBuffer
	command := exec.CommandContext(probeCtx, executable, args...)
	command.Stdin = nil
	command.Stdout, command.Stderr = &output, &output
	if err := command.Run(); err != nil {
		if probeCtx.Err() != nil {
			return nil, probeCtx.Err()
		}
		return nil, err
	}
	return output.Bytes(), nil
}

type boundedProbeBuffer struct {
	buffer bytes.Buffer
	full   bool
}

func (buffer *boundedProbeBuffer) Write(value []byte) (int, error) {
	if buffer == nil || buffer.full || buffer.buffer.Len()+len(value) > hostProbeOutputMax {
		if buffer != nil {
			buffer.full = true
		}
		return 0, errors.New("Host probe output exceeds its closed bound")
	}
	return buffer.buffer.Write(value)
}

func (buffer *boundedProbeBuffer) Bytes() []byte {
	if buffer == nil || buffer.full {
		return nil
	}
	return append([]byte(nil), buffer.buffer.Bytes()...)
}

var _ io.Writer = (*boundedProbeBuffer)(nil)
