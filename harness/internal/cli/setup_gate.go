package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
)

const (
	setupHookOutputLimit = 512
	setupHookRetryLimit  = time.Second
	setupHookRetryPoll   = 20 * time.Millisecond
)

var errSetupHookGate = errors.New("managed Host Hook self-check")

// setupHookGate executes the actual projected Host Hook after authenticated
// health becomes ready. It supplies no synthetic Event or Run capability: a
// setup process may have inherited an attachment environment variable, so the
// gate removes that variable before proving the ordinary external-Hook path.
type setupHookGate struct {
	workspace string
	hook      string
	identity  os.FileInfo
}

func newSetupHookGate(workspace string, host assets.Host) (*setupHookGate, error) {
	hostRoot := map[assets.Host]string{
		assets.HostCodex: ".codex", assets.HostClaudeCode: ".claude",
	}[host]
	if hostRoot == "" || workspace == "" || !filepath.IsAbs(workspace) ||
		filepath.Clean(workspace) != workspace {
		return nil, setupHookError("configure", nil)
	}
	hook := filepath.Join(workspace, hostRoot, "hooks", "mnemon-harness", "hook.sh")
	identity, err := validateSetupHookPath(hook)
	if err != nil {
		return nil, setupHookError("validate projection", nil)
	}
	return &setupHookGate{workspace: workspace, hook: hook, identity: identity}, nil
}

func (gate *setupHookGate) VerifyReady(ctx context.Context,
	_ localapi.HealthResponse,
) error {
	deadline := time.Now().Add(setupHookRetryLimit)
	for {
		retry, err := gate.verifyReadyOnce(ctx)
		if err == nil || !retry || time.Now().After(deadline) {
			return err
		}
		timer := time.NewTimer(setupHookRetryPoll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return setupHookError("execute", ctx.Err())
		case <-timer.C:
		}
	}
}

func (gate *setupHookGate) verifyReadyOnce(ctx context.Context) (bool, error) {
	if gate == nil || ctx == nil || gate.identity == nil {
		return false, setupHookError("execute", nil)
	}
	if err := ctx.Err(); err != nil {
		return false, setupHookError("execute", err)
	}
	before, err := validateSetupHookPath(gate.hook)
	if err != nil || !os.SameFile(gate.identity, before) {
		return false, setupHookError("revalidate projection", nil)
	}
	var stdout, stderr boundedSetupHookBuffer
	command := exec.CommandContext(ctx, gate.hook)
	command.Dir = gate.workspace
	command.Stdin = bytes.NewReader(nil)
	command.Stdout = &stdout
	command.Stderr = &stderr
	command.Env = withoutEnvironment(os.Environ(), localapi.RunAttachmentEnv)
	runErr := command.Run()
	defer stdout.clear()
	defer stderr.clear()
	after, pathErr := validateSetupHookPath(gate.hook)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return false, setupHookError("execute", ctxErr)
	}
	if runErr != nil || pathErr != nil || !os.SameFile(gate.identity, after) ||
		stdout.overflow || stderr.overflow || stderr.data.Len() != 0 {
		retry := pathErr == nil && os.SameFile(gate.identity, after) &&
			!stdout.overflow && stdout.data.Len() == 0 && !stderr.overflow &&
			setupHookRetryable(runErr, stderr.data.String())
		return retry, setupHookError("execute", nil)
	}
	output := stdout.data.String()
	if output != "" && output != WakeCue+"\n" {
		return false, setupHookError("validate output", nil)
	}
	return false, nil
}

func setupHookRetryable(runErr error, stderr string) bool {
	return runErr != nil && strings.Contains(stderr, "asset_revision_mismatch:")
}

func validateSetupHookPath(path string) (os.FileInfo, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("Hook path is not absolute and canonical")
	}
	physical, err := filepath.EvalSymlinks(path)
	if err != nil || physical != path {
		return nil, errors.New("Hook path contains a symlink")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm() != 0o755 {
		return nil, errors.New("Hook is not a canonical executable")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 {
		return nil, errors.New("Hook owner or link count is unsafe")
	}
	return info, nil
}

func withoutEnvironment(environment []string, removed string) []string {
	prefix := removed + "="
	result := make([]string, 0, len(environment))
	for _, entry := range environment {
		if entry != removed && !strings.HasPrefix(entry, prefix) {
			result = append(result, entry)
		}
	}
	return result
}

type boundedSetupHookBuffer struct {
	data     bytes.Buffer
	overflow bool
}

func (buffer *boundedSetupHookBuffer) Write(value []byte) (int, error) {
	written := len(value)
	remaining := setupHookOutputLimit - buffer.data.Len()
	if remaining < len(value) {
		buffer.overflow = true
	}
	if remaining > 0 {
		if remaining > len(value) {
			remaining = len(value)
		}
		_, _ = buffer.data.Write(value[:remaining])
	}
	return written, nil
}

func (buffer *boundedSetupHookBuffer) clear() {
	if buffer == nil {
		return
	}
	raw := buffer.data.Bytes()
	clear(raw)
	buffer.data.Reset()
	buffer.overflow = false
}

func setupHookError(stage string, cause error) error {
	if cause != nil {
		return fmt.Errorf("%w: %s: %w", errSetupHookGate, stage, cause)
	}
	return fmt.Errorf("%w: %s", errSetupHookGate, stage)
}
