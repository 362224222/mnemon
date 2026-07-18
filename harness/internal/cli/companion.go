package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

var errManagedCompanion = errors.New("managed mnemond companion")

const (
	companionVersionTimeout = 2 * time.Second
	companionCommandTimeout = 15 * time.Second
	companionStartTimeout   = 15 * time.Second
	companionWaitDelay      = 250 * time.Millisecond
	companionVersionBytes   = 256
	companionResponseBytes  = localapi.MaxAuthorityResponseBytes
	companionStderrBytes    = 2048
	companionWriterActive   = node.OfflineAuthorityActiveExitCode
)

type companionInitializeReceipt struct {
	AssetRevision string `json:"asset_revision"`
	Created       bool   `json:"created"`
	Host          string `json:"host"`
	SchemaVersion int    `json:"schema_version"`
	Status        string `json:"status"`
}

type companionLifecycleReceipt struct {
	AssetRevision string `json:"asset_revision"`
	Changed       bool   `json:"changed"`
	Host          string `json:"host"`
	SchemaVersion int    `json:"schema_version"`
	Status        string `json:"status"`
	UpdatedAt     string `json:"updated_at"`
}

type companionRunner struct {
	executable     string
	executableInfo os.FileInfo
	workspace      string
	workspaceInfo  os.FileInfo
	commandContext func(context.Context, string, ...string) *exec.Cmd
}

type companionRunnerDependencies struct {
	currentExecutable func() (string, error)
	lookPath          func(string) (string, error)
	commandContext    func(context.Context, string, ...string) *exec.Cmd
}

func productionCompanionRunnerDependencies() companionRunnerDependencies {
	return companionRunnerDependencies{currentExecutable: os.Executable, lookPath: exec.LookPath,
		commandContext: exec.CommandContext}
}

// newCompanionRunner proves that the harness selected by PATH is the running
// physical binary and that its same-directory mnemond is an exact-version,
// non-symlink companion. The returned runner exposes no arbitrary argv path.
func newCompanionRunner(ctx context.Context, workspace, expectedVersion string) (*companionRunner, error) {
	return newCompanionRunnerWith(ctx, workspace, expectedVersion,
		productionCompanionRunnerDependencies())
}

func newCompanionRunnerWith(ctx context.Context, workspace, expectedVersion string,
	dependencies companionRunnerDependencies,
) (*companionRunner, error) {
	if ctx == nil || dependencies.currentExecutable == nil || dependencies.lookPath == nil ||
		dependencies.commandContext == nil || !validCompanionVersion(expectedVersion) {
		return nil, companionError("discovery", nil)
	}
	workspaceInfo, err := validateCompanionWorkspace(workspace)
	if err != nil {
		return nil, companionError("workspace", nil)
	}
	current, err := dependencies.currentExecutable()
	if err != nil {
		return nil, companionError("harness discovery", nil)
	}
	physicalCurrent, currentInfo, err := resolveSecureCompanionExecutable(current, false)
	if err != nil {
		return nil, companionError("harness validation", nil)
	}
	pathHarness, err := dependencies.lookPath("mnemon-harness")
	if err != nil {
		return nil, companionError("PATH validation", nil)
	}
	physicalPATHHarness, pathInfo, err := resolveSecureCompanionExecutable(pathHarness, false)
	if err != nil || physicalPATHHarness != physicalCurrent || !os.SameFile(currentInfo, pathInfo) {
		return nil, companionError("PATH validation", nil)
	}
	companion, err := resolveMnemondCompanion(physicalCurrent)
	if err != nil {
		return nil, companionError("mnemond discovery", nil)
	}
	physicalCompanion, companionInfo, err := resolveSecureCompanionExecutable(companion, true)
	if err != nil || physicalCompanion != companion {
		return nil, companionError("mnemond validation", nil)
	}
	runner := &companionRunner{executable: physicalCompanion, executableInfo: companionInfo,
		workspace: workspace, workspaceInfo: workspaceInfo,
		commandContext: dependencies.commandContext}
	stdout, err := runner.execute(ctx, "version", companionVersionTimeout,
		companionVersionBytes, "--version")
	if err != nil || string(stdout) != "mnemond version "+expectedVersion+"\n" {
		return nil, companionError("version validation", companionContextError(err))
	}
	return runner, nil
}

func (runner *companionRunner) Initialize(ctx context.Context, host model.HostKind,
	assetRevision string,
) (companionInitializeReceipt, error) {
	if runner == nil || !validCompanionAuthority(host, assetRevision) {
		return companionInitializeReceipt{}, companionError("initialize request", nil)
	}
	raw, err := runner.execute(ctx, "initialize", companionCommandTimeout,
		companionResponseBytes, "initialize", "--project-root", runner.workspace,
		"--host", string(host), "--asset-revision", assetRevision)
	if err != nil {
		return companionInitializeReceipt{}, err
	}
	var receipt companionInitializeReceipt
	if decodeErr := decodeCanonicalCompanionLine(raw, &receipt); decodeErr != nil ||
		validateInitializeReceipt(receipt, host, assetRevision) != nil {
		return companionInitializeReceipt{}, companionError("initialize response", nil)
	}
	return receipt, nil
}

func (runner *companionRunner) Inspect(ctx context.Context) (localapi.AuthorityResponse, error) {
	if runner == nil {
		return localapi.AuthorityResponse{}, companionError("inspect request", nil)
	}
	raw, err := runner.execute(ctx, "inspect", companionCommandTimeout,
		companionResponseBytes, "inspect", "--project-root", runner.workspace)
	if err != nil {
		return localapi.AuthorityResponse{}, err
	}
	var response localapi.AuthorityResponse
	if decodeErr := decodeCanonicalCompanionLine(raw, &response); decodeErr != nil ||
		validateCompanionAuthorityResponse(response) != nil {
		return localapi.AuthorityResponse{}, companionError("inspect response", nil)
	}
	return response, nil
}

// ConfirmOffline asks the closed companion command to acquire the existing
// Store writer, compare the complete expected authority, and recover only an
// unreachable owner control socket while that writer remains held. Exit 75 is
// the sole retryable process result and maps back to the Node lifecycle's
// writer-active sentinel without exposing daemon diagnostics.
func (runner *companionRunner) ConfirmOffline(ctx context.Context,
	expected localapi.AuthorityResponse,
) (localapi.AuthorityResponse, error) {
	if runner == nil {
		return localapi.AuthorityResponse{}, companionError("confirm-offline request", nil)
	}
	digest, err := localapi.AuthorityDigest(expected)
	if err != nil {
		return localapi.AuthorityResponse{}, companionError("confirm-offline request", nil)
	}
	raw, err := runner.executeClassified(ctx, "confirm-offline", companionCommandTimeout,
		companionResponseBytes, companionWriterActive, "confirm-offline", "--project-root",
		runner.workspace, "--expected-authority-digest", digest.String())
	if err != nil {
		return localapi.AuthorityResponse{}, err
	}
	var response localapi.AuthorityResponse
	if decodeErr := decodeCanonicalCompanionLine(raw, &response); decodeErr != nil ||
		validateCompanionAuthorityResponse(response) != nil || response != expected {
		return localapi.AuthorityResponse{}, companionError("confirm-offline response", nil)
	}
	return response, nil
}

func (runner *companionRunner) Activate(ctx context.Context, host model.HostKind,
	assetRevision string, expectedUpdatedAt time.Time,
) (companionLifecycleReceipt, error) {
	return runner.lifecycle(ctx, "activate", "active", host, assetRevision, expectedUpdatedAt)
}

func (runner *companionRunner) Deactivate(ctx context.Context, host model.HostKind,
	assetRevision string, expectedUpdatedAt time.Time,
) (companionLifecycleReceipt, error) {
	return runner.lifecycle(ctx, "deactivate", "inactive", host, assetRevision, expectedUpdatedAt)
}

func (runner *companionRunner) lifecycle(ctx context.Context, command, status string,
	host model.HostKind, assetRevision string, expectedUpdatedAt time.Time,
) (companionLifecycleReceipt, error) {
	expectedWire, timeErr := canonicalCompanionTime(expectedUpdatedAt)
	if runner == nil || command != "activate" && command != "deactivate" ||
		!validCompanionAuthority(host, assetRevision) || timeErr != nil {
		return companionLifecycleReceipt{}, companionError(command+" request", nil)
	}
	raw, err := runner.execute(ctx, command, companionCommandTimeout,
		companionResponseBytes, command, "--project-root", runner.workspace,
		"--host", string(host), "--asset-revision", assetRevision,
		"--expected-updated-at", expectedWire)
	if err != nil {
		return companionLifecycleReceipt{}, err
	}
	var receipt companionLifecycleReceipt
	if decodeErr := decodeCanonicalCompanionLine(raw, &receipt); decodeErr != nil ||
		validateLifecycleReceipt(receipt, status, host, assetRevision, expectedUpdatedAt) != nil {
		return companionLifecycleReceipt{}, companionError(command+" response", nil)
	}
	return receipt, nil
}

func (runner *companionRunner) execute(ctx context.Context, operation string, timeout time.Duration,
	stdoutLimit int, args ...string,
) ([]byte, error) {
	return runner.executeClassified(ctx, operation, timeout, stdoutLimit, 0, args...)
}

func (runner *companionRunner) executeClassified(ctx context.Context, operation string,
	timeout time.Duration, stdoutLimit, retryableExit int, args ...string,
) ([]byte, error) {
	if runner == nil || ctx == nil || runner.commandContext == nil || timeout <= 0 ||
		stdoutLimit <= 0 || len(args) == 0 {
		return nil, companionError(operation, nil)
	}
	if err := ctx.Err(); err != nil {
		return nil, companionError(operation, err)
	}
	if err := runner.validateFrozenPaths(); err != nil {
		return nil, companionError(operation+" authority", nil)
	}
	stdin, err := os.Open(os.DevNull)
	if err != nil {
		return nil, companionError(operation+" stdin", nil)
	}
	defer stdin.Close()
	stdout := newBoundedCompanionBuffer(stdoutLimit)
	stderr := newBoundedCompanionBuffer(companionStderrBytes)
	defer stdout.clear()
	defer stderr.clear()
	// The fixed execution budget starts only after the OS has successfully
	// created the child. Starting it before Command.Start makes scheduler and
	// fork/exec admission time consume a protocol budget even though there is
	// no companion process to terminate yet. The caller context remains bound
	// from entry through construction and Start, so explicit cancellation is
	// never deferred.
	commandCtx, cancelCommand := context.WithCancel(ctx)
	defer cancelCommand()
	command := runner.commandContext(commandCtx, runner.executable, args...)
	if command == nil {
		return nil, companionError(operation, nil)
	}
	command.Dir = runner.workspace
	command.WaitDelay = companionWaitDelay
	command.Stdin = stdin
	command.Stdout = stdout
	command.Stderr = stderr
	if err := ctx.Err(); err != nil {
		return nil, companionError(operation, err)
	}
	// Process admission is distinct from protocol execution. It has its own
	// rejection deadline so full-system fork/exec pressure cannot consume the
	// shorter version protocol budget, while a child returned by Start after
	// that deadline is killed and never accepted. os.StartProcess itself has no
	// asynchronous interruption primitive, so this boundary is checked as soon
	// as Start returns.
	startDeadline := time.Now().Add(companionStartTimeout)
	if startErr := command.Start(); startErr != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, companionError(operation, contextErr)
		}
		if !time.Now().Before(startDeadline) {
			return nil, companionError(operation, context.DeadlineExceeded)
		}
		return nil, companionError(operation, nil)
	}
	executionDeadline := time.Now().Add(timeout)
	if contextErr := ctx.Err(); contextErr != nil {
		cancelCommand()
		_ = command.Wait()
		return nil, companionError(operation, contextErr)
	}
	if !time.Now().Before(startDeadline) {
		cancelCommand()
		_ = command.Wait()
		return nil, companionError(operation, context.DeadlineExceeded)
	}
	runErr, contextErr := waitCompanionCommand(ctx, cancelCommand, command, executionDeadline)
	pathErr := runner.validateFrozenPaths()
	if contextErr == nil {
		contextErr = ctx.Err()
	}
	if contextErr != nil {
		return nil, companionError(operation, contextErr)
	}
	if runErr != nil && retryableExit > 0 && pathErr == nil && !stdout.overflowed() &&
		!stderr.overflowed() && stdout.len() == 0 && stderr.len() == 0 {
		var exitErr *exec.ExitError
		if errors.As(runErr, &exitErr) && exitErr.ExitCode() == retryableExit {
			return nil, node.ErrOfflineAuthorityActive
		}
	}
	if runErr != nil || pathErr != nil || stdout.overflowed() || stderr.overflowed() || stderr.len() != 0 {
		return nil, companionError(operation, nil)
	}
	return stdout.copyBytes(), nil
}

// waitCompanionCommand is the one completion arbiter for an already-started
// child. Wait, caller cancellation, and the fixed execution deadline compete
// explicitly. Cancellation always terminates the Cmd and drains Wait; a Wait
// result is accepted only while both caller authority and the monotonic
// absolute deadline remain live, so delayed timer delivery cannot extend the
// protocol budget.
func waitCompanionCommand(ctx context.Context, cancelCommand context.CancelFunc,
	command *exec.Cmd, executionDeadline time.Time,
) (error, error) {
	if ctx == nil || cancelCommand == nil || command == nil || executionDeadline.IsZero() ||
		command.Process == nil {
		return errors.New("companion command is unavailable"), nil
	}
	remaining := time.Until(executionDeadline)
	if remaining < 0 {
		remaining = 0
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	waited := make(chan error, 1)
	go func() { waited <- command.Wait() }()

	select {
	case runErr := <-waited:
		if err := ctx.Err(); err != nil {
			return runErr, err
		}
		if !time.Now().Before(executionDeadline) {
			return runErr, context.DeadlineExceeded
		}
		return runErr, nil
	case <-ctx.Done():
		cancelCommand()
		return <-waited, ctx.Err()
	case <-timer.C:
		cancelCommand()
		runErr := <-waited
		if err := ctx.Err(); err != nil {
			return runErr, err
		}
		return runErr, context.DeadlineExceeded
	}
}

func (runner *companionRunner) validateFrozenPaths() error {
	if runner == nil || runner.executableInfo == nil || runner.workspaceInfo == nil {
		return errors.New("runner is incomplete")
	}
	physical, info, err := resolveSecureCompanionExecutable(runner.executable, true)
	if err != nil || physical != runner.executable || !os.SameFile(runner.executableInfo, info) {
		return errors.New("mnemond identity changed")
	}
	workspaceInfo, err := validateCompanionWorkspace(runner.workspace)
	if err != nil || !os.SameFile(runner.workspaceInfo, workspaceInfo) {
		return errors.New("workspace identity changed")
	}
	return nil
}

func validateCompanionWorkspace(workspace string) (os.FileInfo, error) {
	if workspace == "" || !filepath.IsAbs(workspace) || filepath.Clean(workspace) != workspace {
		return nil, errors.New("workspace path is not absolute and canonical")
	}
	physical, err := filepath.EvalSymlinks(workspace)
	if err != nil || physical != workspace {
		return nil, errors.New("workspace path is not physical")
	}
	info, err := os.Lstat(workspace)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm()&0o022 != 0 {
		return nil, errors.New("workspace is not a real directory")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || uint32(stat.Uid) != uint32(os.Geteuid()) {
		return nil, errors.New("workspace is not owned by the current effective user")
	}
	return info, nil
}

func resolveSecureCompanionExecutable(path string, rejectSymlink bool) (string, os.FileInfo, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", nil, errors.New("executable path is not absolute and canonical")
	}
	if rejectSymlink {
		linkInfo, err := os.Lstat(path)
		if err != nil || linkInfo.Mode()&os.ModeSymlink != 0 || !linkInfo.Mode().IsRegular() {
			return "", nil, errors.New("executable must be a non-symlink regular file")
		}
	}
	physical, err := filepath.EvalSymlinks(path)
	if err != nil || !filepath.IsAbs(physical) || filepath.Clean(physical) != physical {
		return "", nil, errors.New("physical executable is unavailable")
	}
	info, err := os.Lstat(physical)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() ||
		info.Mode().Perm()&0o022 != 0 || info.Mode().Perm()&0o111 == 0 {
		return "", nil, errors.New("physical executable is unsafe")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || uint32(stat.Uid) != uint32(os.Geteuid()) && stat.Uid != 0 {
		return "", nil, errors.New("physical executable owner is unsafe")
	}
	if err := unix.Access(physical, unix.X_OK); err != nil {
		return "", nil, errors.New("physical executable is not executable")
	}
	return physical, info, nil
}

func validCompanionVersion(version string) bool {
	if len(version) == 0 || len(version) > 128 {
		return false
	}
	for _, value := range []byte(version) {
		if value < 0x21 || value > 0x7e {
			return false
		}
	}
	return true
}

func validCompanionAuthority(host model.HostKind, assetRevision string) bool {
	_, hostOK := model.RuntimeForHost(host)
	_, digestErr := model.ParseDigest(assetRevision)
	return hostOK && digestErr == nil
}

func validateInitializeReceipt(receipt companionInitializeReceipt, expectedHost model.HostKind,
	expectedRevision string,
) error {
	if receipt.SchemaVersion != model.SchemaVersion || receipt.Status != "initialized" ||
		!validCompanionAuthority(model.HostKind(receipt.Host), receipt.AssetRevision) {
		return errors.New("invalid initialization receipt")
	}
	// A created receipt proves the exact requested first authority. Replay is
	// observational: mnemond returns the frozen durable Host and revision for
	// setup to classify as an exact replay, upgrade, or conflict.
	if receipt.Created && (model.HostKind(receipt.Host) != expectedHost ||
		receipt.AssetRevision != expectedRevision) {
		return errors.New("invalid newly-created initialization receipt")
	}
	return nil
}

func validateLifecycleReceipt(receipt companionLifecycleReceipt, expectedStatus string,
	expectedHost model.HostKind, expectedRevision string, expectedUpdatedAt time.Time,
) error {
	updatedAt, timeErr := parseCanonicalCompanionTime(receipt.UpdatedAt)
	expectedWire, expectedErr := canonicalCompanionTime(expectedUpdatedAt)
	if receipt.SchemaVersion != model.SchemaVersion || receipt.Status != expectedStatus ||
		model.HostKind(receipt.Host) != expectedHost || receipt.AssetRevision != expectedRevision ||
		!validCompanionAuthority(model.HostKind(receipt.Host), receipt.AssetRevision) || timeErr != nil ||
		expectedErr != nil {
		return errors.New("invalid lifecycle receipt")
	}
	expected, _ := parseCanonicalCompanionTime(expectedWire)
	if receipt.Changed && !updatedAt.After(expected) || !receipt.Changed && !updatedAt.Equal(expected) {
		return errors.New("invalid lifecycle receipt generation")
	}
	return nil
}

func canonicalCompanionTime(value time.Time) (string, error) {
	canonical := value.Round(0).UTC()
	if canonical.IsZero() || canonical.UnixNano() <= 0 ||
		!time.Unix(0, canonical.UnixNano()).UTC().Equal(canonical) {
		return "", errors.New("invalid companion authority time")
	}
	return canonical.Format(time.RFC3339Nano), nil
}

func parseCanonicalCompanionTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || parsed.UnixNano() <= 0 || parsed.UTC().Format(time.RFC3339Nano) != value ||
		!time.Unix(0, parsed.UnixNano()).UTC().Equal(parsed.UTC()) {
		return time.Time{}, errors.New("invalid companion authority time")
	}
	return parsed.UTC(), nil
}

func validateCompanionAuthorityResponse(response localapi.AuthorityResponse) error {
	host := model.HostKind(response.Host)
	runtimeKind := model.RuntimeKind(response.Runtime)
	peerID, peerErr := model.ParsePeerID(response.PeerID)
	updatedAt, timeErr := time.Parse(time.RFC3339Nano, response.UpdatedAt)
	wantedRuntime, hostOK := model.RuntimeForHost(host)
	if response.SchemaVersion != localapi.SchemaVersion || !hostOK || runtimeKind != wantedRuntime ||
		peerErr != nil || timeErr != nil {
		return errors.New("invalid authority response")
	}
	want, err := localapi.NewAuthorityResponse(localapi.AuthoritySnapshot{Host: host,
		Runtime: runtimeKind, Enabled: response.Enabled, AssetRevision: response.AssetRevision,
		UpdatedAt: updatedAt, PeerID: peerID, ActiveAssetRevision: response.ActiveAssetRevision})
	if err != nil || want != response {
		return errors.New("invalid authority response")
	}
	return nil
}

func decodeCanonicalCompanionLine(raw []byte, destination any) error {
	if len(raw) < 3 || raw[len(raw)-1] != '\n' || bytes.ContainsAny(raw[:len(raw)-1], "\r\n") {
		return errors.New("response is not one LF-terminated line")
	}
	body := raw[:len(raw)-1]
	canonical, err := model.CanonicalizeJSON(body)
	if err != nil || !bytes.Equal(canonical, body) {
		return errors.New("response is not canonical JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return errors.New("response has an unknown or invalid field")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("response has trailing content")
	}
	closed, err := model.CanonicalMarshal(destination)
	if err != nil || !bytes.Equal(closed, body) {
		return errors.New("response does not match its closed shape")
	}
	return nil
}

func companionContextError(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return nil
}

func companionError(stage string, cause error) error {
	if cause != nil {
		return fmt.Errorf("%w: %s did not complete: %w", errManagedCompanion, stage, cause)
	}
	return fmt.Errorf("%w: %s failed", errManagedCompanion, stage)
}

type boundedCompanionBuffer struct {
	mu       sync.Mutex
	limit    int
	data     []byte
	overflow bool
}

func newBoundedCompanionBuffer(limit int) *boundedCompanionBuffer {
	return &boundedCompanionBuffer{limit: limit, data: make([]byte, 0, limit)}
}

func (buffer *boundedCompanionBuffer) Write(value []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	available := buffer.limit - len(buffer.data)
	if available < len(value) {
		buffer.overflow = true
	}
	if available > 0 {
		if available > len(value) {
			available = len(value)
		}
		buffer.data = append(buffer.data, value[:available]...)
	}
	return len(value), nil
}

func (buffer *boundedCompanionBuffer) copyBytes() []byte {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return append([]byte(nil), buffer.data...)
}

func (buffer *boundedCompanionBuffer) overflowed() bool {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.overflow
}

func (buffer *boundedCompanionBuffer) len() int {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return len(buffer.data)
}

func (buffer *boundedCompanionBuffer) clear() {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	clear(buffer.data)
	buffer.data = buffer.data[:0]
}
