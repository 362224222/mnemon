//go:build darwin || linux

package process_test

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

const (
	lc04OfferContent         = "LC-04 concurrent current ownership"
	lc04RuntimeStartedMarker = ".lc04-runtime-started"
	lc04RuntimeDoneMarker    = ".lc04-runtime-done"
	lc04RuntimeReleaseFIFO   = ".lc04-runtime-release"
)

func init() {
	if os.Getenv("LC_MNEMON_LC04_CODEX_HELPER") == "1" {
		os.Exit(lc04RunCodexHelper())
	}
}

// lc04RunCodexHelper is reached only when mnemond launches the test binary
// through the external codex trampoline installed below. It implements the
// ordinary app-server JSONL contract and exits before the testing package can
// parse arguments or emit its own output.
func lc04RunCodexHelper() int {
	switch {
	case len(os.Args) == 2 && os.Args[1] == "--version":
		_, _ = io.WriteString(os.Stdout, "codex process-test\n")
		return 0
	case len(os.Args) == 3 && os.Args[1] == "app-server" && os.Args[2] == "--help":
		_, _ = io.WriteString(os.Stdout, "Usage: codex app-server\n")
		return 0
	case len(os.Args) != 3 || os.Args[1] != "app-server" || os.Args[2] != "--stdio":
		return 64
	default:
		return lc04ServeCodexAppServer()
	}
}

func lc04ServeCodexAppServer() int {
	workspace, err := os.Getwd()
	if err != nil {
		return 65
	}
	decoder := json.NewDecoder(bufio.NewReader(os.Stdin))
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)

	if !lc04InitializeCodex(decoder, encoder) {
		return 66
	}
	if !lc04RegisterCodexHook(decoder, encoder, workspace) {
		return 67
	}
	if status := lc04WaitForRuntimeRelease(workspace); status != 0 {
		return status
	}
	if !lc04StartCodexTurn(decoder, encoder, workspace) {
		return 71
	}
	return lc04CompleteCodexTurn(encoder, workspace)
}

func lc04InitializeCodex(decoder *json.Decoder, encoder *json.Encoder) bool {
	if !lc04ReadCodexMethod(decoder, "initialize") ||
		encoder.Encode(map[string]any{"id": 1, "result": map[string]any{
			"codexHome": "/bounded", "platformFamily": "unix",
			"platformOs": "test", "userAgent": "process-test",
		}}) != nil ||
		!lc04ReadCodexMethod(decoder, "initialized") {
		return false
	}
	return true
}

func lc04RegisterCodexHook(
	decoder *json.Decoder,
	encoder *json.Encoder,
	workspace string,
) bool {
	if !lc04ReadCodexMethod(decoder, "hooks/list") {
		return false
	}
	hook := map[string]any{
		"command":     filepath.Join(workspace, ".codex", "hooks", "mnemon-harness", "hook.sh"),
		"currentHash": "sha256:process-test", "displayOrder": 1, "enabled": true,
		"eventName": "userPromptSubmit", "handlerType": "command", "isManaged": false,
		"key": "project:userPromptSubmit:1", "matcher": nil, "pluginId": nil,
		"source": "project", "sourcePath": filepath.Join(workspace, ".codex", "hooks.json"),
		"statusMessage": "Checking Mnemon Teamwork", "timeoutSec": 10,
		"trustStatus": "trusted",
	}
	if encoder.Encode(map[string]any{"id": 2, "result": map[string]any{
		"data": []any{map[string]any{"cwd": workspace, "errors": []any{},
			"hooks": []any{hook}, "warnings": []any{}}},
	}}) != nil || !lc04ReadCodexMethod(decoder, "thread/start") {
		return false
	}
	return true
}

func lc04WaitForRuntimeRelease(workspace string) int {
	pid := []byte(strconv.Itoa(os.Getpid()) + "\n")
	if err := os.WriteFile(filepath.Join(workspace, lc04RuntimeStartedMarker), pid, 0o600); err != nil {
		return 68
	}
	release, err := os.Open(filepath.Join(workspace, lc04RuntimeReleaseFIFO))
	if err != nil {
		return 69
	}
	line, readErr := bufio.NewReader(release).ReadString('\n')
	closeErr := release.Close()
	if readErr != nil || closeErr != nil || line != "release\n" {
		return 70
	}
	return 0
}

func lc04StartCodexTurn(
	decoder *json.Decoder,
	encoder *json.Encoder,
	workspace string,
) bool {
	threadResult := map[string]any{
		"approvalPolicy": "never", "approvalsReviewer": "user",
		"cwd": workspace, "model": "process-test", "modelProvider": "openai",
		"sandbox": map[string]any{
			"excludeSlashTmp": false, "excludeTmpdirEnvVar": false,
			"networkAccess": false, "type": "workspaceWrite", "writableRoots": []any{},
		},
		"thread": map[string]any{
			"cliVersion": "process-test", "createdAt": 1000, "cwd": workspace,
			"ephemeral": true, "id": "thread-managed", "modelProvider": "openai",
			"preview": "", "sessionId": "thread-managed", "source": "process-test",
			"status": map[string]any{"type": "idle"}, "turns": []any{}, "updatedAt": 1000,
		},
	}
	if encoder.Encode(map[string]any{"id": 3, "result": threadResult}) != nil ||
		!lc04ReadCodexMethod(decoder, "turn/start") ||
		encoder.Encode(map[string]any{"id": 4, "result": map[string]any{
			"turn": map[string]any{
				"id": "turn-managed", "items": []any{}, "status": "inProgress",
			},
		}}) != nil {
		return false
	}
	return true
}

func lc04CompleteCodexTurn(encoder *json.Encoder, workspace string) int {
	sourcePath := filepath.Join(workspace, ".codex", "hooks.json")
	started := map[string]any{
		"displayOrder": 1, "entries": []any{}, "eventName": "userPromptSubmit",
		"executionMode": "sync", "handlerType": "command", "id": "hook-managed",
		"scope": "turn", "source": "project", "sourcePath": sourcePath,
		"startedAt": 1000, "status": "running",
		"statusMessage": "Checking Mnemon Teamwork",
	}
	completed := map[string]any{
		"completedAt": 1003, "displayOrder": 1, "durationMs": 3,
		"entries": []any{map[string]any{
			"kind": "context",
			"text": "[mnemon:wake] Managed work is pending. Use the Mnemon Harness skill to process one Event.",
		}},
		"eventName": "userPromptSubmit", "executionMode": "sync",
		"handlerType": "command", "id": "hook-managed", "scope": "turn",
		"source": "project", "sourcePath": sourcePath, "startedAt": 1000,
		"status": "completed", "statusMessage": "Checking Mnemon Teamwork",
	}
	for _, notification := range []any{
		map[string]any{"method": "hook/started", "params": map[string]any{
			"run": started, "threadId": "thread-managed", "turnId": "turn-managed",
		}},
		map[string]any{"method": "hook/completed", "params": map[string]any{
			"run": completed, "threadId": "thread-managed", "turnId": "turn-managed",
		}},
		map[string]any{"method": "turn/completed", "params": map[string]any{
			"threadId": "thread-managed", "turn": map[string]any{
				"id": "turn-managed", "items": []any{}, "status": "completed",
			},
		}},
	} {
		if encoder.Encode(notification) != nil {
			return 72
		}
	}
	if err := os.WriteFile(filepath.Join(workspace, lc04RuntimeDoneMarker), nil, 0o600); err != nil {
		return 73
	}
	return 0
}

func lc04ReadCodexMethod(decoder *json.Decoder, want string) bool {
	var envelope struct {
		Method string `json:"method"`
	}
	return decoder.Decode(&envelope) == nil && envelope.Method == want
}

// TestPublicConcurrentCurrentHasOneOwnerAndObservableLoser proves the LC-04
// Runtime fence with ordinary mnemon-harness and mnemond processes. Two public
// current commands inherit the same mnemond-created Run attachment and leave
// the attachment CAS as their only ownership decision. The losing process
// must expose a stable public error, while a read-only post-shutdown SQLite
// observation proves that the race created no second Handling or Run.
func TestPublicConcurrentCurrentHasOneOwnerAndObservableLoser(t *testing.T) {
	fixture := channelProcessSetupFixture(t)
	owner, reviewer := fixture.nodes["A"], fixture.nodes["B"]

	alpha := channelProcessCreate(t, fixture.harnessExecutable, owner, "Alpha", "alpha")
	channelProcessJoinWithToken(t, fixture.harnessExecutable, reviewer, "alpha",
		alpha.InviteToken)
	views := channelProcessWaitReadyViews(t, fixture.harnessExecutable, fixture.nodes,
		[]string{"A", "B"}, "A", "alpha", "Alpha",
		[]string{owner.peerID, reviewer.peerID})
	reviewerAlias, err := channelProcessMemberAlias(views["A"], reviewer.peerID)
	if err != nil {
		t.Fatal(err)
	}

	lc04InstallControlledCodex(t, filepath.Join(filepath.Dir(fixture.harnessExecutable), "codex"))
	releaseRuntime := lc04HoldRuntime(t, reviewer.workspace)
	t.Cleanup(func() { _ = releaseRuntime() })

	contentPath := filepath.Join(owner.workspace, "lc04-offer.txt")
	if err := os.WriteFile(contentPath, []byte(lc04OfferContent), 0o600); err != nil {
		t.Fatal(err)
	}
	offeredResult := channelProcessRun(t, fixture.harnessExecutable, owner,
		"teamwork", "offer", "--channel", "alpha", "--to", reviewerAlias,
		"--deadline", "24h", "--content-file", filepath.Base(contentPath), "--json")
	offered, err := channelProcessDecode[localapi.OperationResponse](offeredResult)
	if err != nil || offered.SchemaVersion != 1 || offered.Status != "accepted" ||
		offered.Action != "teamwork.offer" || offered.Replayed || offered.Handling != nil ||
		len(offered.Results) != 1 || offered.OperationID == "" || offered.Receipt == "" {
		t.Fatalf("public offer = (status=%q action=%q results=%d, %v)",
			offered.Status, offered.Action, len(offered.Results), err)
	}

	runtimePID := lc04WaitForRuntimeStart(t, fixture.harnessExecutable, reviewer)
	attachmentPath := lc04RequireSingleAttachment(t, reviewer.nodeState)
	results := lc04RaceCurrentProcesses(t, fixture.harnessExecutable, reviewer, attachmentPath)

	winner, loser := lc04SplitCurrentRace(t, results)
	contextPath, projectionDigest := lc04AssertActionableCurrent(t, reviewer,
		attachmentPath, winner)
	lc04AssertObservableLoser(t, attachmentPath, loser)
	lc04AssertSingleOwnerFile(t, reviewer.nodeState, attachmentPath, contextPath)
	lc04WaitPublicOwner(t, fixture.harnessExecutable, reviewer)

	if err := releaseRuntime(); err != nil {
		t.Fatalf("release controlled Runtime: %v", err)
	}
	lc04WaitForRegularFile(t, filepath.Join(reviewer.workspace, lc04RuntimeDoneMarker))
	lc04WaitForProcessExit(t, runtimePID)
	op01StopProcessNode(t, reviewer)
	lc04AssertOfflineSinglePersistence(t, reviewer.nodeState, projectionDigest)
}

// lc04InstallControlledCodex replaces only the external test Runtime after
// setup has selected it. mnemond still performs its production projection
// verification and launches this executable through the production adapter.
func lc04InstallControlledCodex(t *testing.T, path string) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	script := fmt.Sprintf("#!/bin/sh\nLC_MNEMON_LC04_CODEX_HELPER=1\n"+
		"export LC_MNEMON_LC04_CODEX_HELPER\nexec %s \"$@\"\n",
		lc04ShellQuote(executable))
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatal(err)
	}
}

func lc04ShellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func lc04HoldRuntime(t *testing.T, workspace string) func() error {
	t.Helper()
	path := filepath.Join(workspace, lc04RuntimeReleaseFIFO)
	if err := unix.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	descriptor, err := unix.Open(path, unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	file := os.NewFile(uintptr(descriptor), path)
	released := false
	return func() error {
		if released {
			return nil
		}
		released = true
		_, writeErr := io.WriteString(file, "release\n")
		closeErr := file.Close()
		return errors.Join(writeErr, closeErr)
	}
}

func lc04WaitForRegularFile(t *testing.T, path string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), channelProcessConvergenceTimeout)
	defer cancel()
	for {
		info, err := os.Lstat(path)
		if err == nil && info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 {
			return
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("inspect Runtime synchronization marker: %v", err)
		}
		if err := setupProcessPoll(ctx); err != nil {
			t.Fatalf("controlled Runtime did not reach %s", filepath.Base(path))
		}
	}
}

func lc04WaitForRuntimeStart(t *testing.T, executable string, node *channelProcessNode) int {
	t.Helper()
	path := filepath.Join(node.workspace, lc04RuntimeStartedMarker)
	ctx, cancel := context.WithTimeout(context.Background(), channelProcessConvergenceTimeout)
	defer cancel()
	for {
		info, err := os.Lstat(path)
		if err == nil && info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 {
			raw, readErr := os.ReadFile(path)
			pid, parseErr := strconv.Atoi(strings.TrimSuffix(string(raw), "\n"))
			if readErr != nil || parseErr != nil || pid <= 1 ||
				!bytes.Equal(raw, []byte(strconv.Itoa(pid)+"\n")) {
				t.Fatalf("controlled Runtime PID marker is invalid: read=%v parse=%v",
					readErr, parseErr)
			}
			return pid
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("inspect Runtime start marker: %v", err)
		}
		if err := setupProcessPoll(ctx); err == nil {
			continue
		}

		statusCtx, statusCancel := context.WithTimeout(context.Background(), 5*time.Second)
		result := setupProcessRunHarness(statusCtx, executable, node.workspace,
			node.environment, "status")
		statusCancel()
		decoded := result
		decoded.err = nil
		status, statusErr := setupProcessParseStatus(decoded)
		entries, entriesErr := os.ReadDir(filepath.Join(node.nodeState, "runs"))
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("controlled Runtime did not start: public status=%#v status_error=%v "+
			"process_exit=%v stdout=%s stderr=%s overflow=%t "+
			"run_files=%v run_files_error=%v",
			status, statusErr, result.err, setupProcessFingerprint(result.stdout),
			setupProcessFingerprint(result.stderr), result.overflow, names, entriesErr)
	}
}

func lc04WaitForProcessExit(t *testing.T, pid int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		err := unix.Kill(pid, 0)
		if errors.Is(err, unix.ESRCH) {
			return
		}
		if err != nil {
			t.Fatalf("observe controlled Runtime PID %d: %v", pid, err)
		}
		if err := setupProcessPoll(ctx); err != nil {
			t.Fatalf("controlled Runtime PID %d did not exit", pid)
		}
	}
}

func lc04RequireSingleAttachment(t *testing.T, nodeState string) string {
	t.Helper()
	runs := filepath.Join(nodeState, "runs")
	entries, err := os.ReadDir(runs)
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".attach") {
			paths = append(paths, filepath.Join(runs, entry.Name()))
		}
	}
	if len(paths) != 1 {
		t.Fatalf("published Run attachments = %d, want 1", len(paths))
	}
	info, err := os.Lstat(paths[0])
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm() != 0o600 {
		t.Fatalf("published Run attachment is not one owner-only regular file: %v", err)
	}
	return paths[0]
}

func lc04RaceCurrentProcesses(t *testing.T, executable string, node *channelProcessNode,
	attachmentPath string,
) [2]setupProcessResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	type process struct {
		command *exec.Cmd
		permit  *os.File
		stdout  *setupProcessOutput
		stderr  *setupProcessOutput
	}
	var processes [2]process
	for index := range processes {
		readPermit, writePermit, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		stdout := newSetupProcessOutput(commandOutputMax)
		stderr := newSetupProcessOutput(commandOutputMax)
		command := exec.CommandContext(ctx, "/bin/sh", "-c",
			`IFS= read -r permit <&3; test "$permit" = go; exec "$@"`,
			"lc04-current", executable, "agent", "current", "--json")
		command.Dir = node.workspace
		command.Env = lc04AttachmentEnvironment(node.environment, attachmentPath)
		command.ExtraFiles = []*os.File{readPermit}
		command.Stdin = nil
		command.Stdout = stdout
		command.Stderr = stderr
		command.WaitDelay = 250 * time.Millisecond
		if err := command.Start(); err != nil {
			_ = readPermit.Close()
			_ = writePermit.Close()
			for previous := 0; previous < index; previous++ {
				_ = processes[previous].permit.Close()
				_ = processes[previous].command.Process.Kill()
				_ = processes[previous].command.Wait()
			}
			t.Fatal(err)
		}
		if err := readPermit.Close(); err != nil {
			t.Fatal(err)
		}
		processes[index] = process{command: command, permit: writePermit,
			stdout: stdout, stderr: stderr}
	}
	for index := range processes {
		if _, err := io.WriteString(processes[index].permit, "go\n"); err != nil {
			t.Fatal(err)
		}
		if err := processes[index].permit.Close(); err != nil {
			t.Fatal(err)
		}
	}

	var results [2]setupProcessResult
	for index := range processes {
		runErr := processes[index].command.Wait()
		results[index] = setupProcessResult{
			stdout: processes[index].stdout.bytes(),
			stderr: processes[index].stderr.bytes(),
			overflow: processes[index].stdout.overflowed() ||
				processes[index].stderr.overflowed(),
			err: runErr,
		}
	}
	if ctx.Err() != nil {
		t.Fatalf("concurrent current processes exceeded their deadline: %v", ctx.Err())
	}
	return results
}

func lc04AttachmentEnvironment(environment []string, path string) []string {
	result := make([]string, 0, len(environment)+1)
	prefix := localapi.RunAttachmentEnv + "="
	for _, entry := range environment {
		if !strings.HasPrefix(entry, prefix) {
			result = append(result, entry)
		}
	}
	return append(result, prefix+path)
}

func lc04SplitCurrentRace(t *testing.T,
	results [2]setupProcessResult,
) (setupProcessResult, setupProcessResult) {
	t.Helper()
	var winner, loser setupProcessResult
	successes := 0
	for _, result := range results {
		if result.err == nil {
			winner = result
			successes++
		} else {
			loser = result
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent current successes = %d, want exactly 1; first=(%v,%s,%s) "+
			"second=(%v,%s,%s)", successes,
			results[0].err, setupProcessFingerprint(results[0].stdout),
			setupProcessFingerprint(results[0].stderr), results[1].err,
			setupProcessFingerprint(results[1].stdout),
			setupProcessFingerprint(results[1].stderr))
	}
	return winner, loser
}

func lc04AssertActionableCurrent(t *testing.T, node *channelProcessNode,
	attachmentPath string, result setupProcessResult,
) (string, model.Digest) {
	t.Helper()
	fields := lc04DecodeCurrentFields(t, result)
	contextPath := lc04RequireCurrentOwnerFields(t, fields, attachmentPath)
	projection := lc04RequireCurrentProjection(t, fields)
	lc04RequireOwnerContextFile(t, node, contextPath)
	return contextPath, projection.Digest()
}

func lc04DecodeCurrentFields(t *testing.T,
	result setupProcessResult,
) map[string]json.RawMessage {
	t.Helper()
	if result.err != nil || result.overflow || len(result.stderr) != 0 ||
		len(result.stdout) < 2 || result.stdout[len(result.stdout)-1] != '\n' {
		t.Fatalf("winning current has an invalid public envelope: exit=%v stdout=%s stderr=%s",
			result.err, setupProcessFingerprint(result.stdout),
			setupProcessFingerprint(result.stderr))
	}
	object := result.stdout[:len(result.stdout)-1]
	canonical, err := model.CanonicalizeJSON(object)
	if err != nil || !bytes.Equal(canonical, object) {
		t.Fatal("winning current is not one canonical JSON object")
	}
	var fields map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(object))
	if err := decoder.Decode(&fields); err != nil || fields == nil {
		t.Fatalf("decode winning current: %v", err)
	}
	return fields
}

func lc04RequireCurrentOwnerFields(t *testing.T, fields map[string]json.RawMessage,
	attachmentPath string,
) string {
	t.Helper()
	var status, contextPath string
	if err := json.Unmarshal(fields["status"], &status); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(fields["context_file"], &contextPath); err != nil {
		t.Fatal(err)
	}
	wantContext := strings.TrimSuffix(attachmentPath, ".attach") + ".context"
	if status != "actionable" || contextPath != wantContext {
		t.Fatalf("winning current = status %q context %q, want actionable %q",
			status, contextPath, wantContext)
	}
	for _, forbidden := range []string{"run_id", "claim_secret", "claim_token",
		"attachment", "attachment_token"} {
		if fields[forbidden] != nil {
			t.Fatalf("winning current exposed forbidden authority field %q", forbidden)
		}
	}
	return contextPath
}

func lc04RequireCurrentProjection(t *testing.T,
	fields map[string]json.RawMessage,
) model.CurrentProjection {
	t.Helper()
	delete(fields, "status")
	delete(fields, "context_file")
	projectionRaw, err := model.CanonicalMarshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := model.ParseCurrentProjection(projectionRaw)
	if err != nil {
		t.Fatalf("winning current did not carry the closed public projection: %v", err)
	}
	brief, hasBrief := projection.ActionWork().Brief()
	if !hasBrief || brief.Content() != lc04OfferContent ||
		projection.SourceEvent().Type() != model.EventReviewOffered ||
		projection.ActionWork().State() != model.WorkOffered ||
		projection.ActionWork().LocalRole() != model.CurrentReviewer {
		t.Fatal("winning current did not bind the offered reviewer work")
	}
	return projection
}

func lc04RequireOwnerContextFile(t *testing.T, node *channelProcessNode,
	contextPath string,
) {
	t.Helper()
	info, err := os.Lstat(contextPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm() != 0o600 {
		t.Fatalf("winning context is not one owner-only regular file: %v", err)
	}
	if !strings.HasPrefix(contextPath, filepath.Join(node.nodeState, "runs")+string(os.PathSeparator)) {
		t.Fatal("winning context escaped the Node run directory")
	}
}

func lc04AssertObservableLoser(t *testing.T, attachmentPath string,
	result setupProcessResult,
) {
	t.Helper()
	var exitErr *exec.ExitError
	if !errors.As(result.err, &exitErr) || exitErr.ExitCode() != 3 {
		t.Fatalf("losing current exit = %v, want public domain exit 3", result.err)
	}
	decodedResult := result
	decodedResult.err = nil
	apiErr, err := channelProcessDecode[localapi.APIError](decodedResult)
	if err != nil || apiErr.SchemaVersion != 1 || apiErr.Status != "error" ||
		(apiErr.Code != localapi.CodeContextStale &&
			apiErr.Code != localapi.CodeContextInvalid) ||
		apiErr.Retryable || apiErr.Replayed || apiErr.OperationID != nil ||
		strings.Contains(apiErr.Message, attachmentPath) {
		t.Fatalf("losing current public error = (%#v, %v)", apiErr, err)
	}
}

func lc04AssertSingleOwnerFile(t *testing.T, nodeState, attachmentPath,
	contextPath string,
) {
	t.Helper()
	if _, err := os.Lstat(attachmentPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("consumed attachment remains after ownership race: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(nodeState, "runs"))
	if err != nil {
		t.Fatal(err)
	}
	contexts := 0
	for _, entry := range entries {
		switch {
		case strings.HasSuffix(entry.Name(), ".attach"):
			t.Fatalf("an attachment remains after ownership race: %s", entry.Name())
		case strings.HasSuffix(entry.Name(), ".context"):
			contexts++
			if filepath.Join(nodeState, "runs", entry.Name()) != contextPath {
				t.Fatalf("unexpected competing context file %s", entry.Name())
			}
		}
	}
	if contexts != 1 {
		t.Fatalf("owner context files = %d, want exactly 1", contexts)
	}
}

func lc04WaitPublicOwner(t *testing.T, executable string, node *channelProcessNode) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), channelProcessConvergenceTimeout)
	defer cancel()
	var last localapi.StatusChannelRuntime
	for {
		result := setupProcessRunHarness(ctx, executable, node.workspace, node.environment,
			"status")
		decoded := result
		decoded.err = nil
		response, err := setupProcessParseStatus(decoded)
		if err == nil && lc04ProcessExit(result.err) == response.ExitStatus() {
			channel := op01RequireStatusChannel(t, response, "alpha")
			last = channel.Runtime
			if channel.Runtime == (localapi.StatusChannelRuntime{
				HandlingClaimed: 1,
				RunActive:       1,
			}) {
				return
			}
		}
		if err := setupProcessPoll(ctx); err != nil {
			t.Fatalf("public status did not expose one owned current: runtime=%#v", last)
		}
	}
}

func lc04ProcessExit(err error) int {
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}

func lc04AssertOfflineSinglePersistence(t *testing.T, nodeState string, projectionDigest model.Digest) {
	t.Helper()
	databasePath := filepath.Join(nodeState, "node.db")
	database, err := sql.Open("sqlite", "file:"+databasePath+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.Ping(); err != nil {
		t.Fatal(err)
	}
	handlingID := lc04RequireSingleOfflineHandling(t, database)
	runID, currentReceipt := lc04RequireSingleOfflineRun(t, database, handlingID)
	lc04RequireOfflineCurrentReceipt(t, currentReceipt, runID, handlingID, projectionDigest)
}

func lc04RequireSingleOfflineHandling(t *testing.T, database *sql.DB) string {
	t.Helper()
	var handlingCount int
	if err := database.QueryRow(`SELECT COUNT(*) FROM agent_handlings`).Scan(&handlingCount); err != nil {
		t.Fatal(err)
	}
	var handlingID string
	var handlingAttempts, handlingRecovery int
	if err := database.QueryRow(`SELECT handling_id,attempts,recovery_count
		FROM agent_handlings`).Scan(&handlingID, &handlingAttempts,
		&handlingRecovery); err != nil {
		t.Fatal(err)
	}
	if handlingCount != 1 || handlingAttempts != 1 || handlingRecovery != 0 {
		t.Fatalf("offline Handling cardinality/generation = count %d attempts %d recovery %d",
			handlingCount, handlingAttempts, handlingRecovery)
	}
	return handlingID
}

func lc04RequireSingleOfflineRun(t *testing.T, database *sql.DB, handlingID string) (string, []byte) {
	t.Helper()
	var runCount int
	if err := database.QueryRow(`SELECT COUNT(*) FROM agent_runs`).Scan(&runCount); err != nil {
		t.Fatal(err)
	}
	var runID, runHandlingID, launcher string
	var attempt, recovery int
	var attached, runtimeStarted int
	var currentReceipt []byte
	if err := database.QueryRow(`SELECT run_id,handling_id,launcher,
		handling_attempt,handling_recovery,attached_at IS NOT NULL,
		runtime_started_at IS NOT NULL,current_read_receipt_json
		FROM agent_runs`).Scan(&runID, &runHandlingID, &launcher,
		&attempt, &recovery, &attached, &runtimeStarted, &currentReceipt); err != nil {
		t.Fatal(err)
	}
	if runCount != 1 || runHandlingID != handlingID ||
		launcher != "mnemond-wake" || attempt != 1 || recovery != 0 ||
		attached != 1 || runtimeStarted != 1 {
		t.Fatalf("offline Run cardinality/ownership = count %d handling_match=%t "+
			"launcher %q attempt/recovery %d/%d attached/started %d/%d",
			runCount, runHandlingID == handlingID, launcher, attempt, recovery,
			attached, runtimeStarted)
	}
	return runID, currentReceipt
}

func lc04RequireOfflineCurrentReceipt(t *testing.T, currentReceipt []byte, runID, handlingID string, projectionDigest model.Digest) {
	t.Helper()
	receipt, err := model.ParseCurrentReadReceipt(currentReceipt)
	if err != nil || receipt.RunID().String() != runID ||
		receipt.HandlingID().String() != handlingID ||
		receipt.HandlingAttempt() != 1 ||
		receipt.ProfileID() != model.TeamworkProfileID() ||
		receipt.ProjectionDigest() != projectionDigest {
		t.Fatalf("offline current receipt does not bind the sole public owner: %v", err)
	}
}
