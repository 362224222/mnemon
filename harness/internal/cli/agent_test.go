package cli

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestParseAgentCommandIsClosedAndKeepsNaturalLanguageOutOfArgv(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args []string
		kind commandKind
		code localapi.ErrorCode
	}{
		{"hook", []string{"hook", "check"}, commandHook, ""},
		{"current", []string{"agent", "current", "--json"}, commandCurrent, ""},
		{"offer", []string{"teamwork", "offer", "--channel", "beta", "--to", "team", "--deadline", "24h", "--content-file", "-", "--artifact", "./bundle", "--json"}, commandTeamwork, ""},
		{"resolve", []string{"agent", "resolve", "retry", "--context", "/node/run.context", "--content-file", "-", "--json"}, commandResolve, ""},
		{"natural language argv", []string{"teamwork", "offer", "--content", "review this"}, 0, localapi.CodeInvalidArgument},
		{"generic submit", []string{"agent", "submit", "{}"}, 0, localapi.CodeUnknownAction},
		{"event emit", []string{"event", "emit"}, 0, localapi.CodeUnknownAction},
		{"missing context", []string{"teamwork", "deliver", "--content-file", "-"}, 0, localapi.CodeContextRequired},
		{"selector on action", []string{"teamwork", "accept", "--context", "/node/run.context", "--to", "peer"}, 0, localapi.CodeInvalidArgument},
		{"resolve Artifact", []string{"agent", "resolve", "retry", "--context", "/node/run.context", "--artifact", "result"}, 0, localapi.CodeInvalidArgument},
		{"missing Artifact value", []string{"teamwork", "offer", "--artifact", "--json"}, 0, localapi.CodeInvalidArgument},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			command, apiErr := parseAgentCommand(test.args)
			if test.code == "" {
				if apiErr != nil || command.kind != test.kind {
					t.Fatalf("parseAgentCommand() = (%#v, %v)", command, apiErr)
				}
				return
			}
			if apiErr == nil || apiErr.Code != test.code {
				t.Fatalf("parseAgentCommand() error = %v, want %s", apiErr, test.code)
			}
			if test.name == "missing Artifact value" && !command.jsonOutput {
				t.Fatal("trailing --json did not select the stable JSON error surface")
			}
		})
	}
}

func TestAgentAppHookAndCurrentExposeOnlyFixedCueAndContextPath(t *testing.T) {
	workspace, nodeState := cliWorkspace(t)
	secret := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x5a}, 32))
	fake := &fakeControlClient{
		hook: localapi.HookCheckResponse{SchemaVersion: localapi.SchemaVersion, Pending: true},
		current: localapi.AgentCurrentResponse{SchemaVersion: localapi.SchemaVersion, Status: "actionable",
			RunID: "run-cli-current", ClaimSecret: secret,
			Projection: cliCurrentProjection(t)},
	}

	stdout, stderr, app := cliTestApp(t, workspace, fake, nil)
	if exit := app.Run(context.Background(), []string{"hook", "check"}); exit != 0 ||
		stdout.String() != WakeCue+"\n" || stderr.Len() != 0 {
		t.Fatalf("hook = exit %d stdout %q stderr %q", exit, stdout.String(), stderr.String())
	}
	stdout.Reset()
	if exit := app.Run(context.Background(), []string{"agent", "current", "--json"}); exit != 0 || stderr.Len() != 0 {
		t.Fatalf("current = exit %d stdout %q stderr %q", exit, stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String(), secret) || strings.Contains(stdout.String(), "claim") {
		t.Fatalf("current leaked claim capability: %s", stdout.String())
	}
	var output map[string]json.RawMessage
	if err := json.Unmarshal(stdout.Bytes(), &output); err != nil {
		t.Fatal(err)
	}
	var status, contextPath string
	_ = json.Unmarshal(output["status"], &status)
	_ = json.Unmarshal(output["context_file"], &contextPath)
	if status != "actionable" || contextPath != filepath.Join(nodeState, "runs", "run-cli-current.context") {
		t.Fatalf("current public projection = status %q context %q", status, contextPath)
	}
	info, err := os.Lstat(contextPath)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("context file = %#v, %v", info, err)
	}

	fake.current = localapi.AgentCurrentResponse{SchemaVersion: 1, Status: "none",
		Projection: json.RawMessage(`{"initiation_context":{"channels":[]},"schema_version":1}`)}
	stdout.Reset()
	if exit := app.Run(context.Background(), []string{"agent", "current", "--json"}); exit != 0 ||
		stdout.String() != "{\"initiation_context\":{\"channels\":[]},\"schema_version\":1,\"status\":\"none\"}\n" {
		t.Fatalf("none current = exit %d output %q", exit, stdout.String())
	}
}

func TestAgentAppFailsClosedBeforeEveryManagedCommandWhenDaemonEnsureFails(t *testing.T) {
	workspace, nodeState := cliWorkspace(t)
	fake := &fakeControlClient{hook: localapi.HookCheckResponse{
		SchemaVersion: localapi.SchemaVersion, Pending: true,
	}}
	stdout, stderr, app := cliTestApp(t, workspace, fake, nil)
	called := 0
	app.deps.ensureDaemon = func(_ context.Context, gotWorkspace, gotNodeState string,
		gotClient controlClient,
	) *localapi.APIError {
		called++
		if gotWorkspace != workspace || gotNodeState != nodeState || gotClient != fake {
			t.Fatalf("ensure authority = workspace %q Node %q client %#v", gotWorkspace,
				gotNodeState, gotClient)
		}
		return localapi.NewAPIError(localapi.CodeMnemondUnavailable,
			"mnemond could not be made ready")
	}
	if exit := app.Run(context.Background(), []string{"hook", "check"}); exit != 5 ||
		called != 1 || stdout.Len() != 0 ||
		stderr.String() != "mnemond_unavailable: mnemond could not be made ready\n" {
		t.Fatalf("failed ensure = exit %d calls %d stdout %q stderr %q", exit, called,
			stdout.String(), stderr.String())
	}
}

func TestAgentAppTeamworkUsesContentContextAndTerminalJournal(t *testing.T) {
	workspace, nodeState := cliWorkspace(t)
	if err := os.WriteFile(filepath.Join(workspace, "reason.txt"), []byte("review accepted"), 0o600); err != nil {
		t.Fatal(err)
	}
	contextFile, err := localapi.WriteContextFile(nodeState, mustRunID(t, "run-cli-action"),
		base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x71}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeControlClient{teamworkResponse: acceptedCLIResponse("accept")}
	stdout, stderr, app := cliTestApp(t, workspace, fake, nil)
	exit := app.Run(context.Background(), []string{"teamwork", "accept", "--context", contextFile.Path(),
		"--content-file", "./reason.txt", "--json"})
	if exit != 0 || stderr.Len() != 0 || !strings.Contains(stdout.String(), `"status":"accepted"`) {
		t.Fatalf("accept = exit %d stdout %q stderr %q", exit, stdout.String(), stderr.String())
	}
	if fake.teamworkRequest.Action != "accept" || fake.teamworkRequest.Content != "review accepted" ||
		fake.teamworkContext == nil || fake.teamworkContext.Digest() != contextFile.Digest() ||
		fake.teamworkJournal.RequestDigest().IsZero() {
		t.Fatalf("submitted action = %#v context %#v journal %#v", fake.teamworkRequest,
			fake.teamworkContext, fake.teamworkJournal)
	}
	if _, err := os.Lstat(contextFile.Path()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("terminal context still exists: %v", err)
	}
	assertJournalSuffixes(t, nodeState, []string{".presented"})
}

func TestAgentAppResponseLossReusesJournalAndStableJSONError(t *testing.T) {
	workspace, nodeState := cliWorkspace(t)
	fake := &fakeControlClient{teamworkError: localapi.NewAPIError(localapi.CodeMnemondUnavailable,
		"mnemond local control is unavailable")}
	stdin := bytes.NewBufferString("review this\n")
	stdout, stderr, app := cliTestApp(t, workspace, fake, stdin)
	firstArgs := []string{"teamwork", "offer", "--channel", "alpha", "--to", "auto", "--content-file", "-",
		"--artifact", "z-result", "--artifact", "a-result", "--json"}
	if exit := app.Run(context.Background(), firstArgs); exit != 5 || stderr.Len() != 0 {
		t.Fatalf("lost response = exit %d stdout %q stderr %q", exit, stdout.String(), stderr.String())
	}
	var firstError localapi.APIError
	if err := json.Unmarshal(stdout.Bytes(), &firstError); err != nil || firstError.Code != localapi.CodeMnemondUnavailable || firstError.Replayed {
		t.Fatalf("error envelope = %#v, %v", firstError, err)
	}
	firstKey := fake.teamworkJournal.OperationKeyHash()
	if firstKey.IsZero() {
		t.Fatal("response loss did not retain an operation key")
	}

	fake.teamworkError = nil
	fake.teamworkResponse = acceptedCLIResponse("offer")
	app.stdin = bytes.NewBufferString("review this\n")
	stdout.Reset()
	retryArgs := []string{"teamwork", "offer", "--channel", "alpha", "--to", "auto", "--content-file", "-",
		"--artifact", "a-result", "--artifact", "z-result", "--json"}
	if exit := app.Run(context.Background(), retryArgs); exit != 0 || fake.teamworkJournal.OperationKeyHash() != firstKey {
		t.Fatalf("replay = exit %d key %s output %q", exit,
			fake.teamworkJournal.OperationKeyHash().String(), stdout.String())
	}
	assertJournalSuffixes(t, nodeState, []string{".presented"})

	app.stdin = bytes.NewBufferString("review this\n")
	stdout.Reset()
	if exit := app.Run(context.Background(), retryArgs); exit != 0 ||
		fake.teamworkJournal.OperationKeyHash() == firstKey {
		t.Fatalf("new intentional offer = exit %d key %s output %q", exit,
			fake.teamworkJournal.OperationKeyHash().String(), stdout.String())
	}
	assertJournalSuffixes(t, nodeState, []string{".presented", ".presented"})
}

func TestAgentAppTerminalReceiptSurvivesStdoutFailure(t *testing.T) {
	workspace, nodeState := cliWorkspace(t)
	fake := &fakeControlClient{teamworkResponse: acceptedCLIResponse("offer")}
	_, stderr, app := cliTestApp(t, workspace, fake, bytes.NewBufferString("review once"))
	app.stdout = failingWriter{}
	args := []string{"teamwork", "offer", "--content-file", "-", "--json"}
	if exit := app.Run(context.Background(), args); exit != 1 || stderr.Len() != 0 {
		t.Fatalf("failed presentation = exit %d stderr %q", exit, stderr.String())
	}
	firstKey := fake.teamworkJournal.OperationKeyHash()
	assertJournalSuffixes(t, nodeState, []string{".terminal"})

	stdout := &bytes.Buffer{}
	app.stdout, app.stdin = stdout, bytes.NewBufferString("review once")
	if exit := app.Run(context.Background(), args); exit != 0 ||
		fake.teamworkJournal.OperationKeyHash() != firstKey {
		t.Fatalf("terminal replay = exit %d key %s output %q", exit,
			fake.teamworkJournal.OperationKeyHash().String(), stdout.String())
	}
	assertJournalSuffixes(t, nodeState, []string{".presented"})
}

func TestAgentAppConcurrentPresentationKeepsOldIdentityAndNextCallGetsFreshKey(t *testing.T) {
	workspace, nodeState := cliWorkspace(t)
	client := newBarrierControlClient(2, acceptedCLIResponse("offer"))
	stdoutA, stderrA, appA := cliTestApp(t, workspace, client, bytes.NewBufferString("same offer"))
	stdoutB, stderrB, appB := cliTestApp(t, workspace, client, bytes.NewBufferString("same offer"))
	args := []string{"teamwork", "offer", "--content-file", "-", "--json"}
	exits := make(chan int, 2)
	go func() { exits <- appA.Run(context.Background(), args) }()
	go func() { exits <- appB.Run(context.Background(), args) }()
	for index := 0; index < 2; index++ {
		<-client.entered
	}
	close(client.releases[0])
	if exit := <-exits; exit != 0 {
		t.Fatalf("first concurrent presentation exit = %d", exit)
	}
	assertJournalSuffixes(t, nodeState, []string{".presented"})
	close(client.releases[1])
	if exit := <-exits; exit != 0 {
		t.Fatalf("second concurrent presentation exit = %d", exit)
	}
	if stderrA.Len() != 0 || stderrB.Len() != 0 ||
		!strings.Contains(stdoutA.String(), `"status":"accepted"`) ||
		!strings.Contains(stdoutB.String(), `"status":"accepted"`) {
		t.Fatalf("concurrent output = A(%q,%q) B(%q,%q)", stdoutA.String(), stderrA.String(),
			stdoutB.String(), stderrB.String())
	}
	keys := client.operationKeys()
	if len(keys) != 2 || keys[0] != keys[1] {
		t.Fatalf("concurrent operation keys = %v", keys)
	}
	assertJournalSuffixes(t, nodeState, []string{".presented"})

	fresh := &fakeControlClient{teamworkResponse: acceptedCLIResponse("offer")}
	_, freshStderr, freshApp := cliTestApp(t, workspace, fresh, bytes.NewBufferString("same offer"))
	if exit := freshApp.Run(context.Background(), args); exit != 0 || freshStderr.Len() != 0 ||
		fresh.teamworkJournal.OperationKeyHash() == keys[0] {
		t.Fatalf("fresh identical offer = exit %d key %s stderr %q", exit,
			fresh.teamworkJournal.OperationKeyHash().String(), freshStderr.String())
	}
	assertJournalSuffixes(t, nodeState, []string{".presented", ".presented"})
}

func TestAgentAppRejectedActionRetainsContextAfterPresentation(t *testing.T) {
	workspace, nodeState := cliWorkspace(t)
	contextFile, err := localapi.WriteContextFile(nodeState, mustRunID(t, "run-cli-rejected"),
		base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x91}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	operationID := "operation-cli-rejected"
	rejected := localapi.NewAPIError(localapi.CodeActionNotAllowed, "action is not allowed by current")
	rejected.OperationID = &operationID
	fake := &fakeControlClient{teamworkError: rejected}
	stdout, stderr, app := cliTestApp(t, workspace, fake, nil)
	if exit := app.Run(context.Background(), []string{"teamwork", "accept", "--context",
		contextFile.Path(), "--json"}); exit != 4 || stderr.Len() != 0 {
		t.Fatalf("rejected action = exit %d stdout %q stderr %q", exit, stdout.String(), stderr.String())
	}
	if info, err := os.Lstat(contextFile.Path()); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("rejected action context = %#v, %v", info, err)
	}
	assertJournalSuffixes(t, nodeState, []string{".presented"})
}

func TestAgentAppPresentedEnvelopeKeepsItsExitWhenMarkerFails(t *testing.T) {
	tests := []struct {
		name     string
		response localapi.OperationResponse
		remote   *localapi.APIError
		wantExit int
		wantCode string
	}{
		{name: "success", response: acceptedCLIResponse("offer"), wantExit: 0,
			wantCode: `"status":"accepted"`},
		{name: "domain rejection", wantExit: 4, wantCode: `"code":"action_not_allowed"`},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			workspace, nodeState := cliWorkspace(t)
			fake := &fakeControlClient{teamworkResponse: test.response}
			if test.name == "domain rejection" {
				operationID := "operation-cli-marker-rejected"
				fake.teamworkError = localapi.NewAPIError(localapi.CodeActionNotAllowed,
					"action is not allowed by current")
				fake.teamworkError.OperationID = &operationID
			}
			stdout, stderr, app := cliTestApp(t, workspace, fake, bytes.NewBufferString("review once"))
			app.deps.newJournals = func(state string) (journalStore, error) {
				store, err := localapi.NewPendingJournalStore(state)
				if err != nil {
					return nil, err
				}
				return markerFailingJournalStore{journalStore: store}, nil
			}
			exit := app.Run(context.Background(), []string{"teamwork", "offer", "--content-file", "-", "--json"})
			if exit != test.wantExit || stderr.Len() != 0 || !strings.Contains(stdout.String(), test.wantCode) {
				t.Fatalf("marker failure = exit %d stdout %q stderr %q", exit, stdout.String(), stderr.String())
			}
			assertJournalSuffixes(t, nodeState, []string{".terminal"})
		})
	}
}

func TestAgentAppResolveAndInputConfinement(t *testing.T) {
	workspace, nodeState := cliWorkspace(t)
	contextFile, err := localapi.WriteContextFile(nodeState, mustRunID(t, "run-cli-resolve"),
		base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x81}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeControlClient{resolveResponse: resolvedCLIResponse("retry")}
	stdout, stderr, app := cliTestApp(t, workspace, fake, bytes.NewBufferString("try later"))
	if exit := app.Run(context.Background(), []string{"agent", "resolve", "retry", "--context",
		contextFile.Path(), "--content-file", "-", "--json"}); exit != 0 || stderr.Len() != 0 {
		t.Fatalf("resolve = exit %d stdout %q stderr %q", exit, stdout.String(), stderr.String())
	}
	if fake.resolveRequest.Decision != "retry" || fake.resolveRequest.Content != "try later" {
		t.Fatalf("resolve request = %#v", fake.resolveRequest)
	}
	assertJournalSuffixes(t, nodeState, []string{".presented"})

	outside := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(outside, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(workspace, "linked.txt")); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"../outside.txt", outside, "linked.txt", ".mnemon/harness/node/identity.key",
		".MNEMON/HARNESS/node/runs/claim.context"} {
		if _, err := readBoundedWorkspaceFile(workspace, path, maxContentBytes); err == nil {
			t.Fatalf("unsafe content path %q was accepted", path)
		}
	}
	if _, apiErr := currentOutput(json.RawMessage(`{}`), "/node/run.context"); apiErr == nil {
		t.Fatal("empty current projection was reflected to the Agent")
	}
	for _, path := range []string{"../result", "/absolute", ".mnemon/harness/node/objects"} {
		if _, apiErr := normalizeArtifactArguments([]string{path}); apiErr == nil || apiErr.Code != localapi.CodeArtifactInvalid {
			t.Fatalf("unsafe Artifact path %q error = %v", path, apiErr)
		}
	}
	viewPath := ".mnemon/harness/node/views/run-cli-current/0/result.txt"
	if normalized, apiErr := normalizeArtifactArguments([]string{viewPath}); apiErr != nil ||
		len(normalized) != 1 || normalized[0] != viewPath {
		t.Fatalf("managed readonly view normalization = (%v, %v)", normalized, apiErr)
	}
}

type fakeControlClient struct {
	hook             localapi.HookCheckResponse
	hookError        *localapi.APIError
	current          localapi.AgentCurrentResponse
	currentError     *localapi.APIError
	teamworkRequest  localapi.TeamworkActionRequest
	teamworkContext  *localapi.ContextFile
	teamworkJournal  localapi.PendingJournal
	teamworkResponse localapi.OperationResponse
	teamworkError    *localapi.APIError
	resolveRequest   localapi.AgentResolveRequest
	resolveContext   localapi.ContextFile
	resolveJournal   localapi.PendingJournal
	resolveResponse  localapi.OperationResponse
	resolveError     *localapi.APIError
}

type barrierControlClient struct {
	mu        sync.Mutex
	entered   chan int
	releases  []chan struct{}
	keys      []model.Digest
	response  localapi.OperationResponse
	nextIndex int
}

func newBarrierControlClient(callers int, response localapi.OperationResponse) *barrierControlClient {
	releases := make([]chan struct{}, callers)
	for index := range releases {
		releases[index] = make(chan struct{})
	}
	return &barrierControlClient{entered: make(chan int, callers), releases: releases,
		response: response}
}

func (client *barrierControlClient) HookCheck(context.Context) (localapi.HookCheckResponse, *localapi.APIError) {
	return localapi.HookCheckResponse{}, localapi.NewAPIError(localapi.CodeInternal, "unexpected Hook call")
}

func (client *barrierControlClient) ProbeHealth(context.Context) (localapi.HealthResponse, *localapi.APIError) {
	return localapi.HealthResponse{}, localapi.NewAPIError(localapi.CodeMnemondUnavailable,
		"mnemond local control is unavailable")
}

func (client *barrierControlClient) AgentCurrent(context.Context) (localapi.AgentCurrentResponse, *localapi.APIError) {
	return localapi.AgentCurrentResponse{}, localapi.NewAPIError(localapi.CodeInternal, "unexpected current call")
}

func (client *barrierControlClient) TeamworkAction(_ context.Context, _ localapi.TeamworkActionRequest,
	_ *localapi.ContextFile, journal localapi.PendingJournal,
) (localapi.OperationResponse, *localapi.APIError) {
	client.mu.Lock()
	index := client.nextIndex
	client.nextIndex++
	client.keys = append(client.keys, journal.OperationKeyHash())
	client.mu.Unlock()
	client.entered <- index
	<-client.releases[index]
	return client.response, nil
}

func (client *barrierControlClient) AgentResolve(context.Context, localapi.AgentResolveRequest,
	localapi.ContextFile, localapi.PendingJournal,
) (localapi.OperationResponse, *localapi.APIError) {
	return localapi.OperationResponse{}, localapi.NewAPIError(localapi.CodeInternal, "unexpected resolve call")
}

func (client *barrierControlClient) operationKeys() []model.Digest {
	client.mu.Lock()
	defer client.mu.Unlock()
	return append([]model.Digest(nil), client.keys...)
}

func (fake *fakeControlClient) HookCheck(context.Context) (localapi.HookCheckResponse, *localapi.APIError) {
	return fake.hook, fake.hookError
}

func (fake *fakeControlClient) ProbeHealth(context.Context) (localapi.HealthResponse, *localapi.APIError) {
	return localapi.HealthResponse{}, localapi.NewAPIError(localapi.CodeMnemondUnavailable,
		"mnemond local control is unavailable")
}

func (fake *fakeControlClient) AgentCurrent(context.Context) (localapi.AgentCurrentResponse, *localapi.APIError) {
	return fake.current, fake.currentError
}

func (fake *fakeControlClient) TeamworkAction(_ context.Context, request localapi.TeamworkActionRequest,
	contextFile *localapi.ContextFile, journal localapi.PendingJournal,
) (localapi.OperationResponse, *localapi.APIError) {
	fake.teamworkRequest, fake.teamworkContext, fake.teamworkJournal = request, contextFile, journal
	return fake.teamworkResponse, fake.teamworkError
}

func (fake *fakeControlClient) AgentResolve(_ context.Context, request localapi.AgentResolveRequest,
	contextFile localapi.ContextFile, journal localapi.PendingJournal,
) (localapi.OperationResponse, *localapi.APIError) {
	fake.resolveRequest, fake.resolveContext, fake.resolveJournal = request, contextFile, journal
	return fake.resolveResponse, fake.resolveError
}

func cliTestApp(t *testing.T, workspace string, fake controlClient, stdin ioReader) (*bytes.Buffer, *bytes.Buffer, *App) {
	t.Helper()
	if stdin == nil {
		stdin = bytes.NewReader(nil)
	}
	stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
	app := New(stdin, stdout, stderr)
	app.deps.workingDirectory = func() (string, error) { return filepath.Join(workspace, "nested"), nil }
	app.deps.newClient = func(string) (controlClient, error) { return fake, nil }
	app.deps.ensureDaemon = func(context.Context, string, string, controlClient) *localapi.APIError {
		return nil
	}
	return stdout, stderr, app
}

type ioReader interface{ Read([]byte) (int, error) }

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("injected write failure") }

type markerFailingJournalStore struct{ journalStore }

func (store markerFailingJournalStore) MarkPresented(localapi.PendingJournal) (localapi.PendingJournal, error) {
	return localapi.PendingJournal{}, errors.New("injected marker failure")
}

func cliWorkspace(t *testing.T) (string, string) {
	t.Helper()
	workspace := t.TempDir()
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	if err := os.MkdirAll(filepath.Join(workspace, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(nodeState, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(nodeState, 0o700); err != nil {
		t.Fatal(err)
	}
	canonical, err := filepath.EvalSymlinks(workspace)
	if err != nil {
		t.Fatal(err)
	}
	return canonical, filepath.Join(canonical, ".mnemon", "harness", "node")
}

func mustRunID(t *testing.T, value string) model.RunID {
	t.Helper()
	id, err := model.ParseRunID(value)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func cliCurrentProjection(t *testing.T) json.RawMessage {
	t.Helper()
	home, _ := model.ParsePeerID("peer-cli-home")
	origin, _ := model.ParsePeerID("peer-cli-origin")
	epoch, _ := model.ParseOriginEpoch("epoch-cli-origin")
	eventID, _ := model.ParseEventID("event-cli-current")
	key, _ := model.NewEventKey(origin, epoch, eventID)
	workID, _ := model.ParseWorkID("work-cli-current")
	ref, _ := model.NewWorkRef(home, workID)
	at := time.Date(2026, 7, 17, 1, 0, 0, 0, time.UTC)
	payload, _ := model.NewJSON([]byte(`{"content":"review this","deadline":"2026-07-18T01:00:00Z","iteration":1,"work_version":1}`))
	eventValue, err := model.NewCurrentEvent(model.CurrentEventSpec{Key: key,
		Digest: model.Sum([]byte("cli-current")), Type: model.EventReviewOffered, WorkRef: ref,
		Summary: "Review this", Payload: payload, AcceptedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	work, err := model.NewCurrentWork(model.CurrentWorkSpec{Ref: ref, Version: 1, Iteration: 1,
		DeadlineUnixNano: at.Add(24 * time.Hour).UnixNano(), State: model.WorkOffered,
		StateData: payload, LocalRole: model.CurrentReviewer})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := model.NewCurrentProjection(model.CurrentProjectionSpec{SourceEvent: eventValue,
		ActionWork: work, AllowedActions: []model.OperationKind{model.OperationTeamworkAccept}})
	if err != nil {
		t.Fatal(err)
	}
	return projection.CanonicalJSON().Bytes()
}

func acceptedCLIResponse(action string) localapi.OperationResponse {
	return localapi.OperationResponse{SchemaVersion: 1, Status: "accepted", Action: "teamwork." + action,
		OperationID: "operation-cli-" + action, Results: []localapi.OperationResult{{EventID: "event-cli-" + action,
			EventType: "review.offered", Work: localapi.WorkReceipt{Ref: "peer/work", Version: 1, State: "OFFERED"}}},
		Handling: &localapi.HandlingReceipt{Status: "completed"}, Receipt: "receipt"}
}

func resolvedCLIResponse(decision string) localapi.OperationResponse {
	return localapi.OperationResponse{SchemaVersion: 1, Status: "resolved", Action: "agent.resolve." + decision,
		OperationID: "operation-cli-resolve-" + decision, Results: []localapi.OperationResult{},
		Handling: &localapi.HandlingReceipt{Status: "requeued"}, Receipt: "receipt"}
}

func assertJournalSuffixes(t *testing.T, nodeState string, want []string) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(nodeState, "operations"))
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(entries))
	for _, entry := range entries {
		for _, suffix := range []string{".pending", ".terminal", ".presented"} {
			if strings.HasSuffix(entry.Name(), suffix) {
				got = append(got, suffix)
			}
		}
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("operation journal suffixes = %v, want %v", got, want)
	}
}
