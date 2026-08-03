package agencycli

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

const testCredentialText = "private-credential-never-project"

type fakeAgencyClient struct {
	mu                sync.Mutex
	attachCalls       int
	currentOperations []string
	submitOperations  []string
	submitCandidates  [][]candidateBinding
	captureCalls      int
	currentFailures   int
	attachBlock       chan struct{}
}

func (client *fakeAgencyClient) Attach(context.Context) (attachment, *controlError) {
	client.mu.Lock()
	client.attachCalls++
	block := client.attachBlock
	client.mu.Unlock()
	if block != nil {
		<-block
	}
	credential := make([]byte, journalCredentialBytes)
	copy(credential, testCredentialText)
	return attachment{ID: "attachment:test", Credential: credential,
		ExpiresAt: time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)}, nil
}

func (client *fakeAgencyClient) Current(_ context.Context, _ attachment,
	operation string,
) ([]byte, *controlError) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.currentOperations = append(client.currentOperations, operation)
	if client.currentFailures > 0 {
		client.currentFailures--
		return nil, newControlError(codeMnemondUnavailable, "test transport loss")
	}
	return []byte(`{"schema":"mnemon.agent.view","version":2,"view":"view:test","allowed_intents":[]}`), nil
}

func (client *fakeAgencyClient) Submit(_ context.Context, _ attachment,
	_, operation string, _ []byte, candidates []candidateBinding,
) ([]byte, *controlError) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.submitOperations = append(client.submitOperations, operation)
	client.submitCandidates = append(client.submitCandidates,
		append([]candidateBinding(nil), candidates...))
	replayed := len(client.submitOperations) > 1
	if replayed {
		return []byte(`{"schema":"mnemon.agent.receipt","version":1,"outcome":"accepted","replayed":true}`), nil
	}
	return []byte(`{"schema":"mnemon.agent.receipt","version":1,"outcome":"accepted","replayed":false}`), nil
}

func (client *fakeAgencyClient) Capture(_ context.Context,
	content []byte,
) (artifactCapture, *controlError) {
	client.mu.Lock()
	client.captureCalls++
	client.mu.Unlock()
	return artifactCapture{Handle: "artifact:test-candidate",
		Digest: agency.Sum(content).String(), ByteSize: int64(len(content))}, nil
}

type appFixture struct {
	root      string
	nodeState string
	client    *fakeAgencyClient
	ensure    atomic.Int32
	now       time.Time
}

func newAppFixture(t *testing.T) *appFixture {
	t.Helper()
	root := t.TempDir()
	nodeState := filepath.Join(root, ".mnemon", "harness", "node")
	if err := os.MkdirAll(nodeState, ownerDirectoryMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(nodeState, ownerDirectoryMode); err != nil {
		t.Fatal(err)
	}
	return &appFixture{root: root, nodeState: nodeState, client: &fakeAgencyClient{},
		now: time.Date(2029, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (fixture *appFixture) app(stdin io.Reader, stdout io.Writer) *App {
	app := New(stdin, stdout, io.Discard, func(context.Context, string) error {
		fixture.ensure.Add(1)
		return nil
	})
	app.deps.workingDirectory = func() (string, error) { return fixture.root, nil }
	app.deps.newClient = func(string) (agencyClient, error) { return fixture.client, nil }
	app.deps.random = bytes.NewReader(bytes.Repeat([]byte{0x42}, 4096))
	app.deps.clock = func() time.Time { return fixture.now }
	return app
}

func (fixture *appFixture) attach(t *testing.T) string {
	t.Helper()
	var output bytes.Buffer
	exit := fixture.app(strings.NewReader(""), &output).
		Run(context.Background(), []string{"hook", "attach", "--json"})
	if exit != 0 {
		t.Fatalf("hook attach = exit %d output %s", exit, output.String())
	}
	return output.String()
}

func TestCommandsRequireContextWithoutEnsuringDaemon(t *testing.T) {
	fixture := newAppFixture(t)
	for _, args := range [][]string{
		{"agent", "current", "--json"},
		{"agent", "submit", "--json"},
		{"artifact", "capture", "--json"},
	} {
		var output bytes.Buffer
		exit := fixture.app(strings.NewReader(""), &output).Run(context.Background(), args)
		if exit != codeContextRequired.exitStatus() ||
			!strings.Contains(output.String(), `"code":"context_required"`) {
			t.Fatalf("absent journal for %q = exit %d output %q", args, exit, output.String())
		}
	}
	if fixture.ensure.Load() != 0 {
		t.Fatalf("Ensure calls without an Agent context = %d, want 0", fixture.ensure.Load())
	}
}

func TestAgentCurrentReadsViewAfterAttach(t *testing.T) {
	fixture := newAppFixture(t)
	fixture.attach(t)
	var output bytes.Buffer
	exit := fixture.app(strings.NewReader(""), &output).
		Run(context.Background(), []string{"agent", "current", "--json"})
	if exit != 0 || output.String() !=
		`{"schema":"mnemon.agent.view","version":2,"view":"view:test","allowed_intents":[]}`+"\n" {
		t.Fatalf("R7 current = exit %d output %q", exit, output.String())
	}
}

func TestAgentCurrentWithoutSetupIsUnavailableWithoutEnsuringDaemon(t *testing.T) {
	fixture := newAppFixture(t)
	unconfigured := t.TempDir()
	app := fixture.app(strings.NewReader(""), &bytes.Buffer{})
	app.deps.workingDirectory = func() (string, error) { return unconfigured, nil }
	var output bytes.Buffer
	app.stdout = &output
	exit := app.Run(context.Background(), []string{"agent", "current", "--json"})
	if exit != codeMnemondUnavailable.exitStatus() ||
		!strings.Contains(output.String(), `"code":"mnemond_unavailable"`) || fixture.ensure.Load() != 0 {
		t.Fatalf("unconfigured current = exit %d output %q ensure %d",
			exit, output.String(), fixture.ensure.Load())
	}
}

func TestUnsupportedCommandsFailClosedWithoutEnsuringDaemon(t *testing.T) {
	fixture := newAppFixture(t)
	for _, args := range [][]string{
		{"agency", "status", "--json"},
		{"teamwork", "list", "--json"},
		{"hook", "attach"},
	} {
		var output bytes.Buffer
		exit := fixture.app(strings.NewReader(""), &output).Run(context.Background(), args)
		if exit != codeInvalidArgument.exitStatus() ||
			!strings.Contains(output.String(), `"code":"invalid_argument"`) {
			t.Fatalf("unsupported command %q = exit %d output %q", args, exit, output.String())
		}
	}
	if fixture.ensure.Load() != 0 {
		t.Fatalf("Ensure calls for unsupported commands = %d, want 0", fixture.ensure.Load())
	}
}

func TestHookAttachProjectsNoPrivateAuthorityAndReusesJournal(t *testing.T) {
	fixture := newAppFixture(t)
	first := fixture.attach(t)
	second := fixture.attach(t)
	if first != second || first != `{"schema":"mnemon.hook.attach","status":"ready","version":1}`+"\n" {
		t.Fatalf("hook outputs = %q / %q", first, second)
	}
	fixture.client.mu.Lock()
	attachCalls := fixture.client.attachCalls
	fixture.client.mu.Unlock()
	if attachCalls != 1 || strings.Contains(first, "attachment:test") ||
		strings.Contains(first, testCredentialText) {
		t.Fatalf("attach calls/output = %d / %q", attachCalls, first)
	}
	assertMode(t, filepath.Join(fixture.nodeState, journalDirectoryName), ownerDirectoryMode)
	assertMode(t, filepath.Join(fixture.nodeState, journalDirectoryName, journalLockName), ownerFileMode)
	assertMode(t, filepath.Join(fixture.nodeState, journalDirectoryName, journalActiveName), ownerFileMode)
}

func TestCurrentPersistsOperationBeforeTransportAndReplaysIt(t *testing.T) {
	fixture := newAppFixture(t)
	fixture.attach(t)
	fixture.client.currentFailures = 1
	var first bytes.Buffer
	firstExit := fixture.app(strings.NewReader(""), &first).
		Run(context.Background(), []string{"agent", "current", "--json"})
	if firstExit != codeMnemondUnavailable.exitStatus() {
		t.Fatalf("first current exit/output = %d / %s", firstExit, first.String())
	}
	var second bytes.Buffer
	secondExit := fixture.app(strings.NewReader(""), &second).
		Run(context.Background(), []string{"agent", "current", "--json"})
	fixture.client.mu.Lock()
	operations := append([]string(nil), fixture.client.currentOperations...)
	fixture.client.mu.Unlock()
	if secondExit != 0 || len(operations) != 2 || operations[0] == "" || operations[0] != operations[1] {
		t.Fatalf("current replay = exit %d operations %#v output %q", secondExit, operations, second.String())
	}
}

func TestHookAttachSerializesConcurrentIssuance(t *testing.T) {
	fixture := newAppFixture(t)
	fixture.client.attachBlock = make(chan struct{})
	const callers = 12
	results := make(chan int, callers)
	for range callers {
		go func() {
			exit := fixture.app(strings.NewReader(""), io.Discard).
				Run(context.Background(), []string{"hook", "attach", "--json"})
			results <- exit
		}()
	}
	for {
		fixture.client.mu.Lock()
		calls := fixture.client.attachCalls
		fixture.client.mu.Unlock()
		if calls == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(fixture.client.attachBlock)
	for range callers {
		if exit := <-results; exit != 0 {
			t.Fatalf("concurrent attach exit = %d", exit)
		}
	}
	fixture.client.mu.Lock()
	calls := fixture.client.attachCalls
	fixture.client.mu.Unlock()
	if calls != 1 {
		t.Fatalf("Attach calls = %d, want 1", calls)
	}
}

func TestHookAttachRenewsOnlyExpiredActiveJournal(t *testing.T) {
	fixture := newAppFixture(t)
	fixture.attach(t)
	fixture.now = time.Date(2031, 1, 1, 0, 0, 0, 0, time.UTC)
	fixture.attach(t)
	fixture.client.mu.Lock()
	calls := fixture.client.attachCalls
	fixture.client.mu.Unlock()
	if calls != 2 {
		t.Fatalf("Attach calls after expiry = %d, want 2", calls)
	}

	var output bytes.Buffer
	exit := fixture.app(strings.NewReader(""), &output).
		Run(context.Background(), []string{"agent", "current", "--json"})
	if exit != 0 {
		t.Fatalf("renewed current = exit %d output %s", exit, output.String())
	}
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil || info.Mode().Perm() != want || info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("mode %s = %v / %v, want %04o", path, info, err, want)
	}
}
