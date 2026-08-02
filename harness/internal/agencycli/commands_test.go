package agencycli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

func TestCaptureKeepsDigestPrivateAndSubmitReplaysAfterPresentationLoss(t *testing.T) {
	fixture := newAppFixture(t)
	fixture.attach(t)
	var current bytes.Buffer
	_, exit := fixture.app(strings.NewReader(""), &current).
		TryRun(context.Background(), []string{"agent", "current", "--json"})
	if exit != 0 {
		t.Fatalf("current exit = %d, output %s", exit, current.String())
	}

	content := "verified artifact bytes"
	var capture bytes.Buffer
	_, exit = fixture.app(strings.NewReader(content), &capture).
		TryRun(context.Background(), []string{"artifact", "capture", "--json"})
	digest := nodeDigestForTest(content)
	if exit != 0 || !strings.Contains(capture.String(), `"handle":"artifact:test-candidate"`) ||
		strings.Contains(capture.String(), digest) {
		t.Fatalf("capture exit/output = %d / %q", exit, capture.String())
	}

	intent := candidateRootIntent(t, "artifact:test-candidate")
	failing := &failWriter{}
	_, exit = fixture.app(bytes.NewReader(intent), failing).
		TryRun(context.Background(), []string{"agent", "submit", "--json"})
	if exit != 1 {
		t.Fatalf("presentation-loss submit exit = %d", exit)
	}
	store := newJournalStore(fixture.nodeState, bytes.NewReader(make([]byte, 64)))
	if exists, err := store.exists(); err != nil || !exists {
		t.Fatalf("terminal replay journal exists = %v, %v", exists, err)
	}

	var replay bytes.Buffer
	_, exit = fixture.app(bytes.NewReader(intent), &replay).
		TryRun(context.Background(), []string{"agent", "submit", "--json"})
	fixture.client.mu.Lock()
	operations := append([]string(nil), fixture.client.submitOperations...)
	bindings := append([][]node.AgencyCandidateBinding(nil), fixture.client.submitCandidates...)
	fixture.client.mu.Unlock()
	if exit != 0 || len(operations) != 2 || operations[0] == "" || operations[0] != operations[1] ||
		len(bindings) != 2 || len(bindings[0]) != 1 || bindings[0][0].Digest != digest ||
		!strings.Contains(replay.String(), `"replayed":true`) {
		t.Fatalf("submit replay = exit %d operations %#v bindings %#v output %q",
			exit, operations, bindings, replay.String())
	}
	if exists, err := store.exists(); err != nil || exists {
		t.Fatalf("presented journal exists = %v, %v", exists, err)
	}
}

func TestTerminalReplayRejectsChangedIntentLocally(t *testing.T) {
	fixture := newAppFixture(t)
	fixture.attach(t)
	_, _ = fixture.app(strings.NewReader(""), io.Discard).
		TryRun(context.Background(), []string{"agent", "current", "--json"})
	original := candidateFreeRootIntent(t, "first")
	_, exit := fixture.app(bytes.NewReader(original), &failWriter{}).
		TryRun(context.Background(), []string{"agent", "submit", "--json"})
	if exit != 1 {
		t.Fatalf("first submit exit = %d", exit)
	}
	var output bytes.Buffer
	_, exit = fixture.app(bytes.NewReader(candidateFreeRootIntent(t, "changed")), &output).
		TryRun(context.Background(), []string{"agent", "submit", "--json"})
	if exit != localapi.NewAPIError(localapi.CodeOperationMismatch, "x").ExitStatus() ||
		!strings.Contains(output.String(), string(localapi.CodeOperationMismatch)) {
		t.Fatalf("changed terminal Intent exit/output = %d / %q", exit, output.String())
	}
	fixture.client.mu.Lock()
	calls := len(fixture.client.submitOperations)
	fixture.client.mu.Unlock()
	if calls != 1 {
		t.Fatalf("Submit calls = %d, want 1", calls)
	}
}

func TestHookNeverOverwritesExpiredTerminalReplayJournal(t *testing.T) {
	fixture := newAppFixture(t)
	fixture.attach(t)
	_, _ = fixture.app(strings.NewReader(""), io.Discard).
		TryRun(context.Background(), []string{"agent", "current", "--json"})
	intent := candidateFreeRootIntent(t, "terminal")
	_, exit := fixture.app(bytes.NewReader(intent), &failWriter{}).
		TryRun(context.Background(), []string{"agent", "submit", "--json"})
	if exit != 1 {
		t.Fatalf("terminal setup exit = %d", exit)
	}
	fixture.now = fixture.now.AddDate(2, 0, 0)
	fixture.attach(t)
	fixture.client.mu.Lock()
	calls := fixture.client.attachCalls
	fixture.client.mu.Unlock()
	if calls != 1 {
		t.Fatalf("Attach calls with terminal journal = %d, want 1", calls)
	}

	var current bytes.Buffer
	handled, exit := fixture.app(strings.NewReader(""), &current).
		TryRun(context.Background(), []string{"agent", "current", "--json"})
	if !handled || exit != localapi.NewAPIError(localapi.CodeOperationPending, "x").ExitStatus() {
		t.Fatalf("terminal current = handled %v exit %d output %s", handled, exit, current.String())
	}
}

func TestExpiredCurrentNeverFallsBackToR5(t *testing.T) {
	fixture := newAppFixture(t)
	fixture.attach(t)
	fixture.now = fixture.now.AddDate(2, 0, 0)
	var output bytes.Buffer
	handled, _ := fixture.app(strings.NewReader(""), &output).
		TryRun(context.Background(), []string{"agent", "current", "--json"})
	if !handled {
		t.Fatal("expired R7 journal silently fell back to R5")
	}
}

func TestUnsafeJournalFailsClosedInsteadOfFallingBack(t *testing.T) {
	fixture := newAppFixture(t)
	target := t.TempDir()
	if err := os.Symlink(target, filepath.Join(fixture.nodeState, journalDirectoryName)); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	handled, exit := fixture.app(strings.NewReader(""), &output).
		TryRun(context.Background(), []string{"agent", "current", "--json"})
	if !handled || exit != localapi.NewAPIError(localapi.CodeAuthenticationFailed, "x").ExitStatus() ||
		!strings.Contains(output.String(), string(localapi.CodeAuthenticationFailed)) || fixture.ensure.Load() != 0 {
		t.Fatalf("unsafe journal route = handled %v exit %d output %q ensure %d",
			handled, exit, output.String(), fixture.ensure.Load())
	}
}

func TestInterruptedJournalStageIsRecovered(t *testing.T) {
	fixture := newAppFixture(t)
	fixture.attach(t)
	directory := filepath.Join(fixture.nodeState, journalDirectoryName)
	if err := os.Remove(filepath.Join(directory, journalActiveName)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, journalStageName), []byte("partial"), ownerFileMode); err != nil {
		t.Fatal(err)
	}
	fixture.attach(t)
	if _, err := os.Lstat(filepath.Join(directory, journalStageName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stage after recovery = %v", err)
	}
}

func TestEnsureFailurePreventsPrivateClientCall(t *testing.T) {
	fixture := newAppFixture(t)
	app := fixture.app(strings.NewReader(""), io.Discard)
	app.deps.ensureDaemon = func(context.Context, string, string) *localapi.APIError {
		return localapi.NewAPIError(localapi.CodeMnemondUnavailable, "mnemond could not be made ready")
	}
	_, exit := app.TryRun(context.Background(), []string{"hook", "attach", "--json"})
	fixture.client.mu.Lock()
	calls := fixture.client.attachCalls
	fixture.client.mu.Unlock()
	if exit != localapi.NewAPIError(localapi.CodeMnemondUnavailable, "x").ExitStatus() || calls != 0 {
		t.Fatalf("ensure failure = exit %d Attach calls %d", exit, calls)
	}
}

func candidateRootIntent(t *testing.T, handleValue string) []byte {
	t.Helper()
	handle, err := agency.NewOpaqueHandle(handleValue)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := agency.NewArtifactCandidate(handle)
	if err != nil {
		t.Fatal(err)
	}
	return rootIntent(t, "with-artifact", []agency.ArtifactInput{artifact})
}

func candidateFreeRootIntent(t *testing.T, payloadValue string) []byte {
	t.Helper()
	return rootIntent(t, payloadValue, nil)
}

func rootIntent(t *testing.T, payloadValue string, artifacts []agency.ArtifactInput) []byte {
	t.Helper()
	kind, err := agency.NewSemanticLabel("test.request")
	if err != nil {
		t.Fatal(err)
	}
	payload, err := agency.NewSemanticPayload(payloadValue)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := agency.NewAgentIntent(agency.IntentSpec{Kind: kind, Payload: payload,
		Consequence: agency.ConsequenceCreateHandlings,
		Successors:  []agency.TargetRef{agency.SelfTarget()}, Artifacts: artifacts})
	if err != nil {
		t.Fatal(err)
	}
	return intent.CanonicalJSON()
}

func nodeDigestForTest(content string) string {
	return agency.Sum([]byte(content)).String()
}

type failWriter struct{}

func (*failWriter) Write([]byte) (int, error) { return 0, errors.New("presentation lost") }
