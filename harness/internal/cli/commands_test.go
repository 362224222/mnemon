package cli

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
)

func TestCaptureKeepsDigestPrivateAndSubmitReplaysAfterPresentationLoss(t *testing.T) {
	fixture := newAppFixture(t)
	fixture.attach(t)
	var current bytes.Buffer
	exit := fixture.app(strings.NewReader(""), &current).
		Run(context.Background(), []string{"agent", "current", "--json"})
	if exit != 0 {
		t.Fatalf("current exit = %d, output %s", exit, current.String())
	}

	content := "verified artifact bytes"
	var capture bytes.Buffer
	exit = fixture.app(strings.NewReader(content), &capture).
		Run(context.Background(), []string{"artifact", "capture", "--json"})
	digest := nodeDigestForTest(content)
	if exit != 0 || !strings.Contains(capture.String(), `"handle":"artifact:test-candidate"`) ||
		strings.Contains(capture.String(), digest) {
		t.Fatalf("capture exit/output = %d / %q", exit, capture.String())
	}

	intent := candidateRootIntent(t, "artifact:test-candidate")
	failing := &failWriter{}
	exit = fixture.app(bytes.NewReader(intent), failing).
		Run(context.Background(), []string{"agent", "submit", "--json"})
	if exit != 1 {
		t.Fatalf("presentation-loss submit exit = %d", exit)
	}
	store := newJournalStore(fixture.nodeState, bytes.NewReader(make([]byte, 64)))
	if exists, err := store.exists(); err != nil || !exists {
		t.Fatalf("terminal replay journal exists = %v, %v", exists, err)
	}

	var replay bytes.Buffer
	exit = fixture.app(bytes.NewReader(intent), &replay).
		Run(context.Background(), []string{"agent", "submit", "--json"})
	fixture.client.mu.Lock()
	operations := append([]string(nil), fixture.client.submitOperations...)
	bindings := append([][]candidateBinding(nil), fixture.client.submitCandidates...)
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

func TestAcceptedReferenceKeepsAttachmentAndRefreshesCurrent(t *testing.T) {
	fixture := newAppFixture(t)
	intent := prepareReferenceSubmit(t, fixture)
	var receipt bytes.Buffer
	if exit := fixture.app(bytes.NewReader(intent), &receipt).
		Run(context.Background(), []string{"agent", "submit", "--json"}); exit != 0 {
		t.Fatalf("Reference submit exit = %d output %q", exit, receipt.String())
	}

	journal := loadJournalForTest(t, fixture.nodeState)
	if journal.fileName != journalActiveName || !journal.CurrentOperation.IsZero() ||
		journal.CurrentProjection != "" || len(journal.Candidates) != 0 ||
		journal.Attachment.ID != "attachment:test" {
		journal.clear()
		t.Fatalf("post-Reference journal = file %q current %q projection %q candidates %d attachment %q",
			journal.fileName, journal.CurrentOperation.String(), journal.CurrentProjection,
			len(journal.Candidates), journal.Attachment.ID)
	}
	journal.clear()

	var next bytes.Buffer
	nextApp := fixture.app(strings.NewReader(""), &next)
	nextApp.deps.random = bytes.NewReader(bytes.Repeat([]byte{0x43}, 64))
	if exit := nextApp.Run(context.Background(),
		[]string{"agent", "current", "--json"}); exit != 0 {
		t.Fatalf("next Current exit = %d output %q", exit, next.String())
	}
	fixture.client.mu.Lock()
	attachCalls := fixture.client.attachCalls
	operations := append([]string(nil), fixture.client.currentOperations...)
	fixture.client.mu.Unlock()
	if attachCalls != 1 || len(operations) != 2 || operations[0] == operations[1] {
		t.Fatalf("continued attachment = attaches %d operations %#v", attachCalls, operations)
	}
}

func TestReferencePresentationLossRetainsExactReplayBeforeRefresh(t *testing.T) {
	fixture := newAppFixture(t)
	intent := leaveUnpresentedReference(t, fixture)
	terminal := loadJournalForTest(t, fixture.nodeState)
	if !validTerminalName(terminal.fileName) || terminal.CurrentOperation.IsZero() ||
		len(terminal.Candidates) != 1 {
		terminal.clear()
		t.Fatalf("unpresented terminal = file %q current %q candidates %d",
			terminal.fileName, terminal.CurrentOperation.String(), len(terminal.Candidates))
	}
	terminal.clear()

	var replay bytes.Buffer
	if exit := fixture.app(bytes.NewReader(intent), &replay).
		Run(context.Background(), []string{"agent", "submit", "--json"}); exit != 0 ||
		!strings.Contains(replay.String(), `"replayed":true`) {
		t.Fatalf("Reference replay exit/output = %d / %q", exit, replay.String())
	}
	fixture.client.mu.Lock()
	operations := append([]string(nil), fixture.client.submitOperations...)
	fixture.client.mu.Unlock()
	if len(operations) != 2 || operations[0] != operations[1] {
		t.Fatalf("Reference replay operations = %#v", operations)
	}
	active := loadJournalForTest(t, fixture.nodeState)
	if active.fileName != journalActiveName || !active.CurrentOperation.IsZero() ||
		len(active.Candidates) != 0 {
		active.clear()
		t.Fatalf("replayed Reference reset = file %q current %q candidates %d",
			active.fileName, active.CurrentOperation.String(), len(active.Candidates))
	}
	active.clear()
}

func TestCurrentRecoversPresentedReferencePhase(t *testing.T) {
	fixture := newAppFixture(t)
	_ = leaveUnpresentedReference(t, fixture)

	store := newJournalStore(fixture.nodeState, bytes.NewReader(make([]byte, 64)))
	if err := store.withLock(false, func(directory *lockedJournalDirectory) error {
		terminal, err := directory.load()
		if err != nil {
			return err
		}
		defer terminal.clear()
		presented, err := directory.markPresented(terminal)
		presented.clear()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	presented := loadJournalForTest(t, fixture.nodeState)
	if !validTerminalName(presented.fileName) || !presented.CurrentOperation.IsZero() {
		presented.clear()
		t.Fatalf("presented phase = file %q current %q",
			presented.fileName, presented.CurrentOperation.String())
	}
	presented.clear()

	var output bytes.Buffer
	if exit := fixture.app(strings.NewReader(""), &output).
		Run(context.Background(), []string{"agent", "current", "--json"}); exit != 0 {
		t.Fatalf("recovering Current exit/output = %d / %q", exit, output.String())
	}
	active := loadJournalForTest(t, fixture.nodeState)
	if active.fileName != journalActiveName || active.CurrentOperation.IsZero() {
		active.clear()
		t.Fatalf("recovered active journal = file %q current %q",
			active.fileName, active.CurrentOperation.String())
	}
	active.clear()
}

func TestTerminalReplayRejectsChangedIntentLocally(t *testing.T) {
	fixture := newAppFixture(t)
	fixture.attach(t)
	_ = fixture.app(strings.NewReader(""), io.Discard).
		Run(context.Background(), []string{"agent", "current", "--json"})
	original := candidateFreeRootIntent(t, "first")
	exit := fixture.app(bytes.NewReader(original), &failWriter{}).
		Run(context.Background(), []string{"agent", "submit", "--json"})
	if exit != 1 {
		t.Fatalf("first submit exit = %d", exit)
	}
	var output bytes.Buffer
	exit = fixture.app(bytes.NewReader(candidateFreeRootIntent(t, "changed")), &output).
		Run(context.Background(), []string{"agent", "submit", "--json"})
	if exit != codeOperationMismatch.exitStatus() ||
		!strings.Contains(output.String(), string(codeOperationMismatch)) {
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
	_ = fixture.app(strings.NewReader(""), io.Discard).
		Run(context.Background(), []string{"agent", "current", "--json"})
	intent := candidateFreeRootIntent(t, "terminal")
	exit := fixture.app(bytes.NewReader(intent), &failWriter{}).
		Run(context.Background(), []string{"agent", "submit", "--json"})
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
	exit = fixture.app(strings.NewReader(""), &current).
		Run(context.Background(), []string{"agent", "current", "--json"})
	if exit != codeOperationPending.exitStatus() {
		t.Fatalf("terminal current = exit %d output %s", exit, current.String())
	}
}

func TestExpiredJournalStillUsesR7ControlPath(t *testing.T) {
	fixture := newAppFixture(t)
	fixture.attach(t)
	fixture.now = fixture.now.AddDate(2, 0, 0)
	var output bytes.Buffer
	exit := fixture.app(strings.NewReader(""), &output).
		Run(context.Background(), []string{"agent", "current", "--json"})
	fixture.client.mu.Lock()
	currentCalls := len(fixture.client.currentOperations)
	fixture.client.mu.Unlock()
	if exit != 0 || output.Len() == 0 || currentCalls != 1 || fixture.ensure.Load() != 2 {
		t.Fatalf("expired current = exit %d output %q", exit, output.String())
	}
}

func TestUnsafeJournalFailsClosedBeforeEnsure(t *testing.T) {
	for _, args := range [][]string{
		{"hook", "attach", "--json"},
		{"agent", "current", "--json"},
		{"agent", "submit", "--json"},
		{"artifact", "capture", "--json"},
	} {
		t.Run(strings.Join(args[:2], "_"), func(t *testing.T) {
			fixture := newAppFixture(t)
			if err := os.Symlink(t.TempDir(),
				filepath.Join(fixture.nodeState, journalDirectoryName)); err != nil {
				t.Fatal(err)
			}
			var output bytes.Buffer
			exit := fixture.app(strings.NewReader(""), &output).Run(context.Background(), args)
			if exit != codeAuthenticationFailed.exitStatus() ||
				!strings.Contains(output.String(), string(codeAuthenticationFailed)) ||
				fixture.ensure.Load() != 0 {
				t.Fatalf("unsafe journal = exit %d output %q ensure %d",
					exit, output.String(), fixture.ensure.Load())
			}
		})
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
	var output bytes.Buffer
	app := fixture.app(strings.NewReader(""), &output)
	app.deps.ensureDaemon = func(context.Context, string) error {
		return errors.New("private daemon failure detail")
	}
	exit := app.Run(context.Background(), []string{"hook", "attach", "--json"})
	fixture.client.mu.Lock()
	calls := fixture.client.attachCalls
	fixture.client.mu.Unlock()
	if exit != codeMnemondUnavailable.exitStatus() || calls != 0 ||
		strings.Contains(output.String(), "private daemon failure detail") {
		t.Fatalf("ensure failure = exit %d Attach calls %d output %q", exit, calls, output.String())
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

func prepareReferenceSubmit(t *testing.T, fixture *appFixture) []byte {
	t.Helper()
	fixture.client.currentView = subjectViewForTest()
	fixture.attach(t)
	if exit := fixture.app(strings.NewReader(""), io.Discard).
		Run(context.Background(), []string{"agent", "current", "--json"}); exit != 0 {
		t.Fatalf("Reference Current exit = %d", exit)
	}
	if exit := fixture.app(strings.NewReader("review playbook"), io.Discard).
		Run(context.Background(), []string{"artifact", "capture", "--json"}); exit != 0 {
		t.Fatalf("Reference capture exit = %d", exit)
	}
	return referencePublishIntent(t, "artifact:test-candidate")
}

func leaveUnpresentedReference(t *testing.T, fixture *appFixture) []byte {
	t.Helper()
	intent := prepareReferenceSubmit(t, fixture)
	if exit := fixture.app(bytes.NewReader(intent), &failWriter{}).
		Run(context.Background(), []string{"agent", "submit", "--json"}); exit != 1 {
		t.Fatalf("presentation-loss Reference exit = %d", exit)
	}
	return intent
}

func referencePublishIntent(t *testing.T, artifactHandle string) []byte {
	t.Helper()
	kind, err := agency.NewSemanticLabel("knowledge.playbook")
	if err != nil {
		t.Fatal(err)
	}
	payload, err := agency.NewSemanticPayload("publish a reusable review playbook")
	if err != nil {
		t.Fatal(err)
	}
	key, err := agency.NewReferenceKey("playbook.review")
	if err != nil {
		t.Fatal(err)
	}
	handle, err := agency.NewOpaqueHandle(artifactHandle)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := agency.NewArtifactCandidate(handle)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := agency.NewAgentIntent(agency.IntentSpec{Kind: kind, Payload: payload,
		Consequence: agency.ConsequencePublishReference, ReferenceKey: key,
		Artifacts: []agency.ArtifactInput{artifact}})
	if err != nil {
		t.Fatal(err)
	}
	return intent.CanonicalJSON()
}

func subjectViewForTest() []byte {
	return []byte(`{"schema":"mnemon.agent.view","version":2,` +
		`"view":"view:test","current":{"facts":{"handle":"r7:subject:test"},` +
		`"semantic":{"kind":"review.request","payload":"review"}},` +
		`"allowed_intents":[]}`)
}

func loadJournalForTest(t *testing.T, nodeState string) clientJournal {
	t.Helper()
	store := newJournalStore(nodeState, bytes.NewReader(make([]byte, 64)))
	var result clientJournal
	if err := store.withLock(false, func(directory *lockedJournalDirectory) error {
		journal, err := directory.load()
		if err != nil {
			return err
		}
		result = journal
		result.Attachment.Credential = append([]byte(nil), journal.Attachment.Credential...)
		journal.clear()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return result
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
