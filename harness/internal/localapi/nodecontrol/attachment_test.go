package nodecontrol

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

func TestAgentAttachmentEnvironmentMatchesLocalAPI(t *testing.T) {
	if agent.RunAttachmentEnvironment != localapi.RunAttachmentEnv {
		t.Fatalf("Run attachment environment = %q, local API = %q",
			agent.RunAttachmentEnvironment, localapi.RunAttachmentEnv)
	}
}

func TestAgentAttachmentFilesystemCleansAndDiscardsExactStages(t *testing.T) {
	nodeState := newAgentAttachmentNodeState(t)
	filesystem := NewAgentAttachmentFilesystem(nodeState)
	at := time.Date(2026, 7, 19, 9, 0, 0, 0, time.UTC)

	orphan := stageAgentAttachment(t, filesystem, 0x31)
	orphanPath := onlyAgentAttachmentStagePath(t, nodeState)
	old := at.Add(-11 * time.Minute)
	if err := os.Chtimes(orphanPath, old, old); err != nil {
		t.Fatal(err)
	}
	if removed, err := filesystem.CleanupStages(at); err != nil || removed != 1 {
		t.Fatalf("CleanupStages() = (%d, %v)", removed, err)
	}
	if err := orphan.Discard(); err != nil {
		t.Fatalf("Discard() after cleanup = %v", err)
	}

	discarded := stageAgentAttachment(t, filesystem, 0x41)
	if err := discarded.Discard(); err != nil {
		t.Fatalf("Discard() = %v", err)
	}
	if entries := agentAttachmentRunEntries(t, nodeState); len(entries) != 0 {
		t.Fatalf("discarded attachment entries = %v", entries)
	}
}

func TestAgentAttachmentFilesystemPublishesAndProjectsCandidate(t *testing.T) {
	fixture := newPublishedAgentAttachment(t, 0x51, "run-agent-attachment-bridge")
	if fixture.tokenHash != model.Sum(fixture.token) {
		t.Fatalf("TokenHash() = %s", fixture.tokenHash)
	}
	wantPath := filepath.Join(fixture.nodeState, "runs", fixture.runID.String()+".attach")
	if fixture.attachment.Path() != wantPath {
		t.Fatalf("Path() = %q, want %q", fixture.attachment.Path(), wantPath)
	}
	verified, err := localapi.ReadRunAttachment(fixture.nodeState, wantPath)
	if err != nil || verified.RunID() != fixture.runID || verified.HeaderValue() !=
		base64.RawURLEncoding.EncodeToString(fixture.token) {
		t.Fatalf("ReadRunAttachment() = (%#v, %v)", verified, err)
	}
	candidates, err := fixture.filesystem.ListCandidates()
	if err != nil || len(candidates) != 1 || candidates[0].RunID != fixture.runID ||
		candidates[0].TokenHash != model.Sum(fixture.token) {
		t.Fatalf("ListCandidates() = (%#v, %v)", candidates, err)
	}
}

func TestAgentAttachmentFilesystemRejectsReplacementAndReapsAuthorizedSecret(t *testing.T) {
	fixture := newPublishedAgentAttachment(t, 0x51, "run-agent-attachment-replacement")
	replacement := bytes.Repeat([]byte{0x61}, 32)
	if err := os.Remove(fixture.attachment.Path()); err != nil {
		t.Fatal(err)
	}
	payload := append([]byte(base64.RawURLEncoding.EncodeToString(replacement)), '\n')
	if err := os.WriteFile(fixture.attachment.Path(), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := fixture.attachment.Remove(); !errors.Is(err, localapi.ErrUnsafeClientState) {
		t.Fatalf("Remove() after inode replacement = %v", err)
	}
	if removed, err := fixture.filesystem.RemoveReapable(fixture.runID,
		model.Sum(fixture.token)); removed || !errors.Is(err, localapi.ErrUnsafeClientState) {
		t.Fatalf("RemoveReapable(original) = (%t, %v)", removed, err)
	}
	if removed, err := fixture.filesystem.RemoveReapable(fixture.runID,
		model.Sum(replacement)); err != nil || !removed {
		t.Fatalf("RemoveReapable(replacement) = (%t, %v)", removed, err)
	}
}

func TestAgentAttachmentFilesystemPreservesCandidatePageBoundAndOrder(t *testing.T) {
	nodeState := newAgentAttachmentNodeState(t)
	filesystem := NewAgentAttachmentFilesystem(nodeState)
	for index := 0; index < 66; index++ {
		staged := stageAgentAttachment(t, filesystem, byte(index+1))
		runID, err := model.ParseRunID(fmt.Sprintf("run-agent-candidate-%03d", index))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := staged.Publish(runID); err != nil {
			t.Fatal(err)
		}
	}
	direct, err := localapi.ListRunAttachmentCandidates(nodeState)
	if err != nil || !direct.More() {
		t.Fatalf("direct candidate page = (%#v, %v)", direct, err)
	}
	want := direct.Candidates()
	got, err := filesystem.ListCandidates()
	if err != nil || len(got) != len(want) {
		t.Fatalf("ListCandidates() count = (%d, %v), want %d", len(got), err, len(want))
	}
	for index := range want {
		if got[index].RunID != want[index].RunID() || got[index].TokenHash != want[index].TokenHash() {
			t.Fatalf("candidate %d = %#v, want (%s, %s)", index, got[index],
				want[index].RunID(), want[index].TokenHash())
		}
	}
}

func newAgentAttachmentNodeState(t *testing.T) string {
	t.Helper()
	nodeState := filepath.Join(t.TempDir(), "node")
	if err := os.Mkdir(nodeState, 0o700); err != nil {
		t.Fatal(err)
	}
	return nodeState
}

type publishedAgentAttachment struct {
	nodeState  string
	filesystem *AgentAttachmentFilesystem
	runID      model.RunID
	token      []byte
	tokenHash  model.Digest
	attachment node.RunAttachment
}

func newPublishedAgentAttachment(t *testing.T, tokenByte byte,
	runText string,
) publishedAgentAttachment {
	t.Helper()
	nodeState := newAgentAttachmentNodeState(t)
	filesystem := NewAgentAttachmentFilesystem(nodeState)
	staged := stageAgentAttachment(t, filesystem, tokenByte)
	runID, err := model.ParseRunID(runText)
	if err != nil {
		t.Fatal(err)
	}
	attachment, err := staged.Publish(runID)
	if err != nil {
		t.Fatal(err)
	}
	return publishedAgentAttachment{nodeState: nodeState, filesystem: filesystem, runID: runID,
		token: bytes.Repeat([]byte{tokenByte}, 32), tokenHash: staged.TokenHash(), attachment: attachment}
}

func stageAgentAttachment(t *testing.T, filesystem *AgentAttachmentFilesystem,
	value byte,
) node.StagedRunAttachment {
	t.Helper()
	staged, err := filesystem.Stage(bytes.NewReader(bytes.Repeat([]byte{value}, 48)))
	if err != nil {
		t.Fatal(err)
	}
	return staged
}

func onlyAgentAttachmentStagePath(t *testing.T, nodeState string) string {
	t.Helper()
	entries := agentAttachmentRunEntries(t, nodeState)
	if len(entries) != 1 {
		t.Fatalf("attachment stage entries = %v", entries)
	}
	return filepath.Join(nodeState, "runs", entries[0])
}

func agentAttachmentRunEntries(t *testing.T, nodeState string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(nodeState, "runs"))
	if err != nil {
		t.Fatal(err)
	}
	result := make([]string, len(entries))
	for index, entry := range entries {
		result[index] = entry.Name()
	}
	return result
}
