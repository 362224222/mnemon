package localapi

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestRunAttachmentStagesPublishesAndRemovesExactOwnerCapability(t *testing.T) {
	t.Parallel()
	nodeState := newClientNodeState(t)
	token := bytes.Repeat([]byte{0x71}, opaqueSecretBytes)
	stageID := bytes.Repeat([]byte{0x72}, runAttachmentStageIDBytes)
	staged, err := StageRunAttachment(nodeState, bytes.NewReader(append(token, stageID...)))
	if err != nil {
		t.Fatal(err)
	}
	if staged.TokenHash() != model.Sum(token) || !validRunAttachmentStageName(filepath.Base(staged.path)) {
		t.Fatalf("staged attachment = %#v", staged)
	}
	assertOwnerPath(t, staged.path, false, ownerRegularFileMode)
	runID, _ := model.ParseRunID("run-wake-attachment")
	published, err := staged.Publish(runID)
	if err != nil {
		t.Fatal(err)
	}
	wantPath := filepath.Join(nodeState, "runs", runID.String()+runAttachmentSuffix)
	if published.Path() != wantPath || published.RunID() != runID ||
		published.HeaderValue() != base64Value(token) {
		t.Fatalf("published attachment = %#v", published)
	}
	if _, err := os.Lstat(staged.path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stage remains after publication: %v", err)
	}
	read, err := ReadRunAttachment(nodeState, wantPath)
	if err != nil || read.Digest() != published.Digest() || !os.SameFile(read.identity, published.identity) {
		t.Fatalf("ReadRunAttachment() = (%#v, %v)", read, err)
	}
	if err := RemoveRunAttachment(nodeState, read); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(wantPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("attachment remains after consume: %v", err)
	}
}

func TestRunAttachmentFailsClosedForReplacementAndUnsafePaths(t *testing.T) {
	t.Parallel()
	nodeState := newClientNodeState(t)
	staged, err := StageRunAttachment(nodeState, bytes.NewReader(bytes.Repeat([]byte{0x81},
		opaqueSecretBytes+runAttachmentStageIDBytes)))
	if err != nil {
		t.Fatal(err)
	}
	runID, _ := model.ParseRunID("run-wake-replaced")
	attachment, err := staged.Publish(runID)
	if err != nil {
		t.Fatal(err)
	}
	moved := attachment.Path() + ".original"
	if err := os.Rename(attachment.Path(), moved); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(moved)
	mustWrite(t, attachment.Path(), raw, ownerRegularFileMode)
	if err := RemoveRunAttachment(nodeState, attachment); !errors.Is(err, ErrUnsafeClientState) {
		t.Fatalf("replacement removal error = %v", err)
	}
	if _, err := ReadRunAttachment(nodeState, "relative.attach"); !errors.Is(err, ErrUnsafeClientState) {
		t.Fatalf("relative attachment error = %v", err)
	}
	if _, err := ReadRunAttachment(nodeState, filepath.Join(nodeState, "outside.attach")); !errors.Is(err, ErrUnsafeClientState) {
		t.Fatalf("outside attachment error = %v", err)
	}
	mustChmod(t, attachment.Path(), 0o644)
	if _, err := ReadRunAttachment(nodeState, attachment.Path()); !errors.Is(err, ErrUnsafeClientState) {
		t.Fatalf("unsafe mode error = %v", err)
	}
}

func TestRunAttachmentStageDiscardAndBoundedOrphanCleanup(t *testing.T) {
	t.Parallel()
	nodeState := newClientNodeState(t)
	first, err := StageRunAttachment(nodeState, bytes.NewReader(bytes.Repeat([]byte{0x91},
		opaqueSecretBytes+runAttachmentStageIDBytes)))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Discard(); err != nil {
		t.Fatal(err)
	}
	if err := first.Discard(); err != nil {
		t.Fatalf("idempotent discard error = %v", err)
	}
	second, err := StageRunAttachment(nodeState, bytes.NewReader(bytes.Repeat([]byte{0x92},
		opaqueSecretBytes+runAttachmentStageIDBytes)))
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 7, 17, 8, 0, 0, 0, time.UTC)
	old := at.Add(-maxRunAttachmentStageAge)
	if err := os.Chtimes(second.path, old, old); err != nil {
		t.Fatal(err)
	}
	runID, _ := model.ParseRunID("run-final-not-mtime-gc")
	third, err := StageRunAttachment(nodeState, bytes.NewReader(bytes.Repeat([]byte{0x93},
		opaqueSecretBytes+runAttachmentStageIDBytes)))
	if err != nil {
		t.Fatal(err)
	}
	final, err := third.Publish(runID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(final.Path(), old, old); err != nil {
		t.Fatal(err)
	}
	removed, err := CleanupRunAttachmentStages(nodeState, at)
	if err != nil || removed != 1 {
		t.Fatalf("CleanupRunAttachmentStages() = (%d, %v)", removed, err)
	}
	if _, err := os.Lstat(second.path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired stage remains: %v", err)
	}
	if _, err := os.Lstat(final.Path()); err != nil {
		t.Fatalf("final attachment was removed by mtime cleanup: %v", err)
	}
	if _, err := CleanupRunAttachmentStages(nodeState, time.Time{}); !errors.Is(err, ErrUnsafeClientState) {
		t.Fatalf("invalid cleanup time error = %v", err)
	}
}

func TestRunAttachmentErrorsNeverExposeCapability(t *testing.T) {
	t.Parallel()
	nodeState := newClientNodeState(t)
	secret := bytes.Repeat([]byte{0xa1}, opaqueSecretBytes)
	stageID := bytes.Repeat([]byte{0xa2}, runAttachmentStageIDBytes)
	staged, err := StageRunAttachment(nodeState, bytes.NewReader(append(secret, stageID...)))
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, staged.path, []byte(base64Value(secret)+"=\n"), ownerRegularFileMode)
	if err := staged.Discard(); err == nil || strings.Contains(err.Error(), base64Value(secret)) {
		t.Fatalf("unsafe stage error leaks capability: %v", err)
	}
}

func TestRunAttachmentReapRequiresExactDurableTokenHash(t *testing.T) {
	t.Parallel()
	nodeState := newClientNodeState(t)
	token := bytes.Repeat([]byte{0xb1}, opaqueSecretBytes)
	stageID := bytes.Repeat([]byte{0xb2}, runAttachmentStageIDBytes)
	staged, err := StageRunAttachment(nodeState, bytes.NewReader(append(token, stageID...)))
	if err != nil {
		t.Fatal(err)
	}
	runID, _ := model.ParseRunID("run-reapable-attachment")
	attachment, err := staged.Publish(runID)
	if err != nil {
		t.Fatal(err)
	}
	if removed, err := RemoveReapableRunAttachment(nodeState, runID,
		model.Sum([]byte("wrong-token"))); !errors.Is(err, ErrUnsafeClientState) || removed {
		t.Fatalf("wrong-hash reap = (%t, %v)", removed, err)
	}
	if _, err := os.Lstat(attachment.Path()); err != nil {
		t.Fatalf("wrong-hash reap removed attachment: %v", err)
	}
	if removed, err := RemoveReapableRunAttachment(nodeState, runID, model.Sum(token)); err != nil || !removed {
		t.Fatalf("exact reap = (%t, %v)", removed, err)
	}
	if removed, err := RemoveReapableRunAttachment(nodeState, runID, model.Sum(token)); err != nil || removed {
		t.Fatalf("idempotent reap = (%t, %v)", removed, err)
	}
}

func TestRunAttachmentCandidatePagesAreFilesystemBounded(t *testing.T) {
	t.Parallel()
	nodeState := newClientNodeState(t)
	want := make(map[model.RunID]model.Digest, maxRunAttachmentCandidates+1)
	for index := 0; index < maxRunAttachmentCandidates+1; index++ {
		token := bytes.Repeat([]byte{byte(index + 1)}, opaqueSecretBytes)
		stageID := make([]byte, runAttachmentStageIDBytes)
		stageID[0], stageID[len(stageID)-1] = byte(index+1), byte(0xff-index)
		staged, err := StageRunAttachment(nodeState, bytes.NewReader(append(token, stageID...)))
		if err != nil {
			t.Fatal(err)
		}
		runID, err := model.ParseRunID(fmt.Sprintf("run-candidate-page-%03d", index))
		if err != nil {
			t.Fatal(err)
		}
		if runID.IsZero() || !safeManagedFilename(runID.String()) {
			t.Fatalf("candidate Run ID %q is unexpectedly unsafe", runID.String())
		}
		if _, err := staged.Publish(runID); err != nil {
			t.Fatalf("publish candidate %d %q: %v", index, runID.String(), err)
		}
		want[runID] = model.Sum(token)
	}
	first, err := ListRunAttachmentCandidates(nodeState)
	if err != nil || len(first.Candidates()) != maxRunAttachmentCandidates || !first.More() {
		t.Fatalf("first candidate page = (%d, more=%t, %v)", len(first.Candidates()), first.More(), err)
	}
	for _, candidate := range first.Candidates() {
		if want[candidate.RunID()] != candidate.TokenHash() {
			t.Fatalf("candidate %s hash differs", candidate.RunID())
		}
		if removed, err := RemoveReapableRunAttachment(nodeState, candidate.RunID(),
			candidate.TokenHash()); err != nil || !removed {
			t.Fatalf("remove candidate %s = (%t, %v)", candidate.RunID(), removed, err)
		}
		delete(want, candidate.RunID())
	}
	second, err := ListRunAttachmentCandidates(nodeState)
	if err != nil || len(second.Candidates()) != 1 || second.More() {
		t.Fatalf("second candidate page = (%d, more=%t, %v)", len(second.Candidates()), second.More(), err)
	}
	remaining := second.Candidates()[0]
	if len(want) != 1 || want[remaining.RunID()] != remaining.TokenHash() {
		t.Fatalf("remaining candidates = %#v, want=%#v", second.Candidates(), want)
	}
}

func base64Value(raw []byte) string {
	return base64.RawURLEncoding.EncodeToString(raw)
}
