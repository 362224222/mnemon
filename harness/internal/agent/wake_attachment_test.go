package agent

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type wakePreclaimStoreFunc func(context.Context, store.AgentWakePreclaimSpec) (store.AgentClaimResult, error)

type wakeCleanupStore struct {
	wakePreclaimStoreFunc
	cleanup func(context.Context,
		store.AgentAttachmentCleanupSpec) ([]store.ReapableAgentRunAttachment, error)
}

func (call wakePreclaimStoreFunc) PreclaimAgentWake(ctx context.Context,
	spec store.AgentWakePreclaimSpec,
) (store.AgentClaimResult, error) {
	return call(ctx, spec)
}

func (wakePreclaimStoreFunc) ListReapableAgentRunAttachments(context.Context,
	store.AgentAttachmentCleanupSpec,
) ([]store.ReapableAgentRunAttachment, error) {
	return []store.ReapableAgentRunAttachment{}, nil
}

func (fake wakeCleanupStore) ListReapableAgentRunAttachments(ctx context.Context,
	spec store.AgentAttachmentCleanupSpec,
) ([]store.ReapableAgentRunAttachment, error) {
	return fake.cleanup(ctx, spec)
}

func TestWakeAttachmentPrepareOrdersFilesystemStoreAndPublish(t *testing.T) {
	at := time.Date(2026, 7, 17, 9, 0, 0, 0, time.UTC)
	profile := serviceTestProfile(t, at)
	runID, _ := model.ParseRunID("run-wake-prepared")
	expiredRunID, _ := model.ParseRunID("run-wake-expired")
	token := bytes.Repeat([]byte{0xb1}, 32)
	tokenHash := model.Sum(token)
	attachment := &fakeWakeRunAttachment{path: "/tmp/run-wake-prepared.attach"}
	sequence := []string{}
	stage := &fakeStagedRunAttachment{tokenHash: tokenHash, attachment: attachment,
		sequence: &sequence}
	filesystem := &fakeWakeAttachmentFilesystem{
		candidates: []WakeAttachmentCandidate{{RunID: expiredRunID, TokenHash: tokenHash}},
		stage:      stage, sequence: &sequence,
	}
	preclaimer := wakeCleanupStore{
		wakePreclaimStoreFunc: func(_ context.Context,
			spec store.AgentWakePreclaimSpec,
		) (store.AgentClaimResult, error) {
			sequence = append(sequence, "preclaim")
			if spec.AttachmentTokenHash != tokenHash || !spec.At.Equal(at) ||
				!spec.LeaseUntil.Equal(at.Add(5*time.Minute)) || spec.ClaimOwner == "" {
				t.Fatalf("preclaim spec = %#v", spec)
			}
			return store.AgentClaimResult{Status: store.AgentClaimActionable,
				Run: wakeTestRun(t, profile, runID, spec, at)}, nil
		},
		cleanup: func(_ context.Context,
			spec store.AgentAttachmentCleanupSpec,
		) ([]store.ReapableAgentRunAttachment, error) {
			sequence = append(sequence, "list_reapable")
			if spec.ProfileID != profile.ID() || !spec.At.Equal(at) || len(spec.Candidates) != 1 ||
				spec.Candidates[0].RunID != expiredRunID || spec.Candidates[0].TokenHash != tokenHash {
				t.Fatalf("cleanup spec = %#v", spec)
			}
			return []store.ReapableAgentRunAttachment{{RunID: expiredRunID,
				TokenHash: tokenHash}}, nil
		},
	}
	preparer := newWakeAttachmentPreparerForTest(t, preclaimer, filesystem, profile, at)
	prepared, err := preparer.Prepare(context.Background(), profile)
	if err != nil || prepared.Status() != store.AgentClaimActionable || prepared.Run().ID() != runID {
		t.Fatalf("Prepare() = (%#v, %v)", prepared, err)
	}
	wantSequence := []string{"list_candidates", "list_reapable", "remove_reapable",
		"cleanup_stages", "stage", "preclaim", "publish"}
	if strings.Join(sequence, ",") != strings.Join(wantSequence, ",") ||
		len(filesystem.removed) != 1 || filesystem.removed[0].RunID != expiredRunID {
		t.Fatalf("Prepare sequence = %v, removals = %#v", sequence, filesystem.removed)
	}
	wantEnvironment := RunAttachmentEnvironment + "=" + attachment.path
	if prepared.AttachmentPath() != attachment.path || prepared.Environment() != wantEnvironment ||
		strings.Contains(prepared.Environment(), base64.RawURLEncoding.EncodeToString(token)) {
		t.Fatalf("prepared attachment surface = (%q, %q)", prepared.AttachmentPath(), prepared.Environment())
	}
	if err := prepared.RemoveAttachment(); err != nil || attachment.removeCalls != 1 {
		t.Fatalf("RemoveAttachment() = %v, calls = %d", err, attachment.removeCalls)
	}
}

func TestWakeAttachmentPrepareDiscardsStagesOnNoWorkAndFailure(t *testing.T) {
	at := time.Date(2026, 7, 17, 9, 30, 0, 0, time.UTC)
	profile := serviceTestProfile(t, at)
	for _, test := range []struct {
		name   string
		result store.AgentClaimResult
		err    error
	}{
		{name: "waiting", result: store.AgentClaimResult{Status: store.AgentClaimWaiting}},
		{name: "Store failure", err: errors.New("preclaim failed")},
	} {
		t.Run(test.name, func(t *testing.T) {
			stage := &fakeStagedRunAttachment{tokenHash: model.Sum([]byte("stage"))}
			filesystem := &fakeWakeAttachmentFilesystem{stage: stage}
			preparer := newWakeAttachmentPreparerForTest(t, wakePreclaimStoreFunc(
				func(context.Context, store.AgentWakePreclaimSpec) (store.AgentClaimResult, error) {
					return test.result, test.err
				}), filesystem, profile, at)
			prepared, gotErr := preparer.Prepare(context.Background(), profile)
			if test.err == nil && (gotErr != nil || prepared.Status() != store.AgentClaimWaiting ||
				prepared.AttachmentPath() != "" || prepared.Environment() != "") {
				t.Fatalf("waiting Prepare() = (%#v, %v)", prepared, gotErr)
			}
			if test.err != nil && !errors.Is(gotErr, ErrWakeAttachment) {
				t.Fatalf("failed Prepare() error = %v", gotErr)
			}
			if stage.discardCalls != 1 || stage.publishCalls != 0 {
				t.Fatalf("stage calls = publish %d, discard %d", stage.publishCalls, stage.discardCalls)
			}
		})
	}
}

func TestWakeAttachmentPreservesProofThatStoreWasNotInvoked(t *testing.T) {
	at := time.Date(2026, 7, 17, 9, 45, 0, 0, time.UTC)
	profile := serviceTestProfile(t, at)

	for _, test := range []struct {
		name  string
		store WakePreclaimStore
	}{
		{name: "cleanup admission", store: wakeCleanupStore{
			wakePreclaimStoreFunc: func(context.Context,
				store.AgentWakePreclaimSpec,
			) (store.AgentClaimResult, error) {
				t.Fatal("preclaim called after cleanup admission rejection")
				return store.AgentClaimResult{}, nil
			},
			cleanup: func(context.Context,
				store.AgentAttachmentCleanupSpec,
			) ([]store.ReapableAgentRunAttachment, error) {
				return nil, ErrWakeStoreNotInvoked
			},
		}},
		{name: "preclaim admission", store: wakePreclaimStoreFunc(func(context.Context,
			store.AgentWakePreclaimSpec,
		) (store.AgentClaimResult, error) {
			return store.AgentClaimResult{}, ErrWakeStoreNotInvoked
		})},
	} {
		t.Run(test.name, func(t *testing.T) {
			stage := &fakeStagedRunAttachment{tokenHash: model.Sum([]byte("stage"))}
			filesystem := &fakeWakeAttachmentFilesystem{stage: stage}
			preparer := newWakeAttachmentPreparerForTest(t, test.store, filesystem, profile, at)
			prepared, err := preparer.Prepare(context.Background(), profile)
			if prepared != (PreparedWake{}) || !errors.Is(err, ErrWakeAttachment) ||
				!errors.Is(err, ErrWakeStoreNotInvoked) {
				t.Fatalf("Prepare() = (%#v, %v)", prepared, err)
			}
			if test.name == "cleanup admission" && filesystem.stageCalls != 0 {
				t.Fatal("attachment was staged after cleanup admission rejection")
			}
		})
	}
}

func TestWakeAttachmentPublishFailureRetainsActionableRunWithoutAttachment(t *testing.T) {
	at := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	profile := serviceTestProfile(t, at)
	runID, _ := model.ParseRunID("run-wake-publish-failure")
	for _, test := range []struct {
		name       string
		attachment RunAttachment
		publishErr error
	}{
		{name: "publish error", publishErr: errors.New("publish failed")},
		{name: "missing published attachment"},
	} {
		t.Run(test.name, func(t *testing.T) {
			stage := &fakeStagedRunAttachment{tokenHash: model.Sum([]byte("stage")),
				attachment: test.attachment, publishErr: test.publishErr}
			filesystem := &fakeWakeAttachmentFilesystem{stage: stage}
			preclaimer := wakePreclaimStoreFunc(func(_ context.Context,
				spec store.AgentWakePreclaimSpec,
			) (store.AgentClaimResult, error) {
				return store.AgentClaimResult{Status: store.AgentClaimActionable,
					Run: wakeTestRun(t, profile, runID, spec, at)}, nil
			})
			preparer := newWakeAttachmentPreparerForTest(t, preclaimer, filesystem, profile, at)
			prepared, err := preparer.Prepare(context.Background(), profile)
			if !errors.Is(err, ErrWakeAttachment) || prepared.Status() != store.AgentClaimActionable ||
				prepared.Run().ID() != runID || prepared.AttachmentPath() != "" ||
				prepared.Environment() != "" || stage.discardCalls != 1 {
				t.Fatalf("publish failure Prepare() = (%#v, %v), discard=%d",
					prepared, err, stage.discardCalls)
			}
		})
	}
}

func TestWakeAttachmentPassesBoundedCandidateOrderAndReapsOnlyStoreApproval(t *testing.T) {
	at := time.Date(2026, 7, 17, 10, 30, 0, 0, time.UTC)
	profile := serviceTestProfile(t, at)
	firstRunID, _ := model.ParseRunID("run-wake-first")
	secondRunID, _ := model.ParseRunID("run-wake-second")
	firstHash := model.Sum([]byte("first"))
	secondHash := model.Sum([]byte("second"))
	filesystem := &fakeWakeAttachmentFilesystem{
		candidates: []WakeAttachmentCandidate{
			{RunID: firstRunID, TokenHash: firstHash},
			{RunID: secondRunID, TokenHash: secondHash},
		},
		stage: &fakeStagedRunAttachment{tokenHash: model.Sum([]byte("stage"))},
	}
	fake := wakeCleanupStore{
		wakePreclaimStoreFunc: func(context.Context,
			store.AgentWakePreclaimSpec,
		) (store.AgentClaimResult, error) {
			return store.AgentClaimResult{Status: store.AgentClaimWaiting}, nil
		},
		cleanup: func(_ context.Context,
			spec store.AgentAttachmentCleanupSpec,
		) ([]store.ReapableAgentRunAttachment, error) {
			if len(spec.Candidates) != 2 || spec.Candidates[0].RunID != firstRunID ||
				spec.Candidates[0].TokenHash != firstHash || spec.Candidates[1].RunID != secondRunID ||
				spec.Candidates[1].TokenHash != secondHash {
				t.Fatalf("ordered cleanup candidates = %#v", spec.Candidates)
			}
			return []store.ReapableAgentRunAttachment{{RunID: secondRunID, TokenHash: secondHash}}, nil
		},
	}
	preparer := newWakeAttachmentPreparerForTest(t, fake, filesystem, profile, at)
	prepared, err := preparer.Prepare(context.Background(), profile)
	if err != nil || prepared.Status() != store.AgentClaimWaiting {
		t.Fatalf("Prepare() = (%#v, %v)", prepared, err)
	}
	if len(filesystem.removed) != 1 || filesystem.removed[0].RunID != secondRunID ||
		filesystem.removed[0].TokenHash != secondHash {
		t.Fatalf("filesystem removals = %#v", filesystem.removed)
	}
}

func newWakeAttachmentPreparerForTest(t *testing.T, st WakePreclaimStore,
	filesystem WakeAttachmentFilesystem, profile model.Profile, at time.Time,
) *WakeAttachmentPreparer {
	t.Helper()
	preparer, err := NewWakeAttachmentPreparer(st, WakeAttachmentOptions{
		Attachments: filesystem, AssetRevision: profile.ActiveAssetRevision(),
		Clock: serviceTestClock{at}, Random: bytes.NewReader(bytes.Repeat([]byte{0xc1}, 80)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return preparer
}

type fakeWakeAttachmentFilesystem struct {
	candidates []WakeAttachmentCandidate
	stage      StagedRunAttachment
	listErr    error
	removeErr  error
	cleanupErr error
	stageErr   error
	sequence   *[]string
	removed    []WakeAttachmentCandidate
	stageCalls int
}

func (filesystem *fakeWakeAttachmentFilesystem) record(value string) {
	if filesystem.sequence != nil {
		*filesystem.sequence = append(*filesystem.sequence, value)
	}
}

func (filesystem *fakeWakeAttachmentFilesystem) ListCandidates() ([]WakeAttachmentCandidate, error) {
	filesystem.record("list_candidates")
	return append([]WakeAttachmentCandidate(nil), filesystem.candidates...), filesystem.listErr
}

func (filesystem *fakeWakeAttachmentFilesystem) RemoveReapable(runID model.RunID,
	tokenHash model.Digest,
) (bool, error) {
	filesystem.record("remove_reapable")
	filesystem.removed = append(filesystem.removed,
		WakeAttachmentCandidate{RunID: runID, TokenHash: tokenHash})
	return filesystem.removeErr == nil, filesystem.removeErr
}

func (filesystem *fakeWakeAttachmentFilesystem) CleanupStages(time.Time) (int, error) {
	filesystem.record("cleanup_stages")
	return 0, filesystem.cleanupErr
}

func (filesystem *fakeWakeAttachmentFilesystem) Stage(io.Reader) (StagedRunAttachment, error) {
	filesystem.record("stage")
	filesystem.stageCalls++
	return filesystem.stage, filesystem.stageErr
}

type fakeStagedRunAttachment struct {
	tokenHash    model.Digest
	attachment   RunAttachment
	publishErr   error
	discardErr   error
	sequence     *[]string
	publishCalls int
	discardCalls int
}

func (stage *fakeStagedRunAttachment) TokenHash() model.Digest { return stage.tokenHash }

func (stage *fakeStagedRunAttachment) Publish(model.RunID) (RunAttachment, error) {
	stage.publishCalls++
	if stage.sequence != nil {
		*stage.sequence = append(*stage.sequence, "publish")
	}
	return stage.attachment, stage.publishErr
}

func (stage *fakeStagedRunAttachment) Discard() error {
	stage.discardCalls++
	return stage.discardErr
}

type fakeWakeRunAttachment struct {
	path        string
	removeErr   error
	removeCalls int
}

func (attachment *fakeWakeRunAttachment) Path() string { return attachment.path }
func (attachment *fakeWakeRunAttachment) Remove() error {
	attachment.removeCalls++
	return attachment.removeErr
}

func wakeTestRun(t *testing.T, profile model.Profile, runID model.RunID,
	spec store.AgentWakePreclaimSpec, at time.Time,
) model.AgentRun {
	t.Helper()
	handlingID, _ := model.ParseHandlingID("handling-wake-prepared")
	empty, _ := model.NewJSON([]byte(`{}`))
	cause, _ := model.NewJSON([]byte(`{"kind":"wake"}`))
	run, err := model.NewAgentRun(model.AgentRunSpec{ID: runID, ProfileID: profile.ID(),
		HandlingID: &handlingID, Cause: cause, HandlingAttempt: 1,
		ClaimFenceHash: &spec.AttachmentTokenHash, LeaseUntil: &spec.LeaseUntil,
		AttachmentTokenHash: &spec.AttachmentTokenHash, AttachmentExpiresAt: &spec.LeaseUntil,
		Launcher: "mnemond-wake", Runtime: profile.Runtime(), LauncherDiagnostic: empty,
		RuntimeIDs: empty, Status: model.AgentRunStarting, StartedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	return run
}

var _ WakeAttachmentFilesystem = (*fakeWakeAttachmentFilesystem)(nil)
var _ StagedRunAttachment = (*fakeStagedRunAttachment)(nil)
var _ RunAttachment = (*fakeWakeRunAttachment)(nil)
