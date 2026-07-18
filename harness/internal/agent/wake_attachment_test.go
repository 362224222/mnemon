package agent

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
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

func TestWakeAttachmentPrepareOrdersStageStorePublishAndHidesCapability(t *testing.T) {
	at := time.Date(2026, 7, 17, 9, 0, 0, 0, time.UTC)
	nodeState := wakeTestNodeState(t)
	profile := serviceTestProfile(t, at)
	runID, _ := model.ParseRunID("run-wake-prepared")
	token := bytes.Repeat([]byte{0xb1}, 32)
	stageID := bytes.Repeat([]byte{0xb2}, 16)
	owner := bytes.Repeat([]byte{0xb3}, 32)
	entropy := append(append(append([]byte{}, token...), stageID...), owner...)
	storeCalled := false
	preclaimer := wakePreclaimStoreFunc(func(_ context.Context,
		spec store.AgentWakePreclaimSpec,
	) (store.AgentClaimResult, error) {
		storeCalled = true
		if spec.AttachmentTokenHash != model.Sum(token) || !spec.At.Equal(at) ||
			!spec.LeaseUntil.Equal(at.Add(5*time.Minute)) || spec.ClaimOwner == "" {
			t.Fatalf("preclaim spec = %#v", spec)
		}
		runs := filepath.Join(nodeState, "runs")
		entries, err := os.ReadDir(runs)
		if err != nil || len(entries) != 1 || !strings.HasSuffix(entries[0].Name(), ".tmp") {
			t.Fatalf("Store boundary files = %v, %v", entries, err)
		}
		if _, err := os.Lstat(filepath.Join(runs, runID.String()+".attach")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("final attachment exists before Store commit: %v", err)
		}
		return store.AgentClaimResult{Status: store.AgentClaimActionable,
			Run: wakeTestRun(t, profile, runID, spec, at)}, nil
	})
	preparer, err := NewWakeAttachmentPreparer(preclaimer, WakeAttachmentOptions{
		NodeState: nodeState, AssetRevision: profile.ActiveAssetRevision(),
		Clock: serviceTestClock{at}, Random: bytes.NewReader(entropy),
	})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := preparer.Prepare(context.Background(), profile)
	if err != nil || !storeCalled || prepared.Status() != store.AgentClaimActionable ||
		prepared.Run().ID() != runID {
		t.Fatalf("Prepare() = (%#v, %v), store=%t", prepared, err, storeCalled)
	}
	wantPath := filepath.Join(nodeState, "runs", runID.String()+".attach")
	if prepared.AttachmentPath() != wantPath ||
		prepared.Environment() != localapi.RunAttachmentEnv+"="+wantPath ||
		strings.Contains(prepared.Environment(), base64.RawURLEncoding.EncodeToString(token)) {
		t.Fatalf("prepared attachment surface = (%q, %q)", prepared.AttachmentPath(), prepared.Environment())
	}
	if err := prepared.RemoveAttachment(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(wantPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("removed prepared attachment remains: %v", err)
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
			nodeState := wakeTestNodeState(t)
			entropy := bytes.Repeat([]byte{0xc1}, 32+16+32)
			preparer, err := NewWakeAttachmentPreparer(wakePreclaimStoreFunc(func(context.Context,
				store.AgentWakePreclaimSpec,
			) (store.AgentClaimResult, error) {
				return test.result, test.err
			}), WakeAttachmentOptions{NodeState: nodeState,
				AssetRevision: profile.ActiveAssetRevision(), Clock: serviceTestClock{at},
				Random: bytes.NewReader(entropy)})
			if err != nil {
				t.Fatal(err)
			}
			prepared, gotErr := preparer.Prepare(context.Background(), profile)
			if test.err == nil && (gotErr != nil || prepared.Status() != store.AgentClaimWaiting ||
				prepared.AttachmentPath() != "") {
				t.Fatalf("waiting Prepare() = (%#v, %v)", prepared, gotErr)
			}
			if test.err != nil && !errors.Is(gotErr, ErrWakeAttachment) {
				t.Fatalf("failed Prepare() error = %v", gotErr)
			}
			entries, err := os.ReadDir(filepath.Join(nodeState, "runs"))
			if err != nil || len(entries) != 0 {
				t.Fatalf("failed Prepare stages = %v, %v", entries, err)
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
			preparer, err := NewWakeAttachmentPreparer(test.store, WakeAttachmentOptions{
				NodeState: wakeTestNodeState(t), AssetRevision: profile.ActiveAssetRevision(),
				Clock: serviceTestClock{at}, Random: bytes.NewReader(bytes.Repeat([]byte{0xc2}, 80)),
			})
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := preparer.Prepare(context.Background(), profile)
			if prepared != (PreparedWake{}) || !errors.Is(err, ErrWakeAttachment) ||
				!errors.Is(err, ErrWakeStoreNotInvoked) {
				t.Fatalf("Prepare() = (%#v, %v)", prepared, err)
			}
		})
	}
}

func TestWakeAttachmentPublishFailureNeverReturnsLaunchableRuntime(t *testing.T) {
	at := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	nodeState := wakeTestNodeState(t)
	profile := serviceTestProfile(t, at)
	runID, _ := model.ParseRunID("run-wake-collision")
	preclaimer := wakePreclaimStoreFunc(func(_ context.Context,
		spec store.AgentWakePreclaimSpec,
	) (store.AgentClaimResult, error) {
		path := filepath.Join(nodeState, "runs", runID.String()+".attach")
		if err := os.WriteFile(path, []byte("operator-owned\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return store.AgentClaimResult{Status: store.AgentClaimActionable,
			Run: wakeTestRun(t, profile, runID, spec, at)}, nil
	})
	preparer, err := NewWakeAttachmentPreparer(preclaimer, WakeAttachmentOptions{
		NodeState: nodeState, AssetRevision: profile.ActiveAssetRevision(), Clock: serviceTestClock{at},
		Random: bytes.NewReader(bytes.Repeat([]byte{0xd1}, 32+16+32))})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := preparer.Prepare(context.Background(), profile)
	if !errors.Is(err, ErrWakeAttachment) || prepared.Status() != store.AgentClaimActionable ||
		prepared.Run().ID() != runID || prepared.AttachmentPath() != "" || prepared.Environment() != "" {
		t.Fatalf("publish collision Prepare() = (%#v, %v)", prepared, err)
	}
	entries, readErr := os.ReadDir(filepath.Join(nodeState, "runs"))
	if readErr != nil || len(entries) != 1 || entries[0].Name() != runID.String()+".attach" {
		t.Fatalf("publish collision files = %v, %v", entries, readErr)
	}
}

func TestWakeAttachmentPrepareReapsOnlyStoreApprovedExpiredCapability(t *testing.T) {
	at := time.Date(2026, 7, 17, 10, 30, 0, 0, time.UTC)
	nodeState := wakeTestNodeState(t)
	profile := serviceTestProfile(t, at)
	token := bytes.Repeat([]byte{0xe1}, 32)
	stageID := bytes.Repeat([]byte{0xe2}, 16)
	staged, err := localapi.StageRunAttachment(nodeState,
		bytes.NewReader(append(token, stageID...)))
	if err != nil {
		t.Fatal(err)
	}
	runID, _ := model.ParseRunID("run-wake-reapable")
	attachment, err := staged.Publish(runID)
	if err != nil {
		t.Fatal(err)
	}
	cleanupCalled := false
	fake := wakeCleanupStore{
		wakePreclaimStoreFunc: func(context.Context,
			store.AgentWakePreclaimSpec,
		) (store.AgentClaimResult, error) {
			return store.AgentClaimResult{Status: store.AgentClaimWaiting}, nil
		},
		cleanup: func(_ context.Context,
			spec store.AgentAttachmentCleanupSpec,
		) ([]store.ReapableAgentRunAttachment, error) {
			cleanupCalled = true
			if spec.ProfileID != profile.ID() || !spec.At.Equal(at) || len(spec.Candidates) != 1 ||
				spec.Candidates[0].RunID != runID || spec.Candidates[0].TokenHash != model.Sum(token) {
				t.Fatalf("cleanup spec = %#v", spec)
			}
			return []store.ReapableAgentRunAttachment{{RunID: runID, TokenHash: model.Sum(token)}}, nil
		},
	}
	preparer, err := NewWakeAttachmentPreparer(fake, WakeAttachmentOptions{NodeState: nodeState,
		AssetRevision: profile.ActiveAssetRevision(), Clock: serviceTestClock{at},
		Random: bytes.NewReader(bytes.Repeat([]byte{0xe3}, 32+16+32))})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := preparer.Prepare(context.Background(), profile)
	if err != nil || !cleanupCalled || prepared.Status() != store.AgentClaimWaiting {
		t.Fatalf("cleanup Prepare() = (%#v, %v), called=%t", prepared, err, cleanupCalled)
	}
	if _, err := os.Lstat(attachment.Path()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("approved expired attachment remains: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(nodeState, "runs"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("cleanup Prepare files = %v, %v", entries, err)
	}
}

func wakeTestNodeState(t *testing.T) string {
	t.Helper()
	nodeState := filepath.Join(t.TempDir(), "node")
	if err := os.Mkdir(nodeState, 0o700); err != nil {
		t.Fatal(err)
	}
	return nodeState
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
