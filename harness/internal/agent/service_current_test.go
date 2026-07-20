package agent

import (
	"context"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestClaimAgentCurrentAttachmentUsesDurableFenceWithoutEntropy(t *testing.T) {
	at := time.Date(2026, 7, 20, 9, 0, 0, 0, time.UTC)
	profile := serviceTestProfile(t, at)
	runID, _ := model.ParseRunID("run-current-helper-attachment")
	handlingID, _ := model.ParseHandlingID("handling-current-helper-attachment")
	claimHash := model.Sum([]byte("current-helper-fence"))
	leaseUntil := at.Add(5 * time.Minute)
	run, err := model.NewAgentRun(model.AgentRunSpec{ID: runID, ProfileID: profile.ID(),
		HandlingID: &handlingID, Cause: mustServiceJSON(t, `{"kind":"wake"}`), HandlingAttempt: 1,
		ClaimFenceHash: &claimHash, LeaseUntil: &leaseUntil, AttachmentTokenHash: &claimHash,
		AttachmentExpiresAt: &leaseUntil, AttachedAt: &at, Launcher: "service-current-test",
		Runtime: profile.Runtime(), LauncherDiagnostic: mustServiceJSON(t, `{}`),
		RuntimeIDs: mustServiceJSON(t, `{}`), Status: model.AgentRunRunning,
		RuntimeStartedAt: &at, StartedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	consumeCalls := 0
	fake := &fakeControlStore{consume: func(_ context.Context,
		spec store.AgentAttachmentSpec,
	) (store.AgentClaimResult, error) {
		consumeCalls++
		if spec.ProfileID != profile.ID() || spec.ExpectedAssetRevision != profile.ActiveAssetRevision() ||
			spec.AttachmentTokenHash != claimHash || !spec.At.Equal(at) {
			t.Fatalf("attachment claim spec = %#v", spec)
		}
		return store.AgentClaimResult{Status: store.AgentClaimActionable, Run: run}, nil
	}}
	random := &countingServiceRandom{}
	service, err := NewService(fake, ServiceOptions{Actions: testActionHandlers(t),
		Clock: serviceTestClock{at}, Random: random, CurrentViews: &fakeAgentCurrentViews{}})
	if err != nil {
		t.Fatal(err)
	}
	claim, apiErr := service.claimAgentCurrent(context.Background(), ControlMetadata{
		Profile: profile, HasRunAttachment: true, RunAttachmentHash: claimHash,
	}, at)
	if apiErr != nil || claim.result.Run.ID() != runID || claim.hash != claimHash ||
		claim.secret != nil || consumeCalls != 1 || random.calls != 0 {
		t.Fatalf("attached claim = (%#v, %v), consumes=%d entropy=%d",
			claim, apiErr, consumeCalls, random.calls)
	}
}
