package agent

import (
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestServiceCurrentReadSpecBindsRuntimeActionPolicy(t *testing.T) {
	at := time.Date(2026, 7, 19, 9, 0, 0, 0, time.UTC)
	actions := testActionHandlers(t)
	service := &Service{assetRevision: actions.AssetRevision().String(), actions: actions}
	profile := serviceTestProfile(t, at)
	runID, _ := model.ParseRunID("run-service-current-policy")
	claim := model.Sum([]byte("claim-service-current-policy"))

	spec := service.currentReadSpec(profile, runID, claim, at)
	if spec.ProfileID != profile.ID() || spec.ExpectedAssetRevision != service.assetRevision ||
		spec.RunID != runID || spec.ClaimTokenHash != claim || !spec.At.Equal(at) ||
		spec.ActionPolicy.AssetRevision() != actions.AssetRevision() ||
		len(spec.ActionPolicy.Entries()) != model.TeamworkActionCount {
		t.Fatalf("current read spec = %#v", spec)
	}
}
