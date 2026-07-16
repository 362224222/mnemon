package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestDeactivateProfileWithdrawsExactAuthorityAndReplays(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	node, disabled := bootstrapValues(t, "peer-deactivate", "principal-deactivate", "/workspace/deactivate")
	node, active := activateTestNode(t, st, node, disabled)
	at := active.UpdatedAt().Add(time.Minute)

	first, err := st.DeactivateProfile(context.Background(), active, at)
	if err != nil || !first.Changed || first.Profile.Enabled() ||
		first.Profile.Host() != active.Host() || first.Profile.Runtime() != active.Runtime() ||
		first.Profile.ActiveAssetRevision() != active.ActiveAssetRevision() ||
		first.Node.ActiveAssetRevision() != node.ActiveAssetRevision() ||
		!first.Node.UpdatedAt().Equal(at) || !first.Profile.UpdatedAt().Equal(at) {
		t.Fatalf("DeactivateProfile() = (%#v, %v)", first, err)
	}

	second, err := st.DeactivateProfile(context.Background(), first.Profile, at.Add(time.Hour))
	if err != nil || second.Changed || second.Profile.Enabled() ||
		!second.Node.UpdatedAt().Equal(at) || !second.Profile.UpdatedAt().Equal(at) {
		t.Fatalf("replayed DeactivateProfile() = (%#v, %v)", second, err)
	}
	staged, err := st.ActivateProfile(context.Background(), active, first.Profile.UpdatedAt(), at.Add(time.Hour))
	if err != nil || !staged.Changed || !staged.Profile.Enabled() ||
		staged.Profile.Host() != active.Host() || staged.Node.ActiveAssetRevision() != node.ActiveAssetRevision() {
		t.Fatalf("reactivation = (%#v, %v)", staged, err)
	}
}

func TestDeactivateProfileRejectsDriftAndRegressedTime(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	node, disabled := bootstrapValues(t, "peer-deactivate-drift", "principal-deactivate-drift",
		"/workspace/deactivate-drift")
	_, active := activateTestNode(t, st, node, disabled)

	driftedSpec := active.Spec()
	driftedSpec.ActiveAssetRevision = "asset-other"
	drifted, err := model.NewProfile(driftedSpec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.DeactivateProfile(context.Background(), drifted,
		active.UpdatedAt().Add(time.Minute)); !errors.Is(err, ErrProfileDeactivationConflict) {
		t.Fatalf("authority drift error = %v", err)
	}
	staleSpec := active.Spec()
	staleSpec.UpdatedAt = staleSpec.UpdatedAt.Add(-time.Nanosecond)
	stale, err := model.NewProfile(staleSpec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.DeactivateProfile(context.Background(), stale,
		active.UpdatedAt().Add(time.Minute)); !errors.Is(err, ErrProfileDeactivationConflict) {
		t.Fatalf("generation drift error = %v", err)
	}
	if _, err := st.DeactivateProfile(context.Background(), active,
		active.UpdatedAt().Add(-time.Second)); !errors.Is(err, ErrProfileDeactivationConflict) {
		t.Fatalf("regressed time error = %v", err)
	}
	if _, err := st.DeactivateProfile(context.Background(), active,
		active.UpdatedAt()); !errors.Is(err, ErrProfileDeactivationConflict) {
		t.Fatalf("equal-time deactivation error = %v", err)
	}
	read, err := st.ReadLocalAuthority(context.Background())
	if err != nil || !read.Profile.Enabled() || read.Profile.ActiveAssetRevision() != active.ActiveAssetRevision() {
		t.Fatalf("failed deactivation changed authority = (%#v, %v)", read, err)
	}
}

func TestDeactivateProfileRequiresQuiescentAgentAuthority(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		id   string
		busy func(*testing.T, *Store, model.Node, model.Profile)
	}{
		{name: "claimed handling", id: "deactivate-handling", busy: insertActivationClaimedHandling},
		{name: "starting Agent", id: "deactivate-run", busy: insertActivationAgentRun},
		{name: "runtime-finished Agent", id: "deactivate-runtime-finished", busy: insertActivationRuntimeFinishedRun},
		{name: "started operation", id: "deactivate-operation", busy: insertActivationStartedOperation},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			st := openTestStore(t)
			node, disabled := bootstrapValues(t, "peer-"+test.id, "principal-"+test.id,
				"/workspace/"+test.id)
			node, active := activateTestNode(t, st, node, disabled)
			test.busy(t, st, node, active)

			if _, err := st.DeactivateProfile(context.Background(), active,
				active.UpdatedAt().Add(time.Minute)); !errors.Is(err, ErrProfileDeactivationBusy) {
				t.Fatalf("busy deactivation error = %v", err)
			}
			read, err := st.ReadLocalAuthority(context.Background())
			if err != nil || !read.Profile.Enabled() ||
				!read.Node.UpdatedAt().Equal(active.UpdatedAt()) ||
				!read.Profile.UpdatedAt().Equal(active.UpdatedAt()) {
				t.Fatalf("busy failure changed authority = (%#v, %v)", read, err)
			}
		})
	}
}
