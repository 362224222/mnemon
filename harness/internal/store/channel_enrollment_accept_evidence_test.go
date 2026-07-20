package store

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestChannelEnrollmentReplayPlanTracksTerminalAuthorityProgress(t *testing.T) {
	t.Parallel()
	fixture := newOwnerAcceptancePlanFixture(t, "accept-replay-plan-progress")
	accepted, closedAt := commitAndCloseEnrollmentForReplay(t, fixture)
	replaySpec, replay := prepareNoopChannelEnrollmentReplay(t, fixture, closedAt.Add(time.Second))
	assertNoopChannelEnrollmentReplay(t, fixture, replay, accepted)
	terminalAt := replaySpec.At.Add(time.Second)
	terminalRoster := advanceChannelEnrollmentToRevoked(t, fixture, accepted, terminalAt)
	assertEnrollmentPlanDiverged(t, fixture.owner.ownerStore, replay)
	replaySpec.At = terminalAt.Add(time.Second)
	assertRefreshedTerminalEnrollmentReplay(t, fixture, replaySpec, accepted, terminalRoster)
}

func commitAndCloseEnrollmentForReplay(t *testing.T,
	fixture ownerAcceptancePlanFixture,
) (AcceptChannelEnrollmentResult, time.Time) {
	t.Helper()
	accepted, err := fixture.owner.ownerStore.CommitChannelEnrollment(context.Background(),
		fixture.finalPlan(t))
	if err != nil {
		t.Fatal(err)
	}
	closedAt := fixture.owner.acceptedAt.Add(time.Second)
	if _, err := fixture.owner.ownerStore.CloseChannelInvite(context.Background(),
		fixture.owner.channel.Channel().ID(), fixture.owner.grantID, closedAt); err != nil {
		t.Fatal(err)
	}
	return accepted, closedAt
}

func prepareNoopChannelEnrollmentReplay(t testing.TB, fixture ownerAcceptancePlanFixture,
	at time.Time,
) (PrepareChannelEnrollmentSigningSpec, ChannelEnrollmentPlan) {
	t.Helper()
	spec := fixture.signingSpec
	spec.At = at
	signing, err := fixture.owner.ownerStore.PrepareChannelEnrollmentSigning(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if signing.RequiresSignatures() || len(signing.MemberSigningMessage()) != 0 ||
		len(signing.ReceiptSigningMessage()) != 0 {
		t.Fatalf("replay signing plan requires signatures: %#v", signing)
	}
	if _, err := fixture.owner.ownerStore.PrepareSignedChannelEnrollment(context.Background(), signing,
		ChannelEnrollmentSignatures{MemberSignature: []byte{1}}); !errors.Is(err,
		ErrChannelEnrollmentOwner) {
		t.Fatalf("signed replay error = %v", err)
	}
	plan, err := fixture.owner.ownerStore.PrepareSignedChannelEnrollment(context.Background(), signing,
		ChannelEnrollmentSignatures{})
	if err != nil || plan.ChangesAuthority() {
		t.Fatalf("PrepareSignedChannelEnrollment(replay) = (%t,%v)", plan.ChangesAuthority(), err)
	}
	return spec, plan
}

func assertNoopChannelEnrollmentReplay(t testing.TB, fixture ownerAcceptancePlanFixture,
	plan ChannelEnrollmentPlan, accepted AcceptChannelEnrollmentResult,
) {
	t.Helper()
	if resolution, err := fixture.owner.ownerStore.ResolveChannelEnrollment(context.Background(),
		plan); err != nil || resolution != ChannelAuthorityPlanCandidate {
		t.Fatalf("ResolveChannelEnrollment(no-op) = (%q,%v)", resolution, err)
	}
	replayed, err := fixture.owner.ownerStore.CommitChannelEnrollment(context.Background(), plan)
	if err != nil || replayed.Status != ChannelEnrollmentReplayed {
		t.Fatalf("CommitChannelEnrollment(no-op) = (%#v,%v)", replayed, err)
	}
	if !bytes.Equal(replayed.Receipt.WireJSON().Bytes(), accepted.Receipt.WireJSON().Bytes()) {
		t.Fatal("no-op replay changed the original receipt")
	}
	assertEnrollmentTableCounts(t, fixture.owner.ownerStore, map[string]int{
		"channel_members": 2, "enrollment_grant_uses": 1, "enrollment_receipts": 1,
		"peer_bindings": 1,
	})
}

func advanceChannelEnrollmentToRevoked(t testing.TB, fixture ownerAcceptancePlanFixture,
	accepted AcceptChannelEnrollmentResult, at time.Time,
) model.VerifiedRoster {
	t.Helper()
	terminal, terminalRoster := appendRosterTerminal(t, fixture.owner.channel.Descriptor(),
		fixture.owner.signer, accepted.Roster, fixture.owner.joiner.PeerID(), model.MemberRevoked, at)
	merged, err := fixture.owner.ownerStore.MergeChannelRoster(context.Background(),
		MergeChannelRosterSpec{ChannelID: fixture.owner.channel.Channel().ID(),
			AuthenticatedTransportPeerID: fixture.owner.channel.Owner().PeerID(),
			Records:                      []model.Member{terminal}, At: at})
	if err != nil || merged.Roster.Head() != terminalRoster.Head() {
		t.Fatalf("terminal authority progress = (%#v,%v)", merged, err)
	}
	return terminalRoster
}

func assertRefreshedTerminalEnrollmentReplay(t testing.TB, fixture ownerAcceptancePlanFixture,
	spec PrepareChannelEnrollmentSigningSpec, accepted AcceptChannelEnrollmentResult,
	terminalRoster model.VerifiedRoster,
) {
	t.Helper()
	signing, err := fixture.owner.ownerStore.PrepareChannelEnrollmentSigning(context.Background(), spec)
	if err != nil || signing.RequiresSignatures() {
		t.Fatalf("refreshed terminal signing = (%t,%v)", signing.RequiresSignatures(), err)
	}
	plan, err := fixture.owner.ownerStore.PrepareSignedChannelEnrollment(context.Background(), signing,
		ChannelEnrollmentSignatures{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := fixture.owner.ownerStore.CommitChannelEnrollment(context.Background(), plan)
	if err != nil || result.Status != ChannelEnrollmentMemberRevoked ||
		result.Roster.Head() != terminalRoster.Head() {
		t.Fatalf("refreshed terminal replay = (%#v,%v)", result, err)
	}
	if !bytes.Equal(result.Receipt.WireJSON().Bytes(), accepted.Receipt.WireJSON().Bytes()) {
		t.Fatal("terminal replay changed the original receipt")
	}
}

func TestChannelEnrollmentPlanExactEvidenceRejectsNonMeshDrift(t *testing.T) {
	t.Parallel()
	t.Run("binding reachability", func(t *testing.T) {
		fixture := newOwnerAcceptancePlanFixture(t, "accept-binding-evidence")
		plan := fixture.finalPlan(t)
		accepted, err := fixture.owner.ownerStore.CommitChannelEnrollment(context.Background(), plan)
		if err != nil {
			t.Fatal(err)
		}
		changed, err := fixture.owner.ownerStore.SetPeerReachability(context.Background(),
			SetPeerReachabilitySpec{ChannelID: accepted.Channel.ID(),
				PeerID:             fixture.owner.joiner.PeerID(),
				OriginEpoch:        fixture.owner.joiner.OriginEpoch(),
				ExpectedRosterHead: accepted.Roster.Head(),
				Reachability:       model.ReachabilityReachable,
				At:                 accepted.Member.CreatedAt().Add(time.Second)})
		if err != nil || !changed.Changed {
			t.Fatalf("SetPeerReachability() = (%#v,%v)", changed, err)
		}
		assertEnrollmentPlanDiverged(t, fixture.owner.ownerStore, plan)
	})

	t.Run("grant status", func(t *testing.T) {
		fixture := newOwnerAcceptancePlanFixture(t, "accept-grant-evidence")
		plan := fixture.finalPlan(t)
		if _, err := fixture.owner.ownerStore.CommitChannelEnrollment(context.Background(), plan); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.owner.ownerStore.CloseChannelInvite(context.Background(),
			fixture.owner.channel.Channel().ID(), fixture.owner.grantID,
			fixture.owner.acceptedAt.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		assertEnrollmentPlanDiverged(t, fixture.owner.ownerStore, plan)
	})
}

func assertEnrollmentPlanDiverged(t testing.TB, st *Store, plan ChannelEnrollmentPlan) {
	t.Helper()
	if resolution, err := st.ResolveChannelEnrollment(context.Background(), plan); err != nil ||
		resolution != ChannelAuthorityPlanDiverged {
		t.Fatalf("ResolveChannelEnrollment(drift) = (%q,%v)", resolution, err)
	}
	if _, err := st.CommitChannelEnrollment(context.Background(), plan); !errors.Is(err, ErrChannelAuthorityPlanDiverged) {
		t.Fatalf("CommitChannelEnrollment(drift) error = %v", err)
	}
}
