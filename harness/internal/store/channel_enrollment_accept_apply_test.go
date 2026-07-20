package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestPrepareSignedChannelEnrollmentFailsClosedWithoutMutation(t *testing.T) {
	t.Parallel()
	t.Run("invalid external signature", func(t *testing.T) {
		fixture := newOwnerAcceptancePlanFixture(t, "accept-invalid-signed-plan")
		signing := fixture.signingPlan(t)
		signatures := fixture.signatures(t, signing)
		signatures.MemberSignature[0] ^= 0xff
		if _, err := fixture.owner.ownerStore.PrepareSignedChannelEnrollment(context.Background(),
			signing, signatures); !errors.Is(err, ErrChannelEnrollmentOwner) {
			t.Fatalf("invalid signature error = %v", err)
		}
		assertEnrollmentTableCounts(t, fixture.owner.ownerStore, map[string]int{
			"channel_members": 1, "enrollment_grant_uses": 0, "enrollment_receipts": 0,
			"peer_bindings": 0,
		})
		if _, err := fixture.owner.ownerStore.PrepareSignedChannelEnrollment(context.Background(),
			signing, fixture.signatures(t, signing)); err != nil {
			t.Fatalf("valid retry after rollback = %v", err)
		}
	})

	t.Run("stale grant fence", func(t *testing.T) {
		fixture := newOwnerAcceptancePlanFixture(t, "accept-stale-grant-signing")
		signing := fixture.signingPlan(t)
		if _, err := fixture.owner.ownerStore.CloseChannelInvite(context.Background(),
			fixture.owner.channel.Channel().ID(), fixture.owner.grantID,
			fixture.owner.acceptedAt.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.owner.ownerStore.PrepareSignedChannelEnrollment(context.Background(),
			signing, fixture.signatures(t, signing)); !errors.Is(err, ErrChannelAuthorityPlanDiverged) {
			t.Fatalf("stale signing plan error = %v", err)
		}
		assertEnrollmentTableCounts(t, fixture.owner.ownerStore, map[string]int{
			"channel_members": 1, "enrollment_grant_uses": 0, "enrollment_receipts": 0,
			"peer_bindings": 0,
		})
	})
}

func TestChannelEnrollmentPlanRejectsConcurrentAuthorityProgress(t *testing.T) {
	t.Parallel()
	fixture := newOwnerAcceptancePlanFixture(t, "accept-concurrent-progress")
	stale := fixture.finalPlan(t)
	second := testkit.NewIdentity(t, "accept-concurrent-progress-second")
	acceptedAt := fixture.owner.acceptedAt.Add(time.Second)
	requestID := stableEnrollmentRequest(t, fixture.owner.channel.Channel().ID(),
		fixture.owner.grantID, second)
	prepared, err := fixture.owner.ownerStore.PrepareChannelEnrollment(context.Background(),
		PrepareChannelEnrollmentSpec{ChannelID: fixture.owner.channel.Channel().ID(),
			GrantID: fixture.owner.grantID, RequestID: requestID,
			AuthenticatedPeerID: second.PeerID(), JoinerOriginEpoch: second.OriginEpoch(),
			JoinerPublicKey: second.PublicKey(), At: acceptedAt})
	if err != nil {
		t.Fatal(err)
	}
	transcript := enrollmentTestTranscript(t, fixture.owner.channel.Descriptor(),
		fixture.owner.grantID, requestID, second, prepared.RosterHead, 0x61, 0x62)
	result, err := fixture.owner.ownerStore.AcceptChannelEnrollment(context.Background(),
		AcceptChannelEnrollmentSpec{AuthenticatedPeerID: second.PeerID(), Transcript: transcript,
			AdvertisedMultiaddrs: second.Multiaddrs(),
			Proof:                enrollmentTestProof(t, fixture.owner.token, transcript),
			Signer:               fixture.owner.signer, At: acceptedAt})
	if err != nil || result.Status != ChannelEnrollmentAccepted {
		t.Fatalf("concurrent enrollment = (%#v,%v)", result, err)
	}
	if resolution, err := fixture.owner.ownerStore.ResolveChannelEnrollment(context.Background(),
		stale); err != nil || resolution != ChannelAuthorityPlanDiverged {
		t.Fatalf("ResolveChannelEnrollment(stale) = (%q,%v)", resolution, err)
	}
	if _, err := fixture.owner.ownerStore.CommitChannelEnrollment(context.Background(), stale); !errors.Is(err, ErrChannelAuthorityPlanDiverged) {
		t.Fatalf("CommitChannelEnrollment(stale) error = %v", err)
	}
	assertEnrollmentTableCounts(t, fixture.owner.ownerStore, map[string]int{
		"channel_members": 2, "enrollment_grant_uses": 1, "enrollment_receipts": 1,
		"peer_bindings": 1,
	})
	if current, ok := result.Roster.CurrentMember(fixture.owner.joiner.PeerID()); ok {
		t.Fatalf("stale candidate leaked into roster: %#v", current)
	}
}
