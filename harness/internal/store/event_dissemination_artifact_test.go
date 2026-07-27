package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestEventDisseminationRequiresItsOwnLocalArtifactStageReady(t *testing.T) {
	fixture := newAcceptanceFixture(t, 1)
	cas, err := artifactdomain.NewCAS(filepath.Join(t.TempDir(), "objects", "sha256"))
	if err != nil {
		t.Fatal(err)
	}
	closure, root := peerInboxArtifactEmptyTreeClosure(t, "shared-final-root", 0,
		fixture.now.Add(-time.Minute))

	first := commitSharedRootLocalArtifact(t, fixture, cas, closure, root,
		"shared-final-first", true)
	publishSharedRootLocalArtifact(t, fixture, first, fixture.now.Add(2*time.Second))
	firstClaim := claimPublication(t, fixture.store, fixture.channel,
		"shared-final-first-gossip", fixture.now.Add(3*time.Second),
		fixture.now.Add(time.Minute))
	if firstClaim.Lease.Record.Publication().Event().ID() != first.event {
		t.Fatalf("first Gossip Event = %s, want %s",
			firstClaim.Lease.Record.Publication().Event().ID(), first.event)
	}
	if _, err := fixture.store.MarkGossipPublicationPublished(context.Background(),
		MarkGossipPublicationPublishedSpec{
			Fence: firstClaim.Lease.Fence, At: fixture.now.Add(4 * time.Second),
		}); err != nil {
		t.Fatal(err)
	}

	fixture.now = fixture.now.Add(time.Minute)
	second := commitSharedRootLocalArtifact(t, fixture, cas, closure, root,
		"shared-final-second", false)
	checkpoint, found, err := fixture.store.ReadCommittedOperationArtifactPublish(
		context.Background(), ReadCommittedOperationArtifactPublishSpec{
			OperationID: second.operation, At: fixture.now,
		})
	if err != nil || !found || checkpoint.State() != ArtifactStagePublishing {
		t.Fatalf("second operation checkpoint = (%#v,%t,%v)", checkpoint, found, err)
	}
	blockedAt := fixture.now.Add(2 * time.Second)
	blocked, err := fixture.store.ClaimGossipPublication(context.Background(),
		GossipPublicationClaimSpec{
			ChannelID: fixture.channel, LeaseOwner: "shared-final-second-blocked",
			At: blockedAt, LeaseUntil: blockedAt.Add(time.Minute),
		})
	if err != nil || blocked.Claimed {
		t.Fatalf("shared-root Gossip before own Ready = (%#v,%v)", blocked, err)
	}
	_, err = fixture.store.ReadPeerPullPage(context.Background(), ReadPeerPullPageSpec{
		AuthenticatedPeerID: fixture.reviewers[0], ChannelID: fixture.channel,
		OriginEpoch: fixture.node.OriginEpoch(), AfterChannelSequence: 1,
		Limit: 1, At: blockedAt,
	})
	if !errors.Is(err, ErrPeerPullPublicationPending) {
		t.Fatalf("shared-root Pull before own Ready error = %v", err)
	}
	var acknowledged, pending int
	if err := fixture.store.db.QueryRow(`SELECT acknowledged_channel_seq
		FROM peer_pull_acks WHERE channel_id=? AND target_peer_id=? AND origin_peer_id=?`,
		fixture.channel.String(), fixture.reviewers[0].String(),
		fixture.node.PeerID().String()).Scan(&acknowledged); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM peer_deliveries
		WHERE status='pending'`).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if acknowledged != 0 || pending != 2 {
		t.Fatalf("blocked shared-root Pull mutated ACK/deliveries = %d/%d",
			acknowledged, pending)
	}

	readyAt := fixture.now.Add(3 * time.Second)
	publishSharedRootLocalArtifact(t, fixture, second, readyAt)
	secondClaim := claimPublication(t, fixture.store, fixture.channel,
		"shared-final-second-ready", readyAt.Add(time.Second),
		readyAt.Add(time.Minute))
	if secondClaim.Lease.Record.Publication().Event().ID() != second.event {
		t.Fatalf("second Gossip Event = %s, want %s",
			secondClaim.Lease.Record.Publication().Event().ID(), second.event)
	}
	page, err := fixture.store.ReadPeerPullPage(context.Background(), ReadPeerPullPageSpec{
		AuthenticatedPeerID: fixture.reviewers[0], ChannelID: fixture.channel,
		OriginEpoch: fixture.node.OriginEpoch(), AfterChannelSequence: 1,
		Limit: 1, At: readyAt.Add(2 * time.Second),
	})
	if err != nil || len(page.Publications) != 1 ||
		page.Publications[0].Event().ID() != second.event ||
		!page.AcknowledgementAdvanced || page.AcknowledgedSequence != 1 {
		t.Fatalf("shared-root Pull after own Ready = (%#v,%v)", page, err)
	}
}

type sharedRootLocalArtifact struct {
	operation model.OperationID
	event     model.EventID
	fence     OperationArtifactStageFence
	stage     *artifactdomain.Stage
	closure   artifactdomain.Closure
}

func commitSharedRootLocalArtifact(t *testing.T, fixture *acceptanceFixture,
	cas *artifactdomain.CAS, closure VerifiedArtifactClosure,
	root VerifiedArtifactRoot, suffix string, putManifest bool,
) sharedRootLocalArtifact {
	t.Helper()
	operation, authority := fixture.reserveOffer(t, suffix, nil)
	leaseUntil, _ := operation.LeaseUntil()
	begun, err := fixture.store.BeginOperationArtifactStage(context.Background(),
		BeginOperationArtifactStageSpec{
			OperationID: operation.ID(), LeaseOwner: operation.LeaseOwner(),
			LeaseUntil: leaseUntil, At: fixture.now.Add(-20 * time.Second),
		})
	if err != nil {
		t.Fatal(err)
	}
	stage, err := cas.OpenStage(begun.Fence().Owner())
	if err != nil {
		t.Fatal(err)
	}
	if putManifest {
		if _, err := stage.Put(root.ManifestDigest, root.Manifest.Bytes()); err != nil {
			t.Fatal(err)
		}
	}
	capture := operationCaptureJSON(t, []captureRoot{{
		RootDigest: root.RootDigest, ManifestDigest: root.ManifestDigest,
	}})
	if _, err := fixture.store.PrepareOperationArtifactPublish(context.Background(),
		PrepareOperationArtifactPublishSpec{
			Fence: begun.Fence(), Capture: capture, Closure: closure,
			At: fixture.now.Add(-10 * time.Second),
		}); err != nil {
		t.Fatal(err)
	}
	ref, _ := model.NewArtifactRef(root.RootDigest, model.ArtifactProduced)
	acceptance := fixture.offer(t, authority, suffix, fixture.reviewers,
		[]model.ArtifactRef{ref}, nil)
	acceptedAt := fixture.now.Add(time.Second)
	if _, err := fixture.store.CommitLocalAcceptance(context.Background(),
		acceptance, acceptedAt); err != nil {
		t.Fatal(err)
	}
	rebuilt, err := RebuildArtifactClosure(context.Background(), closure, acceptedAt)
	if err != nil {
		t.Fatal(err)
	}
	return sharedRootLocalArtifact{
		operation: operation.ID(), event: acceptance.Items[0].Publication.Event().ID(),
		fence: begun.Fence(), stage: stage, closure: rebuilt,
	}
}

func publishSharedRootLocalArtifact(t *testing.T, fixture *acceptanceFixture,
	published sharedRootLocalArtifact, at time.Time,
) {
	t.Helper()
	if err := published.stage.Publish(context.Background(), published.closure); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.MarkOperationArtifactReady(context.Background(),
		MarkOperationArtifactReadySpec{Fence: published.fence, At: at}); err != nil {
		t.Fatal(err)
	}
}
