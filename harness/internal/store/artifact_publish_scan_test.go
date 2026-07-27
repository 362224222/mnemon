package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestScanAcceptedArtifactPublishesBoundsCommittedOperations(t *testing.T) {
	fixture := newAcceptanceFixture(t, 1)
	operationIDs := seedAcceptedOperationPublishes(t, fixture)
	at := fixture.now.Add(3 * time.Second)
	assertAcceptedOperationPublishPagination(t, fixture.store, operationIDs, at)
	assertFullAcceptedOperationPublishScan(t, fixture.store, operationIDs, at)
	assertInvalidAcceptedArtifactPublishScans(t, fixture.store, at)
}

func seedAcceptedOperationPublishes(t *testing.T,
	fixture *acceptanceFixture,
) []model.OperationID {
	t.Helper()
	first := seedAcceptedOperationPublish(t, fixture, 0, true)
	fixture.now = fixture.now.Add(2 * time.Second)
	second := seedAcceptedOperationPublish(t, fixture, 1, false)
	return []model.OperationID{first, second}
}

func seedAcceptedOperationPublish(t *testing.T, fixture *acceptanceFixture,
	index int, assertUncommitted bool,
) model.OperationID {
	t.Helper()
	suffix := fmt.Sprintf("accepted-publish-scan-%d", index)
	operation, authority := fixture.reserveOffer(t, suffix, nil)
	closure, root := peerInboxArtifactEmptyTreeClosure(t, suffix, 0,
		fixture.now.Add(-time.Minute))
	capture := operationCaptureJSON(t, []captureRoot{{
		RootDigest: root.RootDigest, ManifestDigest: root.ManifestDigest,
	}})
	leaseUntil, _ := operation.LeaseUntil()
	stageAt := fixture.now.Add(time.Duration(-20+index) * time.Second)
	begun, err := fixture.store.BeginOperationArtifactStage(context.Background(),
		BeginOperationArtifactStageSpec{OperationID: operation.ID(),
			LeaseOwner: operation.LeaseOwner(), LeaseUntil: leaseUntil,
			At: stageAt.Add(-time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.PrepareOperationArtifactPublish(context.Background(),
		PrepareOperationArtifactPublishSpec{Fence: begun.Fence(), Capture: capture,
			Closure: closure, At: stageAt}); err != nil {
		t.Fatal(err)
	}
	if assertUncommitted {
		assertNoAcceptedArtifactPublishes(t, fixture.store, fixture.now)
	}
	ref, _ := model.NewArtifactRef(root.RootDigest, model.ArtifactProduced)
	acceptance := fixture.offer(t, authority, suffix, fixture.reviewers,
		[]model.ArtifactRef{ref}, nil)
	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), acceptance,
		fixture.now.Add(time.Duration(index+1)*time.Second)); err != nil {
		t.Fatal(err)
	}
	return operation.ID()
}

func assertNoAcceptedArtifactPublishes(t *testing.T, st *Store, at time.Time) {
	t.Helper()
	page, err := st.ScanAcceptedArtifactPublishes(context.Background(),
		ScanAcceptedArtifactPublishesSpec{At: at, MaxExamined: 1})
	if err != nil || page.Examined() != 0 || len(page.Candidates()) != 0 {
		t.Fatalf("uncommitted operation publish scan = (%#v,%v)", page, err)
	}
}

func assertAcceptedOperationPublishPagination(t *testing.T, st *Store,
	operationIDs []model.OperationID, at time.Time,
) {
	t.Helper()
	first := assertAcceptedOperationPublishPage(t, st,
		ScanAcceptedArtifactPublishesSpec{At: at, MaxExamined: 1}, operationIDs[0])
	second := assertAcceptedOperationPublishPage(t, st,
		ScanAcceptedArtifactPublishesSpec{
			At: at, MaxExamined: 1, After: first.Cursor(),
		}, operationIDs[1])
	end, err := st.ScanAcceptedArtifactPublishes(context.Background(),
		ScanAcceptedArtifactPublishesSpec{
			At: at, MaxExamined: 1, After: second.Cursor(),
		})
	if err != nil || end.Examined() != 0 || len(end.Candidates()) != 0 {
		t.Fatalf("accepted publish end page = (%#v,%v)", end, err)
	}
	assertAcceptedOperationPublishPage(t, st,
		ScanAcceptedArtifactPublishesSpec{At: at, MaxExamined: 1}, operationIDs[0])
}

func assertAcceptedOperationPublishPage(t *testing.T, st *Store,
	spec ScanAcceptedArtifactPublishesSpec, operationID model.OperationID,
) AcceptedArtifactPublishPage {
	t.Helper()
	page, err := st.ScanAcceptedArtifactPublishes(context.Background(), spec)
	if err != nil || page.Examined() != 1 || len(page.Candidates()) != 1 {
		t.Fatalf("accepted operation publish page = (%#v,%v)", page, err)
	}
	candidate := page.Candidates()[0]
	if candidate.Kind() != artifactdomain.StageOwnerOperation ||
		candidate.OperationID() != operationID ||
		!candidate.PeerInboxFence().InboxID().IsZero() || !candidate.Owner().IsZero() {
		t.Fatalf("operation publish candidate = %#v", candidate)
	}
	return page
}

func assertFullAcceptedOperationPublishScan(t *testing.T, st *Store,
	operationIDs []model.OperationID, at time.Time,
) {
	t.Helper()
	full, err := st.ScanAcceptedArtifactPublishes(context.Background(),
		ScanAcceptedArtifactPublishesSpec{
			At: at, MaxExamined: maxAcceptedArtifactPublishExamined,
		})
	if err != nil || full.Examined() != 2 || len(full.Candidates()) != 2 {
		t.Fatalf("full accepted publish scan = (%#v,%v)", full, err)
	}
	for index, candidate := range full.Candidates() {
		if candidate.Kind() != artifactdomain.StageOwnerOperation ||
			candidate.OperationID() != operationIDs[index] {
			t.Fatalf("operation candidate[%d] = %#v", index, candidate)
		}
		checkpoint, found, err := st.ReadCommittedOperationArtifactPublish(
			context.Background(), ReadCommittedOperationArtifactPublishSpec{
				OperationID: candidate.OperationID(), At: at,
			})
		if err != nil || !found || checkpoint.State() != ArtifactStagePublishing {
			t.Fatalf("operation candidate[%d] exact read = (%#v,%t,%v)",
				index, checkpoint, found, err)
		}
	}
}

func assertInvalidAcceptedArtifactPublishScans(t *testing.T, st *Store, at time.Time) {
	t.Helper()
	for _, test := range []struct {
		name string
		spec ScanAcceptedArtifactPublishesSpec
	}{
		{name: "zero", spec: ScanAcceptedArtifactPublishesSpec{At: at}},
		{name: "above maximum", spec: ScanAcceptedArtifactPublishesSpec{
			At: at, MaxExamined: maxAcceptedArtifactPublishExamined + 1,
		}},
		{name: "noncanonical time", spec: ScanAcceptedArtifactPublishesSpec{
			At:          at.In(time.FixedZone("scan-offset", 60*60)),
			MaxExamined: 1,
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := st.ScanAcceptedArtifactPublishes(
				context.Background(), test.spec); !errors.Is(err, ErrArtifactStageConflict) {
				t.Fatalf("scan error = %v, want Artifact stage conflict", err)
			}
		})
	}
}

func TestScanAcceptedArtifactPublishesSkipsUnacceptedPeerInbox(t *testing.T) {
	fixture, claim, _, closure := newPeerInboxArtifactClosureClaim(t,
		"accepted-publish-scan-peer", false)
	stageAt := fixture.at.Add(2 * time.Second)
	begun, err := fixture.store.BeginPeerInboxArtifactStage(context.Background(),
		BeginPeerInboxArtifactStageSpec{Fence: claim.Fence(), At: stageAt})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.PreparePeerInboxArtifactPublish(context.Background(),
		PreparePeerInboxArtifactPublishSpec{Fence: claim.Fence(), Owner: begun.Owner(),
			Closure: closure, At: stageAt}); err != nil {
		t.Fatal(err)
	}

	before, err := fixture.store.ScanAcceptedArtifactPublishes(context.Background(),
		ScanAcceptedArtifactPublishesSpec{At: stageAt, MaxExamined: 1})
	if err != nil || before.Examined() != 0 || len(before.Candidates()) != 0 {
		t.Fatalf("unaccepted peer publish scan = (%#v,%v)", before, err)
	}

	acceptedAt := stageAt.Add(time.Second)
	if _, err := fixture.store.AcceptPeerInboxArtifactPublish(context.Background(),
		AcceptPeerInboxArtifactPublishSpec{Fence: claim.Fence(), Owner: begun.Owner(),
			At: acceptedAt}); err != nil {
		t.Fatal(err)
	}
	scanAt := claim.Fence().LeaseUntil().Add(time.Second)
	accepted, err := fixture.store.ScanAcceptedArtifactPublishes(context.Background(),
		ScanAcceptedArtifactPublishesSpec{At: scanAt, MaxExamined: 1})
	if err != nil || accepted.Examined() != 1 || len(accepted.Candidates()) != 1 {
		t.Fatalf("accepted peer publish scan = (%#v,%v)", accepted, err)
	}
	candidate := accepted.Candidates()[0]
	fence := candidate.PeerInboxFence()
	if candidate.Kind() != artifactdomain.StageOwnerInbox ||
		!candidate.OperationID().IsZero() || candidate.Owner() != begun.Owner() ||
		fence.InboxID() != claim.InboxID() ||
		fence.LeaseOwner() != claim.Fence().LeaseOwner() ||
		!fence.LeaseUntil().Equal(claim.Fence().LeaseUntil()) ||
		fence.Attempt() != claim.Fence().Attempt() {
		t.Fatalf("accepted peer candidate = %#v", candidate)
	}
	checkpoint, err := fixture.store.ReadPeerInboxArtifactPublish(context.Background(),
		ReadPeerInboxArtifactPublishSpec{
			Fence: fence, Owner: candidate.Owner(), At: scanAt,
		})
	if err != nil || checkpoint.State() != ArtifactStagePublishing {
		t.Fatalf("accepted peer exact read = (%#v,%v)", checkpoint, err)
	}
}
