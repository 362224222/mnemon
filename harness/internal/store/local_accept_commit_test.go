package store

import (
	"context"
	"errors"
	"testing"
	"time"

	eventpkg "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestCommitLocalAcceptanceRollsBackStalePublicationSequence(t *testing.T) {
	fixture := newAcceptanceFixture(t, 1)
	operation, authority := fixture.reserveOffer(t, "stale-sequence", nil)
	spec := fixture.offer(t, authority, "stale-sequence", fixture.reviewers, nil, nil)
	mustExec(t, fixture.store, `UPDATE publication_epochs SET source_head_channel_seq=1
		WHERE channel_id=? AND origin_peer_id=? AND origin_epoch=?`, fixture.channel.String(),
		fixture.node.PeerID().String(), fixture.node.OriginEpoch().String())

	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), spec,
		fixture.now.Add(time.Second)); !errors.Is(err, ErrAdmissionConflict) {
		t.Fatalf("stale sequence error = %v", err)
	}
	assertAcceptanceCounts(t, fixture.store, []int{0, 0, 0, 0, 0, 0, 0, 0})
	assertAcceptanceHeads(t, fixture.store, 1, 1)
	assertOperationStatus(t, fixture.store, operation.ID(), model.OperationStarted)
}

func TestCommitLocalAcceptanceRollsBackStaleWorkCAS(t *testing.T) {
	fixture := newAcceptanceFixture(t, 1)
	_, authority := fixture.reserveOffer(t, "cas-offer", nil)
	offer := fixture.offer(t, authority, "cas-offer", fixture.reviewers, nil, nil)
	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), offer,
		fixture.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	current, err := fixture.store.GetReviewWork(context.Background(), offer.Items[0].Work.Work.Ref())
	if err != nil {
		t.Fatal(err)
	}
	acceptedAt := fixture.now.Add(2 * time.Second)
	scope, publication := controllerAcceptance(t, fixture, current,
		offer.Items[0].Publication.Event(), acceptedAt)
	nextSpec := current.Spec()
	nextSpec.Version = 2
	nextSpec.State = model.WorkActive
	nextSpec.StateData = publication.Event().Payload()
	nextSpec.UpdatedBy = publication.Event().ID()
	nextSpec.UpdatedAt = acceptedAt
	next, err := model.NewReviewWork(nextSpec)
	if err != nil {
		t.Fatal(err)
	}
	// The immutable successor is valid, but the claimed predecessor is stale.
	mutation := WorkMutation{Work: next, ExpectedVersion: 2, ExpectedState: model.WorkOffered}
	spec := LocalAcceptanceSpec{Scope: scope, Controller: true,
		Items: []LocalAcceptanceItem{{Publication: publication, Work: &mutation}}}
	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), spec,
		acceptedAt.Add(time.Second)); !errors.Is(err, ErrWorkCASConflict) {
		t.Fatalf("stale Work CAS error = %v", err)
	}

	assertAcceptanceCounts(t, fixture.store, []int{2, 1, 2, 0, 1, 1, 0, 0})
	assertAcceptanceHeads(t, fixture.store, 2, 1)
	durable, err := fixture.store.GetReviewWork(context.Background(), current.Ref())
	if err != nil || durable.Version() != 1 || durable.State() != model.WorkOffered {
		t.Fatalf("durable Work after rollback = (%#v, %v)", durable, err)
	}
}

func TestCommitLocalAcceptanceDoesNotSubstituteUnrelatedActiveRun(t *testing.T) {
	fixture := newAcceptanceFixture(t, 1)
	operation, authority := fixture.reserveOffer(t, "durable-run", nil)
	spec := fixture.offer(t, authority, "durable-run", fixture.reviewers, nil, nil)
	unrelated, _ := model.ParseRunID("run-unrelated-active")
	mustExec(t, fixture.store, `INSERT INTO agent_runs(run_id,profile_id,cause_json,launcher,runtime_kind,
		launcher_diagnostic_json,runtime_ids_json,status,started_at)
		VALUES(?,?,'{}','test',?,'{}','{}','running',?)`, unrelated.String(),
		model.TeamworkProfileID().String(), string(fixture.profile.Runtime()), storeTime(fixture.now.Add(-time.Minute)))
	mustExec(t, fixture.store, "UPDATE agent_runs SET status='failed',finished_at=? WHERE run_id=?",
		storeTime(fixture.now), operation.AgentRunID().String())

	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), spec,
		fixture.now.Add(time.Second)); !errors.Is(err, ErrAdmissionConflict) {
		t.Fatalf("unrelated Run substitution error = %v", err)
	}
	assertAcceptanceCounts(t, fixture.store, []int{0, 0, 0, 0, 0, 0, 0, 0})
	assertOperationStatus(t, fixture.store, operation.ID(), model.OperationStarted)
}

func TestCommitLocalAcceptanceRejectsInvalidEvidenceWithoutWrites(t *testing.T) {
	t.Run("missing outbound baseline", func(t *testing.T) {
		fixture := newAcceptanceFixture(t, 1)
		operation, authority := fixture.reserveOffer(t, "missing-baseline", nil)
		spec := fixture.offer(t, authority, "missing-baseline", fixture.reviewers, nil, nil)
		mustExec(t, fixture.store, "DROP TRIGGER peer_pull_acks_no_delete")
		mustExec(t, fixture.store, "DELETE FROM peer_pull_acks WHERE channel_id=?", fixture.channel.String())
		if _, err := fixture.store.CommitLocalAcceptance(context.Background(), spec,
			fixture.now.Add(time.Second)); !errors.Is(err, ErrAudienceUnavailable) {
			t.Fatalf("missing baseline error = %v", err)
		}
		assertAcceptanceCounts(t, fixture.store, []int{0, 0, 0, 0, 0, 0, 0, 0})
		assertOperationStatus(t, fixture.store, operation.ID(), model.OperationStarted)
	})

	t.Run("capture closure mismatch", func(t *testing.T) {
		fixture := newAcceptanceFixture(t, 1)
		operation, authority := fixture.reserveOffer(t, "capture-mismatch", nil)
		root := verifiedRoot(t, "capture-mismatch", `{"entries":[],"kind":"review","total_bytes":0}`, 0)
		if _, err := fixture.store.CheckpointVerifiedArtifactRoot(context.Background(), root); err != nil {
			t.Fatal(err)
		}
		checkpointOperationRoot(t, fixture, operation, authority.LeaseOwner, root)
		spec := fixture.offer(t, authority, "capture-mismatch", fixture.reviewers, nil, nil)
		if _, err := fixture.store.CommitLocalAcceptance(context.Background(), spec,
			fixture.now.Add(time.Second)); !errors.Is(err, ErrCaptureMismatch) {
			t.Fatalf("capture mismatch error = %v", err)
		}
		assertAcceptanceCounts(t, fixture.store, []int{0, 0, 0, 0, 0, 0, 0, 0})
		assertOperationStatus(t, fixture.store, operation.ID(), model.OperationStarted)
	})

	t.Run("forged publication signature", func(t *testing.T) {
		fixture := newAcceptanceFixture(t, 1)
		operation, authority := fixture.reserveOffer(t, "forged-signature", nil)
		spec := fixture.offer(t, authority, "forged-signature", fixture.reviewers, nil, nil)
		body := spec.Items[0].Publication.Body()
		spec.Items[0].Publication, _ = model.AttachSignature(body, make([]byte, 64))
		if _, err := fixture.store.CommitLocalAcceptance(context.Background(), spec,
			fixture.now.Add(time.Second)); !errors.Is(err, ErrPublicationInvalid) {
			t.Fatalf("forged signature error = %v", err)
		}
		assertAcceptanceCounts(t, fixture.store, []int{0, 0, 0, 0, 0, 0, 0, 0})
		assertOperationStatus(t, fixture.store, operation.ID(), model.OperationStarted)
	})

	t.Run("participant audience drift", func(t *testing.T) {
		fixture := newAcceptanceFixture(t, 2)
		operation, authority := fixture.reserveOffer(t, "audience-drift", nil)
		spec := fixture.offer(t, authority, "audience-drift", fixture.reviewers[:1], nil, nil)
		original := spec.Items[0].Work.Work
		participants, _ := model.NewParticipantSnapshot(fixture.channel,
			original.Participants().RosterRevision(), fixture.node.PeerID(), fixture.reviewers[1])
		workSpec := original.Spec()
		workSpec.Participants = participants
		drifted, err := model.NewReviewWork(workSpec)
		if err != nil {
			t.Fatal(err)
		}
		mutation, _ := NewWorkCreation(drifted)
		spec.Items[0].Work = &mutation
		if _, err := fixture.store.CommitLocalAcceptance(context.Background(), spec,
			fixture.now.Add(time.Second)); err == nil {
			t.Fatal("participant/audience drift was accepted")
		}
		assertAcceptanceCounts(t, fixture.store, []int{0, 0, 0, 0, 0, 0, 0, 0})
		assertOperationStatus(t, fixture.store, operation.ID(), model.OperationStarted)
	})
}

func controllerAcceptance(t *testing.T, fixture *acceptanceFixture, current model.ReviewWork,
	offered model.Event, acceptedAt time.Time,
) (LocalAdmissionScope, model.SignedPublication) {
	t.Helper()
	audience, _ := model.NewAudience([]model.PeerID{current.Participants().ReviewerPeerID()})
	scope, err := fixture.store.PrepareLocalAdmission(context.Background(), fixture.channel, audience, 1)
	if err != nil {
		t.Fatal(err)
	}
	eventScope, _ := scope.EventScope(0, current.Ref())
	cause := insertImportedCausalEvent(t, fixture, offered, model.EventReviewAcceptRequested,
		"event-controller-accept-request", `{"iteration":1,"note":"ready","work_version":1}`,
		acceptedAt.Add(-time.Second))
	eventID, _ := model.ParseEventID("event-controller-accepted")
	stamp, err := eventpkg.NewAdmissionStamp(eventpkg.AdmissionStampSpec{
		Node: scope.Node(), Profile: scope.Profile(), EventID: eventID, ChannelID: fixture.channel,
		WorkRef: current.Ref(), OriginSequence: eventScope.OriginSequence(),
		ChannelSequence: eventScope.ChannelSequence(), OriginMember: eventScope.OriginMember(),
		PublicationRoster: eventScope.PublicationRoster(), Audience: audience,
		WorkVersion: current.Version(), Iteration: current.Iteration(),
		WorkDeadlineUnixNano: current.DeadlineUnixNano(), CausedBy: []model.EventKey{cause},
	})
	if err != nil {
		t.Fatal(err)
	}
	signer, _ := eventpkg.NewEd25519Signer(fixture.privateKey)
	factory, _ := eventpkg.NewFactory(acceptanceClock{acceptedAt}, signer)
	bundle, err := factory.AdmitController(context.Background(), stamp, eventpkg.AcceptedDecision{})
	if err != nil {
		t.Fatal(err)
	}
	return scope, bundle.Publication()
}

func assertOperationStatus(t *testing.T, store *Store, id model.OperationID, want model.OperationStatus) {
	t.Helper()
	var got string
	if err := store.db.QueryRow("SELECT status FROM operations WHERE operation_id=?", id.String()).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Fatalf("operation status = %q, want %q", got, want)
	}
}
