package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	eventpkg "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestReconcileWorkDerivationDispositionWaitsForEveryChildThenResumes(t *testing.T) {
	t.Parallel()
	fixture := newDerivationDispositionFixture(t, false)
	ctx := context.Background()
	if err := fixture.store.ReconcileWorkDerivationDisposition(ctx, fixture.children[0]); err != nil {
		t.Fatal(err)
	}
	assertDispositionHandlingCount(t, fixture.store, 0)

	lastAt := fixture.now.Add(12 * time.Second)
	fixture.terminalizeChild(t, 1, "event-child-b-closed", 42, lastAt)
	if err := fixture.store.ReconcileWorkDerivationDisposition(ctx, fixture.children[0]); err != nil {
		t.Fatal(err)
	}
	handling := fixture.handling(t)
	lastEvent, _ := model.ParseEventID("event-child-b-closed")
	if handling.EventID() != lastEvent || handling.Status() != model.HandlingPending ||
		handling.LastDisposition() != "" || handling.Attempts() != 0 ||
		!handling.AvailableAt().Equal(lastAt) || !handling.CreatedAt().Equal(lastAt) {
		t.Fatalf("resume Handling = %#v", handling)
	}
	assertDispositionHandlingCount(t, fixture.store, 1)
	assertDispositionHandlingPins(t, fixture, 1)
}

func TestReconcileWorkDerivationDispositionRecordsParentStaleWithoutWake(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		eventType model.EventType
		state     model.WorkState
		version   uint64
	}{
		{name: "version changed", eventType: model.EventReviewDelivered, state: model.WorkDelivered, version: 3},
		{name: "state changed at frozen version", eventType: model.EventReviewCancelled, state: model.WorkCancelled, version: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newDerivationDispositionFixture(t, true)
			fixture.advanceParent(t, test.eventType, test.state, test.version)
			if err := fixture.store.ReconcileWorkDerivationDisposition(context.Background(), fixture.children[1]); err != nil {
				t.Fatal(err)
			}
			handling := fixture.handling(t)
			if handling.Status() != model.HandlingCompleted || handling.LastDisposition() != parentStaleDisposition ||
				handling.Attempts() != 0 {
				t.Fatalf("parent_stale Handling = %#v", handling)
			}
			var ready int
			if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM agent_handlings
				WHERE status='pending' AND available_at<=?`, storeTime(fixture.now.Add(time.Hour))).Scan(&ready); err != nil {
				t.Fatal(err)
			}
			if ready != 0 {
				t.Fatalf("parent_stale created %d wakeable Handlings", ready)
			}
			assertDispositionHandlingPins(t, fixture, 0)
		})
	}
}

func TestReconcileWorkDerivationDispositionIsAtomicConcurrentAndRestartSafe(t *testing.T) {
	fixture := newDerivationDispositionFixture(t, true)
	ctx := context.Background()

	// The transaction helper never publishes partial evidence when its caller
	// rolls back the child transition transaction.
	tx, err := fixture.store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := reconcileWorkDerivationDisposition(ctx, tx, fixture.children[0]); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	assertDispositionHandlingCount(t, fixture.store, 0)

	const callers = 8
	errorsByCaller := make(chan error, callers)
	var wait sync.WaitGroup
	for index := 0; index < callers; index++ {
		wait.Add(1)
		go func(child model.WorkRef) {
			defer wait.Done()
			errorsByCaller <- fixture.store.ReconcileWorkDerivationDisposition(ctx, child)
		}(fixture.children[index%len(fixture.children)])
	}
	wait.Wait()
	close(errorsByCaller)
	for err := range errorsByCaller {
		if err != nil {
			t.Fatalf("concurrent reconciliation: %v", err)
		}
	}
	assertDispositionHandlingCount(t, fixture.store, 1)
	assertDispositionHandlingPins(t, fixture, 1)
	before := fixture.handling(t)

	// A later parent transition cannot flip an already chosen resume into the
	// mutually exclusive stale disposition.
	fixture.advanceParent(t, model.EventReviewDelivered, model.WorkDelivered, 3)
	if err := fixture.store.ReconcileWorkDerivationDisposition(ctx, fixture.children[0]); err != nil {
		t.Fatal(err)
	}
	if after := fixture.handling(t); !sameHandling(before, after) {
		t.Fatalf("replay changed prior disposition: before=%#v after=%#v", before, after)
	}

	path := fixture.store.Path()
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.store = nil
	restarted, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	fixture.store = restarted
	if err := restarted.ReconcileWorkDerivationDisposition(ctx, fixture.children[1]); err != nil {
		t.Fatal(err)
	}
	if after := fixture.handling(t); !sameHandling(before, after) {
		t.Fatalf("restart replay changed Handling: before=%#v after=%#v", before, after)
	}
	assertDispositionHandlingCount(t, restarted, 1)
	assertDispositionHandlingPins(t, fixture, 1)
}

func TestReconcileWorkDerivationDispositionRejectsInvalidClosedResultChain(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		replace func(*testing.T, *derivationDispositionFixture)
	}{
		{name: "terminal Event carries Artifact", replace: func(t *testing.T, fixture *derivationDispositionFixture) {
			delivered := fixture.eventKey(t, fixture.node.PeerID(), "event-child-a-initial-delivered")
			fixture.replaceClosedChildEvent(t, 0, "event-invalid-close-artifact", 90,
				fixture.artifacts[0], []model.EventKey{delivered})
		}},
		{name: "CLOSED has no delivered cause", replace: func(t *testing.T, fixture *derivationDispositionFixture) {
			fixture.replaceClosedChildEvent(t, 0, "event-invalid-close-cause", 90, nil, nil)
		}},
		{name: "delivered Artifact is produced", replace: func(t *testing.T, fixture *derivationDispositionFixture) {
			produced, _ := model.NewArtifactRef(fixture.artifacts[0][0].RootDigest(), model.ArtifactProduced)
			fixture.insertEvent(t, "event-invalid-delivered-produced", fixture.children[0], fixture.node.PeerID(),
				fixture.reviewers[1], model.EventReviewDelivered, 89, fixture.now.Add(49*time.Second),
				[]model.ArtifactRef{produced}, nil)
			cause := fixture.eventKey(t, fixture.node.PeerID(), "event-invalid-delivered-produced")
			fixture.replaceClosedChildEvent(t, 0, "event-invalid-close-produced", 90, nil,
				[]model.EventKey{cause})
		}},
		{name: "referenced Artifact lacks provenance", replace: func(t *testing.T, fixture *derivationDispositionFixture) {
			root := verifiedRoot(t, "derivation-unowned-result",
				`{"entries":[],"kind":"derivation-result","total_bytes":0}`, 0)
			if _, err := fixture.store.CheckpointVerifiedArtifactRoot(context.Background(), root); err != nil {
				t.Fatal(err)
			}
			ref, _ := model.NewArtifactRef(root.RootDigest, model.ArtifactReferenced)
			fixture.insertEvent(t, "event-invalid-delivered-unowned", fixture.children[0], fixture.node.PeerID(),
				fixture.reviewers[1], model.EventReviewDelivered, 89, fixture.now.Add(49*time.Second),
				[]model.ArtifactRef{ref}, nil)
			cause := fixture.eventKey(t, fixture.node.PeerID(), "event-invalid-delivered-unowned")
			fixture.replaceClosedChildEvent(t, 0, "event-invalid-close-unowned", 90, nil,
				[]model.EventKey{cause})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newDerivationDispositionFixture(t, true)
			test.replace(t, fixture)
			err := fixture.store.ReconcileWorkDerivationDisposition(context.Background(), fixture.children[0])
			if !errors.Is(err, ErrDerivationDispositionConflict) {
				t.Fatalf("reconciliation error = %v, want disposition conflict", err)
			}
			assertDispositionHandlingCount(t, fixture.store, 0)
			assertDispositionHandlingPins(t, fixture, 0)
		})
	}
}

func TestReconcileWorkDerivationDispositionRejectsClosedCauseFromPriorIteration(t *testing.T) {
	t.Parallel()
	fixture := newDerivationDispositionFixture(t, true)
	child := fixture.children[0]
	deliveredAt := fixture.now.Add(49 * time.Second)
	closedAt := fixture.now.Add(50 * time.Second)

	fixture.insertEventAtVersion(t, "event-child-a-old-iteration-delivered", child,
		fixture.node.PeerID(), fixture.reviewers[1], model.EventReviewDelivered, 4, 1, 89,
		deliveredAt, nil, nil)
	delivered := fixture.eventKey(t, fixture.node.PeerID(), "event-child-a-old-iteration-delivered")
	fixture.insertEventAtVersion(t, "event-child-a-iteration-two-closed", child,
		fixture.node.PeerID(), fixture.reviewers[1], model.EventReviewClosed, 5, 2, 90,
		closedAt, nil, []model.EventKey{delivered})
	closedPayload := fixture.eventPayload(t, "event-child-a-iteration-two-closed")
	mustExec(t, fixture.store, `UPDATE works SET version=6,iteration=2,state='CLOSED',state_json=?,
		updated_by_event='event-child-a-iteration-two-closed',updated_at=?
		WHERE home_peer_id=? AND work_id=?`, closedPayload.Bytes(), storeTime(closedAt),
		child.HomePeerID().String(), child.WorkID().String())

	err := fixture.store.ReconcileWorkDerivationDisposition(context.Background(), child)
	if !errors.Is(err, ErrDerivationDispositionConflict) {
		t.Fatalf("reconciliation error = %v, want disposition conflict", err)
	}
	assertDispositionHandlingCount(t, fixture.store, 0)
	assertDispositionHandlingPins(t, fixture, 0)
}

func TestCommitLocalAcceptanceRollsBackTerminalChildAndDispositionAfterLaterEvidenceFailure(t *testing.T) {
	fixture := newDerivationDispositionFixture(t, false)
	ctx := context.Background()
	child := fixture.children[1]
	deliveredAt := fixture.now.Add(22 * time.Second)
	fixture.insertDeliveredChildResult(t, 1, "event-child-b-delivered", 42, deliveredAt)
	deliveredPayload := fixture.eventPayload(t, "event-child-b-delivered")
	mustExec(t, fixture.store, `UPDATE works SET version=3,state='DELIVERED',state_json=?,
		updated_by_event='event-child-b-delivered',updated_at=? WHERE home_peer_id=? AND work_id=?`,
		deliveredPayload.Bytes(), storeTime(deliveredAt), child.HomePeerID().String(), child.WorkID().String())
	// Align the synthetic durable history with the real local admission heads so
	// the close Event is origin/channel sequence 43 and is therefore the last
	// terminal child Event.
	mustExec(t, fixture.store, "UPDATE node SET next_origin_seq=43 WHERE singleton=1")
	mustExec(t, fixture.store, `UPDATE publication_epochs SET source_head_channel_seq=42
		WHERE channel_id=? AND origin_peer_id=? AND origin_epoch=?`, fixture.channel.String(),
		fixture.node.PeerID().String(), fixture.node.OriginEpoch().String())

	run, _ := model.ParseRunID("run-derivation")
	operationID, _ := model.ParseOperationID("operation-close-derived")
	contextHash := model.Sum([]byte("context-close-derived"))
	closeAt := fixture.now.Add(30 * time.Second)
	leaseUntil := closeAt.Add(time.Minute)
	operation, err := model.NewOperation(model.OperationSpec{ID: operationID,
		ProfileID: model.TeamworkProfileID(), AgentRunID: run,
		ClientKeyHash: model.Sum([]byte("key-close-derived")), ContextHash: &contextHash,
		Kind: model.OperationTeamworkClose, RequestDigest: model.Sum([]byte("request-close-derived")),
		Status: model.OperationStarted, LeaseOwner: "owner-close-derived", LeaseUntil: &leaseUntil,
		CreatedAt: fixture.now.Add(23 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.ReserveOperation(ctx, operation, fixture.now.Add(24*time.Second)); err != nil {
		t.Fatal(err)
	}
	authority := &LocalOperationAuthority{operation.ID(), operation.Kind(), operation.RequestDigest(), operation.LeaseOwner()}

	current, err := fixture.store.GetReviewWork(ctx, child)
	if err != nil {
		t.Fatal(err)
	}
	audience, _ := model.NewAudience([]model.PeerID{current.Participants().ReviewerPeerID()})
	scope, err := fixture.store.PrepareLocalAdmission(ctx, fixture.channel, audience, 1)
	if err != nil {
		t.Fatal(err)
	}
	eventScope, _ := scope.EventScope(0, child)
	closeEvent, _ := model.ParseEventID("event-child-b-close-committed")
	causeEvent, _ := model.ParseEventID("event-child-b-delivered")
	cause, _ := model.NewEventKey(fixture.node.PeerID(), fixture.node.OriginEpoch(), causeEvent)
	stamp, err := eventpkg.NewAdmissionStamp(eventpkg.AdmissionStampSpec{Node: scope.Node(), Profile: scope.Profile(),
		EventID: closeEvent, ChannelID: fixture.channel, WorkRef: child,
		OriginSequence: eventScope.OriginSequence(), ChannelSequence: eventScope.ChannelSequence(),
		OriginMember: eventScope.OriginMember(), PublicationRoster: eventScope.PublicationRoster(), Audience: audience,
		WorkVersion: current.Version(), Iteration: current.Iteration(),
		WorkDeadlineUnixNano: current.DeadlineUnixNano(), CausedBy: []model.EventKey{cause}})
	if err != nil {
		t.Fatal(err)
	}
	signer, _ := eventpkg.NewEd25519Signer(fixture.privateKey)
	factory, _ := eventpkg.NewFactory(acceptanceClock{closeAt}, signer)
	candidate, _ := eventpkg.NewCloseCandidate("")
	bundle, err := factory.AdmitAgent(ctx, stamp, candidate)
	if err != nil {
		t.Fatal(err)
	}
	nextSpec := current.Spec()
	nextSpec.Version = current.Version() + 1
	nextSpec.State = model.WorkClosed
	nextSpec.StateData = bundle.Event().Payload()
	nextSpec.UpdatedBy = bundle.Event().ID()
	nextSpec.UpdatedAt = closeAt
	next, err := model.NewReviewWork(nextSpec)
	if err != nil {
		t.Fatal(err)
	}
	mutation, err := NewWorkTransition(next, current.Version(), current.State())
	if err != nil {
		t.Fatal(err)
	}

	// This valid pre-existing row collides only after Event insertion, Work CAS,
	// derivation reconciliation and Gossip publication insertion have run.
	collisionID := deterministicDeliveryID(closeEvent, current.Participants().ReviewerPeerID())
	mustExec(t, fixture.store, `INSERT INTO peer_deliveries(delivery_id,channel_id,target_peer_id,event_id,
		status,created_at,updated_at) VALUES(?,?,?,'event-child-b-delivered','pending',?,?)`, collisionID,
		fixture.channel.String(), current.Participants().ReviewerPeerID().String(), storeTime(closeAt), storeTime(closeAt))
	spec := LocalAcceptanceSpec{Scope: scope, Items: []LocalAcceptanceItem{{Publication: bundle.Publication(),
		Work: &mutation}}, Operation: authority}
	if _, err := fixture.store.CommitLocalAcceptance(ctx, spec, closeAt.Add(time.Second)); err == nil {
		t.Fatal("late PeerDelivery collision did not fail local acceptance")
	}

	durable, err := fixture.store.GetReviewWork(ctx, child)
	if err != nil || durable.Version() != current.Version() || durable.State() != model.WorkDelivered {
		t.Fatalf("terminal Work survived rollback: work=%#v err=%v", durable, err)
	}
	var closeEvents int
	if err := fixture.store.db.QueryRow("SELECT COUNT(*) FROM events WHERE event_id=?", closeEvent.String()).Scan(&closeEvents); err != nil {
		t.Fatal(err)
	}
	if closeEvents != 0 {
		t.Fatalf("terminal Event survived rollback: count=%d", closeEvents)
	}
	assertDispositionHandlingCount(t, fixture.store, 0)
	assertDispositionHandlingPins(t, fixture, 0)
	assertOperationStatus(t, fixture.store, operation.ID(), model.OperationStarted)
}

type derivationDispositionFixture struct {
	*acceptanceFixture
	operation model.OperationID
	parent    model.WorkRef
	children  []model.WorkRef
	artifacts [][]model.ArtifactRef
}

func newDerivationDispositionFixture(t *testing.T, allTerminal bool) *derivationDispositionFixture {
	t.Helper()
	base := newAcceptanceFixture(t, 3)
	fixture := &derivationDispositionFixture{acceptanceFixture: base}
	fixture.operation, _ = model.ParseOperationID("operation-derivation")
	run, _ := model.ParseRunID("run-derivation")
	created := base.now.Add(-time.Minute)
	mustExec(t, base.store, `INSERT INTO agent_runs(run_id,profile_id,cause_json,launcher,runtime_kind,
		launcher_diagnostic_json,runtime_ids_json,status,started_at)
		VALUES(?,?,'{}','test',?,'{}','{}','running',?)`, run.String(), model.TeamworkProfileID().String(),
		string(base.profile.Runtime()), storeTime(created))
	mustExec(t, base.store, `INSERT INTO operations(operation_id,profile_id,agent_run_id,client_key_hash,
		context_hash,kind,request_digest,status,result_json,created_at,finished_at)
		VALUES(?,?,?,?,?,'teamwork.offer',?,'committed','{}',?,?)`, fixture.operation.String(),
		model.TeamworkProfileID().String(), run.String(), model.Sum([]byte("derivation-key")).Bytes(),
		model.Sum([]byte("derivation-context")).Bytes(), model.Sum([]byte("derivation-request")).Bytes(),
		storeTime(created), storeTime(base.now))
	root := verifiedRoot(t, "derivation-result",
		`{"entries":[],"kind":"derivation-result","total_bytes":0}`, 0)
	if _, err := base.store.CheckpointVerifiedArtifactRoot(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	ref, err := model.NewArtifactRef(root.RootDigest, model.ArtifactReferenced)
	if err != nil {
		t.Fatal(err)
	}
	fixture.artifacts = [][]model.ArtifactRef{{ref}, nil}

	parentID, _ := model.ParseWorkID("work-parent")
	fixture.parent, _ = model.NewWorkRef(base.reviewers[0], parentID)
	fixture.insertEvent(t, "event-parent-active", fixture.parent, base.reviewers[0], base.node.PeerID(),
		model.EventReviewAccepted, 1, base.now, nil, nil)
	fixture.insertWork(t, fixture.parent, base.node.PeerID(), model.WorkActive, 2,
		"event-parent-active", base.now)

	for index := 0; index < 2; index++ {
		workID, _ := model.ParseWorkID(fmt.Sprintf("work-child-%c", 'a'+index))
		child, _ := model.NewWorkRef(base.node.PeerID(), workID)
		fixture.children = append(fixture.children, child)
		state, version := model.WorkClosed, uint64(4)
		if index == 1 && !allTerminal {
			state, version = model.WorkActive, 2
		}
		eventID := fmt.Sprintf("event-child-%c-initial", 'a'+index)
		acceptedAt := base.now.Add(time.Duration(index+4) * time.Second)
		if state == model.WorkClosed {
			fixture.insertClosedChildHistory(t, index, eventID, uint64(30+index), uint64(40+index), acceptedAt)
		} else {
			fixture.insertEvent(t, eventID, child, base.node.PeerID(), base.reviewers[index+1],
				model.EventReviewAccepted, uint64(30+index), acceptedAt, nil, nil)
		}
		fixture.insertWork(t, child, base.reviewers[index+1], state, version, eventID, acceptedAt)
	}

	createdAt := storeTime(base.now.Add(6 * time.Second))
	for index, child := range fixture.children {
		mustExec(t, base.store, `INSERT INTO work_derivations(operation_id,child_ordinal,child_channel_id,
			child_home_peer_id,child_work_id,parent_channel_id,parent_home_peer_id,parent_work_id,
			parent_version,parent_event_id,created_at) VALUES(?,?,?,?,?,?,?,?,2,'event-parent-active',?)`,
			fixture.operation.String(), index, base.channel.String(), child.HomePeerID().String(), child.WorkID().String(),
			base.channel.String(), fixture.parent.HomePeerID().String(), fixture.parent.WorkID().String(), createdAt)
	}
	// Raw historical Events above intentionally bypass publication workers. Put
	// the synthetic local heads immediately after that history so tests that
	// exercise real admission can continue at sequence 42 without a gap.
	mustExec(t, base.store, "UPDATE node SET next_origin_seq=42 WHERE singleton=1")
	for head := 1; head <= 41; head++ {
		mustExec(t, base.store, `UPDATE publication_epochs SET source_head_channel_seq=?
			WHERE channel_id=? AND origin_peer_id=? AND origin_epoch=?`, head, base.channel.String(),
			base.node.PeerID().String(), base.node.OriginEpoch().String())
	}
	return fixture
}

func (fixture *derivationDispositionFixture) insertEvent(t *testing.T, eventText string, ref model.WorkRef,
	origin, audiencePeer model.PeerID, eventType model.EventType, sequence uint64, acceptedAt time.Time,
	artifacts []model.ArtifactRef, causes []model.EventKey,
) {
	t.Helper()
	fixture.insertEventAtVersion(t, eventText, ref, origin, audiencePeer, eventType,
		derivationFixtureWorkVersion(t, eventType), 1, sequence, acceptedAt, artifacts, causes)
}

func (fixture *derivationDispositionFixture) insertEventAtVersion(t *testing.T, eventText string,
	ref model.WorkRef, origin, audiencePeer model.PeerID, eventType model.EventType,
	workVersion uint64, iteration uint8, sequence uint64, acceptedAt time.Time,
	artifacts []model.ArtifactRef, causes []model.EventKey,
) {
	t.Helper()
	var memberRevision, rosterRevision uint64
	var memberHash, rosterHash []byte
	var epoch string
	if err := fixture.store.db.QueryRow(`SELECT revision,record_hash,origin_epoch FROM channel_members
		WHERE channel_id=? AND member_peer_id=? ORDER BY revision DESC LIMIT 1`, fixture.channel.String(),
		origin.String()).Scan(&memberRevision, &memberHash, &epoch); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.db.QueryRow(`SELECT roster_head_revision,roster_head_hash FROM channels
		WHERE channel_id=?`, fixture.channel.String()).Scan(&rosterRevision, &rosterHash); err != nil {
		t.Fatal(err)
	}
	audience, _ := model.NewAudience([]model.PeerID{audiencePeer})
	audienceJSON, _ := model.JSONFrom(audience)
	resourceJSON, _ := model.JSONFrom(ref)
	artifactJSON, _ := model.JSONFrom(append([]model.ArtifactRef{}, artifacts...))
	causedByJSON, _ := model.JSONFrom(append([]model.EventKey{}, causes...))
	payloadJSON := derivationFixturePayload(t, eventType, workVersion, iteration,
		fixture.now.Add(24*time.Hour))
	source := model.EventSourceImported
	if origin == fixture.node.PeerID() {
		source = model.EventSourceLocal
	}
	digest := model.Sum([]byte(eventText))
	mustExec(t, fixture.store, `INSERT INTO events(event_id,schema_version,channel_id,origin_peer_id,
		origin_epoch,origin_seq,channel_seq,origin_member_revision,origin_member_record_hash,
		publication_roster_revision,publication_roster_hash,source,actor_principal,event_type,
		audience_json,resource_json,work_home_peer_id,work_id,summary,payload_json,artifact_roots_json,
		caused_by_json,canonical_event_json,event_digest,canonical_publication_json,publication_digest,
		origin_signature,created_at,accepted_at) VALUES(?,1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
		?,'{}',?,'{}',?,X'01',?,?)`, eventText, fixture.channel.String(), origin.String(), epoch,
		sequence, sequence, memberRevision, memberHash, rosterRevision, rosterHash, string(source),
		"principal-derivation", string(eventType), audienceJSON.Bytes(), resourceJSON.Bytes(),
		ref.HomePeerID().String(), ref.WorkID().String(), "derivation event", payloadJSON.Bytes(),
		artifactJSON.Bytes(), causedByJSON.Bytes(), digest.Bytes(), digest.Bytes(),
		storeTime(acceptedAt), storeTime(acceptedAt))
}

func derivationFixtureWorkVersion(t *testing.T, eventType model.EventType) uint64 {
	t.Helper()
	switch eventType {
	case model.EventReviewAccepted:
		return 1
	case model.EventReviewDeliveryReady, model.EventReviewDelivered, model.EventReviewCancelled,
		model.EventReviewDeclined, model.EventReviewExpired:
		return 2
	case model.EventReviewClosed:
		return 3
	default:
		t.Fatalf("no default fixture Work version for Event type %s", eventType)
		return 0
	}
}

func derivationFixturePayload(t *testing.T, eventType model.EventType, workVersion uint64,
	iteration uint8, deadline time.Time,
) model.JSON {
	t.Helper()
	var value any
	switch eventType {
	case model.EventReviewAccepted, model.EventReviewDelivered, model.EventReviewDeclined:
		value = struct {
			Iteration   uint8  `json:"iteration"`
			WorkVersion uint64 `json:"work_version"`
		}{iteration, workVersion}
	case model.EventReviewClosed:
		value = struct {
			Iteration   uint8  `json:"iteration"`
			Note        string `json:"note"`
			WorkVersion uint64 `json:"work_version"`
		}{iteration, "", workVersion}
	case model.EventReviewDeliveryReady, model.EventReviewCancelled:
		value = struct {
			Content     string `json:"content"`
			Iteration   uint8  `json:"iteration"`
			WorkVersion uint64 `json:"work_version"`
		}{"fixture content", iteration, workVersion}
	case model.EventReviewExpired:
		value = struct {
			Deadline    string `json:"deadline"`
			Iteration   uint8  `json:"iteration"`
			WorkVersion uint64 `json:"work_version"`
		}{deadline.UTC().Format(time.RFC3339Nano), iteration, workVersion}
	default:
		t.Fatalf("no fixture payload schema for Event type %s", eventType)
	}
	payload, err := model.JSONFrom(value)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func (fixture *derivationDispositionFixture) eventPayload(t *testing.T, eventText string) model.JSON {
	t.Helper()
	var raw []byte
	if err := fixture.store.db.QueryRow("SELECT payload_json FROM events WHERE event_id=?", eventText).
		Scan(&raw); err != nil {
		t.Fatal(err)
	}
	payload, err := model.NewJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func (fixture *derivationDispositionFixture) insertDeliveredChildResult(t *testing.T, index int,
	deliveredText string, sequence uint64, acceptedAt time.Time,
) model.EventKey {
	t.Helper()
	child, reviewer := fixture.children[index], fixture.reviewers[index+1]
	readyText := deliveredText + "-ready"
	readyAt := acceptedAt.Add(-time.Second)
	produced := make([]model.ArtifactRef, len(fixture.artifacts[index]))
	for refIndex, ref := range fixture.artifacts[index] {
		produced[refIndex], _ = model.NewArtifactRef(ref.RootDigest(), model.ArtifactProduced)
	}
	fixture.insertEvent(t, readyText, child, reviewer, fixture.node.PeerID(),
		model.EventReviewDeliveryReady, 1, readyAt, produced, nil)
	readyKey := fixture.eventKey(t, reviewer, readyText)
	if len(produced) != 0 {
		tx, err := fixture.store.db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, ref := range produced {
			provenance, err := model.NewArtifactProvenance(model.ArtifactProvenanceSpec{
				RootDigest: ref.RootDigest(), ProducerEvent: readyKey, ProducerOriginPeer: reviewer,
				Relation: model.ProvenanceReplica, CreatedAt: readyAt,
			})
			if err != nil {
				_ = tx.Rollback()
				t.Fatal(err)
			}
			if _, err := insertArtifactProvenance(context.Background(), tx, provenance); err != nil {
				_ = tx.Rollback()
				t.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	fixture.insertEvent(t, deliveredText, child, fixture.node.PeerID(), reviewer,
		model.EventReviewDelivered, sequence, acceptedAt, fixture.artifacts[index], []model.EventKey{readyKey})
	return fixture.eventKey(t, fixture.node.PeerID(), deliveredText)
}

func (fixture *derivationDispositionFixture) insertClosedChildHistory(t *testing.T, index int,
	closedText string, deliveredSequence, closedSequence uint64, acceptedAt time.Time,
) {
	t.Helper()
	deliveredKey := fixture.insertDeliveredChildResult(t, index, closedText+"-delivered",
		deliveredSequence, acceptedAt.Add(-time.Second))
	fixture.insertEvent(t, closedText, fixture.children[index], fixture.node.PeerID(),
		fixture.reviewers[index+1], model.EventReviewClosed, closedSequence, acceptedAt, nil,
		[]model.EventKey{deliveredKey})
}

func (fixture *derivationDispositionFixture) eventKey(t *testing.T, origin model.PeerID,
	eventText string,
) model.EventKey {
	t.Helper()
	var epochText string
	if err := fixture.store.db.QueryRow(`SELECT origin_epoch FROM channel_members WHERE channel_id=?
		AND member_peer_id=? ORDER BY revision DESC LIMIT 1`, fixture.channel.String(), origin.String()).
		Scan(&epochText); err != nil {
		t.Fatal(err)
	}
	epoch, err := model.ParseOriginEpoch(epochText)
	if err != nil {
		t.Fatal(err)
	}
	event, err := model.ParseEventID(eventText)
	if err != nil {
		t.Fatal(err)
	}
	key, err := model.NewEventKey(origin, epoch, event)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func (fixture *derivationDispositionFixture) insertWork(t *testing.T, ref model.WorkRef, reviewer model.PeerID,
	state model.WorkState, version uint64, eventText string, updatedAt time.Time,
) {
	t.Helper()
	stateData := fixture.eventPayload(t, eventText)
	var rosterRevision uint64
	if err := fixture.store.db.QueryRow("SELECT roster_head_revision FROM channels WHERE channel_id=?",
		fixture.channel.String()).Scan(&rosterRevision); err != nil {
		t.Fatal(err)
	}
	mustExec(t, fixture.store, `INSERT INTO works(channel_id,home_peer_id,work_id,
		participant_roster_revision,version,iteration,deadline_unix_nano,state,state_json,updated_by_event,
		updated_at) VALUES(?,?,?,?,?,1,?,?,?,?,?)`, fixture.channel.String(), ref.HomePeerID().String(),
		ref.WorkID().String(), rosterRevision, version, fixture.now.Add(24*time.Hour).UnixNano(), string(state),
		stateData.Bytes(), eventText, storeTime(updatedAt))
	mustExec(t, fixture.store, `INSERT INTO work_members(channel_id,home_peer_id,work_id,peer_id,role)
		VALUES(?,?,?,?,'initiator')`, fixture.channel.String(), ref.HomePeerID().String(), ref.WorkID().String(),
		ref.HomePeerID().String())
	mustExec(t, fixture.store, `INSERT INTO work_members(channel_id,home_peer_id,work_id,peer_id,role)
		VALUES(?,?,?,?,'reviewer')`, fixture.channel.String(), ref.HomePeerID().String(), ref.WorkID().String(),
		reviewer.String())
}

func (fixture *derivationDispositionFixture) replaceClosedChildEvent(t *testing.T, index int,
	eventText string, sequence uint64, artifacts []model.ArtifactRef, causes []model.EventKey,
) {
	t.Helper()
	acceptedAt := fixture.now.Add(50 * time.Second)
	child := fixture.children[index]
	fixture.insertEvent(t, eventText, child, fixture.node.PeerID(), fixture.reviewers[index+1],
		model.EventReviewClosed, sequence, acceptedAt, artifacts, causes)
	stateData := fixture.eventPayload(t, eventText)
	mustExec(t, fixture.store, `UPDATE works SET state_json=?,updated_by_event=?,updated_at=?
		WHERE home_peer_id=? AND work_id=?`, stateData.Bytes(), eventText, storeTime(acceptedAt),
		child.HomePeerID().String(), child.WorkID().String())
}

func (fixture *derivationDispositionFixture) terminalizeChild(t *testing.T, index int, eventText string,
	sequence uint64, acceptedAt time.Time,
) {
	t.Helper()
	child := fixture.children[index]
	fixture.insertClosedChildHistory(t, index, eventText, sequence-1, sequence, acceptedAt)
	stateData := fixture.eventPayload(t, eventText)
	mustExec(t, fixture.store, `UPDATE works SET version=4,state='CLOSED',state_json=?,
		updated_by_event=?,updated_at=? WHERE home_peer_id=? AND work_id=?`, stateData.Bytes(), eventText,
		storeTime(acceptedAt),
		child.HomePeerID().String(), child.WorkID().String())
}

func (fixture *derivationDispositionFixture) advanceParent(t *testing.T, eventType model.EventType,
	state model.WorkState, version uint64,
) {
	t.Helper()
	acceptedAt := fixture.now.Add(20 * time.Second)
	fixture.insertEvent(t, "event-parent-advanced", fixture.parent, fixture.parent.HomePeerID(),
		fixture.node.PeerID(), eventType, 2, acceptedAt, nil, nil)
	stateData := fixture.eventPayload(t, "event-parent-advanced")
	mustExec(t, fixture.store, `UPDATE works SET version=?,state=?,state_json=?,updated_by_event=?,updated_at=?
		WHERE home_peer_id=? AND work_id=?`, version, string(state), stateData.Bytes(), "event-parent-advanced",
		storeTime(acceptedAt),
		fixture.parent.HomePeerID().String(), fixture.parent.WorkID().String())
}

func (fixture *derivationDispositionFixture) handling(t *testing.T) model.Handling {
	t.Helper()
	id, err := deterministicDerivationHandlingID(fixture.operation)
	if err != nil {
		t.Fatal(err)
	}
	handling, err := fixture.store.GetAgentHandling(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return handling
}

func assertDispositionHandlingCount(t *testing.T, store *Store, want int) {
	t.Helper()
	var got int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM agent_handlings
		WHERE handling_id LIKE 'handling-derivation-%'`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("derivation Handling count = %d, want %d", got, want)
	}
}

func assertDispositionHandlingPins(t *testing.T, fixture *derivationDispositionFixture, want int) {
	t.Helper()
	handling, err := deterministicDerivationHandlingID(fixture.operation)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := fixture.store.db.Query(`SELECT root_digest FROM artifact_pins
		WHERE owner_kind='handling' AND owner_id=? ORDER BY root_digest`, handling.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := make(map[model.Digest]struct{})
	for rows.Next() {
		var rootText string
		if err := rows.Scan(&rootText); err != nil {
			t.Fatal(err)
		}
		root, err := model.ParseDigest(rootText)
		if err != nil {
			t.Fatal(err)
		}
		got[root] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != want {
		t.Fatalf("derivation Handling pin count = %d, want %d", len(got), want)
	}
	if want == 0 {
		return
	}
	for _, refs := range fixture.artifacts {
		for _, ref := range refs {
			if _, ok := got[ref.RootDigest()]; !ok {
				t.Fatalf("missing derivation Handling pin for %s", ref.RootDigest().String())
			}
		}
	}
}
