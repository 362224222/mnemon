package store

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/teamwork"
)

func TestCommitPeerInboxSemanticAppliesAndReplaysExactDecision(t *testing.T) {
	fixture := newPeerInboxFixture(t, "semantic-commit-apply", 0)
	installPeerInboxSemanticLocalAuthority(t, fixture)
	publication, work, _ := peerInboxSemanticCurrentWorkPublication(t, fixture,
		"semantic-commit-apply", 1, 1)
	put := fixture.put(t, publication, fixture.at)
	readyAt := fixture.at.Add(time.Second)
	markPeerInboxSemanticReady(t, fixture.store, put.InboxID, readyAt)
	claim := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-commit-worker",
		readyAt.Add(time.Second))
	commitAt := readyAt.Add(2 * time.Second)
	spec := peerInboxSemanticCommitSpec(t, fixture, claim, commitAt)

	trustedNow := commitAt.Add(time.Nanosecond)
	result, err := fixture.store.CommitPeerInboxSemantic(context.Background(), spec, trustedNow)
	if err != nil || !result.Changed() || result.Replayed() ||
		result.Status() != model.InboxAccepted || result.ImportedEventID() != publication.Event().ID() {
		t.Fatalf("CommitPeerInboxSemantic() = (%#v,%v)", result, err)
	}
	responses := result.ResponseEventIDs()
	if len(responses) != 1 || responses[0] != spec.Responses[0].Event().ID() {
		t.Fatalf("response Events = %#v", responses)
	}
	receipt, ok := result.ReceiptEventID()
	if !ok || receipt != responses[0] || result.Decision().IsZero() {
		t.Fatalf("receipt/decision = (%s,%t,%s)", receipt, ok, result.Decision().String())
	}
	durable, err := fixture.store.GetReviewWork(context.Background(), work.Ref())
	if err != nil || durable.Version() != 2 || durable.State() != model.WorkActive ||
		durable.UpdatedBy() != responses[0] {
		t.Fatalf("durable Work = (%#v,%v)", durable, err)
	}
	assertPeerInboxSemanticTerminalProjection(t, fixture.store, put.InboxID,
		model.InboxAccepted, publication.Event().ID(), responses[0])
	assertPeerInboxSemanticImportedEventHasNoGossip(t, fixture.store, publication.Event().ID())

	path := fixture.store.Path()
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.store = nil
	restarted, err := OpenExisting(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	replay, err := restarted.CommitPeerInboxSemantic(context.Background(), spec,
		trustedNow.Add(time.Hour))
	if err != nil || replay.Changed() || !replay.Replayed() ||
		replay.Decision().String() != result.Decision().String() {
		t.Fatalf("restart replay = (%#v,%v)", replay, err)
	}

	different := spec
	differentAt := spec.Plan.DecisionAt().Add(time.Nanosecond)
	different.Plan = peerInboxSemanticStorePlan(t, claim, differentAt,
		teamworkPlanForClaim(t, claim, differentAt))
	if _, err := restarted.CommitPeerInboxSemantic(context.Background(), different,
		trustedNow.Add(time.Hour)); !errors.Is(err, ErrPeerInboxSemanticInvariant) {
		t.Fatalf("different request replay error = %v", err)
	}
}

func TestCommitPeerInboxSemanticRetryAndRollbackLeaveNoDomainEffects(t *testing.T) {
	t.Run("planner retry", func(t *testing.T) {
		fixture := newPeerInboxFixture(t, "semantic-commit-retry", 0)
		draft := fixture.publication(t, 1, 1, "semantic-commit-retry", true).Event()
		payload, _ := model.NewJSON([]byte(`{"iteration":1,"work_version":1}`))
		publication := fixture.signEvent(t, model.EventSpec{ID: draft.ID(), Scope: draft.Scope(),
			Source: model.EventSourceLocal, ActorPrincipal: draft.ActorPrincipal(), Type: draft.Type(),
			Audience: draft.Audience(), Summary: draft.Summary(), Payload: payload,
			CreatedAt: draft.CreatedAt(), AcceptedAt: draft.AcceptedAt()})
		put := fixture.put(t, publication, fixture.at)
		inboxID := put.InboxID
		readyAt := fixture.at.Add(time.Second)
		markPeerInboxSemanticReady(t, fixture.store, inboxID, readyAt)
		claim := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-retry-commit-worker",
			readyAt.Add(time.Second))
		decisionAt := readyAt.Add(2 * time.Second)
		policy := teamworkPlanForClaim(t, claim, decisionAt)
		if policy.Disposition() != teamwork.ImportRetry {
			t.Fatalf("policy disposition = %q, want retry", policy.Disposition())
		}
		_, err := fixture.store.CommitPeerInboxSemantic(context.Background(),
			CommitPeerInboxSemanticSpec{Fence: claim.Fence()}, decisionAt)
		if !errors.Is(err, ErrPeerInboxSemanticInput) {
			t.Fatalf("retry entered terminal commit: %v", err)
		}
		assertPeerInboxSemanticState(t, fixture.store, inboxID, "processing",
			claim.Fence().attempt, "", true)
		assertPeerInboxSemanticNoDomainMutation(t, fixture.store, claim.ImportedEvent().ID())
	})

	t.Run("terminal write rollback", func(t *testing.T) {
		fixture := newPeerInboxFixture(t, "semantic-commit-rollback", 0)
		installPeerInboxSemanticLocalAuthority(t, fixture)
		publication, work, _ := peerInboxSemanticCurrentWorkPublication(t, fixture,
			"semantic-commit-rollback", 1, 1)
		put := fixture.put(t, publication, fixture.at)
		readyAt := fixture.at.Add(time.Second)
		markPeerInboxSemanticReady(t, fixture.store, put.InboxID, readyAt)
		claim := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-rollback-commit-worker",
			readyAt.Add(time.Second))
		spec := peerInboxSemanticCommitSpec(t, fixture, claim, readyAt.Add(2*time.Second))
		mustExec(t, fixture.store, `CREATE TEMP TRIGGER reject_semantic_terminal_commit
			BEFORE UPDATE OF status ON peer_inbox WHEN NEW.status='accepted'
			BEGIN SELECT RAISE(ABORT,'injected semantic terminal failure'); END`)
		if _, err := fixture.store.CommitPeerInboxSemantic(context.Background(), spec,
			spec.Plan.DecisionAt()); err == nil {
			t.Fatal("injected terminal failure was accepted")
		}
		assertPeerInboxSemanticState(t, fixture.store, put.InboxID, "processing", 1, "", true)
		assertPeerInboxSemanticNoDomainMutation(t, fixture.store, publication.Event().ID())
		durable, err := fixture.store.GetReviewWork(context.Background(), work.Ref())
		if err != nil || durable.Version() != 1 || durable.State() != model.WorkOffered {
			t.Fatalf("Work after rollback = (%#v,%v)", durable, err)
		}
		var localResponses int
		if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_id=?`,
			spec.Responses[0].Event().ID().String()).Scan(&localResponses); err != nil || localResponses != 0 {
			t.Fatalf("local response count after rollback = (%d,%v)", localResponses, err)
		}
	})
}

func TestCommitPeerInboxSemanticRejectsResponseArtifactSelfAuthorization(t *testing.T) {
	fixture := newPeerInboxFixture(t, "semantic-response-artifact", 0)
	installPeerInboxSemanticLocalAuthority(t, fixture)
	publication, _, _ := peerInboxSemanticCurrentWorkPublication(t, fixture,
		"semantic-response-artifact", 1, 1)
	put := fixture.put(t, publication, fixture.at)
	readyAt := fixture.at.Add(time.Second)
	markPeerInboxSemanticReady(t, fixture.store, put.InboxID, readyAt)
	claim := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-artifact-response-worker",
		readyAt.Add(time.Second))
	commitAt := readyAt.Add(2 * time.Second)
	spec := peerInboxSemanticCommitSpec(t, fixture, claim, commitAt)
	ref, _ := model.NewArtifactRef(model.Sum([]byte("semantic-response-artifact")),
		model.ArtifactReferenced)
	spec.Responses[0] = peerInboxSemanticSignedResponse(t, fixture, claim, spec.Scope,
		teamworkPlanForClaim(t, claim, commitAt).Responses()[0], 0, commitAt,
		[]model.ArtifactRef{ref})
	if _, err := fixture.store.CommitPeerInboxSemantic(context.Background(), spec,
		spec.Plan.DecisionAt()); !errors.Is(err, ErrPeerInboxSemanticInput) {
		t.Fatalf("self-authorized response Artifact error = %v", err)
	}
	assertPeerInboxSemanticNoDomainMutation(t, fixture.store, publication.Event().ID())
}

func TestCommitPeerInboxSemanticReplansAfterTrustedCommitCrossesDeadline(t *testing.T) {
	fixture := newPeerInboxFixture(t, "semantic-deadline-crossing", 0)
	installPeerInboxSemanticLocalAuthority(t, fixture)
	publication, work, _ := peerInboxSemanticCurrentWorkPublication(t, fixture,
		"semantic-deadline-crossing", 1, 1)
	put := fixture.put(t, publication, fixture.at)
	deadline := time.Unix(0, work.DeadlineUnixNano()).UTC()
	readyAt := deadline.Add(-10 * time.Second)
	markPeerInboxSemanticReady(t, fixture.store, put.InboxID, readyAt)
	claim := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-deadline-worker",
		readyAt.Add(time.Second))
	decisionAt := deadline.Add(-time.Second)
	beforeDeadline := peerInboxSemanticCommitSpec(t, fixture, claim, decisionAt)
	if len(beforeDeadline.Responses) != 1 ||
		beforeDeadline.Responses[0].Event().Type() != model.EventReviewAccepted {
		t.Fatalf("pre-deadline responses = %#v", beforeDeadline.Responses)
	}
	beforeOrigin, beforeChannel := peerInboxSemanticLocalHeads(t, fixture.store,
		fixture.channel.Channel().ID(), fixture.channel.Owner())
	_, err := fixture.store.CommitPeerInboxSemantic(context.Background(), beforeDeadline,
		deadline)
	if !errors.Is(err, ErrDeadlineResolution) {
		t.Fatalf("cross-deadline commit error = %v", err)
	}
	assertPeerInboxSemanticState(t, fixture.store, put.InboxID, "processing", 1, "", true)
	assertPeerInboxSemanticNoDomainMutation(t, fixture.store, publication.Event().ID())
	afterOrigin, afterChannel := peerInboxSemanticLocalHeads(t, fixture.store,
		fixture.channel.Channel().ID(), fixture.channel.Owner())
	if afterOrigin != beforeOrigin || afterChannel != beforeChannel {
		t.Fatalf("local heads changed across rollback: (%d,%d) -> (%d,%d)",
			beforeOrigin, beforeChannel, afterOrigin, afterChannel)
	}
	durable, err := fixture.store.GetReviewWork(context.Background(), work.Ref())
	if err != nil || !samePeerInboxSemanticWork(durable, work) {
		t.Fatalf("Work after deadline rollback = (%#v,%v)", durable, err)
	}
	var transitionReceipts int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM peer_inbox_semantic_transition_receipts
		WHERE inbox_id=?`, put.InboxID.String()).Scan(&transitionReceipts); err != nil ||
		transitionReceipts != 0 {
		t.Fatalf("transition receipts after rollback = (%d,%v)", transitionReceipts, err)
	}

	redecisionAt := deadline.Add(time.Nanosecond)
	afterDeadline := peerInboxSemanticCommitSpec(t, fixture, claim, redecisionAt)
	if len(afterDeadline.Responses) != 2 ||
		afterDeadline.Responses[0].Event().Type() != model.EventReviewExpired ||
		afterDeadline.Responses[1].Event().Type() != model.EventReviewAcceptRejected {
		t.Fatalf("post-deadline responses = %#v", afterDeadline.Responses)
	}
	result, err := fixture.store.CommitPeerInboxSemantic(context.Background(), afterDeadline,
		redecisionAt.Add(time.Nanosecond))
	if err != nil || result.Status() != model.InboxRejected || result.Diagnostic() != "work_expired" ||
		len(result.ResponseEventIDs()) != 2 {
		t.Fatalf("post-deadline CommitPeerInboxSemantic() = (%#v,%v)", result, err)
	}
	receipt, ok := result.ReceiptEventID()
	if !ok || receipt != afterDeadline.Responses[1].Event().ID() {
		t.Fatalf("post-deadline receipt = (%s,%t)", receipt, ok)
	}
	durable, err = fixture.store.GetReviewWork(context.Background(), work.Ref())
	if err != nil || durable.Version() != 2 || durable.State() != model.WorkExpired ||
		durable.UpdatedBy() != afterDeadline.Responses[0].Event().ID() {
		t.Fatalf("expired Work = (%#v,%v)", durable, err)
	}
	afterOrigin, afterChannel = peerInboxSemanticLocalHeads(t, fixture.store,
		fixture.channel.Channel().ID(), fixture.channel.Owner())
	if afterOrigin != beforeOrigin+2 || afterChannel != beforeChannel+2 {
		t.Fatalf("two-response local heads = (%d,%d), want (%d,%d)", afterOrigin,
			afterChannel, beforeOrigin+2, beforeChannel+2)
	}
	var eventCount int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	mustExec(t, fixture.store, `UPDATE profiles SET enabled=0,updated_at=? WHERE profile_id=?`,
		storeTime(redecisionAt.Add(time.Second)), model.TeamworkProfileID().String())
	path := fixture.store.Path()
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.store = nil
	restarted, err := OpenExisting(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	replayAt := claim.Fence().leaseUntil.Add(time.Hour)
	replay, err := restarted.CommitPeerInboxSemantic(context.Background(), afterDeadline, replayAt)
	if err != nil || replay.Changed() || !replay.Replayed() ||
		replay.Decision().String() != result.Decision().String() {
		t.Fatalf("post-deadline restart replay = (%#v,%v)", replay, err)
	}
	replayOrigin, replayChannel := peerInboxSemanticLocalHeads(t, restarted,
		fixture.channel.Channel().ID(), fixture.channel.Owner())
	var replayEvents int
	if err := restarted.db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&replayEvents); err != nil ||
		replayEvents != eventCount || replayOrigin != afterOrigin || replayChannel != afterChannel {
		t.Fatalf("restart replay effects = events (%d,%v), heads (%d,%d)", replayEvents,
			err, replayOrigin, replayChannel)
	}
	swapped := afterDeadline
	swapped.Responses = append([]model.SignedPublication(nil), afterDeadline.Responses...)
	swapped.Responses[0], swapped.Responses[1] = swapped.Responses[1], swapped.Responses[0]
	if _, err := restarted.CommitPeerInboxSemantic(context.Background(), swapped,
		replayAt.Add(time.Nanosecond)); !errors.Is(err, ErrPeerInboxSemanticInvariant) {
		t.Fatalf("swapped response replay error = %v", err)
	}
	replayOrigin, replayChannel = peerInboxSemanticLocalHeads(t, restarted,
		fixture.channel.Channel().ID(), fixture.channel.Owner())
	if err := restarted.db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&replayEvents); err != nil ||
		replayEvents != eventCount || replayOrigin != afterOrigin || replayChannel != afterChannel {
		t.Fatalf("failed replay changed effects = events (%d,%v), heads (%d,%d)", replayEvents,
			err, replayOrigin, replayChannel)
	}
}

func TestCommitPeerInboxSemanticConflictHasNoLocalResponse(t *testing.T) {
	fixture, inboxID, readyAt := newReadyPeerInboxSemantic(t, "semantic-zero-response")
	claim := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-zero-response-worker",
		readyAt.Add(time.Second))
	decisionAt := readyAt.Add(2 * time.Second)
	beforeOrigin, beforeChannel := peerInboxSemanticLocalHeads(t, fixture.store,
		fixture.channel.Channel().ID(), fixture.channel.Owner())
	result, err := fixture.store.CommitPeerInboxSemantic(context.Background(),
		peerInboxSemanticCommitSpec(t, fixture, claim, decisionAt), decisionAt)
	if err != nil || result.Status() != model.InboxConflicted || result.Diagnostic() != "invalid_payload" ||
		len(result.ResponseEventIDs()) != 0 || result.Decision().IsZero() {
		t.Fatalf("zero-response conflict = (%#v,%v)", result, err)
	}
	if receipt, ok := result.ReceiptEventID(); ok || !receipt.IsZero() {
		t.Fatalf("zero-response receipt = (%s,%t)", receipt, ok)
	}
	assertPeerInboxSemanticTerminalProjection(t, fixture.store, inboxID,
		model.InboxConflicted, claim.ImportedEvent().ID(), model.EventID{})
	assertPeerInboxSemanticImportedEventHasNoGossip(t, fixture.store, claim.ImportedEvent().ID())
	afterOrigin, afterChannel := peerInboxSemanticLocalHeads(t, fixture.store,
		fixture.channel.Channel().ID(), fixture.channel.Owner())
	if afterOrigin != beforeOrigin || afterChannel != beforeChannel {
		t.Fatalf("zero-response local heads changed: (%d,%d) -> (%d,%d)",
			beforeOrigin, beforeChannel, afterOrigin, afterChannel)
	}
	var localGossip int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM gossip_publications`).Scan(&localGossip); err != nil ||
		localGossip != 0 {
		t.Fatalf("zero-response Gossip rows = (%d,%v)", localGossip, err)
	}
}

func TestCommitPeerInboxSemanticReceiptOnlyConsumesRenewalWithoutResponse(t *testing.T) {
	ctx := context.Background()
	fixture := newPeerInboxFixture(t, "semantic-receipt-only", 0)
	installPeerInboxSemanticLocalAuthority(t, fixture)
	request, initial, _ := peerInboxSemanticCurrentWorkPublication(t, fixture,
		"semantic-receipt-only", 1, 1)
	requestPut := fixture.put(t, request, fixture.at)
	requestReadyAt := fixture.at.Add(time.Second)
	markPeerInboxSemanticReady(t, fixture.store, requestPut.InboxID, requestReadyAt)
	requestClaim := mustClaimPeerInboxSemantic(t, fixture.store,
		"semantic-receipt-request-worker", requestReadyAt.Add(time.Second))
	acceptedAt := requestReadyAt.Add(2 * time.Second)
	requestSpec := peerInboxSemanticCommitSpec(t, fixture, requestClaim, acceptedAt)
	if _, err := fixture.store.CommitPeerInboxSemantic(ctx, requestSpec, acceptedAt); err != nil {
		t.Fatal(err)
	}
	if len(requestSpec.Responses) != 1 ||
		requestSpec.Responses[0].Event().Type() != model.EventReviewAccepted {
		t.Fatalf("accepted response = %#v", requestSpec.Responses)
	}
	accepted := requestSpec.Responses[0].Event()
	active, err := fixture.store.GetReviewWork(ctx, initial.Ref())
	if err != nil || active.State() != model.WorkActive || active.Version() != 2 {
		t.Fatalf("active Work = (%#v,%v)", active, err)
	}

	remote := fixture.remote.Identity()
	scope, err := model.NewEventScope(fixture.channel.Channel().ID(), remote.PeerID(),
		remote.OriginEpoch(), 2, 2, fixture.remote.Member().Head(),
		fixture.channel.Roster().Head(), active.Ref())
	if err != nil {
		t.Fatal(err)
	}
	audience, _ := model.NewAudience([]model.PeerID{fixture.channel.Owner().PeerID()})
	payload, err := model.JSONFrom(struct {
		DecisionRef string `json:"decision_ref"`
		Diagnostic  string `json:"diagnostic_code"`
		Iteration   uint8  `json:"iteration"`
		Status      string `json:"status"`
		WorkVersion uint64 `json:"work_version"`
	}{accepted.ID().String(), "applied", 1, "accepted", 1})
	if err != nil {
		t.Fatal(err)
	}
	outcomeID, _ := model.ParseEventID("event-semantic-receipt-only-outcome")
	outcomeAt := acceptedAt.Add(time.Second)
	outcome := fixture.signEvent(t, model.EventSpec{ID: outcomeID, Scope: scope,
		Source: model.EventSourceLocal, ActorPrincipal: "principal-semantic-remote",
		Type: model.EventReviewOutcome, Audience: audience, Summary: "semantic receipt only",
		Payload: payload, CausedBy: []model.EventKey{accepted.Key()},
		CreatedAt: outcomeAt, AcceptedAt: outcomeAt})
	putAt := outcomeAt.Add(time.Second)
	outcomePut := fixture.put(t, outcome, putAt)
	readyAt := putAt.Add(time.Second)
	markPeerInboxSemanticReady(t, fixture.store, outcomePut.InboxID, readyAt)
	claimAt := readyAt.Add(time.Second)
	claim := mustClaimPeerInboxSemantic(t, fixture.store,
		"semantic-receipt-only-worker", claimAt)
	renewAt := claimAt.Add(time.Second)
	renewed, err := fixture.store.RenewPeerInboxSemantic(ctx,
		RenewPeerInboxSemanticSpec{Fence: claim.Fence(), At: renewAt})
	if err != nil || !renewed.Changed() || renewed.Replayed() {
		t.Fatalf("semantic renewal = (%#v,%v)", renewed, err)
	}
	assertPeerInboxSemanticTransitionReceipt(t, fixture.store, outcomePut.InboxID,
		"renew", claim.Fence().Attempt(), claim.Fence().LeaseUntil(),
		renewed.Fence().LeaseUntil())
	beforeOrigin, beforeChannel := peerInboxSemanticLocalHeads(t, fixture.store,
		fixture.channel.Channel().ID(), fixture.channel.Owner())
	var beforeEvents int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&beforeEvents); err != nil {
		t.Fatal(err)
	}
	decisionAt := renewAt.Add(time.Second)
	spec := peerInboxSemanticCommitSpecWithFence(t, fixture, claim, renewed.Fence(), decisionAt)
	result, err := fixture.store.CommitPeerInboxSemantic(ctx, spec, decisionAt)
	if err != nil || !result.Changed() || result.Replayed() ||
		result.Status() != model.InboxAccepted || result.Diagnostic() != "" ||
		len(result.ResponseEventIDs()) != 0 {
		t.Fatalf("receipt-only commit = (%#v,%v)", result, err)
	}
	if receipt, ok := result.ReceiptEventID(); ok || !receipt.IsZero() {
		t.Fatalf("receipt-only local receipt = (%s,%t)", receipt, ok)
	}
	var transitionReceipts, afterEvents int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM peer_inbox_semantic_transition_receipts
		WHERE inbox_id=?`, outcomePut.InboxID.String()).Scan(&transitionReceipts); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&afterEvents); err != nil {
		t.Fatal(err)
	}
	afterOrigin, afterChannel := peerInboxSemanticLocalHeads(t, fixture.store,
		fixture.channel.Channel().ID(), fixture.channel.Owner())
	durable, err := fixture.store.GetReviewWork(ctx, initial.Ref())
	if transitionReceipts != 0 || afterEvents != beforeEvents+1 ||
		afterOrigin != beforeOrigin || afterChannel != beforeChannel || err != nil ||
		!samePeerInboxSemanticWork(durable, active) {
		t.Fatalf("receipt-only effects = receipts %d events %d->%d heads (%d,%d)->(%d,%d) Work (%#v,%v)",
			transitionReceipts, beforeEvents, afterEvents, beforeOrigin, beforeChannel,
			afterOrigin, afterChannel, durable, err)
	}
	assertPeerInboxSemanticTerminalProjection(t, fixture.store, outcomePut.InboxID,
		model.InboxAccepted, outcomeID, model.EventID{})
	assertPeerInboxSemanticImportedEventHasNoGossip(t, fixture.store, outcomeID)
	replay, err := fixture.store.CommitPeerInboxSemantic(ctx, spec, decisionAt.Add(time.Hour))
	if err != nil || replay.Changed() || !replay.Replayed() ||
		replay.Decision().String() != result.Decision().String() {
		t.Fatalf("receipt-only replay = (%#v,%v)", replay, err)
	}
}

func TestCommitPeerInboxSemanticMaterializesImportedReplicaArtifactsAtomically(t *testing.T) {
	t.Run("commit", func(t *testing.T) {
		fixture, inboxID, root, readyAt := newReadyPeerInboxSemanticArtifact(t,
			"semantic-imported-replica")
		claim := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-replica-worker",
			readyAt.Add(time.Second))
		decisionAt := readyAt.Add(2 * time.Second)
		result, err := fixture.store.CommitPeerInboxSemantic(context.Background(),
			peerInboxSemanticCommitSpec(t, fixture, claim, decisionAt), decisionAt)
		if err != nil || result.ImportedEventID() != claim.ImportedEvent().ID() || result.Changed() == false {
			t.Fatalf("replica semantic commit = (%#v,%v)", result, err)
		}
		var origin, relation, created string
		var runID, operationID sql.NullString
		if err := fixture.store.db.QueryRow(`SELECT producer_origin_peer_id,local_agent_run_id,
			operation_id,relation,created_at FROM artifact_provenance
			WHERE root_digest=? AND producer_event_id=?`, root.String(),
			claim.ImportedEvent().ID().String()).Scan(&origin, &runID, &operationID,
			&relation, &created); err != nil {
			t.Fatal(err)
		}
		if origin != claim.ImportedEvent().Scope().OriginPeerID().String() || runID.Valid ||
			operationID.Valid || relation != string(model.ProvenanceReplica) ||
			created != storeTime(claim.ImportedEvent().AcceptedAt()) {
			t.Fatalf("replica provenance = (%q,%#v,%#v,%q,%q)", origin, runID,
				operationID, relation, created)
		}
		assertPeerInboxSemanticArtifactOwnerPin(t, fixture.store, root, "inbox",
			inboxID.String(), true, "")
		assertPeerInboxSemanticArtifactOwnerPin(t, fixture.store, root, "event",
			claim.ImportedEvent().ID().String(), false,
			storeTime(claim.ImportedEvent().AcceptedAt()))
		assertPeerInboxSemanticImportedEventHasNoGossip(t, fixture.store,
			claim.ImportedEvent().ID())
	})

	t.Run("terminal rollback", func(t *testing.T) {
		fixture, inboxID, root, readyAt := newReadyPeerInboxSemanticArtifact(t,
			"semantic-imported-replica-rollback")
		claim := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-replica-rollback-worker",
			readyAt.Add(time.Second))
		mustExec(t, fixture.store, `CREATE TEMP TRIGGER reject_semantic_replica_terminal
			BEFORE UPDATE OF status ON peer_inbox WHEN NEW.status IN ('accepted','rejected','conflicted')
			BEGIN SELECT RAISE(ABORT,'injected replica terminal failure'); END`)
		decisionAt := readyAt.Add(2 * time.Second)
		if _, err := fixture.store.CommitPeerInboxSemantic(context.Background(),
			peerInboxSemanticCommitSpec(t, fixture, claim, decisionAt),
			decisionAt); err == nil {
			t.Fatal("injected replica terminal failure was accepted")
		}
		assertPeerInboxSemanticState(t, fixture.store, inboxID, "processing",
			claim.Fence().attempt, "", true)
		assertPeerInboxSemanticNoDomainMutation(t, fixture.store, claim.ImportedEvent().ID())
		var provenance, eventPins int
		if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM artifact_provenance
			WHERE root_digest=? AND producer_event_id=?`, root.String(),
			claim.ImportedEvent().ID().String()).Scan(&provenance); err != nil {
			t.Fatal(err)
		}
		if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM artifact_pins
			WHERE root_digest=? AND owner_kind='event' AND owner_id=?`, root.String(),
			claim.ImportedEvent().ID().String()).Scan(&eventPins); err != nil {
			t.Fatal(err)
		}
		if provenance != 0 || eventPins != 0 {
			t.Fatalf("rolled back replica effects = provenance %d event pins %d",
				provenance, eventPins)
		}
		assertPeerInboxSemanticArtifactOwnerPin(t, fixture.store, root, "inbox",
			inboxID.String(), true, "")
	})
}

func TestCommitPeerInboxSemanticDeliveryReadyPreservesMixedArtifactsAcrossReplay(t *testing.T) {
	ctx := context.Background()
	fixture := newPeerInboxFixture(t, "semantic-delivery-mixed-artifacts", 0)
	installPeerInboxSemanticLocalAuthority(t, fixture)

	acceptRequest, initialWork, _ := peerInboxSemanticCurrentWorkPublication(t, fixture,
		"semantic-delivery-mixed-artifacts", 1, 1)
	acceptPut := fixture.put(t, acceptRequest, fixture.at)
	acceptReadyAt := fixture.at.Add(time.Second)
	markPeerInboxSemanticReady(t, fixture.store, acceptPut.InboxID, acceptReadyAt)
	acceptClaim := mustClaimPeerInboxSemantic(t, fixture.store,
		"semantic-delivery-mixed-accept-worker", acceptReadyAt.Add(time.Second))
	acceptAt := acceptReadyAt.Add(2 * time.Second)
	acceptSpec := peerInboxSemanticCommitSpec(t, fixture, acceptClaim, acceptAt)
	acceptResult, err := fixture.store.CommitPeerInboxSemantic(ctx, acceptSpec, acceptAt)
	if err != nil || !acceptResult.Changed() || acceptResult.Replayed() ||
		acceptResult.Status() != model.InboxAccepted || len(acceptSpec.Responses) != 1 ||
		acceptSpec.Responses[0].Event().Type() != model.EventReviewAccepted {
		t.Fatalf("accept semantic commit = (%#v,%v), responses %#v",
			acceptResult, err, acceptSpec.Responses)
	}
	accepted := acceptSpec.Responses[0].Event()
	active, err := fixture.store.GetReviewWork(ctx, initialWork.Ref())
	if err != nil || active.Version() != 2 || active.Iteration() != 1 ||
		active.State() != model.WorkActive || active.UpdatedBy() != accepted.ID() ||
		!active.UpdatedAt().Equal(acceptAt) {
		t.Fatalf("active Work = (%#v,%v)", active, err)
	}

	closureAt := fixture.at.Add(-5 * time.Second)
	producedClosure, producedRoot, _ := newArtifactSourceClosure(t,
		"semantic-mixed-produced", []byte("newly produced mixed delivery Artifact"), closureAt)
	referencedClosure, referencedRoot, _ := newArtifactSourceClosure(t,
		"semantic-mixed-referenced", []byte("previously accepted reusable Artifact"), closureAt)
	seedEvent := seedPeerInboxSemanticReusableReplicaRoot(t, fixture, referencedClosure,
		referencedRoot, 90, 90, "semantic-mixed-reference-seed")
	assertPeerInboxSemanticExactReplicaProvenance(t, fixture.store, seedEvent,
		referencedRoot.RootDigest)

	deliveryAt := acceptAt.Add(time.Second)
	delivery := peerInboxSemanticMixedDeliveryReadyPublication(t, fixture, active, accepted,
		2, 2, producedRoot.RootDigest, referencedRoot.RootDigest, deliveryAt)
	deliveryPutAt := deliveryAt.Add(time.Second)
	deliveryPut := fixture.put(t, delivery, deliveryPutAt)
	artifactClaim := mustClaimPeerInboxArtifact(t, fixture.store,
		"semantic-mixed-artifact-worker", deliveryPutAt.Add(time.Second))
	stageAt := deliveryPutAt.Add(2 * time.Second)
	combined := combinePeerInboxArtifactClosures(t, producedClosure, referencedClosure)
	if _, err := fixture.store.StagePeerInboxArtifactClosure(ctx,
		StagePeerInboxArtifactClosureSpec{Fence: artifactClaim.Fence(), Closure: combined,
			At: stageAt}); err != nil {
		t.Fatal(err)
	}
	artifactReadyAt := stageAt.Add(time.Second)
	if _, err := fixture.store.MarkPeerInboxArtifactReady(ctx,
		MarkPeerInboxArtifactReadySpec{Fence: artifactClaim.Fence(), At: artifactReadyAt}); err != nil {
		t.Fatal(err)
	}

	semanticClaim := mustClaimPeerInboxSemantic(t, fixture.store,
		"semantic-mixed-delivery-worker", artifactReadyAt.Add(time.Second))
	decisionAt := artifactReadyAt.Add(2 * time.Second)
	spec := peerInboxSemanticCommitSpec(t, fixture, semanticClaim, decisionAt)
	imported := semanticClaim.ImportedEvent()
	if imported.Type() != model.EventReviewDeliveryReady || len(imported.Artifacts()) != 2 {
		t.Fatalf("imported mixed delivery = %#v", imported)
	}
	roles := map[model.ArtifactRole]int{}
	for _, ref := range imported.Artifacts() {
		roles[ref.Role()]++
	}
	if roles[model.ArtifactProduced] != 1 || roles[model.ArtifactReferenced] != 1 {
		t.Fatalf("imported mixed roles = %#v", roles)
	}
	if len(spec.Responses) != 1 ||
		spec.Responses[0].Event().Type() != model.EventReviewDelivered {
		t.Fatalf("delivery response = %#v", spec.Responses)
	}
	response := spec.Responses[0].Event()
	if len(response.Artifacts()) != len(imported.Artifacts()) {
		t.Fatalf("response Artifact count = %d, want %d", len(response.Artifacts()),
			len(imported.Artifacts()))
	}
	for index, ref := range response.Artifacts() {
		if ref.Role() != model.ArtifactReferenced ||
			ref.RootDigest() != imported.Artifacts()[index].RootDigest() {
			t.Fatalf("response Artifact %d = (%s,%s), source (%s,%s)", index,
				ref.RootDigest(), ref.Role(), imported.Artifacts()[index].RootDigest(),
				imported.Artifacts()[index].Role())
		}
	}

	before := readPeerInboxSemanticMixedArtifactCounts(t, fixture.store)
	producedProvenanceBefore := peerInboxSemanticRootProvenanceCount(t, fixture.store,
		producedRoot.RootDigest)
	referencedProvenanceBefore := peerInboxSemanticRootProvenanceCount(t, fixture.store,
		referencedRoot.RootDigest)
	if producedProvenanceBefore != 0 || referencedProvenanceBefore != 1 {
		t.Fatalf("pre-commit provenance counts = produced %d referenced %d",
			producedProvenanceBefore, referencedProvenanceBefore)
	}

	result, err := fixture.store.CommitPeerInboxSemantic(ctx, spec, decisionAt)
	if err != nil || !result.Changed() || result.Replayed() ||
		result.Status() != model.InboxAccepted || result.ImportedEventID() != imported.ID() {
		t.Fatalf("mixed delivery semantic commit = (%#v,%v)", result, err)
	}
	responseIDs := result.ResponseEventIDs()
	receipt, hasReceipt := result.ReceiptEventID()
	if len(responseIDs) != 1 || responseIDs[0] != response.ID() ||
		!hasReceipt || receipt != response.ID() || result.Decision().IsZero() {
		t.Fatalf("mixed delivery response/receipt = (%#v,%s,%t,%s)", responseIDs,
			receipt, hasReceipt, result.Decision().String())
	}

	wantCounts := before
	wantCounts.events += 2
	wantCounts.pins += 10
	wantCounts.provenance++
	wantCounts.handlings++
	wantCounts.gossip++
	wantCounts.deliveries++
	committedCounts := readPeerInboxSemanticMixedArtifactCounts(t, fixture.store)
	if committedCounts != wantCounts {
		t.Fatalf("mixed delivery commit counts = %#v, want %#v", committedCounts, wantCounts)
	}

	roots := []model.Digest{producedRoot.RootDigest, referencedRoot.RootDigest}
	handlingID, err := peerInboxSemanticHandlingID(response.ID())
	if err != nil {
		t.Fatal(err)
	}
	deliveryID := deterministicDeliveryID(response.ID(), fixture.remote.Identity().PeerID())
	assertPeerInboxSemanticExactOwnerPins(t, fixture.store, "inbox",
		deliveryPut.InboxID.String(), roots, stageAt)
	assertPeerInboxSemanticExactOwnerPins(t, fixture.store, "event",
		imported.ID().String(), roots, imported.AcceptedAt())
	assertPeerInboxSemanticExactOwnerPins(t, fixture.store, "event",
		response.ID().String(), roots, decisionAt)
	assertPeerInboxSemanticExactOwnerPins(t, fixture.store, "publication",
		response.ID().String(), roots, decisionAt)
	assertPeerInboxSemanticExactOwnerPins(t, fixture.store, "delivery", deliveryID,
		roots, decisionAt)
	assertPeerInboxSemanticExactOwnerPins(t, fixture.store, "handling",
		handlingID.String(), roots, decisionAt)
	assertPeerInboxSemanticExactReplicaProvenance(t, fixture.store, imported,
		producedRoot.RootDigest)
	assertPeerInboxSemanticNoProducerProvenance(t, fixture.store, response.ID())
	if got := peerInboxSemanticRootProvenanceCount(t, fixture.store,
		producedRoot.RootDigest); got != producedProvenanceBefore+1 {
		t.Fatalf("produced root provenance count = %d, want %d", got,
			producedProvenanceBefore+1)
	}
	if got := peerInboxSemanticRootProvenanceCount(t, fixture.store,
		referencedRoot.RootDigest); got != referencedProvenanceBefore {
		t.Fatalf("referenced root provenance count = %d, want %d", got,
			referencedProvenanceBefore)
	}
	assertPeerInboxSemanticMixedDeliveryWork(t, fixture.store, active, response, decisionAt)
	assertPeerInboxSemanticPendingHandling(t, fixture.store, handlingID, response.ID(), decisionAt)
	assertPeerInboxSemanticImportedEventHasNoGossip(t, fixture.store, imported.ID())
	committedWork, err := fixture.store.GetReviewWork(ctx, active.Ref())
	if err != nil {
		t.Fatal(err)
	}
	committedOriginHead, committedChannelHead := peerInboxSemanticLocalHeads(t, fixture.store,
		fixture.channel.Channel().ID(), fixture.channel.Owner())

	path := fixture.store.Path()
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.store = nil
	restarted, err := OpenExisting(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	replay, err := restarted.CommitPeerInboxSemantic(ctx, spec, decisionAt.Add(time.Hour))
	if err != nil || replay.Changed() || !replay.Replayed() ||
		replay.Status() != result.Status() || replay.Diagnostic() != result.Diagnostic() ||
		replay.ImportedEventID() != result.ImportedEventID() ||
		replay.Decision().String() != result.Decision().String() {
		t.Fatalf("mixed delivery restart replay = (%#v,%v)", replay, err)
	}
	replayResponses := replay.ResponseEventIDs()
	replayReceipt, replayHasReceipt := replay.ReceiptEventID()
	if len(replayResponses) != 1 || replayResponses[0] != response.ID() ||
		!replayHasReceipt || replayReceipt != response.ID() {
		t.Fatalf("mixed delivery replay response/receipt = (%#v,%s,%t)",
			replayResponses, replayReceipt, replayHasReceipt)
	}
	if replayCounts := readPeerInboxSemanticMixedArtifactCounts(t, restarted); replayCounts != committedCounts {
		t.Fatalf("restart replay counts = %#v, want %#v", replayCounts, committedCounts)
	}
	replayWork, err := restarted.GetReviewWork(ctx, active.Ref())
	if err != nil || !samePeerInboxSemanticWork(replayWork, committedWork) {
		t.Fatalf("restart replay Work = (%#v,%v), want %#v", replayWork, err, committedWork)
	}
	replayOriginHead, replayChannelHead := peerInboxSemanticLocalHeads(t, restarted,
		fixture.channel.Channel().ID(), fixture.channel.Owner())
	if replayOriginHead != committedOriginHead || replayChannelHead != committedChannelHead {
		t.Fatalf("restart replay local heads = (%d,%d), want (%d,%d)", replayOriginHead,
			replayChannelHead, committedOriginHead, committedChannelHead)
	}
	assertPeerInboxSemanticExactOwnerPins(t, restarted, "inbox",
		deliveryPut.InboxID.String(), roots, stageAt)
	assertPeerInboxSemanticExactOwnerPins(t, restarted, "event",
		imported.ID().String(), roots, imported.AcceptedAt())
	assertPeerInboxSemanticExactOwnerPins(t, restarted, "event",
		response.ID().String(), roots, decisionAt)
	assertPeerInboxSemanticExactOwnerPins(t, restarted, "publication",
		response.ID().String(), roots, decisionAt)
	assertPeerInboxSemanticExactOwnerPins(t, restarted, "delivery", deliveryID,
		roots, decisionAt)
	assertPeerInboxSemanticExactOwnerPins(t, restarted, "handling",
		handlingID.String(), roots, decisionAt)
	assertPeerInboxSemanticExactReplicaProvenance(t, restarted, imported,
		producedRoot.RootDigest)
	assertPeerInboxSemanticNoProducerProvenance(t, restarted, response.ID())
	assertPeerInboxSemanticPendingHandling(t, restarted, handlingID, response.ID(), decisionAt)
}

func TestCommitPeerInboxSemanticReplayRejectsExpandedOrMalformedEffects(t *testing.T) {
	t.Run("extra imported Event pin", func(t *testing.T) {
		fixture, spec, claim, _ := committedPeerInboxSemanticReplicaFixture(t,
			"semantic-replay-extra-imported-pin")
		closure, extraRoot, _ := newArtifactSourceClosure(t, "semantic-extra-pin",
			[]byte("semantic extra pin"), spec.Plan.DecisionAt())
		if _, err := fixture.store.CheckpointVerifiedArtifactClosure(context.Background(),
			closure); err != nil {
			t.Fatal(err)
		}
		mustExec(t, fixture.store, `INSERT INTO artifact_pins(root_digest,owner_kind,owner_id,
			created_at) VALUES(?,'event',?,?)`, extraRoot.RootDigest.String(),
			claim.ImportedEvent().ID().String(), storeTime(claim.ImportedEvent().AcceptedAt()))
		if _, err := fixture.store.CommitPeerInboxSemantic(context.Background(), spec,
			spec.Plan.DecisionAt().Add(time.Hour)); !errors.Is(err, ErrPeerInboxSemanticInvariant) {
			t.Fatalf("extra imported Event pin replay error = %v", err)
		}
	})

	t.Run("extra imported Event provenance", func(t *testing.T) {
		fixture, spec, claim, _ := committedPeerInboxSemanticReplicaFixture(t,
			"semantic-replay-extra-imported-provenance")
		closure, extraRoot, _ := newArtifactSourceClosure(t, "semantic-extra-provenance",
			[]byte("semantic extra provenance"), spec.Plan.DecisionAt())
		if _, err := fixture.store.CheckpointVerifiedArtifactClosure(context.Background(),
			closure); err != nil {
			t.Fatal(err)
		}
		mustExec(t, fixture.store, `INSERT INTO artifact_provenance(root_digest,
			producer_event_id,producer_origin_peer_id,relation,created_at)
			VALUES(?,?,?,'replica',?)`, extraRoot.RootDigest.String(),
			claim.ImportedEvent().ID().String(),
			claim.ImportedEvent().Scope().OriginPeerID().String(),
			storeTime(claim.ImportedEvent().AcceptedAt()))
		if _, err := fixture.store.CommitPeerInboxSemantic(context.Background(), spec,
			spec.Plan.DecisionAt().Add(time.Hour)); !errors.Is(err, ErrPeerInboxSemanticInvariant) {
			t.Fatalf("extra imported provenance replay error = %v", err)
		}
	})

	t.Run("extra response delivery", func(t *testing.T) {
		fixture := newPeerInboxObserverFixture(t, "semantic-replay-extra-delivery")
		installPeerInboxSemanticLocalAuthority(t, fixture)
		publication, _, _ := peerInboxSemanticCurrentWorkPublication(t, fixture,
			"semantic-replay-extra-delivery", 1, 1)
		put := fixture.put(t, publication, fixture.at)
		readyAt := fixture.at.Add(time.Second)
		markPeerInboxSemanticReady(t, fixture.store, put.InboxID, readyAt)
		claim := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-extra-delivery-worker",
			readyAt.Add(time.Second))
		spec := peerInboxSemanticCommitSpec(t, fixture, claim, readyAt.Add(2*time.Second))
		if _, err := fixture.store.CommitPeerInboxSemantic(context.Background(), spec,
			spec.Plan.DecisionAt()); err != nil {
			t.Fatal(err)
		}
		response := spec.Responses[0].Event()
		extraTarget := fixture.observer.Identity().PeerID()
		mustExec(t, fixture.store, `DROP TRIGGER peer_deliveries_binding_ready_insert`)
		mustExec(t, fixture.store, `INSERT INTO peer_deliveries(delivery_id,channel_id,
			target_peer_id,event_id,status,created_at,updated_at) VALUES(?,?,?,?,'pending',?,?)`,
			deterministicDeliveryID(response.ID(), extraTarget),
			response.Scope().ChannelID().String(), extraTarget.String(), response.ID().String(),
			storeTime(response.AcceptedAt()), storeTime(response.AcceptedAt()))
		if _, err := fixture.store.CommitPeerInboxSemantic(context.Background(), spec,
			spec.Plan.DecisionAt().Add(time.Hour)); !errors.Is(err, ErrPeerInboxSemanticInvariant) {
			t.Fatalf("extra response delivery replay error = %v", err)
		}
	})

	t.Run("malformed later Work", func(t *testing.T) {
		fixture := newPeerInboxFixture(t, "semantic-replay-malformed-work", 0)
		installPeerInboxSemanticLocalAuthority(t, fixture)
		publication, work, _ := peerInboxSemanticCurrentWorkPublication(t, fixture,
			"semantic-replay-malformed-work", 1, 1)
		put := fixture.put(t, publication, fixture.at)
		readyAt := fixture.at.Add(time.Second)
		markPeerInboxSemanticReady(t, fixture.store, put.InboxID, readyAt)
		claim := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-malformed-work-worker",
			readyAt.Add(time.Second))
		spec := peerInboxSemanticCommitSpec(t, fixture, claim, readyAt.Add(2*time.Second))
		if _, err := fixture.store.CommitPeerInboxSemantic(context.Background(), spec,
			spec.Plan.DecisionAt()); err != nil {
			t.Fatal(err)
		}
		mustExec(t, fixture.store, `UPDATE works SET version=version+1,state='REWORK'
			WHERE home_peer_id=? AND work_id=?`, work.Ref().HomePeerID().String(),
			work.Ref().WorkID().String())
		if _, err := fixture.store.CommitPeerInboxSemantic(context.Background(), spec,
			spec.Plan.DecisionAt().Add(time.Hour)); !errors.Is(err, ErrPeerInboxSemanticInvariant) {
			t.Fatalf("malformed later Work replay error = %v", err)
		}
	})

	t.Run("later Work on disjoint causal branch", func(t *testing.T) {
		fixture := newPeerInboxFixture(t, "semantic-replay-disjoint-work", 0)
		installPeerInboxSemanticLocalAuthority(t, fixture)
		publication, work, offered := peerInboxSemanticCurrentWorkPublication(t, fixture,
			"semantic-replay-disjoint-work", 1, 1)
		put := fixture.put(t, publication, fixture.at)
		readyAt := fixture.at.Add(time.Second)
		markPeerInboxSemanticReady(t, fixture.store, put.InboxID, readyAt)
		claim := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-disjoint-work-worker",
			readyAt.Add(time.Second))
		spec := peerInboxSemanticCommitSpec(t, fixture, claim, readyAt.Add(2*time.Second))
		if _, err := fixture.store.CommitPeerInboxSemantic(context.Background(), spec,
			spec.Plan.DecisionAt()); err != nil {
			t.Fatal(err)
		}
		branchAt := installPeerInboxSemanticDisjointLaterWork(t, fixture, spec, work, offered)
		if _, err := fixture.store.CommitPeerInboxSemantic(context.Background(), spec,
			branchAt.Add(time.Hour)); !errors.Is(err, ErrPeerInboxSemanticInvariant) ||
			!strings.Contains(err.Error(), "does not descend from semantic effect") {
			t.Fatalf("disjoint later Work replay error = %v", err)
		}
	})
}

func installPeerInboxSemanticDisjointLaterWork(t *testing.T, fixture peerInboxFixture,
	spec CommitPeerInboxSemanticSpec, work model.ReviewWork, offered model.Event,
) time.Time {
	t.Helper()
	active, err := fixture.store.GetReviewWork(context.Background(), work.Ref())
	if err != nil || active.Version() != 2 || active.State() != model.WorkActive {
		t.Fatalf("semantic Work = (%#v,%v)", active, err)
	}
	audience, err := model.NewAudience([]model.PeerID{fixture.remote.Identity().PeerID()})
	if err != nil {
		t.Fatal(err)
	}
	scope, err := fixture.store.PrepareLocalAdmission(context.Background(),
		active.ChannelID(), audience, 1)
	if err != nil {
		t.Fatal(err)
	}
	eventScope, err := scope.EventScope(0, active.Ref())
	if err != nil {
		t.Fatal(err)
	}
	payload, err := model.NewJSON([]byte(`{"iteration":1,"work_version":2}`))
	if err != nil {
		t.Fatal(err)
	}
	eventID, err := model.ParseEventID("event-inbox-semantic-replay-disjoint-delivered")
	if err != nil {
		t.Fatal(err)
	}
	branchAt := spec.Plan.DecisionAt().Add(time.Second)
	branch := fixture.signEventAs(t, model.EventSpec{ID: eventID, Scope: eventScope,
		Source: model.EventSourceLocal, ActorPrincipal: scope.Profile().Principal(),
		Type: model.EventReviewDelivered, Audience: audience, Summary: "disjoint delivery",
		Payload: payload, CausedBy: []model.EventKey{offered.Key()},
		CreatedAt: branchAt, AcceptedAt: branchAt}, fixture.channel.Owner())
	nextSpec := active.Spec()
	nextSpec.Version++
	nextSpec.State = model.WorkDelivered
	nextSpec.StateData = branch.Event().Payload()
	nextSpec.UpdatedBy = branch.Event().ID()
	nextSpec.UpdatedAt = branchAt
	next, err := model.NewReviewWork(nextSpec)
	if err != nil {
		t.Fatal(err)
	}
	mutation, err := NewWorkTransition(next, active.Version(), active.State())
	if err != nil {
		t.Fatal(err)
	}
	tx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := insertAcceptedEvent(context.Background(), tx, branch); err != nil {
		t.Fatal(err)
	}
	if err := applyWorkMutation(context.Background(), tx, mutation, branch.Event()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	durable, err := fixture.store.GetReviewWork(context.Background(), work.Ref())
	if err != nil || !samePeerInboxSemanticWork(durable, next) {
		t.Fatalf("disjoint later Work = (%#v,%v), want %#v", durable, err, next)
	}
	return branchAt
}

type peerInboxSemanticMixedArtifactCounts struct {
	events     int
	pins       int
	provenance int
	works      int
	handlings  int
	gossip     int
	deliveries int
}

func peerInboxSemanticMixedDeliveryReadyPublication(t *testing.T, fixture peerInboxFixture,
	current model.ReviewWork, cause model.Event, originSequence, channelSequence uint64,
	produced, referenced model.Digest, at time.Time,
) model.SignedPublication {
	t.Helper()
	remote := fixture.remote.Identity()
	scope, err := model.NewEventScope(fixture.channel.Channel().ID(), remote.PeerID(),
		remote.OriginEpoch(), originSequence, channelSequence, fixture.remote.Member().Head(),
		fixture.channel.Roster().Head(), current.Ref())
	if err != nil {
		t.Fatal(err)
	}
	audience, err := model.NewAudience([]model.PeerID{fixture.channel.Owner().PeerID()})
	if err != nil {
		t.Fatal(err)
	}
	producedRef, err := model.NewArtifactRef(produced, model.ArtifactProduced)
	if err != nil {
		t.Fatal(err)
	}
	referencedRef, err := model.NewArtifactRef(referenced, model.ArtifactReferenced)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := model.NewJSON([]byte(
		`{"content":"mixed Artifact delivery","iteration":1,"work_version":2}`))
	if err != nil {
		t.Fatal(err)
	}
	eventID, err := model.ParseEventID("event-inbox-semantic-mixed-delivery-ready")
	if err != nil {
		t.Fatal(err)
	}
	return fixture.signEvent(t, model.EventSpec{ID: eventID, Scope: scope,
		Source: model.EventSourceLocal, ActorPrincipal: "principal-semantic-remote",
		Type: model.EventReviewDeliveryReady, Audience: audience,
		Summary: "mixed Artifact delivery ready", Payload: payload,
		Artifacts: []model.ArtifactRef{producedRef, referencedRef},
		CausedBy:  []model.EventKey{cause.Key()}, CreatedAt: at, AcceptedAt: at})
}

func seedPeerInboxSemanticReusableReplicaRoot(t *testing.T, fixture peerInboxFixture,
	closure VerifiedArtifactClosure, root VerifiedArtifactRoot, originSequence,
	channelSequence uint64, seed string,
) model.Event {
	t.Helper()
	if _, err := fixture.store.CheckpointVerifiedArtifactClosure(context.Background(),
		closure); err != nil {
		t.Fatal(err)
	}
	wire := peerInboxArtifactPublication(t, fixture, originSequence, channelSequence,
		seed, []model.Digest{root.RootDigest})
	imported, err := model.ProjectImportedPublication(&wire)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := insertAcceptedEvent(context.Background(), tx, imported); err != nil {
		t.Fatal(err)
	}
	if err := materializePeerInboxSemanticArtifacts(context.Background(), tx,
		imported.Event()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return imported.Event()
}

func readPeerInboxSemanticMixedArtifactCounts(t *testing.T,
	store *Store,
) peerInboxSemanticMixedArtifactCounts {
	t.Helper()
	var result peerInboxSemanticMixedArtifactCounts
	queries := []struct {
		table string
		value *int
	}{
		{"events", &result.events},
		{"artifact_pins", &result.pins},
		{"artifact_provenance", &result.provenance},
		{"works", &result.works},
		{"agent_handlings", &result.handlings},
		{"gossip_publications", &result.gossip},
		{"peer_deliveries", &result.deliveries},
	}
	for _, query := range queries {
		if err := store.db.QueryRow("SELECT COUNT(*) FROM " + query.table).Scan(query.value); err != nil {
			t.Fatalf("count %s: %v", query.table, err)
		}
	}
	return result
}

func assertPeerInboxSemanticExactOwnerPins(t *testing.T, store *Store, kind, owner string,
	roots []model.Digest, createdAt time.Time,
) {
	t.Helper()
	want := make([]string, len(roots))
	for index, root := range roots {
		want[index] = root.String()
	}
	sort.Strings(want)
	rows, err := store.db.Query(`SELECT root_digest,expires_at,created_at FROM artifact_pins
		WHERE owner_kind=? AND owner_id=? ORDER BY root_digest`, kind, owner)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var root, created string
		var expires sql.NullString
		if err := rows.Scan(&root, &expires, &created); err != nil {
			t.Fatal(err)
		}
		if expires.Valid || created != storeTime(createdAt) {
			t.Fatalf("%s/%s pin %s = expires %#v created %q, want permanent at %q",
				kind, owner, root, expires, created, storeTime(createdAt))
		}
		got = append(got, root)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("%s/%s pins = %#v, want %#v", kind, owner, got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("%s/%s pins = %#v, want %#v", kind, owner, got, want)
		}
	}
}

func assertPeerInboxSemanticExactReplicaProvenance(t *testing.T, store *Store,
	event model.Event, root model.Digest,
) {
	t.Helper()
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM artifact_provenance
		WHERE producer_event_id=?`, event.ID().String()).Scan(&count); err != nil || count != 1 {
		t.Fatalf("Event %s provenance count = (%d,%v), want one", event.ID(), count, err)
	}
	var gotRoot, origin, relation, created string
	var runID, operationID sql.NullString
	if err := store.db.QueryRow(`SELECT root_digest,producer_origin_peer_id,
		local_agent_run_id,operation_id,relation,created_at FROM artifact_provenance
		WHERE producer_event_id=?`, event.ID().String()).Scan(&gotRoot, &origin, &runID,
		&operationID, &relation, &created); err != nil {
		t.Fatal(err)
	}
	if gotRoot != root.String() || origin != event.Scope().OriginPeerID().String() ||
		runID.Valid || operationID.Valid || relation != string(model.ProvenanceReplica) ||
		created != storeTime(event.AcceptedAt()) {
		t.Fatalf("Event %s replica provenance = (%q,%q,%#v,%#v,%q,%q)", event.ID(),
			gotRoot, origin, runID, operationID, relation, created)
	}
}

func assertPeerInboxSemanticNoProducerProvenance(t *testing.T, store *Store,
	event model.EventID,
) {
	t.Helper()
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM artifact_provenance
		WHERE producer_event_id=?`, event.String()).Scan(&count); err != nil || count != 0 {
		t.Fatalf("Event %s producer provenance = (%d,%v), want zero", event, count, err)
	}
}

func peerInboxSemanticRootProvenanceCount(t *testing.T, store *Store,
	root model.Digest,
) int {
	t.Helper()
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM artifact_provenance
		WHERE root_digest=?`, root.String()).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func assertPeerInboxSemanticMixedDeliveryWork(t *testing.T, store *Store,
	predecessor model.ReviewWork, response model.Event, decisionAt time.Time,
) {
	t.Helper()
	durable, err := store.GetReviewWork(context.Background(), predecessor.Ref())
	if err != nil {
		t.Fatal(err)
	}
	if durable.Ref() != predecessor.Ref() || durable.ChannelID() != predecessor.ChannelID() ||
		durable.Participants() != predecessor.Participants() || durable.Version() != 3 ||
		durable.Iteration() != 1 || durable.DeadlineUnixNano() != predecessor.DeadlineUnixNano() ||
		durable.State() != model.WorkDelivered ||
		durable.StateData().String() != response.Payload().String() ||
		durable.UpdatedBy() != response.ID() || !durable.UpdatedAt().Equal(decisionAt) {
		t.Fatalf("mixed delivery Work = %#v, predecessor %#v response %#v",
			durable, predecessor, response)
	}
}

func assertPeerInboxSemanticPendingHandling(t *testing.T, store *Store,
	handling model.HandlingID, event model.EventID, at time.Time,
) {
	t.Helper()
	var eventText, status, availableAt, createdAt, updatedAt string
	var priority, attempts int
	if err := store.db.QueryRow(`SELECT event_id,status,priority,attempts,available_at,
		created_at,updated_at FROM agent_handlings WHERE handling_id=?`, handling.String()).
		Scan(&eventText, &status, &priority, &attempts, &availableAt, &createdAt,
			&updatedAt); err != nil {
		t.Fatal(err)
	}
	wantTime := storeTime(at)
	if eventText != event.String() || status != string(model.HandlingPending) || priority != 0 ||
		attempts != 0 || availableAt != wantTime || createdAt != wantTime || updatedAt != wantTime {
		t.Fatalf("pending Handling = (%q,%q,%d,%d,%q,%q,%q), want Event %s at %q",
			eventText, status, priority, attempts, availableAt, createdAt, updatedAt,
			event, wantTime)
	}
}

func peerInboxSemanticCommitSpec(t *testing.T, fixture peerInboxFixture,
	claim PeerInboxSemanticClaim, at time.Time,
) CommitPeerInboxSemanticSpec {
	t.Helper()
	policy := teamworkPlanForClaim(t, claim, at)
	plan := peerInboxSemanticStorePlan(t, claim, at, policy)
	intents := policy.Responses()
	if len(intents) == 0 {
		return CommitPeerInboxSemanticSpec{Fence: claim.Fence(), Plan: plan}
	}
	audience, err := model.NewAudience([]model.PeerID{claim.ImportedEvent().Scope().OriginPeerID()})
	if err != nil {
		t.Fatal(err)
	}
	scope, err := fixture.store.PrepareLocalAdmission(context.Background(),
		claim.ImportedEvent().Scope().ChannelID(), audience, uint8(len(intents)))
	if err != nil {
		t.Fatal(err)
	}
	responses := make([]model.SignedPublication, len(intents))
	for index, intent := range intents {
		var artifacts []model.ArtifactRef
		if intent.EventType() == model.EventReviewDelivered {
			for _, source := range claim.ImportedEvent().Artifacts() {
				ref, err := model.NewArtifactRef(source.RootDigest(), model.ArtifactReferenced)
				if err != nil {
					t.Fatal(err)
				}
				artifacts = append(artifacts, ref)
			}
		}
		responses[index] = peerInboxSemanticSignedResponse(t, fixture, claim, scope,
			intent, uint8(index), at, artifacts)
	}
	return CommitPeerInboxSemanticSpec{Fence: claim.Fence(), Plan: plan,
		Scope: scope, Responses: responses}
}

func peerInboxSemanticCommitSpecWithFence(t *testing.T, fixture peerInboxFixture,
	claim PeerInboxSemanticClaim, fence PeerInboxSemanticFence, at time.Time,
) CommitPeerInboxSemanticSpec {
	t.Helper()
	spec := peerInboxSemanticCommitSpec(t, fixture, claim, at)
	spec.Fence = fence
	return spec
}

func peerInboxSemanticSignedResponse(t *testing.T, fixture peerInboxFixture,
	claim PeerInboxSemanticClaim, scope LocalAdmissionScope, intent teamwork.LocalResponseIntent,
	ordinal uint8, at time.Time, artifacts []model.ArtifactRef,
) model.SignedPublication {
	t.Helper()
	eventID, err := PeerInboxSemanticResponseEventID(claim.DecisionSeed(), ordinal)
	if err != nil {
		t.Fatal(err)
	}
	eventScope, err := scope.EventScope(ordinal, claim.ImportedEvent().Scope().WorkRef())
	if err != nil {
		t.Fatal(err)
	}
	audience, _ := model.NewAudience([]model.PeerID{claim.ImportedEvent().Scope().OriginPeerID()})
	event, err := model.NewEvent(model.EventSpec{ID: eventID, Scope: eventScope,
		Source: model.EventSourceLocal, ActorPrincipal: scope.Profile().Principal(),
		Type: intent.EventType(), Audience: audience,
		Summary: peerInboxSemanticResponseSummary(intent.EventType()), Payload: intent.Payload(),
		Artifacts: artifacts, CausedBy: []model.EventKey{intent.Cause()}, CreatedAt: at, AcceptedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	body, err := model.NewPublicationBody(event)
	if err != nil {
		t.Fatal(err)
	}
	message, _ := model.PublicationSigningMessage(eventScope.ChannelID(), body.Digest())
	publication, err := model.AttachSignature(body,
		ed25519.Sign(ed25519Private(fixture.channel.Owner()), message))
	if err != nil {
		t.Fatal(err)
	}
	return publication
}

func installPeerInboxSemanticLocalAuthority(t *testing.T, fixture peerInboxFixture) {
	t.Helper()
	at := fixture.channel.Channel().CreatedAt()
	_, profile := signedBootstrapValues(t, fixture.channel.Owner(), "principal-semantic-local",
		"/workspace/semantic", at)
	spec := profile.Spec()
	spec.Enabled = true
	profile, err := model.NewProfile(spec)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := insertProfileRecord(context.Background(), tx, profile); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	reserved, err := fixture.store.ReserveOutboundChannelBaseline(context.Background(),
		ReserveOutboundChannelBaselineSpec{ChannelID: fixture.channel.Channel().ID(),
			TargetPeerID: fixture.remote.Identity().PeerID(), At: fixture.at})
	if err != nil {
		t.Fatal(err)
	}
	baseline := reserved.Baseline
	_, err = fixture.store.ConfirmOutboundChannelBaseline(context.Background(),
		ConfirmOutboundChannelBaselineSpec{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
			Ack: ChannelDataBaselineAck(baseline), At: fixture.at.Add(time.Nanosecond)})
	if err != nil {
		t.Fatal(err)
	}
}

func assertPeerInboxSemanticTerminalProjection(t *testing.T, store *Store,
	inboxID model.InboxID, status model.InboxStatus, imported, receipt model.EventID,
) {
	t.Helper()
	var gotStatus, localEvent string
	var receiptEvent sql.NullString
	var decision []byte
	if err := store.db.QueryRow(`SELECT status,local_event_id,receipt_event_id,decision_json
		FROM peer_inbox WHERE inbox_id=?`, inboxID.String()).
		Scan(&gotStatus, &localEvent, &receiptEvent, &decision); err != nil {
		t.Fatal(err)
	}
	wantReceipt := receipt.String()
	if receipt.IsZero() {
		wantReceipt = ""
	}
	if gotStatus != string(status) || localEvent != imported.String() ||
		receiptEvent.Valid != !receipt.IsZero() || receiptEvent.String != wantReceipt ||
		len(decision) == 0 {
		t.Fatalf("terminal Inbox = (%q,%q,%#v,%d)", gotStatus, localEvent, receiptEvent, len(decision))
	}
}

func peerInboxSemanticLocalHeads(t *testing.T, store *Store, channel model.ChannelID,
	identity interface {
		PeerID() model.PeerID
		OriginEpoch() model.OriginEpoch
	},
) (uint64, uint64) {
	t.Helper()
	var origin, channelSequence uint64
	if err := store.db.QueryRow(`SELECT n.next_origin_seq,p.source_head_channel_seq
		FROM node n JOIN publication_epochs p ON p.origin_peer_id=n.peer_id
		AND p.origin_epoch=n.origin_epoch WHERE n.singleton=1 AND p.channel_id=?`,
		channel.String()).Scan(&origin, &channelSequence); err != nil {
		t.Fatal(err)
	}
	if identity.PeerID().IsZero() || identity.OriginEpoch().IsZero() {
		t.Fatal("incomplete local identity")
	}
	return origin, channelSequence
}

func assertPeerInboxSemanticArtifactOwnerPin(t *testing.T, store *Store, root model.Digest,
	kind, owner string, permanent bool, createdAt string,
) {
	t.Helper()
	var expires sql.NullString
	var created string
	if err := store.db.QueryRow(`SELECT expires_at,created_at FROM artifact_pins
		WHERE root_digest=? AND owner_kind=? AND owner_id=?`, root.String(), kind, owner).
		Scan(&expires, &created); err != nil {
		t.Fatal(err)
	}
	if expires.Valid || permanent && createdAt != "" && created != createdAt ||
		!permanent && created != createdAt {
		t.Fatalf("Artifact %s pin = expires %#v created %q, want permanent=%t created=%q",
			kind, expires, created, permanent, createdAt)
	}
}

func committedPeerInboxSemanticReplicaFixture(t *testing.T, seed string,
) (peerInboxFixture, CommitPeerInboxSemanticSpec, PeerInboxSemanticClaim, model.Digest) {
	t.Helper()
	fixture, _, root, readyAt := newReadyPeerInboxSemanticArtifact(t, seed)
	claim := mustClaimPeerInboxSemantic(t, fixture.store, seed+"-worker", readyAt.Add(time.Second))
	spec := peerInboxSemanticCommitSpec(t, fixture, claim, readyAt.Add(2*time.Second))
	if _, err := fixture.store.CommitPeerInboxSemantic(context.Background(), spec,
		spec.Plan.DecisionAt()); err != nil {
		t.Fatal(err)
	}
	return fixture, spec, claim, root
}

func assertPeerInboxSemanticImportedEventHasNoGossip(t *testing.T, store *Store,
	event model.EventID,
) {
	t.Helper()
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM gossip_publications WHERE event_id=?`,
		event.String()).Scan(&count); err != nil || count != 0 {
		t.Fatalf("imported Gossip rows = (%d,%v)", count, err)
	}
}
