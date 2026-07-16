package store

import (
	"context"
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestValidateLocalCausalityRequiresExactDurableEventKey(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	insertNode(t, st.db)
	insertChannelAndEvent(t, st.db)
	tx, err := st.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	event := eventWithCauseIdentity(t, "peer-home", "epoch-one", "event-one")
	if err := validateLocalCausality(context.Background(), tx, event); err != nil {
		t.Fatalf("exact cause error = %v", err)
	}
	for _, identity := range [][3]string{
		{"peer-other", "epoch-one", "event-one"},
		{"peer-home", "epoch-other", "event-one"},
		{"peer-home", "epoch-one", "event-other"},
	} {
		if err := validateLocalCausality(context.Background(), tx,
			eventWithCauseIdentity(t, identity[0], identity[1], identity[2])); err == nil {
			t.Errorf("cause %v was accepted", identity)
		}
	}
}

func TestValidateLocalCausalSemanticsBindsCurrentWorkUpdate(t *testing.T) {
	fixture := newAcceptanceFixture(t, 2)
	_, authority := fixture.reserveOffer(t, "causal-current", nil)
	spec := fixture.offer(t, authority, "causal-current", fixture.reviewers, nil, nil)
	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), spec, fixture.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	current := spec.Items[0].Publication.Event()
	other := spec.Items[1].Publication.Event()
	valid := causalSuccessorEvent(t, current, model.EventReviewCancelled, "event-causal-cancel",
		`{"content":"cancel","iteration":1,"work_version":1}`, current.Key(), fixture.now.Add(2*time.Second))
	tx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := validateLocalCausalSemantics(context.Background(), tx, model.Operation{}, valid); err != nil {
		t.Fatalf("exact current Work cause error = %v", err)
	}
	wrong := causalSuccessorEvent(t, current, model.EventReviewCancelled, "event-causal-wrong",
		`{"content":"cancel","iteration":1,"work_version":1}`, other.Key(), fixture.now.Add(2*time.Second))
	if err := validateLocalCausalSemantics(context.Background(), tx, model.Operation{}, wrong); err == nil {
		t.Fatal("unrelated durable Work Event authorized cancel")
	}
	missing := valid.Spec()
	missing.ID, _ = model.ParseEventID("event-causal-missing")
	missing.CausedBy = nil
	withoutCause, _ := model.NewEvent(missing)
	if err := validateLocalCausalSemantics(context.Background(), tx, model.Operation{}, withoutCause); err == nil {
		t.Fatal("context action without exact current Work cause was accepted")
	}
}

func TestValidateLocalCausalSemanticsBindsControllerRequest(t *testing.T) {
	fixture := newAcceptanceFixture(t, 1)
	_, authority := fixture.reserveOffer(t, "causal-controller", nil)
	spec := fixture.offer(t, authority, "causal-controller", fixture.reviewers, nil, nil)
	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), spec, fixture.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	offered := spec.Items[0].Publication.Event()
	request := insertImportedCausalEvent(t, fixture, offered, model.EventReviewAcceptRequested,
		"event-causal-accept-request", `{"iteration":1,"note":"ready","work_version":1}`,
		fixture.now.Add(2*time.Second))
	accepted := causalSuccessorEvent(t, offered, model.EventReviewAccepted, "event-causal-accepted",
		`{"iteration":1,"work_version":1}`, request, fixture.now.Add(3*time.Second))
	tx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := validateLocalCausalSemantics(context.Background(), tx, model.Operation{}, accepted); err != nil {
		t.Fatalf("exact participant request cause error = %v", err)
	}
	wrong := causalSuccessorEvent(t, offered, model.EventReviewAccepted, "event-causal-accepted-wrong",
		`{"iteration":1,"work_version":1}`, offered.Key(), fixture.now.Add(3*time.Second))
	if err := validateLocalCausalSemantics(context.Background(), tx, model.Operation{}, wrong); err == nil {
		t.Fatal("home's own durable Event authorized a controller response")
	}
	outcome := causalSuccessorEvent(t, offered, model.EventReviewOutcome, "event-causal-outcome",
		`{"decision_ref":"decision-one","diagnostic_code":"fallback","iteration":1,"status":"rejected","work_version":1}`,
		request, fixture.now.Add(3*time.Second))
	if err := validateLocalCausalSemantics(context.Background(), tx, model.Operation{}, outcome); err != nil {
		t.Fatalf("fallback outcome exact source error = %v", err)
	}
}

func TestValidateLocalCausalSemanticsDoesNotCompareRemoteWallClock(t *testing.T) {
	fixture := newAcceptanceFixture(t, 1)
	_, authority := fixture.reserveOffer(t, "causal-remote-clock", nil)
	spec := fixture.offer(t, authority, "causal-remote-clock", fixture.reviewers, nil, nil)
	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), spec, fixture.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	offered := spec.Items[0].Publication.Event()
	request := insertImportedCausalEvent(t, fixture, offered, model.EventReviewAcceptRequested,
		"event-causal-remote-clock-request", `{"iteration":1,"note":"ready","work_version":1}`,
		fixture.now.Add(24*time.Hour))
	accepted := causalSuccessorEvent(t, offered, model.EventReviewAccepted,
		"event-causal-remote-clock-accepted", `{"iteration":1,"work_version":1}`,
		request, fixture.now.Add(2*time.Second))
	tx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := validateLocalCausalSemantics(context.Background(), tx, model.Operation{}, accepted); err != nil {
		t.Fatalf("durable remote cause with an ahead wall clock was rejected: %v", err)
	}
}

func TestValidateLocalCausalSemanticsSeparatesInitiateAndDerivedOffer(t *testing.T) {
	fixture := newAcceptanceFixture(t, 1)
	_, authority := fixture.reserveOffer(t, "causal-offer", nil)
	spec := fixture.offer(t, authority, "causal-offer", fixture.reviewers, nil, nil)
	offer := spec.Items[0].Publication.Event()
	tx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := validateLocalCausalSemantics(context.Background(), tx, model.Operation{}, offer); err != nil {
		t.Fatalf("contextless offer error = %v", err)
	}
	causedSpec := offer.Spec()
	causePeer, _ := model.ParsePeerID("peer-cause")
	causeEpoch, _ := model.ParseOriginEpoch("epoch-cause")
	causeID, _ := model.ParseEventID("event-cause")
	cause, _ := model.NewEventKey(causePeer, causeEpoch, causeID)
	causedSpec.CausedBy = []model.EventKey{cause}
	caused, _ := model.NewEvent(causedSpec)
	if err := validateLocalCausalSemantics(context.Background(), tx, model.Operation{}, caused); err == nil {
		t.Fatal("contextless offer with claimed parent causality was accepted")
	}
	contextHash := model.Sum([]byte("derived-context"))
	run, _ := model.ParseRunID("run-derived-causality")
	opID, _ := model.ParseOperationID("operation-derived-causality")
	lease := fixture.now.Add(time.Hour)
	operation, err := model.NewOperation(model.OperationSpec{ID: opID, ProfileID: model.TeamworkProfileID(),
		AgentRunID: run, ClientKeyHash: model.Sum([]byte("derived-key")), ContextHash: &contextHash,
		Kind: model.OperationTeamworkOffer, RequestDigest: model.Sum([]byte("derived-request")),
		Status: model.OperationStarted, LeaseOwner: "derived-owner", LeaseUntil: &lease, CreatedAt: fixture.now})
	if err != nil {
		t.Fatal(err)
	}
	if err := validateLocalCausalSemantics(context.Background(), tx, operation, caused); err != nil {
		t.Fatalf("context-bound offer shape error = %v", err)
	}
}

func eventWithCauseIdentity(t *testing.T, peerText, epochText, eventText string) model.Event {
	t.Helper()
	now := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	event := newCodecEvent(t, model.EventReviewOffered,
		`{"content":"review","deadline":"2026-07-17T13:00:00Z","iteration":1,"work_version":1}`, now)
	peer, _ := model.ParsePeerID(peerText)
	epoch, _ := model.ParseOriginEpoch(epochText)
	id, _ := model.ParseEventID(eventText)
	cause, _ := model.NewEventKey(peer, epoch, id)
	spec := event.Spec()
	spec.CausedBy = []model.EventKey{cause}
	result, err := model.NewEvent(spec)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func causalSuccessorEvent(t *testing.T, base model.Event, eventType model.EventType, eventText,
	payloadText string, cause model.EventKey, acceptedAt time.Time,
) model.Event {
	t.Helper()
	spec := base.Spec()
	spec.ID, _ = model.ParseEventID(eventText)
	spec.Type = eventType
	spec.Payload, _ = model.NewJSON([]byte(payloadText))
	spec.Artifacts = nil
	spec.CausedBy = []model.EventKey{cause}
	spec.CreatedAt = acceptedAt
	spec.AcceptedAt = acceptedAt
	event, err := model.NewEvent(spec)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func insertImportedCausalEvent(t *testing.T, fixture *acceptanceFixture, offered model.Event,
	eventType model.EventType, eventText, payloadText string, acceptedAt time.Time,
	artifacts ...model.ArtifactRef,
) model.EventKey {
	t.Helper()
	origin := offered.Audience().Peers()[0]
	var revision uint64
	var memberHash []byte
	var epochText string
	if err := fixture.store.db.QueryRow(`SELECT revision,record_hash,origin_epoch FROM channel_members
		WHERE channel_id=? AND member_peer_id=? ORDER BY revision DESC LIMIT 1`, fixture.channel.String(),
		origin.String()).Scan(&revision, &memberHash, &epochText); err != nil {
		t.Fatal(err)
	}
	var rosterRevision uint64
	var rosterHash []byte
	if err := fixture.store.db.QueryRow(`SELECT roster_head_revision,roster_head_hash FROM channels
		WHERE channel_id=?`, fixture.channel.String()).Scan(&rosterRevision, &rosterHash); err != nil {
		t.Fatal(err)
	}
	epoch, _ := model.ParseOriginEpoch(epochText)
	memberDigest, _ := model.DigestFromBytes(memberHash)
	rosterDigest, _ := model.DigestFromBytes(rosterHash)
	member, _ := model.NewRecordHead(revision, memberDigest)
	roster, _ := model.NewRecordHead(rosterRevision, rosterDigest)
	scope, err := model.NewEventScope(fixture.channel, origin, epoch, 1, 1, member, roster,
		offered.Scope().WorkRef())
	if err != nil {
		t.Fatal(err)
	}
	audience, _ := model.NewAudience([]model.PeerID{fixture.node.PeerID()})
	payload, _ := model.NewJSON([]byte(payloadText))
	eventID, _ := model.ParseEventID(eventText)
	event, err := model.NewEvent(model.EventSpec{ID: eventID, Scope: scope, Source: model.EventSourceImported,
		ActorPrincipal: "principal-remote", Type: eventType, Audience: audience, Summary: "causal request",
		Payload: payload, Artifacts: artifacts, CreatedAt: acceptedAt, AcceptedAt: acceptedAt})
	if err != nil {
		t.Fatal(err)
	}
	body, _ := model.NewPublicationBody(event)
	publication, err := model.AttachSignature(body, make([]byte, ed25519.SignatureSize))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := insertAcceptedEvent(context.Background(), tx, publication); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return event.Key()
}
