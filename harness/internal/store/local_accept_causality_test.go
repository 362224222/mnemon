package store

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"testing"
	"time"

	eventpkg "github.com/mnemon-dev/mnemon/harness/internal/event"
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
		fmt.Sprintf(`{"decision_ref":%q,"diagnostic_code":"fallback","iteration":1,"status":"rejected","work_version":1}`,
			request.EventID().String()),
		request, fixture.now.Add(3*time.Second))
	if err := validateLocalCausalSemantics(context.Background(), tx, model.Operation{}, outcome); err != nil {
		t.Fatalf("fallback outcome exact source error = %v", err)
	}
}

func TestCommitLocalAcceptanceAdmitsStaleAcceptRejectedReceipt(t *testing.T) {
	fixture := newAcceptanceFixture(t, 1)
	offered, current := commitCausalOffer(t, fixture, "stale-accept-rejected")
	current = commitCausalAcceptedWork(t, fixture, offered, current, fixture.now.Add(3*time.Second))
	staleRequest := insertImportedCausalEventAtSequence(t, fixture, offered,
		model.EventReviewAcceptRequested, "event-stale-accept-request",
		`{"iteration":1,"note":"late contender","work_version":1}`,
		fixture.now.Add(4*time.Second), 2)

	decision, err := eventpkg.NewAcceptRejectedDecision("accept-race-lost")
	if err != nil {
		t.Fatal(err)
	}
	scope, publication := causalControllerPublication(t, fixture, current,
		"event-stale-accept-rejected", 1, 1, staleRequest, fixture.now.Add(5*time.Second), decision)
	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), LocalAcceptanceSpec{
		Scope: scope, Controller: true, Items: []LocalAcceptanceItem{{Publication: publication}},
	}, fixture.now.Add(5*time.Second)); err != nil {
		t.Fatalf("stale accept rejection receipt error = %v", err)
	}

	assertCausalReceiptAndWork(t, fixture, publication.Event().ID(), current,
		model.WorkActive, 2)
}

func TestCommitLocalAcceptanceAdmitsAcceptRejectedAfterAtomicExpiry(t *testing.T) {
	fixture := newAcceptanceFixture(t, 1)
	offered, current := commitCausalOffer(t, fixture, "expired-accept-rejected")
	expiredAt := current.Deadline()
	scope, expiry := causalControllerPublication(t, fixture, current,
		"event-causal-expired", current.Version(), current.Iteration(), offered.Key(), expiredAt,
		eventpkg.ExpiredDecision{})
	current = commitCausalWorkTransition(t, fixture, scope, expiry, current,
		model.WorkExpired, current.Iteration(), expiredAt)
	staleRequest := insertImportedCausalEvent(t, fixture, offered,
		model.EventReviewAcceptRequested, "event-expired-accept-request",
		`{"iteration":1,"note":"arrived after expiry","work_version":1}`,
		expiredAt.Add(time.Second))

	decision, err := eventpkg.NewAcceptRejectedDecision("work-expired")
	if err != nil {
		t.Fatal(err)
	}
	receiptAt := expiredAt.Add(2 * time.Second)
	scope, receipt := causalControllerPublication(t, fixture, current,
		"event-expired-accept-rejected", 1, 1, staleRequest, receiptAt, decision)
	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), LocalAcceptanceSpec{
		Scope: scope, Controller: true, Items: []LocalAcceptanceItem{{Publication: receipt}},
	}, receiptAt); err != nil {
		t.Fatalf("post-expiry accept rejection receipt error = %v", err)
	}

	assertCausalReceiptAndWork(t, fixture, receipt.Event().ID(), current,
		model.WorkExpired, 2)
}

func TestCommitLocalAcceptanceAdmitsStaleRemoteOutcomeReceipt(t *testing.T) {
	fixture := newAcceptanceFixture(t, 1)
	offered, current := commitCausalOffer(t, fixture, "stale-outcome")
	current = commitCausalAcceptedWork(t, fixture, offered, current, fixture.now.Add(3*time.Second))
	staleRemote := insertImportedCausalEventAtSequence(t, fixture, offered,
		model.EventReviewDeclineRequested, "event-stale-outcome-source",
		`{"content":"late decline","iteration":1,"work_version":1}`,
		fixture.now.Add(4*time.Second), 2)

	decision, err := eventpkg.NewOutcomeDecision(eventpkg.OutcomeRejected,
		"stale-remote-event", staleRemote.EventID().String())
	if err != nil {
		t.Fatal(err)
	}
	scope, publication := causalControllerPublication(t, fixture, current,
		"event-stale-outcome", 1, 1, staleRemote, fixture.now.Add(5*time.Second), decision)
	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), LocalAcceptanceSpec{
		Scope: scope, Controller: true, Items: []LocalAcceptanceItem{{Publication: publication}},
	}, fixture.now.Add(5*time.Second)); err != nil {
		t.Fatalf("stale remote outcome receipt error = %v", err)
	}

	assertCausalReceiptAndWork(t, fixture, publication.Event().ID(), current,
		model.WorkActive, 2)
}

func TestRequireReceiptSourceEchoBoundsStaleCausality(t *testing.T) {
	fixture := newAcceptanceFixture(t, 1)
	offered, current := commitCausalOffer(t, fixture, "receipt-echo")
	current = commitCausalAcceptedWork(t, fixture, offered, current, fixture.now.Add(3*time.Second))

	tests := []struct {
		name            string
		sourcePayload   string
		responsePayload string
		decisionRef     string
		wantErr         bool
	}{
		{"stale exact echo", `{"iteration":1,"work_version":1}`,
			`{"diagnostic_code":"stale","iteration":1,"status":"rejected","work_version":1}`, "", false},
		{"response source mismatch", `{"iteration":1,"work_version":1}`,
			`{"diagnostic_code":"mismatch","iteration":1,"status":"rejected","work_version":2}`, "", true},
		{"future source version", `{"iteration":1,"work_version":3}`,
			`{"diagnostic_code":"future","iteration":1,"status":"rejected","work_version":3}`, "", true},
		{"same version wrong iteration", `{"iteration":2,"work_version":2}`,
			`{"diagnostic_code":"wrong-iteration","iteration":2,"status":"rejected","work_version":2}`, "", true},
		{"older version future iteration", `{"iteration":2,"work_version":1}`,
			`{"diagnostic_code":"future-iteration","iteration":2,"status":"rejected","work_version":1}`, "", true},
		{"wrong decision ref", `{"iteration":1,"work_version":1}`,
			`{"diagnostic_code":"wrong-ref","iteration":1,"status":"rejected","work_version":1}`, "event-other-source", true},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decisionRef := test.decisionRef
			if decisionRef == "" {
				decisionRef = offered.ID().String()
			}
			responsePayload := fmt.Sprintf(`{"decision_ref":%q,%s`, decisionRef,
				test.responsePayload[1:])
			response := causalSuccessorEvent(t, offered, model.EventReviewOutcome,
				"event-receipt-echo-"+string(rune('a'+index)), responsePayload,
				offered.Key(), fixture.now.Add(4*time.Second))
			source := durableCausalEvent{key: offered.Key(), payloadJSON: []byte(test.sourcePayload)}
			err := requireReceiptSourceEcho(response, source, current)
			if test.wantErr && !errors.Is(err, ErrAdmissionConflict) {
				t.Fatalf("error = %v, want admission conflict", err)
			}
			if !test.wantErr && err != nil {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestRequireReceiptSourceEchoRejectsImpossibleClosedTuples(t *testing.T) {
	fixture := newAcceptanceFixture(t, 1)
	offered, current := commitCausalOffer(t, fixture, "receipt-closed-tuples")
	terminalSpec := current.Spec()
	terminalSpec.Version = 6
	terminalSpec.Iteration = 2
	terminalSpec.State = model.WorkClosed
	terminalSpec.StateData, _ = model.NewJSON([]byte(
		`{"iteration":2,"note":"closed","work_version":5}`))
	terminalSpec.UpdatedBy, _ = model.ParseEventID("event-terminal-second-iteration")
	terminalSpec.UpdatedAt = fixture.now.Add(6 * time.Second)
	terminal, err := model.NewReviewWork(terminalSpec)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name      string
		version   uint64
		iteration uint8
		wantErr   bool
	}{
		{"last iteration one source", 3, 1, false},
		{"first iteration two source", 4, 2, false},
		{"last iteration two source", 5, 2, false},
		{"iteration two too early", 3, 2, true},
		{"terminal iteration one result", 4, 1, true},
		{"iteration one too late", 5, 1, true},
		{"terminal iteration two result", 6, 2, true},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload := fmt.Sprintf(
				`{"decision_ref":%q,"diagnostic_code":"tuple","iteration":%d,"status":"rejected","work_version":%d}`,
				offered.ID().String(), test.iteration, test.version)
			response := causalSuccessorEvent(t, offered, model.EventReviewOutcome,
				"event-receipt-tuple-"+string(rune('a'+index)), payload, offered.Key(),
				fixture.now.Add(7*time.Second))
			source := durableCausalEvent{key: offered.Key(), payloadJSON: []byte(fmt.Sprintf(
				`{"iteration":%d,"work_version":%d}`, test.iteration, test.version))}
			err := requireReceiptSourceEcho(response, source, terminal)
			if test.wantErr && !errors.Is(err, ErrAdmissionConflict) {
				t.Fatalf("error = %v, want admission conflict", err)
			}
			if !test.wantErr && err != nil {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestRequireReceiptSourceEchoRejectsAcceptRejectedOutsideInitialOffer(t *testing.T) {
	fixture := newAcceptanceFixture(t, 1)
	offered, current := commitCausalOffer(t, fixture, "accept-rejected-tuple")
	current = commitCausalAcceptedWork(t, fixture, offered, current, fixture.now.Add(3*time.Second))
	response := causalSuccessorEvent(t, offered, model.EventReviewAcceptRejected,
		"event-accept-rejected-noninitial",
		`{"diagnostic_code":"invalid-phase","iteration":1,"work_version":2}`,
		offered.Key(), fixture.now.Add(4*time.Second))
	source := durableCausalEvent{
		key: offered.Key(), payloadJSON: []byte(`{"iteration":1,"work_version":2}`),
	}
	if err := requireReceiptSourceEcho(response, source, current); !errors.Is(err, ErrAdmissionConflict) {
		t.Fatalf("error = %v, want admission conflict", err)
	}
}

func TestValidateLocalCausalSemanticsRejectsInvalidReceiptSources(t *testing.T) {
	t.Run("outcome decision ref must name exact source", func(t *testing.T) {
		fixture := newAcceptanceFixture(t, 1)
		offered, _ := commitCausalOffer(t, fixture, "wrong-outcome-decision-ref")
		source := insertImportedCausalEvent(t, fixture, offered, model.EventReviewDeclineRequested,
			"event-wrong-outcome-decision-source",
			`{"content":"cannot review","iteration":1,"work_version":1}`,
			fixture.now.Add(2*time.Second))
		response := causalSuccessorEvent(t, offered, model.EventReviewOutcome,
			"event-wrong-outcome-decision-response",
			`{"decision_ref":"event-unrelated","diagnostic_code":"wrong-ref","iteration":1,"status":"rejected","work_version":1}`,
			source, fixture.now.Add(3*time.Second))
		assertCausalConflict(t, fixture, response)
	})

	t.Run("accept rejected requires exact accept request", func(t *testing.T) {
		fixture := newAcceptanceFixture(t, 1)
		offered, _ := commitCausalOffer(t, fixture, "wrong-accept-source")
		source := insertImportedCausalEvent(t, fixture, offered, model.EventReviewDeliveryReady,
			"event-wrong-accept-source", `{"content":"ready","iteration":1,"work_version":1}`,
			fixture.now.Add(2*time.Second))
		response := causalSuccessorEvent(t, offered, model.EventReviewAcceptRejected,
			"event-wrong-accept-response", `{"diagnostic_code":"wrong-source","iteration":1,"work_version":1}`,
			source, fixture.now.Add(3*time.Second))
		assertCausalConflict(t, fixture, response)
	})

	t.Run("outcome cannot cite outcome", func(t *testing.T) {
		fixture := newAcceptanceFixture(t, 1)
		offered, _ := commitCausalOffer(t, fixture, "outcome-loop")
		source := insertImportedCausalEvent(t, fixture, offered, model.EventReviewOutcome,
			"event-outcome-loop-source",
			`{"decision_ref":"remote-decision","diagnostic_code":"remote-outcome","iteration":1,"status":"rejected","work_version":1}`,
			fixture.now.Add(2*time.Second))
		response := causalSuccessorEvent(t, offered, model.EventReviewOutcome,
			"event-outcome-loop-response",
			`{"decision_ref":"local-decision","diagnostic_code":"outcome-loop","iteration":1,"status":"rejected","work_version":1}`,
			source, fixture.now.Add(3*time.Second))
		assertCausalConflict(t, fixture, response)
	})
}

func TestValidateLocalCausalSemanticsKeepsStateChangesOnCurrentWork(t *testing.T) {
	tests := []struct {
		name         string
		sourceType   model.EventType
		responseType model.EventType
		source       string
		response     string
	}{
		{"accepted", model.EventReviewAcceptRequested, model.EventReviewAccepted,
			`{"iteration":1,"note":"stale","work_version":1}`,
			`{"iteration":1,"work_version":1}`},
		{"delivered", model.EventReviewDeliveryReady, model.EventReviewDelivered,
			`{"content":"stale","iteration":1,"work_version":1}`,
			`{"iteration":1,"work_version":1}`},
		{"declined", model.EventReviewDeclineRequested, model.EventReviewDeclined,
			`{"iteration":1,"reason":"stale","work_version":1}`,
			`{"iteration":1,"work_version":1}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAcceptanceFixture(t, 1)
			offered, current := commitCausalOffer(t, fixture, "state-change-"+test.name)
			current = commitCausalAcceptedWork(t, fixture, offered, current, fixture.now.Add(3*time.Second))
			source := insertImportedCausalEventAtSequence(t, fixture, offered, test.sourceType,
				"event-state-change-source-"+test.name, test.source, fixture.now.Add(4*time.Second), 2)
			response := causalSuccessorEvent(t, offered, test.responseType,
				"event-state-change-response-"+test.name, test.response, source, fixture.now.Add(5*time.Second))
			assertCausalConflict(t, fixture, response)
		})
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
	return insertImportedCausalEventAtSequence(t, fixture, offered, eventType, eventText,
		payloadText, acceptedAt, 1, artifacts...)
}

func insertImportedCausalEventAtSequence(t *testing.T, fixture *acceptanceFixture, offered model.Event,
	eventType model.EventType, eventText, payloadText string, acceptedAt time.Time, sequence uint64,
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
	scope, err := model.NewEventScope(fixture.channel, origin, epoch, sequence, sequence, member, roster,
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

func commitCausalOffer(t *testing.T, fixture *acceptanceFixture,
	suffix string,
) (model.Event, model.ReviewWork) {
	t.Helper()
	_, authority := fixture.reserveOffer(t, suffix, nil)
	spec := fixture.offer(t, authority, suffix, fixture.reviewers, nil, nil)
	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), spec,
		fixture.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	offered := spec.Items[0].Publication.Event()
	current, err := fixture.store.GetReviewWork(context.Background(), offered.Scope().WorkRef())
	if err != nil {
		t.Fatal(err)
	}
	return offered, current
}

func commitCausalAcceptedWork(t *testing.T, fixture *acceptanceFixture, offered model.Event,
	current model.ReviewWork, acceptedAt time.Time,
) model.ReviewWork {
	t.Helper()
	scope, publication := controllerAcceptance(t, fixture, current, offered, acceptedAt)
	return commitCausalWorkTransition(t, fixture, scope, publication, current,
		model.WorkActive, current.Iteration(), acceptedAt)
}

func commitCausalWorkTransition(t *testing.T, fixture *acceptanceFixture, scope LocalAdmissionScope,
	publication model.SignedPublication, current model.ReviewWork, state model.WorkState,
	iteration uint8, acceptedAt time.Time,
) model.ReviewWork {
	t.Helper()
	nextSpec := current.Spec()
	nextSpec.Version++
	nextSpec.Iteration = iteration
	nextSpec.State = state
	nextSpec.StateData = publication.Event().Payload()
	nextSpec.UpdatedBy = publication.Event().ID()
	nextSpec.UpdatedAt = acceptedAt
	next, err := model.NewReviewWork(nextSpec)
	if err != nil {
		t.Fatal(err)
	}
	mutation, err := NewWorkTransition(next, current.Version(), current.State())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), LocalAcceptanceSpec{
		Scope: scope, Controller: true,
		Items: []LocalAcceptanceItem{{Publication: publication, Work: &mutation}},
	}, acceptedAt); err != nil {
		t.Fatal(err)
	}
	durable, err := fixture.store.GetReviewWork(context.Background(), current.Ref())
	if err != nil {
		t.Fatal(err)
	}
	return durable
}

func causalControllerPublication(t *testing.T, fixture *acceptanceFixture, current model.ReviewWork,
	eventText string, workVersion uint64, iteration uint8, cause model.EventKey, acceptedAt time.Time,
	candidate eventpkg.ControllerCandidate,
) (LocalAdmissionScope, model.SignedPublication) {
	t.Helper()
	audience, err := model.NewAudience([]model.PeerID{current.Participants().ReviewerPeerID()})
	if err != nil {
		t.Fatal(err)
	}
	scope, err := fixture.store.PrepareLocalAdmission(context.Background(), current.ChannelID(), audience, 1)
	if err != nil {
		t.Fatal(err)
	}
	eventScope, err := scope.EventScope(0, current.Ref())
	if err != nil {
		t.Fatal(err)
	}
	eventID, err := model.ParseEventID(eventText)
	if err != nil {
		t.Fatal(err)
	}
	stamp, err := eventpkg.NewAdmissionStamp(eventpkg.AdmissionStampSpec{
		Node: scope.Node(), Profile: scope.Profile(), EventID: eventID, ChannelID: current.ChannelID(),
		WorkRef: current.Ref(), OriginSequence: eventScope.OriginSequence(),
		ChannelSequence: eventScope.ChannelSequence(), OriginMember: eventScope.OriginMember(),
		PublicationRoster: eventScope.PublicationRoster(), Audience: audience,
		WorkVersion: workVersion, Iteration: iteration, WorkDeadlineUnixNano: current.DeadlineUnixNano(),
		CausedBy: []model.EventKey{cause},
	})
	if err != nil {
		t.Fatal(err)
	}
	signer, err := eventpkg.NewEd25519Signer(fixture.privateKey)
	if err != nil {
		t.Fatal(err)
	}
	factory, err := eventpkg.NewFactory(acceptanceClock{acceptedAt}, signer)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := factory.AdmitController(context.Background(), stamp, candidate)
	if err != nil {
		t.Fatal(err)
	}
	return scope, bundle.Publication()
}

func assertCausalReceiptAndWork(t *testing.T, fixture *acceptanceFixture, receiptID model.EventID,
	prior model.ReviewWork, wantState model.WorkState, wantVersion uint64,
) {
	t.Helper()
	var receipts int
	if err := fixture.store.db.QueryRow("SELECT COUNT(*) FROM events WHERE event_id=?",
		receiptID.String()).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if receipts != 1 {
		t.Fatalf("durable receipt count = %d, want 1", receipts)
	}
	current, err := fixture.store.GetReviewWork(context.Background(), prior.Ref())
	if err != nil {
		t.Fatal(err)
	}
	if current.State() != wantState || current.Version() != wantVersion ||
		current.UpdatedBy() != prior.UpdatedBy() {
		t.Fatalf("receipt changed Work = state %s version %d updated_by %s",
			current.State(), current.Version(), current.UpdatedBy())
	}
}

func assertCausalConflict(t *testing.T, fixture *acceptanceFixture, event model.Event) {
	t.Helper()
	tx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := validateLocalCausalSemantics(context.Background(), tx, model.Operation{}, event); !errors.Is(err, ErrAdmissionConflict) {
		t.Fatalf("causal error = %v, want admission conflict", err)
	}
}
