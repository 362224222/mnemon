package authority

import (
	"crypto/ed25519"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

func TestImportedTerminalDispositionClosesResponderAndReturnsToRequester(t *testing.T) {
	for _, test := range []struct {
		name        string
		consequence agency.Consequence
		outcome     string
	}{
		{name: "completed", consequence: agency.ConsequenceResolveCompleted, outcome: "completed"},
		{name: "declined", consequence: agency.ConsequenceResolveDeclined, outcome: "declined"},
		{name: "unresolved", consequence: agency.ConsequenceResolveUnresolved, outcome: "unresolved"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newPeerRoundTripFixture(t)
			bound, requestDelivery, artifactContent := bindImportedTerminalReply(t,
				&fixture, test.consequence, test.name)
			originAnchor := requireOnlyHandlingID(t, fixture.origin)
			result, err := fixture.receiver.store.Admit(fixture.receiver.ctx,
				fixture.receiver.proof, bound)
			if err != nil {
				t.Fatalf("Admit terminal reply: %v", err)
			}
			requireOutcome(t, result, agency.ReceiptOutcomeAccepted)
			assertHandlingOutcomeCount(t, fixture.receiver, test.outcome, 1)
			assertHandlingStateCount(t, fixture.receiver, "open", 0)
			if got := countRows(t, fixture.receiver.store, "handlings"); got != 1 {
				t.Fatalf("responder Handlings = %d, want only the closed imported Handling", got)
			}
			if got := countRows(t, fixture.receiver.store, "peer_outbox"); got != 1 {
				t.Fatalf("responder outbox = %d, want one terminal disposition", got)
			}

			response := requireOnePendingDelivery(t, fixture.receiver)
			requireOriginCorrelation(t, response, requestDelivery.OriginEvent())
			if artifactContent != "" {
				fixture.origin.catalog(t, artifactContent)
			}
			local := admitTerminalReplyAtRequester(t, &fixture, response)
			assertStoredCorrelation(t, fixture.origin, local, requestDelivery.OriginEvent())
			assertHandlingOpenByID(t, fixture.origin, originAnchor)
		})
	}
}

func TestCorrelatedTerminalReplyRejectsRevokedBoundRoute(t *testing.T) {
	fixture := newPeerRoundTripFixture(t)
	bound, _, _ := bindImportedTerminalReply(t, &fixture,
		agency.ConsequenceResolveDeclined, "revoked-route")
	if _, err := fixture.receiver.store.RevokePeerRoute(fixture.receiver.ctx,
		fixture.receiverRoute.RouteID); err != nil {
		t.Fatal(err)
	}
	beforeEvents := countRows(t, fixture.receiver.store, "events")
	result, err := fixture.receiver.store.Admit(fixture.receiver.ctx,
		fixture.receiver.proof, bound)
	if err != nil {
		t.Fatal(err)
	}
	requireOutcome(t, result, agency.ReceiptOutcomeRejected)
	receipt, err := agency.ParseReceiptCanonicalJSON(result.ReceiptJSON())
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Code() != rejectionStaleRoute {
		t.Fatalf("revoked reply route code = %s, want %s", receipt.Code().String(),
			rejectionStaleRoute.String())
	}
	if countRows(t, fixture.receiver.store, "events") != beforeEvents ||
		countRows(t, fixture.receiver.store, "peer_outbox") != 0 {
		t.Fatal("rejected reply changed Event or outbox state")
	}
	assertHandlingStateCount(t, fixture.receiver, "open", 1)
	replayed, err := fixture.receiver.store.Admit(fixture.receiver.ctx,
		fixture.receiver.proof, bound)
	if err != nil || !replayed.Replayed() ||
		replayed.ReceiptDigest() != result.ReceiptDigest() {
		t.Fatalf("rejected reply replay = %#v, %v", replayed, err)
	}
}

func TestCorrelatedTerminalReplyOutboxFaultRestoresResponderHandling(t *testing.T) {
	fixture := newPeerRoundTripFixture(t)
	bound, requestDelivery, _ := bindImportedTerminalReply(t, &fixture,
		agency.ConsequenceResolveCompleted, "outbox-fault")
	before := snapshotP05Authority(t, fixture.receiver.store)
	drop := installP05Fault(t, fixture.receiver.store, `CREATE TEMP TRIGGER p05_fault
		AFTER INSERT ON peer_outbox
		BEGIN SELECT RAISE(ABORT, 'fault: correlated terminal reply outbox'); END`)
	if _, err := fixture.receiver.store.Admit(fixture.receiver.ctx,
		fixture.receiver.proof, bound); err == nil {
		t.Fatal("faulted terminal reply unexpectedly succeeded")
	}
	requireP05Snapshot(t, fixture.receiver.store, before)
	drop()
	requireExactAdmissionReplay(t, fixture.receiver, bound)
	assertHandlingOutcomeCount(t, fixture.receiver, "completed", 1)
	assertHandlingStateCount(t, fixture.receiver, "open", 0)
	response := requireOnePendingDelivery(t, fixture.receiver)
	requireOriginCorrelation(t, response, requestDelivery.OriginEvent())
}

func bindImportedTerminalReply(t *testing.T, fixture *peerRoundTripFixture,
	consequence agency.Consequence, suffix string,
) (agency.BoundIntent, agency.PeerDelivery, string) {
	t.Helper()
	requestDelivery := fixture.admitOrigin(t)
	fixture.admitReceiver(t, requestDelivery)
	view := fixture.receiver.current(t)
	public := requireReplyContext(t, view,
		fixture.receiverRoute.PublicAlias.String(), "")
	target, err := agency.AliasTarget(mustHandle(t,
		public.Current.Facts.ReplyTarget))
	if err != nil {
		t.Fatal(err)
	}
	operation := mustOperation(t, "operation:terminal-reply-"+suffix)
	spec := agency.IntentSpec{
		Kind:              mustLabel(t, "opaque.disposition"),
		Payload:           mustPayload(t, "bounded "+suffix+" disposition"),
		Consequence:       consequence,
		SubjectHandling:   mustHandle(t, public.Current.Facts.Handle),
		Successors:        []agency.TargetRef{target},
		CorrelationHandle: mustHandle(t, public.Current.Facts.ReplyTo),
	}
	var candidates []agency.CapturedCandidate
	artifactContent := ""
	if consequence == agency.ConsequenceResolveCompleted {
		artifactContent = "verified terminal reply evidence " + suffix
		digest := fixture.receiver.catalog(t, artifactContent)
		input, err := agency.NewArtifactCandidate(mustHandle(t,
			"candidate:terminal-reply-"+suffix))
		if err != nil {
			t.Fatal(err)
		}
		spec.Artifacts = []agency.ArtifactInput{input}
		candidate, err := agency.NewCapturedCandidate(operation, input, digest)
		if err != nil {
			t.Fatal(err)
		}
		candidates = []agency.CapturedCandidate{candidate}
	}
	intent := mustIntent(t, spec)
	bound, err := view.Bind(intent, operation, candidates)
	if err != nil {
		t.Fatalf("Bind terminal reply: %v", err)
	}
	return bound, requestDelivery, artifactContent
}

func admitTerminalReplyAtRequester(t *testing.T, fixture *peerRoundTripFixture,
	delivery agency.PeerDelivery,
) agency.EventRef {
	t.Helper()
	signature := ed25519.Sign(fixture.receiverPrivate, delivery.SigningMessage())
	staged, err := fixture.origin.store.StagePeerDelivery(fixture.origin.ctx,
		fixture.originRoute.RemotePeerID, delivery.CanonicalJSON(), signature)
	if err != nil || staged.State() != PeerAdmissionStateStaged {
		t.Fatalf("Stage terminal reply = %#v, %v", staged, err)
	}
	accepted, err := fixture.origin.store.AdmitPeerDelivery(fixture.origin.ctx, delivery.ID())
	if err != nil || accepted.State() != PeerAdmissionStateAccepted {
		t.Fatalf("Admit terminal reply = %#v, %v", accepted, err)
	}
	receipt, present := accepted.Receipt()
	if !present {
		t.Fatal("terminal reply admission has no Receipt")
	}
	local, present := receipt.LocalEvent()
	if !present {
		t.Fatal("terminal reply admission has no local Event")
	}
	return local
}

func assertStoredCorrelation(t *testing.T, fixture *authorityFixture,
	local, want agency.EventRef,
) {
	t.Helper()
	tx, err := fixture.store.db.BeginTx(fixture.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	details, err := loadStoredEventDetailsTx(fixture.ctx, tx, local.ID().String())
	if err != nil {
		t.Fatal(err)
	}
	if details.correlation != want {
		t.Fatalf("readmitted terminal reply correlation = %v; want %v", details.correlation, want)
	}
}

func requireOnlyHandlingID(t *testing.T, fixture *authorityFixture) string {
	t.Helper()
	var id string
	if err := fixture.store.db.QueryRow(`SELECT handling_id FROM handlings`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func assertHandlingOpenByID(t *testing.T, fixture *authorityFixture, id string) {
	t.Helper()
	var state string
	var outcome any
	if err := fixture.store.db.QueryRow(`SELECT state, outcome FROM handlings
		WHERE handling_id = ?`, id).Scan(&state, &outcome); err != nil {
		t.Fatal(err)
	}
	if state != "open" || outcome != nil {
		t.Fatalf("origin anchor %s = state:%s outcome:%v", id, state, outcome)
	}
}
