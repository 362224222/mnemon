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
			requireTerminalIntegrationHasNoReplyCapability(t, &fixture, test.name)
		})
	}
}

func TestTerminalReplyAdmissionRequiresOpenCorrelatedLocalResponsibility(t *testing.T) {
	t.Run("missing correlation", func(t *testing.T) {
		fixture := newPeerRoundTripFixture(t)
		response := prepareTerminalReplyDelivery(t, &fixture, "missing-correlation")
		response = rebuildPeerDelivery(t, fixture.originRoute.RouteID, response,
			focusEventRef(t, "event:unmatched-reply-root", "unmatched"), agency.EventRef{},
			agency.ConsequenceResolveDeclined, 1, response.Kind())
		requireTerminalReplyRejected(t, &fixture, response)
	})

	t.Run("unmatched correlation", func(t *testing.T) {
		fixture := newPeerRoundTripFixture(t)
		response := prepareTerminalReplyDelivery(t, &fixture, "unmatched-correlation")
		response = rebuildPeerDelivery(t, fixture.originRoute.RouteID, response,
			focusEventRef(t, "event:unmatched-origin", "unmatched origin"),
			focusEventRef(t, "event:unmatched-correlation", "unmatched correlation"),
			agency.ConsequenceResolveDeclined, 1, response.Kind())
		requireTerminalReplyRejected(t, &fixture, response)
	})

	t.Run("matching responsibility is already terminal", func(t *testing.T) {
		fixture := newPeerRoundTripFixture(t)
		response := prepareTerminalReplyDelivery(t, &fixture, "closed-anchor")
		closeCurrentLocally(t, fixture.origin, "operation:close-terminal-reply-anchor")
		requireTerminalReplyRejected(t, &fixture, response)
	})

	t.Run("matching responsibility belongs to another Principal", func(t *testing.T) {
		fixture := newPeerRoundTripFixture(t)
		response := prepareTerminalReplyDelivery(t, &fixture, "wrong-principal")
		other := mustPrincipal(t, "principal:terminal-reply-other")
		if err := fixture.origin.store.EnrollPrincipal(fixture.origin.ctx, other); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.origin.store.db.Exec(`UPDATE handlings SET target_principal_id = ?`,
			other.String()); err != nil {
			t.Fatal(err)
		}
		requireTerminalReplyRejected(t, &fixture, response)
	})

	t.Run("matching root was not sent to authenticated peer", func(t *testing.T) {
		fixture := newPeerRoundTripFixture(t)
		response := prepareTerminalReplyDelivery(t, &fixture, "wrong-route")
		correlation, present := response.OriginCorrelation()
		if !present {
			t.Fatal("terminal reply fixture has no correlation")
		}
		unaddressed := newFocusFederatedPeer(t, fixture.origin, fixture.originPrivate,
			7, "unaddressed-terminal-reply")
		delivery, err := agency.NewPeerDelivery(unaddressed.receiverRoute.RouteID,
			agency.PeerDeliverySpec{
				OriginEvent:    focusEventRef(t, "event:unaddressed-terminal-reply", "unaddressed"),
				OriginSequence: response.OriginSequence(), OriginAcceptedAt: response.OriginAcceptedAt(),
				OriginSource:      unaddressed.receiver.principal,
				OriginConsequence: agency.ConsequenceResolveDeclined, OriginTargetCount: 1,
				OriginCorrelation: correlation,
				TargetAlias:       unaddressed.receiverRoute.RemoteTargetAlias,
				Kind:              response.Kind(), Payload: response.Payload(),
				CausalDepth: response.CausalDepth(), ExpiresAt: response.ExpiresAt(),
			})
		if err != nil {
			t.Fatal(err)
		}
		signature := ed25519.Sign(unaddressed.receiverPrivate, delivery.SigningMessage())
		staged, err := fixture.origin.store.StagePeerDelivery(fixture.origin.ctx,
			unaddressed.originRoute.RemotePeerID, delivery.CanonicalJSON(), signature)
		if err != nil || staged.State() != PeerAdmissionStateStaged {
			t.Fatalf("stage unaddressed terminal reply = %#v, %v", staged, err)
		}
		result, err := fixture.origin.store.AdmitPeerDelivery(fixture.origin.ctx, delivery.ID())
		if err != nil || result.State() != PeerAdmissionStateRejected {
			t.Fatalf("unaddressed terminal reply = %#v, %v", result, err)
		}
	})
}

func TestImportedTerminalReplyCannotBecomeAnotherReplyAnchor(t *testing.T) {
	fixture := newPeerRoundTripFixture(t)
	first := prepareTerminalReplyDelivery(t, &fixture, "first-terminal-reply")
	originAnchor := requireOnlyHandlingID(t, fixture.origin)
	firstResult := stageAndAdmitPeerDelivery(t, &fixture, first)
	if firstResult.State() != PeerAdmissionStateAccepted {
		t.Fatalf("first terminal reply state = %v", firstResult.State())
	}
	assertHandlingOpenByID(t, fixture.origin, originAnchor)
	closeCurrentLocally(t, fixture.origin, "operation:close-original-terminal-reply-anchor")
	assertHandlingStateCount(t, fixture.origin, "open", 1)
	correlation, present := first.OriginCorrelation()
	if !present {
		t.Fatal("terminal reply fixture has no correlation")
	}
	second := rebuildPeerDelivery(t, fixture.originRoute.RouteID, first,
		focusEventRef(t, "event:second-terminal-reply", "second terminal reply"), correlation,
		agency.ConsequenceResolveDeclined, 1, first.Kind())
	requireTerminalReplyRejected(t, &fixture, second)
	assertHandlingStateCount(t, fixture.origin, "open", 1)
}

func TestTerminalReplyAdmissionUsesGenuineAnchorAmongRelatedOpenHandlings(t *testing.T) {
	fixture := newPeerRoundTripFixture(t)
	first := prepareTerminalReplyDelivery(t, &fixture, "first-related-reply")
	firstResult := stageAndAdmitPeerDelivery(t, &fixture, first)
	if firstResult.State() != PeerAdmissionStateAccepted {
		t.Fatalf("first terminal reply state = %v", firstResult.State())
	}
	correlation, present := first.OriginCorrelation()
	if !present {
		t.Fatal("terminal reply fixture has no correlation")
	}
	second := rebuildPeerDelivery(t, fixture.originRoute.RouteID, first,
		focusEventRef(t, "event:second-related-terminal-reply", "second related terminal reply"),
		correlation, agency.ConsequenceResolveDeclined, 1, first.Kind())
	secondResult := stageAndAdmitPeerDelivery(t, &fixture, second)
	if secondResult.State() != PeerAdmissionStateAccepted {
		t.Fatalf("second terminal reply state = %v", secondResult.State())
	}
	assertHandlingStateCount(t, fixture.origin, "open", 3)
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

func prepareTerminalReplyDelivery(t *testing.T, fixture *peerRoundTripFixture,
	suffix string,
) agency.PeerDelivery {
	t.Helper()
	bound, _, _ := bindImportedTerminalReply(t, fixture,
		agency.ConsequenceResolveDeclined, suffix)
	result, err := fixture.receiver.store.Admit(fixture.receiver.ctx,
		fixture.receiver.proof, bound)
	if err != nil {
		t.Fatal(err)
	}
	requireOutcome(t, result, agency.ReceiptOutcomeAccepted)
	return requireOnePendingDelivery(t, fixture.receiver)
}

func rebuildPeerDelivery(t *testing.T, route agency.RouteID, base agency.PeerDelivery,
	origin, correlation agency.EventRef, consequence agency.Consequence, targetCount uint8,
	kind agency.SemanticLabel,
) agency.PeerDelivery {
	t.Helper()
	delivery, err := agency.NewPeerDelivery(route, agency.PeerDeliverySpec{
		OriginEvent: origin, OriginSequence: base.OriginSequence(),
		OriginAcceptedAt: base.OriginAcceptedAt(), OriginSource: base.OriginSource(),
		OriginConsequence: consequence, OriginTargetCount: targetCount,
		OriginCausation: base.OriginCausation(), OriginCorrelation: correlation,
		TargetAlias: base.TargetAlias(), Kind: kind, Payload: base.Payload(),
		Artifacts: base.Artifacts(), CausalDepth: base.CausalDepth(), ExpiresAt: base.ExpiresAt(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return delivery
}

func stageAndAdmitPeerDelivery(t *testing.T, fixture *peerRoundTripFixture,
	delivery agency.PeerDelivery,
) PeerAdmissionResult {
	t.Helper()
	signature := ed25519.Sign(fixture.receiverPrivate, delivery.SigningMessage())
	staged, err := fixture.origin.store.StagePeerDelivery(fixture.origin.ctx,
		fixture.originRoute.RemotePeerID, delivery.CanonicalJSON(), signature)
	if err != nil || staged.State() != PeerAdmissionStateStaged {
		t.Fatalf("stage terminal reply = %#v, %v", staged, err)
	}
	result, err := fixture.origin.store.AdmitPeerDelivery(fixture.origin.ctx, delivery.ID())
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func requireTerminalReplyRejected(t *testing.T, fixture *peerRoundTripFixture,
	delivery agency.PeerDelivery,
) {
	t.Helper()
	beforeEvents := countRows(t, fixture.origin.store, "events")
	result := stageAndAdmitPeerDelivery(t, fixture, delivery)
	if result.State() != PeerAdmissionStateRejected {
		t.Fatalf("terminal reply state = %v, want rejected", result.State())
	}
	if got := countRows(t, fixture.origin.store, "events"); got != beforeEvents {
		t.Fatalf("rejected terminal reply created %d Events; want %d", got, beforeEvents)
	}
}

func closeCurrentLocally(t *testing.T, fixture *authorityFixture, operation string) {
	t.Helper()
	view := fixture.current(t)
	request := subjectRequest(t, view, operation, agency.ConsequenceResolveUnresolved,
		"local responsibility is closed", nil)
	result, err := fixture.store.Admit(fixture.ctx, fixture.proof, request)
	if err != nil {
		t.Fatal(err)
	}
	requireOutcome(t, result, agency.ReceiptOutcomeAccepted)
}

func requireTerminalIntegrationHasNoReplyCapability(t *testing.T,
	fixture *peerRoundTripFixture, suffix string,
) {
	t.Helper()
	closeCurrentLocally(t, fixture.origin, "operation:close-origin-anchor-"+suffix)
	view := fixture.origin.current(t)
	public := decodeFocusView(t, view)
	if public.Current == nil || public.Current.Facts.ReplyRequired ||
		public.Current.Facts.ReplyTarget != "" {
		t.Fatalf("terminal integration reply projection = %#v, want explicit no-reply", public.Current)
	}
	remote, err := agency.AliasTarget(fixture.originRoute.PublicAlias)
	if err != nil {
		t.Fatal(err)
	}
	intent := mustIntent(t, agency.IntentSpec{
		Kind:              mustLabel(t, "opaque.disposition.echo"),
		Payload:           mustPayload(t, "must not bounce terminal disposition"),
		Consequence:       agency.ConsequenceResolveUnresolved,
		SubjectHandling:   mustHandle(t, public.Current.Facts.Handle),
		Successors:        []agency.TargetRef{remote},
		CorrelationHandle: mustHandle(t, public.Current.Facts.ReplyTo),
	})
	if _, err := view.Bind(intent, mustOperation(t, "operation:terminal-echo-"+suffix), nil); err == nil {
		t.Fatal("terminal integration unexpectedly obtained terminal-reply exception")
	}
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
