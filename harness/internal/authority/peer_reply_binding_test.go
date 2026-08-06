package authority

import (
	"crypto/ed25519"
	"database/sql"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

func TestTerminalReplyFollowsBoundSubjectAdvanceAnchor(t *testing.T) {
	fixture := newPeerRoundTripFixture(t)
	root := rootRequest(t, fixture.origin.current(t), "operation:later-remote-root",
		"retain local responsibility before choosing a peer")
	result, err := fixture.origin.store.Admit(fixture.origin.ctx, fixture.origin.proof, root)
	if err != nil {
		t.Fatal(err)
	}
	requireOutcome(t, result, agency.ReceiptOutcomeAccepted)
	anchor := requireOnlyHandlingID(t, fixture.origin)

	delivery := admitRemoteAdvance(t, fixture.origin, fixture.originRoute.PublicAlias,
		"operation:later-remote-advance")
	assertPeerReplyBinding(t, fixture.origin, delivery.ID(), anchor, delivery.OriginEvent())
	stageAndAdmitArtifactFreeDelivery(t, fixture.receiver, fixture.receiverRoute.RemotePeerID,
		fixture.originPrivate, delivery)

	reply := admitTerminalDeclineFromCurrent(t, fixture.receiver)
	assertPeerReplyBindingAbsent(t, fixture.receiver, reply.ID())
	accepted := stageAndAdmitPeerDelivery(t, &fixture, reply)
	if accepted.State() != PeerAdmissionStateAccepted {
		t.Fatalf("terminal reply state = %v, want accepted", accepted.State())
	}
	assertHandlingOpenByID(t, fixture.origin, anchor)
	events, handlings := countRows(t, fixture.origin.store, "events"),
		countRows(t, fixture.origin.store, "handlings")

	signature := ed25519.Sign(fixture.receiverPrivate, reply.SigningMessage())
	replayedStage, err := fixture.origin.store.StagePeerDelivery(fixture.origin.ctx,
		fixture.originRoute.RemotePeerID, reply.CanonicalJSON(), signature)
	if err != nil || !replayedStage.Replayed() || replayedStage.State() != PeerAdmissionStateAccepted {
		t.Fatalf("terminal reply stage replay = %#v, %v", replayedStage, err)
	}
	replayed, err := fixture.origin.store.AdmitPeerDelivery(fixture.origin.ctx, reply.ID())
	if err != nil || !replayed.Replayed() || replayed.State() != PeerAdmissionStateAccepted {
		t.Fatalf("terminal reply admission replay = %#v, %v", replayed, err)
	}
	if countRows(t, fixture.origin.store, "events") != events ||
		countRows(t, fixture.origin.store, "handlings") != handlings {
		t.Fatal("terminal reply replay created a second local effect")
	}
	assertHandlingOpenByID(t, fixture.origin, anchor)
}

func TestImportedIntegrationExplicitAdvanceCreatesReplyAnchor(t *testing.T) {
	fixture := newPeerRoundTripFixture(t)
	request := fixture.admitOrigin(t)
	fixture.admitReceiver(t, request)
	firstReply := admitTerminalDeclineFromCurrent(t, fixture.receiver)
	if result := stageAndAdmitPeerDelivery(t, &fixture, firstReply); result.State() != PeerAdmissionStateAccepted {
		t.Fatalf("first terminal reply state = %v, want accepted", result.State())
	}
	closeCurrentLocally(t, fixture.origin, "operation:close-original-request-anchor")

	next := newFocusFederatedPeer(t, fixture.origin, fixture.originPrivate, 5, "integration-next")
	view := fixture.origin.current(t)
	public := requireReplyContext(t, view, "", "")
	remote, err := agency.AliasTarget(next.originRoute.PublicAlias)
	if err != nil {
		t.Fatal(err)
	}
	intent := mustIntent(t, agency.IntentSpec{
		Kind:            mustLabel(t, "opaque.followup"),
		Payload:         mustPayload(t, "seek independent follow-up after integrating the reply"),
		Consequence:     agency.ConsequenceAdvanceHandling,
		SubjectHandling: mustHandle(t, public.Current.Facts.Handle),
		Successors:      []agency.TargetRef{remote},
	})
	bound, err := view.Bind(intent, mustOperation(t, "operation:advance-imported-integration"), nil)
	if err != nil {
		t.Fatal(err)
	}
	subject, present := bound.Subject()
	if !present {
		t.Fatal("explicit integration advance has no bound subject")
	}
	result, err := fixture.origin.store.Admit(fixture.origin.ctx, fixture.origin.proof, bound)
	if err != nil {
		t.Fatal(err)
	}
	requireOutcome(t, result, agency.ReceiptOutcomeAccepted)

	outbound := requirePendingDeliveryForRoute(t, fixture.origin, next.originRoute.RouteID)
	assertPeerReplyBinding(t, fixture.origin, outbound.ID(), subject.HandlingID().String(),
		outbound.OriginEvent())
	stageAndAdmitArtifactFreeDelivery(t, next.receiver, next.receiverRoute.RemotePeerID,
		fixture.originPrivate, outbound)
	reply := admitTerminalDeclineFromCurrent(t, next.receiver)
	signature := ed25519.Sign(next.receiverPrivate, reply.SigningMessage())
	staged, err := fixture.origin.store.StagePeerDelivery(fixture.origin.ctx,
		next.originRoute.RemotePeerID, reply.CanonicalJSON(), signature)
	if err != nil || staged.State() != PeerAdmissionStateStaged {
		t.Fatalf("stage follow-up reply = %#v, %v", staged, err)
	}
	accepted, err := fixture.origin.store.AdmitPeerDelivery(fixture.origin.ctx, reply.ID())
	if err != nil || accepted.State() != PeerAdmissionStateAccepted {
		t.Fatalf("admit follow-up reply = %#v, %v", accepted, err)
	}
	assertHandlingOpenByID(t, fixture.origin, subject.HandlingID().String())
}

func admitRemoteAdvance(t *testing.T, fixture *authorityFixture,
	remoteAlias agency.OpaqueHandle, operationValue string,
) agency.PeerDelivery {
	t.Helper()
	view := fixture.current(t)
	remote, err := agency.AliasTarget(remoteAlias)
	if err != nil {
		t.Fatal(err)
	}
	intent := mustIntent(t, agency.IntentSpec{
		Kind:            mustLabel(t, "opaque.remote-work"),
		Payload:         mustPayload(t, "request bounded work from the selected peer"),
		Consequence:     agency.ConsequenceAdvanceHandling,
		SubjectHandling: currentSubjectHandle(t, view),
		Successors:      []agency.TargetRef{remote},
	})
	bound, err := view.Bind(intent, mustOperation(t, operationValue), nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := fixture.store.Admit(fixture.ctx, fixture.proof, bound)
	if err != nil {
		t.Fatal(err)
	}
	requireOutcome(t, result, agency.ReceiptOutcomeAccepted)
	return requirePendingDeliveryForRoute(t, fixture, bound.Targets()[0].RemoteRoute())
}

func admitTerminalDeclineFromCurrent(t *testing.T,
	fixture *authorityFixture,
) agency.PeerDelivery {
	t.Helper()
	view := fixture.current(t)
	public := decodeFocusView(t, view)
	if public.Current == nil || !public.Current.Facts.ReplyRequired {
		t.Fatalf("terminal responder has no required reply context: %#v", public.Current)
	}
	target, err := agency.AliasTarget(mustHandle(t, public.Current.Facts.ReplyTarget))
	if err != nil {
		t.Fatal(err)
	}
	intent := mustIntent(t, agency.IntentSpec{
		Kind:              mustLabel(t, "opaque.terminal-reply"),
		Payload:           mustPayload(t, "bounded work is declined"),
		Consequence:       agency.ConsequenceResolveDeclined,
		SubjectHandling:   mustHandle(t, public.Current.Facts.Handle),
		Successors:        []agency.TargetRef{target},
		CorrelationHandle: mustHandle(t, public.Current.Facts.ReplyTo),
	})
	bound, err := view.Bind(intent,
		mustOperation(t, "operation:terminal-decline:"+public.Current.Facts.Handle), nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := fixture.store.Admit(fixture.ctx, fixture.proof, bound)
	if err != nil {
		t.Fatal(err)
	}
	requireOutcome(t, result, agency.ReceiptOutcomeAccepted)
	return requireOnePendingDelivery(t, fixture)
}

func stageAndAdmitArtifactFreeDelivery(t *testing.T, receiver *authorityFixture,
	remotePeer agency.OpaqueHandle, signer ed25519.PrivateKey, delivery agency.PeerDelivery,
) {
	t.Helper()
	signature := ed25519.Sign(signer, delivery.SigningMessage())
	staged, err := receiver.store.StagePeerDelivery(receiver.ctx, remotePeer,
		delivery.CanonicalJSON(), signature)
	if err != nil || staged.State() != PeerAdmissionStateStaged {
		t.Fatalf("stage Artifact-free delivery = %#v, %v", staged, err)
	}
	accepted, err := receiver.store.AdmitPeerDelivery(receiver.ctx, delivery.ID())
	if err != nil || accepted.State() != PeerAdmissionStateAccepted {
		t.Fatalf("admit Artifact-free delivery = %#v, %v", accepted, err)
	}
}

func requirePendingDeliveryForRoute(t *testing.T, fixture *authorityFixture,
	route agency.RouteID,
) agency.PeerDelivery {
	t.Helper()
	pending, err := fixture.store.PendingPeerDeliveries(fixture.ctx, MaxPendingPeerDeliveries)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range pending {
		if item.Route().RouteID() == route {
			return item.Delivery()
		}
	}
	t.Fatalf("no pending delivery for route %s", route.String())
	return agency.PeerDelivery{}
}

func assertPeerReplyBinding(t *testing.T, fixture *authorityFixture,
	delivery agency.DeliveryID, wantHandling string, wantRoot agency.EventRef,
) {
	t.Helper()
	var handling, rootID, rootDigest string
	err := fixture.store.db.QueryRow(`SELECT reply_anchor_handling_id,
		expected_reply_root_event_id, expected_reply_root_event_digest
		FROM peer_outbox WHERE delivery_id = ?`, delivery.String()).
		Scan(&handling, &rootID, &rootDigest)
	if err != nil {
		t.Fatal(err)
	}
	if handling != wantHandling || rootID != wantRoot.ID().String() ||
		rootDigest != wantRoot.Digest().String() {
		t.Fatalf("reply binding = handling:%q root:%q/%q; want %q %v",
			handling, rootID, rootDigest, wantHandling, wantRoot)
	}
}

func assertPeerReplyBindingAbsent(t *testing.T, fixture *authorityFixture,
	delivery agency.DeliveryID,
) {
	t.Helper()
	var handling, rootID, rootDigest sql.NullString
	err := fixture.store.db.QueryRow(`SELECT reply_anchor_handling_id,
		expected_reply_root_event_id, expected_reply_root_event_digest
		FROM peer_outbox WHERE delivery_id = ?`, delivery.String()).
		Scan(&handling, &rootID, &rootDigest)
	if err != nil {
		t.Fatal(err)
	}
	if handling.Valid || rootID.Valid || rootDigest.Valid {
		t.Fatalf("terminal reply outbox acquired reply binding: %#v %#v %#v",
			handling, rootID, rootDigest)
	}
}
