package semantic

import (
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	eventpkg "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestAssemblePeerInboxSemanticResponsesBuildsExactSignedAuthority(t *testing.T) {
	fixture := newSemanticResponseFixture(t, model.EventReviewAcceptRequested, nil)
	payload, _ := model.NewJSON([]byte(`{"iteration":1,"work_version":2}`))
	intent := peerInboxSemanticWorkerIntent{eventType: model.EventReviewAccepted,
		payload: payload, cause: fixture.imported.Key()}
	decision := peerInboxSemanticWorkerDecision{decisionAt: fixture.at,
		responses: []peerInboxSemanticWorkerIntent{intent}}
	publications, err := assemblePeerInboxSemanticResponses(context.Background(), fixture.signer,
		fixture.claim, decision, fixture.scope)
	if err != nil || len(publications) != 1 {
		t.Fatalf("assemble responses = (%d,%v)", len(publications), err)
	}
	publication := publications[0]
	event := publication.Event()
	wantID, _ := store.PeerInboxSemanticResponseEventID(fixture.claim.decisionSeed, 0)
	if event.ID() != wantID || event.Type() != model.EventReviewAccepted ||
		event.Source() != model.EventSourceLocal || event.ActorPrincipal() != fixture.scope.actor ||
		event.Scope().OriginPeerID() != fixture.scope.local ||
		event.Scope().WorkRef() != fixture.imported.Scope().WorkRef() ||
		event.Audience().Len() != 1 || !event.Audience().Contains(fixture.origin) ||
		event.Summary() != store.PeerInboxSemanticResponseSummary(model.EventReviewAccepted) ||
		len(event.CausedBy()) != 1 || event.CausedBy()[0] != fixture.imported.Key() ||
		!event.CreatedAt().Equal(fixture.at) || !event.AcceptedAt().Equal(fixture.at) {
		t.Fatalf("assembled Event = %#v", event)
	}
	if err := eventpkg.VerifyPublication(fixture.publicKey, publication); err != nil {
		t.Fatalf("verify assembled publication: %v", err)
	}
}

func TestAssemblePeerInboxSemanticResponsesConvertsDeliveryClosureToReferences(t *testing.T) {
	root := model.Sum([]byte("semantic-delivery-root"))
	produced, _ := model.NewArtifactRef(root, model.ArtifactProduced)
	fixture := newSemanticResponseFixture(t, model.EventReviewDeliveryReady,
		[]model.ArtifactRef{produced})
	payload, _ := model.NewJSON([]byte(`{"iteration":1,"work_version":3}`))
	intent := peerInboxSemanticWorkerIntent{eventType: model.EventReviewDelivered,
		payload: payload, cause: fixture.imported.Key()}
	publications, err := assemblePeerInboxSemanticResponses(context.Background(), fixture.signer,
		fixture.claim, peerInboxSemanticWorkerDecision{decisionAt: fixture.at,
			responses: []peerInboxSemanticWorkerIntent{intent}}, fixture.scope)
	if err != nil || len(publications) != 1 {
		t.Fatalf("assemble delivered response = (%d,%v)", len(publications), err)
	}
	artifacts := publications[0].Event().Artifacts()
	if len(artifacts) != 1 || artifacts[0].RootDigest() != root ||
		artifacts[0].Role() != model.ArtifactReferenced {
		t.Fatalf("delivered Artifact closure = %#v", artifacts)
	}
	intent.cause = model.EventKey{}
	if _, err := peerInboxSemanticResponseArtifacts(fixture.imported, intent); err == nil {
		t.Fatal("uncorrelated delivery response was accepted")
	}
}

func TestAssemblePeerInboxSemanticResponsesFailsClosedAtExternalBoundaries(t *testing.T) {
	fixture := newSemanticResponseFixture(t, model.EventReviewAcceptRequested, nil)
	payload, _ := model.NewJSON([]byte(`{"iteration":1,"work_version":2}`))
	decision := peerInboxSemanticWorkerDecision{decisionAt: fixture.at,
		responses: []peerInboxSemanticWorkerIntent{{eventType: model.EventReviewAccepted,
			payload: payload, cause: fixture.imported.Key()}}}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := assemblePeerInboxSemanticResponses(cancelled, fixture.signer,
		fixture.claim, decision, fixture.scope); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled signing = %v", err)
	}
	if _, err := assemblePeerInboxSemanticResponses(context.Background(), nil,
		fixture.claim, decision, fixture.scope); err == nil {
		t.Fatal("nil signer was accepted")
	}
	if _, err := assemblePeerInboxSemanticResponses(context.Background(), fixture.signer,
		fixture.claim, decision, semanticResponseScope{}); err == nil {
		t.Fatal("incomplete response scope was accepted")
	}
}

type semanticResponseFixture struct {
	at        time.Time
	local     model.PeerID
	origin    model.PeerID
	imported  model.Event
	claim     peerInboxSemanticWorkerClaim
	scope     semanticResponseScope
	signer    eventpkg.PublicationSigner
	publicKey ed25519.PublicKey
}

type semanticResponseScope struct {
	channel model.ChannelID
	local   model.PeerID
	epoch   model.OriginEpoch
	member  model.RecordHead
	roster  model.RecordHead
	actor   string
}

func (scope semanticResponseScope) eventScope(index uint8,
	work model.WorkRef,
) (model.EventScope, error) {
	return model.NewEventScope(scope.channel, scope.local, scope.epoch,
		10+uint64(index), 20+uint64(index), scope.member, scope.roster, work)
}

func (scope semanticResponseScope) principal() string { return scope.actor }

func newSemanticResponseFixture(t *testing.T, eventType model.EventType,
	artifacts []model.ArtifactRef,
) semanticResponseFixture {
	t.Helper()
	at := semanticWorkerTestTime()
	local, _ := model.ParsePeerID("peer-semantic-response-local")
	origin, _ := model.ParsePeerID("peer-semantic-response-origin")
	channel, _ := model.ParseChannelID("channel-semantic-response")
	originEpoch, _ := model.ParseOriginEpoch("epoch-semantic-response-origin")
	localEpoch, _ := model.ParseOriginEpoch("epoch-semantic-response-local")
	workID, _ := model.ParseWorkID("work-semantic-response")
	home := origin
	if eventType.ParticipantInput() {
		home = local
	}
	work, _ := model.NewWorkRef(home, workID)
	member, _ := model.NewRecordHead(1, model.Sum([]byte("semantic-response-member")))
	roster, _ := model.NewRecordHead(2, model.Sum([]byte("semantic-response-roster")))
	scope, err := model.NewEventScope(channel, origin, originEpoch, 1, 1, member, roster, work)
	if err != nil {
		t.Fatal(err)
	}
	audience, _ := model.NewAudience([]model.PeerID{local})
	eventID, _ := model.ParseEventID("event-semantic-response-imported")
	payload, _ := model.NewJSON([]byte(`{"content":"semantic response fixture"}`))
	imported, err := model.NewEvent(model.EventSpec{ID: eventID, Scope: scope,
		Source: model.EventSourceImported, ActorPrincipal: "principal-semantic-origin",
		Type: eventType, Audience: audience, Summary: "semantic response fixture",
		Payload: payload, Artifacts: artifacts, CreatedAt: at, AcceptedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	seed := make([]byte, ed25519.SeedSize)
	copy(seed, []byte("semantic-response-signing-seed"))
	privateKey := ed25519.NewKeyFromSeed(seed)
	signer, err := eventpkg.NewEd25519Signer(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	responseScope := semanticResponseScope{channel: channel, local: local, epoch: localEpoch,
		member: member, roster: roster, actor: "principal-semantic-local"}
	return semanticResponseFixture{at: at, local: local, origin: origin, imported: imported,
		claim: peerInboxSemanticWorkerClaim{imported: imported,
			decisionSeed: model.Sum([]byte("semantic-response-decision")), attempt: 1},
		scope: responseScope, signer: signer,
		publicKey: append(ed25519.PublicKey(nil), privateKey.Public().(ed25519.PublicKey)...)}
}
