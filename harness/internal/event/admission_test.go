package event

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type fixedClock struct{ now time.Time }

func (clock fixedClock) Now() time.Time { return clock.now }

func TestFactoryMapsExactlySevenAgentAndSixControllerCandidates(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 16, 5, 4, 3, 2, time.UTC)
	factory, publicKey := testFactory(t, now)
	offer, _ := NewOfferCandidate("review the implementation", 0)
	accept, _ := NewAcceptCandidate("")
	decline, _ := NewDeclineCandidate("unavailable")
	deliver, _ := NewDeliverCandidate("checks passed")
	rework, _ := NewReworkCandidate("add race coverage")
	closeCandidate, _ := NewCloseCandidate("")
	cancel, _ := NewCancelCandidate("superseded")
	rejected, _ := NewAcceptRejectedDecision("accept-race-lost")
	outcome, _ := NewOutcomeDecision(OutcomeRejected, "stale-work", "decision-a")

	agentCases := []struct {
		name      string
		candidate AgentCandidate
		want      model.EventType
		atHome    bool
	}{
		{"offer", offer, model.EventReviewOffered, true},
		{"accept", accept, model.EventReviewAcceptRequested, false},
		{"decline", decline, model.EventReviewDeclineRequested, false},
		{"deliver", deliver, model.EventReviewDeliveryReady, false},
		{"rework", rework, model.EventReviewReworkRequested, true},
		{"close", closeCandidate, model.EventReviewClosed, true},
		{"cancel", cancel, model.EventReviewCancelled, true},
	}
	controllerCases := []struct {
		name      string
		candidate ControllerCandidate
		want      model.EventType
	}{
		{"accepted", AcceptedDecision{}, model.EventReviewAccepted},
		{"accept rejected", rejected, model.EventReviewAcceptRejected},
		{"delivered", DeliveredDecision{}, model.EventReviewDelivered},
		{"declined", DeclinedDecision{}, model.EventReviewDeclined},
		{"expired", ExpiredDecision{}, model.EventReviewExpired},
		{"fallback outcome", outcome, model.EventReviewOutcome},
	}

	seen := make(map[model.EventType]bool)
	for _, test := range agentCases {
		t.Run("agent "+test.name, func(t *testing.T) {
			stamp := testStamp(t, string(test.want), test.atHome, test.want == model.EventReviewOffered, now)
			bundle, err := factory.AdmitAgent(context.Background(), stamp, test.candidate)
			assertBundle(t, bundle, err, test.want, now, publicKey)
			seen[test.want] = true
		})
	}
	for _, test := range controllerCases {
		t.Run("controller "+test.name, func(t *testing.T) {
			stamp := testStamp(t, string(test.want), true, false, now)
			if test.want == model.EventReviewExpired {
				stamp.deadlineUnixNano = now.UnixNano()
			}
			bundle, err := factory.AdmitController(context.Background(), stamp, test.candidate)
			assertBundle(t, bundle, err, test.want, now, publicKey)
			seen[test.want] = true
		})
	}
	if len(seen) != 13 {
		t.Fatalf("admitted Event families = %d, want 13", len(seen))
	}
}

func TestAdmissionStampOwnsAllAuthorityFields(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 16, 5, 4, 3, 2, time.UTC)
	factory, _ := testFactory(t, now)
	stamp := testStamp(t, "authority", true, true, now)
	candidate, _ := NewOfferCandidate("agent supplied only natural language", 0)
	bundle, err := factory.AdmitAgent(context.Background(), stamp, candidate)
	if err != nil {
		t.Fatalf("AdmitAgent() error = %v", err)
	}
	event := bundle.Event()
	if event.Source() != model.EventSourceLocal || event.ActorPrincipal() != "managed-agent" ||
		event.Scope().OriginSequence() != 41 || event.Scope().ChannelSequence() != 17 ||
		event.AcceptedAt() != now || event.CreatedAt() != now {
		t.Fatalf("trusted authority was not stamped: %#v", event.Spec())
	}
	if bundle.WorkDeadlineUnixNano() != now.Add(DefaultOfferDeadline).UnixNano() {
		t.Fatalf("Factory deadline = %d", bundle.WorkDeadlineUnixNano())
	}
}

func TestAdmissionFailsClosedOnScopeCausalityAndArtifactViolations(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 16, 5, 4, 3, 2, time.UTC)
	factory, _ := testFactory(t, now)
	accept, _ := NewAcceptCandidate("")
	homeStamp := testStamp(t, "wrong-authority", true, false, now)
	if _, err := factory.AdmitAgent(context.Background(), homeStamp, accept); !errors.Is(err, model.ErrInvariant) {
		t.Fatalf("participant action from home error = %v", err)
	}

	stamp := testStamp(t, "missing-cause", false, false, now)
	stamp.causedBy = nil
	if _, err := factory.AdmitAgent(context.Background(), stamp, accept); !errors.Is(err, ErrInvalidStamp) {
		t.Fatalf("missing cause error = %v", err)
	}

	closeCandidate, _ := NewCloseCandidate("")
	stamp = testStamp(t, "forbidden-artifact", true, false, now)
	ref, _ := model.NewArtifactRef(model.Sum([]byte("root")), model.ArtifactProduced)
	stamp.artifacts = []model.ArtifactRef{ref}
	if _, err := factory.AdmitAgent(context.Background(), stamp, closeCandidate); !errors.Is(err, ErrInvalidStamp) {
		t.Fatalf("forbidden Artifact error = %v", err)
	}
}

func TestAdmissionReusesModelArtifactAndCausalityLimits(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 16, 5, 4, 3, 2, time.UTC)
	factory, _ := testFactory(t, now)
	deliver, _ := NewDeliverCandidate("result")
	stamp := testStamp(t, "artifact-limit", false, false, now)
	for index := 0; index <= model.MaxArtifactRefs; index++ {
		ref, _ := model.NewArtifactRef(model.Sum([]byte(fmt.Sprintf("root-%d", index))), model.ArtifactProduced)
		stamp.artifacts = append(stamp.artifacts, ref)
	}
	if _, err := factory.AdmitAgent(context.Background(), stamp, deliver); !errors.Is(err, model.ErrLimit) {
		t.Fatalf("Artifact limit error = %v", err)
	}

	stamp = testStamp(t, "cause-limit", false, false, now)
	stamp.causedBy = nil
	for index := 0; index <= model.MaxCausalityRefs; index++ {
		peer, _ := model.ParsePeerID("source-peer")
		epoch, _ := model.ParseOriginEpoch("source-epoch")
		id, _ := model.ParseEventID(fmt.Sprintf("source-%d", index))
		key, _ := model.NewEventKey(peer, epoch, id)
		stamp.causedBy = append(stamp.causedBy, key)
	}
	if _, err := factory.AdmitAgent(context.Background(), stamp, deliver); !errors.Is(err, model.ErrLimit) {
		t.Fatalf("causality limit error = %v", err)
	}
}

func TestAdmissionStampRejectsInactiveOrMismatchedAuthority(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 16, 5, 4, 3, 2, time.UTC)
	spec := testStampSpec(t, "bad-stamp", true, false, now)
	spec.Profile = testProfile(t, false, "r5-assets")
	if _, err := NewAdmissionStamp(spec); !errors.Is(err, ErrInvalidStamp) {
		t.Fatalf("disabled Profile error = %v", err)
	}
	spec = testStampSpec(t, "bad-revision", true, false, now)
	spec.Profile = testProfile(t, true, "other-assets")
	if _, err := NewAdmissionStamp(spec); !errors.Is(err, ErrInvalidStamp) {
		t.Fatalf("asset mismatch error = %v", err)
	}
	spec = testStampSpec(t, "large-version", true, false, now)
	spec.WorkVersion = model.MaxSQLiteInteger + 1
	if _, err := NewAdmissionStamp(spec); !errors.Is(err, model.ErrLimit) {
		t.Fatalf("counter limit error = %v", err)
	}
}

func TestFactoryRejectsTypedNilCandidateAndZeroFactory(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 16, 5, 4, 3, 2, time.UTC)
	factory, _ := testFactory(t, now)
	stamp := testStamp(t, "nil-candidate", true, true, now)
	var candidate *OfferCandidate
	if _, err := factory.AdmitAgent(context.Background(), stamp, candidate); !errors.Is(err, ErrInvalidCandidate) {
		t.Fatalf("typed nil candidate error = %v", err)
	}
	var zero *Factory
	offer, _ := NewOfferCandidate("review", 0)
	if _, err := zero.AdmitAgent(context.Background(), stamp, offer); err == nil {
		t.Fatal("zero Factory accepted candidate")
	}
}

func assertBundle(t *testing.T, bundle Bundle, err error, want model.EventType,
	now time.Time, publicKey ed25519.PublicKey,
) {
	t.Helper()
	if err != nil {
		t.Fatalf("admission error = %v", err)
	}
	if bundle.Event().Type() != want || bundle.Event().AcceptedAt() != now {
		t.Fatalf("Event type/time = %q/%s, want %q/%s", bundle.Event().Type(), bundle.Event().AcceptedAt(), want, now)
	}
	if bundle.Publication().Event().Digest() != bundle.Event().Digest() {
		t.Fatal("publication did not preserve admitted Event")
	}
	if err := VerifyPublication(publicKey, bundle.Publication()); err != nil {
		t.Fatalf("VerifyPublication() error = %v", err)
	}
}

func testFactory(t *testing.T, now time.Time) (*Factory, ed25519.PublicKey) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	signer, _ := NewEd25519Signer(privateKey)
	factory, err := NewFactory(fixedClock{now}, signer)
	if err != nil {
		t.Fatalf("NewFactory() error = %v", err)
	}
	return factory, publicKey
}

func testStamp(t *testing.T, suffix string, atHome, offered bool, now time.Time) AdmissionStamp {
	t.Helper()
	stamp, err := NewAdmissionStamp(testStampSpec(t, suffix, atHome, offered, now))
	if err != nil {
		t.Fatalf("NewAdmissionStamp() error = %v", err)
	}
	return stamp
}

func testStampSpec(t *testing.T, suffix string, atHome, offered bool, now time.Time) AdmissionStampSpec {
	t.Helper()
	home, _ := model.ParsePeerID("peer-home")
	reviewer, _ := model.ParsePeerID("peer-reviewer")
	origin, target := home, reviewer
	if !atHome {
		origin, target = reviewer, home
	}
	epoch, _ := model.ParseOriginEpoch("epoch-" + origin.String())
	node, _ := model.NewNode(model.NodeSpec{
		PeerID: origin, OriginEpoch: epoch, NextOriginSequence: 42,
		ActiveAssetRevision: "r5-assets", CreatedAt: now, UpdatedAt: now,
	})
	channel, _ := model.ParseChannelID("channel-a")
	workID, _ := model.ParseWorkID("work-a")
	work, _ := model.NewWorkRef(home, workID)
	eventID, _ := model.ParseEventID("event-" + suffix)
	head, _ := model.NewRecordHead(3, model.Sum([]byte("roster")))
	audience, _ := model.NewAudience([]model.PeerID{target})
	sourceID, _ := model.ParseEventID("source-a")
	sourceKey, _ := model.NewEventKey(target, epoch, sourceID)
	deadline := now.Add(time.Hour).UnixNano()
	if offered {
		deadline = 0
	}
	return AdmissionStampSpec{
		Node: node, Profile: testProfile(t, true, "r5-assets"), EventID: eventID,
		ChannelID: channel, WorkRef: work, OriginSequence: 41, ChannelSequence: 17,
		OriginMember: head, PublicationRoster: head, Audience: audience,
		WorkVersion: 1, Iteration: 1, WorkDeadlineUnixNano: deadline,
		CausedBy: []model.EventKey{sourceKey},
	}
}

func testProfile(t *testing.T, enabled bool, revision string) model.Profile {
	t.Helper()
	now := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	budget, _ := model.NewJSON([]byte(`{"max_concurrency":1,"claim_lease_seconds":300,"max_attempts":3,"retry_initial_seconds":5,"retry_max_seconds":300,"max_current_json_bytes":262144,"max_current_artifact_refs":112,"max_current_path_bytes":512}`))
	profile, err := model.NewProfile(model.ProfileSpec{
		ID: model.TeamworkProfileID(), Principal: "managed-agent", WorkspaceRoot: "/workspace",
		Host: model.HostCodex, Runtime: model.RuntimeCodexAppServer,
		CredentialHash: model.Sum([]byte("credential")), ActiveAssetRevision: revision,
		HandlingBudget: budget, Enabled: enabled, CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("NewProfile() error = %v", err)
	}
	return profile
}
