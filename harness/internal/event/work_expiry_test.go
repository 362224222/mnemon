package event

import (
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestAdmitWorkExpiryBuildsPolicyPlannedSignedResult(t *testing.T) {
	t.Parallel()
	spec, signer := workExpiryFixture(t)
	result, err := AdmitWorkExpiry(context.Background(), signer, spec)
	if err != nil {
		t.Fatal(err)
	}
	event := result.Publication().Event()
	next := result.Work()
	if event.ID() != spec.EventID || event.Type() != model.EventReviewExpired ||
		event.Scope() != spec.Scope || !event.AcceptedAt().Equal(spec.AcceptedAt) ||
		len(event.CausedBy()) != 1 || event.CausedBy()[0] != spec.Cause ||
		next.State() != model.WorkExpired || next.Version() != spec.Work.Version()+1 ||
		next.Iteration() != spec.Work.Iteration() || next.UpdatedBy() != spec.EventID {
		t.Fatalf("expiry result = %#v / %#v", event, next)
	}
	if err := next.ValidateUpdateEvent(event); err != nil {
		t.Fatalf("validate next Work: %v", err)
	}
	if err := VerifyPublication(signer.PublicKey(), result.Publication()); err != nil {
		t.Fatalf("verify expiry publication: %v", err)
	}
	if got := event.Payload().String(); got != `{"deadline":"2026-07-19T07:00:00Z","iteration":1,"work_version":1}` {
		t.Fatalf("expiry payload = %s", got)
	}
}

func TestAdmitWorkExpiryClassifiesStaleAndInvariantAuthority(t *testing.T) {
	t.Parallel()
	spec, signer := workExpiryFixture(t)
	early := spec
	early.AcceptedAt = spec.Work.Deadline().Add(-time.Nanosecond)
	if _, err := AdmitWorkExpiry(context.Background(), signer, early); !errors.Is(err, ErrWorkExpiryStale) {
		t.Fatalf("early expiry error = %v", err)
	}

	terminal := spec
	terminalWork := spec.Work.Spec()
	terminalWork.State, terminalWork.Version = model.WorkDelivered, 2
	terminal.Work = mustReviewWork(t, terminalWork)
	if _, err := AdmitWorkExpiry(context.Background(), signer, terminal); !errors.Is(err, ErrWorkExpiryStale) {
		t.Fatalf("terminal expiry error = %v", err)
	}

	incomplete := spec
	incomplete.Cause = model.EventKey{}
	if _, err := AdmitWorkExpiry(context.Background(), signer, incomplete); !errors.Is(err, ErrWorkExpiryInvariant) {
		t.Fatalf("incomplete expiry error = %v", err)
	}
}

func workExpiryFixture(t *testing.T) (WorkExpirySpec, *Ed25519Signer) {
	t.Helper()
	acceptedAt := time.Date(2026, 7, 19, 7, 0, 0, 0, time.UTC)
	authority := testStampSpec(t, "work-expiry", true, false, acceptedAt)
	reviewer, _ := model.ParsePeerID("peer-reviewer")
	participants, err := model.NewParticipantSnapshot(authority.ChannelID,
		authority.PublicationRoster.Revision(), authority.Node.PeerID(), reviewer)
	if err != nil {
		t.Fatal(err)
	}
	state, err := model.NewJSON([]byte(`{"iteration":1,"work_version":1}`))
	if err != nil {
		t.Fatal(err)
	}
	causeID, _ := model.ParseEventID("event-work-expiry-cause")
	cause, err := model.NewEventKey(authority.Node.PeerID(), authority.Node.OriginEpoch(), causeID)
	if err != nil {
		t.Fatal(err)
	}
	work := mustReviewWork(t, model.ReviewWorkSpec{
		Ref: authority.WorkRef, ChannelID: authority.ChannelID, Participants: participants,
		Version: 1, Iteration: 1, DeadlineUnixNano: acceptedAt.UnixNano(), State: model.WorkOffered,
		StateData: state, UpdatedBy: cause.EventID(), UpdatedAt: acceptedAt.Add(-time.Hour),
	})
	scope, err := model.NewEventScope(authority.ChannelID, authority.Node.PeerID(),
		authority.Node.OriginEpoch(), authority.OriginSequence, authority.ChannelSequence,
		authority.OriginMember, authority.PublicationRoster, authority.WorkRef)
	if err != nil {
		t.Fatal(err)
	}
	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := NewEd25519Signer(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return WorkExpirySpec{Work: work, Cause: cause, EventID: authority.EventID,
		Node: authority.Node, Profile: authority.Profile, Scope: scope, AcceptedAt: acceptedAt}, signer
}

func mustReviewWork(t *testing.T, spec model.ReviewWorkSpec) model.ReviewWork {
	t.Helper()
	work, err := model.NewReviewWork(spec)
	if err != nil {
		t.Fatal(err)
	}
	return work
}
