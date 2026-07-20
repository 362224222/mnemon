package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	eventpkg "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/teamwork"
)

func TestCommitWorkExpiryPersistsCompleteEvidenceAndDeterministicIdentity(t *testing.T) {
	fixture, current := newDeadlineWorkFixture(t, "timer-atomic")
	candidate := onlyWorkDeadlineCandidate(t, fixture.store, current.Deadline())
	first, err := fixture.store.PrepareWorkExpiry(context.Background(), candidate, current.Deadline())
	if err != nil {
		t.Fatal(err)
	}
	second, err := fixture.store.PrepareWorkExpiry(context.Background(), candidate, current.Deadline())
	if err != nil {
		t.Fatal(err)
	}
	derived, err := WorkExpiryEventID(first)
	if err != nil || first.EventID() != derived || second.EventID() != derived ||
		first.Work().Ref() != current.Ref() || first.Cause() != candidate.Cause() ||
		!first.AcceptedAt().Equal(current.Deadline()) {
		t.Fatalf("prepared expiry identity = %s/%s (%v)", first.EventID(), second.EventID(), err)
	}

	wrongID, _ := model.ParseEventID("event-work-expiry-wrong-identity")
	wrong := preparedExpiryItem(t, fixture, first, wrongID)
	if _, err := fixture.store.CommitWorkExpiry(context.Background(), WorkExpiryCommitSpec{
		Preparation: first, Expiry: wrong}, current.Deadline()); !errors.Is(err, ErrWorkDeadlineStale) {
		t.Fatalf("wrong deterministic Event ID error = %v", err)
	}
	assertUnexpiredDeadlineWork(t, fixture.store, current.Ref(), current.Version())

	item := preparedExpiryItem(t, fixture, first, first.EventID())
	result, err := fixture.store.CommitWorkExpiry(context.Background(), WorkExpiryCommitSpec{
		Preparation: first, Expiry: item}, current.Deadline().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if operation, receipt, ok := result.RejectedOperation(); ok || !operation.IsZero() || !receipt.IsZero() {
		t.Fatalf("uncontested expiry invented rejection: %s %s", operation, receipt)
	}
	assertCommittedWorkExpiry(t, fixture.store, current, first.EventID(), "pending", "")
	assertAcceptanceHeads(t, fixture.store, 3, 2)

	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.store = nil
	restarted, err := Open(context.Background(), fixture.path)
	if err != nil {
		t.Fatal(err)
	}
	fixture.store = restarted
	scan, err := restarted.ScanWorkDeadlines(context.Background(), current.Deadline().Add(time.Hour))
	if err != nil || len(scan.Due()) != 0 || scan.MoreDue() {
		t.Fatalf("restart post-expiry scan = (%#v, %v)", scan, err)
	}
	assertCommittedWorkExpiry(t, restarted, current, first.EventID(), "pending", "")
}

func TestCommitWorkExpiryFencesDeliveryAuthorityAndPersistsBlockedDiagnostic(t *testing.T) {
	fixture, current := newDeadlineWorkFixture(t, "binding-drift")
	candidate := onlyWorkDeadlineCandidate(t, fixture.store, current.Deadline())
	prepared, err := fixture.store.PrepareWorkExpiry(context.Background(), candidate, current.Deadline())
	if err != nil {
		t.Fatal(err)
	}
	reviewer := current.Participants().ReviewerPeerID()
	signed := acceptanceSignedChannel(t, fixture)
	terminal := signed.AppendTerminal(t, reviewer, model.MemberRevoked)
	merged, err := fixture.store.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
		ChannelID: current.ChannelID(), AuthenticatedTransportPeerID: reviewer,
		Records: []model.Member{terminal.Member()}, At: fixture.now.Add(2 * time.Second),
	})
	if err != nil || merged.Status != ChannelRosterApplied {
		t.Fatalf("revoke reviewer = (%#v, %v)", merged, err)
	}
	item := preparedExpiryItem(t, fixture, prepared, prepared.EventID())
	if _, err := fixture.store.CommitWorkExpiry(context.Background(), WorkExpiryCommitSpec{
		Preparation: prepared, Expiry: item}, current.Deadline()); !errors.Is(err, ErrAdmissionConflict) {
		t.Fatalf("binding TOCTOU error = %v", err)
	}
	assertUnexpiredDeadlineWork(t, fixture.store, current.Ref(), current.Version())

	audience, _ := model.NewAudience([]model.PeerID{reviewer})
	if _, err := fixture.store.PrepareLocalAdmission(context.Background(), current.ChannelID(),
		audience, 1); !errors.Is(err, ErrAudienceUnavailable) && !errors.Is(err, ErrChannelUnavailable) {
		t.Fatalf("ordinary admission after revocation error = %v", err)
	}
	mustExec(t, fixture.store, `DELETE FROM peer_deliveries WHERE event_id=?`, current.UpdatedBy().String())
	_, err = fixture.store.db.Exec(`INSERT INTO peer_deliveries(delivery_id,channel_id,target_peer_id,
		event_id,status,last_error,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)`,
		"delivery-non-expiry-blocked-probe", current.ChannelID().String(), reviewer.String(),
		current.UpdatedBy().String(), "blocked", "probe", storeTime(current.Deadline()), storeTime(current.Deadline()))
	if err == nil || !strings.Contains(err.Error(), "peer delivery binding baseline not ready") {
		t.Fatalf("schema widened blocked delivery exception: %v", err)
	}
	reprepared, err := fixture.store.PrepareWorkExpiry(context.Background(), candidate, current.Deadline())
	if err != nil {
		t.Fatalf("home expiry used ordinary audience baseline gate: %v", err)
	}
	item = preparedExpiryItem(t, fixture, reprepared, reprepared.EventID())
	if _, err := fixture.store.CommitWorkExpiry(context.Background(), WorkExpiryCommitSpec{
		Preparation: reprepared, Expiry: item}, current.Deadline()); err != nil {
		t.Fatal(err)
	}
	assertCommittedWorkExpiry(t, fixture.store, current, reprepared.EventID(),
		"blocked", workExpiryBlockedReason)
}

func TestWorkExpiryIgnoresTopicRecoveryButFreezesInactiveChannel(t *testing.T) {
	t.Run("active Channel topic recovering", func(t *testing.T) {
		fixture, current := newDeadlineWorkFixture(t, "topic-recovering")
		mustExec(t, fixture.store, `UPDATE channels SET topic_state='not_joined' WHERE channel_id=?`,
			current.ChannelID().String())
		candidate := onlyWorkDeadlineCandidate(t, fixture.store, current.Deadline())
		audience, _ := model.NewAudience([]model.PeerID{current.Participants().ReviewerPeerID()})
		if _, err := fixture.store.PrepareLocalAdmission(context.Background(), current.ChannelID(),
			audience, 1); !errors.Is(err, ErrChannelUnavailable) {
			t.Fatalf("ordinary admission with recovering topic error = %v", err)
		}
		prepared, err := fixture.store.PrepareWorkExpiry(context.Background(), candidate, current.Deadline())
		if err != nil {
			t.Fatal(err)
		}
		item := preparedExpiryItem(t, fixture, prepared, prepared.EventID())
		if _, err := fixture.store.CommitWorkExpiry(context.Background(), WorkExpiryCommitSpec{
			Preparation: prepared, Expiry: item}, current.Deadline()); err != nil {
			t.Fatal(err)
		}
		assertCommittedWorkExpiry(t, fixture.store, current, prepared.EventID(), "pending", "")
	})

	t.Run("closed Channel", func(t *testing.T) {
		fixture, current := newDeadlineWorkFixture(t, "closed-channel")
		candidate := onlyWorkDeadlineCandidate(t, fixture.store, current.Deadline())
		mustExec(t, fixture.store, `UPDATE channels SET status='closed',topic_state='left'
			WHERE channel_id=?`, current.ChannelID().String())
		if _, err := fixture.store.PrepareWorkExpiry(context.Background(), candidate,
			current.Deadline()); !errors.Is(err, ErrChannelUnavailable) {
			t.Fatalf("closed Channel preparation error = %v", err)
		}
		assertUnexpiredDeadlineWork(t, fixture.store, current.Ref(), current.Version())
	})
}

func onlyWorkDeadlineCandidate(t *testing.T, store *Store,
	at time.Time,
) WorkDeadlineCandidate {
	t.Helper()
	scan, err := store.ScanWorkDeadlines(context.Background(), at)
	if err != nil {
		t.Fatal(err)
	}
	if len(scan.Due()) != 1 || scan.MoreDue() {
		t.Fatalf("single deadline scan = %#v", scan)
	}
	return scan.Due()[0]
}

func preparedExpiryItem(t *testing.T, fixture *acceptanceFixture,
	preparation WorkExpiryPreparation, eventID model.EventID,
) LocalAcceptanceItem {
	t.Helper()
	work := preparation.Work()
	audience, _ := model.NewAudience([]model.PeerID{work.Participants().ReviewerPeerID()})
	eventScope, err := preparation.Scope().EventScope(0, work.Ref())
	if err != nil {
		t.Fatal(err)
	}
	stamp, err := eventpkg.NewAdmissionStamp(eventpkg.AdmissionStampSpec{
		Node: preparation.Scope().Node(), Profile: preparation.Scope().Profile(), EventID: eventID,
		ChannelID: work.ChannelID(), WorkRef: work.Ref(), OriginSequence: eventScope.OriginSequence(),
		ChannelSequence: eventScope.ChannelSequence(), OriginMember: eventScope.OriginMember(),
		PublicationRoster: eventScope.PublicationRoster(), Audience: audience,
		WorkVersion: work.Version(), Iteration: work.Iteration(),
		WorkDeadlineUnixNano: work.DeadlineUnixNano(), CausedBy: []model.EventKey{preparation.Cause()},
	})
	if err != nil {
		t.Fatal(err)
	}
	signer, _ := eventpkg.NewEd25519Signer(fixture.privateKey)
	factory, _ := eventpkg.NewFactory(acceptanceClock{preparation.AcceptedAt()}, signer)
	bundle, err := factory.AdmitController(context.Background(), stamp, eventpkg.ExpiredDecision{})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := teamwork.PlanExpiry(teamwork.ExpirySpec{Work: work,
		HomePeerID: work.Ref().HomePeerID(), ExpectedVersion: work.Version(),
		NowUnixNano: preparation.AcceptedAt().UnixNano()})
	if err != nil {
		t.Fatal(err)
	}
	nextSpec := work.Spec()
	nextSpec.Version, nextSpec.Iteration, nextSpec.State = intent.NextVersion(),
		intent.NextIteration(), intent.NextState()
	nextSpec.StateData, nextSpec.UpdatedBy, nextSpec.UpdatedAt = bundle.Event().Payload(),
		bundle.Event().ID(), bundle.Event().AcceptedAt()
	next, err := model.NewReviewWork(nextSpec)
	if err != nil {
		t.Fatal(err)
	}
	mutation, err := NewWorkTransition(next, intent.ExpectedVersion(), intent.ExpectedState())
	if err != nil {
		t.Fatal(err)
	}
	return LocalAcceptanceItem{Publication: bundle.Publication(), Work: &mutation}
}

func assertUnexpiredDeadlineWork(t *testing.T, store *Store, ref model.WorkRef,
	wantVersion uint64,
) {
	t.Helper()
	work, err := store.GetReviewWork(context.Background(), ref)
	if err != nil || work.State() != model.WorkOffered || work.Version() != wantVersion {
		t.Fatalf("unexpired Work = (%#v, %v)", work, err)
	}
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_type='review.expired'`).
		Scan(&count); err != nil || count != 0 {
		t.Fatalf("unexpired Event count = %d, err=%v", count, err)
	}
}

func assertCommittedWorkExpiry(t *testing.T, store *Store, prior model.ReviewWork,
	eventID model.EventID, wantDelivery, wantError string,
) {
	t.Helper()
	work, err := store.GetReviewWork(context.Background(), prior.Ref())
	if err != nil || work.State() != model.WorkExpired || work.Version() != prior.Version()+1 ||
		work.UpdatedBy() != eventID {
		t.Fatalf("expired Work = (%#v, %v)", work, err)
	}
	var eventType, publication, delivery string
	var lastError sql.NullString
	err = store.db.QueryRow(`SELECT e.event_type,p.status,d.status,d.last_error FROM events e
		JOIN gossip_publications p ON p.event_id=e.event_id
		JOIN peer_deliveries d ON d.event_id=e.event_id WHERE e.event_id=?`, eventID.String()).
		Scan(&eventType, &publication, &delivery, &lastError)
	if err != nil || eventType != string(model.EventReviewExpired) || publication != "queued" ||
		delivery != wantDelivery || lastError.String != wantError || lastError.Valid != (wantError != "") {
		t.Fatalf("expiry evidence = type=%q publication=%q delivery=%q error=%#v err=%v",
			eventType, publication, delivery, lastError, err)
	}
}
