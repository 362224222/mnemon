package store

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestPutPeerInboxCommitsPublicationAndCursorBeforeSemanticApplication(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "commit", 0)
	publication := fixture.publication(t, 1, 1, "first", true)
	result, err := fixture.store.PutPeerInbox(context.Background(), PutPeerInboxSpec{
		Publication: publication, TransportPeerID: fixture.remote.Identity().PeerID(),
		ArrivalSource: model.ArrivalPull, ReceivedAt: fixture.at})
	if err != nil || result.Disposition != PeerInboxStored || result.InboxID.IsZero() ||
		result.Cursor.BaselineChannelSequence != 0 || result.Cursor.ContiguousChannelSequence != 1 ||
		result.Cursor.ObservedChannelSequence != 1 {
		t.Fatalf("PutPeerInbox() = (%#v,%v)", result, err)
	}
	var status, source string
	var semanticNonce []byte
	var audience, inboxCount, eventCount int
	if err := fixture.store.db.QueryRow(`SELECT status,arrival_source,is_audience,semantic_nonce
		FROM peer_inbox WHERE inbox_id=?`, result.InboxID.String()).
		Scan(&status, &source, &audience, &semanticNonce); err != nil ||
		status != "stored" || source != "pull" || audience != 1 || len(semanticNonce) != 32 {
		t.Fatalf("durable Inbox = (%q,%q,%d,%d bytes,%v)",
			status, source, audience, len(semanticNonce), err)
	}
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM peer_inbox`).Scan(&inboxCount); err != nil || inboxCount != 1 {
		t.Fatalf("Inbox count = (%d,%v)", inboxCount, err)
	}
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_id=?`,
		publication.Event().ID().String()).Scan(&eventCount); err != nil || eventCount != 0 {
		t.Fatalf("semantic Event count before worker = (%d,%v)", eventCount, err)
	}
}

func TestPutPeerInboxReplayAndGapRepairAreMonotonic(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "repair", 0)
	first := fixture.publication(t, 1, 1, "first", true)
	firstResult := fixture.put(t, first, fixture.at)
	var firstNonce []byte
	if err := fixture.store.db.QueryRow(`SELECT semantic_nonce FROM peer_inbox WHERE inbox_id=?`,
		firstResult.InboxID.String()).Scan(&firstNonce); err != nil {
		t.Fatal(err)
	}
	replay := fixture.put(t, first, fixture.at.Add(time.Second))
	if replay.Disposition != PeerInboxDuplicate || replay.InboxID != firstResult.InboxID ||
		replay.Cursor.ContiguousChannelSequence != 1 {
		t.Fatalf("replay = %#v, first %#v", replay, firstResult)
	}
	var replayNonce []byte
	if err := fixture.store.db.QueryRow(`SELECT semantic_nonce FROM peer_inbox WHERE inbox_id=?`,
		firstResult.InboxID.String()).Scan(&replayNonce); err != nil || !bytes.Equal(replayNonce, firstNonce) {
		t.Fatalf("replay semantic nonce changed: equal=%t error=%v", bytes.Equal(replayNonce, firstNonce), err)
	}
	third := fixture.publication(t, 3, 3, "third", true)
	gap := fixture.put(t, third, fixture.at.Add(2*time.Second))
	if gap.Cursor.ContiguousChannelSequence != 1 || gap.Cursor.ObservedChannelSequence != 3 {
		t.Fatalf("gap cursor = %#v", gap.Cursor)
	}
	second := fixture.publication(t, 2, 2, "second", true)
	repaired := fixture.put(t, second, fixture.at.Add(3*time.Second))
	if repaired.Cursor.ContiguousChannelSequence != 3 || repaired.Cursor.ObservedChannelSequence != 3 {
		t.Fatalf("repaired cursor = %#v", repaired.Cursor)
	}
	var count int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM peer_inbox`).Scan(&count); err != nil || count != 3 {
		t.Fatalf("Inbox count = (%d,%v)", count, err)
	}
	rows, err := fixture.store.db.Query(`SELECT semantic_nonce FROM peer_inbox ORDER BY inbox_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var nonces [][]byte
	for rows.Next() {
		var nonce []byte
		if err := rows.Scan(&nonce); err != nil {
			t.Fatal(err)
		}
		nonces = append(nonces, nonce)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(nonces) != 3 || bytes.Equal(nonces[0], nonces[1]) || bytes.Equal(nonces[0], nonces[2]) ||
		bytes.Equal(nonces[1], nonces[2]) {
		t.Fatalf("audience semantic nonces are not pairwise distinct: %x", nonces)
	}
}

func TestPutPeerInboxFailsClosedWhenAboveBaselineEventLacksInboxEvidence(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "event-without-inbox", 0)
	publication := fixture.publication(t, 1, 1, "event-without-inbox", true)
	insertImportedEventWithoutInbox(t, fixture.store, publication)

	result, err := fixture.store.PutPeerInbox(context.Background(), PutPeerInboxSpec{
		Publication: publication, TransportPeerID: fixture.remote.Identity().PeerID(),
		ArrivalSource: model.ArrivalPull, ReceivedAt: fixture.at})
	if err == nil {
		t.Errorf("above-baseline Event without Inbox = %#v, want fail-closed error", result)
	}
	var contiguous, observed uint64
	if scanErr := fixture.store.db.QueryRow(`SELECT contiguous_channel_seq,observed_channel_seq
		FROM peer_cursors`).Scan(&contiguous, &observed); scanErr != nil || contiguous != 0 || observed != 0 {
		t.Errorf("cursor after failed replay = (%d,%d,%v), want (0,0,nil)", contiguous, observed, scanErr)
	}
	for _, table := range []string{"peer_inbox", "publication_conflicts", "origin_quarantines"} {
		var count int
		if scanErr := fixture.store.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); scanErr != nil || count != 0 {
			t.Errorf("%s count after failed replay = (%d,%v), want zero", table, count, scanErr)
		}
	}
}

func TestPutPeerInboxBareEventCannotFillCursorGap(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "bare-event-gap", 0)
	first := fixture.publication(t, 1, 1, "bare-event-gap-first", true)
	insertImportedEventWithoutInbox(t, fixture.store, first)

	second := fixture.publication(t, 2, 2, "bare-event-gap-second", true)
	result := fixture.put(t, second, fixture.at.Add(time.Second))
	if result.Disposition != PeerInboxStored || result.Cursor.ContiguousChannelSequence != 0 ||
		result.Cursor.ObservedChannelSequence != 2 {
		t.Errorf("gap result = %#v, want stored cursor contiguous=0 observed=2", result)
	}
	for _, table := range []string{"publication_conflicts", "origin_quarantines"} {
		var count int
		if err := fixture.store.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Errorf("%s count after gap arrival = (%d,%v), want zero", table, count, err)
		}
	}
}

func TestPutPeerInboxPersistsIgnoredNonaudienceWithoutDomainEffect(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxObserverFixture(t, "ignored")
	publication := fixture.publication(t, 1, 1, "observer", false)
	result := fixture.put(t, publication, fixture.at)
	if result.Disposition != PeerInboxIgnored || result.Cursor.ContiguousChannelSequence != 1 {
		t.Fatalf("ignored result = %#v", result)
	}
	var status string
	var audience, nonceIsNull int
	if err := fixture.store.db.QueryRow(`SELECT status,is_audience,semantic_nonce IS NULL
		FROM peer_inbox`).Scan(&status, &audience, &nonceIsNull); err != nil ||
		status != "ignored" || audience != 0 || nonceIsNull != 1 {
		t.Fatalf("ignored Inbox = (%q,%d,nonce NULL %d,%v)", status, audience, nonceIsNull, err)
	}
}

func TestPutPeerInboxRecordsEquivocationAndQuarantinesOriginAtomically(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "conflict", 0)
	first := fixture.publication(t, 1, 1, "incumbent", true)
	fixture.put(t, first, fixture.at)
	challenger := fixture.publication(t, 1, 2, "challenger", true)
	conflict := fixture.put(t, challenger, fixture.at.Add(time.Second))
	if conflict.Disposition != PeerInboxConflicted || conflict.ConflictID == "" ||
		conflict.Cursor.ObservedChannelSequence != 2 || conflict.Cursor.ContiguousChannelSequence != 2 {
		t.Fatalf("conflict result = %#v", conflict)
	}
	var conflicts, quarantines, inboxes int
	var reason string
	for table, target := range map[string]*int{"publication_conflicts": &conflicts,
		"origin_quarantines": &quarantines, "peer_inbox": &inboxes} {
		if err := fixture.store.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(target); err != nil {
			t.Fatal(err)
		}
	}
	if conflicts != 1 || quarantines != 1 || inboxes != 1 {
		t.Fatalf("durable counts = conflicts %d quarantines %d inboxes %d", conflicts, quarantines, inboxes)
	}
	if err := fixture.store.db.QueryRow(`SELECT reason FROM publication_conflicts`).Scan(&reason); err != nil ||
		reason != "origin_key_conflict" {
		t.Fatalf("conflict reason = (%q,%v)", reason, err)
	}
	replay := fixture.put(t, challenger, fixture.at.Add(2*time.Second))
	if replay.Disposition != PeerInboxConflicted || replay.ConflictID != conflict.ConflictID {
		t.Fatalf("conflict replay = %#v, first %#v", replay, conflict)
	}
	third := fixture.publication(t, 2, 3, "after-quarantine", true)
	_, err := fixture.store.PutPeerInbox(context.Background(), PutPeerInboxSpec{Publication: third,
		TransportPeerID: fixture.remote.Identity().PeerID(), ArrivalSource: model.ArrivalPull,
		ReceivedAt: fixture.at.Add(3 * time.Second)})
	if !errors.Is(err, ErrPeerInboxQuarantined) {
		t.Fatalf("post-quarantine error = %v", err)
	}
}

func TestPutPeerInboxClassifiesEveryImmutableIdentityConflict(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		challenger func(*testing.T, peerInboxFixture, model.SignedPublication) model.SignedPublication
	}{
		"digest_conflict": {challenger: func(t *testing.T, fixture peerInboxFixture,
			incumbent model.SignedPublication,
		) model.SignedPublication {
			spec := incumbent.Event().Spec()
			spec.Payload, _ = model.NewJSON([]byte(`{"content":"changed signed meaning","iteration":1,"work_version":1}`))
			return fixture.signEvent(t, spec)
		}},
		"origin_key_conflict": {challenger: func(t *testing.T, fixture peerInboxFixture,
			_ model.SignedPublication,
		) model.SignedPublication {
			return fixture.publication(t, 1, 2, "origin-key-challenger", true)
		}},
		"publication_key_conflict": {challenger: func(t *testing.T, fixture peerInboxFixture,
			_ model.SignedPublication,
		) model.SignedPublication {
			return fixture.publication(t, 2, 1, "publication-key-challenger", true)
		}},
		"event_key_conflict": {challenger: func(t *testing.T, fixture peerInboxFixture,
			incumbent model.SignedPublication,
		) model.SignedPublication {
			candidate := fixture.publication(t, 2, 2, "event-key-challenger", true)
			spec := candidate.Event().Spec()
			spec.ID = incumbent.Event().ID()
			return fixture.signEvent(t, spec)
		}},
	}
	for reason, test := range cases {
		t.Run(reason, func(t *testing.T) {
			fixture := newPeerInboxFixture(t, "reason-"+reason, 0)
			incumbent := fixture.publication(t, 1, 1, "reason-incumbent", true)
			fixture.put(t, incumbent, fixture.at)
			challenger := test.challenger(t, fixture, incumbent)
			result := fixture.put(t, challenger, fixture.at.Add(time.Second))
			if result.Disposition != PeerInboxConflicted {
				t.Fatalf("conflict result = %#v", result)
			}
			var got string
			if err := fixture.store.db.QueryRow(`SELECT reason FROM publication_conflicts`).Scan(&got); err != nil || got != reason {
				t.Fatalf("conflict reason = (%q,%v), want %q", got, err, reason)
			}
		})
	}
}

func TestPutPeerInboxDetectsMaterializedEventCollisionBeforeCursorACK(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "event-collision", 0)
	candidate := fixture.publication(t, 1, 1, "materialized-challenger", true)
	owner := fixture.channel.Owner()
	ownerMember, ok := fixture.channel.Roster().CurrentMember(owner.PeerID())
	if !ok {
		t.Fatal("owner is absent from fixture roster")
	}
	workID, _ := model.ParseWorkID("work-inbox-materialized-local")
	work, _ := model.NewWorkRef(owner.PeerID(), workID)
	scope, err := model.NewEventScope(fixture.channel.Channel().ID(), owner.PeerID(),
		owner.OriginEpoch(), 1, 1, ownerMember.Head(), fixture.channel.Roster().Head(), work)
	if err != nil {
		t.Fatal(err)
	}
	audience, _ := model.NewAudience([]model.PeerID{fixture.remote.Identity().PeerID()})
	payload, _ := model.NewJSON([]byte(`{"content":"local event identity incumbent","iteration":1,"work_version":1}`))
	incumbent := fixture.signEventAs(t, model.EventSpec{ID: candidate.Event().ID(), Scope: scope,
		Source: model.EventSourceLocal, ActorPrincipal: "principal-inbox-local",
		Type: model.EventReviewOffered, Audience: audience, Summary: "local identity incumbent",
		Payload: payload, CreatedAt: fixture.at.Add(-time.Second), AcceptedAt: fixture.at.Add(-time.Second)}, owner)
	tx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := insertAcceptedEvent(context.Background(), tx, incumbent); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	spec := candidate.Event().Spec()
	spec.ID = incumbent.Event().ID()
	challenger := fixture.signEvent(t, spec)
	result := fixture.put(t, challenger, fixture.at.Add(time.Second))
	if result.Disposition != PeerInboxConflicted || result.Cursor.ContiguousChannelSequence != 1 ||
		result.Cursor.ObservedChannelSequence != 1 {
		t.Fatalf("materialized collision result = %#v", result)
	}
	var reason string
	var existing sql.NullString
	if err := fixture.store.db.QueryRow(`SELECT reason,existing_inbox_id FROM publication_conflicts`).
		Scan(&reason, &existing); err != nil || reason != "event_key_conflict" || existing.Valid {
		t.Fatalf("materialized conflict = (%q,%#v,%v)", reason, existing, err)
	}
	var inboxes int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM peer_inbox`).Scan(&inboxes); err != nil || inboxes != 0 {
		t.Fatalf("Inbox rows after Event collision = (%d,%v)", inboxes, err)
	}
}

func TestPutPeerInboxFailsClosedOnCorruptIncumbentProjection(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "corrupt-incumbent", 0)
	publication := fixture.publication(t, 1, 1, "corrupt-incumbent", true)
	fixture.put(t, publication, fixture.at)
	mustExec(t, fixture.store, `DROP TRIGGER peer_inbox_identity_immutable`)
	mustExec(t, fixture.store, `UPDATE peer_inbox SET publication_digest=?`,
		model.Sum([]byte("corrupt incumbent digest")).Bytes())
	if _, err := fixture.store.PutPeerInbox(context.Background(), PutPeerInboxSpec{
		Publication: publication, TransportPeerID: fixture.remote.Identity().PeerID(),
		ArrivalSource: model.ArrivalPull, ReceivedAt: fixture.at.Add(time.Second)}); err == nil {
		t.Fatal("corrupt incumbent projection was treated as replay or equivocation")
	}
	for _, table := range []string{"publication_conflicts", "origin_quarantines"} {
		var count int
		if err := fixture.store.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("%s count = (%d,%v), want zero", table, count, err)
		}
	}
}

func TestPutPeerInboxRejectsUnreadyForgedAndStaleAuthority(t *testing.T) {
	t.Parallel()
	t.Run("baseline not installed", func(t *testing.T) {
		fixture := newPeerInboxUnreadyFixture(t, "unready")
		publication := fixture.publication(t, 1, 1, "unready", true)
		_, err := fixture.store.PutPeerInbox(context.Background(), PutPeerInboxSpec{
			Publication: publication, TransportPeerID: fixture.remote.Identity().PeerID(),
			ArrivalSource: model.ArrivalPull, ReceivedAt: fixture.at})
		if !errors.Is(err, ErrPeerInboxAuthority) {
			t.Fatalf("unready error = %v", err)
		}
	})
	t.Run("forged signature", func(t *testing.T) {
		fixture := newPeerInboxFixture(t, "forged", 0)
		valid := fixture.publication(t, 1, 1, "forged", true)
		forged, _ := model.AttachSignature(valid.Body(), make([]byte, ed25519.SignatureSize))
		_, err := fixture.store.PutPeerInbox(context.Background(), PutPeerInboxSpec{
			Publication: forged, TransportPeerID: fixture.remote.Identity().PeerID(),
			ArrivalSource: model.ArrivalPull, ReceivedAt: fixture.at})
		if !errors.Is(err, ErrPeerInboxAuthority) {
			t.Fatalf("forged error = %v", err)
		}
	})
	t.Run("prebaseline is covered", func(t *testing.T) {
		fixture := newPeerInboxFixture(t, "covered", 4)
		publication := fixture.publication(t, 1, 3, "covered", true)
		result := fixture.put(t, publication, fixture.at)
		if result.Disposition != PeerInboxCovered || result.Cursor.ContiguousChannelSequence != 4 {
			t.Fatalf("covered result = %#v", result)
		}
		var count int
		if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM peer_inbox`).Scan(&count); err != nil || count != 0 {
			t.Fatalf("covered Inbox count = (%d,%v)", count, err)
		}
	})
}

func TestPutPeerInboxRejectsPendingPressureBeforeCursorACK(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "pressure", 0)
	mustExec(t, fixture.store, `INSERT INTO peer_inbox_pressure(channel_id,pending_bytes)
		VALUES(?,67108864)`, fixture.channel.Channel().ID().String())
	publication := fixture.publication(t, 1, 1, "pressure", true)
	_, err := fixture.store.PutPeerInbox(context.Background(), PutPeerInboxSpec{
		Publication: publication, TransportPeerID: fixture.remote.Identity().PeerID(),
		ArrivalSource: model.ArrivalPull, ReceivedAt: fixture.at})
	if !errors.Is(err, ErrPeerInboxPressure) {
		t.Fatalf("pressure error = %v", err)
	}
	var inboxes int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM peer_inbox`).Scan(&inboxes); err != nil || inboxes != 0 {
		t.Fatalf("Inbox count = (%d,%v)", inboxes, err)
	}
	var contiguous, observed uint64
	if err := fixture.store.db.QueryRow(`SELECT contiguous_channel_seq,observed_channel_seq
		FROM peer_cursors`).Scan(&contiguous, &observed); err != nil || contiguous != 0 || observed != 0 {
		t.Fatalf("cursor after pressure rejection = (%d,%d,%v)", contiguous, observed, err)
	}
}

func TestPutPeerInboxRejectsNodePressureBeforeCursorACK(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "node-pressure", 0)
	mustExec(t, fixture.store, `UPDATE peer_inbox_node_pressure SET pending_bytes=268435456
		WHERE singleton_id=1`)
	publication := fixture.publication(t, 1, 1, "node-pressure", true)
	_, err := fixture.store.PutPeerInbox(context.Background(), PutPeerInboxSpec{
		Publication: publication, TransportPeerID: fixture.remote.Identity().PeerID(),
		ArrivalSource: model.ArrivalPull, ReceivedAt: fixture.at})
	if !errors.Is(err, ErrPeerInboxPressure) {
		t.Fatalf("Node pressure error = %v", err)
	}
	var inboxes int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM peer_inbox`).Scan(&inboxes); err != nil || inboxes != 0 {
		t.Fatalf("Inbox count = (%d,%v)", inboxes, err)
	}
	var contiguous, observed uint64
	if err := fixture.store.db.QueryRow(`SELECT contiguous_channel_seq,observed_channel_seq
		FROM peer_cursors`).Scan(&contiguous, &observed); err != nil || contiguous != 0 || observed != 0 {
		t.Fatalf("cursor after Node pressure rejection = (%d,%d,%v)", contiguous, observed, err)
	}
}

type peerInboxFixture struct {
	channelBaselineFixture
	observer *testkit.MemberFixture
}

func newPeerInboxUnreadyFixture(t *testing.T, seed string) peerInboxFixture {
	t.Helper()
	return peerInboxFixture{channelBaselineFixture: newChannelBaselineFixture(t,
		"inbox-"+seed, model.TopicJoined)}
}

func newPeerInboxFixture(t *testing.T, seed string, baseline uint64) peerInboxFixture {
	t.Helper()
	fixture := newPeerInboxUnreadyFixture(t, seed)
	_, err := fixture.store.InstallInboundChannelBaseline(context.Background(),
		InstallInboundChannelBaselineSpec{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
			Baseline: fixture.remoteBaseline(baseline), At: fixture.at})
	if err != nil {
		t.Fatal(err)
	}
	fixture.at = fixture.at.Add(time.Second)
	return fixture
}

func newPeerInboxObserverFixture(t *testing.T, seed string) peerInboxFixture {
	t.Helper()
	st := openTestStore(t)
	channel := testkit.NewSignedChannel(t, "baseline-inbox-"+seed)
	remote := channel.AppendActive(t, "baseline-inbox-"+seed+"-remote")
	observer := channel.AppendActive(t, "baseline-inbox-"+seed+"-observer")
	insertChannelTestNode(t, st.db, channel.Owner(), channel.Channel().CreatedAt())
	insertSignedChannelFixture(t, st.db, channel, model.TopicJoined)
	insertSignedPeerBinding(t, st.db, channel.Channel().ID(), remote, "inbox-remote",
		model.BindingPending, model.ReachabilityUnknown, remote.Member().CreatedAt())
	insertSignedPeerBinding(t, st.db, channel.Channel().ID(), observer, "inbox-observer",
		model.BindingPending, model.ReachabilityUnknown, observer.Member().CreatedAt())
	mustExec(t, st, `INSERT INTO publication_epochs(channel_id,origin_peer_id,origin_epoch,
		source_floor_channel_seq,source_head_channel_seq,updated_at) VALUES(?,?,?,1,0,?)`,
		channel.Channel().ID().String(), channel.Owner().PeerID().String(),
		channel.Owner().OriginEpoch().String(), storeTime(channel.Channel().UpdatedAt()))
	fixture := peerInboxFixture{channelBaselineFixture: channelBaselineFixture{store: st,
		channel: channel, remote: remote, at: channel.Channel().UpdatedAt().Add(time.Second)},
		observer: &observer}
	_, err := st.InstallInboundChannelBaseline(context.Background(),
		InstallInboundChannelBaselineSpec{AuthenticatedPeerID: remote.Identity().PeerID(),
			Baseline: fixture.remoteBaseline(0), At: fixture.at})
	if err != nil {
		t.Fatal(err)
	}
	fixture.at = fixture.at.Add(time.Second)
	return fixture
}

func (fixture peerInboxFixture) publication(t *testing.T, originSequence, channelSequence uint64,
	suffix string, localAudience bool,
) model.SignedPublication {
	t.Helper()
	local := fixture.channel.Owner().PeerID()
	target := local
	if !localAudience {
		if fixture.observer == nil {
			t.Fatal("non-audience publication fixture requires an active observer")
		}
		target = fixture.observer.Identity().PeerID()
	}
	workID, _ := model.ParseWorkID(fmt.Sprintf("work-inbox-%s-%d", suffix, channelSequence))
	work, _ := model.NewWorkRef(local, workID)
	scope, err := model.NewEventScope(fixture.channel.Channel().ID(),
		fixture.remote.Identity().PeerID(), fixture.remote.Identity().OriginEpoch(), originSequence,
		channelSequence, fixture.remote.Member().Head(), fixture.channel.Roster().Head(), work)
	if err != nil {
		t.Fatal(err)
	}
	audience, _ := model.NewAudience([]model.PeerID{target})
	eventID, _ := model.ParseEventID(fmt.Sprintf("event-inbox-%s-%d", suffix, channelSequence))
	payload, _ := model.NewJSON([]byte(`{"content":"inbox fixture","iteration":1,"work_version":1}`))
	eventType := model.EventReviewAcceptRequested
	if !localAudience {
		eventType = model.EventReviewOutcome
	}
	event, err := model.NewEvent(model.EventSpec{ID: eventID, Scope: scope,
		Source: model.EventSourceLocal, ActorPrincipal: "principal-inbox-remote",
		Type: eventType, Audience: audience, Summary: "inbox fixture",
		Payload: payload, CreatedAt: fixture.at.Add(-time.Second), AcceptedAt: fixture.at.Add(-time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	body, err := model.NewPublicationBody(event)
	if err != nil {
		t.Fatal(err)
	}
	message, _ := model.PublicationSigningMessage(scope.ChannelID(), body.Digest())
	publication, err := model.AttachSignature(body,
		ed25519.Sign(ed25519Private(fixture.remote.Identity()), message))
	if err != nil {
		t.Fatal(err)
	}
	return publication
}

func (fixture peerInboxFixture) signEvent(t *testing.T,
	spec model.EventSpec,
) model.SignedPublication {
	return fixture.signEventAs(t, spec, fixture.remote.Identity())
}

func (fixture peerInboxFixture) signEventAs(t *testing.T, spec model.EventSpec,
	identity testkit.Identity,
) model.SignedPublication {
	t.Helper()
	event, err := model.NewEvent(spec)
	if err != nil {
		t.Fatal(err)
	}
	body, err := model.NewPublicationBody(event)
	if err != nil {
		t.Fatal(err)
	}
	message, err := model.PublicationSigningMessage(event.Scope().ChannelID(), body.Digest())
	if err != nil {
		t.Fatal(err)
	}
	publication, err := model.AttachSignature(body,
		ed25519.Sign(ed25519Private(identity), message))
	if err != nil {
		t.Fatal(err)
	}
	return publication
}

func (fixture peerInboxFixture) put(t *testing.T, publication model.SignedPublication,
	at time.Time,
) PutPeerInboxResult {
	t.Helper()
	result, err := fixture.store.PutPeerInbox(context.Background(), PutPeerInboxSpec{
		Publication: publication, TransportPeerID: fixture.remote.Identity().PeerID(),
		ArrivalSource: model.ArrivalPull, ReceivedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func insertImportedEventWithoutInbox(t *testing.T, store *Store,
	publication model.SignedPublication,
) {
	t.Helper()
	projected, err := model.ProjectImportedPublication(&publication)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := insertAcceptedEvent(context.Background(), tx, projected); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
