package store

import (
	"context"
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestCommitLocalAcceptancePersistsExactContextDerivations(t *testing.T) {
	fixture := newAcceptanceFixture(t, 2)
	parent, source := seedDerivationParent(t, fixture)
	contextHash := model.Sum([]byte("parent-read-receipt"))
	operation, authority := fixture.reserveOffer(t, "derived", &contextHash)
	spec := fixture.offer(t, authority, "derived", fixture.reviewers, nil, []model.EventKey{source})
	spec.Derivation = &LocalDerivationParent{ChannelID: parent.ChannelID(), WorkRef: parent.Ref(),
		ExpectedVersion: parent.Version(), UpdatedByEvent: parent.UpdatedBy()}

	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), spec,
		fixture.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	rows, err := fixture.store.db.Query(`SELECT child_ordinal,child_home_peer_id,parent_channel_id,
		parent_home_peer_id,parent_work_id,parent_version,parent_event_id FROM work_derivations
		WHERE operation_id=? ORDER BY child_ordinal`, operation.ID().String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	ordinal := 0
	for rows.Next() {
		var gotOrdinal int
		var childHome, parentChannel, parentHome, parentWork, parentEvent string
		var parentVersion uint64
		if err := rows.Scan(&gotOrdinal, &childHome, &parentChannel, &parentHome, &parentWork,
			&parentVersion, &parentEvent); err != nil {
			t.Fatal(err)
		}
		if gotOrdinal != ordinal || childHome != fixture.node.PeerID().String() ||
			parentChannel != parent.ChannelID().String() || parentHome != parent.Ref().HomePeerID().String() ||
			parentWork != parent.Ref().WorkID().String() || parentVersion != parent.Version() ||
			parentEvent != parent.UpdatedBy().String() {
			t.Fatalf("derivation row %d drifted", ordinal)
		}
		ordinal++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if ordinal != len(fixture.reviewers) {
		t.Fatalf("derivation count = %d, want %d", ordinal, len(fixture.reviewers))
	}
	assertOperationStatus(t, fixture.store, operation.ID(), model.OperationCommitted)
}

func TestCommitLocalAcceptanceRejectsMissingDerivationCausalityWithoutWrites(t *testing.T) {
	fixture := newAcceptanceFixture(t, 1)
	parent, _ := seedDerivationParent(t, fixture)
	contextHash := model.Sum([]byte("missing-parent-cause"))
	operation, authority := fixture.reserveOffer(t, "derived-missing-cause", &contextHash)
	spec := fixture.offer(t, authority, "derived-missing-cause", fixture.reviewers, nil, nil)
	spec.Derivation = &LocalDerivationParent{ChannelID: parent.ChannelID(), WorkRef: parent.Ref(),
		ExpectedVersion: parent.Version(), UpdatedByEvent: parent.UpdatedBy()}

	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), spec,
		fixture.now.Add(time.Second)); err == nil {
		t.Fatal("context-bound offer without exact parent cause was accepted")
	}
	assertAcceptanceCounts(t, fixture.store, []int{1, 1, 2, 0, 0, 0, 0, 0})
	assertAcceptanceHeads(t, fixture.store, 1, 0)
	assertOperationStatus(t, fixture.store, operation.ID(), model.OperationStarted)
	var derivations int
	if err := fixture.store.db.QueryRow("SELECT COUNT(*) FROM work_derivations").Scan(&derivations); err != nil {
		t.Fatal(err)
	}
	if derivations != 0 {
		t.Fatalf("derivations after rejection = %d", derivations)
	}
}

func seedDerivationParent(t *testing.T, fixture *acceptanceFixture) (model.ReviewWork, model.EventKey) {
	t.Helper()
	home := fixture.reviewers[0]
	var memberRevision, rosterRevision uint64
	var memberBytes, rosterBytes []byte
	var epochText string
	if err := fixture.store.db.QueryRow(`SELECT revision,record_hash,origin_epoch FROM channel_members
		WHERE channel_id=? AND member_peer_id=? ORDER BY revision DESC LIMIT 1`, fixture.channel.String(),
		home.String()).Scan(&memberRevision, &memberBytes, &epochText); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.db.QueryRow(`SELECT roster_head_revision,roster_head_hash FROM channels
		WHERE channel_id=?`, fixture.channel.String()).Scan(&rosterRevision, &rosterBytes); err != nil {
		t.Fatal(err)
	}
	memberDigest, _ := model.DigestFromBytes(memberBytes)
	rosterDigest, _ := model.DigestFromBytes(rosterBytes)
	member, _ := model.NewRecordHead(memberRevision, memberDigest)
	roster, _ := model.NewRecordHead(rosterRevision, rosterDigest)
	epoch, _ := model.ParseOriginEpoch(epochText)
	workID, _ := model.ParseWorkID("work-derivation-parent")
	workRef, _ := model.NewWorkRef(home, workID)
	eventID, _ := model.ParseEventID("event-derivation-parent-accepted")
	eventScope, _ := model.NewEventScope(fixture.channel, home, epoch, 1, 1, member, roster, workRef)
	audience, _ := model.NewAudience([]model.PeerID{fixture.node.PeerID()})
	payload, _ := model.NewJSON([]byte(`{"iteration":1,"work_version":1}`))
	acceptedAt := fixture.now.Add(-30 * time.Minute)
	event, err := model.NewEvent(model.EventSpec{ID: eventID, Scope: eventScope,
		Source: model.EventSourceImported, ActorPrincipal: "principal-derivation-parent",
		Type: model.EventReviewAccepted, Audience: audience, Summary: "Parent review accepted",
		Payload: payload, CreatedAt: acceptedAt, AcceptedAt: acceptedAt})
	if err != nil {
		t.Fatal(err)
	}
	body, _ := model.NewPublicationBody(event)
	publication, _ := model.AttachSignature(body, make([]byte, ed25519.SignatureSize))
	tx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := insertAcceptedEvent(context.Background(), tx, publication); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	participants, _ := model.NewParticipantSnapshot(fixture.channel, rosterRevision, home, fixture.node.PeerID())
	parent, err := model.NewReviewWork(model.ReviewWorkSpec{Ref: workRef, ChannelID: fixture.channel,
		Participants: participants, Version: 2, Iteration: 1,
		DeadlineUnixNano: fixture.now.Add(24 * time.Hour).UnixNano(), State: model.WorkActive,
		StateData: payload, UpdatedBy: eventID, UpdatedAt: acceptedAt})
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, fixture.store, `INSERT INTO works(channel_id,home_peer_id,work_id,
		participant_roster_revision,version,iteration,deadline_unix_nano,state,state_json,updated_by_event,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?)`, fixture.channel.String(), home.String(), workID.String(), rosterRevision,
		parent.Version(), parent.Iteration(), parent.DeadlineUnixNano(), string(parent.State()), payload.Bytes(),
		eventID.String(), storeTime(acceptedAt))
	mustExec(t, fixture.store, `INSERT INTO work_members(channel_id,home_peer_id,work_id,peer_id,role)
		VALUES(?,?,?,?,?)`, fixture.channel.String(), home.String(), workID.String(), home.String(),
		string(model.WorkRoleInitiator))
	mustExec(t, fixture.store, `INSERT INTO work_members(channel_id,home_peer_id,work_id,peer_id,role)
		VALUES(?,?,?,?,?)`, fixture.channel.String(), home.String(), workID.String(), fixture.node.PeerID().String(),
		string(model.WorkRoleReviewer))
	return parent, event.Key()
}
