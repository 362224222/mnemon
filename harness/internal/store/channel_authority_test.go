package store

import (
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

func TestReadVerifiedChannelAuthorityReconstructsCompleteSignedRoster(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	fixture := testkit.NewSignedChannel(t, "authority-reader")
	remote := fixture.AppendActive(t, "authority-reader-remote")
	insertSignedChannelFixture(t, st.db, fixture, model.TopicJoined)
	tx, err := st.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := readVerifiedChannelAuthority(context.Background(), tx,
		fixture.Owner().PeerID(), fixture.Channel().ID())
	if err != nil {
		t.Fatal(err)
	}
	if authority.channel.ID() != fixture.Channel().ID() || authority.roster.Head() != fixture.Roster().Head() {
		t.Fatalf("verified authority = %#v", authority)
	}
	current, ok := authority.roster.CurrentMember(remote.Identity().PeerID())
	if !ok || current.Head() != remote.Member().Head() {
		t.Fatal("verified authority omitted current remote member")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestReadVerifiedChannelAuthorityAllowsTerminalBindingChurnBeyondActiveLimit(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	fixture := testkit.NewSignedChannel(t, "authority-terminal-binding-churn")
	terminal := make([]testkit.MemberFixture, 0, model.MaxMembersPerChannel)
	for index := 0; index < model.MaxMembersPerChannel; index++ {
		active := fixture.AppendActive(t, fmt.Sprintf("authority-churn-%d", index))
		terminal = append(terminal, fixture.AppendTerminal(t,
			active.Identity().PeerID(), model.MemberRevoked))
	}
	insertSignedChannelFixture(t, st.db, fixture, model.TopicJoined)
	for index, member := range terminal {
		insertSignedPeerBinding(t, st.db, fixture.Channel().ID(), member,
			fmt.Sprintf("former-%d", index), model.BindingRevoked,
			model.ReachabilityUnknown, fixture.Channel().CreatedAt())
	}
	tx, err := st.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	authority, err := readVerifiedChannelAuthority(context.Background(), tx,
		fixture.Owner().PeerID(), fixture.Channel().ID())
	if err != nil {
		t.Fatal(err)
	}
	if len(authority.bindings) != model.MaxMembersPerChannel {
		t.Fatalf("terminal PeerBinding count = %d, want %d", len(authority.bindings),
			model.MaxMembersPerChannel)
	}
}

func TestReadVerifiedChannelAuthorityRejectsActiveReplicaWithoutLocalMembership(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	fixture := testkit.NewSignedChannel(t, "authority-missing-local-membership")
	insertSignedChannelFixture(t, st.db, fixture, model.TopicJoined)
	outsider := testkit.NewIdentity(t, "authority-outsider")
	tx, err := st.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	_, err = readVerifiedChannelAuthority(context.Background(), tx,
		outsider.PeerID(), fixture.Channel().ID())
	if !errors.Is(err, ErrChannelAuthorityInvariant) {
		t.Fatalf("active replica without local membership error = %v", err)
	}
}

func TestReadVerifiedChannelAuthorityRejectsHugeSparseRosterHeadWithoutAllocation(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	fixture := testkit.NewSignedChannel(t, "authority-huge-sparse-head")
	insertSignedChannelFixture(t, st.db, fixture, model.TopicJoined)
	genesis := fixture.OwnerMember().Projection()
	forgedDigest := model.Sum([]byte("forged-huge-sparse-head"))
	createdAt := fixture.Channel().UpdatedAt().Add(time.Second)
	tx, err := st.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO channel_members(channel_id,revision,record_hash,previous_hash,
		member_peer_id,origin_epoch,display_label,public_key,multiaddrs_json,protocols_json,limits_json,
		status,signed_record_json,owner_signature,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		fixture.Channel().ID().String(), model.MaxSQLiteInteger, forgedDigest.Bytes(),
		fixture.OwnerMember().Member().Head().Digest().Bytes(), genesis.MemberPeerID, genesis.OriginEpoch,
		genesis.DisplayLabel, genesis.PublicKey, genesis.MultiaddrsJSON, genesis.ProtocolsJSON,
		genesis.LimitsJSON, genesis.Status, genesis.SignedRecordJSON, genesis.OwnerSignature,
		storeTime(createdAt)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE channels SET roster_head_revision=?,roster_head_hash=?,updated_at=?
		WHERE channel_id=?`, model.MaxSQLiteInteger, forgedDigest.Bytes(), storeTime(createdAt),
		fixture.Channel().ID().String()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	readTx, err := st.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer readTx.Rollback()
	_, err = readVerifiedChannelAuthority(context.Background(), readTx,
		fixture.Owner().PeerID(), fixture.Channel().ID())
	if !errors.Is(err, ErrChannelAuthorityInvariant) {
		t.Fatalf("huge sparse roster error = %v", err)
	}
}

func TestReadVerifiedChannelAuthorityAllowsTerminalIncumbentForConflictedForensics(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	fixture := testkit.NewSignedChannel(t, "authority-owner-terminal-conflict")
	incumbent := fixture.AppendTerminal(t, fixture.Owner().PeerID(), model.MemberLeft)
	challenger := ownerConflictChallenger(t, fixture, incumbent.Member().CreatedAt())
	insertSignedChannelFixture(t, st.db, fixture, model.TopicNotJoined)
	insertSignedConflictFixture(t, st.db, fixture, incumbent, challenger, challenger.OwnerSignature(),
		incumbent.Member().CreatedAt())
	mustExec(t, st, `UPDATE channels SET status='conflicted',topic_state='blocked'
		WHERE channel_id=?`, fixture.Channel().ID().String())
	tx, err := st.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	authority, err := readVerifiedChannelAuthority(context.Background(), tx,
		fixture.Owner().PeerID(), fixture.Channel().ID())
	if err != nil || authority.channel.Status() != model.ChannelConflicted {
		t.Fatalf("conflicted terminal-incumbent authority = (%#v, %v)", authority, err)
	}
}

func TestReadVerifiedChannelAuthorityRejectsForgedConflictChallenger(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	fixture := testkit.NewSignedChannel(t, "authority-forged-conflict")
	incumbent := fixture.AppendTerminal(t, fixture.Owner().PeerID(), model.MemberLeft)
	challenger := ownerConflictChallenger(t, fixture, incumbent.Member().CreatedAt())
	insertSignedChannelFixture(t, st.db, fixture, model.TopicNotJoined)
	forgedSignature := challenger.OwnerSignature()
	forgedSignature[0] ^= 0xff
	insertSignedConflictFixture(t, st.db, fixture, incumbent, challenger, forgedSignature,
		incumbent.Member().CreatedAt())
	mustExec(t, st, `UPDATE channels SET status='conflicted',topic_state='blocked'
		WHERE channel_id=?`, fixture.Channel().ID().String())
	tx, err := st.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	_, err = readVerifiedChannelAuthority(context.Background(), tx,
		fixture.Owner().PeerID(), fixture.Channel().ID())
	if !errors.Is(err, ErrChannelAuthorityInvariant) {
		t.Fatalf("forged conflict challenger error = %v", err)
	}
}

func TestReadVerifiedChannelAuthorityRejectsConflictDetectedBeforeEitherBranch(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	fixture := testkit.NewSignedChannel(t, "authority-conflict-time")
	incumbent := fixture.AppendTerminal(t, fixture.Owner().PeerID(), model.MemberLeft)
	challenger := ownerConflictChallenger(t, fixture, incumbent.Member().CreatedAt())
	insertSignedChannelFixture(t, st.db, fixture, model.TopicNotJoined)
	insertSignedConflictFixture(t, st.db, fixture, incumbent, challenger, challenger.OwnerSignature(),
		incumbent.Member().CreatedAt().Add(-time.Nanosecond))
	mustExec(t, st, `UPDATE channels SET status='conflicted',topic_state='blocked'
		WHERE channel_id=?`, fixture.Channel().ID().String())
	tx, err := st.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	_, err = readVerifiedChannelAuthority(context.Background(), tx,
		fixture.Owner().PeerID(), fixture.Channel().ID())
	if !errors.Is(err, ErrChannelAuthorityInvariant) {
		t.Fatalf("predating conflict evidence error = %v", err)
	}
}

func TestReadVerifiedChannelAuthorityRejectsProjectionThatSchemaCannotInterpret(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	fixture := testkit.NewSignedChannelAt(t, "authority-projection-mismatch",
		time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC))
	projection := fixture.Projection()
	tx, err := st.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO channels(channel_id,name,local_alias,owner_peer_id,owner_public_key,
		descriptor_json,descriptor_digest,descriptor_signature,member_limit,roster_head_revision,
		roster_head_hash,status,topic_state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		projection.ChannelID, projection.Name+" forged", projection.LocalAlias, projection.OwnerPeerID,
		projection.OwnerPublicKey, projection.DescriptorJSON, projection.DescriptorDigest,
		projection.DescriptorSignature, projection.MemberLimit, projection.RosterHeadRevision,
		projection.RosterHeadHash, projection.Status, projection.TopicState, projection.CreatedAt,
		projection.UpdatedAt); err != nil {
		t.Fatal(err)
	}
	member := fixture.OwnerMember().Projection()
	if _, err := tx.Exec(`INSERT INTO channel_members(channel_id,revision,record_hash,previous_hash,
		member_peer_id,origin_epoch,display_label,public_key,multiaddrs_json,protocols_json,limits_json,
		status,signed_record_json,owner_signature,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		member.ChannelID, member.Revision, member.RecordHash, nil, member.MemberPeerID, member.OriginEpoch,
		member.DisplayLabel, member.PublicKey, member.MultiaddrsJSON, member.ProtocolsJSON, member.LimitsJSON,
		member.Status, member.SignedRecordJSON, member.OwnerSignature, member.CreatedAt); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	readTx, err := st.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer readTx.Rollback()
	_, err = readVerifiedChannelAuthority(context.Background(), readTx,
		fixture.Owner().PeerID(), fixture.Channel().ID())
	if !errors.Is(err, ErrChannelAuthorityInvariant) {
		t.Fatalf("projection mismatch error = %v", err)
	}
}

func ownerConflictChallenger(t testing.TB, fixture *testkit.SignedChannel,
	createdAt time.Time,
) model.Member {
	t.Helper()
	genesis := fixture.OwnerMember().Member()
	previous := genesis.Head().Digest()
	record, err := model.NewMemberRecord(model.MemberRecordSpec{
		ChannelID: fixture.Channel().ID(), DescriptorDigest: fixture.Descriptor().Descriptor().Digest(),
		Revision: 2, PreviousDigest: &previous, PeerID: fixture.Owner().PeerID(),
		OriginEpoch: fixture.Owner().OriginEpoch(), DisplayLabel: "owner-conflicting-active-branch",
		PublicKey: fixture.Owner().PublicKey(), Multiaddrs: fixture.Owner().Multiaddrs(),
		Protocols: testkit.ChannelProtocols(), Limits: model.DefaultMemberLimits(),
		Status: model.MemberActive, CreatedAt: createdAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	message, err := model.MemberRecordSigningMessage(record.ChannelID(), record.Digest())
	if err != nil {
		t.Fatal(err)
	}
	member, err := model.AttachMemberSignature(record,
		ed25519.Sign(ed25519Private(fixture.Owner()), message))
	if err != nil {
		t.Fatal(err)
	}
	return member
}

func insertSignedConflictFixture(t testing.TB, db *sql.DB, fixture *testkit.SignedChannel,
	incumbent testkit.MemberFixture, challenger model.Member, challengerSignature []byte,
	detectedAt time.Time,
) {
	t.Helper()
	transport := testkit.NewIdentity(t, "conflict-transport-"+fixture.Channel().ID().String())
	_, err := db.Exec(`INSERT INTO channel_conflicts(conflict_id,channel_id,revision,
		incumbent_record_hash,incumbent_signed_record_json,incumbent_owner_signature,
		challenger_record_hash,challenger_signed_record_json,challenger_owner_signature,
		transport_peer_id,detected_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
		"conflict-"+fixture.Channel().ID().String(), fixture.Channel().ID().String(),
		incumbent.Member().Head().Revision(), incumbent.Member().Head().Digest().Bytes(),
		incumbent.Member().SignedRecord().Bytes(), incumbent.Member().OwnerSignature(),
		challenger.Head().Digest().Bytes(), challenger.SignedRecord().Bytes(), challengerSignature,
		transport.PeerID().String(), storeTime(detectedAt))
	if err != nil {
		t.Fatalf("insert signed conflict fixture: %v", err)
	}
}
