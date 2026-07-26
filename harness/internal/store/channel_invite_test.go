package store

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestRotateChannelInviteIsAtomicReplaySafeAndSecretFree(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := openTestStore(t)
	fixture := testkit.NewSignedChannel(t, "invite-rotation")
	insertChannelTestNode(t, st.db, fixture.Owner(), fixture.Channel().CreatedAt())
	initial := inviteTestGrant(t, fixture, "initial", fixture.Channel().CreatedAt(), 7)
	if _, err := st.CreateChannel(ctx, CreateChannelSpec{Channel: fixture.Channel(),
		Genesis: fixture.OwnerMember().Member(), Token: initial.Token}); err != nil {
		t.Fatal(err)
	}
	sameTime := inviteTestGrant(t, fixture, "same-time", fixture.Channel().CreatedAt(), 1)
	if _, err := st.RotateChannelInvite(ctx, RotateChannelInviteSpec{
		ChannelID: fixture.Channel().ID(), Token: sameTime.Token,
		At: fixture.Channel().CreatedAt(), ExpectedRosterHead: fixture.Roster().Head(),
		ExpectedOpenGrant: inviteTestOpenGrantFence(initial.ID())}); !errors.Is(err, ErrChannelInviteInput) {
		t.Fatalf("same-time rotation error = %v", err)
	}
	assertInviteGrantState(t, st, initial.ID(), "open", 0)

	secondAt := fixture.Channel().CreatedAt().Add(10 * time.Minute)
	second := inviteTestGrant(t, fixture, "second", secondAt, 3)
	rotated, err := st.RotateChannelInvite(ctx, RotateChannelInviteSpec{
		ChannelID: fixture.Channel().ID(), Token: second.Token, At: secondAt,
		ExpectedRosterHead: fixture.Roster().Head(),
		ExpectedOpenGrant:  inviteTestOpenGrantFence(initial.ID())})
	if err != nil || !rotated.Created || rotated.GrantID != second.ID() ||
		rotated.ReplacedGrant != initial.ID() || rotated.RemainingSeats != 7 || rotated.Status != "open" {
		t.Fatalf("RotateChannelInvite() = (%#v, %v)", rotated, err)
	}
	assertInviteGrantState(t, st, initial.ID(), "closed", 0)
	assertInviteGrantState(t, st, second.ID(), "open", 0)
	assertEnrollmentCredentialAbsent(t, st.Path(), second.Token)

	closed, err := st.CloseChannelInvite(ctx, fixture.Channel().ID(), second.ID(),
		secondAt.Add(time.Minute))
	if err != nil || !closed.Changed || closed.Status != "closed" {
		t.Fatalf("CloseChannelInvite() = (%#v, %v)", closed, err)
	}
	thirdAt := secondAt.Add(2 * time.Minute)
	third := inviteTestGrant(t, fixture, "third", thirdAt, 2)
	if _, err := st.RotateChannelInvite(ctx, RotateChannelInviteSpec{
		ChannelID: fixture.Channel().ID(), Token: third.Token, At: thirdAt,
		ExpectedRosterHead: fixture.Roster().Head(),
		ExpectedOpenGrant:  inviteTestOpenGrantFence(model.GrantID{})}); err != nil {
		t.Fatal(err)
	}
	delayedAt := secondAt.Add(90 * time.Second)
	delayed := inviteTestGrant(t, fixture, "delayed", delayedAt, 1)
	if _, err := st.RotateChannelInvite(ctx, RotateChannelInviteSpec{
		ChannelID: fixture.Channel().ID(), Token: delayed.Token, At: delayedAt,
		ExpectedRosterHead: fixture.Roster().Head(),
		ExpectedOpenGrant:  inviteTestOpenGrantFence(third.ID())}); !errors.Is(err, ErrChannelInviteInput) {
		t.Fatalf("backdated delayed rotation error = %v", err)
	}
	assertInviteGrantState(t, st, third.ID(), "open", 0)

	// A lost response may replay the older replacement after a later rotation.
	// Its exact immutable row is returned without touching the current grant.
	replay, err := st.RotateChannelInvite(ctx, RotateChannelInviteSpec{
		ChannelID: fixture.Channel().ID(), Token: second.Token, At: secondAt,
		ExpectedRosterHead: fixture.Roster().Head(),
		ExpectedOpenGrant:  inviteTestOpenGrantFence(initial.ID())})
	if err != nil || replay.Created || replay.GrantID != second.ID() || replay.Status != "closed" {
		t.Fatalf("RotateChannelInvite(old replay) = (%#v, %v)", replay, err)
	}
	closeReplay, err := st.CloseChannelInvite(ctx, fixture.Channel().ID(), second.ID(),
		thirdAt.Add(time.Minute))
	if err != nil || closeReplay.Changed || closeReplay.Status != "closed" {
		t.Fatalf("CloseChannelInvite(old replay) = (%#v, %v)", closeReplay, err)
	}
	assertInviteGrantState(t, st, third.ID(), "open", 0)
}

func TestRotateChannelInviteRejectsCapacityAndConflictWithoutRetiringCurrent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := openTestStore(t)
	owner := testkit.NewIdentity(t, "invite-conflict-owner")
	createdAt := time.Date(2026, 7, 18, 1, 0, 0, 0, time.UTC)
	insertChannelTestNode(t, st.db, owner, createdAt)
	first := testkit.NewSignedChannelForOwnerAt(t, "invite-conflict-first", owner, createdAt)
	firstGrant := inviteTestGrant(t, first, "first-open", createdAt, 7)
	if _, err := st.CreateChannel(ctx, CreateChannelSpec{Channel: first.Channel(),
		Genesis: first.OwnerMember().Member(), Token: firstGrant.Token}); err != nil {
		t.Fatal(err)
	}
	secondAt := createdAt.Add(time.Minute)
	second := testkit.NewSignedChannelForOwnerAt(t, "invite-conflict-second", owner, secondAt)
	collidingID, _ := model.ParseGrantID("grant-invite-collision")
	secondGrant := inviteTestCredentialForID(t, second, collidingID, "second-channel", secondAt, 7)
	if _, err := st.CreateChannel(ctx, CreateChannelSpec{Channel: second.Channel(),
		Genesis: second.OwnerMember().Member(), Token: secondGrant.Token}); err != nil {
		t.Fatal(err)
	}
	collisionAt := createdAt.Add(2 * time.Minute)
	collision := inviteTestCredentialForID(t, first, collidingID, "collision", collisionAt, 1)
	if _, err := st.RotateChannelInvite(ctx, RotateChannelInviteSpec{ChannelID: first.Channel().ID(),
		Token: collision.Token, At: collisionAt, ExpectedRosterHead: first.Roster().Head(),
		ExpectedOpenGrant: inviteTestOpenGrantFence(firstGrant.ID())}); !errors.Is(err, ErrChannelInviteConflict) {
		t.Fatalf("colliding rotation error = %v", err)
	}
	assertInviteGrantState(t, st, firstGrant.ID(), "open", 0)

	full := testkit.NewSignedChannelForOwnerAt(t, "invite-nearly-full", owner,
		createdAt.Add(3*time.Minute))
	for index := 0; index < model.MaxMembersPerChannel-2; index++ {
		full.AppendActive(t, "invite-capacity-member-"+string(rune('a'+index)))
	}
	insertSignedChannelFixture(t, st.db, full, model.TopicNotJoined)
	fullGrant := inviteTestGrant(t, full, "nearly-full-ledger",
		full.Channel().CreatedAt(), 7)
	insertInviteTestGrantWithReceipts(t, st, full, fullGrant.OpenEnrollmentGrant, full.Members()[1:])
	capacityAt := full.Channel().UpdatedAt().Add(time.Minute)
	tooLarge := inviteTestGrant(t, full, "too-large", capacityAt, 2)
	if _, err := st.RotateChannelInvite(ctx, RotateChannelInviteSpec{ChannelID: full.Channel().ID(),
		Token: tooLarge.Token, At: capacityAt, ExpectedRosterHead: full.Roster().Head(),
		ExpectedOpenGrant: inviteTestOpenGrantFence(fullGrant.ID())}); !errors.Is(err, ErrChannelInviteInput) {
		t.Fatalf("over-capacity rotation error = %v", err)
	}
	var count int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM enrollment_grants WHERE channel_id=?`,
		full.Channel().ID().String()).Scan(&count); err != nil || count != 1 {
		t.Fatalf("partial capacity grant count = %d, %v", count, err)
	}
}

func TestRotateChannelInviteRequiresCanonicalPresignFence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := openTestStore(t)
	fixture := testkit.NewSignedChannel(t, "invite-fence-input")
	insertChannelTestNode(t, st.db, fixture.Owner(), fixture.Channel().CreatedAt())
	initial := inviteTestGrant(t, fixture, "fence-input-initial",
		fixture.Channel().CreatedAt(), 7)
	if _, err := st.CreateChannel(ctx, CreateChannelSpec{Channel: fixture.Channel(),
		Genesis: fixture.OwnerMember().Member(), Token: initial.Token}); err != nil {
		t.Fatal(err)
	}
	at := fixture.Channel().CreatedAt().Add(time.Minute)
	candidate := inviteTestGrant(t, fixture, "fence-input-candidate", at, 1)
	for _, test := range []struct {
		name string
		head model.RecordHead
		open ChannelInviteOpenGrantFence
	}{
		{name: "missing roster", open: inviteTestOpenGrantFence(initial.ID())},
		{name: "present without ID", head: fixture.Roster().Head(),
			open: ChannelInviteOpenGrantFence{Present: true}},
		{name: "absent with ID", head: fixture.Roster().Head(),
			open: ChannelInviteOpenGrantFence{GrantID: initial.ID()}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := st.RotateChannelInvite(ctx, RotateChannelInviteSpec{
				ChannelID: fixture.Channel().ID(), Token: candidate.Token, At: at,
				ExpectedRosterHead: test.head, ExpectedOpenGrant: test.open},
			); !errors.Is(err, ErrChannelInviteInput) {
				t.Fatalf("noncanonical fence error = %v", err)
			}
		})
	}
	assertInviteGrantState(t, st, initial.ID(), "open", 0)
	assertInviteGrantAbsent(t, st, candidate.ID())
}

func TestRotateChannelInviteRejectsStalePresignFenceWithoutChangingGrants(t *testing.T) {
	t.Parallel()
	t.Run("roster head", func(t *testing.T) {
		ctx := context.Background()
		st := openTestStore(t)
		fixture := testkit.NewSignedChannel(t, "invite-stale-roster")
		insertChannelTestNode(t, st.db, fixture.Owner(), fixture.Channel().CreatedAt())
		initial := inviteTestGrant(t, fixture, "stale-roster-initial",
			fixture.Channel().CreatedAt(), 7)
		if _, err := st.CreateChannel(ctx, CreateChannelSpec{Channel: fixture.Channel(),
			Genesis: fixture.OwnerMember().Member(), Token: initial.Token}); err != nil {
			t.Fatal(err)
		}
		expectedHead := fixture.Roster().Head()
		staleAt := fixture.Channel().CreatedAt().Add(2 * time.Second)
		stale := inviteTestGrant(t, fixture, "stale-roster-candidate", staleAt, 1)
		ownerUpdate := fixture.AppendActiveUpdate(t, fixture.Owner().PeerID())
		merged, err := st.MergeChannelRoster(ctx, MergeChannelRosterSpec{
			ChannelID: fixture.Channel().ID(), AuthenticatedTransportPeerID: fixture.Owner().PeerID(),
			Records: []model.Member{ownerUpdate.Member()}, At: ownerUpdate.Member().CreatedAt()})
		if err != nil || merged.Status != ChannelRosterApplied {
			t.Fatalf("MergeChannelRoster() = (%#v, %v)", merged, err)
		}

		if _, err := st.RotateChannelInvite(ctx, RotateChannelInviteSpec{
			ChannelID: fixture.Channel().ID(), Token: stale.Token, At: staleAt,
			ExpectedRosterHead: expectedHead,
			ExpectedOpenGrant:  inviteTestOpenGrantFence(initial.ID())},
		); !errors.Is(err, ErrChannelInviteStale) {
			t.Fatalf("stale roster rotation error = %v", err)
		}
		assertInviteGrantState(t, st, initial.ID(), "open", 0)
		assertInviteGrantAbsent(t, st, stale.ID())
	})

	t.Run("open grant", func(t *testing.T) {
		ctx := context.Background()
		st := openTestStore(t)
		fixture := testkit.NewSignedChannel(t, "invite-stale-open-grant")
		insertChannelTestNode(t, st.db, fixture.Owner(), fixture.Channel().CreatedAt())
		initial := inviteTestGrant(t, fixture, "stale-open-initial",
			fixture.Channel().CreatedAt(), 7)
		if _, err := st.CreateChannel(ctx, CreateChannelSpec{Channel: fixture.Channel(),
			Genesis: fixture.OwnerMember().Member(), Token: initial.Token}); err != nil {
			t.Fatal(err)
		}
		staleAt := fixture.Channel().CreatedAt().Add(2 * time.Minute)
		stale := inviteTestGrant(t, fixture, "stale-open-candidate", staleAt, 1)
		staleOperation := ChannelMutationOperation{Kind: ChannelMutationInvite,
			OperationKeyHash: model.Sum([]byte("stale-open-operation-key")),
			RequestDigest:    model.Sum([]byte("stale-open-request"))}
		nextAt := fixture.Channel().CreatedAt().Add(time.Minute)
		next := inviteTestGrant(t, fixture, "stale-open-next", nextAt, 1)
		if _, err := st.RotateChannelInvite(ctx, RotateChannelInviteSpec{
			ChannelID: fixture.Channel().ID(), Token: next.Token, At: nextAt,
			ExpectedRosterHead: fixture.Roster().Head(),
			ExpectedOpenGrant:  inviteTestOpenGrantFence(initial.ID())},
		); err != nil {
			t.Fatal(err)
		}

		if _, err := st.RotateChannelInvite(ctx, RotateChannelInviteSpec{
			ChannelID: fixture.Channel().ID(), Token: stale.Token, At: staleAt,
			ExpectedRosterHead: fixture.Roster().Head(),
			ExpectedOpenGrant:  inviteTestOpenGrantFence(initial.ID()),
			Operation:          &staleOperation},
		); !errors.Is(err, ErrChannelInviteStale) {
			t.Fatalf("stale open grant rotation error = %v", err)
		}
		assertInviteGrantState(t, st, initial.ID(), "closed", 0)
		assertInviteGrantState(t, st, next.ID(), "open", 0)
		assertInviteGrantAbsent(t, st, stale.ID())
		var operationCount int
		if err := st.db.QueryRow(`SELECT COUNT(*) FROM channel_mutation_operations
			WHERE operation_key_hash=?`, staleOperation.OperationKeyHash.Bytes()).
			Scan(&operationCount); err != nil || operationCount != 0 {
			t.Fatalf("stale invite operation count = %d, %v", operationCount, err)
		}
	})
}

func TestRetireOpenChannelInviteRequiresExactCASCardinality(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := openTestStore(t)
	fixture := testkit.NewSignedChannel(t, "invite-retire-cardinality")
	insertChannelTestNode(t, st.db, fixture.Owner(), fixture.Channel().CreatedAt())
	grant := inviteTestGrant(t, fixture, "invite-retire-cardinality-grant",
		fixture.Channel().CreatedAt(), 7)
	if _, err := st.CreateChannel(ctx, CreateChannelSpec{Channel: fixture.Channel(),
		Genesis: fixture.OwnerMember().Member(), Token: grant.Token}); err != nil {
		t.Fatal(err)
	}
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	open, err := readDurableEnrollmentGrant(ctx, tx, grant.ID())
	if err != nil {
		t.Fatal(err)
	}
	at := fixture.Channel().CreatedAt().Add(time.Minute)
	if _, err := tx.ExecContext(ctx, `UPDATE enrollment_grants SET status='closed',closed_at=?
		WHERE grant_id=?`, storeTime(at), grant.ID().String()); err != nil {
		t.Fatal(err)
	}
	if _, err := retireOpenChannelInvite(ctx, tx, at,
		freshChannelInviteRotation{open: open, hasOpen: true},
	); !errors.Is(err, ErrChannelInviteStale) {
		t.Fatalf("retire stale open grant error = %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	assertInviteGrantState(t, st, grant.ID(), "open", 0)
}

func TestCloseChannelInviteExpiresAtItsBoundAndIsIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := openTestStore(t)
	fixture := testkit.NewSignedChannel(t, "invite-expiry")
	insertChannelTestNode(t, st.db, fixture.Owner(), fixture.Channel().CreatedAt())
	grant := inviteTestGrant(t, fixture, "expiring", fixture.Channel().CreatedAt(), 7)
	if _, err := st.CreateChannel(ctx, CreateChannelSpec{Channel: fixture.Channel(),
		Genesis: fixture.OwnerMember().Member(), Token: grant.Token}); err != nil {
		t.Fatal(err)
	}
	joined := fixture.AppendActive(t, "invite-close-after-use")
	useAt := joined.Member().CreatedAt().Add(time.Second)
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	installInviteTestMember(t, ctx, tx, fixture, joined)
	insertInviteTestGrantUse(t, tx, fixture, grant.ID(), joined,
		"use-invite-close-after-use", useAt)
	insertInviteTestReceipt(t, tx, fixture, grant.ID(), joined,
		"use-invite-close-after-use", "close-after-use", useAt)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CloseChannelInvite(ctx, fixture.Channel().ID(), grant.ID(),
		joined.Member().CreatedAt()); !errors.Is(err, ErrChannelInviteInput) {
		t.Fatalf("close before latest use error = %v", err)
	}
	assertInviteGrantState(t, st, grant.ID(), "open", 1)
	result, err := st.CloseChannelInvite(ctx, fixture.Channel().ID(), grant.ID(),
		grant.ExpiresAt().Add(time.Minute))
	if err != nil || !result.Changed || result.Status != "expired" {
		t.Fatalf("CloseChannelInvite(expired) = (%#v, %v)", result, err)
	}
	var closedAt string
	if err := st.db.QueryRow(`SELECT closed_at FROM enrollment_grants WHERE grant_id=?`,
		grant.ID().String()).Scan(&closedAt); err != nil || closedAt != storeTime(grant.ExpiresAt().Add(time.Minute)) {
		t.Fatalf("expired closed_at = %q, %v", closedAt, err)
	}
	replay, err := st.CloseChannelInvite(ctx, fixture.Channel().ID(), grant.ID(),
		grant.ExpiresAt().Add(2*time.Minute))
	if err != nil || replay.Changed || replay.Status != "expired" {
		t.Fatalf("CloseChannelInvite(expired replay) = (%#v, %v)", replay, err)
	}
}

func TestChannelInviteReaderRejectsRetroactiveMemberGrantAttribution(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := openTestStore(t)
	fixture := testkit.NewSignedChannel(t, "invite-retroactive-member")
	insertChannelTestNode(t, st.db, fixture.Owner(), fixture.Channel().CreatedAt())
	initial := inviteTestGrant(t, fixture, "retroactive-initial", fixture.Channel().CreatedAt(), 7)
	createSpec := CreateChannelSpec{Channel: fixture.Channel(),
		Genesis: fixture.OwnerMember().Member(), Token: initial.Token}
	if _, err := st.CreateChannel(ctx, createSpec); err != nil {
		t.Fatal(err)
	}
	joined := fixture.AppendActive(t, "invite-retroactive-joined")
	laterAt := joined.Member().CreatedAt().Add(time.Minute)
	later := inviteTestGrant(t, fixture, "retroactive-later", laterAt, 1)
	useAt := laterAt.Add(time.Minute)

	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	installInviteTestMember(t, ctx, tx, fixture, joined)
	if _, err := tx.Exec(`UPDATE enrollment_grants SET status='closed',closed_at=? WHERE grant_id=?`,
		storeTime(joined.Member().CreatedAt()), initial.ID().String()); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO enrollment_grants(grant_id,channel_id,verifier,expires_at,
		max_uses,used_uses,status,created_at,closed_at) VALUES(?,?,?,?,?,0,'open',?,NULL)`,
		later.ID().String(), later.ChannelID().String(), later.Verifier().Bytes(),
		storeTime(later.ExpiresAt()), later.MaxUses(), storeTime(later.CreatedAt())); err != nil {
		t.Fatal(err)
	}
	// Simulate a Store predating the schema fence; the verified reader must
	// independently reject the same impossible history after restart.
	if _, err := tx.Exec(`DROP TRIGGER enrollment_grant_uses_validate_insert`); err != nil {
		t.Fatal(err)
	}
	insertInviteTestGrantUse(t, tx, fixture, later.ID(), joined,
		"use-invite-retroactive", useAt)
	insertInviteTestReceipt(t, tx, fixture, later.ID(), joined,
		"use-invite-retroactive", "retroactive", useAt)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	replacementAt := useAt.Add(time.Minute)
	replacement := inviteTestGrant(t, fixture, "retroactive-replacement", replacementAt, 1)
	if _, err := st.RotateChannelInvite(ctx, RotateChannelInviteSpec{ChannelID: fixture.Channel().ID(),
		Token: replacement.Token, At: replacementAt, ExpectedRosterHead: fixture.Roster().Head(),
		ExpectedOpenGrant: inviteTestOpenGrantFence(later.ID())}); !errors.Is(err, ErrChannelInviteConflict) {
		t.Fatalf("retroactive member attribution rotation error = %v", err)
	}
	if _, err := st.CreateChannel(ctx, createSpec); !errors.Is(err, ErrChannelCreateConflict) {
		t.Fatalf("retroactive member attribution create replay error = %v", err)
	}
}

func TestChannelInviteFailsClosedOnPartialEnrollmentLedger(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := openTestStore(t)
	fixture := testkit.NewSignedChannel(t, "invite-partial-ledger")
	insertChannelTestNode(t, st.db, fixture.Owner(), fixture.Channel().CreatedAt())
	grant := inviteTestGrant(t, fixture, "partial-ledger",
		fixture.Channel().CreatedAt(), 7)
	createSpec := CreateChannelSpec{Channel: fixture.Channel(),
		Genesis: fixture.OwnerMember().Member(), Token: grant.Token}
	if _, err := st.CreateChannel(ctx, createSpec); err != nil {
		t.Fatal(err)
	}
	joined := fixture.AppendActive(t, "invite-partial-ledger-member")
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	installInviteTestMember(t, ctx, tx, fixture, joined)
	insertInviteTestGrantUse(t, tx, fixture, grant.ID(), joined,
		"use-invite-partial-ledger", joined.Member().CreatedAt())
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	replacementAt := joined.Member().CreatedAt().Add(time.Minute)
	replacement := inviteTestGrant(t, fixture, "partial-ledger-replacement",
		replacementAt, 1)
	if _, err := st.RotateChannelInvite(ctx, RotateChannelInviteSpec{ChannelID: fixture.Channel().ID(),
		Token: replacement.Token, At: replacementAt, ExpectedRosterHead: fixture.Roster().Head(),
		ExpectedOpenGrant: inviteTestOpenGrantFence(grant.ID())}); !errors.Is(err, ErrChannelInviteConflict) {
		t.Fatalf("partial ledger rotation error = %v", err)
	}
	if _, err := st.CreateChannel(ctx, createSpec); !errors.Is(err, ErrChannelCreateConflict) {
		t.Fatalf("partial ledger create replay error = %v", err)
	}
	assertInviteGrantState(t, st, grant.ID(), "open", 1)
	var replacements int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM enrollment_grants WHERE grant_id=?`,
		replacement.ID().String()).Scan(&replacements); err != nil || replacements != 0 {
		t.Fatalf("partial ledger replacement count = %d, %v", replacements, err)
	}
}

type inviteTestCredential struct {
	model.OpenEnrollmentGrant
	Token model.EnrollmentToken
}

func inviteTestOpenGrantFence(id model.GrantID) ChannelInviteOpenGrantFence {
	return ChannelInviteOpenGrantFence{Present: !id.IsZero(), GrantID: id}
}

func inviteTestGrant(t testing.TB, fixture *testkit.SignedChannel, seed string, at time.Time,
	maxUses uint8,
) inviteTestCredential {
	t.Helper()
	id, err := model.ParseGrantID("grant-invite-" + seed)
	if err != nil {
		t.Fatal(err)
	}
	return inviteTestCredentialForID(t, fixture, id, seed, at, maxUses)
}

func inviteTestCredentialForID(t testing.TB, fixture *testkit.SignedChannel,
	id model.GrantID, seed string, at time.Time, maxUses uint8,
) inviteTestCredential {
	t.Helper()
	token := storeTestEnrollmentToken(t, fixture.Descriptor(), fixture.Owner(), id,
		"invite-"+seed, at, maxUses)
	return inviteTestCredential{OpenEnrollmentGrant: storeTestEnrollmentGrant(t, token, at), Token: token}
}

func assertInviteGrantState(t testing.TB, st *Store, grantID model.GrantID, status string,
	used uint8,
) {
	t.Helper()
	var gotStatus string
	var gotUsed uint8
	if err := st.db.QueryRow(`SELECT status,used_uses FROM enrollment_grants WHERE grant_id=?`,
		grantID.String()).Scan(&gotStatus, &gotUsed); err != nil || gotStatus != status || gotUsed != used {
		t.Fatalf("grant %s state = (%q,%d,%v), want (%q,%d)", grantID.String(), gotStatus,
			gotUsed, err, status, used)
	}
}

func assertInviteGrantAbsent(t testing.TB, st *Store, grantID model.GrantID) {
	t.Helper()
	var count int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM enrollment_grants WHERE grant_id=?`,
		grantID.String()).Scan(&count); err != nil || count != 0 {
		t.Fatalf("grant %s count = (%d,%v), want 0", grantID.String(), count, err)
	}
}

func installInviteTestMember(t testing.TB, ctx context.Context, tx *sql.Tx,
	fixture *testkit.SignedChannel, joined testkit.MemberFixture,
) {
	t.Helper()
	member := joined.Member()
	if err := insertChannelMember(ctx, tx, member); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE channels SET roster_head_revision=?,roster_head_hash=?,updated_at=?
		WHERE channel_id=?`, member.Head().Revision(), member.Head().Digest().Bytes(),
		storeTime(member.CreatedAt()), fixture.Channel().ID().String()); err != nil {
		t.Fatal(err)
	}
}

func insertInviteTestGrantUse(t testing.TB, tx *sql.Tx, fixture *testkit.SignedChannel,
	grantID model.GrantID, joined testkit.MemberFixture, useID string, usedAt time.Time,
) {
	t.Helper()
	member := joined.Member()
	joinIdentity := inviteTestJoinIdentity(t, fixture.Channel().ID(), grantID, joined)
	if _, err := tx.Exec(`INSERT INTO enrollment_grant_uses(use_id,grant_id,channel_id,
		member_peer_id,join_identity_digest,member_revision,member_record_hash,used_at)
		VALUES(?,?,?,?,?,?,?,?)`, useID, grantID.String(), fixture.Channel().ID().String(),
		joined.Identity().PeerID().String(), joinIdentity.Bytes(), member.Head().Revision(),
		member.Head().Digest().Bytes(), storeTime(usedAt)); err != nil {
		t.Fatal(err)
	}
}

func insertInviteTestReceipt(t testing.TB, tx *sql.Tx, fixture *testkit.SignedChannel,
	grantID model.GrantID, joined testkit.MemberFixture, useID, seed string, usedAt time.Time,
) {
	t.Helper()
	receipt := inviteTestReceipt(t, fixture, grantID, joined, seed, usedAt)
	member := joined.Member()
	if _, err := tx.Exec(`INSERT INTO enrollment_receipts(receipt_id,owner_use_id,channel_id,
		member_peer_id,roster_head_revision,roster_head_hash,receipt_json,owner_signature,created_at)
		VALUES(?,?,?,?,?,?,?,?,?)`, receipt.ReceiptID().String(), useID,
		fixture.Channel().ID().String(), joined.Identity().PeerID().String(),
		member.Head().Revision(), member.Head().Digest().Bytes(),
		receipt.ReceiptJSON().Bytes(), receipt.OwnerSignature(),
		storeTime(receipt.AcceptedAt())); err != nil {
		t.Fatal(err)
	}
}

func insertInviteTestGrantWithReceipts(t testing.TB, st *Store, fixture *testkit.SignedChannel,
	grant model.OpenEnrollmentGrant, members []testkit.MemberFixture,
) {
	t.Helper()
	tx, err := st.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO enrollment_grants(grant_id,channel_id,verifier,expires_at,
		max_uses,used_uses,status,created_at,closed_at) VALUES(?,?,?,?,?,0,'open',?,NULL)`,
		grant.ID().String(), grant.ChannelID().String(), grant.Verifier().Bytes(),
		storeTime(grant.ExpiresAt()), grant.MaxUses(), storeTime(grant.CreatedAt())); err != nil {
		t.Fatal(err)
	}
	for index, member := range members {
		useID := "use-invite-ledger-" + string(rune('a'+index))
		usedAt := member.Member().CreatedAt()
		if _, err := tx.Exec(`INSERT INTO enrollment_grant_uses(use_id,grant_id,channel_id,
			member_peer_id,join_identity_digest,member_revision,member_record_hash,used_at)
			VALUES(?,?,?,?,?,?,?,?)`, useID, grant.ID().String(), grant.ChannelID().String(),
			member.Identity().PeerID().String(), inviteTestJoinIdentity(t, grant.ChannelID(), grant.ID(), member).Bytes(),
			member.Member().Head().Revision(), member.Member().Head().Digest().Bytes(), storeTime(usedAt)); err != nil {
			t.Fatal(err)
		}
		receipt := inviteTestReceipt(t, fixture, grant.ID(), member,
			"ledger-"+string(rune('a'+index)), usedAt)
		if _, err := tx.Exec(`INSERT INTO enrollment_receipts(receipt_id,owner_use_id,channel_id,
			member_peer_id,roster_head_revision,roster_head_hash,receipt_json,owner_signature,created_at)
			VALUES(?,?,?,?,?,?,?,?,?)`, receipt.ReceiptID().String(), useID, grant.ChannelID().String(),
			member.Identity().PeerID().String(), member.Member().Head().Revision(),
			member.Member().Head().Digest().Bytes(), receipt.ReceiptJSON().Bytes(),
			receipt.OwnerSignature(), storeTime(receipt.AcceptedAt())); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func inviteTestJoinIdentity(t testing.TB, channelID model.ChannelID, grantID model.GrantID,
	member testkit.MemberFixture,
) model.Digest {
	t.Helper()
	digest, err := model.EnrollmentJoinIdentityDigest(channelID, grantID,
		member.Identity().PeerID(), member.Identity().PublicKey(), member.Identity().OriginEpoch())
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func inviteTestReceipt(t testing.TB, fixture *testkit.SignedChannel, grantID model.GrantID,
	member testkit.MemberFixture, seed string, acceptedAt time.Time,
) model.EnrollmentReceipt {
	t.Helper()
	receiptID, err := model.ParseEnrollmentReceiptID("receipt-invite-" + seed)
	if err != nil {
		t.Fatal(err)
	}
	requestID, err := model.ParseEnrollmentRequestID("request-invite-" + seed)
	if err != nil {
		t.Fatal(err)
	}
	record, err := model.NewEnrollmentReceiptRecord(model.EnrollmentReceiptRecordSpec{
		ReceiptID: receiptID, RequestID: requestID, GrantID: grantID,
		ChannelID: fixture.Channel().ID(), MemberPeerID: member.Identity().PeerID(),
		JoinIdentityDigest: inviteTestJoinIdentity(t, fixture.Channel().ID(), grantID, member),
		MemberHead:         member.Member().Head(), AcceptedAt: acceptedAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	message, err := model.EnrollmentReceiptSigningMessage(fixture.Channel().ID(), record.Digest())
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := model.AttachEnrollmentReceiptSignature(record,
		ed25519.Sign(ed25519Private(fixture.Owner()), message))
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}
