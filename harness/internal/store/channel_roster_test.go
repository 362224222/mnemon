package store

import (
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestMergeChannelRosterAppliesOverlapDetectsGapAndKeepsBindingsPending(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	fixture := testkit.NewSignedChannel(t, "merge-roster-suffix")
	insertChannelTestNode(t, st.db, fixture.Owner(), fixture.Channel().CreatedAt())
	insertSignedChannelFixture(t, st.db, fixture, model.TopicJoined)
	remote := fixture.AppendActive(t, "merge-roster-remote")
	result, err := st.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
		ChannelID: fixture.Channel().ID(), AuthenticatedTransportPeerID: fixture.Owner().PeerID(),
		Records: []model.Member{remote.Member()}, At: fixture.Channel().UpdatedAt(),
	})
	if err != nil || result.Status != ChannelRosterApplied || result.Roster.Head().Revision() != 2 ||
		result.Channel.TopicState() != model.TopicJoining || result.ExpectedNextRevision != 3 {
		t.Fatalf("MergeChannelRoster(suffix) = (%#v,%v)", result, err)
	}
	var state string
	if err := st.db.QueryRow(`SELECT state FROM peer_bindings WHERE channel_id=? AND peer_id=?`,
		fixture.Channel().ID().String(), remote.Identity().PeerID().String()).Scan(&state); err != nil ||
		state != string(model.BindingPending) {
		t.Fatalf("new roster binding state = (%q,%v)", state, err)
	}
	duplicate, err := st.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
		ChannelID: fixture.Channel().ID(), AuthenticatedTransportPeerID: remote.Identity().PeerID(),
		Records: []model.Member{remote.Member()}, At: fixture.Channel().UpdatedAt().Add(time.Second),
	})
	if err != nil || duplicate.Status != ChannelRosterDuplicate || duplicate.Roster.Head() != result.Roster.Head() {
		t.Fatalf("MergeChannelRoster(duplicate) = (%#v,%v)", duplicate, err)
	}
	third := fixture.AppendActiveUpdate(t, remote.Identity().PeerID())
	fourth := fixture.AppendActive(t, "merge-roster-fourth")
	badSignature, err := model.AttachMemberSignature(fourth.Member().Record(), make([]byte, ed25519.SignatureSize))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
		ChannelID: fixture.Channel().ID(), AuthenticatedTransportPeerID: remote.Identity().PeerID(),
		Records: []model.Member{badSignature}, At: fixture.Channel().UpdatedAt(),
	}); !errors.Is(err, ErrChannelRosterInput) {
		t.Fatalf("invalid signed gap error = %v", err)
	}
	overflow := signedRosterOverflowRecord(t, fixture, remote.Identity())
	if _, err := st.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
		ChannelID: fixture.Channel().ID(), AuthenticatedTransportPeerID: remote.Identity().PeerID(),
		Records: []model.Member{overflow}, At: overflow.CreatedAt(),
	}); !errors.Is(err, ErrChannelRosterInput) {
		t.Fatalf("revision overflow error = %v", err)
	}
	gap, err := st.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
		ChannelID: fixture.Channel().ID(), AuthenticatedTransportPeerID: remote.Identity().PeerID(),
		Records: []model.Member{fourth.Member()}, At: fixture.Channel().UpdatedAt(),
	})
	if err != nil || gap.Status != ChannelRosterGap || gap.ExpectedNextRevision != 3 ||
		gap.Roster.Head().Revision() != 2 {
		t.Fatalf("MergeChannelRoster(gap) = (%#v,%v)", gap, err)
	}
	merged, err := st.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
		ChannelID: fixture.Channel().ID(), AuthenticatedTransportPeerID: remote.Identity().PeerID(),
		Records: []model.Member{third.Member(), fourth.Member()}, At: fixture.Channel().UpdatedAt(),
	})
	if err != nil || merged.Status != ChannelRosterApplied || merged.Roster.Head().Revision() != 4 ||
		merged.ExpectedNextRevision != 5 {
		t.Fatalf("MergeChannelRoster(repair) = (%#v,%v)", merged, err)
	}
}

func TestMergeChannelRosterBlocksOnlyAffectedPendingEgress(t *testing.T) {
	t.Parallel()
	t.Run("remote terminal", func(t *testing.T) {
		fixture, _ := newAgentClaimFixture(t, 1, "roster-remote-terminal")
		signed := acceptanceSignedChannel(t, fixture)
		terminal := signed.AppendTerminal(t, fixture.reviewers[0], model.MemberRevoked)
		result, err := fixture.store.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
			ChannelID: fixture.channel, AuthenticatedTransportPeerID: fixture.reviewers[0],
			Records: []model.Member{terminal.Member()}, At: fixture.now.Add(2 * time.Second),
		})
		if err != nil || result.Status != ChannelRosterApplied {
			t.Fatalf("remote terminal merge = (%#v,%v)", result, err)
		}
		assertRosterEgressState(t, fixture.store, "queued", "blocked", false)
	})

	t.Run("fork", func(t *testing.T) {
		fixture, _ := newAgentClaimFixture(t, 1, "roster-fork-egress")
		signed := acceptanceSignedChannel(t, fixture)
		challenger := ownerConflictChallenger(t, signed, fixture.now.Add(time.Second))
		mustExec(t, fixture.store, `UPDATE gossip_publications SET status='leased',
			lease_owner='roster-test',lease_until=?`, storeTime(fixture.now.Add(time.Minute)))
		result, err := fixture.store.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
			ChannelID: fixture.channel, AuthenticatedTransportPeerID: fixture.reviewers[0],
			Records: []model.Member{challenger}, At: fixture.now.Add(2 * time.Second),
		})
		if err != nil || result.Status != ChannelRosterConflicted {
			t.Fatalf("fork merge = (%#v,%v)", result, err)
		}
		assertRosterEgressState(t, fixture.store, "blocked", "blocked", true)
	})
}

func acceptanceSignedChannel(t *testing.T, fixture *acceptanceFixture) *testkit.SignedChannel {
	t.Helper()
	signed := testkit.NewSignedChannelAt(t, "accept-"+t.Name(),
		fixture.now.Add(-time.Hour).Add(time.Minute))
	for index := range fixture.reviewers {
		signed.AppendActiveIdentity(t, testkit.NewIdentity(t,
			"peer-accept-reviewer-"+string(rune('a'+index))))
	}
	return signed
}

func assertRosterEgressState(t *testing.T, st *Store, publication, delivery string,
	leaseCleared bool,
) {
	t.Helper()
	var publicationState, deliveryState string
	var leaseOwner, leaseUntil any
	if err := st.db.QueryRow(`SELECT status,lease_owner,lease_until FROM gossip_publications`).Scan(
		&publicationState, &leaseOwner, &leaseUntil); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT status FROM peer_deliveries`).Scan(&deliveryState); err != nil {
		t.Fatal(err)
	}
	if publicationState != publication || deliveryState != delivery ||
		(leaseCleared && (leaseOwner != nil || leaseUntil != nil)) {
		t.Fatalf("egress = publication %q delivery %q lease (%v,%v)",
			publicationState, deliveryState, leaseOwner, leaseUntil)
	}
}

func TestMergeChannelRosterPersistsValidForkAndFailsChannelClosed(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	fixture := testkit.NewSignedChannel(t, "merge-roster-fork")
	fixture.AppendActiveUpdate(t, fixture.Owner().PeerID())
	insertChannelTestNode(t, st.db, fixture.Owner(), fixture.Channel().CreatedAt())
	insertSignedChannelFixture(t, st.db, fixture, model.TopicJoined)
	mustExec(t, st, `UPDATE channels SET status='leaving',topic_state='left' WHERE channel_id=?`,
		fixture.Channel().ID().String())
	challenger := ownerConflictChallenger(t, fixture, fixture.Channel().UpdatedAt().Add(time.Second))
	result, err := st.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
		ChannelID: fixture.Channel().ID(), AuthenticatedTransportPeerID: fixture.Owner().PeerID(),
		Records: []model.Member{challenger}, At: challenger.CreatedAt(),
	})
	if err != nil || result.Status != ChannelRosterConflicted ||
		result.Channel.Status() != model.ChannelConflicted || result.Channel.TopicState() != model.TopicBlocked ||
		result.Roster.Head().Revision() != 2 {
		t.Fatalf("MergeChannelRoster(fork) = (%#v,%v)", result, err)
	}
	replayed, err := st.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
		ChannelID: fixture.Channel().ID(), AuthenticatedTransportPeerID: fixture.Owner().PeerID(),
		Records: []model.Member{challenger}, At: challenger.CreatedAt().Add(time.Second),
	})
	if err != nil || replayed.Status != ChannelRosterConflicted {
		t.Fatalf("MergeChannelRoster(fork replay) = (%#v,%v)", replayed, err)
	}
	var conflicts int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM channel_conflicts WHERE channel_id=?`,
		fixture.Channel().ID().String()).Scan(&conflicts); err != nil || conflicts != 1 {
		t.Fatalf("durable conflict count = (%d,%v)", conflicts, err)
	}
}

func signedRosterOverflowRecord(t *testing.T, fixture *testkit.SignedChannel,
	identity testkit.Identity,
) model.Member {
	t.Helper()
	previous := fixture.Roster().Head().Digest()
	record, err := model.NewMemberRecord(model.MemberRecordSpec{
		ChannelID: fixture.Channel().ID(), DescriptorDigest: fixture.Descriptor().Descriptor().Digest(),
		Revision: model.MaxMemberRecordsPerChannel + 1, PreviousDigest: &previous,
		PeerID: identity.PeerID(), OriginEpoch: identity.OriginEpoch(), DisplayLabel: identity.DisplayName(),
		PublicKey: identity.PublicKey(), Multiaddrs: identity.Multiaddrs(),
		Protocols: model.RequiredMemberProtocols(), Limits: model.DefaultMemberLimits(),
		Status: model.MemberActive, CreatedAt: fixture.Channel().UpdatedAt().Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	message, _ := model.MemberRecordSigningMessage(record.ChannelID(), record.Digest())
	member, err := model.AttachMemberSignature(record, ed25519.Sign(ed25519Private(fixture.Owner()), message))
	if err != nil {
		t.Fatal(err)
	}
	return member
}

func TestMergeChannelRosterAppliesOwnerTerminalAndRejectsUnauthorizedRelay(t *testing.T) {
	t.Parallel()
	t.Run("owner terminal", func(t *testing.T) {
		st := openTestStore(t)
		fixture := testkit.NewSignedChannel(t, "merge-roster-owner-left")
		insertChannelTestNode(t, st.db, fixture.Owner(), fixture.Channel().CreatedAt())
		insertSignedChannelFixture(t, st.db, fixture, model.TopicJoined)
		left := fixture.AppendTerminal(t, fixture.Owner().PeerID(), model.MemberLeft)
		result, err := st.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
			ChannelID: fixture.Channel().ID(), AuthenticatedTransportPeerID: fixture.Owner().PeerID(),
			Records: []model.Member{left.Member()}, At: fixture.Channel().UpdatedAt(),
		})
		if err != nil || result.Status != ChannelRosterApplied ||
			result.Channel.Status() != model.ChannelClosed || result.Channel.TopicState() != model.TopicLeft {
			t.Fatalf("owner terminal merge = (%#v,%v)", result, err)
		}
	})

	t.Run("unauthorized relay", func(t *testing.T) {
		st := openTestStore(t)
		fixture := testkit.NewSignedChannel(t, "merge-roster-unauthorized")
		insertChannelTestNode(t, st.db, fixture.Owner(), fixture.Channel().CreatedAt())
		insertSignedChannelFixture(t, st.db, fixture, model.TopicJoined)
		remote := fixture.AppendActive(t, "merge-roster-authorized-member")
		outsider := testkit.NewIdentity(t, "merge-roster-outsider")
		_, err := st.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
			ChannelID: fixture.Channel().ID(), AuthenticatedTransportPeerID: outsider.PeerID(),
			Records: []model.Member{remote.Member()}, At: fixture.Channel().UpdatedAt(),
		})
		if !errors.Is(err, ErrChannelRosterInput) {
			t.Fatalf("unauthorized relay error = %v", err)
		}
		var revision uint64
		if err := st.db.QueryRow(`SELECT roster_head_revision FROM channels WHERE channel_id=?`,
			fixture.Channel().ID().String()).Scan(&revision); err != nil || revision != 1 {
			t.Fatalf("unauthorized relay durable head = (%d,%v)", revision, err)
		}
	})
}
