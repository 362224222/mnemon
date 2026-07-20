package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestPrepareJoinedChannelFencesConcurrentAttemptsAndCommitUnknown(t *testing.T) {
	t.Parallel()
	owner := newChannelEnrollmentFixture(t, "join-reservation-attempt")
	transcript := owner.transcript(t, 0x91, 0x92, owner.head)
	accepted := owner.accept(t, transcript)
	joinerStore := openTestStore(t)
	insertChannelTestNode(t, joinerStore.db, owner.joiner, owner.channel.Channel().CreatedAt())
	installSpec := InstallJoinedChannelSpec{
		AuthenticatedOwnerPeerID: owner.channel.Owner().PeerID(),
		LocalAlias:               "reservation-team",
		Descriptor:               owner.channel.Descriptor(),
		Transcript:               transcript,
		Receipt:                  accepted.Receipt,
		Members:                  accepted.Roster.Members(),
		At:                       owner.acceptedAt.Add(time.Second),
	}
	prepareSpec := PrepareJoinedChannelSpec{
		AuthenticatedLocalPeerID: owner.joiner.PeerID(),
		LocalPublicKey:           owner.joiner.PublicKey(),
		Descriptor:               owner.channel.Descriptor(),
		GrantID:                  owner.grantID,
		LocalAlias:               installSpec.LocalAlias,
		At:                       installSpec.At,
	}
	first, err := joinerStore.PrepareJoinedChannel(context.Background(), prepareSpec)
	if err != nil || first.RequestID != transcript.RequestID() ||
		first.OriginEpoch != owner.joiner.OriginEpoch() || first.Attempt != 1 ||
		!first.Reserved || first.CommitUnknown {
		t.Fatalf("first reservation = (%#v,%v)", first, err)
	}
	prepareSpec.At = prepareSpec.At.Add(time.Second)
	second, err := joinerStore.PrepareJoinedChannel(context.Background(), prepareSpec)
	if err != nil || second.Attempt != 2 || second.CommitUnknown {
		t.Fatalf("second reservation attempt = (%#v,%v)", second, err)
	}
	if err := joinerStore.MarkJoinedChannelCommitUnknown(context.Background(), first.RequestID,
		owner.joiner.PeerID(), first.Attempt, prepareSpec.At); !errors.Is(err, ErrChannelJoinConflict) {
		t.Fatalf("stale attempt mark error = %v", err)
	}
	if err := joinerStore.ReleaseJoinedChannelReservation(context.Background(), first.RequestID,
		owner.joiner.PeerID(), first.Attempt); err != nil {
		t.Fatalf("stale attempt release error = %v", err)
	}
	if err := joinerStore.MarkJoinedChannelCommitUnknown(context.Background(), second.RequestID,
		owner.joiner.PeerID(), second.Attempt, prepareSpec.At); err != nil {
		t.Fatal(err)
	}
	prepareSpec.At = prepareSpec.At.Add(time.Second)
	third, err := joinerStore.PrepareJoinedChannel(context.Background(), prepareSpec)
	if err != nil || third.Attempt != 3 || !third.CommitUnknown {
		t.Fatalf("commit-unknown takeover = (%#v,%v)", third, err)
	}
	if _, err := joinerStore.db.Exec(`UPDATE channel_join_reservations SET state='reserved'
		WHERE request_id=?`, third.RequestID.String()); err == nil {
		t.Fatal("schema allowed commit_unknown to regress to reserved")
	}
	if err := joinerStore.ReleaseJoinedChannelReservation(context.Background(), second.RequestID,
		owner.joiner.PeerID(), second.Attempt); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := joinerStore.db.QueryRow(`SELECT COUNT(*) FROM channel_join_reservations`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("stale release reservation count = (%d,%v)", count, err)
	}
	if err := joinerStore.ReleaseJoinedChannelReservation(context.Background(), third.RequestID,
		owner.joiner.PeerID(), third.Attempt); err != nil {
		t.Fatal(err)
	}
	if err := joinerStore.db.QueryRow(`SELECT COUNT(*) FROM channel_join_reservations`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("resolved reservation count = (%d,%v)", count, err)
	}
}

func TestPrepareJoinedChannelRejectsNoncanonicalLocalAuthority(t *testing.T) {
	t.Parallel()
	owner := newChannelEnrollmentFixture(t, "join-reservation-input")
	joinerStore := openTestStore(t)
	insertChannelTestNode(t, joinerStore.db, owner.joiner, owner.channel.Channel().CreatedAt())
	base := PrepareJoinedChannelSpec{
		AuthenticatedLocalPeerID: owner.joiner.PeerID(),
		LocalPublicKey:           owner.joiner.PublicKey(),
		Descriptor:               owner.channel.Descriptor(),
		GrantID:                  owner.grantID,
		LocalAlias:               "valid-local-alias",
		At:                       owner.acceptedAt,
	}
	wrongPeer := base
	wrongPeer.AuthenticatedLocalPeerID = owner.channel.Owner().PeerID()
	badAlias := base
	badAlias.LocalAlias = "invalid alias"
	ownedChannel := testkit.NewSignedChannelForOwnerAt(t, "join-reservation-self-owner",
		owner.joiner, owner.channel.Channel().CreatedAt())
	selfOwner := base
	selfOwner.Descriptor = ownedChannel.Descriptor()
	for name, spec := range map[string]PrepareJoinedChannelSpec{
		"wrong durable peer": wrongPeer,
		"invalid alias":      badAlias,
		"local owner":        selfOwner,
	} {
		if _, err := joinerStore.PrepareJoinedChannel(context.Background(), spec); !errors.Is(err, ErrChannelJoinInput) {
			t.Errorf("%s error = %v", name, err)
		}
	}
	if _, err := joinerStore.PrepareJoinedChannel(nil, base); !errors.Is(err, ErrChannelJoinInput) {
		t.Fatalf("nil context error = %v", err)
	}
	var count int
	if err := joinerStore.db.QueryRow(`SELECT COUNT(*) FROM channel_join_reservations`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("invalid input reservation count = (%d,%v)", count, err)
	}
}

func TestChannelCreateAndJoinReservationRejectCrossScopeConflicts(t *testing.T) {
	t.Parallel()
	remote := newChannelEnrollmentFixture(t, "join-reservation-create-conflict")
	joinerStore := openTestStore(t)
	createdAt := remote.channel.Channel().CreatedAt()
	insertChannelTestNode(t, joinerStore.db, remote.joiner, createdAt)
	remoteSpec := PrepareJoinedChannelSpec{
		AuthenticatedLocalPeerID: remote.joiner.PeerID(),
		LocalPublicKey:           remote.joiner.PublicKey(),
		Descriptor:               remote.channel.Descriptor(),
		GrantID:                  remote.grantID,
		LocalAlias:               "shared-local-alias",
		At:                       remote.acceptedAt,
	}
	reserved, err := joinerStore.PrepareJoinedChannel(context.Background(), remoteSpec)
	if err != nil {
		t.Fatal(err)
	}
	local := testkit.NewSignedChannelForOwnerAt(t, "join-reservation-local-create",
		remote.joiner, createdAt)
	conflictingChannel, err := model.NewChannel(model.ChannelSpec{Descriptor: local.Descriptor(),
		LocalAlias: remoteSpec.LocalAlias, RosterHead: local.Roster().Head(), Status: model.ChannelActive,
		TopicState: model.TopicNotJoined, UpdatedAt: createdAt})
	if err != nil {
		t.Fatal(err)
	}
	localGrant, _ := model.ParseGrantID("grant-join-reservation-local-create")
	localToken := storeTestEnrollmentToken(t, local.Descriptor(), local.Owner(), localGrant,
		"join-reservation-local-create", createdAt, model.MaxMembersPerChannel-1)
	if _, err := joinerStore.CreateChannel(context.Background(), CreateChannelSpec{
		Channel: conflictingChannel, Genesis: local.OwnerMember().Member(), Token: localToken,
	}); !errors.Is(err, ErrChannelCreateConflict) {
		t.Fatalf("create over reserved alias error = %v", err)
	}
	if err := joinerStore.ReleaseJoinedChannelReservation(context.Background(), reserved.RequestID,
		remote.joiner.PeerID(), reserved.Attempt); err != nil {
		t.Fatal(err)
	}
	if _, err := joinerStore.CreateChannel(context.Background(), CreateChannelSpec{
		Channel: local.Channel(), Genesis: local.OwnerMember().Member(), Token: localToken,
	}); err != nil {
		t.Fatalf("CreateChannel() after release = %v", err)
	}
	remoteSpec.LocalAlias = local.Channel().LocalAlias()
	remoteSpec.At = remoteSpec.At.Add(time.Second)
	if _, err := joinerStore.PrepareJoinedChannel(context.Background(), remoteSpec); !errors.Is(err, ErrChannelJoinConflict) {
		t.Fatalf("reserve installed alias error = %v", err)
	}
	assertEnrollmentTableCounts(t, joinerStore, map[string]int{
		"channels": 1, "channel_join_reservations": 0,
	})
}
