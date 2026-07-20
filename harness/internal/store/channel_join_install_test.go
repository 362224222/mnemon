package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestInstallJoinedChannelCommitsReplicaBindingsAndReplays(t *testing.T) {
	t.Parallel()
	owner := newChannelEnrollmentFixture(t, "join-install")
	transcript := owner.transcript(t, 0x71, 0x72, owner.head)
	accepted := owner.accept(t, transcript)
	joinerStore := openTestStore(t)
	insertChannelTestNode(t, joinerStore.db, owner.joiner, owner.channel.Channel().CreatedAt())
	spec := InstallJoinedChannelSpec{AuthenticatedOwnerPeerID: owner.channel.Owner().PeerID(),
		LocalAlias: "project-team", Descriptor: owner.channel.Descriptor(), Transcript: transcript,
		Receipt: accepted.Receipt, Members: accepted.Roster.Members(), At: owner.acceptedAt.Add(time.Second)}
	wrongOwner := testkit.NewIdentity(t, "join-install-wrong-secure-owner")
	wrongSpec := spec
	wrongSpec.AuthenticatedOwnerPeerID = wrongOwner.PeerID()
	if _, err := joinerStore.InstallJoinedChannel(context.Background(), wrongSpec); !errors.Is(err, ErrChannelJoinInput) {
		t.Fatalf("wrong secure owner error = %v", err)
	}
	wrongHeadSpec := spec
	wrongHeadSpec.Transcript = enrollmentTestTranscript(t, owner.channel.Descriptor(), owner.grantID,
		owner.requestID, owner.joiner, accepted.Roster.Head(), 0x6f, 0x70)
	if _, err := joinerStore.InstallJoinedChannel(context.Background(), wrongHeadSpec); !errors.Is(err, ErrChannelJoinInput) {
		t.Fatalf("wrong historical predecessor error = %v", err)
	}
	assertEnrollmentTableCounts(t, joinerStore, map[string]int{
		"channels": 0, "channel_members": 0, "enrollment_receipts": 0,
		"publication_epochs": 0, "peer_bindings": 0,
	})
	reserveJoinedChannelTest(t, joinerStore, spec)
	installed, err := joinerStore.InstallJoinedChannel(context.Background(), spec)
	if err != nil || !installed.Installed || installed.Status != ChannelEnrollmentAccepted ||
		installed.Channel.LocalAlias() != "project-team" {
		t.Fatalf("InstallJoinedChannel() = (%#v, %v)", installed, err)
	}
	assertEnrollmentTableCounts(t, joinerStore, map[string]int{
		"channels": 1, "channel_members": 2, "enrollment_receipts": 1,
		"publication_epochs": 1, "peer_bindings": 1, "enrollment_grants": 0,
		"enrollment_grant_uses": 0,
	})
	var epochPeer, epochText string
	var floor, head uint64
	if err := joinerStore.db.QueryRow(`SELECT origin_peer_id,origin_epoch,source_floor_channel_seq,
		source_head_channel_seq FROM publication_epochs`).Scan(&epochPeer, &epochText, &floor, &head); err != nil ||
		epochPeer != owner.joiner.PeerID().String() || epochText != owner.joiner.OriginEpoch().String() ||
		floor != 1 || head != 0 {
		t.Fatalf("local publication epoch = (%q,%q,%d,%d,%v)", epochPeer, epochText, floor, head, err)
	}
	var ownerUse sql.NullString
	var bindingState, alias string
	if err := joinerStore.db.QueryRow(`SELECT owner_use_id FROM enrollment_receipts`).Scan(&ownerUse); err != nil || ownerUse.Valid {
		t.Fatalf("replica owner_use_id = %#v, %v", ownerUse, err)
	}
	if err := joinerStore.db.QueryRow(`SELECT state,effective_alias FROM peer_bindings`).Scan(
		&bindingState, &alias); err != nil || bindingState != "pending" || alias == "" {
		t.Fatalf("owner binding = (%q,%q,%v)", bindingState, alias, err)
	}
	replayed, err := joinerStore.InstallJoinedChannel(context.Background(), spec)
	if err != nil || replayed.Installed || replayed.Status != ChannelEnrollmentReplayed {
		t.Fatalf("InstallJoinedChannel(replay) = (%#v, %v)", replayed, err)
	}
	assertEnrollmentTableCounts(t, joinerStore, map[string]int{
		"channels": 1, "channel_members": 2, "enrollment_receipts": 1,
		"publication_epochs": 1, "peer_bindings": 1,
	})
}

func TestInstallJoinedChannelRejectsNinthNonterminalReplicaWithoutPartialState(t *testing.T) {
	t.Parallel()
	sharedJoiner := testkit.NewIdentity(t, "join-channel-limit-shared")
	joinerStore := openTestStore(t)
	base := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	insertChannelTestNode(t, joinerStore.db, sharedJoiner, base)
	var rejectedChannel model.ChannelID
	for index := 0; index <= model.MaxChannelsPerNode; index++ {
		seed := "join-channel-limit-" + string(rune('a'+index))
		owner := newChannelEnrollmentFixture(t, seed)
		owner.joiner = sharedJoiner
		owner.requestID = stableEnrollmentRequest(t, owner.channel.Channel().ID(), owner.grantID,
			sharedJoiner)
		prepared, err := owner.ownerStore.PrepareChannelEnrollment(context.Background(),
			owner.prepareSpec(owner.acceptedAt))
		if err != nil {
			t.Fatalf("prepare Channel %d: %v", index+1, err)
		}
		transcript := owner.transcript(t, byte(0x80+index), byte(0x90+index), prepared.RosterHead)
		accepted := owner.accept(t, transcript)
		installSpec := InstallJoinedChannelSpec{
			AuthenticatedOwnerPeerID: owner.channel.Owner().PeerID(),
			LocalAlias:               "joined-" + string(rune('a'+index)),
			Descriptor:               owner.channel.Descriptor(), Transcript: transcript,
			Receipt: accepted.Receipt, Members: accepted.Roster.Members(), At: owner.acceptedAt.Add(time.Second)}
		if index == model.MaxChannelsPerNode {
			rejectedChannel = owner.channel.Channel().ID()
			_, err := joinerStore.PrepareJoinedChannel(context.Background(), PrepareJoinedChannelSpec{
				AuthenticatedLocalPeerID: sharedJoiner.PeerID(), LocalPublicKey: sharedJoiner.PublicKey(),
				Descriptor: owner.channel.Descriptor(), GrantID: owner.grantID,
				LocalAlias: installSpec.LocalAlias, At: installSpec.At})
			if !errors.Is(err, ErrNodeChannelLimit) {
				t.Fatalf("ninth joined Channel preflight error = %v", err)
			}
			continue
		}
		reserveJoinedChannelTest(t, joinerStore, installSpec)
		result, err := joinerStore.InstallJoinedChannel(context.Background(), installSpec)
		if err != nil || !result.Installed {
			t.Fatalf("install Channel %d = (%#v,%v)", index+1, result, err)
		}
	}
	assertEnrollmentTableCounts(t, joinerStore, map[string]int{
		"channels": model.MaxChannelsPerNode, "channel_members": model.MaxChannelsPerNode * 2,
		"enrollment_receipts": model.MaxChannelsPerNode,
		"publication_epochs":  model.MaxChannelsPerNode, "peer_bindings": model.MaxChannelsPerNode,
	})
	for _, table := range []string{"channels", "channel_members", "enrollment_receipts",
		"publication_epochs", "peer_bindings"} {
		var count int
		if err := joinerStore.db.QueryRow(`SELECT COUNT(*) FROM `+table+` WHERE channel_id=?`,
			rejectedChannel.String()).Scan(&count); err != nil || count != 0 {
			t.Fatalf("partial ninth %s rows = %d, %v", table, count, err)
		}
	}
}

func TestInstallJoinedChannelFinalBindingFailureRollsBackEverything(t *testing.T) {
	t.Parallel()
	owner := newChannelEnrollmentFixture(t, "join-binding-rollback")
	transcript := owner.transcript(t, 0x73, 0x74, owner.head)
	accepted := owner.accept(t, transcript)
	joinerStore := openTestStore(t)
	insertChannelTestNode(t, joinerStore.db, owner.joiner, owner.channel.Channel().CreatedAt())
	if _, err := joinerStore.db.Exec(`CREATE TRIGGER test_reject_join_binding BEFORE INSERT ON peer_bindings
		BEGIN SELECT RAISE(ABORT, 'test reject binding'); END`); err != nil {
		t.Fatal(err)
	}
	installSpec := InstallJoinedChannelSpec{
		AuthenticatedOwnerPeerID: owner.channel.Owner().PeerID(), LocalAlias: "rollback-team",
		Descriptor: owner.channel.Descriptor(), Transcript: transcript, Receipt: accepted.Receipt,
		Members: accepted.Roster.Members(), At: owner.acceptedAt.Add(time.Second)}
	reserveJoinedChannelTest(t, joinerStore, installSpec)
	_, err := joinerStore.InstallJoinedChannel(context.Background(), installSpec)
	if err == nil {
		t.Fatal("binding failure did not reject join install")
	}
	assertEnrollmentTableCounts(t, joinerStore, map[string]int{
		"channels": 0, "channel_members": 0, "enrollment_receipts": 0,
		"publication_epochs": 0, "peer_bindings": 0, "channel_join_reservations": 1,
	})
}

func TestInstallJoinedChannelOwnerCloseSuffixClosesReplicaAndFreshJoinStaysEmpty(t *testing.T) {
	t.Parallel()
	owner := newChannelEnrollmentFixture(t, "join-owner-close")
	transcript := owner.transcript(t, 0x79, 0x7a, owner.head)
	accepted := owner.accept(t, transcript)
	baseAt := owner.acceptedAt.Add(time.Second)
	baseSpec := InstallJoinedChannelSpec{AuthenticatedOwnerPeerID: owner.channel.Owner().PeerID(),
		LocalAlias: "owner-close-team", Descriptor: owner.channel.Descriptor(), Transcript: transcript,
		Receipt: accepted.Receipt, Members: accepted.Roster.Members(), At: baseAt}
	joinerStore := openTestStore(t)
	insertChannelTestNode(t, joinerStore.db, owner.joiner, owner.channel.Channel().CreatedAt())
	reserveJoinedChannelTest(t, joinerStore, baseSpec)
	if result, err := joinerStore.InstallJoinedChannel(context.Background(), baseSpec); err != nil || !result.Installed {
		t.Fatalf("initial install = (%#v,%v)", result, err)
	}
	_, closedRoster := appendRosterTerminal(t, owner.channel.Descriptor(), owner.signer,
		accepted.Roster, owner.channel.Owner().PeerID(), model.MemberLeft, baseAt.Add(time.Second))
	closeSpec := baseSpec
	closeSpec.Members = closedRoster.Members()
	closeSpec.At = baseAt.Add(2 * time.Second)
	closed, err := joinerStore.InstallJoinedChannel(context.Background(), closeSpec)
	if err != nil || closed.Status != ChannelEnrollmentChannelClosed ||
		closed.Channel.Status() != model.ChannelClosed || closed.Channel.TopicState() != model.TopicLeft ||
		closed.Channel.RosterHead() != closedRoster.Head() {
		t.Fatalf("owner close suffix = (%#v,%v)", closed, err)
	}

	fresh := openTestStore(t)
	insertChannelTestNode(t, fresh.db, owner.joiner, owner.channel.Channel().CreatedAt())
	reserveJoinedChannelTest(t, fresh, closeSpec)
	freshResult, err := fresh.InstallJoinedChannel(context.Background(), closeSpec)
	if err != nil || freshResult.Installed || freshResult.Status != ChannelEnrollmentChannelClosed {
		t.Fatalf("fresh owner-close install = (%#v,%v)", freshResult, err)
	}
	assertEnrollmentTableCounts(t, fresh, map[string]int{
		"channels": 0, "channel_members": 0, "enrollment_receipts": 0,
		"publication_epochs": 0, "peer_bindings": 0,
	})
}
