package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
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

func TestInstallJoinedChannelAppliesTerminalReplaySuffixButFreshJoinStaysEmpty(t *testing.T) {
	t.Parallel()
	owner := newChannelEnrollmentFixture(t, "join-terminal-replay")
	transcript := owner.transcript(t, 0x75, 0x76, owner.head)
	accepted := owner.accept(t, transcript)
	initialAt := owner.acceptedAt.Add(time.Second)
	baseSpec := InstallJoinedChannelSpec{AuthenticatedOwnerPeerID: owner.channel.Owner().PeerID(),
		LocalAlias: "terminal-team", Descriptor: owner.channel.Descriptor(), Transcript: transcript,
		Receipt: accepted.Receipt, Members: accepted.Roster.Members(), At: initialAt}
	joinerStore := openTestStore(t)
	insertChannelTestNode(t, joinerStore.db, owner.joiner, owner.channel.Channel().CreatedAt())
	reserveJoinedChannelTest(t, joinerStore, baseSpec)
	if result, err := joinerStore.InstallJoinedChannel(context.Background(), baseSpec); err != nil || !result.Installed {
		t.Fatalf("initial InstallJoinedChannel() = (%#v,%v)", result, err)
	}
	other := testkit.NewIdentity(t, "join-terminal-replay-other")
	_, expandedRoster := appendRosterMemberWithLabel(t, owner.channel.Descriptor(), owner.signer,
		accepted.Roster, other, other.DisplayName())
	terminalAt := initialAt.Add(2 * time.Second)
	_, terminalRoster := appendRosterTerminal(t, owner.channel.Descriptor(), owner.signer,
		expandedRoster, owner.joiner.PeerID(), model.MemberLeft, terminalAt)
	replaySpec := baseSpec
	replaySpec.Members = terminalRoster.Members()
	replaySpec.At = terminalAt.Add(time.Second)
	replayed, err := joinerStore.InstallJoinedChannel(context.Background(), replaySpec)
	if err != nil || replayed.Installed || replayed.Status != ChannelEnrollmentMemberRevoked ||
		replayed.Channel.Status() != model.ChannelLeft || replayed.Channel.TopicState() != model.TopicLeft ||
		replayed.Channel.RosterHead() != terminalRoster.Head() {
		t.Fatalf("terminal replay = (%#v,%v)", replayed, err)
	}
	assertEnrollmentTableCounts(t, joinerStore, map[string]int{
		"channels": 1, "channel_members": 4, "enrollment_receipts": 1,
		"publication_epochs": 1, "peer_bindings": 2, "enrollment_grants": 0,
	})

	fresh := openTestStore(t)
	insertChannelTestNode(t, fresh.db, owner.joiner, owner.channel.Channel().CreatedAt())
	reserveJoinedChannelTest(t, fresh, replaySpec)
	freshResult, err := fresh.InstallJoinedChannel(context.Background(), replaySpec)
	if err != nil || freshResult.Installed || freshResult.Status != ChannelEnrollmentMemberRevoked {
		t.Fatalf("fresh terminal install = (%#v,%v)", freshResult, err)
	}
	assertEnrollmentTableCounts(t, fresh, map[string]int{
		"channels": 0, "channel_members": 0, "enrollment_receipts": 0,
		"publication_epochs": 0, "peer_bindings": 0,
	})
	ownerCloseAt := terminalAt.Add(2 * time.Second)
	_, closedRoster := appendRosterTerminal(t, owner.channel.Descriptor(), owner.signer,
		terminalRoster, owner.channel.Owner().PeerID(), model.MemberLeft, ownerCloseAt)
	closedSpec := replaySpec
	closedSpec.Members = closedRoster.Members()
	closedSpec.At = ownerCloseAt.Add(time.Second)
	closed, err := joinerStore.InstallJoinedChannel(context.Background(), closedSpec)
	if err != nil || closed.Status != ChannelEnrollmentMemberRevoked ||
		closed.Channel.Status() != model.ChannelClosed || closed.Channel.TopicState() != model.TopicLeft ||
		closed.Channel.RosterHead() != closedRoster.Head() {
		t.Fatalf("owner close after local terminal = (%#v,%v)", closed, err)
	}
	assertEnrollmentTableCounts(t, joinerStore, map[string]int{
		"channels": 1, "channel_members": 5, "enrollment_receipts": 1,
		"publication_epochs": 1, "peer_bindings": 2,
	})
}

func TestInstallJoinedChannelSuffixFailureRollsBackHeadMembersBindingsAndAliases(t *testing.T) {
	t.Parallel()
	fixture := newInstalledJoinedChannelFixture(t, "join-suffix-rollback",
		"suffix-rollback-team", 0x77, 0x78)
	var originalAlias string
	if err := fixture.store.db.QueryRow(`SELECT effective_alias FROM peer_bindings`).Scan(&originalAlias); err != nil {
		t.Fatal(err)
	}
	newPeer := testkit.NewIdentity(t, "join-suffix-rollback-new")
	ownerMember, _ := fixture.accepted.Roster.CurrentMember(fixture.owner.channel.Owner().PeerID())
	_, expanded := appendRosterMemberWithLabel(t, fixture.owner.channel.Descriptor(), fixture.owner.signer,
		fixture.accepted.Roster, newPeer, ownerMember.DisplayLabel())
	if _, err := fixture.store.db.Exec(`CREATE TRIGGER test_reject_suffix_binding
		BEFORE INSERT ON peer_bindings
		BEGIN SELECT RAISE(ABORT, 'test reject suffix binding'); END`); err != nil {
		t.Fatal(err)
	}
	replaySpec := fixture.spec
	replaySpec.Members = expanded.Members()
	replaySpec.At = fixture.at.Add(2 * time.Second)
	if _, err := fixture.store.InstallJoinedChannel(context.Background(), replaySpec); err == nil {
		t.Fatal("suffix binding failure was accepted")
	}
	assertEnrollmentTableCounts(t, fixture.store, map[string]int{
		"channels": 1, "channel_members": 2, "enrollment_receipts": 1,
		"publication_epochs": 1, "peer_bindings": 1,
	})
	var head uint64
	var status, topic, alias string
	if err := fixture.store.db.QueryRow(`SELECT roster_head_revision,status,topic_state FROM channels`).Scan(
		&head, &status, &topic); err != nil || head != 2 || status != "active" || topic != "not_joined" {
		t.Fatalf("Channel after suffix rollback = (%d,%q,%q,%v)", head, status, topic, err)
	}
	if err := fixture.store.db.QueryRow(`SELECT effective_alias FROM peer_bindings`).Scan(&alias); err != nil ||
		alias != originalAlias {
		t.Fatalf("binding alias after suffix rollback = (%q,%v), want %q", alias, err, originalAlias)
	}
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

func TestInstallJoinedChannelKeepsTerminalAliasAndDisambiguatesReplacementPeer(t *testing.T) {
	t.Parallel()
	fixture := newInstalledJoinedChannelFixture(t, "join-alias-churn", "alias-churn-team", 0x7b, 0x7c)
	firstPeer := testkit.NewIdentity(t, "join-alias-churn-first")
	_, firstRoster := appendRosterMemberWithLabel(t, fixture.owner.channel.Descriptor(),
		fixture.owner.signer, fixture.accepted.Roster, firstPeer, "reviewer")
	firstSpec := fixture.spec
	firstSpec.Members = firstRoster.Members()
	firstSpec.At = fixture.at.Add(2 * time.Second)
	if result, err := fixture.store.InstallJoinedChannel(context.Background(), firstSpec); err != nil ||
		result.Roster.Head() != firstRoster.Head() {
		t.Fatalf("first alias suffix = (%#v,%v)", result, err)
	}
	var firstAlias string
	if err := fixture.store.db.QueryRow(`SELECT effective_alias FROM peer_bindings WHERE peer_id=?`,
		firstPeer.PeerID().String()).Scan(&firstAlias); err != nil || firstAlias != "reviewer" {
		t.Fatalf("first reviewer alias = (%q,%v)", firstAlias, err)
	}
	_, terminalRoster := appendRosterTerminal(t, fixture.owner.channel.Descriptor(), fixture.owner.signer,
		firstRoster, firstPeer.PeerID(), model.MemberRevoked, fixture.at.Add(3*time.Second))
	secondPeer := testkit.NewIdentity(t, "join-alias-churn-second")
	_, replacementRoster := appendRosterMemberWithLabel(t, fixture.owner.channel.Descriptor(), fixture.owner.signer,
		terminalRoster, secondPeer, "reviewer")
	replacementSpec := fixture.spec
	replacementSpec.Members = replacementRoster.Members()
	replacementSpec.At = fixture.at.Add(5 * time.Second)
	if result, err := fixture.store.InstallJoinedChannel(context.Background(), replacementSpec); err != nil ||
		result.Roster.Head() != replacementRoster.Head() {
		t.Fatalf("replacement alias suffix = (%#v,%v)", result, err)
	}
	var firstState, retainedAlias, secondState, replacementAlias string
	if err := fixture.store.db.QueryRow(`SELECT state,effective_alias FROM peer_bindings WHERE peer_id=?`,
		firstPeer.PeerID().String()).Scan(&firstState, &retainedAlias); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.db.QueryRow(`SELECT state,effective_alias FROM peer_bindings WHERE peer_id=?`,
		secondPeer.PeerID().String()).Scan(&secondState, &replacementAlias); err != nil {
		t.Fatal(err)
	}
	if firstState != "revoked" || retainedAlias != firstAlias || secondState != "pending" ||
		replacementAlias == firstAlias || !strings.HasPrefix(replacementAlias, "reviewer~") {
		t.Fatalf("alias churn = first(%q,%q) second(%q,%q)", firstState, retainedAlias,
			secondState, replacementAlias)
	}
}

type installedJoinedChannelFixture struct {
	owner    channelEnrollmentFixture
	accepted AcceptChannelEnrollmentResult
	store    *Store
	at       time.Time
	spec     InstallJoinedChannelSpec
}

func newInstalledJoinedChannelFixture(t *testing.T, seed, localAlias string,
	ownerNonce, joinerNonce byte,
) installedJoinedChannelFixture {
	t.Helper()
	owner := newChannelEnrollmentFixture(t, seed)
	transcript := owner.transcript(t, ownerNonce, joinerNonce, owner.head)
	accepted := owner.accept(t, transcript)
	st := openTestStore(t)
	insertChannelTestNode(t, st.db, owner.joiner, owner.channel.Channel().CreatedAt())
	at := owner.acceptedAt.Add(time.Second)
	spec := InstallJoinedChannelSpec{AuthenticatedOwnerPeerID: owner.channel.Owner().PeerID(),
		LocalAlias: localAlias, Descriptor: owner.channel.Descriptor(), Transcript: transcript,
		Receipt: accepted.Receipt, Members: accepted.Roster.Members(), At: at}
	reserveJoinedChannelTest(t, st, spec)
	if result, err := st.InstallJoinedChannel(context.Background(), spec); err != nil || !result.Installed {
		t.Fatalf("initial install = (%#v,%v)", result, err)
	}
	return installedJoinedChannelFixture{owner: owner, accepted: accepted, store: st, at: at, spec: spec}
}
