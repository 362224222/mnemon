package node

import (
	"context"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestChannelMemberReconcilerMergesOwnTerminalHelloWithoutSyncOrBaseline(t *testing.T) {
	t.Parallel()
	target, terminal := newTerminalChannelMemberTarget(t, "member-reconciler-terminal")
	ack, err := peer.NewMemberHelloAck(peer.MemberHelloAckSpec{ChannelID: target.channel.ID(),
		MissingRecords: []model.Member{terminal}, RosterHead: terminal.Head()})
	if err != nil {
		t.Fatal(err)
	}
	backend := &fakeChannelMemberBackend{target: target, hasTarget: true}
	client := &fakeChannelMemberClient{helloResponse: ack}
	clock := &mutableChannelMemberClock{at: terminal.CreatedAt().Add(time.Second)}
	worker, err := newChannelMemberReconciler(backend, client, clock, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.runCycle(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	merged, head := backend.merged()
	if len(merged) != 1 || merged[0].Head() != terminal.Head() || head != terminal.Head() ||
		client.syncCallsCount() != 0 || client.installCalls() != 0 {
		t.Fatalf("terminal merge = records %#v head %#v sync %d baseline %d", merged, head,
			client.syncCallsCount(), client.installCalls())
	}
}

func newTerminalChannelMemberTarget(t *testing.T, seed string) (channelMemberTarget, model.Member) {
	t.Helper()
	fixture := testkit.NewSignedChannelAt(t, seed,
		time.Date(2026, 7, 21, 7, 0, 0, 0, time.UTC))
	localIdentity := testkit.NewIdentity(t, seed+"-local")
	local := fixture.AppendActiveIdentity(t, localIdentity).Member()
	owner, _ := fixture.Roster().CurrentMember(fixture.Owner().PeerID())
	roster, channel := fixture.Roster(), fixture.Channel()
	lastSeen := local.CreatedAt()
	binding, err := model.NewPeerBinding(local.PeerID(), model.PeerBindingSpec{
		Channel: channel, Roster: roster, PeerID: owner.PeerID(), EffectiveAlias: "owner",
		State: model.BindingActive, Reachability: model.ReachabilityReachable,
		JoinedAt: local.CreatedAt(), LastSeenAt: &lastSeen})
	if err != nil {
		t.Fatal(err)
	}
	terminal := fixture.AppendTerminal(t, local.PeerID(), model.MemberRevoked).Member()
	return channelMemberTarget{channel: channel, roster: roster, localMember: local,
		remoteMember: owner, binding: binding, outboundReady: true}, terminal
}
