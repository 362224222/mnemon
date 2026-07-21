package peer

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestTerminalMemberHelloAcknowledgesOnlyThroughRequesterTerminal(t *testing.T) {
	t.Parallel()
	fixture := testkit.NewSignedChannelAt(t, "terminal-hello",
		time.Date(2026, 7, 21, 6, 0, 0, 0, time.UTC))
	remote := fixture.AppendActive(t, "terminal-hello-remote").Member()
	hello, err := NewMemberHello(MemberHelloSpec{ChannelID: fixture.Channel().ID(),
		ActiveMemberRecord: remote, KnownRosterHead: fixture.Roster().Head(),
		OwnerSignedProofChain: fixture.Roster().Members()})
	if err != nil {
		t.Fatal(err)
	}
	terminal := fixture.AppendTerminal(t, remote.PeerID(), model.MemberRevoked).Member()
	undisclosed := fixture.AppendActiveUpdate(t, fixture.Owner().PeerID()).Member()
	ack, matched, err := terminalMemberHelloAcknowledgement(fixture.Roster(), hello,
		remote.PeerID(), remote.PublicKey())
	missing := ack.MissingRecords()
	if err != nil || !matched || len(missing) != 1 || !sameMember(missing[0], terminal) ||
		ack.RosterHead() != terminal.Head() || ack.RosterHead() == undisclosed.Head() {
		t.Fatalf("terminal acknowledgement = (%#v,%v,%v)", ack, matched, err)
	}
	wrongKey := bytes.Repeat([]byte{0x91}, len(remote.PublicKey()))
	if _, matched, err := terminalMemberHelloAcknowledgement(fixture.Roster(), hello,
		remote.PeerID(), wrongKey); err == nil || matched {
		t.Fatalf("wrong-key terminal acknowledgement = (%v,%v)", matched, err)
	}
}

func TestChannelMemberServiceReturnsTerminalSuffixToHistoricalActivePeer(t *testing.T) {
	fixture := newChannelMemberStoreFixture(t, "terminal-hello-service")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fixture.bootstrapDirect(t, ctx)
	knownRoster := fixture.channel.Roster()
	hello, err := NewMemberHello(MemberHelloSpec{ChannelID: fixture.channel.Channel().ID(),
		ActiveMemberRecord: fixture.remote.Member(), KnownRosterHead: knownRoster.Head(),
		OwnerSignedProofChain: knownRoster.Members()})
	if err != nil {
		t.Fatal(err)
	}
	terminal := fixture.channel.AppendTerminal(t, fixture.remote.Identity().PeerID(),
		model.MemberRevoked).Member()
	result, err := fixture.store.MergeChannelRoster(ctx, store.MergeChannelRosterSpec{
		ChannelID:                    fixture.channel.Channel().ID(),
		AuthenticatedTransportPeerID: fixture.channel.Owner().PeerID(),
		Records:                      []model.Member{terminal}, At: fixture.now})
	if err != nil || result.Status != store.ChannelRosterApplied {
		t.Fatalf("install terminal authority = (%#v,%v)", result, err)
	}
	dispatcher := fixture.serve(t, ctx, fixture.service(t))
	defer dispatcher.Close()
	client, err := NewChannelMemberClient(ChannelMemberClientOptions{Host: fixture.remoteHost,
		Random: bytes.NewReader(bytes.Repeat([]byte{0xa3}, channelRequestIDBytes))})
	if err != nil {
		t.Fatal(err)
	}
	owner, _ := knownRoster.CurrentMember(fixture.channel.Owner().PeerID())
	ack, err := client.Hello(ctx, owner.PeerID(), owner.PublicKey(), hello)
	missing := ack.MissingRecords()
	if err != nil || len(missing) != 1 || !sameMember(missing[0], terminal) ||
		ack.RosterHead() != terminal.Head() {
		t.Fatalf("terminal service acknowledgement = (%#v,%v)", ack, err)
	}
}
