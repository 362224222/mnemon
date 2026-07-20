package peer

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestChannelMemberClientAuthenticatesHelloSyncAndBaseline(t *testing.T) {
	fixture := newChannelMemberStoreFixture(t, "member-client")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dispatcher := fixture.serve(t, ctx, fixture.service(t))
	defer dispatcher.Close()
	client, err := NewChannelMemberClient(ChannelMemberClientOptions{Host: fixture.remoteHost,
		Random: bytes.NewReader(bytes.Repeat([]byte{0x51}, 256))})
	if err != nil {
		t.Fatal(err)
	}
	owner := fixture.channel.OwnerMember().Member()
	hello, err := NewMemberHello(MemberHelloSpec{ChannelID: fixture.channel.Channel().ID(),
		ActiveMemberRecord: fixture.remote.Member(), KnownRosterHead: fixture.channel.Roster().Head(),
		OwnerSignedProofChain: fixture.channel.Roster().Members()})
	if err != nil {
		t.Fatal(err)
	}
	ack, err := client.Hello(ctx, owner.PeerID(), owner.PublicKey(), hello)
	if err != nil || ack.RosterHead() != fixture.channel.Roster().Head() || len(ack.MissingRecords()) != 0 {
		t.Fatalf("Hello() = (%#v,%v)", ack, err)
	}
	syncRequest, err := NewSyncRequest(SyncRequestSpec{ChannelID: fixture.channel.Channel().ID(),
		AfterHead: fixture.channel.OwnerMember().Member().Head()})
	if err != nil {
		t.Fatal(err)
	}
	pages, err := client.Sync(ctx, owner.PeerID(), owner.PublicKey(), syncRequest)
	if err != nil || len(pages) != 1 || len(pages[0].OwnerSignedRecords()) != 1 ||
		pages[0].RosterHead() != fixture.channel.Roster().Head() {
		t.Fatalf("Sync() = (%#v,%v)", pages, err)
	}
	baseline, err := NewDataBaseline(DataBaselineSpec{ChannelID: fixture.channel.Channel().ID(),
		OriginPeerID: fixture.remote.Identity().PeerID(), OriginEpoch: fixture.remote.Identity().OriginEpoch(),
		BaselineChannelSequence: 3})
	if err != nil {
		t.Fatal(err)
	}
	baselineAck, err := client.InstallBaseline(ctx, owner.PeerID(), owner.PublicKey(), baseline)
	if err != nil || baselineAck.BaselineChannelSequence() != 3 ||
		baselineAck.OriginPeerID() != fixture.remote.Identity().PeerID() {
		t.Fatalf("InstallBaseline() = (%#v,%v)", baselineAck, err)
	}
}

func TestChannelMemberClientRejectsWrongSignedKeyAndRemoteFailure(t *testing.T) {
	fixture := newChannelMemberStoreFixture(t, "member-client-failure")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dispatcher := fixture.serve(t, ctx, fixture.service(t))
	defer dispatcher.Close()
	client, err := NewChannelMemberClient(ChannelMemberClientOptions{Host: fixture.remoteHost})
	if err != nil {
		t.Fatal(err)
	}
	owner := fixture.channel.OwnerMember().Member()
	hello, err := NewMemberHello(MemberHelloSpec{ChannelID: fixture.channel.Channel().ID(),
		ActiveMemberRecord: fixture.remote.Member(), KnownRosterHead: fixture.channel.Roster().Head(),
		OwnerSignedProofChain: fixture.channel.Roster().Members()})
	if err != nil {
		t.Fatal(err)
	}
	wrongKey := append([]byte(nil), owner.PublicKey()...)
	wrongKey[0] ^= 0xff
	if _, err := client.Hello(ctx, owner.PeerID(), wrongKey, hello); !errors.Is(err,
		ErrChannelMemberClientResponse) {
		t.Fatalf("wrong signed key error = %v", err)
	}
	if _, err := client.Hello(ctx, owner.PeerID(), owner.PublicKey(), hello); err != nil {
		t.Fatalf("bootstrap Hello: %v", err)
	}
	ahead, _ := model.NewRecordHead(fixture.channel.Roster().Head().Revision()+1,
		model.Sum([]byte("member-client-ahead")))
	aheadHello, err := NewMemberHello(MemberHelloSpec{ChannelID: fixture.channel.Channel().ID(),
		ActiveMemberRecord: fixture.remote.Member(), KnownRosterHead: ahead})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Hello(ctx, owner.PeerID(), owner.PublicKey(), aheadHello)
	var failure *ChannelProtocolFailure
	if !errors.As(err, &failure) || failure.Code() != ChannelErrorRosterGap ||
		!failure.Retryable() || failure.RetryAfter() != channelMemberGapRetry {
		t.Fatalf("remote roster gap = %#v (%v)", failure, err)
	}
}

func TestChannelMemberClientRejectsDisconnectedTransportAndInvalidConstruction(t *testing.T) {
	fixture := newChannelMemberStoreFixture(t, "member-client-transport")
	client, err := NewChannelMemberClient(ChannelMemberClientOptions{Host: fixture.remoteHost})
	if err != nil {
		t.Fatal(err)
	}
	owner := fixture.channel.OwnerMember().Member()
	hello, err := NewMemberHello(MemberHelloSpec{ChannelID: fixture.channel.Channel().ID(),
		ActiveMemberRecord: fixture.remote.Member(), KnownRosterHead: fixture.channel.Roster().Head(),
		OwnerSignedProofChain: fixture.channel.Roster().Members()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := client.Hello(ctx, owner.PeerID(), owner.PublicKey(), hello); err == nil ||
		(!errors.Is(err, ErrChannelMemberClientTransport) && !errors.Is(err, context.DeadlineExceeded)) {
		t.Fatalf("disconnected Hello error = %v", err)
	}
	if constructed, err := NewChannelMemberClient(ChannelMemberClientOptions{}); constructed != nil ||
		!errors.Is(err, ErrChannelMemberClient) {
		t.Fatalf("missing Host construction = (%#v,%v)", constructed, err)
	}
}
