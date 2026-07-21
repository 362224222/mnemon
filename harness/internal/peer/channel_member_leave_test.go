package peer

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestChannelMemberClientLeaveUsesSecureIdentityAndOwnerReceipt(t *testing.T) {
	fixture := newChannelMemberStoreFixture(t, "member-leave")
	request, receipt := channelMemberLeaveEvidence(t, fixture)
	controller := &channelLeaveControllerStub{ChannelMemberController: fixture.controller,
		result: ChannelMemberLeaveAuthority{Descriptor: fixture.channel.Descriptor(),
			ActiveMember: fixture.remote.Member(), Receipt: receipt}}
	service, err := NewChannelMemberService(ChannelMemberServiceOptions{
		Controller: controller, Clock: fixedChannelMemberClock{at: fixture.now}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dispatcher := fixture.serve(t, ctx, service)
	defer dispatcher.Close()
	client, err := NewChannelMemberClient(ChannelMemberClientOptions{Host: fixture.remoteHost,
		Random: bytes.NewReader(bytes.Repeat([]byte{0x83}, channelRequestIDBytes))})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := NewLeaveRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Leave(ctx, fixture.channel.Owner().PeerID(),
		fixture.channel.Owner().PublicKey(), payload)
	if err != nil || !bytes.Equal(response.SignedReceipt().WireJSON().Bytes(), receipt.WireJSON().Bytes()) {
		t.Fatalf("ChannelMemberClient.Leave() = (%#v,%v)", response, err)
	}
	if controller.control.AuthenticatedPeerID != fixture.remote.Identity().PeerID() ||
		controller.control.Request.RequestID() != request.RequestID() {
		t.Fatalf("leave controller control = %#v", controller.control)
	}
}

type channelLeaveControllerStub struct {
	ChannelMemberController
	control ChannelMemberLeaveControl
	result  ChannelMemberLeaveAuthority
}

func (controller *channelLeaveControllerStub) AcceptMemberLeaveGate(_ context.Context,
	control ChannelMemberLeaveControl,
) (ChannelMemberLeaveAuthority, error) {
	controller.control = control
	return controller.result, nil
}

func channelMemberLeaveEvidence(t *testing.T,
	fixture *channelMemberStoreFixture,
) (model.SignedChannelLeaveRequest, model.SignedChannelLeaveReceipt) {
	t.Helper()
	head := fixture.channel.Roster().Head()
	requestRecord, err := model.NewChannelLeaveRequestRecord(model.ChannelLeaveRequestRecordSpec{
		ChannelID: fixture.channel.Channel().ID(), MemberPeerID: fixture.remote.Identity().PeerID(),
		ActiveMemberHead: fixture.remote.Member().Head(), KnownRosterHead: head, RequestedAt: fixture.now})
	if err != nil {
		t.Fatal(err)
	}
	requestMessage, _ := model.ChannelLeaveRequestSigningMessage(fixture.channel.Channel().ID(),
		requestRecord.Digest())
	request, err := model.AttachChannelLeaveRequestSignature(requestRecord,
		ed25519.Sign(testIdentityPrivateKey(t, fixture.remote.Identity()), requestMessage))
	if err != nil {
		t.Fatal(err)
	}
	terminal := fixture.channel.AppendTerminal(t, fixture.remote.Identity().PeerID(), model.MemberLeft).Member()
	receiptRecord, err := model.NewChannelLeaveReceiptRecord(model.ChannelLeaveReceiptRecordSpec{
		ChannelID: fixture.channel.Channel().ID(), MemberPeerID: fixture.remote.Identity().PeerID(),
		RequestDigest: request.Digest(), RosterRecords: []model.Member{terminal},
		FinalRosterHead: terminal.Head(), AcceptedAt: fixture.now})
	if err != nil {
		t.Fatal(err)
	}
	receiptMessage, _ := model.ChannelLeaveReceiptSigningMessage(fixture.channel.Channel().ID(),
		receiptRecord.Digest())
	receipt, err := model.AttachChannelLeaveReceiptSignature(receiptRecord,
		ed25519.Sign(testIdentityPrivateKey(t, fixture.channel.Owner()), receiptMessage))
	if err != nil {
		t.Fatal(err)
	}
	return request, receipt
}

func testIdentityPrivateKey(t *testing.T, identity testkit.Identity) ed25519.PrivateKey {
	t.Helper()
	privateKey, err := identity.Libp2pPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := privateKey.Raw()
	if err != nil {
		t.Fatal(err)
	}
	return ed25519.PrivateKey(raw)
}
