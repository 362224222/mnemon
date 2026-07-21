package peer

import (
	"bytes"
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestChannelLeaveFramesPreserveSignedEvidence(t *testing.T) {
	t.Parallel()
	fixture := newChannelFrameFixture(t)
	requestRecord, err := model.NewChannelLeaveRequestRecord(model.ChannelLeaveRequestRecordSpec{
		ChannelID: fixture.channelID, MemberPeerID: fixture.joiner.modelID,
		ActiveMemberHead: fixture.joiningMember.Head(), KnownRosterHead: fixture.joiningMember.Head(),
		RequestedAt: fixture.createdAt.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	requestMessage, _ := model.ChannelLeaveRequestSigningMessage(fixture.channelID,
		requestRecord.Digest())
	signedRequest, err := model.AttachChannelLeaveRequestSignature(requestRecord,
		ed25519.Sign(fixture.joiner.privateKey, requestMessage))
	if err != nil {
		t.Fatal(err)
	}
	terminal := fixture.terminalJoiningMember(t)
	receiptRecord, err := model.NewChannelLeaveReceiptRecord(model.ChannelLeaveReceiptRecordSpec{
		ChannelID: fixture.channelID, MemberPeerID: fixture.joiner.modelID,
		RequestDigest: signedRequest.Digest(), RosterRecords: []model.Member{terminal},
		FinalRosterHead: terminal.Head(), AcceptedAt: fixture.createdAt.Add(3 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	receiptMessage, _ := model.ChannelLeaveReceiptSigningMessage(fixture.channelID,
		receiptRecord.Digest())
	signedReceipt, err := model.AttachChannelLeaveReceiptSignature(receiptRecord,
		ed25519.Sign(fixture.owner.privateKey, receiptMessage))
	if err != nil {
		t.Fatal(err)
	}
	request, _ := NewLeaveRequest(signedRequest)
	receipt, _ := NewLeaveReceipt(signedReceipt)
	for _, test := range []struct {
		payload ChannelFramePayload
		want    ChannelFrameType
	}{
		{payload: request, want: ChannelFrameLeaveRequest},
		{payload: receipt, want: ChannelFrameLeaveReceipt},
	} {
		frame, err := NewChannelFrame(fixture.frameRequestID, test.payload)
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := ParseChannelFrame(frame.CanonicalJSON().Bytes())
		if err != nil || parsed.Type() != test.want ||
			!bytes.Equal(parsed.Payload().CanonicalJSON().Bytes(), test.payload.CanonicalJSON().Bytes()) {
			t.Fatalf("leave frame round trip = (%#v,%v), want %q", parsed, err, test.want)
		}
	}
	if err := model.VerifyChannelLeaveReceipt(fixture.descriptor, fixture.joiningMember,
		signedRequest, signedReceipt); err != nil {
		t.Fatal(err)
	}
}
