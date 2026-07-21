package model

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"
)

func TestSignedChannelLeaveRequestAndReceiptRoundTrip(t *testing.T) {
	t.Parallel()
	fixture := newChannelLeaveModelFixture(t)
	request := fixture.signedRequest(t)
	if err := VerifyChannelLeaveRequest(fixture.descriptor, fixture.activeMember, request); err != nil {
		t.Fatal(err)
	}
	parsedRequest, err := ParseSignedChannelLeaveRequest(request.WireJSON().Bytes())
	if err != nil || parsedRequest.RequestID() != request.RequestID() ||
		parsedRequest.Record().KnownRosterHead() != fixture.activeMember.Head() {
		t.Fatalf("ParseSignedChannelLeaveRequest() = (%#v, %v)", parsedRequest, err)
	}
	terminal := fixture.memberRecord(t, fixture.activeMember, MemberLeft, fixture.at.Add(2*time.Second))
	receiptRecord, err := NewChannelLeaveReceiptRecord(ChannelLeaveReceiptRecordSpec{
		ChannelID: fixture.channelID, MemberPeerID: fixture.memberPeerID,
		RequestDigest: request.Digest(), RosterRecords: []Member{terminal},
		FinalRosterHead: terminal.Head(), AcceptedAt: fixture.at.Add(3 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	message, _ := ChannelLeaveReceiptSigningMessage(fixture.channelID, receiptRecord.Digest())
	receipt, err := AttachChannelLeaveReceiptSignature(receiptRecord,
		ed25519.Sign(fixture.ownerPrivate, message))
	if err != nil {
		t.Fatal(err)
	}
	parsedReceipt, err := ParseSignedChannelLeaveReceipt(receipt.WireJSON().Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyChannelLeaveReceipt(fixture.descriptor, fixture.activeMember,
		parsedRequest, parsedReceipt); err != nil {
		t.Fatal(err)
	}
	copyRecords := parsedReceipt.Record().RosterRecords()
	copySignature := parsedReceipt.OwnerSignature()
	copyRecords[0] = Member{}
	copySignature[0] ^= 0xff
	if parsedReceipt.Record().RosterRecords()[0].IsZero() ||
		!bytes.Equal(parsedReceipt.OwnerSignature(), receipt.OwnerSignature()) {
		t.Fatal("leave receipt exposed mutable canonical evidence")
	}
}

func TestChannelLeaveEvidenceRejectsForgeryAndDiscontinuousAuthority(t *testing.T) {
	t.Parallel()
	fixture := newChannelLeaveModelFixture(t)
	request := fixture.signedRequest(t)
	wrongSignature := bytes.Repeat([]byte{0x7f}, ed25519.SignatureSize)
	forged, err := AttachChannelLeaveRequestSignature(request.Record(), wrongSignature)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyChannelLeaveRequest(fixture.descriptor, fixture.activeMember, forged); !errors.Is(err, ErrInvalid) {
		t.Fatalf("forged leave request error = %v", err)
	}
	terminal := fixture.memberRecord(t, fixture.activeMember, MemberLeft, fixture.at.Add(2*time.Second))
	otherHead, _ := NewRecordHead(fixture.activeMember.Head().Revision(), Sum([]byte("other-prefix")))
	otherRecord, _ := NewChannelLeaveRequestRecord(ChannelLeaveRequestRecordSpec{
		ChannelID: fixture.channelID, MemberPeerID: fixture.memberPeerID,
		ActiveMemberHead: fixture.activeMember.Head(), KnownRosterHead: otherHead,
		RequestedAt: fixture.at.Add(time.Second)})
	requestMessage, _ := ChannelLeaveRequestSigningMessage(fixture.channelID, otherRecord.Digest())
	otherRequest, _ := AttachChannelLeaveRequestSignature(otherRecord,
		ed25519.Sign(fixture.memberPrivate, requestMessage))
	receiptRecord, _ := NewChannelLeaveReceiptRecord(ChannelLeaveReceiptRecordSpec{
		ChannelID: fixture.channelID, MemberPeerID: fixture.memberPeerID,
		RequestDigest: otherRequest.Digest(), RosterRecords: []Member{terminal},
		FinalRosterHead: terminal.Head(), AcceptedAt: fixture.at.Add(3 * time.Second)})
	receiptMessage, _ := ChannelLeaveReceiptSigningMessage(fixture.channelID, receiptRecord.Digest())
	receipt, _ := AttachChannelLeaveReceiptSignature(receiptRecord,
		ed25519.Sign(fixture.ownerPrivate, receiptMessage))
	if err := VerifyChannelLeaveReceipt(fixture.descriptor, fixture.activeMember,
		otherRequest, receipt); !errors.Is(err, ErrInvalid) {
		t.Fatalf("discontinuous leave receipt error = %v", err)
	}
}

type channelLeaveModelFixture struct {
	channelID     ChannelID
	memberPeerID  PeerID
	descriptor    SignedChannelDescriptor
	ownerPrivate  ed25519.PrivateKey
	memberPrivate ed25519.PrivateKey
	ownerMember   Member
	activeMember  Member
	at            time.Time
}

func newChannelLeaveModelFixture(t *testing.T) channelLeaveModelFixture {
	t.Helper()
	ownerPeerID, ownerKey, ownerPrivate := canonicalDescriptorIdentity(t, "leave-model-owner")
	memberPeerID, memberKey, memberPrivate := canonicalDescriptorIdentity(t, "leave-model-member")
	channelID, _ := ParseChannelID("channel-leave-model")
	descriptor := signedRecordDescriptor(t, channelID, ownerPeerID, ownerKey, ownerPrivate)
	at := descriptor.Descriptor().CreatedAt()
	ownerEpoch, _ := ParseOriginEpoch("epoch-leave-model-owner")
	genesisRecord, _ := NewMemberRecord(MemberRecordSpec{ChannelID: channelID,
		DescriptorDigest: descriptor.Descriptor().Digest(), Revision: 1, PeerID: ownerPeerID,
		OriginEpoch: ownerEpoch, DisplayLabel: "owner", PublicKey: ownerKey,
		Multiaddrs: []string{"/ip4/127.0.0.1/tcp/4101"}, Protocols: RequiredMemberProtocols(),
		Limits: DefaultMemberLimits(), Status: MemberActive, CreatedAt: at})
	genesisMessage, _ := MemberRecordSigningMessage(channelID, genesisRecord.Digest())
	genesis, _ := AttachMemberSignature(genesisRecord, ed25519.Sign(ownerPrivate, genesisMessage))
	memberEpoch, _ := ParseOriginEpoch("epoch-leave-model-member")
	previous := genesis.Head().Digest()
	memberRecord, _ := NewMemberRecord(MemberRecordSpec{ChannelID: channelID,
		DescriptorDigest: descriptor.Descriptor().Digest(), Revision: 2, PreviousDigest: &previous,
		PeerID: memberPeerID, OriginEpoch: memberEpoch, DisplayLabel: "member", PublicKey: memberKey,
		Multiaddrs: []string{"/ip4/127.0.0.1/tcp/4102"}, Protocols: RequiredMemberProtocols(),
		Limits: DefaultMemberLimits(), Status: MemberActive, CreatedAt: at.Add(time.Second)})
	memberMessage, _ := MemberRecordSigningMessage(channelID, memberRecord.Digest())
	member, _ := AttachMemberSignature(memberRecord, ed25519.Sign(ownerPrivate, memberMessage))
	return channelLeaveModelFixture{channelID: channelID, memberPeerID: memberPeerID,
		descriptor: descriptor, ownerPrivate: ownerPrivate, memberPrivate: memberPrivate,
		ownerMember: genesis, activeMember: member, at: at.Add(time.Second)}
}

func (fixture channelLeaveModelFixture) signedRequest(t *testing.T) SignedChannelLeaveRequest {
	t.Helper()
	record, err := NewChannelLeaveRequestRecord(ChannelLeaveRequestRecordSpec{
		ChannelID: fixture.channelID, MemberPeerID: fixture.memberPeerID,
		ActiveMemberHead: fixture.activeMember.Head(), KnownRosterHead: fixture.activeMember.Head(),
		RequestedAt: fixture.at.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	message, _ := ChannelLeaveRequestSigningMessage(fixture.channelID, record.Digest())
	request, err := AttachChannelLeaveRequestSignature(record,
		ed25519.Sign(fixture.memberPrivate, message))
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func (fixture channelLeaveModelFixture) memberRecord(t *testing.T, previous Member,
	status MemberStatus, at time.Time,
) Member {
	t.Helper()
	prior := previous.Head().Digest()
	record, err := NewMemberRecord(MemberRecordSpec{ChannelID: fixture.channelID,
		DescriptorDigest: fixture.descriptor.Descriptor().Digest(),
		Revision:         previous.Head().Revision() + 1, PreviousDigest: &prior,
		PeerID: fixture.memberPeerID, OriginEpoch: previous.OriginEpoch(),
		DisplayLabel: previous.DisplayLabel(), PublicKey: previous.PublicKey(),
		Multiaddrs: previous.Multiaddrs(), Protocols: previous.Protocols(), Limits: previous.Limits(),
		Status: status, CreatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	message, _ := MemberRecordSigningMessage(fixture.channelID, record.Digest())
	member, err := AttachMemberSignature(record, ed25519.Sign(fixture.ownerPrivate, message))
	if err != nil {
		t.Fatal(err)
	}
	return member
}
