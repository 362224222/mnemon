package model

import (
	"crypto/ed25519"
	"errors"
	"testing"
	"time"
)

func TestChannelLeaveReceiptAcceptsOwnerClosePrecedence(t *testing.T) {
	t.Parallel()
	fixture := newChannelLeaveModelFixture(t)
	request := fixture.signedRequest(t)
	receipt := fixture.signedOwnerTerminalReceipt(t, request, MemberLeft)
	if err := VerifyChannelLeaveReceipt(fixture.descriptor, fixture.activeMember,
		request, receipt); err != nil {
		t.Fatalf("owner-close leave receipt = %v", err)
	}
}

func TestChannelLeaveReceiptRejectsInapplicableOwnerTerminal(t *testing.T) {
	t.Parallel()
	fixture := newChannelLeaveModelFixture(t)
	request := fixture.signedRequest(t)
	receipt := fixture.signedOwnerTerminalReceipt(t, request, MemberRevoked)
	if err := VerifyChannelLeaveReceipt(fixture.descriptor, fixture.activeMember,
		request, receipt); !errors.Is(err, ErrInvalid) {
		t.Fatalf("owner-revoked leave receipt error = %v", err)
	}
}

func (fixture channelLeaveModelFixture) signedOwnerTerminalReceipt(t *testing.T,
	request SignedChannelLeaveRequest, status MemberStatus,
) SignedChannelLeaveReceipt {
	t.Helper()
	previous := fixture.activeMember.Head().Digest()
	owner := fixture.ownerMember
	record, err := NewMemberRecord(MemberRecordSpec{ChannelID: fixture.channelID,
		DescriptorDigest: fixture.descriptor.Descriptor().Digest(),
		Revision:         request.Record().KnownRosterHead().Revision() + 1, PreviousDigest: &previous,
		PeerID: owner.PeerID(), OriginEpoch: owner.OriginEpoch(), DisplayLabel: owner.DisplayLabel(),
		PublicKey: owner.PublicKey(), Multiaddrs: owner.Multiaddrs(), Protocols: owner.Protocols(),
		Limits: owner.Limits(), Status: status, CreatedAt: fixture.at.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	message, _ := MemberRecordSigningMessage(fixture.channelID, record.Digest())
	ownerLeft, err := AttachMemberSignature(record, ed25519.Sign(fixture.ownerPrivate, message))
	if err != nil {
		t.Fatal(err)
	}
	receiptRecord, err := NewChannelLeaveReceiptRecord(ChannelLeaveReceiptRecordSpec{
		ChannelID: fixture.channelID, MemberPeerID: fixture.memberPeerID,
		RequestDigest: request.Digest(), RosterRecords: []Member{ownerLeft},
		FinalRosterHead: ownerLeft.Head(), AcceptedAt: fixture.at.Add(3 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	receiptMessage, _ := ChannelLeaveReceiptSigningMessage(fixture.channelID, receiptRecord.Digest())
	receipt, _ := AttachChannelLeaveReceiptSignature(receiptRecord,
		ed25519.Sign(fixture.ownerPrivate, receiptMessage))
	return receipt
}
