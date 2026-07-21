package store

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestAcceptChannelLeavePersistsAdvancedSuffixAndExactReplay(t *testing.T) {
	t.Parallel()
	fixture, accepted := acceptedOwnerLeaveFixture(t, "owner-leave-advanced")
	request := signedOwnerStoreLeaveRequest(t, fixture, accepted.Roster,
		fixture.acceptedAt.Add(500*time.Millisecond))
	owner, _ := accepted.Roster.CurrentMember(fixture.channel.Owner().PeerID())
	previous := accepted.Roster.Head().Digest()
	ownerUpdate, advanced := signAndAppendRosterMember(t, fixture.channel.Descriptor(), fixture.signer,
		accepted.Roster, model.MemberRecordSpec{ChannelID: fixture.channel.Channel().ID(),
			DescriptorDigest: fixture.channel.Descriptor().Descriptor().Digest(),
			Revision:         accepted.Roster.Head().Revision() + 1, PreviousDigest: &previous,
			PeerID: owner.PeerID(), OriginEpoch: owner.OriginEpoch(), DisplayLabel: owner.DisplayLabel(),
			PublicKey: owner.PublicKey(), Multiaddrs: owner.Multiaddrs(), Protocols: owner.Protocols(),
			Limits: owner.Limits(), Status: model.MemberActive,
			CreatedAt: fixture.acceptedAt.Add(time.Second)})
	merged, err := fixture.ownerStore.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
		ChannelID:                    fixture.channel.Channel().ID(),
		AuthenticatedTransportPeerID: fixture.channel.Owner().PeerID(),
		Records:                      []model.Member{ownerUpdate}, At: ownerUpdate.CreatedAt()})
	if err != nil || merged.Roster.Head() != advanced.Head() {
		t.Fatalf("advance owner roster = (%#v,%v)", merged, err)
	}
	acceptedAt := fixture.acceptedAt.Add(2 * time.Second)
	result, err := fixture.ownerStore.AcceptChannelLeave(context.Background(), AcceptChannelLeaveSpec{
		AuthenticatedPeerID: fixture.joiner.PeerID(), Request: request,
		Signer: fixture.signer, At: acceptedAt})
	if err != nil || result.Replay || result.Terminal.Status() != model.MemberLeft ||
		result.Roster.Head().Revision() != advanced.Head().Revision()+1 {
		t.Fatalf("AcceptChannelLeave() = (%#v,%v)", result, err)
	}
	suffix := result.Receipt.Record().RosterRecords()
	if len(suffix) != 2 || suffix[0].Head() != ownerUpdate.Head() ||
		suffix[1].Head() != result.Terminal.Head() {
		t.Fatalf("advanced leave suffix = %#v", suffix)
	}
	assertAcceptedOwnerLeaveRow(t, fixture.ownerStore, request, result.Receipt)
	replay, err := fixture.ownerStore.AcceptChannelLeave(context.Background(), AcceptChannelLeaveSpec{
		AuthenticatedPeerID: fixture.joiner.PeerID(), Request: request,
		Signer: fixture.signer, At: acceptedAt.Add(time.Second)})
	if err != nil || !replay.Replay || replay.Roster.Head() != result.Roster.Head() ||
		!bytes.Equal(replay.Receipt.WireJSON().Bytes(), result.Receipt.WireJSON().Bytes()) {
		t.Fatalf("AcceptChannelLeave(replay) = (%#v,%v)", replay, err)
	}
}

func TestAcceptChannelLeaveHonorsPriorRemoveAndSigningRollback(t *testing.T) {
	t.Parallel()
	t.Run("remove wins terminal precedence", func(t *testing.T) {
		fixture, accepted := acceptedOwnerLeaveFixture(t, "owner-leave-revoked")
		request := signedOwnerStoreLeaveRequest(t, fixture, accepted.Roster,
			fixture.acceptedAt.Add(500*time.Millisecond))
		joiner, _ := accepted.Roster.CurrentMember(fixture.joiner.PeerID())
		previous := accepted.Roster.Head().Digest()
		revoked, revokedRoster := signAndAppendRosterMember(t, fixture.channel.Descriptor(),
			fixture.signer, accepted.Roster, model.MemberRecordSpec{
				ChannelID:        fixture.channel.Channel().ID(),
				DescriptorDigest: fixture.channel.Descriptor().Descriptor().Digest(),
				Revision:         accepted.Roster.Head().Revision() + 1, PreviousDigest: &previous,
				PeerID: joiner.PeerID(), OriginEpoch: joiner.OriginEpoch(),
				DisplayLabel: joiner.DisplayLabel(), PublicKey: joiner.PublicKey(),
				Multiaddrs: joiner.Multiaddrs(), Protocols: joiner.Protocols(), Limits: joiner.Limits(),
				Status: model.MemberRevoked, CreatedAt: fixture.acceptedAt.Add(time.Second)})
		if _, err := fixture.ownerStore.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
			ChannelID:                    fixture.channel.Channel().ID(),
			AuthenticatedTransportPeerID: fixture.channel.Owner().PeerID(),
			Records:                      []model.Member{revoked}, At: revoked.CreatedAt()}); err != nil {
			t.Fatal(err)
		}
		result, err := fixture.ownerStore.AcceptChannelLeave(context.Background(), AcceptChannelLeaveSpec{
			AuthenticatedPeerID: fixture.joiner.PeerID(), Request: request,
			Signer: fixture.signer, At: fixture.acceptedAt.Add(2 * time.Second)})
		if err != nil || result.Terminal.Status() != model.MemberRevoked ||
			result.Roster.Head() != revokedRoster.Head() || len(result.Receipt.Record().RosterRecords()) != 1 {
			t.Fatalf("AcceptChannelLeave(after remove) = (%#v,%v)", result, err)
		}
		var leftRecords int
		if err := fixture.ownerStore.db.QueryRow(`SELECT COUNT(*) FROM channel_members
			WHERE channel_id=? AND member_peer_id=? AND status='left'`,
			fixture.channel.Channel().ID().String(), fixture.joiner.PeerID().String()).Scan(&leftRecords); err != nil ||
			leftRecords != 0 {
			t.Fatalf("forged left records after revoke = (%d,%v)", leftRecords, err)
		}
	})

	t.Run("receipt signer failure rolls back member", func(t *testing.T) {
		fixture, accepted := acceptedOwnerLeaveFixture(t, "owner-leave-sign-fail")
		request := signedOwnerStoreLeaveRequest(t, fixture, accepted.Roster,
			fixture.acceptedAt.Add(500*time.Millisecond))
		signer := &failAfterSigner{delegate: fixture.signer, remaining: 1}
		_, err := fixture.ownerStore.AcceptChannelLeave(context.Background(), AcceptChannelLeaveSpec{
			AuthenticatedPeerID: fixture.joiner.PeerID(), Request: request,
			Signer: signer, At: fixture.acceptedAt.Add(time.Second)})
		if err == nil {
			t.Fatal("receipt signer failure unexpectedly committed")
		}
		var head uint64
		var requests int
		if err := fixture.ownerStore.db.QueryRow(`SELECT roster_head_revision FROM channels
			WHERE channel_id=?`, fixture.channel.Channel().ID().String()).Scan(&head); err != nil {
			t.Fatal(err)
		}
		if err := fixture.ownerStore.db.QueryRow(`SELECT COUNT(*) FROM channel_leave_requests
			WHERE channel_id=?`, fixture.channel.Channel().ID().String()).Scan(&requests); err != nil {
			t.Fatal(err)
		}
		if head != accepted.Roster.Head().Revision() || requests != 0 {
			t.Fatalf("failed owner leave partial state = head %d requests %d", head, requests)
		}
	})
}

func TestAcceptChannelLeaveHonorsPriorOwnerCloseWithExactReplay(t *testing.T) {
	t.Parallel()
	fixture, accepted := acceptedOwnerLeaveFixture(t, "owner-leave-closed")
	request := signedOwnerStoreLeaveRequest(t, fixture, accepted.Roster,
		fixture.acceptedAt.Add(500*time.Millisecond))
	owner, _ := accepted.Roster.CurrentMember(fixture.channel.Owner().PeerID())
	previous := accepted.Roster.Head().Digest()
	ownerLeft, closedRoster := signAndAppendRosterMember(t, fixture.channel.Descriptor(),
		fixture.signer, accepted.Roster, model.MemberRecordSpec{
			ChannelID:        fixture.channel.Channel().ID(),
			DescriptorDigest: fixture.channel.Descriptor().Descriptor().Digest(),
			Revision:         accepted.Roster.Head().Revision() + 1, PreviousDigest: &previous,
			PeerID: owner.PeerID(), OriginEpoch: owner.OriginEpoch(), DisplayLabel: owner.DisplayLabel(),
			PublicKey: owner.PublicKey(), Multiaddrs: owner.Multiaddrs(), Protocols: owner.Protocols(),
			Limits: owner.Limits(), Status: model.MemberLeft,
			CreatedAt: fixture.acceptedAt.Add(time.Second)})
	merged, err := fixture.ownerStore.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
		ChannelID:                    fixture.channel.Channel().ID(),
		AuthenticatedTransportPeerID: fixture.channel.Owner().PeerID(),
		Records:                      []model.Member{ownerLeft}, At: ownerLeft.CreatedAt()})
	if err != nil || merged.Channel.Status() != model.ChannelClosed {
		t.Fatalf("close owner Channel = (%#v,%v)", merged, err)
	}
	acceptedAt := fixture.acceptedAt.Add(2 * time.Second)
	result, err := fixture.ownerStore.AcceptChannelLeave(context.Background(), AcceptChannelLeaveSpec{
		AuthenticatedPeerID: fixture.joiner.PeerID(), Request: request,
		Signer: fixture.signer, At: acceptedAt})
	if err != nil || result.Replay || result.Channel.Status() != model.ChannelClosed ||
		result.Roster.Head() != closedRoster.Head() || result.Terminal.Head() != ownerLeft.Head() ||
		len(result.Receipt.Record().RosterRecords()) != 1 {
		t.Fatalf("AcceptChannelLeave(after owner close) = (%#v,%v)", result, err)
	}
	replay, err := fixture.ownerStore.AcceptChannelLeave(context.Background(), AcceptChannelLeaveSpec{
		AuthenticatedPeerID: fixture.joiner.PeerID(), Request: request,
		Signer: fixture.signer, At: acceptedAt.Add(time.Second)})
	if err != nil || !replay.Replay ||
		!bytes.Equal(replay.Receipt.WireJSON().Bytes(), result.Receipt.WireJSON().Bytes()) {
		t.Fatalf("owner-close leave replay = (%#v,%v)", replay, err)
	}
}

func acceptedOwnerLeaveFixture(t *testing.T,
	seed string,
) (channelEnrollmentFixture, AcceptChannelEnrollmentResult) {
	t.Helper()
	fixture := newChannelEnrollmentFixture(t, seed)
	transcript := fixture.transcript(t, 0x71, 0x72, fixture.head)
	return fixture, fixture.accept(t, transcript)
}

func signedOwnerStoreLeaveRequest(t *testing.T, fixture channelEnrollmentFixture,
	roster model.VerifiedRoster, at time.Time,
) model.SignedChannelLeaveRequest {
	t.Helper()
	member, ok := roster.CurrentMember(fixture.joiner.PeerID())
	if !ok {
		t.Fatal("owner roster has no joiner")
	}
	record, err := model.NewChannelLeaveRequestRecord(model.ChannelLeaveRequestRecordSpec{
		ChannelID: fixture.channel.Channel().ID(), MemberPeerID: fixture.joiner.PeerID(),
		ActiveMemberHead: member.Head(), KnownRosterHead: roster.Head(), RequestedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	message, _ := model.ChannelLeaveRequestSigningMessage(record.ChannelID(), record.Digest())
	request, err := model.AttachChannelLeaveRequestSignature(record,
		ed25519.Sign(ed25519Private(fixture.joiner), message))
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func assertAcceptedOwnerLeaveRow(t *testing.T, st *Store,
	request model.SignedChannelLeaveRequest, receipt model.SignedChannelLeaveReceipt,
) {
	t.Helper()
	var status string
	var raw []byte
	if err := st.db.QueryRow(`SELECT status,receipt_json FROM channel_leave_requests
		WHERE request_id=?`, request.RequestID().String()).Scan(&status, &raw); err != nil {
		t.Fatal(err)
	}
	if status != "accepted" || !bytes.Equal(raw, receipt.WireJSON().Bytes()) {
		t.Fatalf("accepted owner leave row = status %q receipt %d bytes", status, len(raw))
	}
}
