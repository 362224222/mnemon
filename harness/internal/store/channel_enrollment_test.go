package store

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestAcceptChannelEnrollmentCommitsAndReplaysAfterGrantClosure(t *testing.T) {
	t.Parallel()
	fixture := newChannelEnrollmentFixture(t, "accept-replay")
	accepted := fixture.accept(t, fixture.transcript(t, 0x21, 0x22, fixture.head))
	if accepted.Status != ChannelEnrollmentAccepted || accepted.Member.PeerID() != fixture.joiner.PeerID() ||
		accepted.Roster.Head().Revision() != 2 {
		t.Fatalf("AcceptChannelEnrollment() = %#v", accepted)
	}
	assertEnrollmentTableCounts(t, fixture.ownerStore, map[string]int{
		"channel_members": 2, "enrollment_grant_uses": 1, "enrollment_receipts": 1,
		"peer_bindings": 1,
	})
	if _, err := fixture.ownerStore.CloseChannelInvite(context.Background(), fixture.channel.Channel().ID(),
		fixture.grantID, fixture.acceptedAt.Add(time.Second)); err != nil {
		t.Fatalf("CloseChannelInvite() error = %v", err)
	}

	prepared, err := fixture.ownerStore.PrepareChannelEnrollment(context.Background(), fixture.prepareSpec(
		fixture.acceptedAt.Add(2*time.Second)))
	if err != nil || prepared.Status != ChannelEnrollmentReplayed || prepared.RosterHead != fixture.head {
		t.Fatalf("PrepareChannelEnrollment(replay) = (%#v, %v)", prepared, err)
	}
	replayed := fixture.acceptAt(t, fixture.transcript(t, 0x31, 0x32, prepared.RosterHead),
		fixture.acceptedAt.Add(2*time.Second))
	if replayed.Status != ChannelEnrollmentReplayed ||
		!bytes.Equal(replayed.Receipt.WireJSON().Bytes(), accepted.Receipt.WireJSON().Bytes()) ||
		replayed.Channel.RosterHead() != accepted.Channel.RosterHead() {
		t.Fatalf("AcceptChannelEnrollment(replay) = %#v", replayed)
	}
	assertEnrollmentTableCounts(t, fixture.ownerStore, map[string]int{
		"channel_members": 2, "enrollment_grant_uses": 1, "enrollment_receipts": 1,
	})
	var status string
	var used uint8
	if err := fixture.ownerStore.db.QueryRow(`SELECT status,used_uses FROM enrollment_grants
		WHERE grant_id=?`, fixture.grantID.String()).Scan(&status, &used); err != nil ||
		status != "closed" || used != 1 {
		t.Fatalf("closed grant = (%q,%d,%v)", status, used, err)
	}
	changedRequest, _ := model.ParseEnrollmentRequestID("request-accept-replay-changed")
	requestSpec := fixture.prepareSpec(fixture.acceptedAt.Add(3 * time.Second))
	requestSpec.RequestID = changedRequest
	if _, err := fixture.ownerStore.PrepareChannelEnrollment(context.Background(), requestSpec); !errors.Is(err, ErrChannelEnrollmentProof) {
		t.Fatalf("changed replay request error = %v", err)
	}
	changedEpoch, _ := model.ParseOriginEpoch("epoch-accept-replay-changed")
	epochSpec := fixture.prepareSpec(fixture.acceptedAt.Add(3 * time.Second))
	epochSpec.JoinerOriginEpoch = changedEpoch
	if _, err := fixture.ownerStore.PrepareChannelEnrollment(context.Background(), epochSpec); !errors.Is(err, ErrChannelEnrollmentProof) {
		t.Fatalf("changed replay epoch error = %v", err)
	}
	wrongPredecessor := fixture.transcript(t, 0x35, 0x36, accepted.Roster.Head())
	_, err = fixture.ownerStore.AcceptChannelEnrollment(context.Background(), AcceptChannelEnrollmentSpec{
		AuthenticatedPeerID: fixture.joiner.PeerID(), Transcript: wrongPredecessor,
		AdvertisedMultiaddrs: fixture.joiner.Multiaddrs(),
		Proof:                enrollmentTestProof(t, fixture.token, wrongPredecessor), Signer: fixture.signer,
		At: fixture.acceptedAt.Add(3 * time.Second),
	})
	if !errors.Is(err, ErrChannelEnrollmentProof) {
		t.Fatalf("changed replay predecessor error = %v", err)
	}
	assertEnrollmentTableCounts(t, fixture.ownerStore, map[string]int{
		"channel_members": 2, "enrollment_grant_uses": 1, "enrollment_receipts": 1,
	})
}

func TestAcceptChannelEnrollmentUsesOneGrantForMultipleAuthenticatedPeers(t *testing.T) {
	t.Parallel()
	fixture := newChannelEnrollmentFixture(t, "multi-use")
	first := fixture.accept(t, fixture.transcript(t, 0x33, 0x34, fixture.head))
	secondPeer := testkit.NewIdentity(t, "multi-use-second-peer")
	secondRequest := stableEnrollmentRequest(t, fixture.channel.Channel().ID(), fixture.grantID,
		secondPeer)
	secondAt := fixture.acceptedAt.Add(time.Second)
	prepared, err := fixture.ownerStore.PrepareChannelEnrollment(context.Background(),
		PrepareChannelEnrollmentSpec{ChannelID: fixture.channel.Channel().ID(), GrantID: fixture.grantID,
			RequestID: secondRequest, AuthenticatedPeerID: secondPeer.PeerID(),
			JoinerOriginEpoch: secondPeer.OriginEpoch(), JoinerPublicKey: secondPeer.PublicKey(), At: secondAt})
	if err != nil || prepared.RosterHead != first.Roster.Head() {
		t.Fatalf("prepare second grant use = (%#v,%v)", prepared, err)
	}
	transcript := enrollmentTestTranscript(t, fixture.channel.Descriptor(), fixture.grantID,
		secondRequest, secondPeer, prepared.RosterHead, 0x35, 0x36)
	proof := enrollmentTestProof(t, fixture.token, transcript)
	second, err := fixture.ownerStore.AcceptChannelEnrollment(context.Background(), AcceptChannelEnrollmentSpec{
		AuthenticatedPeerID: secondPeer.PeerID(), Transcript: transcript,
		AdvertisedMultiaddrs: secondPeer.Multiaddrs(), Proof: proof, Signer: fixture.signer, At: secondAt})
	if err != nil || second.Status != ChannelEnrollmentAccepted || second.Roster.Head().Revision() != 3 {
		t.Fatalf("second grant use = (%#v,%v)", second, err)
	}
	assertEnrollmentTableCounts(t, fixture.ownerStore, map[string]int{
		"channel_members": 3, "enrollment_grant_uses": 2, "enrollment_receipts": 2,
	})
	var status string
	var used uint8
	if err := fixture.ownerStore.db.QueryRow(`SELECT status,used_uses FROM enrollment_grants
		WHERE grant_id=?`, fixture.grantID.String()).Scan(&status, &used); err != nil ||
		status != "open" || used != 2 {
		t.Fatalf("multi-use grant projection = (%q,%d,%v)", status, used, err)
	}
	secondStore := openTestStore(t)
	insertChannelTestNode(t, secondStore.db, secondPeer, fixture.channel.Channel().CreatedAt())
	installSpec := InstallJoinedChannelSpec{
		AuthenticatedOwnerPeerID: fixture.channel.Owner().PeerID(), LocalAlias: "multi-use-team",
		OwnerOutcome: ChannelEnrollmentAccepted,
		Descriptor:   fixture.channel.Descriptor(), Transcript: transcript, Receipt: second.Receipt,
		Members: second.Roster.Members(), At: secondAt.Add(time.Second)}
	reserveJoinedChannelTest(t, secondStore, installSpec)
	installed, err := secondStore.InstallJoinedChannel(context.Background(), installSpec)
	if err != nil || !installed.Installed || len(installed.Roster.Members()) != 3 {
		t.Fatalf("multi-member initial install = (%#v,%v)", installed, err)
	}
	assertEnrollmentTableCounts(t, secondStore, map[string]int{
		"channels": 1, "channel_members": 3, "enrollment_receipts": 1,
		"publication_epochs": 1, "peer_bindings": 2, "enrollment_grants": 0,
	})
}

func TestAcceptChannelEnrollmentTerminalReplayReturnsOriginalReceiptAndLatestRoster(t *testing.T) {
	t.Parallel()
	fixture := newChannelEnrollmentFixture(t, "terminal-owner-replay")
	initialTranscript := fixture.transcript(t, 0x37, 0x38, fixture.head)
	accepted := fixture.accept(t, initialTranscript)
	closedAt := fixture.acceptedAt.Add(time.Second)
	if _, err := fixture.ownerStore.CloseChannelInvite(context.Background(),
		fixture.channel.Channel().ID(), fixture.grantID, closedAt); err != nil {
		t.Fatal(err)
	}
	terminalAt := closedAt.Add(time.Second)
	terminalMember, terminalRoster := appendRosterTerminal(t, fixture.channel.Descriptor(), fixture.signer,
		accepted.Roster, fixture.joiner.PeerID(), model.MemberRevoked, terminalAt)
	merged, err := fixture.ownerStore.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
		ChannelID:                    fixture.channel.Channel().ID(),
		AuthenticatedTransportPeerID: fixture.channel.Owner().PeerID(),
		Records:                      []model.Member{terminalMember}, At: terminalAt,
	})
	if err != nil || merged.Status != ChannelRosterApplied || merged.Roster.Head() != terminalRoster.Head() {
		t.Fatalf("terminal roster merge = (%#v,%v)", merged, err)
	}
	replayAt := terminalAt.Add(time.Second)
	prepared, err := fixture.ownerStore.PrepareChannelEnrollment(context.Background(),
		fixture.prepareSpec(replayAt))
	if err != nil || prepared.Status != ChannelEnrollmentMemberRevoked || prepared.RosterHead != fixture.head {
		t.Fatalf("terminal Prepare replay = (%#v,%v)", prepared, err)
	}
	replayTranscript := fixture.transcript(t, 0x39, 0x3a, prepared.RosterHead)
	replayed := fixture.acceptAt(t, replayTranscript, replayAt)
	if replayed.Status != ChannelEnrollmentMemberRevoked || replayed.Roster.Head() != terminalRoster.Head() ||
		!bytes.Equal(replayed.Receipt.WireJSON().Bytes(), accepted.Receipt.WireJSON().Bytes()) ||
		replayed.Member.Head() != accepted.Member.Head() {
		t.Fatalf("terminal Accept replay = %#v", replayed)
	}
	assertEnrollmentTableCounts(t, fixture.ownerStore, map[string]int{
		"channel_members": 3, "enrollment_grant_uses": 1, "enrollment_receipts": 1,
	})
	var status string
	var used uint8
	if err := fixture.ownerStore.db.QueryRow(`SELECT status,used_uses FROM enrollment_grants
		WHERE grant_id=?`, fixture.grantID.String()).Scan(&status, &used); err != nil ||
		status != "closed" || used != 1 {
		t.Fatalf("terminal replay grant = (%q,%d,%v)", status, used, err)
	}
}

func TestAcceptChannelEnrollmentRejectsBadProofStaleHeadAndRollsBackSignerFailure(t *testing.T) {
	t.Parallel()
	t.Run("noncanonical request ID", func(t *testing.T) {
		fixture := newChannelEnrollmentFixture(t, "noncanonical-request")
		requestID, _ := model.ParseEnrollmentRequestID("request-noncanonical-owner-input")
		transcript := enrollmentTestTranscript(t, fixture.channel.Descriptor(), fixture.grantID,
			requestID, fixture.joiner, fixture.head, 0x3d, 0x3e)
		_, err := fixture.ownerStore.AcceptChannelEnrollment(context.Background(), AcceptChannelEnrollmentSpec{
			AuthenticatedPeerID: fixture.joiner.PeerID(), Transcript: transcript,
			AdvertisedMultiaddrs: fixture.joiner.Multiaddrs(),
			Proof:                enrollmentTestProof(t, fixture.token, transcript), Signer: fixture.signer,
			At: fixture.acceptedAt,
		})
		if !errors.Is(err, ErrChannelEnrollmentProof) {
			t.Fatalf("noncanonical request error = %v", err)
		}
		assertEnrollmentTableCounts(t, fixture.ownerStore, map[string]int{
			"channel_members": 1, "enrollment_grant_uses": 0, "enrollment_receipts": 0,
		})
	})
	t.Run("bad proof", func(t *testing.T) {
		fixture := newChannelEnrollmentFixture(t, "bad-proof")
		transcript := fixture.transcript(t, 0x41, 0x42, fixture.head)
		_, err := fixture.ownerStore.AcceptChannelEnrollment(context.Background(), AcceptChannelEnrollmentSpec{
			AuthenticatedPeerID: fixture.joiner.PeerID(), Transcript: transcript,
			AdvertisedMultiaddrs: fixture.joiner.Multiaddrs(), Proof: model.Sum([]byte("forged proof")),
			Signer: fixture.signer, At: fixture.acceptedAt})
		if !errors.Is(err, ErrChannelEnrollmentProof) {
			t.Fatalf("bad proof error = %v", err)
		}
		assertEnrollmentTableCounts(t, fixture.ownerStore, map[string]int{
			"channel_members": 1, "enrollment_grant_uses": 0, "enrollment_receipts": 0,
		})
	})

	t.Run("secure peer cannot be asserted by frame", func(t *testing.T) {
		fixture := newChannelEnrollmentFixture(t, "secure-peer-binding")
		transcript := fixture.transcript(t, 0x43, 0x44, fixture.head)
		proof := enrollmentTestProof(t, fixture.token, transcript)
		otherPeer := testkit.NewIdentity(t, "secure-peer-binding-attacker")
		_, err := fixture.ownerStore.AcceptChannelEnrollment(context.Background(), AcceptChannelEnrollmentSpec{
			AuthenticatedPeerID: otherPeer.PeerID(), Transcript: transcript,
			AdvertisedMultiaddrs: fixture.joiner.Multiaddrs(), Proof: proof,
			Signer: fixture.signer, At: fixture.acceptedAt})
		if !errors.Is(err, ErrChannelEnrollmentInput) {
			t.Fatalf("forged frame PeerID error = %v", err)
		}
		assertEnrollmentTableCounts(t, fixture.ownerStore, map[string]int{
			"channel_members": 1, "enrollment_grant_uses": 0, "enrollment_receipts": 0,
		})
	})

	t.Run("cached init addresses bind transcript", func(t *testing.T) {
		fixture := newChannelEnrollmentFixture(t, "address-binding")
		transcript := fixture.transcript(t, 0x45, 0x46, fixture.head)
		proof := enrollmentTestProof(t, fixture.token, transcript)
		_, err := fixture.ownerStore.AcceptChannelEnrollment(context.Background(), AcceptChannelEnrollmentSpec{
			AuthenticatedPeerID: fixture.joiner.PeerID(), Transcript: transcript,
			AdvertisedMultiaddrs: []string{"/ip4/127.0.0.1/tcp/49999"}, Proof: proof,
			Signer: fixture.signer, At: fixture.acceptedAt})
		if !errors.Is(err, ErrChannelEnrollmentInput) {
			t.Fatalf("address transcript mismatch error = %v", err)
		}
		assertEnrollmentTableCounts(t, fixture.ownerStore, map[string]int{
			"channel_members": 1, "enrollment_grant_uses": 0, "enrollment_receipts": 0,
		})
	})

	t.Run("stale challenge", func(t *testing.T) {
		fixture := newChannelEnrollmentFixture(t, "stale-head")
		other := testkit.NewIdentity(t, "stale-head-other")
		otherRequest := stableEnrollmentRequest(t, fixture.channel.Channel().ID(), fixture.grantID,
			other)
		otherPrepare := PrepareChannelEnrollmentSpec{ChannelID: fixture.channel.Channel().ID(),
			GrantID: fixture.grantID, RequestID: otherRequest, AuthenticatedPeerID: other.PeerID(),
			JoinerOriginEpoch: other.OriginEpoch(), JoinerPublicKey: other.PublicKey(), At: fixture.acceptedAt}
		prepared, err := fixture.ownerStore.PrepareChannelEnrollment(context.Background(), otherPrepare)
		if err != nil || prepared.RosterHead != fixture.head {
			t.Fatalf("prepare second join = (%#v,%v)", prepared, err)
		}
		fixture.accept(t, fixture.transcript(t, 0x51, 0x52, fixture.head))
		otherTranscript := enrollmentTestTranscript(t, fixture.channel.Descriptor(), fixture.grantID,
			otherRequest, other, prepared.RosterHead, 0x53, 0x54)
		proof := enrollmentTestProof(t, fixture.token, otherTranscript)
		_, err = fixture.ownerStore.AcceptChannelEnrollment(context.Background(), AcceptChannelEnrollmentSpec{
			AuthenticatedPeerID: other.PeerID(), Transcript: otherTranscript,
			AdvertisedMultiaddrs: other.Multiaddrs(), Proof: proof, Signer: fixture.signer,
			At: fixture.acceptedAt.Add(time.Second)})
		if !errors.Is(err, ErrChannelEnrollmentStale) {
			t.Fatalf("stale challenge error = %v", err)
		}
		assertEnrollmentTableCounts(t, fixture.ownerStore, map[string]int{
			"channel_members": 2, "enrollment_grant_uses": 1, "enrollment_receipts": 1,
		})
	})

	t.Run("second signature failure", func(t *testing.T) {
		fixture := newChannelEnrollmentFixture(t, "signer-rollback")
		transcript := fixture.transcript(t, 0x61, 0x62, fixture.head)
		proof := enrollmentTestProof(t, fixture.token, transcript)
		_, err := fixture.ownerStore.AcceptChannelEnrollment(context.Background(), AcceptChannelEnrollmentSpec{
			AuthenticatedPeerID: fixture.joiner.PeerID(), Transcript: transcript,
			AdvertisedMultiaddrs: fixture.joiner.Multiaddrs(), Proof: proof,
			Signer: &failAfterSigner{delegate: fixture.signer, remaining: 1}, At: fixture.acceptedAt})
		if err == nil {
			t.Fatal("failing receipt signer was accepted")
		}
		assertEnrollmentTableCounts(t, fixture.ownerStore, map[string]int{
			"channel_members": 1, "enrollment_grant_uses": 0, "enrollment_receipts": 0,
		})
		var head uint64
		if err := fixture.ownerStore.db.QueryRow(`SELECT roster_head_revision FROM channels`).Scan(&head); err != nil || head != 1 {
			t.Fatalf("roster head after rollback = %d, %v", head, err)
		}
	})

	t.Run("final receipt insert failure", func(t *testing.T) {
		fixture := newChannelEnrollmentFixture(t, "receipt-rollback")
		if _, err := fixture.ownerStore.db.Exec(`CREATE TRIGGER test_reject_owner_receipt
			BEFORE INSERT ON enrollment_receipts
			BEGIN SELECT RAISE(ABORT, 'test reject owner receipt'); END`); err != nil {
			t.Fatal(err)
		}
		transcript := fixture.transcript(t, 0x63, 0x64, fixture.head)
		proof := enrollmentTestProof(t, fixture.token, transcript)
		_, err := fixture.ownerStore.AcceptChannelEnrollment(context.Background(), AcceptChannelEnrollmentSpec{
			AuthenticatedPeerID: fixture.joiner.PeerID(), Transcript: transcript,
			AdvertisedMultiaddrs: fixture.joiner.Multiaddrs(), Proof: proof,
			Signer: fixture.signer, At: fixture.acceptedAt})
		if err == nil {
			t.Fatal("receipt insert failure was accepted")
		}
		assertEnrollmentTableCounts(t, fixture.ownerStore, map[string]int{
			"channel_members": 1, "enrollment_grant_uses": 0, "enrollment_receipts": 0,
		})
		var head uint64
		var grantStatus string
		var used uint8
		if err := fixture.ownerStore.db.QueryRow(`SELECT roster_head_revision FROM channels`).Scan(&head); err != nil || head != 1 {
			t.Fatalf("roster head after receipt rollback = %d, %v", head, err)
		}
		if err := fixture.ownerStore.db.QueryRow(`SELECT status,used_uses FROM enrollment_grants
			WHERE grant_id=?`, fixture.grantID.String()).Scan(&grantStatus, &used); err != nil ||
			grantStatus != "open" || used != 0 {
			t.Fatalf("grant after receipt rollback = (%q,%d,%v)", grantStatus, used, err)
		}
	})
}

func TestAcceptChannelEnrollmentPreservesClosedAndExpiredReasonsAcrossChallengeRace(t *testing.T) {
	t.Parallel()
	t.Run("closed after challenge", func(t *testing.T) {
		fixture := newChannelEnrollmentFixture(t, "closed-after-challenge")
		transcript := fixture.transcript(t, 0x65, 0x66, fixture.head)
		closedAt := fixture.channel.Channel().CreatedAt().Add(5 * time.Second)
		if _, err := fixture.ownerStore.CloseChannelInvite(context.Background(),
			fixture.channel.Channel().ID(), fixture.grantID, closedAt); err != nil {
			t.Fatal(err)
		}
		proof := enrollmentTestProof(t, fixture.token, transcript)
		_, err := fixture.ownerStore.AcceptChannelEnrollment(context.Background(), AcceptChannelEnrollmentSpec{
			AuthenticatedPeerID: fixture.joiner.PeerID(), Transcript: transcript,
			AdvertisedMultiaddrs: fixture.joiner.Multiaddrs(), Proof: proof,
			Signer: fixture.signer, At: fixture.acceptedAt})
		if !errors.Is(err, ErrChannelEnrollmentTokenClosed) {
			t.Fatalf("closed-after-challenge error = %v", err)
		}
		_, err = fixture.ownerStore.AcceptChannelEnrollment(context.Background(), AcceptChannelEnrollmentSpec{
			AuthenticatedPeerID: fixture.joiner.PeerID(), Transcript: transcript,
			AdvertisedMultiaddrs: fixture.joiner.Multiaddrs(), Proof: model.Sum([]byte("forged closed proof")),
			Signer: fixture.signer, At: fixture.acceptedAt})
		if !errors.Is(err, ErrChannelEnrollmentTokenClosed) {
			t.Fatalf("closed policy did not precede proof error = %v", err)
		}
		assertEnrollmentTableCounts(t, fixture.ownerStore, map[string]int{
			"channel_members": 1, "enrollment_grant_uses": 0, "enrollment_receipts": 0,
		})
	})

	t.Run("expired after challenge", func(t *testing.T) {
		fixture := newChannelEnrollmentFixture(t, "expired-after-challenge")
		transcript := fixture.transcript(t, 0x67, 0x68, fixture.head)
		expiredAt := fixture.token.Payload().ExpiresAt()
		proof := enrollmentTestProof(t, fixture.token, transcript)
		_, err := fixture.ownerStore.AcceptChannelEnrollment(context.Background(), AcceptChannelEnrollmentSpec{
			AuthenticatedPeerID: fixture.joiner.PeerID(), Transcript: transcript,
			AdvertisedMultiaddrs: fixture.joiner.Multiaddrs(), Proof: proof,
			Signer: fixture.signer, At: expiredAt})
		if !errors.Is(err, ErrChannelEnrollmentTokenExpired) {
			t.Fatalf("expired-after-challenge error = %v", err)
		}
		_, err = fixture.ownerStore.AcceptChannelEnrollment(context.Background(), AcceptChannelEnrollmentSpec{
			AuthenticatedPeerID: fixture.joiner.PeerID(), Transcript: transcript,
			AdvertisedMultiaddrs: fixture.joiner.Multiaddrs(), Proof: model.Sum([]byte("forged expired proof")),
			Signer: fixture.signer, At: expiredAt})
		if !errors.Is(err, ErrChannelEnrollmentTokenExpired) {
			t.Fatalf("expiry policy did not precede proof error = %v", err)
		}
		if _, err := fixture.ownerStore.PrepareChannelEnrollment(context.Background(),
			fixture.prepareSpec(expiredAt)); !errors.Is(err, ErrChannelEnrollmentTokenExpired) {
			t.Fatalf("expired prepare error = %v", err)
		}
		assertEnrollmentTableCounts(t, fixture.ownerStore, map[string]int{
			"channel_members": 1, "enrollment_grant_uses": 0, "enrollment_receipts": 0,
		})
	})

	t.Run("single-use rotation exhausted", func(t *testing.T) {
		fixture := newChannelEnrollmentFixture(t, "exhausted-after-use")
		rotationAt := fixture.channel.Channel().CreatedAt().Add(time.Second)
		rotatedID, _ := model.ParseGrantID("grant-exhausted-after-use-rotation")
		rotatedToken := storeTestEnrollmentToken(t, fixture.channel.Descriptor(), fixture.channel.Owner(),
			rotatedID, "exhausted-rotation", rotationAt, 1)
		if _, err := fixture.ownerStore.RotateChannelInvite(context.Background(), RotateChannelInviteSpec{
			ChannelID: fixture.channel.Channel().ID(), Token: rotatedToken, At: rotationAt}); err != nil {
			t.Fatal(err)
		}
		fixture.grantID = rotatedID
		fixture.token = rotatedToken
		fixture.requestID = stableEnrollmentRequest(t, fixture.channel.Channel().ID(), rotatedID,
			fixture.joiner)
		prepared, err := fixture.ownerStore.PrepareChannelEnrollment(context.Background(),
			fixture.prepareSpec(fixture.acceptedAt))
		if err != nil {
			t.Fatal(err)
		}
		fixture.accept(t, fixture.transcript(t, 0x69, 0x6a, prepared.RosterHead))
		other := testkit.NewIdentity(t, "exhausted-after-use-other")
		otherRequest := stableEnrollmentRequest(t, fixture.channel.Channel().ID(), rotatedID, other)
		_, err = fixture.ownerStore.PrepareChannelEnrollment(context.Background(),
			PrepareChannelEnrollmentSpec{ChannelID: fixture.channel.Channel().ID(), GrantID: rotatedID,
				RequestID: otherRequest, AuthenticatedPeerID: other.PeerID(),
				JoinerOriginEpoch: other.OriginEpoch(), JoinerPublicKey: other.PublicKey(),
				At: fixture.acceptedAt.Add(time.Second)})
		if !errors.Is(err, ErrChannelEnrollmentTokenExhausted) {
			t.Fatalf("exhausted grant error = %v", err)
		}
		var status string
		var used uint8
		if err := fixture.ownerStore.db.QueryRow(`SELECT status,used_uses FROM enrollment_grants
			WHERE grant_id=?`, rotatedID.String()).Scan(&status, &used); err != nil ||
			status != "exhausted" || used != 1 {
			t.Fatalf("exhausted grant projection = (%q,%d,%v)", status, used, err)
		}
	})

	t.Run("owner closes after challenge", func(t *testing.T) {
		fixture := newChannelEnrollmentFixture(t, "owner-closed-after-challenge")
		accepted := fixture.accept(t, fixture.transcript(t, 0x6b, 0x6c, fixture.head))
		other := testkit.NewIdentity(t, "owner-closed-after-challenge-other")
		requestID := stableEnrollmentRequest(t, fixture.channel.Channel().ID(), fixture.grantID,
			other)
		challengeAt := fixture.acceptedAt.Add(time.Second)
		prepared, err := fixture.ownerStore.PrepareChannelEnrollment(context.Background(),
			PrepareChannelEnrollmentSpec{ChannelID: fixture.channel.Channel().ID(), GrantID: fixture.grantID,
				RequestID: requestID, AuthenticatedPeerID: other.PeerID(),
				JoinerOriginEpoch: other.OriginEpoch(), JoinerPublicKey: other.PublicKey(), At: challengeAt})
		if err != nil {
			t.Fatal(err)
		}
		transcript := enrollmentTestTranscript(t, fixture.channel.Descriptor(), fixture.grantID,
			requestID, other, prepared.RosterHead, 0x6d, 0x6e)
		closeAt := challengeAt.Add(time.Second)
		ownerLeft, closedRoster := appendRosterTerminal(t, fixture.channel.Descriptor(), fixture.signer,
			accepted.Roster, fixture.channel.Owner().PeerID(), model.MemberLeft, closeAt)
		merged, err := fixture.ownerStore.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
			ChannelID:                    fixture.channel.Channel().ID(),
			AuthenticatedTransportPeerID: fixture.channel.Owner().PeerID(),
			Records:                      []model.Member{ownerLeft}, At: closeAt,
		})
		if err != nil || merged.Status != ChannelRosterApplied ||
			merged.Roster.Head() != closedRoster.Head() || merged.Channel.Status() != model.ChannelClosed {
			t.Fatalf("owner-close roster merge = (%#v,%v)", merged, err)
		}
		proof := enrollmentTestProof(t, fixture.token, transcript)
		_, err = fixture.ownerStore.AcceptChannelEnrollment(context.Background(), AcceptChannelEnrollmentSpec{
			AuthenticatedPeerID: other.PeerID(), Transcript: transcript,
			AdvertisedMultiaddrs: other.Multiaddrs(), Proof: proof, Signer: fixture.signer,
			At: closeAt.Add(time.Second)})
		if !errors.Is(err, ErrChannelEnrollmentChannelClosed) {
			t.Fatalf("owner-close Accept error = %v", err)
		}
		_, err = fixture.ownerStore.PrepareChannelEnrollment(context.Background(),
			PrepareChannelEnrollmentSpec{ChannelID: fixture.channel.Channel().ID(), GrantID: fixture.grantID,
				RequestID: requestID, AuthenticatedPeerID: other.PeerID(),
				JoinerOriginEpoch: other.OriginEpoch(), JoinerPublicKey: other.PublicKey(),
				At: closeAt.Add(time.Second)})
		if !errors.Is(err, ErrChannelEnrollmentChannelClosed) {
			t.Fatalf("owner-close Prepare error = %v", err)
		}
		assertEnrollmentTableCounts(t, fixture.ownerStore, map[string]int{
			"channel_members": 3, "enrollment_grant_uses": 1, "enrollment_receipts": 1,
		})
	})
}

func TestInstallJoinedChannelSuffixFailureRollsBackHeadMembersBindingsAndAliases(t *testing.T) {
	t.Parallel()
	owner, joinerStore, baseSpec, initialRoster := newInstalledJoinedChannelFixture(t,
		"join-suffix-rollback", "suffix-rollback-team")
	baseAt := baseSpec.At
	var originalAlias string
	if err := joinerStore.db.QueryRow(`SELECT effective_alias FROM peer_bindings`).Scan(&originalAlias); err != nil {
		t.Fatal(err)
	}
	newPeer := testkit.NewIdentity(t, "join-suffix-rollback-new")
	ownerMember, _ := initialRoster.CurrentMember(owner.channel.Owner().PeerID())
	_, expanded := appendRosterMemberWithLabel(t, owner.channel.Descriptor(), owner.signer,
		initialRoster, newPeer, ownerMember.DisplayLabel())
	if _, err := joinerStore.db.Exec(`CREATE TRIGGER test_reject_suffix_binding
		BEFORE INSERT ON peer_bindings
		BEGIN SELECT RAISE(ABORT, 'test reject suffix binding'); END`); err != nil {
		t.Fatal(err)
	}
	replaySpec := baseSpec
	replaySpec.Members = expanded.Members()
	replaySpec.At = baseAt.Add(2 * time.Second)
	if _, err := joinerStore.InstallJoinedChannel(context.Background(), replaySpec); err == nil {
		t.Fatal("suffix binding failure was accepted")
	}
	assertEnrollmentTableCounts(t, joinerStore, map[string]int{
		"channels": 1, "channel_members": 2, "enrollment_receipts": 1,
		"publication_epochs": 1, "peer_bindings": 1,
	})
	var head uint64
	var status, topic, alias string
	if err := joinerStore.db.QueryRow(`SELECT roster_head_revision,status,topic_state FROM channels`).Scan(
		&head, &status, &topic); err != nil || head != 2 || status != "active" || topic != "not_joined" {
		t.Fatalf("Channel after suffix rollback = (%d,%q,%q,%v)", head, status, topic, err)
	}
	if err := joinerStore.db.QueryRow(`SELECT effective_alias FROM peer_bindings`).Scan(&alias); err != nil ||
		alias != originalAlias {
		t.Fatalf("binding alias after suffix rollback = (%q,%v), want %q", alias, err, originalAlias)
	}
}

func TestInstallJoinedChannelKeepsTerminalAliasAndDisambiguatesReplacementPeer(t *testing.T) {
	t.Parallel()
	owner, joinerStore, baseSpec, initialRoster := newInstalledJoinedChannelFixture(t,
		"join-alias-churn", "alias-churn-team")
	baseAt := baseSpec.At
	firstPeer := testkit.NewIdentity(t, "join-alias-churn-first")
	_, firstRoster := appendRosterMemberWithLabel(t, owner.channel.Descriptor(), owner.signer,
		initialRoster, firstPeer, "reviewer")
	firstSpec := baseSpec
	firstSpec.Members = firstRoster.Members()
	firstSpec.At = baseAt.Add(2 * time.Second)
	if result, err := joinerStore.InstallJoinedChannel(context.Background(), firstSpec); err != nil ||
		result.Roster.Head() != firstRoster.Head() {
		t.Fatalf("first alias suffix = (%#v,%v)", result, err)
	}
	var firstAlias string
	if err := joinerStore.db.QueryRow(`SELECT effective_alias FROM peer_bindings WHERE peer_id=?`,
		firstPeer.PeerID().String()).Scan(&firstAlias); err != nil || firstAlias != "reviewer" {
		t.Fatalf("first reviewer alias = (%q,%v)", firstAlias, err)
	}
	_, terminalRoster := appendRosterTerminal(t, owner.channel.Descriptor(), owner.signer,
		firstRoster, firstPeer.PeerID(), model.MemberRevoked, baseAt.Add(3*time.Second))
	secondPeer := testkit.NewIdentity(t, "join-alias-churn-second")
	_, replacementRoster := appendRosterMemberWithLabel(t, owner.channel.Descriptor(), owner.signer,
		terminalRoster, secondPeer, "reviewer")
	replacementSpec := baseSpec
	replacementSpec.Members = replacementRoster.Members()
	replacementSpec.At = baseAt.Add(5 * time.Second)
	if result, err := joinerStore.InstallJoinedChannel(context.Background(), replacementSpec); err != nil ||
		result.Roster.Head() != replacementRoster.Head() {
		t.Fatalf("replacement alias suffix = (%#v,%v)", result, err)
	}
	var firstState, retainedAlias, secondState, replacementAlias string
	if err := joinerStore.db.QueryRow(`SELECT state,effective_alias FROM peer_bindings WHERE peer_id=?`,
		firstPeer.PeerID().String()).Scan(&firstState, &retainedAlias); err != nil {
		t.Fatal(err)
	}
	if err := joinerStore.db.QueryRow(`SELECT state,effective_alias FROM peer_bindings WHERE peer_id=?`,
		secondPeer.PeerID().String()).Scan(&secondState, &replacementAlias); err != nil {
		t.Fatal(err)
	}
	if firstState != "revoked" || retainedAlias != firstAlias || secondState != "pending" ||
		replacementAlias == firstAlias || !strings.HasPrefix(replacementAlias, "reviewer~") {
		t.Fatalf("alias churn = first(%q,%q) second(%q,%q)", firstState, retainedAlias,
			secondState, replacementAlias)
	}
}

func newInstalledJoinedChannelFixture(t *testing.T, seed, localAlias string) (
	channelEnrollmentFixture, *Store, InstallJoinedChannelSpec, model.VerifiedRoster,
) {
	t.Helper()
	owner, joinerStore, spec := newJoinedChannelInstallFixture(t, seed, localAlias)
	result, err := joinerStore.InstallJoinedChannel(context.Background(), spec)
	if err != nil || !result.Installed {
		t.Fatalf("initial joined Channel install = (%#v,%v)", result, err)
	}
	return owner, joinerStore, spec, result.Roster
}

func TestInstallJoinedChannelAppliesTerminalReplaySuffixButFreshJoinStaysEmpty(t *testing.T) {
	t.Parallel()
	owner := newChannelEnrollmentFixture(t, "join-terminal-replay")
	transcript := owner.transcript(t, 0x75, 0x76, owner.head)
	accepted := owner.accept(t, transcript)
	initialAt := owner.acceptedAt.Add(time.Second)
	baseSpec := InstallJoinedChannelSpec{AuthenticatedOwnerPeerID: owner.channel.Owner().PeerID(),
		OwnerOutcome: ChannelEnrollmentAccepted, LocalAlias: "terminal-team",
		Descriptor: owner.channel.Descriptor(), Transcript: transcript,
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
	replaySpec.OwnerOutcome = ChannelEnrollmentMemberRevoked
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
