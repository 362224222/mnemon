package store

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	eventpkg "github.com/mnemon-dev/mnemon/harness/internal/event"
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
		"peer_bindings": 0,
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
	assertEnrollmentTableCounts(t, fixture.ownerStore, map[string]int{
		"channel_members": 2, "enrollment_grant_uses": 1, "enrollment_receipts": 1,
	})
}

func TestAcceptChannelEnrollmentUsesOneGrantForMultipleAuthenticatedPeers(t *testing.T) {
	t.Parallel()
	fixture := newChannelEnrollmentFixture(t, "multi-use")
	first := fixture.accept(t, fixture.transcript(t, 0x33, 0x34, fixture.head))
	secondPeer := testkit.NewIdentity(t, "multi-use-second-peer")
	secondRequest, _ := model.ParseEnrollmentRequestID("request-multi-use-second")
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
	installed, err := secondStore.InstallJoinedChannel(context.Background(), InstallJoinedChannelSpec{
		AuthenticatedOwnerPeerID: fixture.channel.Owner().PeerID(), LocalAlias: "multi-use-team",
		Descriptor: fixture.channel.Descriptor(), Transcript: transcript, Receipt: second.Receipt,
		Members: second.Roster.Members(), At: secondAt.Add(time.Second)})
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
	tx, err := fixture.ownerStore.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := insertChannelMember(context.Background(), tx, terminalMember); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE channels SET roster_head_revision=?,roster_head_hash=?,updated_at=?
		WHERE channel_id=?`, terminalRoster.Head().Revision(), terminalRoster.Head().Digest().Bytes(),
		storeTime(terminalAt), fixture.channel.Channel().ID().String()); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
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
		otherRequest, _ := model.ParseEnrollmentRequestID("request-stale-other")
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
		fixture.requestID, _ = model.ParseEnrollmentRequestID("request-exhausted-after-use-rotation")
		prepared, err := fixture.ownerStore.PrepareChannelEnrollment(context.Background(),
			fixture.prepareSpec(fixture.acceptedAt))
		if err != nil {
			t.Fatal(err)
		}
		fixture.accept(t, fixture.transcript(t, 0x69, 0x6a, prepared.RosterHead))
		other := testkit.NewIdentity(t, "exhausted-after-use-other")
		otherRequest, _ := model.ParseEnrollmentRequestID("request-exhausted-after-use-other")
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
		requestID, _ := model.ParseEnrollmentRequestID("request-owner-closed-after-challenge-other")
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
		tx, err := fixture.ownerStore.db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := insertChannelMember(context.Background(), tx, ownerLeft); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		if _, err := tx.Exec(`UPDATE channels SET roster_head_revision=?,roster_head_hash=?,status='closed',
			topic_state='left',updated_at=? WHERE channel_id=?`, closedRoster.Head().Revision(),
			closedRoster.Head().Digest().Bytes(), storeTime(closeAt), fixture.channel.Channel().ID().String()); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
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
	assertEnrollmentTableCounts(t, joinerStore, map[string]int{
		"channels": 0, "channel_members": 0, "enrollment_receipts": 0,
		"publication_epochs": 0, "peer_bindings": 0,
	})
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
		owner.requestID, _ = model.ParseEnrollmentRequestID("request-" + seed + "-shared")
		prepared, err := owner.ownerStore.PrepareChannelEnrollment(context.Background(),
			owner.prepareSpec(owner.acceptedAt))
		if err != nil {
			t.Fatalf("prepare Channel %d: %v", index+1, err)
		}
		transcript := owner.transcript(t, byte(0x80+index), byte(0x90+index), prepared.RosterHead)
		accepted := owner.accept(t, transcript)
		result, err := joinerStore.InstallJoinedChannel(context.Background(), InstallJoinedChannelSpec{
			AuthenticatedOwnerPeerID: owner.channel.Owner().PeerID(),
			LocalAlias:               "joined-" + string(rune('a'+index)),
			Descriptor:               owner.channel.Descriptor(), Transcript: transcript,
			Receipt: accepted.Receipt, Members: accepted.Roster.Members(), At: owner.acceptedAt.Add(time.Second)})
		if index < model.MaxChannelsPerNode && (err != nil || !result.Installed) {
			t.Fatalf("install Channel %d = (%#v,%v)", index+1, result, err)
		}
		if index == model.MaxChannelsPerNode {
			rejectedChannel = owner.channel.Channel().ID()
			if !errors.Is(err, ErrNodeChannelLimit) || result.Installed {
				t.Fatalf("ninth joined Channel = (%#v,%v)", result, err)
			}
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
	_, err := joinerStore.InstallJoinedChannel(context.Background(), InstallJoinedChannelSpec{
		AuthenticatedOwnerPeerID: owner.channel.Owner().PeerID(), LocalAlias: "rollback-team",
		Descriptor: owner.channel.Descriptor(), Transcript: transcript, Receipt: accepted.Receipt,
		Members: accepted.Roster.Members(), At: owner.acceptedAt.Add(time.Second)})
	if err == nil {
		t.Fatal("binding failure did not reject join install")
	}
	assertEnrollmentTableCounts(t, joinerStore, map[string]int{
		"channels": 0, "channel_members": 0, "enrollment_receipts": 0,
		"publication_epochs": 0, "peer_bindings": 0,
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
	owner := newChannelEnrollmentFixture(t, "join-suffix-rollback")
	transcript := owner.transcript(t, 0x77, 0x78, owner.head)
	accepted := owner.accept(t, transcript)
	joinerStore := openTestStore(t)
	insertChannelTestNode(t, joinerStore.db, owner.joiner, owner.channel.Channel().CreatedAt())
	baseAt := owner.acceptedAt.Add(time.Second)
	baseSpec := InstallJoinedChannelSpec{AuthenticatedOwnerPeerID: owner.channel.Owner().PeerID(),
		LocalAlias: "suffix-rollback-team", Descriptor: owner.channel.Descriptor(), Transcript: transcript,
		Receipt: accepted.Receipt, Members: accepted.Roster.Members(), At: baseAt}
	if result, err := joinerStore.InstallJoinedChannel(context.Background(), baseSpec); err != nil || !result.Installed {
		t.Fatalf("initial install = (%#v,%v)", result, err)
	}
	var originalAlias string
	if err := joinerStore.db.QueryRow(`SELECT effective_alias FROM peer_bindings`).Scan(&originalAlias); err != nil {
		t.Fatal(err)
	}
	newPeer := testkit.NewIdentity(t, "join-suffix-rollback-new")
	ownerMember, _ := accepted.Roster.CurrentMember(owner.channel.Owner().PeerID())
	_, expanded := appendRosterMemberWithLabel(t, owner.channel.Descriptor(), owner.signer,
		accepted.Roster, newPeer, ownerMember.DisplayLabel())
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
	owner := newChannelEnrollmentFixture(t, "join-alias-churn")
	transcript := owner.transcript(t, 0x7b, 0x7c, owner.head)
	accepted := owner.accept(t, transcript)
	joinerStore := openTestStore(t)
	insertChannelTestNode(t, joinerStore.db, owner.joiner, owner.channel.Channel().CreatedAt())
	baseAt := owner.acceptedAt.Add(time.Second)
	baseSpec := InstallJoinedChannelSpec{AuthenticatedOwnerPeerID: owner.channel.Owner().PeerID(),
		LocalAlias: "alias-churn-team", Descriptor: owner.channel.Descriptor(), Transcript: transcript,
		Receipt: accepted.Receipt, Members: accepted.Roster.Members(), At: baseAt}
	if result, err := joinerStore.InstallJoinedChannel(context.Background(), baseSpec); err != nil || !result.Installed {
		t.Fatalf("initial install = (%#v,%v)", result, err)
	}
	firstPeer := testkit.NewIdentity(t, "join-alias-churn-first")
	_, firstRoster := appendRosterMemberWithLabel(t, owner.channel.Descriptor(), owner.signer,
		accepted.Roster, firstPeer, "reviewer")
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

func TestDeriveEffectiveAliasesHandlesDuplicatesReservedAndSecondaryCollision(t *testing.T) {
	t.Parallel()
	channel := testkit.NewSignedChannel(t, "effective-aliases")
	roster := channel.Roster()
	ownerSigner := enrollmentTestSigner(t, channel.Owner())
	firstIdentity := testkit.NewIdentity(t, "duplicate-one")
	first, roster := appendRosterMemberWithLabel(t, channel.Descriptor(), ownerSigner, roster,
		firstIdentity, "duplicate")
	secondIdentity := testkit.NewIdentity(t, "duplicate-two")
	_, roster = appendRosterMemberWithLabel(t, channel.Descriptor(), ownerSigner, roster,
		secondIdentity, "duplicate")
	reservedIdentity := testkit.NewIdentity(t, "reserved-team")
	_, roster = appendRosterMemberWithLabel(t, channel.Descriptor(), ownerSigner, roster,
		reservedIdentity, "team")
	aliases, _, err := deriveEffectiveAliases(channel.Owner().PeerID(), roster)
	if err != nil {
		t.Fatal(err)
	}
	if aliases[firstIdentity.PeerID()] == first.DisplayLabel() ||
		aliases[secondIdentity.PeerID()] == first.DisplayLabel() ||
		aliases[firstIdentity.PeerID()] == aliases[secondIdentity.PeerID()] ||
		aliases[reservedIdentity.PeerID()] == "team" {
		t.Fatalf("derived aliases = %#v", aliases)
	}
}

func TestDeriveEffectiveAliasesSanitizesDisplayWhitespaceBeforeCollisionRules(t *testing.T) {
	t.Parallel()
	channel := testkit.NewSignedChannel(t, "effective-alias-whitespace")
	roster := channel.Roster()
	signer := enrollmentTestSigner(t, channel.Owner())
	first := testkit.NewIdentity(t, "alias-space-one")
	second := testkit.NewIdentity(t, "alias-space-two")
	_, roster = appendRosterMemberWithLabel(t, channel.Descriptor(), signer, roster, first, "Alice Smith")
	_, roster = appendRosterMemberWithLabel(t, channel.Descriptor(), signer, roster, second, "Alice\u00a0Smith")
	aliases, _, err := deriveEffectiveAliases(channel.Owner().PeerID(), roster)
	if err != nil {
		t.Fatal(err)
	}
	if aliases[first.PeerID()] == "Alice-Smith" || aliases[second.PeerID()] == "Alice-Smith" ||
		aliases[first.PeerID()] == aliases[second.PeerID()] ||
		strings.ContainsAny(aliases[first.PeerID()]+aliases[second.PeerID()], " \u00a0") {
		t.Fatalf("whitespace aliases = %#v", aliases)
	}
}

type channelEnrollmentFixture struct {
	ownerStore *Store
	channel    *testkit.SignedChannel
	joiner     testkit.Identity
	grantID    model.GrantID
	requestID  model.EnrollmentRequestID
	token      model.EnrollmentToken
	signer     ChannelAuthoritySigner
	head       model.RecordHead
	acceptedAt time.Time
}

func newChannelEnrollmentFixture(t *testing.T, seed string) channelEnrollmentFixture {
	t.Helper()
	st := openTestStore(t)
	channel := testkit.NewSignedChannel(t, "enrollment-"+seed)
	insertChannelTestNode(t, st.db, channel.Owner(), channel.Channel().CreatedAt())
	grantID, _ := model.ParseGrantID("grant-" + seed)
	token := storeTestEnrollmentToken(t, channel.Descriptor(), channel.Owner(), grantID, seed,
		channel.Channel().CreatedAt(), model.MaxMembersPerChannel-1)
	if _, err := st.CreateChannel(context.Background(), CreateChannelSpec{Channel: channel.Channel(),
		Genesis: channel.OwnerMember().Member(), Token: token}); err != nil {
		t.Fatalf("CreateChannel() error = %v", err)
	}
	requestID, _ := model.ParseEnrollmentRequestID("request-" + seed)
	joiner := testkit.NewIdentity(t, "joiner-"+seed)
	signer := enrollmentTestSigner(t, channel.Owner())
	acceptedAt := channel.Channel().CreatedAt().Add(10 * time.Second)
	fixture := channelEnrollmentFixture{ownerStore: st, channel: channel, joiner: joiner,
		grantID: grantID, requestID: requestID, token: token, signer: signer,
		head: channel.Roster().Head(), acceptedAt: acceptedAt}
	prepared, err := st.PrepareChannelEnrollment(context.Background(), fixture.prepareSpec(acceptedAt))
	if err != nil || prepared.Status != ChannelEnrollmentAccepted || prepared.RosterHead != fixture.head {
		t.Fatalf("PrepareChannelEnrollment() = (%#v, %v)", prepared, err)
	}
	return fixture
}

func (fixture channelEnrollmentFixture) prepareSpec(at time.Time) PrepareChannelEnrollmentSpec {
	return PrepareChannelEnrollmentSpec{ChannelID: fixture.channel.Channel().ID(), GrantID: fixture.grantID,
		RequestID: fixture.requestID, AuthenticatedPeerID: fixture.joiner.PeerID(),
		JoinerOriginEpoch: fixture.joiner.OriginEpoch(), JoinerPublicKey: fixture.joiner.PublicKey(), At: at}
}

func (fixture channelEnrollmentFixture) transcript(t testing.TB, ownerNonce, joinerNonce byte,
	head model.RecordHead,
) model.EnrollmentTranscript {
	t.Helper()
	return enrollmentTestTranscript(t, fixture.channel.Descriptor(), fixture.grantID,
		fixture.requestID, fixture.joiner, head, ownerNonce, joinerNonce)
}

func (fixture channelEnrollmentFixture) accept(t testing.TB,
	transcript model.EnrollmentTranscript,
) AcceptChannelEnrollmentResult {
	t.Helper()
	return fixture.acceptAt(t, transcript, fixture.acceptedAt)
}

func (fixture channelEnrollmentFixture) acceptAt(t testing.TB, transcript model.EnrollmentTranscript,
	at time.Time,
) AcceptChannelEnrollmentResult {
	t.Helper()
	proof := enrollmentTestProof(t, fixture.token, transcript)
	result, err := fixture.ownerStore.AcceptChannelEnrollment(context.Background(), AcceptChannelEnrollmentSpec{
		AuthenticatedPeerID: fixture.joiner.PeerID(), Transcript: transcript,
		AdvertisedMultiaddrs: fixture.joiner.Multiaddrs(), Proof: proof, Signer: fixture.signer, At: at})
	if err != nil {
		t.Fatalf("AcceptChannelEnrollment() error = %v", err)
	}
	return result
}

func enrollmentTestTranscript(t testing.TB, descriptor model.SignedChannelDescriptor,
	grantID model.GrantID, requestID model.EnrollmentRequestID, joiner testkit.Identity,
	head model.RecordHead, ownerNonce, joinerNonce byte,
) model.EnrollmentTranscript {
	t.Helper()
	transcript, err := model.NewEnrollmentTranscript(model.EnrollmentTranscriptSpec{
		ChannelID: descriptor.Descriptor().ID(), GrantID: grantID, RequestID: requestID,
		OwnerPeerID: descriptor.Descriptor().OwnerPeerID(), JoinerPeerID: joiner.PeerID(),
		OwnerNonce:      bytes.Repeat([]byte{ownerNonce}, model.EnrollmentNonceBytes),
		JoinerNonce:     bytes.Repeat([]byte{joinerNonce}, model.EnrollmentNonceBytes),
		SelectedVersion: model.EnrollmentProtocolMinVersion, Limits: model.DefaultMemberLimits(),
		JoinerOriginEpoch: joiner.OriginEpoch(), JoinerDisplayLabel: joiner.DisplayName(),
		JoinerPublicKey: joiner.PublicKey(), AdvertisedMultiaddrs: joiner.Multiaddrs(), RosterHead: head})
	if err != nil {
		t.Fatal(err)
	}
	return transcript
}

func enrollmentTestProof(t testing.TB, token model.EnrollmentToken,
	transcript model.EnrollmentTranscript,
) model.Digest {
	t.Helper()
	verifier, err := model.VerifierForEnrollment(token.Payload().BearerSecret(), transcript.ChannelID(),
		transcript.GrantID())
	if err != nil {
		t.Fatal(err)
	}
	proof, err := model.ComputeEnrollmentProof(verifier, transcript)
	if err != nil {
		t.Fatal(err)
	}
	return proof
}

func enrollmentTestSigner(t testing.TB, identity testkit.Identity) ChannelAuthoritySigner {
	t.Helper()
	privateKey, err := identity.Libp2pPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := privateKey.Raw()
	if err != nil {
		t.Fatal(err)
	}
	signer, err := eventpkg.NewEd25519Signer(ed25519.PrivateKey(raw))
	if err != nil {
		t.Fatal(err)
	}
	return signer
}

type failAfterSigner struct {
	delegate  ChannelAuthoritySigner
	remaining int
}

func (signer *failAfterSigner) Sign(ctx context.Context, message []byte) ([]byte, error) {
	if signer.remaining == 0 {
		return nil, errors.New("injected signer failure")
	}
	signer.remaining--
	return signer.delegate.Sign(ctx, message)
}

func assertEnrollmentTableCounts(t testing.TB, st *Store, wants map[string]int) {
	t.Helper()
	for table, want := range wants {
		var got int
		if err := st.db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&got); err != nil || got != want {
			t.Fatalf("%s count = %d, err=%v, want %d", table, got, err, want)
		}
	}
}

// appendRosterMemberWithLabel creates a test-only label variant while
// preserving real owner signatures and complete roster verification.
func appendRosterMemberWithLabel(t testing.TB, descriptor model.SignedChannelDescriptor,
	ownerSigner ChannelAuthoritySigner, roster model.VerifiedRoster, identity testkit.Identity, label string,
) (model.Member, model.VerifiedRoster) {
	t.Helper()
	previous := roster.Head().Digest()
	revision := roster.Head().Revision() + 1
	members := roster.Members()
	record, err := model.NewMemberRecord(model.MemberRecordSpec{ChannelID: descriptor.Descriptor().ID(),
		DescriptorDigest: descriptor.Descriptor().Digest(), Revision: revision,
		PreviousDigest: &previous, PeerID: identity.PeerID(), OriginEpoch: identity.OriginEpoch(),
		DisplayLabel: label, PublicKey: identity.PublicKey(), Multiaddrs: identity.Multiaddrs(),
		Protocols: model.RequiredMemberProtocols(), Limits: model.DefaultMemberLimits(),
		Status: model.MemberActive, CreatedAt: members[len(members)-1].CreatedAt().Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	message, _ := model.MemberRecordSigningMessage(record.ChannelID(), record.Digest())
	signature, err := ownerSigner.Sign(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
	member, err := model.AttachMemberSignature(record, signature)
	if err != nil {
		t.Fatal(err)
	}
	members = append(members, member)
	verified, err := model.NewVerifiedRoster(descriptor, members)
	if err != nil {
		t.Fatal(err)
	}
	return member, verified
}

func appendRosterTerminal(t testing.TB, descriptor model.SignedChannelDescriptor,
	ownerSigner ChannelAuthoritySigner, roster model.VerifiedRoster, peerID model.PeerID,
	status model.MemberStatus, at time.Time,
) (model.Member, model.VerifiedRoster) {
	t.Helper()
	current, ok := roster.CurrentMember(peerID)
	if !ok || current.Status() != model.MemberActive || !status.Terminal() {
		t.Fatal("terminal test member lacks active authority")
	}
	previous := roster.Head().Digest()
	record, err := model.NewMemberRecord(model.MemberRecordSpec{ChannelID: descriptor.Descriptor().ID(),
		DescriptorDigest: descriptor.Descriptor().Digest(), Revision: roster.Head().Revision() + 1,
		PreviousDigest: &previous, PeerID: current.PeerID(), OriginEpoch: current.OriginEpoch(),
		DisplayLabel: current.DisplayLabel(), PublicKey: current.PublicKey(), Multiaddrs: current.Multiaddrs(),
		Protocols: current.Protocols(), Limits: current.Limits(), Status: status, CreatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	message, _ := model.MemberRecordSigningMessage(record.ChannelID(), record.Digest())
	signature, err := ownerSigner.Sign(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
	member, err := model.AttachMemberSignature(record, signature)
	if err != nil {
		t.Fatal(err)
	}
	members := roster.Members()
	members = append(members, member)
	verified, err := model.NewVerifiedRoster(descriptor, members)
	if err != nil {
		t.Fatal(err)
	}
	return member, verified
}
