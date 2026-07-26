package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type channelAcceptanceCallbackSigner struct {
	delegate ChannelAuthoritySigner
	callback func(context.Context, int) error
	calls    int
}

func (signer *channelAcceptanceCallbackSigner) Sign(ctx context.Context,
	message []byte,
) ([]byte, error) {
	signer.calls++
	if signer.callback != nil {
		if err := signer.callback(ctx, signer.calls); err != nil {
			return nil, err
		}
	}
	return signer.delegate.Sign(ctx, message)
}

func TestChannelAcceptanceSigningCallbacksCanReadStore(t *testing.T) {
	t.Run("enrollment", testEnrollmentSigningCallbackCanReadStore)
	t.Run("leave", testLeaveSigningCallbackCanReadStore)
}

func testEnrollmentSigningCallbackCanReadStore(t *testing.T) {
	fixture := newChannelEnrollmentFixture(t, "sign-callback-read")
	transcript := fixture.transcript(t, 0x81, 0x82, fixture.head)
	signer := storeReadingChannelSigner(fixture.ownerStore, fixture.signer)
	result, err := fixture.ownerStore.AcceptChannelEnrollment(context.Background(),
		AcceptChannelEnrollmentSpec{
			AuthenticatedPeerID: fixture.joiner.PeerID(), Transcript: transcript,
			AdvertisedMultiaddrs: fixture.joiner.Multiaddrs(),
			Proof:                enrollmentTestProof(t, fixture.token, transcript),
			Signer:               signer, At: fixture.acceptedAt,
		})
	if err != nil || result.Status != ChannelEnrollmentAccepted || signer.calls != 2 {
		t.Fatalf("AcceptChannelEnrollment() = (%#v,%v), signer calls %d", result, err, signer.calls)
	}
}

func testLeaveSigningCallbackCanReadStore(t *testing.T) {
	fixture, accepted := acceptedOwnerLeaveFixture(t, "leave-sign-callback-read")
	request := signedOwnerStoreLeaveRequest(t, fixture, accepted.Roster,
		fixture.acceptedAt.Add(500*time.Millisecond))
	signer := storeReadingChannelSigner(fixture.ownerStore, fixture.signer)
	result, err := fixture.ownerStore.AcceptChannelLeave(context.Background(), AcceptChannelLeaveSpec{
		AuthenticatedPeerID: fixture.joiner.PeerID(), Request: request,
		Signer: signer, At: fixture.acceptedAt.Add(time.Second),
	})
	if err != nil || result.Terminal.Status() != model.MemberLeft || signer.calls != 2 {
		t.Fatalf("AcceptChannelLeave() = (%#v,%v), signer calls %d", result, err, signer.calls)
	}
}

func storeReadingChannelSigner(st *Store,
	delegate ChannelAuthoritySigner,
) *channelAcceptanceCallbackSigner {
	return &channelAcceptanceCallbackSigner{delegate: delegate,
		callback: func(ctx context.Context, _ int) error {
			readCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()
			if _, err := st.ReadChannelMeshAuthority(readCtx); err != nil {
				return fmt.Errorf("re-enter Store read from signer: %w", err)
			}
			return nil
		}}
}

func TestChannelAcceptanceRejectsAuthorityChangeDuringSigning(t *testing.T) {
	t.Run("enrollment", testEnrollmentRejectsSigningAuthorityChange)
	t.Run("leave", testLeaveRejectsSigningAuthorityChange)
}

func TestChannelAcceptanceReplaysCommitWonDuringSigning(t *testing.T) {
	t.Run("enrollment", testEnrollmentReplaysCommitWonDuringSigning)
	t.Run("leave", testLeaveReplaysCommitWonDuringSigning)
}

func testEnrollmentReplaysCommitWonDuringSigning(t *testing.T) {
	fixture := newChannelEnrollmentFixture(t, "sign-response-loss")
	transcript := fixture.transcript(t, 0x85, 0x86, fixture.head)
	spec := AcceptChannelEnrollmentSpec{
		AuthenticatedPeerID: fixture.joiner.PeerID(), Transcript: transcript,
		AdvertisedMultiaddrs: fixture.joiner.Multiaddrs(),
		Proof:                enrollmentTestProof(t, fixture.token, transcript),
		Signer:               fixture.signer, At: fixture.acceptedAt,
	}
	var committed AcceptChannelEnrollmentResult
	var committedErr error
	spec.Signer = &channelAcceptanceCallbackSigner{delegate: fixture.signer,
		callback: func(ctx context.Context, call int) error {
			if call == 1 {
				inner := spec
				inner.Signer = fixture.signer
				committed, committedErr = fixture.ownerStore.AcceptChannelEnrollment(ctx, inner)
			}
			return committedErr
		}}
	replayed, err := fixture.ownerStore.AcceptChannelEnrollment(context.Background(), spec)
	if err != nil || committed.Status != ChannelEnrollmentAccepted ||
		replayed.Status != ChannelEnrollmentReplayed ||
		replayed.Receipt.WireJSON().String() != committed.Receipt.WireJSON().String() {
		t.Fatalf("signing-gap enrollment replay = committed %#v/%v replay %#v/%v",
			committed, committedErr, replayed, err)
	}
}

func testLeaveReplaysCommitWonDuringSigning(t *testing.T) {
	fixture, accepted := acceptedOwnerLeaveFixture(t, "leave-sign-response-loss")
	request := signedOwnerStoreLeaveRequest(t, fixture, accepted.Roster,
		fixture.acceptedAt.Add(500*time.Millisecond))
	spec := AcceptChannelLeaveSpec{AuthenticatedPeerID: fixture.joiner.PeerID(),
		Request: request, Signer: fixture.signer, At: fixture.acceptedAt.Add(time.Second)}
	var committed AcceptChannelLeaveResult
	var committedErr error
	spec.Signer = &channelAcceptanceCallbackSigner{delegate: fixture.signer,
		callback: func(ctx context.Context, call int) error {
			if call == 1 {
				inner := spec
				inner.Signer = fixture.signer
				committed, committedErr = fixture.ownerStore.AcceptChannelLeave(ctx, inner)
			}
			return committedErr
		}}
	replayed, err := fixture.ownerStore.AcceptChannelLeave(context.Background(), spec)
	if err != nil || committed.Replay || !replayed.Replay ||
		replayed.Receipt.WireJSON().String() != committed.Receipt.WireJSON().String() {
		t.Fatalf("signing-gap leave replay = committed %#v/%v replay %#v/%v",
			committed, committedErr, replayed, err)
	}
}

func testEnrollmentRejectsSigningAuthorityChange(t *testing.T) {
	fixture := newChannelEnrollmentFixture(t, "sign-authority-race")
	transcript := fixture.transcript(t, 0x83, 0x84, fixture.head)
	signer := &channelAcceptanceCallbackSigner{delegate: fixture.signer,
		callback: advanceTopicOnFirstSignature(fixture.ownerStore, fixture.channel.Channel(),
			fixture.channel.Channel().CreatedAt().Add(time.Second))}
	_, err := fixture.ownerStore.AcceptChannelEnrollment(context.Background(),
		AcceptChannelEnrollmentSpec{
			AuthenticatedPeerID: fixture.joiner.PeerID(), Transcript: transcript,
			AdvertisedMultiaddrs: fixture.joiner.Multiaddrs(),
			Proof:                enrollmentTestProof(t, fixture.token, transcript),
			Signer:               signer, At: fixture.acceptedAt,
		})
	if !errors.Is(err, ErrChannelEnrollmentStale) {
		t.Fatalf("authority-raced enrollment error = %v", err)
	}
	assertEnrollmentTableCounts(t, fixture.ownerStore, map[string]int{
		"channel_members": 1, "enrollment_grant_uses": 0, "enrollment_receipts": 0,
	})
}

func testLeaveRejectsSigningAuthorityChange(t *testing.T) {
	fixture, accepted := acceptedOwnerLeaveFixture(t, "leave-sign-authority-race")
	request := signedOwnerStoreLeaveRequest(t, fixture, accepted.Roster,
		fixture.acceptedAt.Add(500*time.Millisecond))
	revokedAt := fixture.acceptedAt.Add(time.Second)
	revoked, revokedRoster := appendRosterTerminal(t, fixture.channel.Descriptor(), fixture.signer,
		accepted.Roster, fixture.joiner.PeerID(), model.MemberRevoked, revokedAt)
	signer := &channelAcceptanceCallbackSigner{delegate: fixture.signer,
		callback: mergeRosterOnFirstSignature(fixture.ownerStore, fixture.channel.Owner().PeerID(),
			fixture.channel.Channel().ID(), revoked)}
	_, err := fixture.ownerStore.AcceptChannelLeave(context.Background(), AcceptChannelLeaveSpec{
		AuthenticatedPeerID: fixture.joiner.PeerID(), Request: request,
		Signer: signer, At: revokedAt.Add(time.Second),
	})
	if !errors.Is(err, ErrChannelLeaveConflict) {
		t.Fatalf("authority-raced leave error = %v", err)
	}
	assertOwnerLeaveRaceHasNoPartialWrite(t, fixture.ownerStore, fixture, revokedRoster)
	replay, err := fixture.ownerStore.AcceptChannelLeave(context.Background(), AcceptChannelLeaveSpec{
		AuthenticatedPeerID: fixture.joiner.PeerID(), Request: request,
		Signer: fixture.signer, At: revokedAt.Add(2 * time.Second),
	})
	if err != nil || replay.Replay || replay.Terminal.Head() != revoked.Head() ||
		replay.Roster.Head() != revokedRoster.Head() {
		t.Fatalf("terminal-precedence retry = (%#v,%v)", replay, err)
	}
}

func mergeRosterOnFirstSignature(st *Store, owner model.PeerID, channelID model.ChannelID,
	member model.Member,
) func(context.Context, int) error {
	return func(ctx context.Context, call int) error {
		if call != 1 {
			return nil
		}
		result, err := st.MergeChannelRoster(ctx, MergeChannelRosterSpec{
			ChannelID: channelID, AuthenticatedTransportPeerID: owner,
			Records: []model.Member{member}, At: member.CreatedAt(),
		})
		if err != nil || result.Status != ChannelRosterApplied {
			return fmt.Errorf("merge signing-race roster = (%s,%v)", result.Status, err)
		}
		return nil
	}
}

func advanceTopicOnFirstSignature(st *Store, channel model.Channel,
	at time.Time,
) func(context.Context, int) error {
	return func(ctx context.Context, call int) error {
		if call != 1 {
			return nil
		}
		result, err := st.CompareAndSetChannelTopicState(ctx, CompareAndSetChannelTopicStateSpec{
			ChannelID: channel.ID(), ExpectedStatus: channel.Status(),
			ExpectedRosterHead: channel.RosterHead(), ExpectedTopicState: channel.TopicState(),
			TopicState: model.TopicJoining, At: at,
		})
		if err != nil || !result.Changed {
			return fmt.Errorf("advance signing-race topic = (%#v,%v)", result, err)
		}
		return nil
	}
}

func assertOwnerLeaveRaceHasNoPartialWrite(t *testing.T, st *Store,
	fixture channelEnrollmentFixture, expected model.VerifiedRoster,
) {
	t.Helper()
	var requests, leftRecords int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM channel_leave_requests
		WHERE channel_id=?`, fixture.channel.Channel().ID().String()).Scan(&requests); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM channel_members
		WHERE channel_id=? AND member_peer_id=? AND status='left'`,
		fixture.channel.Channel().ID().String(), fixture.joiner.PeerID().String()).Scan(&leftRecords); err != nil {
		t.Fatal(err)
	}
	authority, err := st.ReadChannelMeshAuthority(context.Background())
	if err != nil || requests != 0 || leftRecords != 0 || len(authority.Channels()) != 1 ||
		authority.Channels()[0].Roster().Head() != expected.Head() {
		t.Fatalf("leave race partial state = requests %d left %d authority %#v err %v",
			requests, leftRecords, authority, err)
	}
}
