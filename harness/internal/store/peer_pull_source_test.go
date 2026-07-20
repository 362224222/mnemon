package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestPeerPullSourceReadsContinuousExactPagesAndReplaysResponseLoss(t *testing.T) {
	fixture, publications := newPeerPullSourceFixture(t, 5)
	baseAt := peerPullTestAt(fixture)
	first := readPeerPullPage(t, fixture, 0, 2, baseAt)
	assertPeerPullPublications(t, first, publications[:2], 0, 2)

	requestAt := baseAt.Add(time.Second)
	second := readPeerPullPage(t, fixture, first.ScannedChannelSequence, 2, requestAt)
	assertPeerPullPublications(t, second, publications[2:4], 2, 4)
	if !second.AcknowledgementAdvanced || second.AcknowledgedSequence != 2 {
		t.Fatalf("second page acknowledgement = %#v", second)
	}
	assertPeerPullDeliveryStates(t, fixture, 2, 3)
	var ackUpdated string
	if err := fixture.store.db.QueryRow(`SELECT updated_at FROM peer_pull_acks WHERE channel_id=?
		AND target_peer_id=?`, fixture.channel.String(), fixture.reviewers[0].String()).Scan(&ackUpdated); err != nil {
		t.Fatal(err)
	}

	// The response may be lost after the origin commits the request cursor.
	// Replaying the same authenticated request returns identical signed bytes
	// without changing acknowledgement evidence.
	replay := readPeerPullPage(t, fixture, 2, 2, requestAt.Add(time.Hour))
	assertPeerPullPublications(t, replay, publications[2:4], 2, 4)
	if replay.AcknowledgementAdvanced {
		t.Fatal("exact Pull request replay advanced acknowledgement again")
	}
	var replayUpdated string
	if err := fixture.store.db.QueryRow(`SELECT updated_at FROM peer_pull_acks WHERE channel_id=?
		AND target_peer_id=?`, fixture.channel.String(), fixture.reviewers[0].String()).Scan(&replayUpdated); err != nil ||
		replayUpdated != ackUpdated {
		t.Fatalf("Pull replay updated_at = (%q,%v), want %q", replayUpdated, err, ackUpdated)
	}

	last := readPeerPullPage(t, fixture, 4, 2, requestAt.Add(2*time.Hour))
	assertPeerPullPublications(t, last, publications[4:], 4, 5)
	empty := readPeerPullPage(t, fixture, 5, 2, requestAt.Add(3*time.Hour))
	if len(empty.Publications) != 0 || empty.ScannedChannelSequence != 5 || empty.SourceHead != 5 {
		t.Fatalf("empty head page = %#v", empty)
	}
}

func TestPeerPullSourceCursorAckIsMonotonicIdempotentAndSettlesDeliveries(t *testing.T) {
	fixture, _ := newPeerPullSourceFixture(t, 4)
	at := peerPullTestAt(fixture)
	first, err := fixture.store.CommitPeerPullCursorAck(context.Background(), CommitPeerPullCursorAckSpec{
		AuthenticatedPeerID: fixture.reviewers[0], ChannelID: fixture.channel,
		OriginEpoch: fixture.node.OriginEpoch(), ContiguousChannelSequence: 3, At: at})
	if err != nil || !first.Advanced || first.Replayed || first.AcknowledgedSequence != 3 {
		t.Fatalf("first CursorAck = (%#v,%v)", first, err)
	}
	assertPeerPullDeliveryStates(t, fixture, 3, 1)
	var originalUpdated string
	if err := fixture.store.db.QueryRow(`SELECT updated_at FROM peer_pull_acks WHERE channel_id=?
		AND target_peer_id=?`, fixture.channel.String(), fixture.reviewers[0].String()).Scan(&originalUpdated); err != nil {
		t.Fatal(err)
	}
	replay, err := fixture.store.CommitPeerPullCursorAck(context.Background(), CommitPeerPullCursorAckSpec{
		AuthenticatedPeerID: fixture.reviewers[0], ChannelID: fixture.channel,
		OriginEpoch: fixture.node.OriginEpoch(), ContiguousChannelSequence: 3, At: at.Add(time.Hour)})
	if err != nil || replay.Advanced || !replay.Replayed || replay.AcknowledgedSequence != 3 {
		t.Fatalf("replayed CursorAck = (%#v,%v)", replay, err)
	}
	var replayUpdated string
	if err := fixture.store.db.QueryRow(`SELECT updated_at FROM peer_pull_acks WHERE channel_id=?
		AND target_peer_id=?`, fixture.channel.String(), fixture.reviewers[0].String()).Scan(&replayUpdated); err != nil ||
		replayUpdated != originalUpdated {
		t.Fatalf("CursorAck replay updated_at = (%q,%v), want %q", replayUpdated, err, originalUpdated)
	}

	for name, sequence := range map[string]uint64{"backward": 2, "past head": 5} {
		t.Run(name, func(t *testing.T) {
			_, err := fixture.store.CommitPeerPullCursorAck(context.Background(), CommitPeerPullCursorAckSpec{
				AuthenticatedPeerID: fixture.reviewers[0], ChannelID: fixture.channel,
				OriginEpoch: fixture.node.OriginEpoch(), ContiguousChannelSequence: sequence,
				At: at.Add(2 * time.Hour)})
			if !errors.Is(err, ErrPeerPullCursor) {
				t.Fatalf("cursor %d error = %v", sequence, err)
			}
		})
	}
}

func TestPeerPullSourceEnforcesBaselineFloorAndRequestBounds(t *testing.T) {
	t.Run("nonzero binding baseline", func(t *testing.T) {
		fixture := newChannelBaselineFixture(t, "peer-pull-nonzero", model.TopicJoined)
		fixture.advanceLocalHead(t, 3, fixture.at)
		reserved, err := fixture.store.ReserveOutboundChannelBaseline(context.Background(),
			fixture.reserveSpec(fixture.at.Add(time.Second)))
		if err != nil || reserved.Baseline.BaselineChannelSequence != 3 {
			t.Fatalf("reserve nonzero baseline = (%#v,%v)", reserved, err)
		}
		if _, err := fixture.store.InstallInboundChannelBaseline(context.Background(),
			InstallInboundChannelBaselineSpec{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
				Baseline: fixture.remoteBaseline(0), At: fixture.at.Add(time.Second)}); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.ConfirmOutboundChannelBaseline(context.Background(),
			fixture.confirmSpec(ChannelDataBaselineAck(reserved.Baseline),
				fixture.at.Add(2*time.Second))); err != nil {
			t.Fatal(err)
		}
		_, err = fixture.store.ReadPeerPullPage(context.Background(), ReadPeerPullPageSpec{
			AuthenticatedPeerID:  fixture.remote.Identity().PeerID(),
			ChannelID:            fixture.channel.Channel().ID(),
			OriginEpoch:          fixture.channel.Owner().OriginEpoch(),
			AfterChannelSequence: 2, Limit: 1, At: fixture.at.Add(3 * time.Second)})
		if !errors.Is(err, ErrPeerPullCursor) {
			t.Fatalf("request below nonzero baseline error = %v", err)
		}
		page, err := fixture.store.ReadPeerPullPage(context.Background(), ReadPeerPullPageSpec{
			AuthenticatedPeerID:  fixture.remote.Identity().PeerID(),
			ChannelID:            fixture.channel.Channel().ID(),
			OriginEpoch:          fixture.channel.Owner().OriginEpoch(),
			AfterChannelSequence: 3, Limit: 1, At: fixture.at.Add(3 * time.Second)})
		if err != nil || len(page.Publications) != 0 || page.ScannedChannelSequence != 3 ||
			page.AcknowledgedSequence != 3 || page.AcknowledgementAdvanced {
			t.Fatalf("baseline head page = (%#v,%v)", page, err)
		}
	})

	t.Run("confirmed baseline and acknowledged lower bound", func(t *testing.T) {
		fixture, _ := newPeerPullSourceFixture(t, 3)
		at := peerPullTestAt(fixture)
		if _, err := fixture.store.CommitPeerPullCursorAck(context.Background(), CommitPeerPullCursorAckSpec{
			AuthenticatedPeerID: fixture.reviewers[0], ChannelID: fixture.channel,
			OriginEpoch: fixture.node.OriginEpoch(), ContiguousChannelSequence: 2,
			At: at}); err != nil {
			t.Fatal(err)
		}
		_, err := fixture.store.ReadPeerPullPage(context.Background(), ReadPeerPullPageSpec{
			AuthenticatedPeerID: fixture.reviewers[0], ChannelID: fixture.channel,
			OriginEpoch: fixture.node.OriginEpoch(), AfterChannelSequence: 1, Limit: 1,
			At: at.Add(time.Second)})
		if !errors.Is(err, ErrPeerPullCursor) {
			t.Fatalf("request below durable ACK error = %v", err)
		}
		mustExec(t, fixture.store, `DROP TRIGGER peer_pull_acks_confirmation_immutable`)
		mustExec(t, fixture.store, `UPDATE peer_pull_acks SET baseline_confirmed_at=NULL
			WHERE channel_id=? AND target_peer_id=?`, fixture.channel.String(), fixture.reviewers[0].String())
		_, err = fixture.store.ReadPeerPullPage(context.Background(), ReadPeerPullPageSpec{
			AuthenticatedPeerID: fixture.reviewers[0], ChannelID: fixture.channel,
			OriginEpoch: fixture.node.OriginEpoch(), AfterChannelSequence: 2, Limit: 1,
			At: at.Add(2 * time.Second)})
		if !errors.Is(err, ErrPeerPullNotMember) || !errors.Is(err, ErrPeerPullAuthority) {
			t.Fatalf("unconfirmed outbound baseline error = %v", err)
		}
	})

	t.Run("explicit history gap", func(t *testing.T) {
		fixture, _ := newPeerPullSourceFixture(t, 4)
		at := peerPullTestAt(fixture)
		mustExec(t, fixture.store, `UPDATE publication_epochs SET source_floor_channel_seq=3,
			updated_at=? WHERE channel_id=?`, storeTime(at), fixture.channel.String())
		_, err := fixture.store.ReadPeerPullPage(context.Background(), ReadPeerPullPageSpec{
			AuthenticatedPeerID: fixture.reviewers[0], ChannelID: fixture.channel,
			OriginEpoch: fixture.node.OriginEpoch(), AfterChannelSequence: 0, Limit: 2,
			At: at.Add(time.Second)})
		var gap PeerPullHistoryGap
		if !errors.Is(err, ErrPeerPullHistoryGap) || !errors.As(err, &gap) || gap.SourceFloor != 3 {
			t.Fatalf("history gap = %v (%#v)", err, gap)
		}
		var acknowledged uint64
		if scanErr := fixture.store.db.QueryRow(`SELECT acknowledged_channel_seq FROM peer_pull_acks
			WHERE channel_id=? AND target_peer_id=?`, fixture.channel.String(),
			fixture.reviewers[0].String()).Scan(&acknowledged); scanErr != nil || acknowledged != 0 {
			t.Fatalf("history gap changed ACK = (%d,%v)", acknowledged, scanErr)
		}
		page := readPeerPullPage(t, fixture, 2, 2, at.Add(2*time.Second))
		if page.SourceFloor != 3 || page.ScannedChannelSequence != 4 {
			t.Fatalf("floor-forward page = %#v", page)
		}
	})

	t.Run("input and source head bounds", func(t *testing.T) {
		fixture, _ := newPeerPullSourceFixture(t, 1)
		at := peerPullTestAt(fixture)
		for name, spec := range map[string]ReadPeerPullPageSpec{
			"zero limit": {AuthenticatedPeerID: fixture.reviewers[0], ChannelID: fixture.channel,
				OriginEpoch: fixture.node.OriginEpoch(), Limit: 0, At: at},
			"large limit": {AuthenticatedPeerID: fixture.reviewers[0], ChannelID: fixture.channel,
				OriginEpoch: fixture.node.OriginEpoch(), Limit: 33, At: at},
			"past head": {AuthenticatedPeerID: fixture.reviewers[0], ChannelID: fixture.channel,
				OriginEpoch: fixture.node.OriginEpoch(), AfterChannelSequence: 2, Limit: 1,
				At: at},
		} {
			t.Run(name, func(t *testing.T) {
				_, err := fixture.store.ReadPeerPullPage(context.Background(), spec)
				if name == "past head" {
					if !errors.Is(err, ErrPeerPullCursor) {
						t.Fatalf("error = %v", err)
					}
				} else if !errors.Is(err, ErrPeerPullInput) {
					t.Fatalf("error = %v", err)
				}
			})
		}
	})
}

func TestPeerPullSourceRejectsWrongPeerEpochRevocationAndDurableCorruption(t *testing.T) {
	t.Run("wrong authenticated peer and local origin epoch", func(t *testing.T) {
		fixture, _ := newPeerPullSourceFixture(t, 1)
		at := peerPullTestAt(fixture)
		for name, mutate := range map[string]func(*ReadPeerPullPageSpec){
			"peer": func(spec *ReadPeerPullPageSpec) { spec.AuthenticatedPeerID = fixture.node.PeerID() },
			"epoch": func(spec *ReadPeerPullPageSpec) {
				spec.OriginEpoch, _ = model.ParseOriginEpoch("epoch-peer-pull-wrong")
			},
		} {
			t.Run(name, func(t *testing.T) {
				spec := ReadPeerPullPageSpec{AuthenticatedPeerID: fixture.reviewers[0],
					ChannelID: fixture.channel, OriginEpoch: fixture.node.OriginEpoch(), Limit: 1,
					At: at}
				mutate(&spec)
				_, err := fixture.store.ReadPeerPullPage(context.Background(), spec)
				want := ErrPeerPullNotMember
				if name == "epoch" {
					want = ErrPeerPullEpochMismatch
				}
				if !errors.Is(err, want) {
					t.Fatalf("error = %v, want %v", err, want)
				}
				ackSpec := CommitPeerPullCursorAckSpec{AuthenticatedPeerID: spec.AuthenticatedPeerID,
					ChannelID: spec.ChannelID, OriginEpoch: spec.OriginEpoch,
					ContiguousChannelSequence: 0, At: at}
				_, err = fixture.store.CommitPeerPullCursorAck(context.Background(), ackSpec)
				if !errors.Is(err, want) {
					t.Fatalf("CursorAck error = %v, want %v", err, want)
				}
			})
		}
	})

	t.Run("revoked current requester", func(t *testing.T) {
		fixture, _ := newPeerPullSourceFixture(t, 1)
		at := peerPullTestAt(fixture)
		signed := acceptanceSignedChannel(t, fixture)
		terminal := signed.AppendTerminal(t, fixture.reviewers[0], model.MemberRevoked)
		if _, err := fixture.store.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
			ChannelID: fixture.channel, AuthenticatedTransportPeerID: fixture.reviewers[0],
			Records: []model.Member{terminal.Member()}, At: at}); err != nil {
			t.Fatal(err)
		}
		_, err := fixture.store.ReadPeerPullPage(context.Background(), ReadPeerPullPageSpec{
			AuthenticatedPeerID: fixture.reviewers[0], ChannelID: fixture.channel,
			OriginEpoch: fixture.node.OriginEpoch(), Limit: 1, At: at.Add(time.Second)})
		if !errors.Is(err, ErrPeerPullMemberRevoked) || !errors.Is(err, ErrPeerPullAuthority) {
			t.Fatalf("revoked requester error = %v", err)
		}
	})

	t.Run("missing source sequence fails closed", func(t *testing.T) {
		fixture, _ := newPeerPullSourceFixture(t, 2)
		at := peerPullTestAt(fixture)
		// Immutable Event deletion is forbidden in production. Removing the
		// queue projection is enough to model damaged source publication bytes.
		mustExec(t, fixture.store, `DELETE FROM gossip_publications WHERE channel_seq=2`)
		_, err := fixture.store.ReadPeerPullPage(context.Background(), ReadPeerPullPageSpec{
			AuthenticatedPeerID: fixture.reviewers[0], ChannelID: fixture.channel,
			OriginEpoch: fixture.node.OriginEpoch(), AfterChannelSequence: 1, Limit: 2, At: at})
		if !errors.Is(err, ErrPeerPullInvariant) || errors.Is(err, ErrPeerPullCursor) {
			t.Fatalf("damaged publication error = %v", err)
		}
		var acknowledged uint64
		if scanErr := fixture.store.db.QueryRow(`SELECT acknowledged_channel_seq FROM peer_pull_acks
			WHERE channel_id=? AND target_peer_id=?`, fixture.channel.String(),
			fixture.reviewers[0].String()).Scan(&acknowledged); scanErr != nil || acknowledged != 0 {
			t.Fatalf("failed page committed ACK = (%d,%v)", acknowledged, scanErr)
		}
		assertPeerPullDeliveryStates(t, fixture, 0, 2)
	})

	t.Run("corrupt authority is not downgraded to not origin", func(t *testing.T) {
		fixture, _ := newPeerPullSourceFixture(t, 1)
		mustExec(t, fixture.store, `DROP TRIGGER channels_descriptor_immutable`)
		mustExec(t, fixture.store, `UPDATE channels SET descriptor_json='{}' WHERE channel_id=?`,
			fixture.channel.String())
		_, err := fixture.store.ReadPeerPullPage(context.Background(), ReadPeerPullPageSpec{
			AuthenticatedPeerID: fixture.reviewers[0], ChannelID: fixture.channel,
			OriginEpoch: fixture.node.OriginEpoch(), Limit: 1, At: peerPullTestAt(fixture)})
		if !errors.Is(err, ErrPeerPullInvariant) || errors.Is(err, ErrPeerPullNotOrigin) ||
			errors.Is(err, ErrPeerPullAuthority) {
			t.Fatalf("corrupt authority error = %v", err)
		}
	})
}

func TestPeerPullSourceReturnsClosedAuthorityFailures(t *testing.T) {
	t.Run("not origin", func(t *testing.T) {
		fixture := newAcceptanceFixture(t, 1)
		otherChannel, err := model.ParseChannelID("channel-peer-pull-other-origin")
		if err != nil {
			t.Fatal(err)
		}
		assertPeerPullAuthorityFailure(t, fixture, otherChannel, fixture.reviewers[0],
			ErrPeerPullNotOrigin, peerPullTestAt(fixture))
	})

	t.Run("not member", func(t *testing.T) {
		fixture := newAcceptanceFixture(t, 1)
		assertPeerPullAuthorityFailure(t, fixture, fixture.channel, fixture.node.PeerID(),
			ErrPeerPullNotMember, peerPullTestAt(fixture))
	})

	t.Run("member revoked", func(t *testing.T) {
		fixture := newAcceptanceFixture(t, 1)
		at := peerPullTestAt(fixture)
		signed := acceptanceSignedChannel(t, fixture)
		revoked := signed.AppendTerminal(t, fixture.reviewers[0], model.MemberRevoked)
		if _, err := fixture.store.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
			ChannelID: fixture.channel, AuthenticatedTransportPeerID: fixture.reviewers[0],
			Records: []model.Member{revoked.Member()}, At: at}); err != nil {
			t.Fatal(err)
		}
		assertPeerPullAuthorityFailure(t, fixture, fixture.channel, fixture.reviewers[0],
			ErrPeerPullMemberRevoked, at.Add(time.Second))
	})

	t.Run("channel closed", func(t *testing.T) {
		fixture := newAcceptanceFixture(t, 1)
		at := peerPullTestAt(fixture)
		signed := acceptanceSignedChannel(t, fixture)
		left := signed.AppendTerminal(t, fixture.node.PeerID(), model.MemberLeft)
		if _, err := fixture.store.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
			ChannelID: fixture.channel, AuthenticatedTransportPeerID: fixture.reviewers[0],
			Records: []model.Member{left.Member()}, At: at}); err != nil {
			t.Fatal(err)
		}
		assertPeerPullAuthorityFailure(t, fixture, fixture.channel, fixture.reviewers[0],
			ErrPeerPullChannelClosed, at.Add(time.Second))
	})

	t.Run("requester terminal precedes channel closed", func(t *testing.T) {
		fixture := newAcceptanceFixture(t, 1)
		at := peerPullTestAt(fixture)
		signed := acceptanceSignedChannel(t, fixture)
		revoked := signed.AppendTerminal(t, fixture.reviewers[0], model.MemberRevoked)
		left := signed.AppendTerminal(t, fixture.node.PeerID(), model.MemberLeft)
		if _, err := fixture.store.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
			ChannelID: fixture.channel, AuthenticatedTransportPeerID: fixture.reviewers[0],
			Records: []model.Member{revoked.Member(), left.Member()}, At: at}); err != nil {
			t.Fatal(err)
		}
		assertPeerPullAuthorityFailure(t, fixture, fixture.channel, fixture.reviewers[0],
			ErrPeerPullMemberRevoked, at.Add(time.Second))
	})
}

func TestPeerPullSourceAppliesCountAndCanonicalByteBounds(t *testing.T) {
	fixture, publications := newPeerPullSourceFixture(t, 33)
	page := readPeerPullPage(t, fixture, 0, 32, peerPullTestAt(fixture))
	if len(page.Publications) != 32 || page.ScannedChannelSequence != 32 {
		t.Fatalf("bounded page = %d publications, scanned %d", len(page.Publications), page.ScannedChannelSequence)
	}
	total := 0
	for index, publication := range page.Publications {
		total += len(publication.WireJSON().Bytes())
		if !bytes.Equal(publication.WireJSON().Bytes(), publications[index].WireJSON().Bytes()) {
			t.Fatalf("publication %d changed canonical wire", index+1)
		}
	}
	if total > maxPeerPullPageBytes-maxPeerPullPageEnvelopeBytes {
		t.Fatalf("publication bytes = %d, payload budget %d", total,
			maxPeerPullPageBytes-maxPeerPullPageEnvelopeBytes)
	}
}

func newPeerPullSourceFixture(t *testing.T, count int) (*acceptanceFixture, []model.SignedPublication) {
	t.Helper()
	fixture := newAcceptanceFixture(t, 1)
	base := fixture.now
	publications := make([]model.SignedPublication, count)
	for index := 0; index < count; index++ {
		fixture.now = base.Add(time.Duration(index) * time.Second)
		suffix := fmt.Sprintf("peer-pull-%02d", index+1)
		_, authority := fixture.reserveOffer(t, suffix, nil)
		acceptance := fixture.offer(t, authority, suffix, fixture.reviewers, nil, nil)
		if _, err := fixture.store.CommitLocalAcceptance(context.Background(), acceptance,
			fixture.now.Add(500*time.Millisecond)); err != nil {
			t.Fatalf("commit source publication %d: %v", index+1, err)
		}
		publications[index] = acceptance.Items[0].Publication
	}
	fixture.now = base
	return fixture, publications
}

func peerPullTestAt(fixture *acceptanceFixture) time.Time {
	return fixture.now.Add(24 * time.Hour)
}

func assertPeerPullAuthorityFailure(t *testing.T, fixture *acceptanceFixture,
	channelID model.ChannelID, requester model.PeerID, want error, at time.Time,
) {
	t.Helper()
	operations := map[string]func() error{
		"Pull": func() error {
			_, err := fixture.store.ReadPeerPullPage(context.Background(), ReadPeerPullPageSpec{
				AuthenticatedPeerID: requester, ChannelID: channelID,
				OriginEpoch: fixture.node.OriginEpoch(), Limit: 1, At: at})
			return err
		},
		"CursorAck": func() error {
			_, err := fixture.store.CommitPeerPullCursorAck(context.Background(), CommitPeerPullCursorAckSpec{
				AuthenticatedPeerID: requester, ChannelID: channelID,
				OriginEpoch: fixture.node.OriginEpoch(), At: at})
			return err
		},
	}
	closed := []error{ErrPeerPullNotOrigin, ErrPeerPullNotMember, ErrPeerPullMemberRevoked,
		ErrPeerPullChannelClosed}
	for name, operation := range operations {
		err := operation()
		if !errors.Is(err, want) || !errors.Is(err, ErrPeerPullAuthority) {
			t.Fatalf("%s error = %v, want %v and parent authority", name, err, want)
		}
		for _, other := range closed {
			if other != want && errors.Is(err, other) {
				t.Fatalf("%s error = %v also matched distinct failure %v", name, err, other)
			}
		}
	}
}

func readPeerPullPage(t *testing.T, fixture *acceptanceFixture, after uint64, limit uint8,
	at time.Time,
) PeerPullPage {
	t.Helper()
	page, err := fixture.store.ReadPeerPullPage(context.Background(), ReadPeerPullPageSpec{
		AuthenticatedPeerID: fixture.reviewers[0], ChannelID: fixture.channel,
		OriginEpoch: fixture.node.OriginEpoch(), AfterChannelSequence: after, Limit: limit, At: at})
	if err != nil {
		t.Fatalf("ReadPeerPullPage(after=%d,limit=%d): %v", after, limit, err)
	}
	return page
}

func assertPeerPullPublications(t *testing.T, page PeerPullPage,
	want []model.SignedPublication, after, scanned uint64,
) {
	t.Helper()
	if len(page.Publications) != len(want) || page.ScannedChannelSequence != scanned ||
		page.SourceFloor != 1 || page.SourceHead < scanned || page.AcknowledgedSequence != after {
		t.Fatalf("page projection = %#v, want count=%d after=%d scanned=%d", page, len(want), after, scanned)
	}
	for index := range want {
		if !bytes.Equal(page.Publications[index].WireJSON().Bytes(), want[index].WireJSON().Bytes()) {
			t.Fatalf("publication %d changed exact signed bytes", index+1)
		}
	}
}

func assertPeerPullDeliveryStates(t *testing.T, fixture *acceptanceFixture, scanned, pending int) {
	t.Helper()
	var gotScanned, gotPending int
	if err := fixture.store.db.QueryRow(`SELECT
		COUNT(*) FILTER (WHERE status='scanned'),COUNT(*) FILTER (WHERE status='pending')
		FROM peer_deliveries WHERE channel_id=? AND target_peer_id=?`, fixture.channel.String(),
		fixture.reviewers[0].String()).Scan(&gotScanned, &gotPending); err != nil ||
		gotScanned != scanned || gotPending != pending {
		t.Fatalf("delivery states = scanned %d pending %d error %v, want %d/%d",
			gotScanned, gotPending, err, scanned, pending)
	}
}
