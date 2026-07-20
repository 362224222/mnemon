package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestReserveOutboundChannelBaselineFreezesCurrentHeadAndReplays(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		head uint64
	}{
		{name: "empty origin", head: 0},
		{name: "nonempty origin", head: 3},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newChannelBaselineFixture(t, "reserve-"+test.name, model.TopicJoined)
			fixture.advanceLocalHead(t, test.head, fixture.at)
			first, err := fixture.store.ReserveOutboundChannelBaseline(context.Background(),
				ReserveOutboundChannelBaselineSpec{ChannelID: fixture.channel.Channel().ID(),
					TargetPeerID: fixture.remote.Identity().PeerID(), At: fixture.at.Add(time.Second)})
			if err != nil || !first.Reserved ||
				first.Baseline.BaselineChannelSequence != test.head ||
				first.Baseline.OriginPeerID != fixture.channel.Owner().PeerID() ||
				first.Baseline.OriginEpoch != fixture.channel.Owner().OriginEpoch() {
				t.Fatalf("first reservation = (%#v, %v)", first, err)
			}
			var originalUpdated string
			if err := fixture.store.db.QueryRow(`SELECT updated_at FROM peer_pull_acks
				WHERE channel_id=? AND target_peer_id=?`, fixture.channel.Channel().ID().String(),
				fixture.remote.Identity().PeerID().String()).Scan(&originalUpdated); err != nil {
				t.Fatal(err)
			}

			fixture.advanceLocalHead(t, test.head+2, fixture.at.Add(2*time.Second))
			replayed, err := fixture.store.ReserveOutboundChannelBaseline(context.Background(),
				ReserveOutboundChannelBaselineSpec{ChannelID: fixture.channel.Channel().ID(),
					TargetPeerID: fixture.remote.Identity().PeerID(), At: fixture.at.Add(3 * time.Second)})
			if err != nil || replayed.Reserved || replayed.Baseline != first.Baseline {
				t.Fatalf("replayed reservation = (%#v, %v), first %#v", replayed, err, first)
			}
			var replayUpdated string
			if err := fixture.store.db.QueryRow(`SELECT updated_at FROM peer_pull_acks
				WHERE channel_id=? AND target_peer_id=?`, fixture.channel.Channel().ID().String(),
				fixture.remote.Identity().PeerID().String()).Scan(&replayUpdated); err != nil {
				t.Fatal(err)
			}
			if replayUpdated != originalUpdated {
				t.Fatalf("reservation replay changed updated_at from %q to %q", originalUpdated, replayUpdated)
			}
			var baseline, acknowledged uint64
			if err := fixture.store.db.QueryRow(`SELECT baseline_channel_seq,acknowledged_channel_seq
				FROM peer_pull_acks WHERE channel_id=? AND target_peer_id=?`,
				fixture.channel.Channel().ID().String(), fixture.remote.Identity().PeerID().String()).
				Scan(&baseline, &acknowledged); err != nil || baseline != test.head || acknowledged != test.head {
				t.Fatalf("durable reservation = (%d,%d,%v)", baseline, acknowledged, err)
			}
		})
	}
}

func TestInstallInboundChannelBaselineActivatesAtomicallyAndReplaysExactly(t *testing.T) {
	t.Parallel()
	fixture := newChannelBaselineFixture(t, "install", model.TopicJoined)
	baseline := fixture.remoteBaseline(7)
	first, err := fixture.store.InstallInboundChannelBaseline(context.Background(),
		InstallInboundChannelBaselineSpec{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
			Baseline: baseline, At: fixture.at})
	if err != nil || !first.Installed || first.Baseline != baseline {
		t.Fatalf("first install = (%#v,%v)", first, err)
	}
	var state, originalUpdated string
	var initial, contiguous, observed uint64
	if err := fixture.store.db.QueryRow(`SELECT binding.state,cursor.baseline_channel_seq,
		cursor.contiguous_channel_seq,cursor.observed_channel_seq,cursor.updated_at
		FROM peer_bindings binding JOIN peer_cursors cursor ON cursor.channel_id=binding.channel_id
		AND cursor.origin_peer_id=binding.peer_id AND cursor.origin_epoch=binding.origin_epoch
		WHERE binding.channel_id=? AND binding.peer_id=?`, fixture.channel.Channel().ID().String(),
		fixture.remote.Identity().PeerID().String()).Scan(&state, &initial, &contiguous, &observed,
		&originalUpdated); err != nil || state != string(model.BindingActive) ||
		initial != 7 || contiguous != 7 || observed != 7 {
		t.Fatalf("installed state = (%q,%d,%d,%d,%v)", state, initial, contiguous, observed, err)
	}

	replay, err := fixture.store.InstallInboundChannelBaseline(context.Background(),
		InstallInboundChannelBaselineSpec{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
			Baseline: baseline, At: fixture.at.Add(time.Hour)})
	if err != nil || replay.Installed || replay.Baseline != baseline ||
		!replay.InstalledAt.Equal(first.InstalledAt) {
		t.Fatalf("install replay = (%#v,%v), first %#v", replay, err, first)
	}
	var replayUpdated string
	if err := fixture.store.db.QueryRow(`SELECT updated_at FROM peer_cursors WHERE channel_id=?
		AND origin_peer_id=?`, fixture.channel.Channel().ID().String(),
		fixture.remote.Identity().PeerID().String()).Scan(&replayUpdated); err != nil ||
		replayUpdated != originalUpdated {
		t.Fatalf("install replay updated_at = (%q,%v), want %q", replayUpdated, err, originalUpdated)
	}

	different := baseline
	different.BaselineChannelSequence++
	if _, err := fixture.store.InstallInboundChannelBaseline(context.Background(),
		InstallInboundChannelBaselineSpec{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
			Baseline: different, At: fixture.at.Add(2 * time.Hour)}); !errors.Is(err, ErrChannelBaselineConflict) {
		t.Fatalf("different replay error = %v", err)
	}
}

func TestChannelBaselineRejectsWrongEpochAuthenticationAndRevokedAuthority(t *testing.T) {
	t.Parallel()
	t.Run("wrong inbound epoch and authenticated source", func(t *testing.T) {
		t.Parallel()
		fixture := newChannelBaselineFixture(t, "wrong-inbound", model.TopicJoined)
		wrongEpoch, _ := model.ParseOriginEpoch("epoch-wrong-inbound-baseline")
		wrong := fixture.remoteBaseline(0)
		wrong.OriginEpoch = wrongEpoch
		if _, err := fixture.store.InstallInboundChannelBaseline(context.Background(),
			InstallInboundChannelBaselineSpec{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
				Baseline: wrong, At: fixture.at}); !errors.Is(err, ErrChannelBaselineEpochMismatch) {
			t.Fatalf("wrong epoch error = %v", err)
		}
		if _, err := fixture.store.InstallInboundChannelBaseline(context.Background(),
			InstallInboundChannelBaselineSpec{AuthenticatedPeerID: fixture.channel.Owner().PeerID(),
				Baseline: fixture.remoteBaseline(0), At: fixture.at}); !errors.Is(err, ErrChannelBaselineInput) {
			t.Fatalf("wrong authenticated source error = %v", err)
		}
		assertBaselineRowCount(t, fixture.store, "peer_cursors", 0)
	})

	t.Run("revoked member", func(t *testing.T) {
		t.Parallel()
		st := openTestStore(t)
		channel := testkit.NewSignedChannel(t, "baseline-revoked")
		active := channel.AppendActive(t, "baseline-revoked-remote")
		terminal := channel.AppendTerminal(t, active.Identity().PeerID(), model.MemberRevoked)
		insertChannelTestNode(t, st.db, channel.Owner(), channel.Channel().CreatedAt())
		insertSignedChannelFixture(t, st.db, channel, model.TopicJoined)
		insertSignedPeerBinding(t, st.db, channel.Channel().ID(), terminal, "former-remote",
			model.BindingRevoked, model.ReachabilityUnknown, active.Member().CreatedAt())
		mustExec(t, st, `INSERT INTO publication_epochs(channel_id,origin_peer_id,origin_epoch,
			source_floor_channel_seq,source_head_channel_seq,updated_at) VALUES(?,?,?,1,0,?)`,
			channel.Channel().ID().String(), channel.Owner().PeerID().String(),
			channel.Owner().OriginEpoch().String(), storeTime(channel.Channel().UpdatedAt()))
		at := channel.Channel().UpdatedAt().Add(time.Second)
		baseline := ChannelDataBaseline{ChannelID: channel.Channel().ID(),
			OriginPeerID: active.Identity().PeerID(), OriginEpoch: active.Identity().OriginEpoch()}
		if _, err := st.InstallInboundChannelBaseline(context.Background(),
			InstallInboundChannelBaselineSpec{AuthenticatedPeerID: active.Identity().PeerID(),
				Baseline: baseline, At: at}); !errors.Is(err, ErrChannelBaselineConflict) {
			t.Fatalf("revoked inbound error = %v", err)
		}
		if _, err := st.ReserveOutboundChannelBaseline(context.Background(),
			ReserveOutboundChannelBaselineSpec{ChannelID: channel.Channel().ID(),
				TargetPeerID: active.Identity().PeerID(), At: at}); !errors.Is(err, ErrChannelBaselineConflict) {
			t.Fatalf("revoked outbound error = %v", err)
		}
		assertBaselineRowCount(t, st, "peer_cursors", 0)
		assertBaselineRowCount(t, st, "peer_pull_acks", 0)
	})
}

func TestConfirmOutboundChannelBaselineIsExactDurableAndOrderIndependent(t *testing.T) {
	t.Parallel()
	fixture := newChannelBaselineFixture(t, "confirm", model.TopicJoined)
	fixture.advanceLocalHead(t, 2, fixture.at)
	reserved, err := fixture.store.ReserveOutboundChannelBaseline(context.Background(),
		ReserveOutboundChannelBaselineSpec{ChannelID: fixture.channel.Channel().ID(),
			TargetPeerID: fixture.remote.Identity().PeerID(), At: fixture.at.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	ack := ChannelDataBaselineAck(reserved.Baseline)
	confirmed, err := fixture.store.ConfirmOutboundChannelBaseline(context.Background(),
		ConfirmOutboundChannelBaselineSpec{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
			Ack: ack, At: fixture.at.Add(2 * time.Second)})
	if err != nil || !confirmed.Confirmed || confirmed.Ack != ack {
		t.Fatalf("first confirmation = (%#v,%v)", confirmed, err)
	}
	var originalConfirmed, originalUpdated string
	if err := fixture.store.db.QueryRow(`SELECT baseline_confirmed_at,updated_at FROM peer_pull_acks
		WHERE channel_id=? AND target_peer_id=?`, fixture.channel.Channel().ID().String(),
		fixture.remote.Identity().PeerID().String()).Scan(&originalConfirmed, &originalUpdated); err != nil {
		t.Fatal(err)
	}
	replay, err := fixture.store.ConfirmOutboundChannelBaseline(context.Background(),
		ConfirmOutboundChannelBaselineSpec{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
			Ack: ack, At: fixture.at.Add(3 * time.Second)})
	if err != nil || replay.Confirmed || !replay.ConfirmedAt.Equal(confirmed.ConfirmedAt) {
		t.Fatalf("confirmation replay = (%#v,%v), first %#v", replay, err, confirmed)
	}
	var replayConfirmed, replayUpdated string
	if err := fixture.store.db.QueryRow(`SELECT baseline_confirmed_at,updated_at FROM peer_pull_acks
		WHERE channel_id=? AND target_peer_id=?`, fixture.channel.Channel().ID().String(),
		fixture.remote.Identity().PeerID().String()).Scan(&replayConfirmed, &replayUpdated); err != nil ||
		replayConfirmed != originalConfirmed || replayUpdated != originalUpdated {
		t.Fatalf("confirmation replay mutated times = (%q,%q,%v), want (%q,%q)",
			replayConfirmed, replayUpdated, err, originalConfirmed, originalUpdated)
	}

	different := ack
	different.BaselineChannelSequence++
	if _, err := fixture.store.ConfirmOutboundChannelBaseline(context.Background(),
		ConfirmOutboundChannelBaselineSpec{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
			Ack: different, At: fixture.at.Add(4 * time.Second)}); !errors.Is(err, ErrChannelBaselineConflict) {
		t.Fatalf("different ACK error = %v", err)
	}
	wrongEpoch := ack
	wrongEpoch.OriginEpoch, _ = model.ParseOriginEpoch("epoch-wrong-local-ack")
	if _, err := fixture.store.ConfirmOutboundChannelBaseline(context.Background(),
		ConfirmOutboundChannelBaselineSpec{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
			Ack: wrongEpoch, At: fixture.at.Add(4 * time.Second)}); !errors.Is(err, ErrChannelBaselineEpochMismatch) {
		t.Fatalf("wrong ACK epoch error = %v", err)
	}

	// An outbound ACK may arrive before the reverse baseline. The two gates are
	// intentionally independent and converge once the inbound transaction lands.
	readiness, err := fixture.store.ReadChannelBaselineReadiness(context.Background(),
		fixture.channel.Channel().ID())
	if err != nil || len(readiness) != 1 || readiness[0].InboundReady ||
		!readiness[0].OutboundReady || readiness[0].Ready() {
		t.Fatalf("pre-inbound readiness = (%#v,%v)", readiness, err)
	}
	if _, err := fixture.store.InstallInboundChannelBaseline(context.Background(),
		InstallInboundChannelBaselineSpec{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
			Baseline: fixture.remoteBaseline(9), At: fixture.at.Add(5 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	readiness, err = fixture.store.ReadChannelBaselineReadiness(context.Background(),
		fixture.channel.Channel().ID())
	if err != nil || len(readiness) != 1 || !readiness[0].InboundReady ||
		!readiness[0].OutboundReady || !readiness[0].Ready() ||
		readiness[0].BindingState != model.BindingActive ||
		readiness[0].TopicState != model.TopicJoined ||
		readiness[0].RosterHead != fixture.channel.Roster().Head() {
		t.Fatalf("converged readiness = (%#v,%v)", readiness, err)
	}
}

func TestInstallInboundChannelBaselineRollsBackCursorWhenActivationFails(t *testing.T) {
	t.Parallel()
	fixture := newChannelBaselineFixture(t, "rollback", model.TopicJoined)
	mustExec(t, fixture.store, `CREATE TRIGGER test_baseline_activation_abort
		BEFORE UPDATE OF state ON peer_bindings WHEN NEW.state='active'
		BEGIN SELECT RAISE(ABORT, 'forced baseline activation failure'); END`)
	_, err := fixture.store.InstallInboundChannelBaseline(context.Background(),
		InstallInboundChannelBaselineSpec{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
			Baseline: fixture.remoteBaseline(4), At: fixture.at})
	if !errors.Is(err, ErrChannelBaselineConflict) {
		t.Fatalf("forced activation error = %v", err)
	}
	assertBaselineRowCount(t, fixture.store, "peer_cursors", 0)
	var state string
	if err := fixture.store.db.QueryRow(`SELECT state FROM peer_bindings WHERE channel_id=? AND peer_id=?`,
		fixture.channel.Channel().ID().String(), fixture.remote.Identity().PeerID().String()).Scan(&state); err != nil || state != string(model.BindingPending) {
		t.Fatalf("binding after rollback = (%q,%v)", state, err)
	}
}

func TestChannelBaselineReadinessRejectsPartiallyActivatedBinding(t *testing.T) {
	t.Parallel()
	fixture := newChannelBaselineFixture(t, "partial-activation", model.TopicJoined)
	mustExec(t, fixture.store, `INSERT INTO peer_cursors(channel_id,origin_peer_id,origin_epoch,
		baseline_channel_seq,contiguous_channel_seq,observed_channel_seq,updated_at)
		VALUES(?,?,?,0,0,0,?)`, fixture.channel.Channel().ID().String(),
		fixture.remote.Identity().PeerID().String(), fixture.remote.Identity().OriginEpoch().String(),
		storeTime(fixture.at))
	if _, err := fixture.store.ReadChannelBaselineReadiness(context.Background(),
		fixture.channel.Channel().ID()); !errors.Is(err, ErrChannelBaselineAuthority) {
		t.Fatalf("partially activated binding readiness error = %v", err)
	}
}

func TestChannelBaselineTransactionsConvergeUnderConcurrencyAndRestart(t *testing.T) {
	t.Parallel()
	fixture := newChannelBaselineFixture(t, "concurrent-restart", model.TopicJoined)
	const callers = 12
	type reserveOutcome struct {
		result ReserveOutboundChannelBaselineResult
		err    error
	}
	outcomes := make(chan reserveOutcome, callers)
	var group sync.WaitGroup
	for index := 0; index < callers; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := fixture.store.ReserveOutboundChannelBaseline(context.Background(),
				ReserveOutboundChannelBaselineSpec{ChannelID: fixture.channel.Channel().ID(),
					TargetPeerID: fixture.remote.Identity().PeerID(), At: fixture.at})
			outcomes <- reserveOutcome{result: result, err: err}
		}()
	}
	group.Wait()
	close(outcomes)
	reservedCount := 0
	for outcome := range outcomes {
		if outcome.err != nil || outcome.result.Baseline.BaselineChannelSequence != 0 {
			t.Fatalf("concurrent reservation = (%#v,%v)", outcome.result, outcome.err)
		}
		if outcome.result.Reserved {
			reservedCount++
		}
	}
	if reservedCount != 1 {
		t.Fatalf("fresh concurrent reservations = %d, want 1", reservedCount)
	}

	type installOutcome struct {
		result InstallInboundChannelBaselineResult
		err    error
	}
	installs := make(chan installOutcome, callers)
	for index := 0; index < callers; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := fixture.store.InstallInboundChannelBaseline(context.Background(),
				InstallInboundChannelBaselineSpec{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
					Baseline: fixture.remoteBaseline(5), At: fixture.at.Add(time.Second)})
			installs <- installOutcome{result: result, err: err}
		}()
	}
	group.Wait()
	close(installs)
	installedCount := 0
	for outcome := range installs {
		if outcome.err != nil || outcome.result.Baseline.BaselineChannelSequence != 5 {
			t.Fatalf("concurrent install = (%#v,%v)", outcome.result, outcome.err)
		}
		if outcome.result.Installed {
			installedCount++
		}
	}
	if installedCount != 1 {
		t.Fatalf("fresh concurrent installs = %d, want 1", installedCount)
	}
	type confirmOutcome struct {
		result ConfirmOutboundChannelBaselineResult
		err    error
	}
	confirmations := make(chan confirmOutcome, callers)
	ack := ChannelDataBaselineAck{ChannelID: fixture.channel.Channel().ID(),
		OriginPeerID: fixture.channel.Owner().PeerID(),
		OriginEpoch:  fixture.channel.Owner().OriginEpoch()}
	for index := 0; index < callers; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := fixture.store.ConfirmOutboundChannelBaseline(context.Background(),
				ConfirmOutboundChannelBaselineSpec{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
					Ack: ack, At: fixture.at.Add(2 * time.Second)})
			confirmations <- confirmOutcome{result: result, err: err}
		}()
	}
	group.Wait()
	close(confirmations)
	confirmedCount := 0
	for outcome := range confirmations {
		if outcome.err != nil || outcome.result.Ack != ack {
			t.Fatalf("concurrent confirmation = (%#v,%v)", outcome.result, outcome.err)
		}
		if outcome.result.Confirmed {
			confirmedCount++
		}
	}
	if confirmedCount != 1 {
		t.Fatalf("fresh concurrent confirmations = %d, want 1", confirmedCount)
	}

	path := fixture.store.Path()
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenExisting(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened baseline Store: %v", err)
		}
	})
	replayed, err := reopened.ReserveOutboundChannelBaseline(context.Background(),
		ReserveOutboundChannelBaselineSpec{ChannelID: fixture.channel.Channel().ID(),
			TargetPeerID: fixture.remote.Identity().PeerID(), At: fixture.at.Add(3 * time.Second)})
	if err != nil || replayed.Reserved || replayed.Baseline.BaselineChannelSequence != 0 {
		t.Fatalf("restart reservation replay = (%#v,%v)", replayed, err)
	}
	installed, err := reopened.InstallInboundChannelBaseline(context.Background(),
		InstallInboundChannelBaselineSpec{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
			Baseline: fixture.remoteBaseline(5), At: fixture.at.Add(3 * time.Second)})
	if err != nil || installed.Installed {
		t.Fatalf("restart install replay = (%#v,%v)", installed, err)
	}
	readiness, err := reopened.ReadChannelBaselineReadiness(context.Background(),
		fixture.channel.Channel().ID())
	if err != nil || len(readiness) != 1 || !readiness[0].InboundReady ||
		!readiness[0].OutboundReady || !readiness[0].Ready() {
		t.Fatalf("restart readiness = (%#v,%v)", readiness, err)
	}
}

type channelBaselineFixture struct {
	store   *Store
	channel *testkit.SignedChannel
	remote  testkit.MemberFixture
	at      time.Time
}

func newChannelBaselineFixture(t *testing.T, seed string, topic model.TopicState) channelBaselineFixture {
	t.Helper()
	st := openTestStore(t)
	channel := testkit.NewSignedChannel(t, "baseline-"+seed)
	remote := channel.AppendActive(t, "baseline-"+seed+"-remote")
	insertChannelTestNode(t, st.db, channel.Owner(), channel.Channel().CreatedAt())
	insertSignedChannelFixture(t, st.db, channel, topic)
	insertSignedPeerBinding(t, st.db, channel.Channel().ID(), remote, "baseline-remote",
		model.BindingPending, model.ReachabilityUnknown, remote.Member().CreatedAt())
	mustExec(t, st, `INSERT INTO publication_epochs(channel_id,origin_peer_id,origin_epoch,
		source_floor_channel_seq,source_head_channel_seq,updated_at) VALUES(?,?,?,1,0,?)`,
		channel.Channel().ID().String(), channel.Owner().PeerID().String(),
		channel.Owner().OriginEpoch().String(), storeTime(channel.Channel().UpdatedAt()))
	return channelBaselineFixture{store: st, channel: channel, remote: remote,
		at: channel.Channel().UpdatedAt().Add(time.Second)}
}

func (fixture channelBaselineFixture) remoteBaseline(sequence uint64) ChannelDataBaseline {
	return ChannelDataBaseline{ChannelID: fixture.channel.Channel().ID(),
		OriginPeerID: fixture.remote.Identity().PeerID(),
		OriginEpoch:  fixture.remote.Identity().OriginEpoch(), BaselineChannelSequence: sequence}
}

func (fixture channelBaselineFixture) advanceLocalHead(t *testing.T, target uint64, at time.Time) {
	t.Helper()
	var current uint64
	if err := fixture.store.db.QueryRow(`SELECT source_head_channel_seq FROM publication_epochs
		WHERE channel_id=?`, fixture.channel.Channel().ID().String()).Scan(&current); err != nil {
		t.Fatal(err)
	}
	for current < target {
		current++
		mustExec(t, fixture.store, `UPDATE publication_epochs SET source_head_channel_seq=?,updated_at=?
			WHERE channel_id=?`, current, storeTime(at.Add(time.Duration(current)*time.Nanosecond)),
			fixture.channel.Channel().ID().String())
	}
}

func assertBaselineRowCount(t *testing.T, st *Store, table string, want int) {
	t.Helper()
	var count int
	if err := st.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != want {
		t.Fatalf("%s count = (%d,%v), want %d", table, count, err, want)
	}
}
