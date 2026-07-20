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

func TestChannelBaselineTransactionsConvergeUnderConcurrencyAndRestart(t *testing.T) {
	t.Parallel()
	fixture := newChannelBaselineFixture(t, "concurrent-restart", model.TopicJoined)
	const callers = 12
	type reserveOutcome struct {
		result ReserveOutboundChannelBaselineResult
		err    error
	}
	reservations := collectConcurrentBaselineOutcomes(t, callers, func() reserveOutcome {
		result, err := fixture.store.ReserveOutboundChannelBaseline(context.Background(),
			ReserveOutboundChannelBaselineSpec{ChannelID: fixture.channel.Channel().ID(),
				TargetPeerID: fixture.remote.Identity().PeerID(), At: fixture.at})
		return reserveOutcome{result: result, err: err}
	})
	reservedCount := 0
	for _, outcome := range reservations {
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
	installs := collectConcurrentBaselineOutcomes(t, callers, func() installOutcome {
		result, err := fixture.store.InstallInboundChannelBaseline(context.Background(),
			InstallInboundChannelBaselineSpec{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
				Baseline: fixture.remoteBaseline(5), At: fixture.at.Add(time.Second)})
		return installOutcome{result: result, err: err}
	})
	installedCount := 0
	for _, outcome := range installs {
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
	ack := ChannelDataBaselineAck{ChannelID: fixture.channel.Channel().ID(),
		OriginPeerID: fixture.channel.Owner().PeerID(),
		OriginEpoch:  fixture.channel.Owner().OriginEpoch()}
	confirmations := collectConcurrentBaselineOutcomes(t, callers, func() confirmOutcome {
		result, err := fixture.store.ConfirmOutboundChannelBaseline(context.Background(),
			ConfirmOutboundChannelBaselineSpec{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
				Ack: ack, At: fixture.at.Add(2 * time.Second)})
		return confirmOutcome{result: result, err: err}
	})
	confirmedCount := 0
	for _, outcome := range confirmations {
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

	assertChannelBaselineSurvivesRestart(t, fixture)
}

// collectConcurrentBaselineOutcomes fans one Store call out over concurrent
// callers and gathers every outcome so the test goroutine owns all assertions.
func collectConcurrentBaselineOutcomes[T any](t *testing.T, callers int, call func() T) []T {
	t.Helper()
	outcomes := make(chan T, callers)
	var group sync.WaitGroup
	for index := 0; index < callers; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			outcomes <- call()
		}()
	}
	group.Wait()
	close(outcomes)
	collected := make([]T, 0, callers)
	for outcome := range outcomes {
		collected = append(collected, outcome)
	}
	return collected
}

// assertChannelBaselineSurvivesRestart reopens the durable Store and verifies
// that every baseline transaction replays read-only after a restart.
func assertChannelBaselineSurvivesRestart(t *testing.T, fixture channelBaselineFixture) {
	t.Helper()
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
