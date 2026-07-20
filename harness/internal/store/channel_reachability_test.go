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

func TestSetPeerReachabilityIsMonotonic(t *testing.T) {
	t.Parallel()
	fixture := newChannelReachabilityFixture(t, "reachability-monotonic")
	reachableAt := fixture.at
	base, first := setInitialPeerReachability(t, fixture, reachableAt)
	var err error
	replayed, err := fixture.store.SetPeerReachability(context.Background(), base)
	if err != nil || replayed.Changed || replayed.Peer != first.Peer {
		t.Fatalf("reachable replay = (%#v,%v), first %#v", replayed, err, first)
	}

	unreachable := fixture.spec(model.ReachabilityUnreachable, reachableAt.Add(time.Hour))
	negative, err := fixture.store.SetPeerReachability(context.Background(), unreachable)
	if err != nil || !negative.Changed || negative.Peer.Reachability != model.ReachabilityUnreachable ||
		!negative.Peer.LastSeenAt.Equal(reachableAt) {
		t.Fatalf("unreachable observation = (%#v,%v)", negative, err)
	}
	unreachableReplay := unreachable
	unreachableReplay.At = unreachable.At.Add(time.Hour)
	replayedNegative, err := fixture.store.SetPeerReachability(context.Background(), unreachableReplay)
	if err != nil || replayedNegative.Changed || !replayedNegative.Peer.LastSeenAt.Equal(reachableAt) {
		t.Fatalf("unreachable replay = (%#v,%v)", replayedNegative, err)
	}
	if _, err := fixture.store.SetPeerReachability(context.Background(), base); !errors.Is(err, ErrChannelRuntimeConflict) {
		t.Fatalf("stale reachable callback error = %v", err)
	}

	newReachableAt := reachableAt.Add(3 * time.Hour)
	newReachable := fixture.spec(model.ReachabilityReachable, newReachableAt)
	if result, err := fixture.store.SetPeerReachability(context.Background(), newReachable); err != nil || !result.Changed || !result.Peer.LastSeenAt.Equal(newReachableAt) {
		t.Fatalf("new reachable observation = (%#v,%v)", result, err)
	}
	olderUnknown := fixture.spec(model.ReachabilityUnknown, newReachableAt.Add(-time.Second))
	if _, err := fixture.store.SetPeerReachability(context.Background(), olderUnknown); !errors.Is(err, ErrChannelRuntimeConflict) {
		t.Fatalf("older negative callback error = %v", err)
	}

	read, err := fixture.store.ReadPeerReachability(context.Background(),
		fixture.channel.Channel().ID(), fixture.remote.Identity().PeerID())
	if err != nil || read.Reachability != model.ReachabilityReachable ||
		!read.LastSeenAt.Equal(newReachableAt) || read.RosterHead != fixture.channel.Roster().Head() {
		t.Fatalf("ReadPeerReachability() = (%#v,%v)", read, err)
	}
}

func setInitialPeerReachability(t *testing.T, fixture channelReachabilityFixture,
	at time.Time,
) (SetPeerReachabilitySpec, SetPeerReachabilityResult) {
	t.Helper()
	spec := fixture.spec(model.ReachabilityReachable, at)
	result, err := fixture.store.SetPeerReachability(context.Background(), spec)
	if err != nil || !result.Changed || result.Peer.Reachability != model.ReachabilityReachable ||
		!result.Peer.HasLastSeen || !result.Peer.LastSeenAt.Equal(at) {
		t.Fatalf("first reachable = (%#v,%v)", result, err)
	}
	return spec, result
}

func TestSetPeerReachabilityFencesExactBindingAuthority(t *testing.T) {
	t.Parallel()
	fixture := newChannelReachabilityFixture(t, "reachability-authority")
	wrongDigest := model.Sum([]byte("wrong-reachability-authority"))
	wrongHead, err := model.NewRecordHead(fixture.channel.Roster().Head().Revision(), wrongDigest)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*SetPeerReachabilitySpec)
	}{
		{name: "origin epoch", mutate: func(spec *SetPeerReachabilitySpec) {
			spec.OriginEpoch, _ = model.ParseOriginEpoch("epoch-wrong-reachability")
		}},
		{name: "roster head", mutate: func(spec *SetPeerReachabilitySpec) {
			spec.ExpectedRosterHead = wrongHead
		}},
		{name: "member head", mutate: func(spec *SetPeerReachabilitySpec) {
			spec.ExpectedMemberHead = wrongHead
		}},
		{name: "binding state", mutate: func(spec *SetPeerReachabilitySpec) {
			spec.ExpectedBindingState = model.BindingActive
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			spec := fixture.spec(model.ReachabilityReachable, fixture.at)
			test.mutate(&spec)
			if _, err := fixture.store.SetPeerReachability(context.Background(), spec); !errors.Is(
				err, ErrChannelRuntimeConflict,
			) {
				t.Fatalf("stale generation error = %v", err)
			}
		})
	}
	read, err := fixture.store.ReadPeerReachability(context.Background(),
		fixture.channel.Channel().ID(), fixture.remote.Identity().PeerID())
	if err != nil || read.Reachability != model.ReachabilityUnknown || read.HasLastSeen {
		t.Fatalf("stale generation changed projection = (%#v,%v)", read, err)
	}
}

func TestSetPeerReachabilityRejectsRevokedBinding(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	channel := testkit.NewSignedChannel(t, "reachability-revoked")
	remote := channel.AppendActive(t, "reachability-revoked-remote")
	terminal := channel.AppendTerminal(t, remote.Identity().PeerID(), model.MemberRevoked)
	insertChannelTestNode(t, st.db, channel.Owner(), channel.Channel().CreatedAt())
	insertSignedChannelFixture(t, st.db, channel, model.TopicJoining)
	insertSignedPeerBinding(t, st.db, channel.Channel().ID(), terminal, "former",
		model.BindingRevoked, model.ReachabilityUnknown, remote.Member().CreatedAt())
	_, err := st.SetPeerReachability(context.Background(), SetPeerReachabilitySpec{
		ChannelID: channel.Channel().ID(), PeerID: remote.Identity().PeerID(),
		OriginEpoch: remote.Identity().OriginEpoch(), ExpectedRosterHead: channel.Roster().Head(),
		ExpectedMemberHead: terminal.Member().Head(), ExpectedBindingState: model.BindingRevoked,
		Reachability: model.ReachabilityReachable, At: channel.Channel().UpdatedAt().Add(time.Hour)})
	if !errors.Is(err, ErrChannelRuntimeAuthority) {
		t.Fatalf("revoked reachability error = %v", err)
	}
	read, err := st.ReadPeerReachability(context.Background(), channel.Channel().ID(),
		remote.Identity().PeerID())
	if err != nil || read.BindingState != model.BindingRevoked || read.HasLastSeen {
		t.Fatalf("revoked read projection = (%#v,%v)", read, err)
	}
}

func TestSetPeerReachabilityConvergesUnderConcurrencyAndRestart(t *testing.T) {
	t.Parallel()
	fixture := newChannelReachabilityFixture(t, "reachability-concurrent")
	path := fixture.store.Path()
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenExisting(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	const callers = 16
	type outcome struct {
		result SetPeerReachabilityResult
		err    error
	}
	outcomes := make(chan outcome, callers)
	var group sync.WaitGroup
	spec := fixture.spec(model.ReachabilityReachable, fixture.at)
	for index := 0; index < callers; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := reopened.SetPeerReachability(context.Background(), spec)
			outcomes <- outcome{result: result, err: err}
		}()
	}
	group.Wait()
	close(outcomes)
	changed := 0
	for outcome := range outcomes {
		if outcome.err != nil || outcome.result.Peer.Reachability != model.ReachabilityReachable ||
			!outcome.result.Peer.LastSeenAt.Equal(fixture.at) {
			t.Fatalf("concurrent reachability = (%#v,%v)", outcome.result, outcome.err)
		}
		if outcome.result.Changed {
			changed++
		}
	}
	if changed != 1 {
		t.Fatalf("concurrent reachability mutations = %d, want 1", changed)
	}
	read, err := reopened.ReadPeerReachability(context.Background(), fixture.channel.Channel().ID(),
		fixture.remote.Identity().PeerID())
	if err != nil || !read.HasLastSeen || !read.LastSeenAt.Equal(fixture.at) {
		t.Fatalf("restarted concurrent projection = (%#v,%v)", read, err)
	}
}

type channelReachabilityFixture struct {
	store   *Store
	channel *testkit.SignedChannel
	remote  testkit.MemberFixture
	at      time.Time
}

func newChannelReachabilityFixture(t *testing.T, seed string) channelReachabilityFixture {
	t.Helper()
	st := openTestStore(t)
	channel := testkit.NewSignedChannel(t, seed)
	remote := channel.AppendActive(t, seed+"-remote")
	insertChannelTestNode(t, st.db, channel.Owner(), channel.Channel().CreatedAt())
	insertSignedChannelFixture(t, st.db, channel, model.TopicJoining)
	insertSignedPeerBinding(t, st.db, channel.Channel().ID(), remote, "remote",
		model.BindingPending, model.ReachabilityUnknown, remote.Member().CreatedAt())
	return channelReachabilityFixture{store: st, channel: channel, remote: remote,
		at: channel.Channel().UpdatedAt().Add(time.Hour)}
}

func (fixture channelReachabilityFixture) spec(reachability model.Reachability,
	at time.Time,
) SetPeerReachabilitySpec {
	return SetPeerReachabilitySpec{ChannelID: fixture.channel.Channel().ID(),
		PeerID: fixture.remote.Identity().PeerID(), OriginEpoch: fixture.remote.Identity().OriginEpoch(),
		ExpectedRosterHead: fixture.channel.Roster().Head(), ExpectedMemberHead: fixture.remote.Member().Head(),
		ExpectedBindingState: model.BindingPending, Reachability: reachability, At: at}
}
