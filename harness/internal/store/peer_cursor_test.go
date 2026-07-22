package store

import (
	"context"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestPeerRepairPageProgressUsesCurrentCursorAndReplaysExactly(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "repair-page-progress", 0)
	target := onlyPeerRepairTarget(t, fixture.store, fixture.at)
	assertPeerRepairInitialTarget(t, fixture, target)
	assertPeerRepairSourceFloor(t, "initial source floor", target, 0, false)
	assertPeerRepairSourceHead(t, "initial source head", target, 0, true)

	first := fixture.publication(t, 1, 1, "repair-page-first", true)
	arrival := fixture.put(t, first, fixture.at.Add(time.Second))
	assertPeerRepairArrivalCursor(t, "first page cursor", arrival, 1)
	committedAt := fixture.at.Add(2 * time.Second)
	spec := CommitPeerRepairSpec{Target: target, Status: PeerRepairProgress,
		ContiguousChannelSequence: 1, SourceFloor: 1, SourceHead: 2,
		NextAttemptAt: committedAt, At: committedAt}
	firstCommit, err := fixture.store.CommitPeerRepair(context.Background(), spec)
	assertPeerRepairCommit(t, "first CommitPeerRepair()", firstCommit, err,
		peerRepairCommitWant{changed: true, status: PeerRepairProgress, generation: 1, contiguous: 1})
	replay, err := fixture.store.CommitPeerRepair(context.Background(), spec)
	assertPeerRepairCommit(t, "CommitPeerRepair() replay", replay, err,
		peerRepairCommitWant{replayed: true, status: PeerRepairProgress, generation: 1, contiguous: 1})

	due := onlyPeerRepairTarget(t, fixture.store, committedAt)
	assertPeerRepairTargetState(t, "due progress target", due,
		peerRepairTargetWant{status: PeerRepairProgress, generation: 1, contiguous: 1})
	second := fixture.publication(t, 2, 2, "repair-page-second", true)
	arrival = fixture.put(t, second, committedAt.Add(time.Second))
	assertPeerRepairArrivalCursor(t, "second page cursor", arrival, 2)
	nextPeriodic := committedAt.Add(time.Hour)
	caughtUp, err := fixture.store.CommitPeerRepair(context.Background(), CommitPeerRepairSpec{
		Target: due, Status: PeerRepairCaughtUp, ContiguousChannelSequence: 2,
		SourceFloor: 1, SourceHead: 2, NextAttemptAt: nextPeriodic,
		At: committedAt.Add(2 * time.Second)})
	assertPeerRepairCommit(t, "caught-up commit", caughtUp, err,
		peerRepairCommitWant{changed: true, status: PeerRepairCaughtUp, generation: 2, contiguous: 2})
	assertPeerRepairTargetCount(t, fixture.store, nextPeriodic.Add(-time.Nanosecond), 0)
	liveAt := committedAt.Add(3 * time.Second)
	third := fixture.publication(t, 3, 3, "repair-page-live-third", true)
	live, err := fixture.store.PutPeerInbox(context.Background(), PutPeerInboxSpec{
		Publication: third, TransportPeerID: fixture.remote.Identity().PeerID(),
		ArrivalSource: model.ArrivalGossip, ReceivedAt: liveAt})
	if err != nil || live.Cursor.ContiguousChannelSequence != 3 {
		t.Fatalf("live Gossip cursor advance = (%#v,%v)", live, err)
	}
	liveDue := onlyPeerRepairTarget(t, fixture.store, liveAt)
	assertPeerRepairTargetState(t, "live cursor repair reschedule", liveDue,
		peerRepairTargetWant{status: PeerRepairCaughtUp, generation: 3, contiguous: 3})
	assertPeerRepairSourceHead(t, "live cursor repair source head", liveDue, 2, true)
	periodic := onlyPeerRepairTarget(t, fixture.store, nextPeriodic)
	assertPeerRepairTargetState(t, "periodic target", periodic,
		peerRepairTargetWant{status: PeerRepairCaughtUp, generation: 3, contiguous: 3})
	assertPeerRepairSourceFloor(t, "periodic floor", periodic, 1, true)
	assertPeerRepairSourceHead(t, "periodic head", periodic, 2, true)
}

type peerRepairCommitWant struct {
	changed    bool
	replayed   bool
	status     PeerRepairStatus
	generation uint64
	contiguous uint64
}

type peerRepairTargetWant struct {
	status     PeerRepairStatus
	generation uint64
	contiguous uint64
}

func assertPeerRepairInitialTarget(t *testing.T, fixture peerInboxFixture, target PeerRepairTarget) {
	t.Helper()
	if target.ChannelID() != fixture.channel.Channel().ID() ||
		target.OriginPeerID() != fixture.remote.Identity().PeerID() ||
		target.OriginEpoch() != fixture.remote.Identity().OriginEpoch() ||
		target.RosterHead() != fixture.channel.Roster().Head() ||
		target.MemberHead() != fixture.remote.Member().Head() ||
		target.Status() != PeerRepairReady || target.Generation() != 0 || target.RetryCount() != 0 ||
		target.BaselineChannelSequence() != 0 || target.ContiguousChannelSequence() != 0 {
		t.Fatalf("initial target = %#v", target)
	}
}

func assertPeerRepairArrivalCursor(t *testing.T, label string, arrival PutPeerInboxResult, want uint64) {
	t.Helper()
	if arrival.Cursor.ContiguousChannelSequence != want {
		t.Fatalf("%s = %#v", label, arrival.Cursor)
	}
}

func assertPeerRepairCommit(t *testing.T, label string, got CommitPeerRepairResult, err error,
	want peerRepairCommitWant,
) {
	t.Helper()
	if err != nil || got.Changed != want.changed || got.Replayed != want.replayed ||
		got.Target.Status() != want.status || got.Target.Generation() != want.generation ||
		got.Target.ContiguousChannelSequence() != want.contiguous {
		t.Fatalf("%s = (%#v,%v)", label, got, err)
	}
}

func assertPeerRepairTargetState(t *testing.T, label string, target PeerRepairTarget,
	want peerRepairTargetWant,
) {
	t.Helper()
	if target.Status() != want.status || target.Generation() != want.generation ||
		target.ContiguousChannelSequence() != want.contiguous || target.RetryCount() != 0 {
		t.Fatalf("%s = %#v", label, target)
	}
}

func assertPeerRepairSourceFloor(t *testing.T, label string, target PeerRepairTarget,
	want uint64, wantKnown bool,
) {
	t.Helper()
	floor, ok := target.SourceFloor()
	if ok != wantKnown || floor != want {
		t.Fatalf("%s = (%d,%t)", label, floor, ok)
	}
}

func assertPeerRepairSourceHead(t *testing.T, label string, target PeerRepairTarget,
	want uint64, wantKnown bool,
) {
	t.Helper()
	head, ok := target.SourceHead()
	if ok != wantKnown || head != want {
		t.Fatalf("%s = (%d,%t)", label, head, ok)
	}
}
