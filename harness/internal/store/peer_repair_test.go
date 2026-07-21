package store

import (
	"context"
	"errors"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestPeerRepairPageProgressUsesCurrentCursorAndReplaysExactly(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "repair-page-progress", 0)
	target := onlyPeerRepairTarget(t, fixture.store, fixture.at)
	if target.ChannelID() != fixture.channel.Channel().ID() ||
		target.OriginPeerID() != fixture.remote.Identity().PeerID() ||
		target.OriginEpoch() != fixture.remote.Identity().OriginEpoch() ||
		target.RosterHead() != fixture.channel.Roster().Head() ||
		target.MemberHead() != fixture.remote.Member().Head() ||
		target.Status() != PeerRepairReady || target.Generation() != 0 || target.RetryCount() != 0 ||
		target.BaselineChannelSequence() != 0 || target.ContiguousChannelSequence() != 0 {
		t.Fatalf("initial target = %#v", target)
	}
	if floor, ok := target.SourceFloor(); ok || floor != 0 {
		t.Fatalf("initial source floor = (%d,%t)", floor, ok)
	}
	if head, ok := target.SourceHead(); !ok || head != 0 {
		t.Fatalf("initial source head = (%d,%t), want known baseline zero", head, ok)
	}

	first := fixture.publication(t, 1, 1, "repair-page-first", true)
	arrival := fixture.put(t, first, fixture.at.Add(time.Second))
	if arrival.Cursor.ContiguousChannelSequence != 1 {
		t.Fatalf("first page cursor = %#v", arrival.Cursor)
	}
	committedAt := fixture.at.Add(2 * time.Second)
	spec := CommitPeerRepairSpec{Target: target, Status: PeerRepairProgress,
		ContiguousChannelSequence: 1, SourceFloor: 1, SourceHead: 2,
		NextAttemptAt: committedAt, At: committedAt}
	firstCommit, err := fixture.store.CommitPeerRepair(context.Background(), spec)
	if err != nil || !firstCommit.Changed || firstCommit.Replayed ||
		firstCommit.Target.Generation() != 1 || firstCommit.Target.Status() != PeerRepairProgress ||
		firstCommit.Target.ContiguousChannelSequence() != 1 {
		t.Fatalf("first CommitPeerRepair() = (%#v,%v)", firstCommit, err)
	}
	replay, err := fixture.store.CommitPeerRepair(context.Background(), spec)
	if err != nil || replay.Changed || !replay.Replayed || replay.Target.Generation() != 1 {
		t.Fatalf("CommitPeerRepair() replay = (%#v,%v)", replay, err)
	}

	due := onlyPeerRepairTarget(t, fixture.store, committedAt)
	if due.Generation() != 1 || due.Status() != PeerRepairProgress ||
		due.ContiguousChannelSequence() != 1 {
		t.Fatalf("due progress target = %#v", due)
	}
	second := fixture.publication(t, 2, 2, "repair-page-second", true)
	arrival = fixture.put(t, second, committedAt.Add(time.Second))
	if arrival.Cursor.ContiguousChannelSequence != 2 {
		t.Fatalf("second page cursor = %#v", arrival.Cursor)
	}
	nextPeriodic := committedAt.Add(time.Hour)
	caughtUp, err := fixture.store.CommitPeerRepair(context.Background(), CommitPeerRepairSpec{
		Target: due, Status: PeerRepairCaughtUp, ContiguousChannelSequence: 2,
		SourceFloor: 1, SourceHead: 2, NextAttemptAt: nextPeriodic,
		At: committedAt.Add(2 * time.Second)})
	if err != nil || !caughtUp.Changed || caughtUp.Target.Status() != PeerRepairCaughtUp ||
		caughtUp.Target.Generation() != 2 {
		t.Fatalf("caught-up commit = (%#v,%v)", caughtUp, err)
	}
	assertPeerRepairTargetCount(t, fixture.store, nextPeriodic.Add(-time.Nanosecond), 0)
	periodic := onlyPeerRepairTarget(t, fixture.store, nextPeriodic)
	if periodic.Status() != PeerRepairCaughtUp || periodic.Generation() != 2 ||
		periodic.RetryCount() != 0 {
		t.Fatalf("periodic target = %#v", periodic)
	}
	if floor, ok := periodic.SourceFloor(); !ok || floor != 1 {
		t.Fatalf("periodic floor = (%d,%t)", floor, ok)
	}
	if head, ok := periodic.SourceHead(); !ok || head != 2 {
		t.Fatalf("periodic head = (%d,%t)", head, ok)
	}
}

func TestPeerRepairRetryAndTerminalEvidenceSurviveRestart(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "node", "node.db")
	st := openStoreTestTemplateCopy(t, path)
	channel := testkit.NewSignedChannel(t, "peer-repair-restart")
	remote := channel.AppendActive(t, "peer-repair-restart-remote")
	insertChannelTestNode(t, st.db, channel.Owner(), channel.Channel().CreatedAt())
	insertSignedChannelFixture(t, st.db, channel, model.TopicJoined)
	insertSignedPeerBinding(t, st.db, channel.Channel().ID(), remote, "repair-remote",
		model.BindingPending, model.ReachabilityUnknown, remote.Member().CreatedAt())
	installRepairCursor(t, st, channel.Channel().ID(), remote, 0, channel.Channel().UpdatedAt())

	at := channel.Channel().UpdatedAt().Add(time.Second)
	target := onlyPeerRepairTarget(t, st, at)
	retryAt := at.Add(30 * time.Second)
	retrySpec := CommitPeerRepairSpec{Target: target, Status: PeerRepairRetry,
		ContiguousChannelSequence: 0, Diagnostic: PeerRepairDiagnosticBusy,
		NextAttemptAt: retryAt, At: at}
	retried, err := st.CommitPeerRepair(context.Background(), retrySpec)
	if err != nil || !retried.Changed || retried.Target.Generation() != 1 ||
		retried.Target.RetryCount() != 1 || retried.Target.Diagnostic() != PeerRepairDiagnosticBusy {
		t.Fatalf("retry commit = (%#v,%v)", retried, err)
	}
	assertPeerRepairTargetCount(t, st, retryAt.Add(-time.Nanosecond), 0)
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st, err = OpenExisting(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	due := onlyPeerRepairTarget(t, st, retryAt)
	if due.Generation() != 1 || due.RetryCount() != 1 || due.Status() != PeerRepairRetry ||
		due.Diagnostic() != PeerRepairDiagnosticBusy {
		t.Fatalf("restarted retry target = %#v", due)
	}
	terminalAt := retryAt.Add(time.Second)
	terminalSpec := CommitPeerRepairSpec{Target: due, Status: PeerRepairTerminal,
		ContiguousChannelSequence: 0, SourceFloor: 7,
		Diagnostic: PeerRepairDiagnosticHistoryGap, At: terminalAt}
	terminal, err := st.CommitPeerRepair(context.Background(), terminalSpec)
	if err != nil || !terminal.Changed || terminal.Target.Generation() != 2 ||
		terminal.Target.Status() != PeerRepairTerminal {
		t.Fatalf("terminal commit = (%#v,%v)", terminal, err)
	}
	replay, err := st.CommitPeerRepair(context.Background(), terminalSpec)
	if err != nil || !replay.Replayed || replay.Changed {
		t.Fatalf("terminal replay = (%#v,%v)", replay, err)
	}
	assertPeerRepairTargetCount(t, st, terminalAt.Add(24*time.Hour), 0)
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st, err = OpenExisting(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	assertPeerRepairTargetCount(t, st, terminalAt.Add(48*time.Hour), 0)
	var status, diagnostic string
	var generation, retryCount, floor uint64
	var next any
	if err := st.db.QueryRow(`SELECT status,generation,retry_count,source_floor_channel_seq,
		diagnostic_code,next_attempt_at FROM peer_repairs`).Scan(&status, &generation, &retryCount,
		&floor, &diagnostic, &next); err != nil || status != "terminal" || generation != 2 ||
		retryCount != 1 || floor != 7 || diagnostic != "history_gap" || next != nil {
		t.Fatalf("durable terminal repair = (%q,%d,%d,%d,%q,%v,%v)", status, generation,
			retryCount, floor, diagnostic, next, err)
	}
}

func TestReadPeerRepairTargetsIsStableAndChannelScoped(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	createdAt := time.Date(2026, 7, 19, 1, 0, 0, 0, time.UTC)
	owner := testkit.NewIdentity(t, "peer-repair-multi-owner")
	insertChannelTestNode(t, st.db, owner, createdAt)

	channels := []*testkit.SignedChannel{
		testkit.NewSignedChannelForOwnerAt(t, "peer-repair-multi-z", owner, createdAt),
		testkit.NewSignedChannelForOwnerAt(t, "peer-repair-multi-a", owner, createdAt.Add(time.Second)),
	}
	for index, channel := range channels {
		remoteB := channel.AppendActive(t, "peer-repair-multi-remote-b-"+string(rune('a'+index)))
		remoteA := channel.AppendActive(t, "peer-repair-multi-remote-a-"+string(rune('a'+index)))
		insertSignedChannelFixture(t, st.db, channel, model.TopicJoined)
		for remoteIndex, remote := range []testkit.MemberFixture{remoteB, remoteA} {
			insertSignedPeerBinding(t, st.db, channel.Channel().ID(), remote,
				"repair-"+string(rune('a'+index))+"-"+string(rune('a'+remoteIndex)),
				model.BindingPending, model.ReachabilityUnknown, remote.Member().CreatedAt())
			installRepairCursor(t, st, channel.Channel().ID(), remote, uint64(index+remoteIndex),
				channel.Channel().UpdatedAt())
		}
	}
	at := channels[1].Channel().UpdatedAt().Add(time.Hour)
	targets, err := st.ReadPeerRepairTargets(context.Background(), at)
	if err != nil || len(targets) != 4 {
		t.Fatalf("ReadPeerRepairTargets() = (%#v,%v)", targets, err)
	}
	keys := make([]string, len(targets))
	for index, target := range targets {
		keys[index] = target.ChannelID().String() + "/" + target.OriginPeerID().String()
	}
	want := append([]string(nil), keys...)
	sort.Strings(want)
	for index := range want {
		if keys[index] != want[index] {
			t.Fatalf("target order = %v, want %v", keys, want)
		}
	}

	// A topic lifecycle change excludes only that Channel while the other
	// Channel retains its stable repair set.
	mustExec(t, st, `UPDATE channels SET topic_state='joining' WHERE channel_id=?`,
		channels[0].Channel().ID().String())
	targets, err = st.ReadPeerRepairTargets(context.Background(), at)
	if err != nil || len(targets) != 2 {
		t.Fatalf("Channel-scoped topic filter = (%#v,%v)", targets, err)
	}
	for _, target := range targets {
		if target.ChannelID() != channels[1].Channel().ID() {
			t.Fatalf("inactive Channel leaked target %#v", target)
		}
	}
}

func TestPeerRepairAuthorityPauseRecoversOrIsExcludedAndPermanentFenceRejects(t *testing.T) {
	t.Parallel()
	t.Run("remote-only convergence retries after bounded pause", func(t *testing.T) {
		t.Parallel()
		fixture := newPeerInboxFixture(t, "repair-authority-remote-only", 0)
		target := onlyPeerRepairTarget(t, fixture.store, fixture.at)
		pausedAt := fixture.at.Add(time.Second)
		paused, err := fixture.store.CommitPeerRepair(context.Background(), CommitPeerRepairSpec{
			Target: target, Status: PeerRepairPaused, ContiguousChannelSequence: 0,
			Diagnostic: PeerRepairDiagnosticNotOrigin, At: pausedAt})
		if err != nil || !paused.Changed ||
			!paused.Target.NextAttemptAt().Equal(pausedAt.Add(peerRepairAuthorityPause)) {
			t.Fatalf("bounded authority pause = (%#v,%v)", paused, err)
		}
		assertPeerRepairTargetCount(t, fixture.store,
			pausedAt.Add(peerRepairAuthorityPause-time.Nanosecond), 0)
		resumed := onlyPeerRepairTarget(t, fixture.store, pausedAt.Add(peerRepairAuthorityPause))
		if resumed.Status() != PeerRepairPaused || resumed.Generation() != 1 {
			t.Fatalf("same-authority resumed target = %#v", resumed)
		}
		recoveredAt := pausedAt.Add(peerRepairAuthorityPause)
		if _, err := fixture.store.CommitPeerRepair(context.Background(), CommitPeerRepairSpec{
			Target: resumed, Status: PeerRepairCaughtUp, ContiguousChannelSequence: 0,
			SourceFloor: 1, SourceHead: 0, NextAttemptAt: recoveredAt.Add(time.Hour),
			At: recoveredAt}); err != nil {
			t.Fatalf("remote-only convergence recovery: %v", err)
		}
	})

	t.Run("authority generation resumes paused repair", func(t *testing.T) {
		t.Parallel()
		fixture := newPeerInboxFixture(t, "repair-authority-change", 0)
		target := onlyPeerRepairTarget(t, fixture.store, fixture.at)
		pausedAt := fixture.at.Add(time.Second)
		paused, err := fixture.store.CommitPeerRepair(context.Background(), CommitPeerRepairSpec{
			Target: target, Status: PeerRepairPaused, ContiguousChannelSequence: 0,
			Diagnostic: PeerRepairDiagnosticNotMember, At: pausedAt})
		if err != nil || !paused.Changed || paused.Target.Status() != PeerRepairPaused ||
			paused.Target.RetryCount() != 1 {
			t.Fatalf("authority pause = (%#v,%v)", paused, err)
		}
		assertPeerRepairTargetCount(t, fixture.store,
			pausedAt.Add(peerRepairAuthorityPause-time.Nanosecond), 0)

		updated := fixture.channel.AppendActiveUpdate(t, fixture.remote.Identity().PeerID())
		result, err := fixture.store.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
			ChannelID:                    fixture.channel.Channel().ID(),
			AuthenticatedTransportPeerID: fixture.remote.Identity().PeerID(),
			Records:                      []model.Member{updated.Member()}, At: pausedAt.Add(time.Second)})
		if err != nil || result.Status != ChannelRosterApplied {
			t.Fatalf("roster update = (%#v,%v)", result, err)
		}
		mustExec(t, fixture.store, `UPDATE channels SET topic_state='joined',updated_at=?
			WHERE channel_id=?`, storeTime(pausedAt.Add(2*time.Second)),
			fixture.channel.Channel().ID().String())
		resumed := onlyPeerRepairTarget(t, fixture.store, pausedAt.Add(2*time.Second))
		if resumed.Status() != PeerRepairPaused || resumed.Generation() != 1 ||
			resumed.MemberHead() != updated.Member().Head() {
			t.Fatalf("resumed authority target = %#v", resumed)
		}
		_, err = fixture.store.CommitPeerRepair(context.Background(), CommitPeerRepairSpec{
			Target: target, Status: PeerRepairRetry, ContiguousChannelSequence: 0,
			Diagnostic: PeerRepairDiagnosticBusy, NextAttemptAt: pausedAt.Add(time.Minute),
			At: pausedAt.Add(3 * time.Second)})
		if !errors.Is(err, ErrPeerRepairStale) {
			t.Fatalf("old authority commit error = %v", err)
		}
		recovered, err := fixture.store.CommitPeerRepair(context.Background(), CommitPeerRepairSpec{
			Target: resumed, Status: PeerRepairCaughtUp, ContiguousChannelSequence: 0,
			SourceFloor: 1, SourceHead: 0, NextAttemptAt: pausedAt.Add(time.Hour),
			At: pausedAt.Add(3 * time.Second)})
		if err != nil || !recovered.Changed || recovered.Target.Status() != PeerRepairCaughtUp {
			t.Fatalf("recovered repair = (%#v,%v)", recovered, err)
		}
	})

	t.Run("terminal local authority excludes paused origin", func(t *testing.T) {
		t.Parallel()
		fixture := newPeerInboxFixture(t, "repair-authority-terminal", 0)
		target := onlyPeerRepairTarget(t, fixture.store, fixture.at)
		pausedAt := fixture.at.Add(time.Second)
		if _, err := fixture.store.CommitPeerRepair(context.Background(), CommitPeerRepairSpec{
			Target: target, Status: PeerRepairPaused, ContiguousChannelSequence: 0,
			Diagnostic: PeerRepairDiagnosticMemberRevoked, At: pausedAt}); err != nil {
			t.Fatal(err)
		}
		terminal := fixture.channel.AppendTerminal(t, fixture.remote.Identity().PeerID(), model.MemberRevoked)
		result, err := fixture.store.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
			ChannelID:                    fixture.channel.Channel().ID(),
			AuthenticatedTransportPeerID: fixture.remote.Identity().PeerID(),
			Records:                      []model.Member{terminal.Member()}, At: pausedAt.Add(time.Second)})
		if err != nil || result.Status != ChannelRosterApplied {
			t.Fatalf("terminal roster update = (%#v,%v)", result, err)
		}
		assertPeerRepairTargetCount(t, fixture.store, pausedAt.Add(24*time.Hour), 0)
	})

	t.Run("origin conflict", func(t *testing.T) {
		t.Parallel()
		fixture := newPeerInboxFixture(t, "repair-origin-fence", 0)
		target := onlyPeerRepairTarget(t, fixture.store, fixture.at)
		incumbent := fixture.publication(t, 1, 1, "repair-fence-incumbent", true)
		fixture.put(t, incumbent, fixture.at.Add(time.Second))
		challenger := fixture.publication(t, 1, 2, "repair-fence-challenger", true)
		conflict := fixture.put(t, challenger, fixture.at.Add(2*time.Second))
		if conflict.Disposition != PeerInboxConflicted {
			t.Fatalf("conflict disposition = %#v", conflict)
		}
		assertPeerRepairTargetCount(t, fixture.store, fixture.at.Add(time.Hour), 0)
		_, err := fixture.store.CommitPeerRepair(context.Background(), CommitPeerRepairSpec{
			Target: target, Status: PeerRepairRetry,
			ContiguousChannelSequence: conflict.Cursor.ContiguousChannelSequence,
			Diagnostic:                PeerRepairDiagnosticBusy, NextAttemptAt: fixture.at.Add(2 * time.Hour),
			At: fixture.at.Add(time.Hour)})
		if !errors.Is(err, ErrPeerRepairStale) {
			t.Fatalf("permanently fenced commit error = %v", err)
		}
	})
}

func TestReadPeerRepairTargetsFailsClosedOnCheckpointCorruption(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *Store)
	}{
		{name: "missing row", mutate: func(t *testing.T, st *Store) {
			mustExec(t, st, "DROP TRIGGER peer_repairs_no_delete")
			mustExec(t, st, "DELETE FROM peer_repairs")
		}},
		{name: "mismatched baseline", mutate: func(t *testing.T, st *Store) {
			mustExec(t, st, "DROP TRIGGER peer_repairs_generation_monotonic")
			mustExec(t, st, "DROP TRIGGER peer_repairs_identity_immutable")
			mustExec(t, st, `UPDATE peer_repairs SET baseline_channel_seq=1,
				source_head_channel_seq=1`)
		}},
		{name: "noncanonical schedule", mutate: func(t *testing.T, st *Store) {
			mustExec(t, st, "DROP TRIGGER peer_repairs_generation_monotonic")
			mustExec(t, st, "DROP TRIGGER peer_repairs_time_monotonic")
			mustExec(t, st, "UPDATE peer_repairs SET next_attempt_at='2026-07-19T00:00:00Z'")
		}},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newPeerInboxFixture(t, "repair-corrupt-"+test.name, 0)
			test.mutate(t, fixture.store)
			if _, err := fixture.store.ReadPeerRepairTargets(context.Background(),
				fixture.at.Add(24*time.Hour)); !errors.Is(err, ErrPeerRepairInvariant) {
				t.Fatalf("corrupt checkpoint error = %v", err)
			}
		})
	}
}

func TestCommitPeerRepairConcurrentCASAdvancesOneGeneration(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "repair-concurrent", 0)
	target := onlyPeerRepairTarget(t, fixture.store, fixture.at)
	at := fixture.at.Add(time.Second)
	specs := []CommitPeerRepairSpec{
		{Target: target, Status: PeerRepairRetry, ContiguousChannelSequence: 0,
			Diagnostic: PeerRepairDiagnosticBusy, NextAttemptAt: at.Add(time.Second), At: at},
		{Target: target, Status: PeerRepairRetry, ContiguousChannelSequence: 0,
			Diagnostic:    PeerRepairDiagnosticTransportUnavailable,
			NextAttemptAt: at.Add(2 * time.Second), At: at},
	}
	var wait sync.WaitGroup
	results := make(chan error, len(specs))
	for _, spec := range specs {
		spec := spec
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := fixture.store.CommitPeerRepair(context.Background(), spec)
			results <- err
		}()
	}
	wait.Wait()
	close(results)
	var succeeded, stale int
	for err := range results {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrPeerRepairStale):
			stale++
		default:
			t.Fatalf("concurrent commit error = %v", err)
		}
	}
	if succeeded != 1 || stale != 1 {
		t.Fatalf("concurrent outcomes = success %d stale %d", succeeded, stale)
	}
	var generation, retryCount uint64
	if err := fixture.store.db.QueryRow(`SELECT generation,retry_count FROM peer_repairs`).
		Scan(&generation, &retryCount); err != nil || generation != 1 || retryCount != 1 {
		t.Fatalf("durable CAS generation = (%d,%d,%v)", generation, retryCount, err)
	}
}

func onlyPeerRepairTarget(t *testing.T, st *Store, at time.Time) PeerRepairTarget {
	t.Helper()
	targets, err := st.ReadPeerRepairTargets(context.Background(), at)
	if err != nil || len(targets) != 1 {
		t.Fatalf("ReadPeerRepairTargets() = (%#v,%v), want one", targets, err)
	}
	return targets[0]
}

func assertPeerRepairTargetCount(t *testing.T, st *Store, at time.Time, want int) {
	t.Helper()
	targets, err := st.ReadPeerRepairTargets(context.Background(), at)
	if err != nil || len(targets) != want {
		t.Fatalf("ReadPeerRepairTargets() count = (%d,%v), want %d", len(targets), err, want)
	}
}

func installRepairCursor(t *testing.T, st *Store, channelID model.ChannelID,
	remote testkit.MemberFixture, baseline uint64, at time.Time,
) {
	t.Helper()
	mustExec(t, st, `INSERT INTO peer_cursors(channel_id,origin_peer_id,origin_epoch,
		baseline_channel_seq,contiguous_channel_seq,observed_channel_seq,updated_at)
		VALUES(?,?,?,?,?,?,?)`, channelID.String(), remote.Identity().PeerID().String(),
		remote.Identity().OriginEpoch().String(), baseline, baseline, baseline, storeTime(at))
	mustExec(t, st, `UPDATE peer_bindings SET state='active' WHERE channel_id=? AND peer_id=?`,
		channelID.String(), remote.Identity().PeerID().String())
}
