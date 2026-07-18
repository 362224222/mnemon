package peer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestEventRepairRunStartupPeriodicAndCoalescedTrigger(t *testing.T) {
	t.Run("startup and periodic", func(t *testing.T) {
		var reads atomic.Int32
		called := make(chan struct{}, 8)
		backend := &eventRepairTestBackend{readFn: func(context.Context, time.Time) ([]eventRepairTarget, error) {
			reads.Add(1)
			called <- struct{}{}
			return nil, nil
		}}
		repair := newEventRepairForTest(t, backend, &eventRepairTestClient{}, 10*time.Millisecond, 1)
		ctx, cancel := context.WithCancel(context.Background())
		done := runEventRepairForTest(repair, ctx)
		waitEventRepairSignal(t, called, "startup cycle")
		waitEventRepairSignal(t, called, "periodic cycle")
		cancel()
		if err := waitEventRepairResult(t, done); err != nil {
			t.Fatal(err)
		}
		if reads.Load() < 2 || repair.Snapshot().State != EventRepairStopped {
			t.Fatalf("periodic snapshot = %#v, reads=%d", repair.Snapshot(), reads.Load())
		}
	})

	t.Run("manual triggers coalesce without blocking", func(t *testing.T) {
		entered := make(chan struct{}, 3)
		release := make(chan struct{})
		var reads atomic.Int32
		backend := &eventRepairTestBackend{readFn: func(ctx context.Context, _ time.Time) ([]eventRepairTarget, error) {
			call := reads.Add(1)
			entered <- struct{}{}
			if call == 1 {
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return nil, nil
		}}
		repair := newEventRepairForTest(t, backend, &eventRepairTestClient{}, 10*time.Second, 1)
		ctx, cancel := context.WithCancel(context.Background())
		done := runEventRepairForTest(repair, ctx)
		waitEventRepairSignal(t, entered, "blocked startup cycle")
		for index := 0; index < 100; index++ {
			repair.Trigger()
		}
		close(release)
		waitEventRepairSignal(t, entered, "coalesced trigger cycle")
		select {
		case <-entered:
			t.Fatal("coalesced trigger created more than one future cycle")
		case <-time.After(30 * time.Millisecond):
		}
		cancel()
		if err := waitEventRepairResult(t, done); err != nil {
			t.Fatal(err)
		}
		if reads.Load() != 2 {
			t.Fatalf("coalesced reads = %d, want 2", reads.Load())
		}
	})
}

func TestEventRepairCycleRepairsOnePagePerTargetInStrictOrder(t *testing.T) {
	target := eventRepairTargetForTest(t, "ordered", 0)
	first := eventRepairPageForTest(t, target, 1, 1, 2)
	second := eventRepairPageForTest(t, target, 2, 2, 2)
	var mu sync.Mutex
	log := make([]string, 0, 8)
	commits := make([]eventRepairCommit, 0, 2)
	current := target
	backend := &eventRepairTestBackend{}
	backend.readFn = func(context.Context, time.Time) ([]eventRepairTarget, error) {
		mu.Lock()
		defer mu.Unlock()
		return []eventRepairTarget{current}, nil
	}
	backend.putFn = func(_ context.Context, spec store.PutPeerInboxPageSpec) (store.PutPeerInboxPageResult, error) {
		mu.Lock()
		defer mu.Unlock()
		log = append(log, fmt.Sprintf("put:%d", spec.AfterChannelSequence))
		return eventRepairPutResult(spec, store.PeerInboxStored), nil
	}
	backend.commitFn = func(_ context.Context, got eventRepairTarget, commit eventRepairCommit) (eventRepairTarget, error) {
		mu.Lock()
		defer mu.Unlock()
		if got.contiguous != current.contiguous {
			return eventRepairTarget{}, store.ErrPeerRepairStale
		}
		log = append(log, fmt.Sprintf("commit:%s:%d", commit.status, commit.contiguous))
		commits = append(commits, commit)
		current.contiguous = commit.contiguous
		return current, nil
	}
	client := &eventRepairTestClient{}
	client.pull = func(_ context.Context, _ model.PeerID, request PullRequest) (PullPage, error) {
		mu.Lock()
		defer mu.Unlock()
		if request.Limit() != eventRepairPageLimit {
			return PullPage{}, fmt.Errorf("Pull limit = %d", request.Limit())
		}
		log = append(log, fmt.Sprintf("pull:%d", request.AfterChannelSequence()))
		if request.AfterChannelSequence() == 0 {
			return first, nil
		}
		return second, nil
	}
	client.ack = func(_ context.Context, _ model.PeerID, ack CursorAck) error {
		mu.Lock()
		defer mu.Unlock()
		log = append(log, fmt.Sprintf("ack:%d", ack.ContiguousChannelSequence()))
		return nil
	}
	repair := newEventRepairForTest(t, backend, client, 10*time.Second, 1)
	if err := repair.runCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if current.contiguous != 1 {
		t.Fatalf("first cycle repaired %d pages", current.contiguous)
	}
	select {
	case <-repair.trigger:
	case <-time.After(time.Second):
		t.Fatal("progress checkpoint did not trigger the next page")
	}
	if err := repair.runCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	gotLog := append([]string(nil), log...)
	mu.Unlock()
	wantLog := []string{"pull:0", "put:0", "commit:progress:1", "ack:1",
		"pull:1", "put:1", "commit:caught_up:2", "ack:2"}
	if fmt.Sprint(gotLog) != fmt.Sprint(wantLog) {
		t.Fatalf("repair order = %v, want %v", gotLog, wantLog)
	}
	if len(commits) != 2 || !commits[0].nextAttempt.Equal(commits[0].at) ||
		commits[1].nextAttempt.Sub(commits[1].at) != 10*time.Second {
		t.Fatalf("repair schedules = %#v", commits)
	}
	snapshot := repair.Snapshot()
	if snapshot.Pages != 2 || snapshot.InboxItems != 2 || snapshot.Acknowledgements != 2 {
		t.Fatalf("repair snapshot = %#v", snapshot)
	}
}

func TestEventRepairCycleUsesStableFairBoundedConcurrency(t *testing.T) {
	limit := HermeticLimits().ApplicationProtocolStreams
	targets := make([]eventRepairTarget, limit+3)
	pages := make(map[model.PeerID]PullPage, len(targets))
	for index := range targets {
		targets[index] = eventRepairTargetForTest(t, fmt.Sprintf("fair-%02d", index), 0)
		pages[targets[index].originPeer] = eventRepairEmptyPageForTest(t, targets[index].originEpoch, 0)
	}
	started := make(chan model.PeerID, len(targets))
	release := make(chan struct{})
	var mu sync.Mutex
	seen := make(map[model.PeerID]int, len(targets))
	backend := &eventRepairTestBackend{
		readFn: func(context.Context, time.Time) ([]eventRepairTarget, error) {
			return append([]eventRepairTarget(nil), targets...), nil
		},
		putFn: func(_ context.Context, spec store.PutPeerInboxPageSpec) (store.PutPeerInboxPageResult, error) {
			return eventRepairPutResult(spec, store.PeerInboxStored), nil
		},
		commitFn: func(_ context.Context, target eventRepairTarget, commit eventRepairCommit) (eventRepairTarget, error) {
			target.contiguous = commit.contiguous
			return target, nil
		},
	}
	client := &eventRepairTestClient{}
	client.pull = func(ctx context.Context, origin model.PeerID, _ PullRequest) (PullPage, error) {
		mu.Lock()
		seen[origin]++
		mu.Unlock()
		started <- origin
		select {
		case <-release:
		case <-ctx.Done():
			return PullPage{}, ctx.Err()
		}
		return pages[origin], nil
	}
	repair := newEventRepairForTest(t, backend, client, 10*time.Second, limit)
	done := make(chan error, 1)
	go func() { done <- repair.runCycle(context.Background()) }()
	for index := 0; index < limit; index++ {
		waitEventRepairSignal(t, started, "bounded target start")
	}
	select {
	case <-started:
		t.Fatalf("more than %d targets started before a slot was released", limit)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if err := waitEventRepairResult(t, done); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != len(targets) {
		t.Fatalf("repaired targets = %d, want %d", len(seen), len(targets))
	}
	for origin, count := range seen {
		if count != 1 {
			t.Fatalf("target %s repaired %d times", origin, count)
		}
	}
	if snapshot := repair.Snapshot(); snapshot.MaximumInFlight != limit || snapshot.InFlight != 0 {
		t.Fatalf("bounded concurrency snapshot = %#v", snapshot)
	}
}

func TestEventRepairAckLossRestartAndGossipAhead(t *testing.T) {
	t.Run("ACK timeout retries from durable cursor after restart", func(t *testing.T) {
		target := eventRepairTargetForTest(t, "ack-restart", 0)
		page := eventRepairPageForTest(t, target, 1, 1, 1)
		empty := eventRepairEmptyPageForTest(t, target.originEpoch, 1)
		var mu sync.Mutex
		current := target
		pulls := 0
		backend := eventRepairDurableTestBackend(&mu, &current)
		client := &eventRepairTestClient{}
		client.pull = func(_ context.Context, _ model.PeerID, request PullRequest) (PullPage, error) {
			mu.Lock()
			defer mu.Unlock()
			pulls++
			if request.AfterChannelSequence() == 0 {
				return page, nil
			}
			return empty, nil
		}
		client.ack = func(context.Context, model.PeerID, CursorAck) error {
			return context.DeadlineExceeded
		}
		first := newEventRepairForTest(t, backend, client, 10*time.Second, 1)
		if err := first.runCycle(context.Background()); err != nil {
			t.Fatal(err)
		}
		if first.Snapshot().AcknowledgementFailures != 1 || current.contiguous != 1 {
			t.Fatalf("lost ACK state = (%#v, %#v)", first.Snapshot(), current)
		}
		var acknowledged atomic.Uint64
		client.ack = func(_ context.Context, _ model.PeerID, ack CursorAck) error {
			acknowledged.Store(ack.ContiguousChannelSequence())
			return nil
		}
		restarted := newEventRepairForTest(t, backend, client, 10*time.Second, 1)
		if err := restarted.runCycle(context.Background()); err != nil {
			t.Fatal(err)
		}
		if pulls != 2 || acknowledged.Load() != 1 || restarted.Snapshot().Acknowledgements != 1 {
			t.Fatalf("restart repair = pulls:%d ACK:%d snapshot:%#v", pulls, acknowledged.Load(), restarted.Snapshot())
		}
	})

	t.Run("Gossip cursor ahead is the only ACK source", func(t *testing.T) {
		target := eventRepairTargetForTest(t, "gossip-ahead", 5)
		page := eventRepairEmptyPageForTest(t, target.originEpoch, 5)
		var committed, acknowledged uint64
		backend := &eventRepairTestBackend{
			readFn: func(context.Context, time.Time) ([]eventRepairTarget, error) {
				return []eventRepairTarget{target}, nil
			},
			putFn: func(_ context.Context, spec store.PutPeerInboxPageSpec) (store.PutPeerInboxPageResult, error) {
				result := eventRepairPutResult(spec, store.PeerInboxStored)
				result.Cursor.ContiguousChannelSequence = 8
				result.Cursor.ObservedChannelSequence = 8
				return result, nil
			},
			commitFn: func(_ context.Context, got eventRepairTarget, commit eventRepairCommit) (eventRepairTarget, error) {
				committed = commit.contiguous
				got.contiguous = commit.contiguous
				return got, nil
			},
		}
		client := &eventRepairTestClient{
			pull: func(context.Context, model.PeerID, PullRequest) (PullPage, error) { return page, nil },
			ack: func(_ context.Context, _ model.PeerID, ack CursorAck) error {
				acknowledged = ack.ContiguousChannelSequence()
				return nil
			},
		}
		repair := newEventRepairForTest(t, backend, client, 10*time.Second, 1)
		if err := repair.runCycle(context.Background()); err != nil {
			t.Fatal(err)
		}
		if committed != 8 || acknowledged != 8 {
			t.Fatalf("Gossip-ahead durable cursor = commit:%d ACK:%d", committed, acknowledged)
		}
	})
}

func TestEventRepairUnsupportedAdvancesAndConflictStopsOrigin(t *testing.T) {
	t.Run("unsupported evidence advances through durable quarantine", func(t *testing.T) {
		target := eventRepairTargetForTest(t, "unsupported", 0)
		base := newEventFramePublication(t, target.channelID, target.originPeer, target.originEpoch, 1, 8)
		raw := resignEventFramePublication(t, base.WireJSON().Bytes(), eventFramePublicationPrivateKey(),
			func(body map[string]any) {
				body["schema_version"] = json.Number("2")
				body["future_semantics"] = map[string]any{"mode": "opaque"}
			})
		evidence, err := model.ParsePublicationEvidence(raw)
		if err != nil || evidence.IsSupported() {
			t.Fatalf("unsupported evidence = (%#v, %v)", evidence, err)
		}
		page, err := newPullPageFromEvidence([]model.PublicationEvidence{evidence}, 1, 1, 1,
			target.originEpoch)
		if err != nil {
			t.Fatal(err)
		}
		var committed, acknowledged uint64
		backend := &eventRepairTestBackend{
			readFn: func(context.Context, time.Time) ([]eventRepairTarget, error) {
				return []eventRepairTarget{target}, nil
			},
			putFn: func(_ context.Context, spec store.PutPeerInboxPageSpec) (store.PutPeerInboxPageResult, error) {
				if len(spec.Publications) != 1 || spec.Publications[0].IsSupported() {
					return store.PutPeerInboxPageResult{}, store.ErrPeerInboxPage
				}
				result := eventRepairPutResult(spec, store.PeerInboxQuarantined)
				result.Quarantined = true
				return result, nil
			},
			commitFn: func(_ context.Context, got eventRepairTarget, commit eventRepairCommit) (eventRepairTarget, error) {
				committed = commit.contiguous
				got.contiguous = commit.contiguous
				return got, nil
			},
		}
		client := &eventRepairTestClient{
			pull: func(context.Context, model.PeerID, PullRequest) (PullPage, error) { return page, nil },
			ack: func(_ context.Context, _ model.PeerID, ack CursorAck) error {
				acknowledged = ack.ContiguousChannelSequence()
				return nil
			},
		}
		repair := newEventRepairForTest(t, backend, client, 10*time.Second, 1)
		if err := repair.runCycle(context.Background()); err != nil {
			t.Fatal(err)
		}
		if committed != 1 || acknowledged != 1 || repair.Snapshot().InboxItems != 1 {
			t.Fatalf("unsupported progress = commit:%d ACK:%d snapshot:%#v",
				committed, acknowledged, repair.Snapshot())
		}
	})

	t.Run("legal prefix and final conflict stops before checkpoint and ACK", func(t *testing.T) {
		target := eventRepairTargetForTest(t, "conflict", 0)
		page := eventRepairPageForTest(t, target, 1, 3, 3)
		var commits, acknowledgements atomic.Int32
		backend := &eventRepairTestBackend{
			readFn: func(context.Context, time.Time) ([]eventRepairTarget, error) {
				return []eventRepairTarget{target}, nil
			},
			putFn: func(_ context.Context, spec store.PutPeerInboxPageSpec) (store.PutPeerInboxPageResult, error) {
				// A previously observed Gossip item may remain ahead of the
				// conflict prefix; only contiguous durable coverage gates ACK.
				cursor := eventRepairCursor(spec, 2, 3)
				return store.PutPeerInboxPageResult{Items: []store.PutPeerInboxResult{
					{Disposition: store.PeerInboxStored, Cursor: cursor},
					{Disposition: store.PeerInboxConflicted, ConflictID: "conflict-2", Cursor: cursor},
				}, Cursor: cursor, Quarantined: true}, nil
			},
			commitFn: func(context.Context, eventRepairTarget, eventRepairCommit) (eventRepairTarget, error) {
				commits.Add(1)
				return eventRepairTarget{}, nil
			},
		}
		client := &eventRepairTestClient{
			pull: func(context.Context, model.PeerID, PullRequest) (PullPage, error) { return page, nil },
			ack: func(context.Context, model.PeerID, CursorAck) error {
				acknowledgements.Add(1)
				return nil
			},
		}
		repair := newEventRepairForTest(t, backend, client, 10*time.Second, 1)
		if err := repair.runCycle(context.Background()); err != nil {
			t.Fatal(err)
		}
		if commits.Load() != 0 || acknowledgements.Load() != 0 || repair.Snapshot().Conflicts != 1 ||
			repair.Snapshot().InboxItems != 2 {
			t.Fatalf("conflict handling = commit:%d ACK:%d snapshot:%#v",
				commits.Load(), acknowledgements.Load(), repair.Snapshot())
		}
	})

	t.Run("short result without final conflict is an invariant", func(t *testing.T) {
		target := eventRepairTargetForTest(t, "short-result", 0)
		page := eventRepairPageForTest(t, target, 1, 2, 2)
		backend := &eventRepairTestBackend{
			readFn: func(context.Context, time.Time) ([]eventRepairTarget, error) {
				return []eventRepairTarget{target}, nil
			},
			putFn: func(_ context.Context, spec store.PutPeerInboxPageSpec) (store.PutPeerInboxPageResult, error) {
				cursor := eventRepairCursor(spec, 1, 1)
				return store.PutPeerInboxPageResult{Items: []store.PutPeerInboxResult{
					{Disposition: store.PeerInboxStored, Cursor: cursor},
				}, Cursor: cursor}, nil
			},
		}
		client := &eventRepairTestClient{pull: func(context.Context, model.PeerID, PullRequest) (PullPage, error) {
			return page, nil
		}}
		repair := newEventRepairForTest(t, backend, client, 10*time.Second, 1)
		err := repair.runCycle(context.Background())
		if !errors.Is(err, ErrEventRepairInvariant) || eventRepairFatalCode(err) != EventRepairFatalStoreInvariant {
			t.Fatalf("short result error = %v", err)
		}
	})
}

func TestEventRepairRemoteFailureMatrix(t *testing.T) {
	tests := []struct {
		name             string
		cause            error
		status           store.PeerRepairStatus
		diagnostic       store.PeerRepairDiagnostic
		floor            uint64
		delay            time.Duration
		reconcile        bool
		reconcileFailure bool
	}{
		{name: "busy clamps retry", cause: &EventRemoteFailure{code: EventErrorBusy,
			retryable: true, retryAfter: time.Minute}, status: store.PeerRepairRetry,
			diagnostic: store.PeerRepairDiagnosticBusy, delay: 10 * time.Second},
		{name: "history gap", cause: &EventRemoteFailure{code: EventErrorHistoryGap, sourceFloor: 7},
			status: store.PeerRepairTerminal, diagnostic: store.PeerRepairDiagnosticHistoryGap, floor: 7},
		{name: "not origin", cause: &EventRemoteFailure{code: EventErrorNotOrigin},
			status: store.PeerRepairPaused, diagnostic: store.PeerRepairDiagnosticNotOrigin, reconcile: true},
		{name: "not member reconcile timeout", cause: &EventRemoteFailure{code: EventErrorNotMember},
			status: store.PeerRepairPaused, diagnostic: store.PeerRepairDiagnosticNotMember,
			reconcile: true, reconcileFailure: true},
		{name: "member revoked", cause: &EventRemoteFailure{code: EventErrorMemberRevoked},
			status: store.PeerRepairPaused, diagnostic: store.PeerRepairDiagnosticMemberRevoked, reconcile: true},
		{name: "Channel closed", cause: &EventRemoteFailure{code: EventErrorChannelClosed},
			status: store.PeerRepairPaused, diagnostic: store.PeerRepairDiagnosticChannelClosed, reconcile: true},
		{name: "origin epoch mismatch", cause: &EventRemoteFailure{code: EventErrorOriginEpochMismatch},
			status: store.PeerRepairTerminal, diagnostic: store.PeerRepairDiagnosticOriginEpochMismatch},
		{name: "transport", cause: ErrEventClientTransport, status: store.PeerRepairRetry,
			diagnostic: store.PeerRepairDiagnosticTransportUnavailable, delay: 8 * time.Second},
		{name: "internal Pull deadline", cause: context.DeadlineExceeded, status: store.PeerRepairRetry,
			diagnostic: store.PeerRepairDiagnosticTransportUnavailable, delay: 8 * time.Second},
		{name: "protocol invalid", cause: ErrEventClientResponse, status: store.PeerRepairTerminal,
			diagnostic: store.PeerRepairDiagnosticProtocolInvalid},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			target := eventRepairTargetForTest(t, "remote-"+eventRepairTestSlug(test.name), 4)
			target.retryCount = 3
			var putCalls atomic.Int32
			var reconciles atomic.Int32
			var got eventRepairCommit
			backend := &eventRepairTestBackend{
				readFn: func(context.Context, time.Time) ([]eventRepairTarget, error) {
					return []eventRepairTarget{target}, nil
				},
				putFn: func(context.Context, store.PutPeerInboxPageSpec) (store.PutPeerInboxPageResult, error) {
					putCalls.Add(1)
					return store.PutPeerInboxPageResult{}, nil
				},
				commitFn: func(_ context.Context, target eventRepairTarget, commit eventRepairCommit) (eventRepairTarget, error) {
					got = commit
					return target, nil
				},
			}
			client := &eventRepairTestClient{pull: func(context.Context, model.PeerID, PullRequest) (PullPage, error) {
				return PullPage{}, test.cause
			}}
			reconciler := &eventRepairTestReconciler{reconcile: func(context.Context, model.ChannelID, model.PeerID) error {
				reconciles.Add(1)
				if test.reconcileFailure {
					return context.DeadlineExceeded
				}
				return nil
			}}
			repair := newEventRepairWithReconcilerForTest(t, backend, client, reconciler, 10*time.Second, 1)
			if err := repair.runCycle(context.Background()); err != nil {
				t.Fatal(err)
			}
			if putCalls.Load() != 0 || got.status != test.status || got.diagnostic != test.diagnostic ||
				got.sourceFloor != test.floor || got.contiguous != target.contiguous {
				t.Fatalf("failure commit = %#v, put calls=%d", got, putCalls.Load())
			}
			if test.delay > 0 && got.nextAttempt.Sub(got.at) != test.delay {
				t.Fatalf("retry delay = %s, want %s", got.nextAttempt.Sub(got.at), test.delay)
			}
			wantReconciles := int32(0)
			if test.reconcile {
				wantReconciles = 1
			}
			if reconciles.Load() != wantReconciles {
				t.Fatalf("reconciles = %d, want %d", reconciles.Load(), wantReconciles)
			}
			snapshot := repair.Snapshot()
			if test.reconcile && (snapshot.Reconciliations != 1 ||
				snapshot.ReconciliationFailures != eventRepairBoolCount(test.reconcileFailure)) {
				t.Fatalf("reconciliation snapshot = %#v", snapshot)
			}
			switch test.status {
			case store.PeerRepairRetry:
				if snapshot.Retries != 1 {
					t.Fatalf("retry snapshot = %#v", snapshot)
				}
			case store.PeerRepairPaused:
				if snapshot.Pauses != 1 {
					t.Fatalf("pause snapshot = %#v", snapshot)
				}
			case store.PeerRepairTerminal:
				if snapshot.Terminals != 1 {
					t.Fatalf("terminal snapshot = %#v", snapshot)
				}
			}
		})
	}
}

func TestEventRepairInboxPressureIsDurableBoundedBackpressure(t *testing.T) {
	pressured := eventRepairTargetForTest(t, "pressure-a", 0)
	healthy := eventRepairTargetForTest(t, "pressure-b", 0)
	pages := map[model.PeerID]PullPage{
		pressured.originPeer: eventRepairPageForTest(t, pressured, 1, 1, 1),
		healthy.originPeer:   eventRepairPageForTest(t, healthy, 1, 1, 1),
	}
	var mu sync.Mutex
	pressure := true
	commits := make(map[model.PeerID][]eventRepairCommit)
	acks := make(map[model.PeerID]int)
	backend := &eventRepairTestBackend{
		readFn: func(context.Context, time.Time) ([]eventRepairTarget, error) {
			return []eventRepairTarget{pressured, healthy}, nil
		},
		putFn: func(_ context.Context, spec store.PutPeerInboxPageSpec) (store.PutPeerInboxPageResult, error) {
			mu.Lock()
			defer mu.Unlock()
			if spec.OriginPeerID == pressured.originPeer && pressure {
				pressure = false
				return store.PutPeerInboxPageResult{}, store.ErrPeerInboxPressure
			}
			return eventRepairPutResult(spec, store.PeerInboxStored), nil
		},
		commitFn: func(_ context.Context, target eventRepairTarget, commit eventRepairCommit) (eventRepairTarget, error) {
			mu.Lock()
			defer mu.Unlock()
			commits[target.originPeer] = append(commits[target.originPeer], commit)
			target.contiguous = commit.contiguous
			return target, nil
		},
	}
	client := &eventRepairTestClient{
		pull: func(_ context.Context, origin model.PeerID, _ PullRequest) (PullPage, error) {
			return pages[origin], nil
		},
		ack: func(_ context.Context, origin model.PeerID, _ CursorAck) error {
			mu.Lock()
			acks[origin]++
			mu.Unlock()
			return nil
		},
	}
	repair := newEventRepairForTest(t, backend, client, 10*time.Second, 2)
	if err := repair.runCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	pressureCommits := append([]eventRepairCommit(nil), commits[pressured.originPeer]...)
	healthyCommits := append([]eventRepairCommit(nil), commits[healthy.originPeer]...)
	healthyACKs := acks[healthy.originPeer]
	pressuredACKs := acks[pressured.originPeer]
	mu.Unlock()
	if len(pressureCommits) != 1 || pressureCommits[0].status != store.PeerRepairRetry ||
		pressureCommits[0].diagnostic != store.PeerRepairDiagnosticBusy ||
		pressureCommits[0].nextAttempt.Sub(pressureCommits[0].at) != time.Second ||
		len(healthyCommits) != 1 || healthyCommits[0].status != store.PeerRepairCaughtUp ||
		healthyACKs != 1 || pressuredACKs != 0 {
		t.Fatalf("pressure first cycle = pressure:%#v healthy:%#v ACKs:(%d,%d)",
			pressureCommits, healthyCommits, pressuredACKs, healthyACKs)
	}
	if err := repair.runCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	pressuredACKs = acks[pressured.originPeer]
	pressureCommits = append([]eventRepairCommit(nil), commits[pressured.originPeer]...)
	mu.Unlock()
	if pressuredACKs != 1 || len(pressureCommits) != 2 ||
		pressureCommits[1].status != store.PeerRepairCaughtUp || repair.Snapshot().Retries != 1 {
		t.Fatalf("pressure recovery = commits:%#v ACKs:%d snapshot:%#v",
			pressureCommits, pressuredACKs, repair.Snapshot())
	}
}

func TestEventRepairDurableCountersIgnoreStaleCommits(t *testing.T) {
	tests := []struct {
		name      string
		cause     error
		reconcile bool
	}{
		{name: "busy", cause: &EventRemoteFailure{code: EventErrorBusy, retryable: true}},
		{name: "history gap", cause: &EventRemoteFailure{code: EventErrorHistoryGap, sourceFloor: 3}},
		{name: "authority pause", cause: &EventRemoteFailure{code: EventErrorNotMember}, reconcile: true},
		{name: "epoch terminal", cause: &EventRemoteFailure{code: EventErrorOriginEpochMismatch}},
		{name: "transport", cause: ErrEventClientTransport},
		{name: "protocol", cause: ErrEventClientResponse},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			target := eventRepairTargetForTest(t, "stale-"+eventRepairTestSlug(test.name), 0)
			var reconciles atomic.Int32
			backend := &eventRepairTestBackend{
				readFn: func(context.Context, time.Time) ([]eventRepairTarget, error) {
					return []eventRepairTarget{target}, nil
				},
				commitFn: func(context.Context, eventRepairTarget, eventRepairCommit) (eventRepairTarget, error) {
					return eventRepairTarget{}, store.ErrPeerRepairStale
				},
			}
			client := &eventRepairTestClient{pull: func(context.Context, model.PeerID, PullRequest) (PullPage, error) {
				return PullPage{}, test.cause
			}}
			reconciler := &eventRepairTestReconciler{reconcile: func(context.Context, model.ChannelID, model.PeerID) error {
				reconciles.Add(1)
				return nil
			}}
			repair := newEventRepairWithReconcilerForTest(t, backend, client, reconciler, 10*time.Second, 1)
			if err := repair.runCycle(context.Background()); err != nil {
				t.Fatal(err)
			}
			snapshot := repair.Snapshot()
			if snapshot.Retries != 0 || snapshot.Pauses != 0 || snapshot.Terminals != 0 ||
				snapshot.Reconciliations != 0 || snapshot.ReconciliationFailures != 0 {
				t.Fatalf("stale durable counters = %#v", snapshot)
			}
			if reconciles.Load() != eventRepairInt32(test.reconcile) {
				t.Fatalf("reconcile calls = %d", reconciles.Load())
			}
		})
	}
}

func TestEventRepairLifecycleCancellationAndStoreFailures(t *testing.T) {
	t.Run("lifecycle cancellation is clean and joins target workers", func(t *testing.T) {
		target := eventRepairTargetForTest(t, "cancel", 0)
		started := make(chan struct{})
		backend := &eventRepairTestBackend{readFn: func(context.Context, time.Time) ([]eventRepairTarget, error) {
			return []eventRepairTarget{target}, nil
		}}
		client := &eventRepairTestClient{pull: func(ctx context.Context, _ model.PeerID, _ PullRequest) (PullPage, error) {
			close(started)
			<-ctx.Done()
			return PullPage{}, ctx.Err()
		}}
		repair := newEventRepairForTest(t, backend, client, 10*time.Second, 1)
		ctx, cancel := context.WithCancel(context.Background())
		done := runEventRepairForTest(repair, ctx)
		waitEventRepairSignal(t, started, "Pull start")
		cancel()
		if err := waitEventRepairResult(t, done); err != nil {
			t.Fatal(err)
		}
		if snapshot := repair.Snapshot(); snapshot.State != EventRepairStopped ||
			snapshot.InFlight != 0 || snapshot.FatalCode != EventRepairFatalNone {
			t.Fatalf("cancel snapshot = %#v", snapshot)
		}
	})

	tests := []struct {
		name string
		err  error
		code EventRepairFatalCode
	}{
		{name: "Store invariant", err: store.ErrPeerRepairInvariant, code: EventRepairFatalStoreInvariant},
		{name: "Store input", err: store.ErrPeerRepairInput, code: EventRepairFatalStoreInvariant},
		{name: "unknown Store failure", err: errors.New("database unavailable"), code: EventRepairFatalStoreFailure},
		{name: "internal Store deadline", err: context.DeadlineExceeded, code: EventRepairFatalStoreFailure},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			backend := &eventRepairTestBackend{readFn: func(context.Context, time.Time) ([]eventRepairTarget, error) {
				return nil, test.err
			}}
			repair := newEventRepairForTest(t, backend, &eventRepairTestClient{}, 10*time.Second, 1)
			err := repair.Run(context.Background())
			if !errors.Is(err, ErrEventRepairInvariant) {
				t.Fatalf("Run error = %v", err)
			}
			if snapshot := repair.Snapshot(); snapshot.State != EventRepairFailed || snapshot.FatalCode != test.code {
				t.Fatalf("fatal snapshot = %#v", snapshot)
			}
		})
	}

	t.Run("authority and quarantine mutation fences are clean", func(t *testing.T) {
		for _, candidate := range []error{store.ErrPeerRepairStale, store.ErrPeerRepairAuthority,
			store.ErrPeerInboxAuthority, store.ErrPeerInboxQuarantined} {
			if err := (&EventRepair{}).handleStoreMutationError("test", candidate); err != nil {
				t.Fatalf("clean Store fence %v became %v", candidate, err)
			}
		}
		for _, candidate := range []error{store.ErrPeerInboxInput, store.ErrPeerInboxPage,
			store.ErrPeerInboxConflict} {
			err := (&EventRepair{}).handleStoreMutationError("test", candidate)
			if eventRepairFatalCode(err) != EventRepairFatalStoreInvariant {
				t.Fatalf("Store invariant %v mapped to %v", candidate, err)
			}
		}
	})
}

func TestEventRepairConstructorAndDoubleRun(t *testing.T) {
	if repair, err := NewEventRepair(EventRepairOptions{}); repair != nil || !errors.Is(err, ErrEventRepair) {
		t.Fatalf("empty constructor = (%p, %v)", repair, err)
	}
	publicStore := eventRepairPublicTestStore{}
	client := &eventRepairTestClient{}
	reconciler := &eventRepairTestReconciler{}
	if repair, err := NewEventRepair(EventRepairOptions{Store: publicStore, Client: client,
		Reconciler: reconciler, Period: eventRepairDefaultPeriod + time.Nanosecond}); repair != nil || !errors.Is(err, ErrEventRepair) {
		t.Fatalf("unbounded period constructor = (%p, %v)", repair, err)
	}
	public, err := NewEventRepair(EventRepairOptions{Store: publicStore, Client: client,
		Reconciler: reconciler})
	if err != nil || public.period != eventRepairDefaultPeriod ||
		public.concurrency != HermeticLimits().ApplicationProtocolStreams {
		t.Fatalf("default constructor = (%#v, %v)", public, err)
	}
	backend := &eventRepairTestBackend{}
	entered := make(chan struct{})
	backend.readFn = func(ctx context.Context, _ time.Time) ([]eventRepairTarget, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	repair := newEventRepairForTest(t, backend, &eventRepairTestClient{}, 10*time.Second, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := runEventRepairForTest(repair, ctx)
	waitEventRepairSignal(t, entered, "first Run entry")
	if err := repair.Run(context.Background()); !errors.Is(err, ErrEventRepairRunning) {
		t.Fatalf("double Run error = %v", err)
	}
	cancel()
	if err := waitEventRepairResult(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestDurableEventRepairBackendReturnsExactOpaqueTargetToStore(t *testing.T) {
	publicStore := &eventRepairPublicCaptureStore{targets: []store.PeerRepairTarget{{}}}
	backend := durableEventRepairBackend{store: publicStore}
	targets, err := backend.readTargets(context.Background(), eventRepairTestTime())
	if err != nil || len(targets) != 1 || !targets[0].hasDurable {
		t.Fatalf("mapped opaque targets = (%#v, %v)", targets, err)
	}
	_, err = backend.commit(context.Background(), targets[0], eventRepairCommit{
		status: store.PeerRepairRetry, diagnostic: store.PeerRepairDiagnosticBusy,
		nextAttempt: eventRepairTestTime().Add(time.Second), at: eventRepairTestTime()})
	if err != nil || publicStore.commits != 1 || publicStore.last.Target != targets[0].durable {
		t.Fatalf("opaque commit = (%#v, %v), calls=%d", publicStore.last, err, publicStore.commits)
	}
	targets[0].hasDurable = false
	if _, err := backend.commit(context.Background(), targets[0], eventRepairCommit{}); !errors.Is(err, store.ErrPeerRepairInput) {
		t.Fatalf("reconstructed target commit error = %v", err)
	}
}

type eventRepairTestBackend struct {
	readFn   func(context.Context, time.Time) ([]eventRepairTarget, error)
	putFn    func(context.Context, store.PutPeerInboxPageSpec) (store.PutPeerInboxPageResult, error)
	commitFn func(context.Context, eventRepairTarget, eventRepairCommit) (eventRepairTarget, error)
}

type eventRepairPublicTestStore struct{}

type eventRepairPublicCaptureStore struct {
	targets []store.PeerRepairTarget
	last    store.CommitPeerRepairSpec
	commits int
}

func (eventRepairPublicTestStore) ReadPeerRepairTargets(context.Context,
	time.Time,
) ([]store.PeerRepairTarget, error) {
	return nil, nil
}

func (eventRepairPublicTestStore) PutPeerInboxPage(context.Context,
	store.PutPeerInboxPageSpec,
) (store.PutPeerInboxPageResult, error) {
	return store.PutPeerInboxPageResult{}, nil
}

func (eventRepairPublicTestStore) CommitPeerRepair(context.Context,
	store.CommitPeerRepairSpec,
) (store.CommitPeerRepairResult, error) {
	return store.CommitPeerRepairResult{}, nil
}

var _ EventRepairStore = eventRepairPublicTestStore{}

func (capture *eventRepairPublicCaptureStore) ReadPeerRepairTargets(context.Context,
	time.Time,
) ([]store.PeerRepairTarget, error) {
	return append([]store.PeerRepairTarget(nil), capture.targets...), nil
}

func (*eventRepairPublicCaptureStore) PutPeerInboxPage(context.Context,
	store.PutPeerInboxPageSpec,
) (store.PutPeerInboxPageResult, error) {
	return store.PutPeerInboxPageResult{}, nil
}

func (capture *eventRepairPublicCaptureStore) CommitPeerRepair(_ context.Context,
	spec store.CommitPeerRepairSpec,
) (store.CommitPeerRepairResult, error) {
	capture.last = spec
	capture.commits++
	return store.CommitPeerRepairResult{Target: spec.Target}, nil
}

var _ EventRepairStore = (*eventRepairPublicCaptureStore)(nil)

func (backend *eventRepairTestBackend) readTargets(ctx context.Context,
	at time.Time,
) ([]eventRepairTarget, error) {
	if backend == nil || backend.readFn == nil {
		return nil, nil
	}
	return backend.readFn(ctx, at)
}

func (backend *eventRepairTestBackend) putPage(ctx context.Context,
	spec store.PutPeerInboxPageSpec,
) (store.PutPeerInboxPageResult, error) {
	if backend == nil || backend.putFn == nil {
		return store.PutPeerInboxPageResult{}, store.ErrPeerInboxPage
	}
	return backend.putFn(ctx, spec)
}

func (backend *eventRepairTestBackend) commit(ctx context.Context, target eventRepairTarget,
	commit eventRepairCommit,
) (eventRepairTarget, error) {
	if backend == nil || backend.commitFn == nil {
		return eventRepairTarget{}, store.ErrPeerRepairInvariant
	}
	return backend.commitFn(ctx, target, commit)
}

type eventRepairTestClient struct {
	pull func(context.Context, model.PeerID, PullRequest) (PullPage, error)
	ack  func(context.Context, model.PeerID, CursorAck) error
}

func (client *eventRepairTestClient) Pull(ctx context.Context, origin model.PeerID,
	request PullRequest,
) (PullPage, error) {
	if client == nil || client.pull == nil {
		return PullPage{}, ErrEventClientTransport
	}
	return client.pull(ctx, origin, request)
}

func (client *eventRepairTestClient) Acknowledge(ctx context.Context, origin model.PeerID,
	ack CursorAck,
) error {
	if client == nil || client.ack == nil {
		return nil
	}
	return client.ack(ctx, origin, ack)
}

type eventRepairTestReconciler struct {
	reconcile func(context.Context, model.ChannelID, model.PeerID) error
}

func (reconciler *eventRepairTestReconciler) ReconcileEventRepair(ctx context.Context,
	channelID model.ChannelID, origin model.PeerID,
) error {
	if reconciler == nil || reconciler.reconcile == nil {
		return nil
	}
	return reconciler.reconcile(ctx, channelID, origin)
}

type eventRepairTestClock struct{ at time.Time }

func (clock eventRepairTestClock) Now() time.Time { return clock.at }

func newEventRepairForTest(t testing.TB, backend eventRepairBackend, client EventRepairClient,
	period time.Duration, concurrency int,
) *EventRepair {
	t.Helper()
	return newEventRepairWithReconcilerForTest(t, backend, client, &eventRepairTestReconciler{},
		period, concurrency)
}

func newEventRepairWithReconcilerForTest(t testing.TB, backend eventRepairBackend,
	client EventRepairClient, reconciler EventRepairReconciler, period time.Duration,
	concurrency int,
) *EventRepair {
	t.Helper()
	repair, err := newEventRepair(backend, client, reconciler,
		eventRepairTestClock{at: eventRepairTestTime()}, period, concurrency)
	if err != nil {
		t.Fatal(err)
	}
	return repair
}

func eventRepairTargetForTest(t testing.TB, suffix string, contiguous uint64) eventRepairTarget {
	t.Helper()
	identity := testkit.NewIdentity(t, "event-repair-"+suffix)
	channelID, err := model.ParseChannelID("channel-event-repair-" + suffix)
	if err != nil {
		t.Fatal(err)
	}
	return eventRepairTarget{channelID: channelID, originPeer: identity.PeerID(),
		originEpoch: identity.OriginEpoch(), contiguous: contiguous}
}

func eventRepairPageForTest(t testing.TB, target eventRepairTarget, first, last,
	sourceHead uint64,
) PullPage {
	t.Helper()
	publications := make([]model.SignedPublication, 0, last-first+1)
	for sequence := first; sequence <= last; sequence++ {
		publications = append(publications, newEventFramePublication(t, target.channelID,
			target.originPeer, target.originEpoch, sequence, 8))
	}
	page, err := NewPullPage(PullPageSpec{Publications: publications,
		ScannedChannelSequence: last, SourceFloor: 1, SourceHead: sourceHead,
		OriginEpoch: target.originEpoch})
	if err != nil {
		t.Fatal(err)
	}
	return page
}

func eventRepairEmptyPageForTest(t testing.TB, epoch model.OriginEpoch, after uint64) PullPage {
	t.Helper()
	page, err := NewPullPage(PullPageSpec{ScannedChannelSequence: after, SourceFloor: 1,
		SourceHead: after, OriginEpoch: epoch})
	if err != nil {
		t.Fatal(err)
	}
	return page
}

func eventRepairPutResult(spec store.PutPeerInboxPageSpec,
	disposition store.PeerInboxDisposition,
) store.PutPeerInboxPageResult {
	cursor := eventRepairCursor(spec, spec.ScannedChannelSeq, spec.ScannedChannelSeq)
	items := make([]store.PutPeerInboxResult, len(spec.Publications))
	for index := range items {
		items[index] = store.PutPeerInboxResult{Disposition: disposition, Cursor: cursor}
	}
	return store.PutPeerInboxPageResult{Items: items, Cursor: cursor,
		Quarantined: disposition == store.PeerInboxQuarantined}
}

func eventRepairCursor(spec store.PutPeerInboxPageSpec, contiguous,
	observed uint64,
) store.PeerCursorProjection {
	return store.PeerCursorProjection{ChannelID: spec.ChannelID, OriginPeerID: spec.OriginPeerID,
		OriginEpoch: spec.OriginEpoch, ContiguousChannelSequence: contiguous,
		ObservedChannelSequence: observed, UpdatedAt: spec.ReceivedAt}
}

func eventRepairDurableTestBackend(mu *sync.Mutex,
	current *eventRepairTarget,
) *eventRepairTestBackend {
	return &eventRepairTestBackend{
		readFn: func(context.Context, time.Time) ([]eventRepairTarget, error) {
			mu.Lock()
			defer mu.Unlock()
			return []eventRepairTarget{*current}, nil
		},
		putFn: func(_ context.Context, spec store.PutPeerInboxPageSpec) (store.PutPeerInboxPageResult, error) {
			return eventRepairPutResult(spec, store.PeerInboxStored), nil
		},
		commitFn: func(_ context.Context, target eventRepairTarget, commit eventRepairCommit) (eventRepairTarget, error) {
			mu.Lock()
			defer mu.Unlock()
			if target.contiguous != current.contiguous {
				return eventRepairTarget{}, store.ErrPeerRepairStale
			}
			current.contiguous = commit.contiguous
			return *current, nil
		},
	}
}

func eventRepairTestTime() time.Time {
	return time.Date(2026, 7, 19, 12, 34, 56, 0, time.UTC)
}

func runEventRepairForTest(repair *EventRepair, ctx context.Context) <-chan error {
	done := make(chan error, 1)
	go func() { done <- repair.Run(ctx) }()
	return done
}

func waitEventRepairSignal[T any](t testing.TB, signal <-chan T, detail string) T {
	t.Helper()
	select {
	case value := <-signal:
		return value
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", detail)
		var zero T
		return zero
	}
}

func waitEventRepairResult(t testing.TB, done <-chan error) error {
	t.Helper()
	return waitEventRepairSignal(t, done, "Event repair completion")
}

func eventRepairBoolCount(value bool) uint64 {
	if value {
		return 1
	}
	return 0
}

func eventRepairInt32(value bool) int32 {
	if value {
		return 1
	}
	return 0
}

func eventRepairTestSlug(value string) string {
	result := make([]byte, 0, len(value))
	for index := 0; index < len(value); index++ {
		candidate := value[index]
		if candidate >= 'a' && candidate <= 'z' {
			result = append(result, candidate)
		} else if candidate >= 'A' && candidate <= 'Z' {
			result = append(result, candidate-'A'+'a')
		} else if len(result) == 0 || result[len(result)-1] != '-' {
			result = append(result, '-')
		}
	}
	return string(result)
}
