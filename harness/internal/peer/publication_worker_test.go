package peer

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestGossipPublicationWorkerPublishesExactLeasesInChannelOrder(t *testing.T) {
	at := time.Date(2026, 7, 19, 1, 0, 0, 0, time.UTC)
	alpha := publicationWorkerLease(t, "alpha", at)
	beta := publicationWorkerLease(t, "beta", at)
	backend := &publicationWorkerBackend{channelsValue: []model.ChannelID{
		beta.Lease.Fence.ChannelID, alpha.Lease.Fence.ChannelID}, leases: map[model.ChannelID]store.GossipPublicationClaimResult{
		alpha.Lease.Fence.ChannelID: alpha, beta.Lease.Fence.ChannelID: beta}}
	sessions := &publicationWorkerSessions{values: map[model.ChannelID]*publicationWorkerSession{
		alpha.Lease.Fence.ChannelID: {}, beta.Lease.Fence.ChannelID: {}}}
	worker := newPublicationWorkerForTest(t, backend, sessions, at)

	if err := worker.runCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	wantOrder := []model.ChannelID{alpha.Lease.Fence.ChannelID, beta.Lease.Fence.ChannelID}
	if wantOrder[1].String() < wantOrder[0].String() {
		wantOrder[0], wantOrder[1] = wantOrder[1], wantOrder[0]
	}
	if len(backend.claimed) != 2 || backend.claimed[0] != wantOrder[0] || backend.claimed[1] != wantOrder[1] {
		t.Fatalf("claim order = %v, want %v", backend.claimed, wantOrder)
	}
	for _, result := range []store.GossipPublicationClaimResult{alpha, beta} {
		session := sessions.values[result.Lease.Fence.ChannelID]
		if len(session.publications) != 1 || !bytes.Equal(session.publications[0].WireJSON().Bytes(),
			result.Lease.Record.Publication().WireJSON().Bytes()) {
			t.Fatalf("Channel %s did not publish exact signed bytes", result.Lease.Fence.ChannelID)
		}
	}
	if len(backend.marked) != 2 || len(backend.retried) != 0 {
		t.Fatalf("settlements = marked %d retry %d", len(backend.marked), len(backend.retried))
	}
	snapshot := worker.Snapshot()
	if snapshot.Cycles != 1 || snapshot.Claims != 2 || snapshot.Published != 2 ||
		snapshot.Retries != 0 || snapshot.MaximumActive != 1 || snapshot.InFlight != 0 {
		t.Fatalf("publication snapshot = %#v", snapshot)
	}
}

func TestGossipPublicationWorkerRetriesTransportAndLeavesStalePublishForRecovery(t *testing.T) {
	at := time.Date(2026, 7, 19, 2, 0, 0, 0, time.UTC)
	t.Run("transport retry", func(t *testing.T) {
		result := publicationWorkerLease(t, "retry", at)
		backend := &publicationWorkerBackend{channelsValue: []model.ChannelID{result.Lease.Fence.ChannelID},
			leases: map[model.ChannelID]store.GossipPublicationClaimResult{result.Lease.Fence.ChannelID: result}}
		sessions := &publicationWorkerSessions{errors: map[model.ChannelID]error{
			result.Lease.Fence.ChannelID: errors.New("router unavailable")}}
		worker := newPublicationWorkerForTest(t, backend, sessions, at)
		if err := worker.runCycle(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(backend.retried) != 1 || !backend.retried[0].next.Equal(at.Add(time.Second)) ||
			backend.retried[0].diagnostic != "Gossip publication transport unavailable" {
			t.Fatalf("retry = %#v", backend.retried)
		}
		if snapshot := worker.Snapshot(); snapshot.Retries != 1 || snapshot.Published != 0 {
			t.Fatalf("retry snapshot = %#v", snapshot)
		}
	})

	t.Run("publish accepted but mark stale", func(t *testing.T) {
		result := publicationWorkerLease(t, "stale", at)
		backend := &publicationWorkerBackend{channelsValue: []model.ChannelID{result.Lease.Fence.ChannelID},
			leases:  map[model.ChannelID]store.GossipPublicationClaimResult{result.Lease.Fence.ChannelID: result},
			markErr: store.ErrGossipPublicationStale}
		sessions := &publicationWorkerSessions{values: map[model.ChannelID]*publicationWorkerSession{
			result.Lease.Fence.ChannelID: {}}}
		worker := newPublicationWorkerForTest(t, backend, sessions, at)
		if err := worker.runCycle(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(backend.retried) != 0 || worker.Snapshot().Stale != 1 {
			t.Fatalf("stale publish was retried: backend %#v snapshot %#v", backend, worker.Snapshot())
		}
	})
}

func TestGossipPublicationWorkerOwnsTriggerCancellationAndBounds(t *testing.T) {
	at := time.Date(2026, 7, 19, 3, 0, 0, 0, time.UTC)
	backend := &publicationWorkerBackend{}
	worker := newPublicationWorkerForTest(t, backend, &publicationWorkerSessions{}, at)
	for count := 0; count < 32; count++ {
		worker.Trigger()
	}
	if len(worker.trigger) != 1 {
		t.Fatalf("coalesced trigger depth = %d", len(worker.trigger))
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	waitForPublicationWorker(t, worker, GossipPublicationWorkerRunning)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("cancelled Run = %v", err)
	}
	if worker.Snapshot().State != GossipPublicationWorkerStopped {
		t.Fatalf("stopped snapshot = %#v", worker.Snapshot())
	}
	if err := worker.Run(context.Background()); !errors.Is(err, ErrGossipPublicationWorkerRunning) {
		t.Fatalf("second Run error = %v", err)
	}

	channels := make([]model.ChannelID, model.MaxChannelsPerNode+1)
	for index := range channels {
		channels[index], _ = model.ParseChannelID("publication-bound-" + string(rune('a'+index)))
	}
	bounded := newPublicationWorkerForTest(t,
		&publicationWorkerBackend{channelsValue: channels}, &publicationWorkerSessions{}, at)
	if err := bounded.runCycle(context.Background()); !errors.Is(err, ErrGossipPublicationWorker) {
		t.Fatalf("Channel bound error = %v", err)
	}
}

type publicationWorkerClock struct{ at time.Time }

func (clock publicationWorkerClock) Now() time.Time { return clock.at }

type publicationWorkerBackend struct {
	mu            sync.Mutex
	channelsValue []model.ChannelID
	leases        map[model.ChannelID]store.GossipPublicationClaimResult
	claimErr      error
	markErr       error
	retryErr      error
	claimed       []model.ChannelID
	marked        []store.GossipPublicationFence
	retried       []publicationWorkerRetry
}

type publicationWorkerRetry struct {
	fence      store.GossipPublicationFence
	at         time.Time
	next       time.Time
	diagnostic string
}

func (backend *publicationWorkerBackend) channels(context.Context) ([]model.ChannelID, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return append([]model.ChannelID(nil), backend.channelsValue...), nil
}

func (backend *publicationWorkerBackend) claim(_ context.Context, channelID model.ChannelID,
	_ string, _, _ time.Time,
) (store.GossipPublicationClaimResult, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.claimed = append(backend.claimed, channelID)
	if backend.claimErr != nil {
		return store.GossipPublicationClaimResult{}, backend.claimErr
	}
	return backend.leases[channelID], nil
}

func (backend *publicationWorkerBackend) mark(_ context.Context,
	fence store.GossipPublicationFence, _ time.Time,
) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.marked = append(backend.marked, fence)
	return backend.markErr
}

func (backend *publicationWorkerBackend) retry(_ context.Context,
	fence store.GossipPublicationFence, at, next time.Time, diagnostic string,
) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.retried = append(backend.retried, publicationWorkerRetry{
		fence: fence, at: at, next: next, diagnostic: diagnostic})
	return backend.retryErr
}

type publicationWorkerSessions struct {
	values map[model.ChannelID]*publicationWorkerSession
	errors map[model.ChannelID]error
}

func (sessions *publicationWorkerSessions) session(channelID model.ChannelID) (gossipPublicationSession, error) {
	if err := sessions.errors[channelID]; err != nil {
		return nil, err
	}
	session := sessions.values[channelID]
	if session == nil {
		return nil, errors.New("missing publication session")
	}
	return session, nil
}

type publicationWorkerSession struct {
	mu           sync.Mutex
	publications []model.SignedPublication
	err          error
}

func (session *publicationWorkerSession) Publish(_ context.Context,
	publication model.SignedPublication,
) error {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.publications = append(session.publications, publication)
	return session.err
}

func newPublicationWorkerForTest(t *testing.T, backend gossipPublicationBackend,
	sessions gossipPublicationSessions, at time.Time,
) *GossipPublicationWorker {
	t.Helper()
	worker, err := newGossipPublicationWorker(backend, sessions,
		publicationWorkerClock{at: at}, 10*time.Millisecond, "publication-test-owner")
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func publicationWorkerLease(t *testing.T, seed string,
	at time.Time,
) store.GossipPublicationClaimResult {
	t.Helper()
	local := testAuthorityPeer(t, "publication-worker-local-"+seed)
	remote := testAuthorityPeer(t, "publication-worker-remote-"+seed)
	channel := testAuthorityChannel(t, "publication-worker-channel-"+seed,
		model.BindingActive, local, remote)
	publication := testPeerPublication(t, channel, local, remote, "publish "+seed)
	leaseUntil := at.Add(gossipPublicationLease)
	record, err := model.NewPublicationRecord(model.PublicationRecordSpec{
		Publication: publication, Status: model.PublicationLeased, Attempts: 1,
		NextAttemptAt: at, LeaseOwner: "publication-test-owner", LeaseUntil: &leaseUntil,
		CreatedAt: at, UpdatedAt: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	return store.GossipPublicationClaimResult{Claimed: true, Lease: store.GossipPublicationLease{
		Record: record, Fence: store.GossipPublicationFence{EventID: publication.Event().ID(),
			ChannelID: channel.ChannelID, LeaseOwner: "publication-test-owner", Attempt: 1,
			LeaseUntil: leaseUntil, RosterHead: channel.RosterHead}}}
}

func waitForPublicationWorker(t *testing.T, worker *GossipPublicationWorker,
	state GossipPublicationWorkerState,
) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if worker.Snapshot().State == state {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("worker state = %q, want %q", worker.Snapshot().State, state)
}
