package semantic

import (
	"sync"
	"testing"
	"time"
)

func TestPeerInboxSemanticWorkerStateSerializesLifecycleAndCounters(t *testing.T) {
	worker := &PeerInboxSemanticWorker{snapshot: PeerInboxSemanticWorkerSnapshot{
		State: PeerInboxSemanticWorkerIdle}}
	if !worker.start() || worker.start() {
		t.Fatal("one-use start transition was not atomic")
	}
	const calls = 32
	var wait sync.WaitGroup
	for index := 0; index < calls; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			worker.recordCycle(semanticWorkerTestTime())
			worker.recordClaim(true)
			worker.recordCommit(peerInboxSemanticWorkerCommit{changed: true, replayed: true})
			worker.recordRetry()
			worker.recordStale()
			worker.recordClaim(false)
		}()
	}
	wait.Wait()
	snapshot := worker.Snapshot()
	if snapshot.State != PeerInboxSemanticWorkerRunning || snapshot.Cycles != calls ||
		snapshot.Claims != calls || snapshot.Committed != calls || snapshot.Replayed != calls ||
		snapshot.Retries != calls || snapshot.Stale != calls || snapshot.InFlight != 0 ||
		snapshot.MaximumActive < 1 || snapshot.MaximumActive > calls ||
		!snapshot.LastCycleAt.Equal(semanticWorkerTestTime()) {
		t.Fatalf("concurrent snapshot = %#v", snapshot)
	}
	failed := false
	worker.stop(&failed)
	if state := worker.Snapshot().State; state != PeerInboxSemanticWorkerStopped {
		t.Fatalf("stopped state = %q", state)
	}
	worker.fail()
	if state := worker.Snapshot().State; state != PeerInboxSemanticWorkerFailed {
		t.Fatalf("failed state = %q", state)
	}
	if worker.Snapshot().LastCycleAt.Location() != time.UTC {
		t.Fatal("cycle time lost its canonical location")
	}
}
