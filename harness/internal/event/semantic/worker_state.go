package semantic

import "time"

func (worker *PeerInboxSemanticWorker) start() bool {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if worker.started {
		return false
	}
	worker.started = true
	worker.snapshot.State = PeerInboxSemanticWorkerRunning
	return true
}

func (worker *PeerInboxSemanticWorker) stop(failed *bool) {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if !*failed {
		worker.snapshot.State = PeerInboxSemanticWorkerStopped
	}
}

func (worker *PeerInboxSemanticWorker) fail() {
	worker.mu.Lock()
	worker.snapshot.State = PeerInboxSemanticWorkerFailed
	worker.mu.Unlock()
}

func (worker *PeerInboxSemanticWorker) recordCycle(at time.Time) {
	worker.mu.Lock()
	worker.snapshot.Cycles++
	worker.snapshot.LastCycleAt = at
	worker.mu.Unlock()
}

func (worker *PeerInboxSemanticWorker) recordClaim(start bool) {
	worker.mu.Lock()
	if start {
		worker.snapshot.Claims++
		worker.snapshot.InFlight++
		if worker.snapshot.InFlight > worker.snapshot.MaximumActive {
			worker.snapshot.MaximumActive = worker.snapshot.InFlight
		}
	} else {
		worker.snapshot.InFlight--
	}
	worker.mu.Unlock()
}

func (worker *PeerInboxSemanticWorker) recordCommit(result peerInboxSemanticWorkerCommit) {
	worker.mu.Lock()
	if result.changed {
		worker.snapshot.Committed++
	}
	if result.replayed {
		worker.snapshot.Replayed++
	}
	worker.mu.Unlock()
}

func (worker *PeerInboxSemanticWorker) recordRetry() {
	worker.mu.Lock()
	worker.snapshot.Retries++
	worker.mu.Unlock()
}

func (worker *PeerInboxSemanticWorker) recordStale() {
	worker.mu.Lock()
	worker.snapshot.Stale++
	worker.mu.Unlock()
}
