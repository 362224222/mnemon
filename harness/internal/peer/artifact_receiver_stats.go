package peer

import "time"

func (receiver *ArtifactReceiver) recordCycle(at time.Time) {
	receiver.mu.Lock()
	receiver.snapshot.Cycles++
	receiver.snapshot.LastCycleAt = at
	receiver.mu.Unlock()
}

func (receiver *ArtifactReceiver) recordClaimScan() {
	receiver.mu.Lock()
	receiver.snapshot.ClaimScans++
	receiver.mu.Unlock()
}

func (receiver *ArtifactReceiver) claimStarted() {
	receiver.mu.Lock()
	receiver.snapshot.Claims++
	receiver.snapshot.InFlightClaims++
	if receiver.snapshot.InFlightClaims > receiver.snapshot.MaximumInFlightClaims {
		receiver.snapshot.MaximumInFlightClaims = receiver.snapshot.InFlightClaims
	}
	receiver.mu.Unlock()
}

func (receiver *ArtifactReceiver) claimFinished() {
	receiver.mu.Lock()
	receiver.snapshot.InFlightClaims--
	receiver.mu.Unlock()
}

func (receiver *ArtifactReceiver) pullStarted() {
	receiver.mu.Lock()
	receiver.snapshot.InFlightPulls++
	if receiver.snapshot.InFlightPulls > receiver.snapshot.MaximumInFlightPulls {
		receiver.snapshot.MaximumInFlightPulls = receiver.snapshot.InFlightPulls
	}
	receiver.mu.Unlock()
}

func (receiver *ArtifactReceiver) pullFinished() {
	receiver.mu.Lock()
	receiver.snapshot.InFlightPulls--
	receiver.mu.Unlock()
}

func (receiver *ArtifactReceiver) closureStarted() {
	receiver.mu.Lock()
	receiver.snapshot.InClosureBuild++
	if receiver.snapshot.InClosureBuild > receiver.snapshot.MaximumInClosureBuild {
		receiver.snapshot.MaximumInClosureBuild = receiver.snapshot.InClosureBuild
	}
	receiver.mu.Unlock()
}

func (receiver *ArtifactReceiver) closureFinished() {
	receiver.mu.Lock()
	receiver.snapshot.InClosureBuild--
	receiver.mu.Unlock()
}

func (receiver *ArtifactReceiver) recordRenewal() {
	receiver.mu.Lock()
	receiver.snapshot.Renewals++
	receiver.mu.Unlock()
}

func (receiver *ArtifactReceiver) recordManifestCacheHit() {
	receiver.mu.Lock()
	receiver.snapshot.ManifestCacheHits++
	receiver.mu.Unlock()
}

func (receiver *ArtifactReceiver) recordManifestPull() {
	receiver.mu.Lock()
	receiver.snapshot.ManifestPulls++
	receiver.mu.Unlock()
}

func (receiver *ArtifactReceiver) recordBlockCacheHit() {
	receiver.mu.Lock()
	receiver.snapshot.BlockCacheHits++
	receiver.mu.Unlock()
}

func (receiver *ArtifactReceiver) recordBlockPull() {
	receiver.mu.Lock()
	receiver.snapshot.BlockPulls++
	receiver.mu.Unlock()
}

func (receiver *ArtifactReceiver) recordCheckpoint() {
	receiver.mu.Lock()
	receiver.snapshot.Checkpoints++
	receiver.mu.Unlock()
}

func (receiver *ArtifactReceiver) recordReady() {
	receiver.mu.Lock()
	receiver.snapshot.Ready++
	receiver.mu.Unlock()
}

func (receiver *ArtifactReceiver) recordRetry() {
	receiver.mu.Lock()
	receiver.snapshot.Retries++
	receiver.mu.Unlock()
}

func (receiver *ArtifactReceiver) recordQuarantine() {
	receiver.mu.Lock()
	receiver.snapshot.Quarantines++
	receiver.mu.Unlock()
}

func (receiver *ArtifactReceiver) recordStale() {
	receiver.mu.Lock()
	receiver.snapshot.StaleClaims++
	receiver.mu.Unlock()
}

func (receiver *ArtifactReceiver) recordReconciliation(success bool) {
	receiver.mu.Lock()
	receiver.snapshot.Reconciliations++
	if !success {
		receiver.snapshot.ReconciliationFailures++
	}
	receiver.mu.Unlock()
}

func (receiver *ArtifactReceiver) fail(code ArtifactReceiverFatalCode) {
	receiver.mu.Lock()
	receiver.snapshot.State = ArtifactReceiverFailed
	receiver.snapshot.FatalCode = code
	receiver.mu.Unlock()
}
