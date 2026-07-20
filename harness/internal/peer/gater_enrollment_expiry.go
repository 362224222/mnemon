package peer

import "time"

func (gater *ConnectionGater) outboundEnrollmentPermits() int {
	if gater == nil {
		return 0
	}
	gater.mu.Lock()
	callbacks := gater.pruneOutboundEnrollmentLocked(gater.now())
	count := len(gater.outbound.permits)
	gater.mu.Unlock()
	runEnrollmentPermitCallbacks(callbacks)
	return count
}

func (gater *ConnectionGater) UnknownEnrollmentSlots() int {
	if gater == nil {
		return 0
	}
	gater.mu.Lock()
	now := gater.now()
	gater.prunePendingLocked(now)
	callbacks := gater.pruneOutboundEnrollmentLocked(now)
	count := gater.enrollmentSlotsLocked()
	gater.mu.Unlock()
	runEnrollmentPermitCallbacks(callbacks)
	return count
}

func (gater *ConnectionGater) enrollmentSlotsLocked() int {
	return gater.pendingCount + len(gater.unknown) + len(gater.outbound.permits) +
		len(gater.outbound.resolving)
}

func (gater *ConnectionGater) pruneOutboundEnrollmentLocked(now time.Time) []outboundEnrollmentPermitRelease {
	callbacks := make([]outboundEnrollmentPermitRelease, 0)
	for key, permit := range gater.outbound.permits {
		if permit.expiresAt.After(now) {
			continue
		}
		delete(gater.outbound.permits, key)
		callbacks = append(callbacks, enrollmentPermitRelease(key, permit))
	}
	return callbacks
}

func (gater *ConnectionGater) nextOutboundEnrollmentExpiryLocked() time.Time {
	var next time.Time
	for _, permit := range gater.outbound.permits {
		if next.IsZero() || permit.expiresAt.Before(next) {
			next = permit.expiresAt
		}
	}
	return next
}

func (gater *ConnectionGater) retireOutboundEnrollmentLocked() []outboundEnrollmentPermitRelease {
	callbacks := make([]outboundEnrollmentPermitRelease, 0, len(gater.outbound.permits))
	for key, permit := range gater.outbound.permits {
		delete(gater.outbound.permits, key)
		callbacks = append(callbacks, enrollmentPermitRelease(key, permit))
	}
	return callbacks
}

func enrollmentPermitRelease(key outboundEnrollmentPermitKey,
	permit *outboundEnrollmentPermit,
) outboundEnrollmentPermitRelease {
	if permit == nil {
		return outboundEnrollmentPermitRelease{}
	}
	return outboundEnrollmentPermitRelease{
		ref:         outboundEnrollmentPermitRef{key: key, generation: permit.generation},
		resetStream: permit.resetStream, callback: permit.onRelease,
	}
}

func runEnrollmentPermitCallbacks(callbacks []outboundEnrollmentPermitRelease) {
	for _, release := range callbacks {
		var resetErr error
		if release.resetStream != nil {
			resetErr = release.resetStream()
		}
		if release.callback != nil {
			release.callback(release.ref, resetErr)
		}
	}
}
