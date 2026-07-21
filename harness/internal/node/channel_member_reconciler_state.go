package node

import (
	"errors"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

const channelMemberRetryMaximum = 10 * time.Second

type channelMemberFailureDisposition uint8

const (
	channelMemberFailureFatal channelMemberFailureDisposition = iota
	channelMemberFailureRetryable
	channelMemberFailurePermanent
)

func classifyChannelMemberFailure(err error) channelMemberFailureDisposition {
	var remote *peer.ChannelProtocolFailure
	if errors.As(err, &remote) {
		if remote.Code() == peer.ChannelErrorNotMember || remote.Retryable() {
			return channelMemberFailureRetryable
		}
		return channelMemberFailurePermanent
	}
	if retryableChannelMemberFailure(err) {
		return channelMemberFailureRetryable
	}
	if permanentChannelMemberFailure(err) {
		return channelMemberFailurePermanent
	}
	return channelMemberFailureFatal
}

func retryableChannelMemberFailure(err error) bool {
	return errors.Is(err, peer.ErrChannelMemberClientTransport) ||
		errors.Is(err, peer.ErrChannelMemberBusy) || errors.Is(err, peer.ErrChannelMemberRosterGap) ||
		errors.Is(err, peer.ErrChannelMemberNotMember) ||
		errors.Is(err, store.ErrChannelBaselineAuthority) ||
		errors.Is(err, store.ErrChannelRuntimeAuthority) || errors.Is(err, store.ErrChannelRuntimeConflict) ||
		errors.Is(err, store.ErrChannelLeaveConflict) || errors.Is(err, store.ErrChannelLeaveAuthority)
}

func permanentChannelMemberFailure(err error) bool {
	return errors.Is(err, peer.ErrChannelMemberClientResponse) ||
		errors.Is(err, peer.ErrChannelMemberRevoked) || errors.Is(err, peer.ErrChannelMemberClosed) ||
		errors.Is(err, peer.ErrChannelMemberRosterConflict) ||
		errors.Is(err, peer.ErrChannelMemberBaselineConflict) ||
		errors.Is(err, peer.ErrChannelMemberEpochMismatch) ||
		errors.Is(err, store.ErrChannelLeaveInput) || errors.Is(err, store.ErrChannelBaselineConflict) ||
		errors.Is(err, store.ErrChannelBaselineEpochMismatch)
}

func channelMemberRetryDelay(attempt uint64) time.Duration {
	delay := time.Second
	for current := uint64(1); current < attempt && delay < channelMemberRetryMaximum; current++ {
		delay *= 2
	}
	if delay > channelMemberRetryMaximum {
		return channelMemberRetryMaximum
	}
	return delay
}

func (worker *ChannelMemberReconciler) finish(failed *bool) {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if failed == nil || !*failed {
		worker.snapshot.State = ChannelMemberReconcilerStopped
	}
}

func (worker *ChannelMemberReconciler) fail() {
	worker.mu.Lock()
	worker.snapshot.State = ChannelMemberReconcilerFailed
	worker.mu.Unlock()
}

func (worker *ChannelMemberReconciler) pruneSchedules(targets []channelMemberTarget) {
	current := make(map[channelMemberTargetKey]model.RecordHead, len(targets))
	for _, target := range targets {
		current[target.key()] = target.roster.Head()
	}
	for key, schedule := range worker.schedules {
		head, ok := current[key]
		if !ok || head != schedule.head {
			delete(worker.schedules, key)
		}
	}
}

func (worker *ChannelMemberReconciler) scheduleFailure(key channelMemberTargetKey,
	head model.RecordHead, prior channelMemberSchedule,
	disposition channelMemberFailureDisposition, at time.Time, cause error,
) {
	schedule := channelMemberSchedule{head: head, attempt: prior.attempt + 1}
	worker.mu.Lock()
	if cause != nil {
		worker.snapshot.LastFailure = cause.Error()
	}
	if disposition == channelMemberFailurePermanent {
		schedule.permanent = true
		worker.snapshot.PermanentFailures++
	} else {
		schedule.next = at.Add(channelMemberRetryDelay(uint64(schedule.attempt)))
		worker.snapshot.RetryableFailures++
	}
	worker.mu.Unlock()
	worker.schedules[key] = schedule
}

func (worker *ChannelMemberReconciler) recordDurableFailure(
	disposition channelMemberFailureDisposition, cause error,
) {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if cause != nil {
		worker.snapshot.LastFailure = cause.Error()
	}
	if disposition == channelMemberFailurePermanent {
		worker.snapshot.PermanentFailures++
	} else {
		worker.snapshot.RetryableFailures++
	}
}

func (worker *ChannelMemberReconciler) recordLeaveRequest() {
	worker.mu.Lock()
	worker.snapshot.LeaveRequests++
	worker.mu.Unlock()
}

func (worker *ChannelMemberReconciler) recordLeaveSettlement() {
	worker.mu.Lock()
	worker.snapshot.LeaveSettlements++
	worker.mu.Unlock()
}

func (worker *ChannelMemberReconciler) recordCycle(at time.Time, targets int) {
	worker.mu.Lock()
	worker.snapshot.Cycles++
	worker.snapshot.Targets += uint64(targets)
	worker.snapshot.LastCycleAt = at
	worker.mu.Unlock()
}

func (worker *ChannelMemberReconciler) recordInFlight(start bool) {
	worker.mu.Lock()
	if start {
		worker.snapshot.InFlight++
		if worker.snapshot.InFlight > worker.snapshot.MaximumInFlight {
			worker.snapshot.MaximumInFlight = worker.snapshot.InFlight
		}
	} else {
		worker.snapshot.InFlight--
	}
	worker.mu.Unlock()
}

func (worker *ChannelMemberReconciler) recordHello() {
	worker.mu.Lock()
	worker.snapshot.Hellos++
	worker.mu.Unlock()
}

func (worker *ChannelMemberReconciler) recordSync() {
	worker.mu.Lock()
	worker.snapshot.Syncs++
	worker.mu.Unlock()
}

func (worker *ChannelMemberReconciler) recordMerge() {
	worker.mu.Lock()
	worker.snapshot.RosterMerges++
	worker.mu.Unlock()
}

func (worker *ChannelMemberReconciler) recordBaseline() {
	worker.mu.Lock()
	worker.snapshot.Baselines++
	worker.mu.Unlock()
}

func (worker *ChannelMemberReconciler) recordReachability(reachability model.Reachability) {
	worker.mu.Lock()
	if reachability == model.ReachabilityReachable {
		worker.snapshot.Reachable++
	} else if reachability == model.ReachabilityUnreachable {
		worker.snapshot.Unreachable++
	}
	worker.mu.Unlock()
}

func (worker *ChannelMemberReconciler) recordStale() {
	worker.mu.Lock()
	worker.snapshot.StaleSettlements++
	worker.mu.Unlock()
}
