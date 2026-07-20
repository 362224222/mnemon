package node

import (
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const channelMemberRetryMaximum = 10 * time.Second

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
	disposition channelMemberFailureDisposition, at time.Time,
) {
	schedule := channelMemberSchedule{head: head, attempt: prior.attempt + 1}
	worker.mu.Lock()
	if disposition == channelMemberFailurePermanent {
		schedule.permanent = true
		worker.snapshot.PermanentFailures++
	} else {
		delay := time.Second
		for attempt := uint8(1); attempt < schedule.attempt && delay < channelMemberRetryMaximum; attempt++ {
			delay *= 2
		}
		if delay > channelMemberRetryMaximum {
			delay = channelMemberRetryMaximum
		}
		schedule.next = at.Add(delay)
		worker.snapshot.RetryableFailures++
	}
	worker.mu.Unlock()
	worker.schedules[key] = schedule
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
