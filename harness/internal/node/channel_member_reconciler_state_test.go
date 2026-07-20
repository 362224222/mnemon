package node

import (
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestChannelMemberReconcilerStateBoundsBackoffAndCounters(t *testing.T) {
	worker := &ChannelMemberReconciler{schedules: make(map[channelMemberTargetKey]channelMemberSchedule)}
	key := channelMemberTargetKey{}
	at := time.Date(2026, 7, 21, 1, 0, 0, 0, time.UTC)
	worker.scheduleFailure(key, model.RecordHead{}, channelMemberSchedule{attempt: 20},
		channelMemberFailureRetryable, at)
	if got := worker.schedules[key].next.Sub(at); got != channelMemberRetryMaximum {
		t.Fatalf("retry delay = %v", got)
	}
	worker.recordInFlight(true)
	worker.recordHello()
	worker.recordSync()
	worker.recordMerge()
	worker.recordBaseline()
	worker.recordReachability(model.ReachabilityReachable)
	worker.recordReachability(model.ReachabilityUnreachable)
	worker.recordStale()
	worker.recordInFlight(false)
	snapshot := worker.Snapshot()
	if snapshot.MaximumInFlight != 1 || snapshot.InFlight != 0 || snapshot.Hellos != 1 ||
		snapshot.Syncs != 1 || snapshot.RosterMerges != 1 || snapshot.Baselines != 1 ||
		snapshot.Reachable != 1 || snapshot.Unreachable != 1 || snapshot.StaleSettlements != 1 ||
		snapshot.RetryableFailures != 1 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}
