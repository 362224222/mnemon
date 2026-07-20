package node

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	channelRuntimeRetryInitial = 250 * time.Millisecond
	channelRuntimeRetryMaximum = 10 * time.Second
)

type channelRuntimeTargetKey struct {
	channelID model.ChannelID
	peerID    model.PeerID
}

type channelRuntimeTargetGeneration struct {
	channelID    model.ChannelID
	peerID       model.PeerID
	originEpoch  model.OriginEpoch
	rosterHead   model.RecordHead
	memberHead   model.RecordHead
	bindingState model.BindingState
}

type channelRuntimeRetryState struct {
	generation channelRuntimeTargetGeneration
	attempts   uint32
	next       time.Time
	permanent  bool
}

type channelRuntimeBackoff interface {
	retryDelay(channelRuntimeTargetGeneration, uint32, time.Duration) time.Duration
}

type deterministicChannelRuntimeBackoff struct{}

type channelRuntimeTimer interface {
	channel() <-chan time.Time
	stop()
}

type channelRuntimeTimerFactory interface {
	newTimer(time.Duration) channelRuntimeTimer
}

type wallChannelRuntimeTimerFactory struct{}
type wallChannelRuntimeTimer struct{ timer *time.Timer }

func (wallChannelRuntimeTimerFactory) newTimer(delay time.Duration) channelRuntimeTimer {
	return &wallChannelRuntimeTimer{timer: time.NewTimer(delay)}
}

func (timer *wallChannelRuntimeTimer) channel() <-chan time.Time { return timer.timer.C }

func (timer *wallChannelRuntimeTimer) stop() {
	if timer == nil || timer.timer == nil {
		return
	}
	if !timer.timer.Stop() {
		select {
		case <-timer.timer.C:
		default:
		}
	}
}

func (deterministicChannelRuntimeBackoff) retryDelay(generation channelRuntimeTargetGeneration,
	attempt uint32, remote time.Duration,
) time.Duration {
	shift := attempt
	if shift > 6 {
		shift = 6
	}
	base := channelRuntimeRetryInitial * time.Duration(uint64(1)<<shift)
	if base > channelRuntimeRetryMaximum {
		base = channelRuntimeRetryMaximum
	}
	material := generation.channelID.String() + "\x00" + generation.peerID.String() + "\x00" +
		generation.originEpoch.String() + "\x00" + generation.rosterHead.Digest().String()
	digest := sha256.Sum256([]byte(material))
	window := base / 4
	if window > 0 {
		base += time.Duration(binary.BigEndian.Uint64(digest[:8]) % uint64(window+1))
	}
	if remote > base {
		base = remote
	}
	if base > channelRuntimeRetryMaximum {
		return channelRuntimeRetryMaximum
	}
	return base
}

func (runtime *ChannelRuntime) dueTargets(targets []channelRuntimeTarget,
	now time.Time,
) []channelRuntimeTarget {
	due := make([]channelRuntimeTarget, 0, len(targets))
	for _, target := range targets {
		state, present := runtime.retries[target.key]
		if !present || state.generation != target.generation {
			state = channelRuntimeRetryState{generation: target.generation}
			runtime.retries[target.key] = state
		}
		if state.permanent || (!state.next.IsZero() && now.Before(state.next)) {
			continue
		}
		due = append(due, target)
	}
	return due
}

func (runtime *ChannelRuntime) pruneRetryStates(targets []channelRuntimeTarget) {
	current := make(map[channelRuntimeTargetKey]channelRuntimeTargetGeneration, len(targets))
	for _, target := range targets {
		current[target.key] = target.generation
	}
	for key, state := range runtime.retries {
		generation, present := current[key]
		if !present || generation != state.generation {
			delete(runtime.retries, key)
		}
	}
}

func (runtime *ChannelRuntime) runTargetFanout(ctx context.Context,
	targets []channelRuntimeTarget, now time.Time,
) []channelRuntimeTargetResult {
	if len(targets) == 0 {
		return nil
	}
	workerCount := len(targets)
	if workerCount > runtime.maximumFanout {
		workerCount = runtime.maximumFanout
	}
	cycleCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan channelRuntimeTarget, len(targets))
	results := make(chan channelRuntimeTargetResult, len(targets))
	for _, target := range targets {
		jobs <- target
	}
	close(jobs)
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for index := 0; index < workerCount; index++ {
		go func() {
			defer workers.Done()
			runtime.runTargetJobs(cycleCtx, cancel, jobs, results, now)
		}()
	}
	workers.Wait()
	close(results)
	collected := make([]channelRuntimeTargetResult, 0, len(targets))
	for result := range results {
		collected = append(collected, result)
	}
	sort.Slice(collected, func(left, right int) bool {
		leftKey, rightKey := collected[left].target.key, collected[right].target.key
		if leftKey.channelID == rightKey.channelID {
			return leftKey.peerID.String() < rightKey.peerID.String()
		}
		return leftKey.channelID.String() < rightKey.channelID.String()
	})
	return collected
}

func (runtime *ChannelRuntime) runTargetJobs(ownerCtx context.Context, cancel context.CancelFunc,
	jobs <-chan channelRuntimeTarget, results chan<- channelRuntimeTargetResult, now time.Time,
) {
	for target := range jobs {
		if ownerCtx.Err() != nil {
			return
		}
		result := runtime.runTargetJob(ownerCtx, target, now)
		results <- result
		if result.disposition == channelRuntimeTargetFatal {
			cancel()
			return
		}
	}
}

func (runtime *ChannelRuntime) runTargetJob(ownerCtx context.Context,
	target channelRuntimeTarget, startedAt time.Time,
) channelRuntimeTargetResult {
	runtime.recordTargetCall(true)
	defer runtime.recordTargetCall(false)
	attemptCtx, cancel := context.WithTimeout(ownerCtx, runtime.requestTimeout)
	result := runtime.convergeTarget(ownerCtx, attemptCtx, target)
	cancel()
	completedAt, err := channelRuntimeNow(runtime.clock)
	if err == nil && completedAt.Before(startedAt) {
		err = errors.New("trusted clock regressed during Channel target attempt")
	}
	if err != nil {
		return target.fatal(err)
	}
	result.completedAt = completedAt
	return result
}

func (runtime *ChannelRuntime) applyTargetResults(results []channelRuntimeTargetResult,
	now time.Time,
) (error, bool) {
	var fatal error
	rescan := false
	for _, result := range results {
		settledAt := result.completedAt
		if settledAt.IsZero() {
			settledAt = now
		}
		state := runtime.retries[result.target.key]
		if state.generation != result.target.generation {
			continue
		}
		switch result.disposition {
		case channelRuntimeTargetConverged:
			state.attempts, state.permanent, state.next = 0, false,
				settledAt.Add(channelRuntimeHealthyPeriod)
		case channelRuntimeTargetPending:
			state.attempts, state.permanent, state.next = 0, false, settledAt.Add(runtime.period)
		case channelRuntimeTargetRetry:
			if state.attempts < ^uint32(0) {
				state.attempts++
			}
			state.permanent = false
			state.next = settledAt.Add(runtime.backoff.retryDelay(state.generation,
				state.attempts-1, result.retryAfter))
		case channelRuntimeTargetPermanent:
			state.attempts, state.permanent, state.next = 0, true, time.Time{}
		case channelRuntimeTargetRescan:
			state.next = settledAt
			rescan = true
		case channelRuntimeTargetCancelled:
			continue
		case channelRuntimeTargetFatal:
			if fatal == nil {
				fatal = result.err
			}
		default:
			if fatal == nil {
				fatal = errors.New("unknown Channel target disposition")
			}
		}
		runtime.retries[result.target.key] = state
	}
	return fatal, rescan
}

func (runtime *ChannelRuntime) nextDelay(now time.Time) time.Duration {
	delay := runtime.period
	for _, state := range runtime.topicRetries {
		delay = earlierChannelRuntimeDelay(delay, state.next, now)
		if delay == 0 {
			return 0
		}
	}
	for _, state := range runtime.retries {
		if state.permanent || state.next.IsZero() {
			continue
		}
		delay = earlierChannelRuntimeDelay(delay, state.next, now)
		if delay == 0 {
			return 0
		}
	}
	return delay
}

func earlierChannelRuntimeDelay(current time.Duration, next, now time.Time) time.Duration {
	if next.IsZero() {
		return current
	}
	candidate := next.Sub(now)
	if candidate <= 0 {
		return 0
	}
	if candidate < current {
		return candidate
	}
	return current
}

func (runtime *ChannelRuntime) recordTargetCall(start bool) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if start {
		runtime.snapshot.InFlight++
		if runtime.snapshot.InFlight > runtime.snapshot.MaximumInFlight {
			runtime.snapshot.MaximumInFlight = runtime.snapshot.InFlight
		}
		return
	}
	runtime.snapshot.InFlight--
}
