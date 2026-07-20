package node

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestChannelRuntimeBackoffIsDeterministicBoundedAndSaturating(t *testing.T) {
	t.Parallel()
	target, _ := channelRuntimeTestTarget(t, "retry-bounds", model.BindingPending, false, false)
	policy := deterministicChannelRuntimeBackoff{}
	first := policy.retryDelay(target.generation, 0, 0)
	if first < channelRuntimeRetryInitial || first > channelRuntimeRetryInitial+channelRuntimeRetryInitial/4 ||
		policy.retryDelay(target.generation, 0, 0) != first {
		t.Fatalf("initial deterministic backoff = %s", first)
	}
	if got := policy.retryDelay(target.generation, ^uint32(0), time.Hour); got != channelRuntimeRetryMaximum {
		t.Fatalf("maximum backoff = %s", got)
	}
	now := time.Date(2026, 7, 20, 7, 0, 0, 0, time.UTC)
	runtime := &ChannelRuntime{period: time.Second, backoff: policy,
		retries: map[channelRuntimeTargetKey]channelRuntimeRetryState{target.key: {
			generation: target.generation, attempts: ^uint32(0)}}}
	runtime.applyTargetResults([]channelRuntimeTargetResult{target.retry(0)}, now)
	state := runtime.retries[target.key]
	if state.attempts != ^uint32(0) || !state.next.After(now) || state.permanent {
		t.Fatalf("saturated retry state = %#v", state)
	}
	runtime.applyTargetResults([]channelRuntimeTargetResult{target.permanent()}, now)
	if state = runtime.retries[target.key]; !state.permanent || state.attempts != 0 {
		t.Fatalf("permanent retry state = %#v", state)
	}
	target.generation.bindingState = model.BindingActive
	if due := runtime.dueTargets([]channelRuntimeTarget{target}, now); len(due) != 1 ||
		runtime.retries[target.key].permanent {
		t.Fatalf("generation-fenced due targets = %#v, state=%#v", due, runtime.retries[target.key])
	}
}

func TestChannelRuntimeCancelledOwnerNeverConsumesBufferedTarget(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 20, 7, 30, 0, 0, time.UTC)
	target, _ := channelRuntimeTestTarget(t, "retry-cancel", model.BindingPending, false, false)
	var calls atomic.Int32
	transport := &channelRuntimeTransportStub{hello: func(context.Context, model.PeerID,
		peer.MemberHello,
	) (peer.MemberHelloAck, error) {
		calls.Add(1)
		return peer.MemberHelloAck{}, peer.ErrChannelMemberClientTransport
	}}
	runtime := channelRuntimeWithStubs(t, &channelRuntimeStoreStub{}, transport,
		channelRuntimeNoopAuthority{}, now)
	jobs := make(chan channelRuntimeTarget, 1)
	jobs <- target
	close(jobs)
	results := make(chan channelRuntimeTargetResult, 1)
	ownerCtx, ownerCancel := context.WithCancel(context.Background())
	ownerCancel()
	runtime.runTargetJobs(ownerCtx, func() {}, jobs, results, now)
	if calls.Load() != 0 || len(results) != 0 {
		t.Fatalf("cancelled owner consumed work: calls=%d results=%d", calls.Load(), len(results))
	}
}

func TestChannelRuntimeFanoutUsesHermeticProtocolStreamBound(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 20, 8, 0, 0, 0, time.UTC)
	target, _ := channelRuntimeTestTarget(t, "retry-fanout", model.BindingPending, false, false)
	release := make(chan struct{})
	started := make(chan struct{}, 16)
	var active, maximum atomic.Int32
	transport := &channelRuntimeTransportStub{hello: func(context.Context, model.PeerID,
		peer.MemberHello,
	) (peer.MemberHelloAck, error) {
		current := active.Add(1)
		for prior := maximum.Load(); current > prior && !maximum.CompareAndSwap(prior, current); {
			prior = maximum.Load()
		}
		started <- struct{}{}
		<-release
		active.Add(-1)
		return peer.MemberHelloAck{}, peer.ErrMeshTransport
	}}
	runtime := channelRuntimeWithStubs(t, &channelRuntimeStoreStub{}, transport,
		channelRuntimeNoopAuthority{}, now)
	targets := make([]channelRuntimeTarget, 16)
	for index := range targets {
		targets[index] = target
	}
	done := make(chan []channelRuntimeTargetResult, 1)
	go func() { done <- runtime.runTargetFanout(context.Background(), targets, now) }()
	for index := 0; index < runtime.maximumFanout; index++ {
		channelRuntimeReceive(t, started)
	}
	if runtime.maximumFanout != peer.HermeticLimits().ApplicationProtocolStreams ||
		maximum.Load() > int32(runtime.maximumFanout) {
		t.Fatalf("fanout authority = configured %d, observed %d",
			runtime.maximumFanout, maximum.Load())
	}
	close(release)
	channelRuntimeReceive(t, done)
}

func TestChannelRuntimeNextDelayIncludesTopicRetryDeadline(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 20, 8, 30, 0, 0, time.UTC)
	runtime := &ChannelRuntime{period: time.Second,
		topicRetries: map[model.ChannelID]channelRuntimeTopicRetryState{},
		retries:      map[channelRuntimeTargetKey]channelRuntimeRetryState{}}
	channelID := testkit.NewSignedChannelAt(t, "topic-retry-delay", now).Channel().ID()
	runtime.topicRetries[channelID] = channelRuntimeTopicRetryState{next: now.Add(300 * time.Millisecond)}
	if delay := runtime.nextDelay(now); delay != 300*time.Millisecond {
		t.Fatalf("topic retry delay = %s", delay)
	}
	runtime.topicRetries[channelID] = channelRuntimeTopicRetryState{next: now}
	if delay := runtime.nextDelay(now); delay != 0 {
		t.Fatalf("due topic retry delay = %s", delay)
	}
}
