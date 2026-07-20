package node

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestChannelRuntimeTopicReconciliationUsesExactCASSequence(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 20, 3, 0, 0, 0, time.UTC)
	channel := channelRuntimeTopicChannel(t, "topic-sequence", model.TopicNotJoined, now)
	st := &channelRuntimeStoreStub{}
	st.compare = func(spec store.CompareAndSetChannelTopicStateSpec) (store.CompareAndSetChannelTopicStateResult, error) {
		st.mu.Lock()
		st.topicSpecs = append(st.topicSpecs, spec)
		st.mu.Unlock()
		return channelRuntimeTopicResult(channel, spec.TopicState, spec.At), nil
	}
	transport := &channelRuntimeTransportStub{}
	transport.ensure = func(model.ChannelID) error {
		transport.mu.Lock()
		transport.current = true
		transport.mu.Unlock()
		return nil
	}
	runtime := channelRuntimeWithStubs(t, st, transport, channelRuntimeNoopAuthority{}, now)
	ready, rescan, err := runtime.reconcileTopic(context.Background(), channel, now)
	if err != nil || !ready || !rescan {
		t.Fatalf("topic reconciliation = (%t,%t,%v)", ready, rescan, err)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.topicSpecs) != 2 ||
		st.topicSpecs[0].ExpectedTopicState != model.TopicNotJoined ||
		st.topicSpecs[0].TopicState != model.TopicJoining ||
		st.topicSpecs[1].ExpectedTopicState != model.TopicJoining ||
		st.topicSpecs[1].TopicState != model.TopicJoined ||
		st.topicSpecs[1].ExpectedRosterHead != channel.RosterHead() {
		t.Fatalf("topic CAS sequence = %#v", st.topicSpecs)
	}
}

func TestChannelRuntimeTopicCASRaceNeverMarksJoined(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 20, 3, 30, 0, 0, time.UTC)
	channel := channelRuntimeTopicChannel(t, "topic-race", model.TopicJoining, now)
	st := &channelRuntimeStoreStub{compare: func(store.CompareAndSetChannelTopicStateSpec) (
		store.CompareAndSetChannelTopicStateResult, error,
	) {
		return store.CompareAndSetChannelTopicStateResult{}, store.ErrChannelRuntimeConflict
	}}
	transport := &channelRuntimeTransportStub{current: true}
	runtime := channelRuntimeWithStubs(t, st, transport, channelRuntimeNoopAuthority{}, now)
	ready, rescan, err := runtime.reconcileTopic(context.Background(), channel, now)
	if err != nil || ready || !rescan {
		t.Fatalf("racing topic reconciliation = (%t,%t,%v)", ready, rescan, err)
	}
}

func TestChannelRuntimeTopicTransientFailureBacksOffAndRecovers(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 20, 4, 0, 0, 0, time.UTC)
	joining := channelRuntimeTopicChannel(t, "topic-retry", model.TopicJoining, now)
	notJoined := channelRuntimeTopicState(t, joining, model.TopicNotJoined, now)
	st := &channelRuntimeStoreStub{}
	st.compare = func(spec store.CompareAndSetChannelTopicStateSpec) (store.CompareAndSetChannelTopicStateResult, error) {
		st.mu.Lock()
		st.topicSpecs = append(st.topicSpecs, spec)
		st.mu.Unlock()
		return channelRuntimeTopicResult(joining, spec.TopicState, spec.At), nil
	}
	transport := &channelRuntimeTransportStub{}
	attempts := 0
	transport.ensure = func(model.ChannelID) error {
		attempts++
		if attempts == 1 {
			return context.DeadlineExceeded
		}
		transport.mu.Lock()
		transport.current = true
		transport.mu.Unlock()
		return nil
	}
	runtime := channelRuntimeWithStubs(t, st, transport, channelRuntimeNoopAuthority{}, now)
	ready, rescan, err := runtime.reconcileTopic(context.Background(), joining, now)
	if ready || rescan || err != nil || attempts != 1 {
		t.Fatalf("first topic attempt = (%t,%t,%v), attempts %d", ready, rescan, err, attempts)
	}
	retry := runtime.topicRetries[joining.ID()]
	if retry.attempts != 1 || !retry.next.After(now) || retry.diagnostic == "" {
		t.Fatalf("topic retry = %#v", retry)
	}
	if ready, _, err := runtime.reconcileTopic(context.Background(), notJoined,
		retry.next.Add(-time.Nanosecond)); ready || err != nil || attempts != 1 {
		t.Fatalf("early topic retry = (%t,%v), attempts %d", ready, err, attempts)
	}
	runtime.clock = channelRuntimeFixedClock{now: retry.next}
	ready, rescan, err = runtime.reconcileTopic(context.Background(), notJoined, retry.next)
	if err != nil || !ready || !rescan || attempts != 2 {
		t.Fatalf("recovered topic = (%t,%t,%v), attempts %d", ready, rescan, err, attempts)
	}
	if _, present := runtime.topicRetries[joining.ID()]; present {
		t.Fatal("recovered topic retained retry state")
	}
}

func TestChannelRuntimeTopicPersistentTransientFailureIsBoundedAndDiagnostic(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 20, 4, 30, 0, 0, time.UTC)
	channel := channelRuntimeTopicChannel(t, "topic-persistent", model.TopicJoining, now)
	st := &channelRuntimeStoreStub{compare: func(spec store.CompareAndSetChannelTopicStateSpec) (
		store.CompareAndSetChannelTopicStateResult, error,
	) {
		return channelRuntimeTopicResult(channel, spec.TopicState, spec.At), nil
	}}
	transport := &channelRuntimeTransportStub{ensure: func(model.ChannelID) error {
		return fmtTopicRetryError(strings.Repeat("x", channelRuntimeTopicDiagnosticLimit+32))
	}}
	runtime := channelRuntimeWithStubs(t, st, transport, channelRuntimeNoopAuthority{}, now)
	if _, _, err := runtime.reconcileTopic(context.Background(), channel, now); err != nil {
		t.Fatal(err)
	}
	first := runtime.topicRetries[channel.ID()]
	notJoined := channelRuntimeTopicState(t, channel, model.TopicNotJoined, first.next)
	runtime.clock = channelRuntimeFixedClock{now: first.next}
	if _, _, err := runtime.reconcileTopic(context.Background(), notJoined, first.next); err != nil {
		t.Fatal(err)
	}
	second := runtime.topicRetries[channel.ID()]
	if second.attempts != 2 || !second.next.After(first.next) ||
		len([]rune(second.diagnostic)) != channelRuntimeTopicDiagnosticLimit ||
		second.next.Sub(first.next) > channelRuntimeRetryMaximum {
		t.Fatalf("persistent topic retry = first %#v, second %#v", first, second)
	}
}

func TestChannelRuntimeTopicPermanentFailureAndCancellationFailClosed(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 20, 4, 45, 0, 0, time.UTC)
	channel := channelRuntimeTopicChannel(t, "topic-closed", model.TopicJoining, now)
	st := &channelRuntimeStoreStub{compare: func(spec store.CompareAndSetChannelTopicStateSpec) (
		store.CompareAndSetChannelTopicStateResult, error,
	) {
		return channelRuntimeTopicResult(channel, spec.TopicState, spec.At), nil
	}}
	permanent := &channelRuntimeTransportStub{ensure: func(model.ChannelID) error {
		return errors.New("invalid local topic contract")
	}}
	runtime := channelRuntimeWithStubs(t, st, permanent, channelRuntimeNoopAuthority{}, now)
	if _, _, err := runtime.reconcileTopic(context.Background(), channel, now); !errors.Is(err, ErrChannelRuntime) {
		t.Fatalf("permanent topic error = %v", err)
	}
	if len(runtime.topicRetries) != 0 {
		t.Fatalf("permanent topic retry state = %#v", runtime.topicRetries)
	}

	cancelled := &channelRuntimeTransportStub{ensure: func(model.ChannelID) error {
		return context.Canceled
	}}
	runtime = channelRuntimeWithStubs(t, st, cancelled, channelRuntimeNoopAuthority{}, now)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := runtime.reconcileTopic(ctx, channel, now); err != nil {
		t.Fatalf("cancelled topic error = %v", err)
	}
	if len(runtime.topicRetries) != 0 {
		t.Fatalf("cancelled topic retry state = %#v", runtime.topicRetries)
	}
}

func TestChannelRuntimeTopicRetrySnapshotIsSortedAndDefensive(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 20, 4, 55, 0, 0, time.UTC)
	first := channelRuntimeTopicChannel(t, "topic-snapshot-first", model.TopicJoining, now)
	second := channelRuntimeTopicChannel(t, "topic-snapshot-second", model.TopicJoining, now)
	runtime := channelRuntimeWithStubs(t, &channelRuntimeStoreStub{},
		&channelRuntimeTransportStub{}, channelRuntimeNoopAuthority{}, now)
	runtime.recordTopicRetry(first, context.DeadlineExceeded, now)
	runtime.recordTopicRetry(second, context.DeadlineExceeded, now)
	runtime.recordCycle(now, channelRuntimeCycle{})

	snapshot := runtime.Snapshot()
	if len(snapshot.TopicRetries) != 2 ||
		snapshot.TopicRetries[0].ChannelID.String() >= snapshot.TopicRetries[1].ChannelID.String() {
		t.Fatalf("sorted topic retry snapshot = %#v", snapshot.TopicRetries)
	}
	snapshot.TopicRetries[0].Diagnostic = "mutated"
	if current := runtime.Snapshot(); current.TopicRetries[0].Diagnostic == "mutated" {
		t.Fatalf("snapshot mutation escaped defensive copy: %#v", current.TopicRetries)
	}
}

type fmtTopicRetryError string

func (err fmtTopicRetryError) Error() string { return string(err) }
func (fmtTopicRetryError) Is(target error) bool {
	return target == context.DeadlineExceeded
}

func channelRuntimeTopicChannel(t *testing.T, seed string, state model.TopicState,
	now time.Time,
) model.Channel {
	t.Helper()
	fixture := testkit.NewSignedChannelAt(t, seed, now.Add(-time.Minute))
	return channelRuntimeTopicState(t, fixture.Channel(), state, now)
}

func channelRuntimeTopicState(t *testing.T, base model.Channel, state model.TopicState,
	now time.Time,
) model.Channel {
	t.Helper()
	channel, err := model.NewChannel(model.ChannelSpec{Descriptor: base.Descriptor(),
		LocalAlias: base.LocalAlias(), RosterHead: base.RosterHead(), Status: model.ChannelActive,
		TopicState: state, UpdatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	return channel
}

func channelRuntimeTopicResult(channel model.Channel, state model.TopicState,
	now time.Time,
) store.CompareAndSetChannelTopicStateResult {
	return store.CompareAndSetChannelTopicStateResult{Changed: true,
		Topic: store.ChannelTopicProjection{ChannelID: channel.ID(), Status: model.ChannelActive,
			RosterHead: channel.RosterHead(), TopicState: state, UpdatedAt: now}}
}
