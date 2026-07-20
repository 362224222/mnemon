package node

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

const channelRuntimeTopicDiagnosticLimit = 256

type channelRuntimeTopicGeneration struct {
	channelID  model.ChannelID
	rosterHead model.RecordHead
}

type channelRuntimeTopicRetryState struct {
	generation channelRuntimeTopicGeneration
	attempts   uint32
	next       time.Time
	diagnostic string
}

func (runtime *ChannelRuntime) reconcileTopics(ctx context.Context,
	channels []store.ChannelMeshChannel, now time.Time,
) (int, bool, bool, error) {
	runtime.pruneTopicRetries(channels)
	active, allReady, rescan := 0, true, false
	for _, channel := range channels {
		value := channel.Channel()
		if value.Status() != model.ChannelActive {
			continue
		}
		active++
		ready, changed, err := runtime.reconcileTopic(ctx, value, now)
		if err != nil {
			return 0, false, false, err
		}
		allReady = allReady && ready
		rescan = rescan || changed
	}
	return active, allReady, rescan, nil
}

func (runtime *ChannelRuntime) reconcileTopic(ctx context.Context,
	channel model.Channel, now time.Time,
) (bool, bool, error) {
	state := channel.TopicState()
	if state == model.TopicJoined && runtime.transport.HasCurrentChannelTopic(channel.ID()) {
		delete(runtime.topicRetries, channel.ID())
		return true, false, nil
	}
	if !runtime.topicRetryDue(channel, now) {
		return false, false, nil
	}
	if state == model.TopicNotJoined || state == model.TopicJoined {
		changed, err := runtime.setTopicState(ctx, channel, state, model.TopicJoining, now)
		if err != nil || !changed {
			return false, !changed && err == nil, err
		}
		state = model.TopicJoining
	}
	if state != model.TopicJoining {
		return false, false, fmt.Errorf("%w: active Channel %q has non-runtime topic state %q",
			ErrChannelRuntime, channel.ID().String(), state)
	}
	requestCtx, cancel := context.WithTimeout(ctx, runtime.requestTimeout)
	err := runtime.transport.EnsureChannelTopic(requestCtx, channel.ID())
	cancel()
	if ctx.Err() != nil {
		return false, false, nil
	}
	if err != nil {
		return runtime.settleTopicFailure(ctx, channel, now, err)
	}
	if !runtime.transport.HasCurrentChannelTopic(channel.ID()) {
		return runtime.settleTopicFailure(ctx, channel, now,
			errors.New("transport returned without a current topic session"))
	}
	joined, err := runtime.setTopicState(ctx, channel, model.TopicJoining, model.TopicJoined, now)
	if joined {
		delete(runtime.topicRetries, channel.ID())
	}
	return joined, err == nil, err
}

func (runtime *ChannelRuntime) settleTopicFailure(ctx context.Context, channel model.Channel,
	startedAt time.Time, cause error,
) (bool, bool, error) {
	rolledBack, err := runtime.setTopicState(ctx, channel,
		model.TopicJoining, model.TopicNotJoined, startedAt)
	if err != nil {
		return false, false, err
	}
	if !rolledBack {
		return false, true, nil
	}
	if !channelRuntimeRetryableTopicFailure(cause) {
		return false, false, fmt.Errorf("%w: ensure Channel %q topic: %w",
			ErrChannelRuntime, channel.ID().String(), cause)
	}
	completedAt, err := channelRuntimeNow(runtime.clock)
	if err != nil {
		return false, false, err
	}
	if completedAt.Before(startedAt) {
		return false, false, fmt.Errorf("%w: trusted clock regressed during topic attempt",
			ErrChannelRuntime)
	}
	runtime.recordTopicRetry(channel, cause, completedAt)
	return false, false, nil
}

func channelRuntimeRetryableTopicFailure(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, peer.ErrGossipTransitionInProgress) ||
		errors.Is(err, peer.ErrMeshAuthorityTransitionInProgress)
}

func (runtime *ChannelRuntime) setTopicState(ctx context.Context, channel model.Channel,
	expected, next model.TopicState, now time.Time,
) (bool, error) {
	result, err := runtime.store.CompareAndSetChannelTopicState(ctx,
		store.CompareAndSetChannelTopicStateSpec{ChannelID: channel.ID(),
			ExpectedStatus: model.ChannelActive, ExpectedRosterHead: channel.RosterHead(),
			ExpectedTopicState: expected, TopicState: next, At: now})
	if errors.Is(err, store.ErrChannelRuntimeConflict) ||
		errors.Is(err, store.ErrChannelRuntimeAuthority) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("%w: set Channel %q topic state: %w",
			ErrChannelRuntime, channel.ID().String(), err)
	}
	if result.Topic.ChannelID != channel.ID() || result.Topic.Status != model.ChannelActive ||
		result.Topic.RosterHead != channel.RosterHead() || result.Topic.TopicState != next {
		return false, fmt.Errorf("%w: Store returned an unfenced topic transition", ErrChannelRuntime)
	}
	return true, nil
}

func (runtime *ChannelRuntime) pruneTopicRetries(channels []store.ChannelMeshChannel) {
	current := make(map[model.ChannelID]model.RecordHead, len(channels))
	for _, channel := range channels {
		if channel.Channel().Status() == model.ChannelActive {
			current[channel.Channel().ID()] = channel.Roster().Head()
		}
	}
	for channelID, retry := range runtime.topicRetries {
		head, active := current[channelID]
		if !active || head != retry.generation.rosterHead {
			delete(runtime.topicRetries, channelID)
		}
	}
}

func (runtime *ChannelRuntime) topicRetryDue(channel model.Channel, now time.Time) bool {
	retry, present := runtime.topicRetries[channel.ID()]
	generation := channelRuntimeTopicGeneration{channelID: channel.ID(), rosterHead: channel.RosterHead()}
	if !present || retry.generation != generation {
		delete(runtime.topicRetries, channel.ID())
		return true
	}
	return retry.next.IsZero() || !now.Before(retry.next)
}

func (runtime *ChannelRuntime) recordTopicRetry(channel model.Channel, cause error, now time.Time) {
	generation := channelRuntimeTopicGeneration{channelID: channel.ID(), rosterHead: channel.RosterHead()}
	retry := runtime.topicRetries[channel.ID()]
	if retry.generation != generation {
		retry = channelRuntimeTopicRetryState{generation: generation}
	}
	if retry.attempts < ^uint32(0) {
		retry.attempts++
	}
	delayGeneration := channelRuntimeTargetGeneration{channelID: generation.channelID,
		rosterHead: generation.rosterHead}
	retry.next = now.Add(runtime.backoff.retryDelay(delayGeneration, retry.attempts-1, 0))
	retry.diagnostic = channelRuntimeTopicDiagnostic(cause)
	runtime.topicRetries[channel.ID()] = retry
}

func channelRuntimeTopicDiagnostic(err error) string {
	if err == nil {
		return ""
	}
	value := []rune(err.Error())
	if len(value) > channelRuntimeTopicDiagnosticLimit {
		value = value[:channelRuntimeTopicDiagnosticLimit]
	}
	return string(value)
}

func (runtime *ChannelRuntime) topicRetrySnapshot() []ChannelRuntimeTopicRetry {
	result := make([]ChannelRuntimeTopicRetry, 0, len(runtime.topicRetries))
	for _, retry := range runtime.topicRetries {
		result = append(result, ChannelRuntimeTopicRetry{ChannelID: retry.generation.channelID,
			RosterHead: retry.generation.rosterHead, Attempts: retry.attempts,
			RetryAt: retry.next, Diagnostic: retry.diagnostic})
	}
	sort.Slice(result, func(left, right int) bool {
		return result[left].ChannelID.String() < result[right].ChannelID.String()
	})
	return result
}
