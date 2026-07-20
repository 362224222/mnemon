package node

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type channelRuntimeCycle struct {
	activeTopics     int
	targets          int
	readyTargets     int
	retryingTargets  int
	permanentTargets int
	localTopicsReady bool
	fullyConverged   bool
	planValid        bool
	immediateRescan  bool
}

type channelRuntimeTarget struct {
	key        channelRuntimeTargetKey
	generation channelRuntimeTargetGeneration
	roster     model.VerifiedRoster
	local      model.Member
	binding    model.PeerBinding
	readiness  store.ChannelPeerReadiness
}

func (runtime *ChannelRuntime) runCycle(ctx context.Context,
	now time.Time,
) (channelRuntimeCycle, error) {
	mesh, err := runtime.store.ReadChannelMeshAuthority(ctx)
	if err != nil {
		return channelRuntimeCycle{}, fmt.Errorf("%w: read mesh authority: %w", ErrChannelRuntime, err)
	}
	channels, err := channelRuntimeChannels(mesh)
	if err != nil {
		return channelRuntimeCycle{}, err
	}
	topicCount, topicsReady, topicRescan, err := runtime.reconcileTopics(ctx, channels, now)
	if err != nil {
		return channelRuntimeCycle{}, err
	}
	if topicsReady {
		runtime.publishLocalTopicsReady(topicCount, now)
	}
	if topicRescan {
		return channelRuntimeCycle{activeTopics: topicCount,
			localTopicsReady: topicsReady, immediateRescan: true}, nil
	}
	targets, stale, err := runtime.planTargets(ctx, mesh.LocalPeerID(), channels)
	if err != nil {
		return channelRuntimeCycle{}, err
	}
	if stale {
		return channelRuntimeCycle{activeTopics: topicCount,
			localTopicsReady: topicsReady, immediateRescan: true}, nil
	}
	runtime.pruneRetryStates(targets)
	due := runtime.dueTargets(targets, now)
	results := runtime.runTargetFanout(ctx, due, now)
	fatal, immediateRescan := runtime.applyTargetResults(results, now)
	cycle := runtime.describeCycle(topicCount, topicsReady, targets)
	cycle.immediateRescan = immediateRescan
	if fatal != nil {
		return cycle, fmt.Errorf("%w: peer convergence: %w", ErrChannelRuntime, fatal)
	}
	return cycle, nil
}

func channelRuntimeChannels(mesh store.ChannelMeshAuthority) ([]store.ChannelMeshChannel, error) {
	if mesh.LocalPeerID().IsZero() {
		return nil, fmt.Errorf("%w: mesh authority has no local PeerID", ErrChannelRuntime)
	}
	channels := mesh.Channels()
	if len(channels) > model.MaxChannelsPerNode {
		return nil, fmt.Errorf("%w: mesh authority exceeds Channel bound", ErrChannelRuntime)
	}
	sort.Slice(channels, func(left, right int) bool {
		return channels[left].Channel().ID().String() < channels[right].Channel().ID().String()
	})
	for index, channel := range channels {
		value := channel.Channel()
		if value.ID().IsZero() || channel.Roster().IsZero() || value.RosterHead() != channel.Roster().Head() {
			return nil, fmt.Errorf("%w: mesh authority contains an incomplete Channel", ErrChannelRuntime)
		}
		if index > 0 && channels[index-1].Channel().ID() == value.ID() {
			return nil, fmt.Errorf("%w: mesh authority repeats a Channel", ErrChannelRuntime)
		}
	}
	return channels, nil
}

func (runtime *ChannelRuntime) planTargets(ctx context.Context, localPeer model.PeerID,
	channels []store.ChannelMeshChannel,
) ([]channelRuntimeTarget, bool, error) {
	targets := make([]channelRuntimeTarget, 0, channelRuntimeMaximumTargets)
	for _, channel := range channels {
		value := channel.Channel()
		if value.Status() != model.ChannelActive || value.TopicState() != model.TopicJoined ||
			!runtime.transport.HasCurrentChannelTopic(value.ID()) {
			continue
		}
		planned, stale, err := runtime.planChannelTargets(ctx, localPeer, channel)
		if err != nil || stale {
			return nil, stale, err
		}
		targets = append(targets, planned...)
	}
	if len(targets) > channelRuntimeMaximumTargets {
		return nil, false, fmt.Errorf("%w: target plan exceeds Node bound", ErrChannelRuntime)
	}
	sort.Slice(targets, func(left, right int) bool {
		if targets[left].key.channelID == targets[right].key.channelID {
			return targets[left].key.peerID.String() < targets[right].key.peerID.String()
		}
		return targets[left].key.channelID.String() < targets[right].key.channelID.String()
	})
	for index := 1; index < len(targets); index++ {
		if targets[index-1].key == targets[index].key {
			return nil, false, fmt.Errorf("%w: target plan contains a duplicate", ErrChannelRuntime)
		}
	}
	return targets, false, nil
}

func (runtime *ChannelRuntime) planChannelTargets(ctx context.Context, localPeer model.PeerID,
	channel store.ChannelMeshChannel,
) ([]channelRuntimeTarget, bool, error) {
	roster, bindings := channel.Roster(), channel.Bindings()
	local, ok := roster.CurrentMember(localPeer)
	if !ok || local.Status() != model.MemberActive {
		return nil, false, fmt.Errorf("%w: active Channel has no active local member", ErrChannelRuntime)
	}
	if len(bindings) > model.MaxMembersPerChannel-1 {
		return nil, false, fmt.Errorf("%w: Channel binding set exceeds bound", ErrChannelRuntime)
	}
	readiness, err := runtime.store.ReadChannelBaselineReadiness(ctx, channel.Channel().ID())
	if err != nil {
		return nil, false, fmt.Errorf("%w: read Channel readiness: %w", ErrChannelRuntime, err)
	}
	byPeer, stale := channelRuntimeReadinessByPeer(readiness, bindings)
	if stale {
		return nil, true, nil
	}
	result := make([]channelRuntimeTarget, 0, len(bindings))
	for _, binding := range bindings {
		target, valid := newChannelRuntimeTarget(channel, local, binding, byPeer[binding.PeerID()])
		if !valid {
			return nil, true, nil
		}
		result = append(result, target)
	}
	return result, false, nil
}

func channelRuntimeReadinessByPeer(readiness []store.ChannelPeerReadiness,
	bindings []model.PeerBinding,
) (map[model.PeerID]store.ChannelPeerReadiness, bool) {
	wanted := make(map[model.PeerID]struct{}, len(bindings))
	for _, binding := range bindings {
		wanted[binding.PeerID()] = struct{}{}
	}
	result := make(map[model.PeerID]store.ChannelPeerReadiness, len(bindings))
	for _, item := range readiness {
		_, needed := wanted[item.PeerID]
		if !needed {
			if item.BindingState != model.BindingRevoked {
				return nil, true
			}
			continue
		}
		if _, duplicate := result[item.PeerID]; duplicate {
			return nil, true
		}
		result[item.PeerID] = item
	}
	return result, len(result) != len(bindings)
}

func newChannelRuntimeTarget(channel store.ChannelMeshChannel, local model.Member,
	binding model.PeerBinding, readiness store.ChannelPeerReadiness,
) (channelRuntimeTarget, bool) {
	value, roster := channel.Channel(), channel.Roster()
	remote, ok := roster.CurrentMember(binding.PeerID())
	valid := ok && remote.Status() == model.MemberActive && binding.ChannelID() == value.ID() &&
		binding.PeerID() != local.PeerID() && binding.OriginEpoch() == remote.OriginEpoch() &&
		binding.MemberHead() == remote.Head() && binding.RosterHead() == roster.Head() &&
		(binding.State() == model.BindingPending || binding.State() == model.BindingActive) &&
		readiness.ChannelID == value.ID() && readiness.PeerID == binding.PeerID() &&
		readiness.OriginEpoch == binding.OriginEpoch() && readiness.BindingState == binding.State() &&
		readiness.TopicState == model.TopicJoined && readiness.RosterHead == roster.Head()
	if !valid {
		return channelRuntimeTarget{}, false
	}
	key := channelRuntimeTargetKey{channelID: value.ID(), peerID: binding.PeerID()}
	generation := channelRuntimeTargetGeneration{channelID: value.ID(), peerID: binding.PeerID(),
		originEpoch: binding.OriginEpoch(), rosterHead: roster.Head(), memberHead: binding.MemberHead(),
		bindingState: binding.State()}
	return channelRuntimeTarget{key: key, generation: generation, roster: roster,
		local: local, binding: binding, readiness: readiness}, true
}

func (runtime *ChannelRuntime) describeCycle(topics int, topicsReady bool,
	targets []channelRuntimeTarget,
) channelRuntimeCycle {
	cycle := channelRuntimeCycle{activeTopics: topics, targets: len(targets),
		localTopicsReady: topicsReady, planValid: true}
	for _, target := range targets {
		if target.readiness.Ready() {
			cycle.readyTargets++
		}
		state := runtime.retries[target.key]
		if state.generation != target.generation {
			continue
		}
		if state.permanent {
			cycle.permanentTargets++
		} else if state.attempts > 0 {
			cycle.retryingTargets++
		}
	}
	cycle.fullyConverged = topicsReady && cycle.readyTargets == len(targets)
	return cycle
}

func (runtime *ChannelRuntime) recordCycle(now time.Time, cycle channelRuntimeCycle) {
	topicRetries := runtime.topicRetrySnapshot()
	runtime.mu.Lock()
	runtime.snapshot.Cycles++
	runtime.snapshot.ActiveTopics = cycle.activeTopics
	runtime.snapshot.LocalTopicsReady = cycle.localTopicsReady
	runtime.snapshot.FullyConverged = cycle.fullyConverged
	if cycle.planValid {
		runtime.snapshot.Targets = cycle.targets
		runtime.snapshot.ReadyTargets = cycle.readyTargets
		runtime.snapshot.RetryingTargets = cycle.retryingTargets
		runtime.snapshot.PermanentTargets = cycle.permanentTargets
	}
	runtime.snapshot.TopicRetries = topicRetries
	runtime.snapshot.LastCycleAt = now
	runtime.mu.Unlock()
}

func (runtime *ChannelRuntime) publishLocalTopicsReady(topics int, now time.Time) {
	runtime.mu.Lock()
	runtime.snapshot.ActiveTopics = topics
	runtime.snapshot.LocalTopicsReady = true
	runtime.snapshot.TopicRetries = nil
	runtime.snapshot.LastCycleAt = now
	runtime.mu.Unlock()
	runtime.readyOnce.Do(func() { close(runtime.ready) })
}
