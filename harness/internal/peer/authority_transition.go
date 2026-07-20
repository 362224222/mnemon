package peer

import (
	"errors"
	"fmt"
	"sort"
	"sync/atomic"

	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrGossipTransitionInProgress = errors.New("Gossip authority transition is in progress")
	ErrGossipTransitionFinalized  = errors.New("Gossip authority transition was already finalized")
)

type gossipTransitionChannel struct {
	channelID       model.ChannelID
	gate            *channelGate
	oldSession      *TopicSession
	drained         <-chan struct{}
	wasDeliverable  bool
	candidateActive bool
	retire          bool
}

const (
	gossipTransitionPending uint32 = iota
	gossipTransitionInstalling
	gossipTransitionAborting
	gossipTransitionInstalled
	gossipTransitionAborted
	gossipTransitionFailed
)

// GossipAuthorityTransition is a prepared, drained authority transition. The
// caller performs its durable CAS after BeginAuthorityTransition returns, then
// chooses exactly one of Install or Abort. No Gossip or Authority mutex remains
// held during that durable operation.
type GossipAuthorityTransition struct {
	gossip        *Gossip
	candidate     *networkAuthorityState
	affected      map[model.ChannelID]struct{}
	channels      []gossipTransitionChannel
	promotedPeers []libp2ppeer.ID
	done          chan struct{}
	phase         atomic.Uint32
	result        error
}

func (transition *GossipAuthorityTransition) Done() <-chan struct{} {
	if transition == nil || transition.done == nil {
		done := make(chan struct{})
		close(done)
		return done
	}
	return transition.done
}

func (transition *GossipAuthorityTransition) Wait() error {
	if transition == nil {
		return fmt.Errorf("%w: authority transition is unavailable", ErrGossipTopic)
	}
	<-transition.Done()
	return transition.result
}

func (transition *GossipAuthorityTransition) affects(channelID model.ChannelID) bool {
	if transition == nil {
		return false
	}
	_, affected := transition.affected[channelID]
	return affected
}

// Reconcile is the in-memory convenience path. Durable callers use the typed
// BeginAuthorityTransition capability and invoke Install only after their CAS.
func (gossip *Gossip) Reconcile(snapshot NetworkAuthoritySnapshot) error {
	transition, err := gossip.BeginAuthorityTransition(snapshot)
	if err != nil {
		return err
	}
	return transition.Install()
}

// BeginAuthorityTransition validates the complete candidate, installs explicit
// admission fences on only changed Channels, and waits for their current work
// to drain. It returns with no runtime, authority, or gate mutex held.
func (gossip *Gossip) BeginAuthorityTransition(snapshot NetworkAuthoritySnapshot) (*GossipAuthorityTransition, error) {
	if gossip == nil || gossip.pubsub == nil || gossip.authority == nil {
		return nil, fmt.Errorf("%w: router is unavailable", ErrGossipTopic)
	}
	candidate, err := gossip.authority.prepare(snapshot)
	if err != nil {
		return nil, fmt.Errorf("%w: prepare authority: %v", ErrGossipTopic, err)
	}

	for {
		gossip.mu.Lock()
		transition, closing, err := gossip.prepareAuthorityTransitionLocked(candidate)
		gossip.mu.Unlock()
		if err != nil {
			return transition, err
		}
		if closing != nil {
			<-closing
			continue
		}
		if err := transition.waitForDrain(); err != nil {
			_ = transition.Abort()
			return nil, err
		}
		return transition, nil
	}
}

func (gossip *Gossip) prepareAuthorityTransitionLocked(candidate *networkAuthorityState) (
	*GossipAuthorityTransition, <-chan struct{}, error,
) {
	if gossip.closed {
		return nil, nil, fmt.Errorf("%w: router is closed", ErrGossipTopic)
	}
	if active := gossip.transition; active != nil {
		return active, nil, fmt.Errorf("%w: %w", ErrGossipTopic, ErrGossipTransitionInProgress)
	}
	current := gossip.authority.state.Load()
	affected := affectedAuthorityChannels(current, candidate)
	for channelID, session := range gossip.sessions {
		if _, changed := affected[channelID]; changed && session != nil &&
			session.closed.Load() && !channelClosed(session.closeDone) {
			return nil, session.closeDone, nil
		}
	}
	transition := &GossipAuthorityTransition{gossip: gossip, candidate: candidate,
		affected: affected, promotedPeers: dataPlanePromotions(current, candidate), done: make(chan struct{})}
	channelIDs := make([]model.ChannelID, 0, len(gossip.gates))
	for channelID, gate := range gossip.gates {
		if gate != nil {
			channelIDs = append(channelIDs, channelID)
		}
	}
	sort.Slice(channelIDs, func(left, right int) bool {
		return channelIDs[left].String() < channelIDs[right].String()
	})
	for _, channelID := range channelIDs {
		candidateChannel, exists := candidate.channels[channelID]
		retire := !exists || candidateChannel.status.Terminal()
		if _, changed := affected[channelID]; !changed && !retire {
			continue
		}
		affected[channelID] = struct{}{}
		gate := gossip.gates[channelID]
		drained, wasDeliverable := gate.beginDrain()
		transition.channels = append(transition.channels, gossipTransitionChannel{
			channelID: channelID, gate: gate, oldSession: gossip.sessions[channelID], drained: drained,
			wasDeliverable:  wasDeliverable,
			candidateActive: exists && candidateChannel.status == model.ChannelActive,
			retire:          retire,
		})
	}
	gossip.transition = transition
	return transition, nil, nil
}

func affectedAuthorityChannels(current, candidate *networkAuthorityState) map[model.ChannelID]struct{} {
	channelIDs := make(map[model.ChannelID]struct{})
	if current != nil {
		for channelID := range current.channels {
			channelIDs[channelID] = struct{}{}
		}
	}
	if candidate != nil {
		for channelID := range candidate.channels {
			channelIDs[channelID] = struct{}{}
		}
	}
	affected := make(map[model.ChannelID]struct{})
	for channelID := range channelIDs {
		if topicGenerationFromState(current, channelID) != topicGenerationFromState(candidate, channelID) {
			affected[channelID] = struct{}{}
		}
	}
	return affected
}

func (transition *GossipAuthorityTransition) waitForDrain() error {
	for _, channel := range transition.channels {
		select {
		case <-channel.drained:
		case <-transition.gossip.ctx.Done():
			return fmt.Errorf("%w: drain authority transition: %v", ErrGossipTopic,
				transition.gossip.ctx.Err())
		}
	}
	return nil
}

// Install atomically publishes the prepared authority and rotates affected
// sessions. Durable state must already have committed before this method runs.
func (transition *GossipAuthorityTransition) Install() error {
	if transition == nil || transition.gossip == nil {
		return fmt.Errorf("%w: authority transition is unavailable", ErrGossipTopic)
	}
	if !transition.phase.CompareAndSwap(gossipTransitionPending, gossipTransitionInstalling) {
		<-transition.Done()
		if phase := transition.phase.Load(); phase == gossipTransitionInstalled ||
			phase == gossipTransitionFailed {
			return transition.result
		}
		return fmt.Errorf("%w: %w", ErrGossipTopic, ErrGossipTransitionFinalized)
	}
	reconcileErrors, err := transition.installCandidate()
	if err != nil {
		return transition.failClosed([]error{err})
	}
	if len(reconcileErrors) > 0 {
		return transition.failClosed(reconcileErrors)
	}
	transition.finishInstall()
	return nil
}

func (transition *GossipAuthorityTransition) installCandidate() ([]error, error) {
	gossip := transition.gossip
	gossip.mu.Lock()
	defer gossip.mu.Unlock()
	if gossip.closed || gossip.transition != transition {
		return nil, fmt.Errorf("%w: router closed during authority transition", ErrGossipTopic)
	}
	gossip.authority.beginUpdate()
	gossip.authority.install(transition.candidate)
	gossip.authority.finishUpdate()
	reconcileErrors := transition.closeAffectedSessionsLocked()
	if len(reconcileErrors) == 0 {
		reconcileErrors = transition.rejoinAffectedSessionsLocked()
	}
	return reconcileErrors, nil
}

func (transition *GossipAuthorityTransition) closeAffectedSessionsLocked() []error {
	gossip := transition.gossip
	var reconcileErrors []error
	for index := range transition.channels {
		channel := &transition.channels[index]
		if session := gossip.sessions[channel.channelID]; session != nil {
			if err := gossip.closeSessionLocked(session); err != nil {
				reconcileErrors = append(reconcileErrors,
					fmt.Errorf("rotate Channel %s: %w", channel.channelID.String(), err))
			}
		}
		if channel.retire {
			channel.gate.retire()
			if gossip.gates[channel.channelID] == channel.gate {
				delete(gossip.gates, channel.channelID)
			}
		}
	}
	return reconcileErrors
}

func (transition *GossipAuthorityTransition) rejoinAffectedSessionsLocked() []error {
	gossip := transition.gossip
	var reconcileErrors []error
	for index := range transition.channels {
		channel := &transition.channels[index]
		if channel.retire || !channel.candidateActive || channel.oldSession == nil {
			continue
		}
		topicName, err := TopicName(channel.channelID)
		if err == nil {
			_, err = gossip.joinLocked(channel.channelID, topicName, false)
		}
		if err != nil {
			reconcileErrors = append(reconcileErrors,
				fmt.Errorf("rejoin Channel %s: %w", channel.channelID.String(), err))
			continue
		}
		channel.oldSession.handoff.Store(true)
	}
	return reconcileErrors
}

func (transition *GossipAuthorityTransition) failClosed(reconcileErrors []error) error {
	gossip := transition.gossip
	gossip.mu.Lock()
	gossip.closed = true
	waits := make([]<-chan struct{}, 0, len(gossip.gates))
	for _, gate := range gossip.gates {
		drained, _ := gate.beginDrain()
		waits = append(waits, drained)
	}
	gossip.mu.Unlock()
	if gossip.cancel != nil {
		gossip.cancel()
	}
	for _, drained := range waits {
		<-drained
	}
	gossip.mu.Lock()
	for _, session := range gossip.sortedSessionsLocked() {
		if err := gossip.closeSessionLocked(session); err != nil {
			reconcileErrors = append(reconcileErrors, err)
		}
	}
	for channelID, gate := range gossip.gates {
		gate.retire()
		delete(gossip.gates, channelID)
	}
	gossip.mu.Unlock()
	if gossip.refreshDone != nil {
		<-gossip.refreshDone
	}
	err := errors.Join(reconcileErrors...)
	gossip.mu.Lock()
	gossip.closeErr = err
	gossip.finishCloseLocked()
	gossip.mu.Unlock()
	transition.complete(gossipTransitionFailed, err)
	return err
}

func (transition *GossipAuthorityTransition) finishInstall() {
	gossip := transition.gossip
	// Network refresh is deliberately outside the Gossip mutex. The affected
	// admission fences remain drained until stream revocation is complete.
	gossip.resetUnauthorizedDataPlaneStreams()
	gossip.mu.Lock()
	gossip.pruneRefreshPeers(dataPlanePeers(transition.candidate))
	gossip.recordPromotedPeers(transition.promotedPeers)
	for _, channel := range transition.channels {
		if channel.retire || gossip.gates[channel.channelID] != channel.gate {
			continue
		}
		session := gossip.sessions[channel.channelID]
		channel.gate.resume(session != nil && !session.closed.Load() && channel.candidateActive)
	}
	gossip.mu.Unlock()
	transition.complete(gossipTransitionInstalled, nil)
	gossip.signalRefresh()
}

// Abort restores the exact pre-transition gate admissions. It is safe only
// when the caller's durable CAS did not commit.
func (transition *GossipAuthorityTransition) Abort() error {
	if transition == nil || transition.gossip == nil {
		return fmt.Errorf("%w: authority transition is unavailable", ErrGossipTopic)
	}
	if !transition.phase.CompareAndSwap(gossipTransitionPending, gossipTransitionAborting) {
		<-transition.Done()
		if transition.phase.Load() == gossipTransitionAborted {
			return transition.result
		}
		return fmt.Errorf("%w: %w", ErrGossipTopic, ErrGossipTransitionFinalized)
	}
	transition.resumePrevious()
	transition.complete(gossipTransitionAborted, nil)
	return nil
}

func (transition *GossipAuthorityTransition) resumePrevious() {
	for _, channel := range transition.channels {
		channel.gate.resume(channel.wasDeliverable)
	}
}

func (transition *GossipAuthorityTransition) complete(phase uint32, result error) {
	gossip := transition.gossip
	gossip.mu.Lock()
	transition.result = result
	transition.phase.Store(phase)
	if gossip.transition == transition {
		gossip.transition = nil
	}
	close(transition.done)
	gossip.mu.Unlock()
}
