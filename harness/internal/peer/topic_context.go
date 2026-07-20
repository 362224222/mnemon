package peer

import (
	"context"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func (gossip *Gossip) join(ctx context.Context,
	channelID model.ChannelID,
) (*TopicSession, error) {
	if gossip == nil || gossip.pubsub == nil || ctx == nil {
		return nil, fmt.Errorf("%w: router is unavailable", ErrGossipTopic)
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("%w: join canceled: %w", ErrGossipTopic, err)
		}
		gossip.mu.Lock()
		if gossip.closed {
			gossip.mu.Unlock()
			return nil, fmt.Errorf("%w: router is closed", ErrGossipTopic)
		}
		wait, waiting := gossip.joinWaitLocked(channelID)
		if waiting {
			lifetime := gossip.ctx
			gossip.mu.Unlock()
			if err := waitGossipTopic(ctx, lifetime, wait); err != nil {
				return nil, err
			}
			continue
		}
		topicName, err := TopicName(channelID)
		if err != nil || !gossip.authority.CanSubscribe(topicName) {
			gossip.mu.Unlock()
			return nil, fmt.Errorf("%w: Channel is not locally active", ErrGossipTopic)
		}
		session, err := gossip.joinLocked(channelID, topicName, true)
		gossip.mu.Unlock()
		return session, err
	}
}

func (gossip *Gossip) joinWaitLocked(channelID model.ChannelID) (<-chan struct{}, bool) {
	if transition := gossip.transition; transition != nil && transition.affects(channelID) {
		return transition.Done(), true
	}
	if current := gossip.sessions[channelID]; current != nil && current.closed.Load() {
		return current.closeDone, true
	}
	return nil, false
}

func waitGossipTopic(ctx, lifetime context.Context, done <-chan struct{}) error {
	if ctx == nil || lifetime == nil || done == nil {
		return fmt.Errorf("%w: topic wait is unavailable", ErrGossipTopic)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: topic wait canceled: %w", ErrGossipTopic, err)
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%w: topic wait canceled: %w", ErrGossipTopic, ctx.Err())
	case <-lifetime.Done():
		return fmt.Errorf("%w: router stopped: %w", ErrGossipTopic, lifetime.Err())
	}
}
