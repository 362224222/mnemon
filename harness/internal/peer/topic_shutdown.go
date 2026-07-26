package peer

import (
	"context"
	"errors"
	"fmt"
)

func (gossip *Gossip) Close() error {
	return gossip.CloseContext(context.Background())
}

// CloseContext seals topic admission, cancels every owned refresh attempt and
// waits for the refresh owner to join them. A caller whose deadline expires
// may retry the wait later; no waiter goroutine is detached from the owner.
func (gossip *Gossip) CloseContext(ctx context.Context) error {
	if gossip == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: shutdown context is unavailable", ErrGossipTopic)
	}
	gossip.mu.Lock()
	if !gossip.closed {
		gossip.closed = true
		if gossip.cancel != nil {
			gossip.cancel()
		}
		var closeErrors []error
		for _, session := range gossip.sortedSessionsLocked() {
			session.gate.Lock()
			if err := gossip.closeSessionLocked(session); err != nil {
				closeErrors = append(closeErrors, err)
			}
			session.gate.Unlock()
		}
		gossip.closeErr = errors.Join(closeErrors...)
	}
	done, closeErr := gossip.refreshDone, gossip.closeErr
	gossip.mu.Unlock()
	if done == nil {
		return closeErr
	}
	select {
	case <-done:
		return closeErr
	default:
	}
	select {
	case <-done:
		return closeErr
	case <-ctx.Done():
		return errors.Join(closeErr,
			fmt.Errorf("%w: drain refresh worker: %w", ErrGossipTopic, ctx.Err()))
	}
}

func (gossip *Gossip) expectedShutdownCancellation(err error) bool {
	return err != nil && gossip != nil && gossip.closed && gossip.ctx != nil &&
		gossip.ctx.Err() != nil && errors.Is(err, gossip.ctx.Err())
}
