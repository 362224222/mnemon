package peer

import (
	"errors"
	"fmt"
)

// FailClosed terminates a prepared Gossip transition without publishing either
// its previous or candidate authority. It is reserved for a durable outcome
// that cannot be reconciled to either frozen generation.
func (transition *GossipAuthorityTransition) FailClosed(cause error) error {
	if transition == nil || transition.gossip == nil {
		return fmt.Errorf("%w: authority transition is unavailable", ErrGossipTopic)
	}
	if cause == nil {
		cause = errors.New("durable Gossip authority diverged")
	}
	if !transition.phase.CompareAndSwap(gossipTransitionPending, gossipTransitionInstalling) {
		<-transition.Done()
		if transition.phase.Load() == gossipTransitionFailed {
			return transition.result
		}
		return fmt.Errorf("%w: %w", ErrGossipTopic, ErrGossipTransitionFinalized)
	}
	return transition.failClosed([]error{cause})
}

// FailClosed closes the entire mesh when an outcome-unknown durable commit no
// longer equals either the frozen preimage or candidate. Reopening the old
// gates or briefly installing the unproved candidate would grant stale
// authority, so the only safe recovery is a fresh process reconstruction from
// Store.
func (transition *MeshAuthorityTransition) FailClosed(cause error) error {
	if transition == nil || transition.runtime == nil {
		return fmt.Errorf("%w: authority transition is unavailable", ErrMeshRuntime)
	}
	<-transition.ready
	if transition.prepareErr != nil {
		return transition.prepareErr
	}
	if cause == nil {
		cause = errors.New("durable mesh authority diverged")
	}
	if !transition.phase.CompareAndSwap(meshTransitionPending, meshTransitionInstalling) {
		<-transition.Done()
		if transition.phase.Load() == meshTransitionFailed {
			return transition.result
		}
		return fmt.Errorf("%w: %w", ErrMeshRuntime, ErrMeshAuthorityTransitionFinalized)
	}

	runtime := transition.runtime
	result := transition.gossipTransition.FailClosed(cause)
	runtime.mu.Lock()
	runtime.closed = true
	runtime.mu.Unlock()
	result = errors.Join(result, runtime.nodeHost.Close())
	transition.complete(meshTransitionFailed, result)
	return result
}
