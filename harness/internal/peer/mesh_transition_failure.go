package peer

import (
	"errors"
	"fmt"
)

// failClosedEnrollmentTransport may run on the gater's sole expiry owner. It
// therefore revokes exact permits and closes the raw Host, but never invokes
// NodeHost.Close, which would wait for that same owner. A prepared Mesh
// transition is claimed and finalized here; a transition still preparing (or
// already installing/aborting) is canceled and completes on its existing
// owner path.
func (runtime *MeshRuntime) failClosedEnrollmentTransport(cause error) {
	if runtime == nil || cause == nil {
		return
	}
	runtime.mu.Lock()
	runtime.terminalErr = errors.Join(runtime.terminalErr,
		fmt.Errorf("%w: retire enrollment transport: %w", ErrMeshRuntime, cause))
	if runtime.closed {
		runtime.mu.Unlock()
		return
	}
	runtime.closed = true
	gossip := runtime.gossip
	nodeHost := runtime.nodeHost
	addressSources := runtime.addressSources
	transition := runtime.transition
	var prepared *MeshAuthorityTransition
	if transition != nil && transition.gossipTransition != nil &&
		transition.phase.CompareAndSwap(meshTransitionPending, meshTransitionInstalling) {
		prepared = transition
	}
	runtime.mu.Unlock()

	if nodeHost != nil && nodeHost.gater != nil {
		nodeHost.gater.revokeOutboundEnrollmentPermits()
	}
	if addressSources != nil {
		addressSources.close()
	}
	var closeErr error
	if prepared != nil {
		closeErr = prepared.gossipTransition.FailClosed(cause)
		prepared.complete(meshTransitionFailed, closeErr)
	} else if transition != nil {
		if gossip != nil && gossip.cancel != nil {
			gossip.cancel()
		}
	} else if gossip != nil {
		closeErr = gossip.Close()
	}
	if nodeHost != nil {
		closeErr = errors.Join(closeErr, nodeHost.closeTransportWithoutJoiningGater())
	}
	if closeErr != nil {
		runtime.mu.Lock()
		runtime.terminalErr = errors.Join(runtime.terminalErr, closeErr)
		runtime.mu.Unlock()
	}
}

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
	runtime.addressSources.close()
	result = errors.Join(result, runtime.nodeHost.Close())
	transition.complete(meshTransitionFailed, result)
	return result
}
