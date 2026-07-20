package peer

import (
	"errors"
	"fmt"
	"sync/atomic"

	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	meshTransitionPending uint32 = iota
	meshTransitionInstalling
	meshTransitionAborting
	meshTransitionInstalled
	meshTransitionAborted
	meshTransitionFailed
)

// MeshAuthorityTransition is the runtime capability that brackets one durable
// authority CAS. BeginAuthorityTransition drains affected Gossip admissions and
// stages candidate addresses, then releases every runtime/Gossip mutex before
// returning. The caller must choose exactly one of Install or Abort.
type MeshAuthorityTransition struct {
	runtime          *MeshRuntime
	gossipTransition *GossipAuthorityTransition
	snapshot         NetworkAuthoritySnapshot
	candidate        map[libp2ppeer.ID][]ma.Multiaddr
	ready            chan struct{}
	done             chan struct{}
	phase            atomic.Uint32
	prepareErr       error
	result           error
}

func (transition *MeshAuthorityTransition) Done() <-chan struct{} {
	if transition == nil || transition.done == nil {
		done := make(chan struct{})
		close(done)
		return done
	}
	return transition.done
}

func (transition *MeshAuthorityTransition) Wait() error {
	if transition == nil {
		return fmt.Errorf("%w: authority transition is unavailable", ErrMeshRuntime)
	}
	<-transition.Done()
	return transition.result
}

// BeginAuthorityTransition prepares the exact candidate that the caller will
// durably commit. Candidate Peerstore addresses are reversible staging state;
// the immutable authority and sessions remain unchanged until Install.
func (runtime *MeshRuntime) BeginAuthorityTransition(mesh store.ChannelMeshAuthority) (*MeshAuthorityTransition, error) {
	if runtime == nil {
		return nil, fmt.Errorf("%w: runtime is unavailable", ErrMeshRuntime)
	}
	snapshot, addresses, err := projectMeshRuntime(mesh)
	if err != nil {
		return nil, err
	}
	transition := &MeshAuthorityTransition{runtime: runtime, snapshot: snapshot,
		candidate: addresses, ready: make(chan struct{}), done: make(chan struct{})}
	gossip, active, err := runtime.registerMeshAuthorityTransition(transition)
	if err != nil {
		return active, err
	}
	gossipTransition, err := gossip.BeginAuthorityTransition(snapshot)
	if err != nil {
		return nil, transition.completeGossipPreparationFailure(err)
	}
	if err := runtime.addressSources.stageDurable(transition, transition.candidate); err != nil {
		return nil, transition.completeAddressPreparationFailure(gossipTransition, err)
	}
	return transition.publishPrepared(gossipTransition)
}

func (runtime *MeshRuntime) registerMeshAuthorityTransition(transition *MeshAuthorityTransition) (
	*Gossip, *MeshAuthorityTransition, error,
) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed || runtime.nodeHost == nil || runtime.gossip == nil {
		return nil, nil, fmt.Errorf("%w: runtime is closed", ErrMeshRuntime)
	}
	if active := runtime.transition; active != nil {
		return nil, active, fmt.Errorf("%w: %w", ErrMeshRuntime,
			ErrMeshAuthorityTransitionInProgress)
	}
	runtime.transition = transition
	return runtime.gossip, nil, nil
}

func (transition *MeshAuthorityTransition) completeGossipPreparationFailure(cause error) error {
	transition.prepareErr = fmt.Errorf("%w: prepare Gossip authority: %w", ErrMeshRuntime, cause)
	transition.runtime.mu.Lock()
	failedClosed := transition.runtime.closed
	transition.runtime.mu.Unlock()
	close(transition.ready)
	phase := uint32(meshTransitionAborted)
	if failedClosed {
		phase = meshTransitionFailed
	}
	transition.complete(phase, transition.prepareErr)
	return transition.prepareErr
}

func (transition *MeshAuthorityTransition) completeAddressPreparationFailure(
	gossipTransition *GossipAuthorityTransition, cause error,
) error {
	transition.runtime.mu.Lock()
	failedClosed := transition.runtime.closed
	transition.runtime.mu.Unlock()
	phase := uint32(meshTransitionAborted)
	var finishErr error
	if failedClosed {
		phase = meshTransitionFailed
		finishErr = gossipTransition.FailClosed(cause)
	} else {
		finishErr = gossipTransition.Abort()
	}
	transition.prepareErr = errors.Join(
		fmt.Errorf("%w: stage Peer addresses: %w", ErrMeshRuntime, cause), finishErr)
	close(transition.ready)
	transition.complete(phase, transition.prepareErr)
	return transition.prepareErr
}

func (transition *MeshAuthorityTransition) publishPrepared(
	gossipTransition *GossipAuthorityTransition,
) (*MeshAuthorityTransition, error) {
	runtime := transition.runtime
	runtime.mu.Lock()
	if !runtime.closed {
		transition.gossipTransition = gossipTransition
		close(transition.ready)
		runtime.mu.Unlock()
		return transition, nil
	}
	failure := runtime.terminalErr
	runtime.mu.Unlock()
	if failure == nil {
		failure = fmt.Errorf("%w: runtime closed during authority preparation", ErrMeshRuntime)
	}
	runtime.addressSources.close()
	transition.prepareErr = gossipTransition.FailClosed(failure)
	close(transition.ready)
	transition.complete(meshTransitionFailed, transition.prepareErr)
	return nil, transition.prepareErr
}

// Install exposes the already-committed durable candidate, rotates affected
// sessions and joins every newly active Channel before reporting completion.
func (transition *MeshAuthorityTransition) Install() error {
	if transition == nil || transition.runtime == nil {
		return fmt.Errorf("%w: authority transition is unavailable", ErrMeshRuntime)
	}
	<-transition.ready
	if transition.prepareErr != nil {
		return transition.prepareErr
	}
	if !transition.phase.CompareAndSwap(meshTransitionPending, meshTransitionInstalling) {
		<-transition.Done()
		if phase := transition.phase.Load(); phase == meshTransitionInstalled ||
			phase == meshTransitionFailed {
			return transition.result
		}
		return fmt.Errorf("%w: %w", ErrMeshRuntime, ErrMeshAuthorityTransitionFinalized)
	}

	runtime := transition.runtime
	err := transition.gossipTransition.Install()
	if err == nil {
		err = runtime.addressSources.installDurable(transition)
	}
	if err == nil {
		// Connection lifecycle hooks do not revisit already-upgraded
		// connections after an authority replacement. Reconcile against the
		// just-installed whole snapshot before the transition is released so a
		// Peer revoked from its final Channel cannot retain a stale physical
		// connection; overlapping Channel authority remains intact.
		err = runtime.nodeHost.ReconcileConnections()
	}
	if err == nil {
		err = joinActiveChannels(runtime.gossip, transition.snapshot)
	}
	runtime.mu.Lock()
	runtime.addresses = cloneManagedAddresses(transition.candidate)
	var primary error
	primaryWon := false
	if err != nil {
		primary = fmt.Errorf("%w: install authority: %w", ErrMeshRuntime, err)
		primaryWon = runtime.terminateAuthorityLocked(primary)
	}
	runtime.mu.Unlock()
	phase := uint32(meshTransitionInstalled)
	if err != nil {
		phase = meshTransitionFailed
		runtime.addressSources.close()
		cleanupErr := errors.Join(runtime.gossip.Close(), runtime.nodeHost.Close())
		runtime.mu.Lock()
		runtime.terminalErr = errors.Join(runtime.terminalErr, cleanupErr)
		runtime.mu.Unlock()
		var firstCause error
		if !primaryWon {
			firstCause = runtime.terminalError()
		}
		err = errors.Join(primary, firstCause, cleanupErr)
	}
	transition.complete(phase, err)
	return err
}

// Abort rolls candidate addresses back before reopening the old Gossip gates.
// It must be used only when the durable CAS did not commit.
func (transition *MeshAuthorityTransition) Abort() error {
	if transition == nil || transition.runtime == nil {
		return fmt.Errorf("%w: authority transition is unavailable", ErrMeshRuntime)
	}
	<-transition.ready
	if transition.prepareErr != nil {
		return transition.prepareErr
	}
	if !transition.phase.CompareAndSwap(meshTransitionPending, meshTransitionAborting) {
		<-transition.Done()
		if transition.phase.Load() == meshTransitionAborted {
			return transition.result
		}
		return fmt.Errorf("%w: %w", ErrMeshRuntime, ErrMeshAuthorityTransitionFinalized)
	}
	runtime := transition.runtime
	err := errors.Join(runtime.addressSources.abortDurable(transition),
		transition.gossipTransition.Abort())
	phase := uint32(meshTransitionAborted)
	var primary error
	primaryWon := false
	if err != nil {
		phase = meshTransitionFailed
		primary = fmt.Errorf("%w: abort authority: %w", ErrMeshRuntime, err)
		runtime.mu.Lock()
		primaryWon = runtime.terminateAuthorityLocked(primary)
		runtime.mu.Unlock()
		runtime.addressSources.close()
		cleanupErr := errors.Join(runtime.gossip.Close(), runtime.nodeHost.Close())
		runtime.mu.Lock()
		runtime.terminalErr = errors.Join(runtime.terminalErr, cleanupErr)
		runtime.mu.Unlock()
		var firstCause error
		if !primaryWon {
			firstCause = runtime.terminalError()
		}
		err = errors.Join(primary, firstCause, cleanupErr)
	}
	transition.complete(phase, err)
	return err
}

func (transition *MeshAuthorityTransition) complete(phase uint32, result error) {
	runtime := transition.runtime
	runtime.mu.Lock()
	transition.result = result
	transition.phase.Store(phase)
	if runtime.transition == transition {
		runtime.transition = nil
	}
	close(transition.done)
	runtime.mu.Unlock()
}
