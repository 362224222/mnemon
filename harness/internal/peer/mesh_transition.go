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
)

// MeshAuthorityTransition is the runtime capability that brackets one durable
// authority CAS. BeginAuthorityTransition drains affected Gossip admissions and
// stages candidate addresses, then releases every runtime/Gossip mutex before
// returning. The caller must choose exactly one of Install or Abort.
type MeshAuthorityTransition struct {
	runtime          *MeshRuntime
	gossipTransition *GossipAuthorityTransition
	snapshot         NetworkAuthoritySnapshot
	previous         map[libp2ppeer.ID][]ma.Multiaddr
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
	runtime.mu.Lock()
	if runtime.closed || runtime.nodeHost == nil || runtime.gossip == nil {
		runtime.mu.Unlock()
		return nil, fmt.Errorf("%w: runtime is closed", ErrMeshRuntime)
	}
	if active := runtime.transition; active != nil {
		runtime.mu.Unlock()
		return active, fmt.Errorf("%w: %w", ErrMeshRuntime, ErrMeshAuthorityTransitionInProgress)
	}
	transition.previous = cloneManagedAddresses(runtime.addresses)
	runtime.transition = transition
	gossip := runtime.gossip
	nodeHost := runtime.nodeHost.Host()
	runtime.mu.Unlock()

	gossipTransition, err := gossip.BeginAuthorityTransition(snapshot)
	if err != nil {
		transition.prepareErr = fmt.Errorf("%w: prepare Gossip authority: %w", ErrMeshRuntime, err)
		close(transition.ready)
		transition.complete(meshTransitionAborted, transition.prepareErr)
		return nil, transition.prepareErr
	}
	transition.gossipTransition = gossipTransition
	applyManagedAddresses(nodeHost, transition.previous, transition.candidate)
	close(transition.ready)
	return transition, nil
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
		if transition.phase.Load() == meshTransitionInstalled {
			return transition.result
		}
		return fmt.Errorf("%w: %w", ErrMeshRuntime, ErrMeshAuthorityTransitionFinalized)
	}

	runtime := transition.runtime
	err := transition.gossipTransition.Install()
	if err == nil {
		err = joinActiveChannels(runtime.gossip, transition.snapshot)
	}
	runtime.mu.Lock()
	runtime.addresses = cloneManagedAddresses(transition.candidate)
	if err != nil {
		runtime.closed = true
	}
	runtime.mu.Unlock()
	if err != nil {
		err = errors.Join(fmt.Errorf("%w: install authority: %w", ErrMeshRuntime, err),
			runtime.gossip.Close())
	}
	transition.complete(meshTransitionInstalled, err)
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
	applyManagedAddresses(transition.runtime.nodeHost.Host(), transition.candidate, transition.previous)
	err := transition.gossipTransition.Abort()
	transition.complete(meshTransitionAborted, err)
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
