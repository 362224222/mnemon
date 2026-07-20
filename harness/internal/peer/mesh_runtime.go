package peer

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	ma "github.com/multiformats/go-multiaddr"
)

var (
	ErrMeshRuntime                       = errors.New("Mnemon mesh runtime")
	ErrMeshAuthorityTransitionInProgress = errors.New("mesh authority transition is in progress")
	ErrMeshAuthorityTransitionFinalized  = errors.New("mesh authority transition was already finalized")
	errMeshRuntimeAuthorityTerminated    = fmt.Errorf("%w: authority terminated", ErrMeshRuntime)
)

// MeshRuntime owns the one libp2p Host, authority projection and Gossip router
// of a Node. Store remains the durable authority; this runtime only installs
// complete Store projections and never invents an independent Channel view.
type MeshRuntime struct {
	nodeHost       *NodeHost
	gossip         *Gossip
	authority      *Authority
	addressSources *meshAddressSources
	endpoint       MeshEndpointSnapshot
	cancel         context.CancelFunc

	mu            sync.Mutex
	addresses     map[libp2ppeer.ID][]ma.Multiaddr
	transition    *MeshAuthorityTransition
	closed        bool
	terminalErr   error
	terminalCause error
	terminalDone  chan struct{}
	closeOnce     sync.Once
	closeErr      error
}

var unavailableMeshRuntimeTerminalSignal = func() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}()

// NewMeshRuntime is the one-shot composition of BindMeshHost and Freeze for
// callers that do not need to persist endpoint authority between the stages.
// It still creates exactly one Host and transfers that same Host into runtime.
func NewMeshRuntime(ctx context.Context, privateKey libp2pcrypto.PrivKey,
	listenAddrs []ma.Multiaddr, mesh store.ChannelMeshAuthority,
) (*MeshRuntime, error) {
	bound, err := BindMeshHost(ctx, privateKey, MeshHostBindSpec{ListenAddrs: listenAddrs})
	if err != nil {
		return nil, fmt.Errorf("%w: bind Host: %w", ErrMeshRuntime, err)
	}
	runtime, err := bound.Freeze(ctx, mesh)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("%w: freeze Host: %w", ErrMeshRuntime, err),
			bound.Close())
	}
	return runtime, nil
}

func freezeMeshRuntime(ctx context.Context, nodeHost *NodeHost, authority *Authority,
	endpoint MeshEndpointSnapshot, mesh store.ChannelMeshAuthority,
) (*MeshRuntime, error) {
	if ctx == nil || ctx.Err() != nil || nodeHost == nil || authority == nil ||
		endpoint.peerID.IsZero() {
		return nil, fmt.Errorf("%w: live bound Host is required", ErrMeshRuntime)
	}
	snapshot, addresses, err := projectMeshRuntime(mesh)
	if err != nil {
		return nil, err
	}
	if snapshot.LocalPeerID != endpoint.peerID || authority.LocalPeerID().String() != endpoint.peerID.String() ||
		nodeHost.managedRuntimeHost() == nil ||
		nodeHost.managedRuntimeHost().ID().String() != endpoint.peerID.String() {
		return nil, fmt.Errorf("%w: bound Host identity differs from durable mesh", ErrMeshRuntime)
	}
	addressSources, err := newMeshAddressSources(nodeHost.managedRuntimeHost().Peerstore())
	if err != nil {
		return nil, err
	}
	fail := func(cause error, gossip *Gossip) (*MeshRuntime, error) {
		addressSources.close()
		if gossip != nil {
			cause = errors.Join(cause, gossip.Close())
		}
		return nil, cause
	}
	if err := addressSources.installInitial(addresses); err != nil {
		return fail(fmt.Errorf("%w: install initial Peer addresses: %w", ErrMeshRuntime, err), nil)
	}
	gossip, err := NewGossip(ctx, nodeHost.managedRuntimeHost(), authority)
	if err != nil {
		return fail(fmt.Errorf("%w: construct Gossip router: %w", ErrMeshRuntime, err), nil)
	}
	if err := gossip.Reconcile(snapshot); err != nil {
		return fail(fmt.Errorf("%w: install initial authority: %w", ErrMeshRuntime, err), gossip)
	}
	if err := joinActiveChannels(gossip, snapshot); err != nil {
		return fail(fmt.Errorf("%w: join initial topics: %w", ErrMeshRuntime, err), gossip)
	}
	if err := ctx.Err(); err != nil {
		return fail(fmt.Errorf("%w: initial reconcile canceled: %w", ErrMeshRuntime, err), gossip)
	}
	return &MeshRuntime{nodeHost: nodeHost, gossip: gossip, authority: authority,
		addressSources: addressSources, endpoint: endpoint.clone(),
		addresses: cloneManagedAddresses(addresses), terminalDone: make(chan struct{})}, nil
}

func (runtime *MeshRuntime) managedRuntimeHost() host.Host {
	if runtime == nil || runtime.nodeHost == nil {
		return nil
	}
	return runtime.nodeHost.managedRuntimeHost()
}

// LocalEnrollmentMultiaddrs returns the bounded, canonical address snapshot
// advertised by the one managed Host. Callers cannot substitute a different
// address set for enrollment evidence.
func (runtime *MeshRuntime) LocalEnrollmentMultiaddrs() ([]string, error) {
	if runtime == nil {
		return nil, fmt.Errorf("%w: runtime is unavailable", ErrMeshRuntime)
	}
	runtime.mu.Lock()
	if runtime.closed || runtime.nodeHost == nil {
		runtime.mu.Unlock()
		return nil, fmt.Errorf("%w: runtime is closed", ErrMeshRuntime)
	}
	addresses := runtime.endpoint.AdvertisedAddrs()
	runtime.mu.Unlock()
	if len(addresses) == 0 {
		return nil, fmt.Errorf("%w: local enrollment snapshot is unavailable", ErrMeshRuntime)
	}
	return addresses, nil
}

func (runtime *MeshRuntime) Session(channelID model.ChannelID) (*TopicSession, error) {
	return runtime.session(context.Background(), channelID)
}

func (runtime *MeshRuntime) session(ctx context.Context,
	channelID model.ChannelID,
) (*TopicSession, error) {
	if runtime == nil || ctx == nil {
		return nil, fmt.Errorf("%w: runtime is unavailable", ErrMeshRuntime)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: acquire Channel session: %w", ErrMeshRuntime, err)
	}
	runtime.mu.Lock()
	if runtime.closed || runtime.gossip == nil {
		runtime.mu.Unlock()
		return nil, fmt.Errorf("%w: runtime is closed", ErrMeshRuntime)
	}
	gossip := runtime.gossip
	runtime.mu.Unlock()
	session, err := gossip.join(ctx, channelID)
	if err != nil {
		return nil, fmt.Errorf("%w: acquire Channel session: %w", ErrMeshRuntime, err)
	}
	return session, nil
}

func (runtime *MeshRuntime) HasCurrentSession(channelID model.ChannelID) bool {
	if runtime == nil {
		return false
	}
	runtime.mu.Lock()
	gossip := runtime.gossip
	available := !runtime.closed && gossip != nil
	runtime.mu.Unlock()
	return available && gossip.HasCurrentSession(channelID)
}

// terminalSignal closes when the frozen authority can no longer admit work.
// It does not wait for the owned transports to finish physical cleanup.
func (runtime *MeshRuntime) terminalSignal() <-chan struct{} {
	if runtime == nil || runtime.terminalDone == nil {
		return unavailableMeshRuntimeTerminalSignal
	}
	return runtime.terminalDone
}

// terminalError reports the primary authority-termination cause after
// terminalSignal closes. Calling it before termination fails closed rather
// than presenting a live runtime as a successful shutdown.
func (runtime *MeshRuntime) terminalError() error {
	if runtime == nil || runtime.terminalDone == nil {
		return fmt.Errorf("%w: authority terminal signal is unavailable", ErrMeshRuntime)
	}
	select {
	case <-runtime.terminalDone:
	default:
		return fmt.Errorf("%w: authority has not terminated", ErrMeshRuntime)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if !runtime.closed {
		return fmt.Errorf("%w: authority terminal state is inconsistent", ErrMeshRuntime)
	}
	if runtime.terminalCause == nil {
		return fmt.Errorf("%w: authority terminal cause is unavailable", ErrMeshRuntime)
	}
	return runtime.terminalCause
}

// terminateAuthorityLocked publishes the one authority-terminal edge. The
// caller holds runtime.mu. Every non-nil primary is joined before checking
// whether another owner already published the edge, preserving concurrent
// failure aggregation without reopening or replacing the one signal.
func (runtime *MeshRuntime) terminateAuthorityLocked(primary error) bool {
	if primary != nil {
		runtime.terminalErr = errors.Join(runtime.terminalErr, primary)
	}
	if runtime.closed {
		return false
	}
	if primary == nil {
		primary = errMeshRuntimeAuthorityTerminated
	}
	runtime.terminalCause = primary
	runtime.closed = true
	if runtime.terminalDone != nil {
		close(runtime.terminalDone)
	}
	return true
}

func (runtime *MeshRuntime) Close() error {
	if runtime == nil {
		return nil
	}
	runtime.closeOnce.Do(func() {
		var gossip *Gossip
		var nodeHost *NodeHost
		var addressSources *meshAddressSources
		var cancel context.CancelFunc
		for {
			runtime.mu.Lock()
			if transition := runtime.transition; transition != nil {
				done := transition.Done()
				runtime.mu.Unlock()
				<-done
				continue
			}
			runtime.terminateAuthorityLocked(nil)
			gossip = runtime.gossip
			nodeHost = runtime.nodeHost
			addressSources = runtime.addressSources
			cancel = runtime.cancel
			runtime.mu.Unlock()
			break
		}
		if addressSources != nil {
			addressSources.close()
		}
		if gossip != nil {
			runtime.closeErr = errors.Join(runtime.closeErr, gossip.Close())
		}
		if nodeHost != nil {
			runtime.closeErr = errors.Join(runtime.closeErr, nodeHost.Close())
		}
		if cancel != nil {
			cancel()
		}
		runtime.mu.Lock()
		runtime.terminalErr = errors.Join(runtime.terminalErr, runtime.closeErr)
		runtime.mu.Unlock()
	})
	runtime.mu.Lock()
	result := runtime.terminalErr
	runtime.mu.Unlock()
	return result
}

func projectMeshRuntime(mesh store.ChannelMeshAuthority) (NetworkAuthoritySnapshot,
	map[libp2ppeer.ID][]ma.Multiaddr, error,
) {
	snapshot, err := ProjectNetworkAuthority(mesh)
	if err != nil {
		return NetworkAuthoritySnapshot{}, nil,
			fmt.Errorf("%w: project durable authority: %w", ErrMeshRuntime, err)
	}
	liveChannels := make(map[model.ChannelID]struct{}, len(snapshot.Channels))
	for _, channel := range snapshot.Channels {
		liveChannels[channel.ChannelID] = struct{}{}
	}
	sets := make(map[libp2ppeer.ID]map[string]ma.Multiaddr)
	for _, durable := range mesh.Channels() {
		channel := durable.Channel()
		if _, live := liveChannels[channel.ID()]; !live {
			continue
		}
		for _, binding := range durable.Bindings() {
			peerID, err := canonicalLibp2pID(binding.PeerID())
			if err != nil {
				return NetworkAuthoritySnapshot{}, nil,
					fmt.Errorf("%w: invalid binding PeerID: %w", ErrMeshRuntime, err)
			}
			if sets[peerID] == nil {
				sets[peerID] = make(map[string]ma.Multiaddr)
			}
			for _, raw := range binding.Multiaddrs() {
				addresses, err := canonicalPeerAddresses(peerID, raw)
				if err != nil {
					return NetworkAuthoritySnapshot{}, nil, err
				}
				for _, address := range addresses {
					sets[peerID][address.String()] = address
				}
			}
		}
	}
	addresses := make(map[libp2ppeer.ID][]ma.Multiaddr, len(sets))
	for peerID, set := range sets {
		values := make([]ma.Multiaddr, 0, len(set))
		for _, address := range set {
			values = append(values, address)
		}
		sort.Slice(values, func(left, right int) bool {
			return values[left].String() < values[right].String()
		})
		addresses[peerID] = values
	}
	return snapshot, addresses, nil
}

func canonicalPeerAddresses(expected libp2ppeer.ID, raw string) ([]ma.Multiaddr, error) {
	address, err := ma.NewMultiaddr(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid Peer address", ErrMeshRuntime)
	}
	if _, err := address.ValueForProtocol(ma.P_P2P); err == nil {
		info, parseErr := libp2ppeer.AddrInfoFromP2pAddr(address)
		if parseErr != nil || info.ID != expected || len(info.Addrs) != 1 {
			return nil, fmt.Errorf("%w: Peer address identity mismatch", ErrMeshRuntime)
		}
		return info.Addrs, nil
	}
	return []ma.Multiaddr{address}, nil
}

func joinActiveChannels(gossip *Gossip, snapshot NetworkAuthoritySnapshot) error {
	channels := make([]model.ChannelID, 0, len(snapshot.Channels))
	for _, channel := range snapshot.Channels {
		if channel.Status == model.ChannelActive {
			channels = append(channels, channel.ChannelID)
		}
	}
	sort.Slice(channels, func(left, right int) bool {
		return channels[left].String() < channels[right].String()
	})
	for _, channelID := range channels {
		if _, err := gossip.Join(channelID); err != nil {
			return err
		}
	}
	return nil
}
