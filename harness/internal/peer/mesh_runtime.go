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
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	ma "github.com/multiformats/go-multiaddr"
)

var ErrMeshRuntime = errors.New("Mnemon mesh runtime")

// MeshRuntime owns the one libp2p Host, authority projection and Gossip router
// of a Node. Store remains the durable authority; this runtime only installs
// complete Store projections and never invents an independent Channel view.
type MeshRuntime struct {
	nodeHost  *NodeHost
	gossip    *Gossip
	authority *Authority

	mu        sync.Mutex
	addresses map[libp2ppeer.ID][]ma.Multiaddr
	closed    bool
	closeOnce sync.Once
	closeErr  error
}

// NewMeshRuntime starts from one coherent Store snapshot. The Host begins with
// empty authority, canonical Peer addresses are installed, and Gossip performs
// the first whole-snapshot reconciliation before any Channel topic is joined.
func NewMeshRuntime(ctx context.Context, privateKey libp2pcrypto.PrivKey,
	listenAddrs []ma.Multiaddr, mesh store.ChannelMeshAuthority,
) (*MeshRuntime, error) {
	if ctx == nil || ctx.Err() != nil || privateKey == nil {
		return nil, fmt.Errorf("%w: live context and Node key are required", ErrMeshRuntime)
	}
	snapshot, addresses, err := projectMeshRuntime(mesh)
	if err != nil {
		return nil, err
	}
	authority, err := NewAuthority(snapshot.LocalPeerID)
	if err != nil {
		return nil, fmt.Errorf("%w: construct authority: %w", ErrMeshRuntime, err)
	}
	nodeHost, err := NewNodeHost(privateKey, authority, listenAddrs)
	if err != nil {
		return nil, fmt.Errorf("%w: construct Host: %w", ErrMeshRuntime, err)
	}
	fail := func(cause error, gossip *Gossip) (*MeshRuntime, error) {
		if gossip != nil {
			cause = errors.Join(cause, gossip.Close())
		}
		return nil, errors.Join(cause, nodeHost.Close())
	}
	applyManagedAddresses(nodeHost.Host(), nil, addresses)
	gossip, err := NewGossip(ctx, nodeHost.Host(), authority)
	if err != nil {
		return fail(fmt.Errorf("%w: construct Gossip router: %w", ErrMeshRuntime, err), nil)
	}
	if err := gossip.Reconcile(snapshot); err != nil {
		return fail(fmt.Errorf("%w: install initial authority: %w", ErrMeshRuntime, err), gossip)
	}
	if err := joinActiveChannels(gossip, snapshot); err != nil {
		return fail(fmt.Errorf("%w: join initial topics: %w", ErrMeshRuntime, err), gossip)
	}
	return &MeshRuntime{nodeHost: nodeHost, gossip: gossip, authority: authority,
		addresses: addresses}, nil
}

// ReconcileWithCommit serializes a complete authority transition. Candidate
// addresses are staged before Gossip computes connection promotions; a failed
// durable callback restores the exact previous address set and leaves runtime
// authority/sessions untouched. Newly active Channels are joined before the
// transition reports success.
func (runtime *MeshRuntime) ReconcileWithCommit(mesh store.ChannelMeshAuthority,
	commit func() error,
) error {
	if runtime == nil || commit == nil {
		return fmt.Errorf("%w: runtime and durable commit are required", ErrMeshRuntime)
	}
	snapshot, addresses, err := projectMeshRuntime(mesh)
	if err != nil {
		return err
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed || runtime.nodeHost == nil || runtime.gossip == nil {
		return fmt.Errorf("%w: runtime is closed", ErrMeshRuntime)
	}
	previous := cloneManagedAddresses(runtime.addresses)
	applyManagedAddresses(runtime.nodeHost.Host(), previous, addresses)
	if err := runtime.gossip.ReconcileWithCommit(snapshot, commit); err != nil {
		applyManagedAddresses(runtime.nodeHost.Host(), addresses, previous)
		return fmt.Errorf("%w: reconcile authority: %w", ErrMeshRuntime, err)
	}
	runtime.addresses = addresses
	if err := joinActiveChannels(runtime.gossip, snapshot); err != nil {
		// Durable authority is already installed. Stop Gossip fail-closed rather
		// than run a partial set of Channel topics until daemon restart.
		runtime.closed = true
		return errors.Join(fmt.Errorf("%w: join reconciled topics: %w", ErrMeshRuntime, err),
			runtime.gossip.Close())
	}
	return nil
}

func (runtime *MeshRuntime) Host() host.Host {
	if runtime == nil || runtime.nodeHost == nil {
		return nil
	}
	return runtime.nodeHost.Host()
}

func (runtime *MeshRuntime) Session(channelID model.ChannelID) (*TopicSession, error) {
	if runtime == nil {
		return nil, fmt.Errorf("%w: runtime is unavailable", ErrMeshRuntime)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed || runtime.gossip == nil {
		return nil, fmt.Errorf("%w: runtime is closed", ErrMeshRuntime)
	}
	session, err := runtime.gossip.Join(channelID)
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
	defer runtime.mu.Unlock()
	return !runtime.closed && runtime.gossip != nil && runtime.gossip.HasCurrentSession(channelID)
}

func (runtime *MeshRuntime) Close() error {
	if runtime == nil {
		return nil
	}
	runtime.closeOnce.Do(func() {
		runtime.mu.Lock()
		runtime.closed = true
		gossip := runtime.gossip
		nodeHost := runtime.nodeHost
		runtime.mu.Unlock()
		if gossip != nil {
			runtime.closeErr = errors.Join(runtime.closeErr, gossip.Close())
		}
		if nodeHost != nil {
			runtime.closeErr = errors.Join(runtime.closeErr, nodeHost.Close())
		}
	})
	return runtime.closeErr
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

func applyManagedAddresses(nodeHost host.Host, previous,
	next map[libp2ppeer.ID][]ma.Multiaddr,
) {
	if nodeHost == nil {
		return
	}
	for peerID, addresses := range previous {
		if len(addresses) > 0 {
			nodeHost.Peerstore().SetAddrs(peerID, addresses, 0)
		}
	}
	for peerID, addresses := range next {
		if len(addresses) > 0 {
			nodeHost.Peerstore().AddAddrs(peerID, addresses, peerstore.PermanentAddrTTL)
		}
	}
}

func cloneManagedAddresses(source map[libp2ppeer.ID][]ma.Multiaddr) map[libp2ppeer.ID][]ma.Multiaddr {
	result := make(map[libp2ppeer.ID][]ma.Multiaddr, len(source))
	for peerID, addresses := range source {
		result[peerID] = append([]ma.Multiaddr(nil), addresses...)
	}
	return result
}
