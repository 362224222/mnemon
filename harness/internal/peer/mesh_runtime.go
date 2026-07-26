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

var errMeshRuntimeRevision = errors.New("mesh runtime authority revision changed")

const meshRuntimeReconcileAttempts = 3

// MeshRuntime owns the one libp2p Host, authority projection and Gossip router
// of a Node. Store remains the durable authority; this runtime only installs
// complete Store projections and never invents an independent Channel view.
type MeshRuntime struct {
	nodeHost  *NodeHost
	gossip    *Gossip
	authority *Authority

	// enrollmentMu serializes only outbound Channel joins. It may be held
	// across the bounded exchange without blocking unrelated mesh operations.
	enrollmentMu sync.Mutex
	// peerstoreMu couples each external address update with the exact set that
	// was applied. It never guards logical runtime state or Gossip operations.
	peerstoreMu sync.Mutex
	mu          sync.Mutex
	mesh        store.ChannelMeshAuthority
	applied     map[libp2ppeer.ID][]ma.Multiaddr
	enrollment  *meshEnrollmentPermit
	revision    uint64
	closed      bool
	closeOnce   sync.Once
	closeErr    error
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
		mesh: mesh, applied: cloneManagedAddresses(addresses), revision: 1}, nil
}

// Reconcile publishes one complete post-commit Store authority. Store mutation
// belongs to the caller and is never hidden behind a runtime callback. Newly
// active Channels are joined before success returns.
func (runtime *MeshRuntime) Reconcile(mesh store.ChannelMeshAuthority) error {
	if runtime == nil {
		return fmt.Errorf("%w: runtime is required", ErrMeshRuntime)
	}
	if _, _, err := projectMeshRuntime(mesh); err != nil {
		return err
	}
	runtime.mu.Lock()
	if runtime.closed || runtime.nodeHost == nil || runtime.gossip == nil {
		runtime.mu.Unlock()
		return fmt.Errorf("%w: runtime is closed", ErrMeshRuntime)
	}
	runtime.mesh = mesh
	runtime.revision++
	revision := runtime.revision
	runtime.mu.Unlock()

	err := runtime.reconcileProjectionOnce(revision)
	if !errors.Is(err, errMeshRuntimeRevision) {
		return err
	}
	return runtime.reconcileCurrentProjection()
}

func (runtime *MeshRuntime) reconcileCurrentProjection() error {
	for attempt := 0; attempt < meshRuntimeReconcileAttempts; attempt++ {
		err := runtime.reconcileProjectionOnce(0)
		if !errors.Is(err, errMeshRuntimeRevision) {
			return err
		}
	}
	return runtime.failClosed(fmt.Errorf("%w: authority did not stabilize after %d attempts",
		ErrMeshRuntime, meshRuntimeReconcileAttempts))
}

func (runtime *MeshRuntime) reconcileProjectionOnce(expectedRevision uint64) error {
	runtime.mu.Lock()
	if runtime.closed || runtime.nodeHost == nil || runtime.gossip == nil {
		runtime.mu.Unlock()
		return fmt.Errorf("%w: runtime is closed", ErrMeshRuntime)
	}
	if expectedRevision != 0 && runtime.revision != expectedRevision {
		runtime.mu.Unlock()
		return errMeshRuntimeRevision
	}
	mesh := runtime.mesh
	permit := runtime.enrollment
	revision := runtime.revision
	nodeHost, gossip := runtime.nodeHost, runtime.gossip
	runtime.mu.Unlock()

	snapshot, addresses, err := projectMeshRuntime(mesh)
	if err != nil {
		return err
	}
	snapshot, addresses, err = overlayEnrollmentPermit(snapshot, addresses, permit)
	if err != nil {
		return err
	}
	runtime.peerstoreMu.Lock()
	runtime.mu.Lock()
	if runtime.closed {
		runtime.mu.Unlock()
		runtime.peerstoreMu.Unlock()
		return fmt.Errorf("%w: runtime is closed", ErrMeshRuntime)
	}
	if runtime.revision != revision || runtime.enrollment != permit {
		runtime.mu.Unlock()
		runtime.peerstoreMu.Unlock()
		return errMeshRuntimeRevision
	}
	previous := cloneManagedAddresses(runtime.applied)
	runtime.mu.Unlock()
	applyManagedAddresses(nodeHost.Host(), previous, addresses)
	runtime.mu.Lock()
	runtime.applied = cloneManagedAddresses(addresses)
	runtime.mu.Unlock()
	runtime.peerstoreMu.Unlock()
	if err := gossip.Reconcile(snapshot); err != nil {
		return runtime.failClosed(fmt.Errorf("%w: reconcile authority: %v", ErrMeshRuntime, err))
	}
	if err := joinActiveChannels(gossip, snapshot); err != nil {
		return runtime.failClosed(fmt.Errorf("%w: join reconciled topics: %v", ErrMeshRuntime, err))
	}

	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed {
		return fmt.Errorf("%w: runtime is closed", ErrMeshRuntime)
	}
	if runtime.revision != revision || runtime.enrollment != permit {
		return errMeshRuntimeRevision
	}
	return nil
}

func (runtime *MeshRuntime) failClosed(cause error) error {
	runtime.mu.Lock()
	runtime.closed = true
	runtime.revision++
	gossip, nodeHost := runtime.gossip, runtime.nodeHost
	runtime.mu.Unlock()
	if gossip != nil {
		cause = errors.Join(cause, gossip.Close())
	}
	if nodeHost != nil {
		cause = errors.Join(cause, nodeHost.Close())
	}
	return cause
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
	if runtime.closed || runtime.gossip == nil {
		runtime.mu.Unlock()
		return nil, fmt.Errorf("%w: runtime is closed", ErrMeshRuntime)
	}
	gossip, revision := runtime.gossip, runtime.revision
	runtime.mu.Unlock()
	session, err := gossip.Join(channelID)
	if err != nil {
		return nil, fmt.Errorf("%w: acquire Channel session: %w", ErrMeshRuntime, err)
	}
	runtime.mu.Lock()
	current := !runtime.closed && runtime.gossip == gossip && runtime.revision == revision
	runtime.mu.Unlock()
	if !current {
		return nil, fmt.Errorf("%w: Channel session authority changed", ErrMeshRuntime)
	}
	return session, nil
}

func (runtime *MeshRuntime) HasCurrentSession(channelID model.ChannelID) bool {
	if runtime == nil {
		return false
	}
	runtime.mu.Lock()
	if runtime.closed || runtime.gossip == nil {
		runtime.mu.Unlock()
		return false
	}
	gossip, revision := runtime.gossip, runtime.revision
	runtime.mu.Unlock()
	current := gossip.HasCurrentSession(channelID)
	runtime.mu.Lock()
	stable := !runtime.closed && runtime.gossip == gossip && runtime.revision == revision
	runtime.mu.Unlock()
	return stable && current
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
