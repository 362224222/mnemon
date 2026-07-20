package peer

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	ma "github.com/multiformats/go-multiaddr"
)

var ErrMeshAddressSources = errors.New("Mnemon mesh address sources")

type enrollmentAddressSource struct {
	peerID    libp2ppeer.ID
	addresses []ma.Multiaddr
}

// meshAddressSources is the sole owner of Peerstore addresses installed by
// durable mesh authority or short-lived enrollment permits. Its mutex also
// serializes the bounded in-memory Peerstore mutation, preventing a permit
// expiry from racing a durable stage/commit/abort and deleting an address that
// still has another source.
type meshAddressSources struct {
	peerstore peerstore.Peerstore

	mu          sync.Mutex
	durable     map[libp2ppeer.ID][]ma.Multiaddr
	staged      map[libp2ppeer.ID][]ma.Multiaddr
	stage       *MeshAuthorityTransition
	permits     map[outboundEnrollmentPermitRef]enrollmentAddressSource
	initialized bool
	closed      bool
}

func newMeshAddressSources(book peerstore.Peerstore) (*meshAddressSources, error) {
	if book == nil {
		return nil, fmt.Errorf("%w: Peerstore is unavailable", ErrMeshAddressSources)
	}
	return &meshAddressSources{peerstore: book,
		durable: make(map[libp2ppeer.ID][]ma.Multiaddr),
		permits: make(map[outboundEnrollmentPermitRef]enrollmentAddressSource)}, nil
}

func (sources *meshAddressSources) installInitial(
	durable map[libp2ppeer.ID][]ma.Multiaddr,
) error {
	if sources == nil {
		return fmt.Errorf("%w: owner is unavailable", ErrMeshAddressSources)
	}
	sources.mu.Lock()
	defer sources.mu.Unlock()
	if sources.closed || sources.initialized {
		return fmt.Errorf("%w: initial authority was already installed", ErrMeshAddressSources)
	}
	next := cloneManagedAddresses(durable)
	sources.applyEffectiveLocked(nil, next)
	sources.durable = next
	sources.initialized = true
	return nil
}

func (sources *meshAddressSources) stageDurable(transition *MeshAuthorityTransition,
	candidate map[libp2ppeer.ID][]ma.Multiaddr,
) error {
	if sources == nil || transition == nil {
		return fmt.Errorf("%w: transition is unavailable", ErrMeshAddressSources)
	}
	sources.mu.Lock()
	defer sources.mu.Unlock()
	if sources.closed || !sources.initialized || sources.stage != nil {
		return fmt.Errorf("%w: another transition is active", ErrMeshAddressSources)
	}
	previous := sources.effectiveLocked()
	sources.stage = transition
	sources.staged = cloneManagedAddresses(candidate)
	sources.applyEffectiveLocked(previous, sources.effectiveLocked())
	return nil
}

func (sources *meshAddressSources) installDurable(transition *MeshAuthorityTransition) error {
	if sources == nil || transition == nil {
		return fmt.Errorf("%w: transition is unavailable", ErrMeshAddressSources)
	}
	sources.mu.Lock()
	defer sources.mu.Unlock()
	if sources.closed || sources.stage != transition {
		return fmt.Errorf("%w: staged transition identity changed", ErrMeshAddressSources)
	}
	sources.durable = sources.staged
	sources.staged = nil
	sources.stage = nil
	return nil
}

func (sources *meshAddressSources) abortDurable(transition *MeshAuthorityTransition) error {
	if sources == nil || transition == nil {
		return fmt.Errorf("%w: transition is unavailable", ErrMeshAddressSources)
	}
	sources.mu.Lock()
	defer sources.mu.Unlock()
	if sources.closed || sources.stage != transition {
		return fmt.Errorf("%w: staged transition identity changed", ErrMeshAddressSources)
	}
	previous := sources.effectiveLocked()
	sources.staged = nil
	sources.stage = nil
	sources.applyEffectiveLocked(previous, sources.effectiveLocked())
	return nil
}

func (sources *meshAddressSources) addPermit(token outboundEnrollmentPermitToken) error {
	if sources == nil || token.generation == 0 || token.key.ownerPeerID == "" || len(token.addresses) == 0 {
		return fmt.Errorf("%w: permit source is incomplete", ErrMeshAddressSources)
	}
	sources.mu.Lock()
	defer sources.mu.Unlock()
	if sources.closed {
		return fmt.Errorf("%w: owner is closed", ErrMeshAddressSources)
	}
	ref := token.ref()
	if _, duplicate := sources.permits[ref]; duplicate {
		return fmt.Errorf("%w: permit source already exists", ErrMeshAddressSources)
	}
	previous := sources.effectiveLocked()
	sources.permits[ref] = enrollmentAddressSource{peerID: token.key.ownerPeerID,
		addresses: append([]ma.Multiaddr(nil), token.addresses...)}
	sources.applyEffectiveLocked(previous, sources.effectiveLocked())
	return nil
}

func (sources *meshAddressSources) removePermit(ref outboundEnrollmentPermitRef) {
	if sources == nil || ref.generation == 0 {
		return
	}
	sources.mu.Lock()
	defer sources.mu.Unlock()
	if sources.closed {
		return
	}
	if _, exists := sources.permits[ref]; !exists {
		return
	}
	previous := sources.effectiveLocked()
	delete(sources.permits, ref)
	sources.applyEffectiveLocked(previous, sources.effectiveLocked())
}

func (sources *meshAddressSources) close() {
	if sources == nil {
		return
	}
	sources.mu.Lock()
	defer sources.mu.Unlock()
	if sources.closed {
		return
	}
	previous := sources.effectiveLocked()
	sources.closed = true
	sources.durable = nil
	sources.staged = nil
	sources.stage = nil
	sources.permits = nil
	sources.applyEffectiveLocked(previous, nil)
}

func (sources *meshAddressSources) effectiveLocked() map[libp2ppeer.ID][]ma.Multiaddr {
	base := sources.durable
	if sources.stage != nil {
		base = sources.staged
	}
	sets := make(map[libp2ppeer.ID]map[string]ma.Multiaddr, len(base)+len(sources.permits))
	mergeAddressSource(sets, base)
	for _, source := range sources.permits {
		mergeAddressSource(sets, map[libp2ppeer.ID][]ma.Multiaddr{source.peerID: source.addresses})
	}
	return addressSetsToSortedSlices(sets)
}

func (sources *meshAddressSources) applyEffectiveLocked(previous,
	next map[libp2ppeer.ID][]ma.Multiaddr,
) {
	peers := make(map[libp2ppeer.ID]struct{}, len(previous)+len(next))
	for peerID := range previous {
		peers[peerID] = struct{}{}
	}
	for peerID := range next {
		peers[peerID] = struct{}{}
	}
	ordered := make([]libp2ppeer.ID, 0, len(peers))
	for peerID := range peers {
		ordered = append(ordered, peerID)
	}
	sort.Slice(ordered, func(left, right int) bool { return ordered[left] < ordered[right] })
	for _, peerID := range ordered {
		before := addressSliceSet(previous[peerID])
		after := addressSliceSet(next[peerID])
		for value, address := range before {
			if _, retained := after[value]; !retained {
				sources.peerstore.SetAddr(peerID, address, 0)
			}
		}
		for value, address := range after {
			if _, retained := before[value]; !retained {
				sources.peerstore.AddAddr(peerID, address, peerstore.PermanentAddrTTL)
			}
		}
	}
}

func mergeAddressSource(destination map[libp2ppeer.ID]map[string]ma.Multiaddr,
	source map[libp2ppeer.ID][]ma.Multiaddr,
) {
	for peerID, addresses := range source {
		if destination[peerID] == nil {
			destination[peerID] = make(map[string]ma.Multiaddr)
		}
		for _, address := range addresses {
			if address != nil {
				destination[peerID][address.String()] = address
			}
		}
	}
}

func addressSetsToSortedSlices(sets map[libp2ppeer.ID]map[string]ma.Multiaddr) map[libp2ppeer.ID][]ma.Multiaddr {
	result := make(map[libp2ppeer.ID][]ma.Multiaddr, len(sets))
	for peerID, set := range sets {
		for _, address := range set {
			result[peerID] = append(result[peerID], address)
		}
		sort.Slice(result[peerID], func(left, right int) bool {
			return result[peerID][left].String() < result[peerID][right].String()
		})
	}
	return result
}

func addressSliceSet(addresses []ma.Multiaddr) map[string]ma.Multiaddr {
	result := make(map[string]ma.Multiaddr, len(addresses))
	for _, address := range addresses {
		if address != nil {
			result[address.String()] = address
		}
	}
	return result
}

func cloneManagedAddresses(source map[libp2ppeer.ID][]ma.Multiaddr) map[libp2ppeer.ID][]ma.Multiaddr {
	result := make(map[libp2ppeer.ID][]ma.Multiaddr, len(source))
	for peerID, addresses := range source {
		result[peerID] = append([]ma.Multiaddr(nil), addresses...)
	}
	return result
}
