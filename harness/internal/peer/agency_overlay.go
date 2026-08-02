package peer

import (
	"errors"
	"fmt"
	"sort"

	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	ma "github.com/multiformats/go-multiaddr"
)

// AgencyPeerRoute is one owner-provisioned physical route. It carries no
// Agent, Event, Handling, or delivery semantics; those remain above peer.
type AgencyPeerRoute struct {
	PeerID     model.PeerID
	Multiaddrs []string
}

// ReconcileAgencyPeers replaces the complete owner-provisioned Agency route
// overlay. It shares the Host and physical connection budget with R5 while
// preserving independent protocol authority in the same immutable revision.
func (runtime *MeshRuntime) ReconcileAgencyPeers(routes []AgencyPeerRoute) error {
	if runtime == nil || runtime.authority == nil {
		return fmt.Errorf("%w: runtime is required", ErrMeshRuntime)
	}
	normalized, _, err := normalizeAgencyPeerRoutes(runtime.authority.LocalPeerID(), routes)
	if err != nil {
		return err
	}
	runtime.mu.Lock()
	if runtime.closed || runtime.nodeHost == nil || runtime.gossip == nil {
		runtime.mu.Unlock()
		return fmt.Errorf("%w: runtime is closed", ErrMeshRuntime)
	}
	runtime.agencyPeers = cloneAgencyPeerRoutes(normalized)
	runtime.revision++
	revision := runtime.revision
	runtime.mu.Unlock()

	err = runtime.reconcileProjectionOnce(revision)
	if !errors.Is(err, errMeshRuntimeRevision) {
		return err
	}
	return runtime.reconcileCurrentProjection()
}

func normalizeAgencyPeerRoutes(local libp2ppeer.ID,
	routes []AgencyPeerRoute,
) ([]AgencyPeerRoute, map[libp2ppeer.ID][]ma.Multiaddr, error) {
	if local == "" || len(routes) > maxAgencyPeers {
		return nil, nil, fmt.Errorf("%w: invalid Agency Peer route snapshot", ErrMeshRuntime)
	}
	normalized := make([]AgencyPeerRoute, 0, len(routes))
	addresses := make(map[libp2ppeer.ID][]ma.Multiaddr, len(routes))
	for _, route := range routes {
		peerID, err := canonicalLibp2pID(route.PeerID)
		if err != nil || peerID == local || len(route.Multiaddrs) == 0 ||
			len(route.Multiaddrs) > model.MaxMemberMultiaddrs {
			return nil, nil, fmt.Errorf("%w: invalid Agency Peer route", ErrMeshRuntime)
		}
		if _, duplicate := addresses[peerID]; duplicate {
			return nil, nil, fmt.Errorf("%w: duplicate Agency Peer route", ErrMeshRuntime)
		}
		canonical := make([]ma.Multiaddr, 0, len(route.Multiaddrs))
		seen := make(map[string]struct{}, len(route.Multiaddrs))
		for _, raw := range route.Multiaddrs {
			parsed, parseErr := canonicalPeerAddresses(peerID, raw)
			if parseErr != nil || len(parsed) != 1 {
				return nil, nil, fmt.Errorf("%w: invalid Agency Peer address", ErrMeshRuntime)
			}
			key := parsed[0].String()
			if _, duplicate := seen[key]; duplicate {
				return nil, nil, fmt.Errorf("%w: duplicate Agency Peer address", ErrMeshRuntime)
			}
			seen[key] = struct{}{}
			canonical = append(canonical, parsed[0])
		}
		sort.Slice(canonical, func(left, right int) bool {
			return canonical[left].String() < canonical[right].String()
		})
		canonicalStrings := make([]string, len(canonical))
		for index, address := range canonical {
			canonicalStrings[index] = address.String()
		}
		normalized = append(normalized, AgencyPeerRoute{PeerID: route.PeerID,
			Multiaddrs: canonicalStrings})
		addresses[peerID] = canonical
	}
	sort.Slice(normalized, func(left, right int) bool {
		return normalized[left].PeerID.String() < normalized[right].PeerID.String()
	})
	return normalized, addresses, nil
}

func overlayAgencyPeerRoutes(snapshot NetworkAuthoritySnapshot,
	addresses map[libp2ppeer.ID][]ma.Multiaddr, routes []AgencyPeerRoute,
) (NetworkAuthoritySnapshot, map[libp2ppeer.ID][]ma.Multiaddr, error) {
	local, err := canonicalLibp2pID(snapshot.LocalPeerID)
	if err != nil {
		return NetworkAuthoritySnapshot{}, nil,
			fmt.Errorf("%w: invalid local identity", ErrMeshRuntime)
	}
	normalized, agencyAddresses, err := normalizeAgencyPeerRoutes(local, routes)
	if err != nil {
		return NetworkAuthoritySnapshot{}, nil, err
	}
	snapshot.AgencyPeers = make([]model.PeerID, len(normalized))
	combined := cloneManagedAddresses(addresses)
	for index, route := range normalized {
		snapshot.AgencyPeers[index] = route.PeerID
		peerID, parseErr := canonicalLibp2pID(route.PeerID)
		if parseErr != nil {
			return NetworkAuthoritySnapshot{}, nil,
				fmt.Errorf("%w: normalized Agency Peer identity: %v", ErrMeshRuntime, parseErr)
		}
		combined[peerID] = mergeManagedAddresses(combined[peerID], agencyAddresses[peerID])
	}
	return snapshot, combined, nil
}

func projectMeshRuntimeAuthority(mesh store.ChannelMeshAuthority, routes []AgencyPeerRoute,
	permit *meshEnrollmentPermit,
) (NetworkAuthoritySnapshot, map[libp2ppeer.ID][]ma.Multiaddr, error) {
	snapshot, addresses, err := projectMeshRuntime(mesh)
	if err != nil {
		return NetworkAuthoritySnapshot{}, nil, err
	}
	snapshot, addresses, err = overlayAgencyPeerRoutes(snapshot, addresses, routes)
	if err != nil {
		return NetworkAuthoritySnapshot{}, nil, err
	}
	return overlayEnrollmentPermit(snapshot, addresses, permit)
}

func mergeManagedAddresses(left, right []ma.Multiaddr) []ma.Multiaddr {
	seen := make(map[string]ma.Multiaddr, len(left)+len(right))
	for _, address := range append(append([]ma.Multiaddr(nil), left...), right...) {
		if address != nil {
			seen[address.String()] = address
		}
	}
	result := make([]ma.Multiaddr, 0, len(seen))
	for _, address := range seen {
		result = append(result, address)
	}
	sort.Slice(result, func(left, right int) bool {
		return result[left].String() < result[right].String()
	})
	return result
}

func cloneAgencyPeerRoutes(routes []AgencyPeerRoute) []AgencyPeerRoute {
	result := make([]AgencyPeerRoute, len(routes))
	for index, route := range routes {
		result[index] = AgencyPeerRoute{PeerID: route.PeerID,
			Multiaddrs: append([]string(nil), route.Multiaddrs...)}
	}
	return result
}
