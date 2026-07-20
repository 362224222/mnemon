package peer

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"sync"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	ma "github.com/multiformats/go-multiaddr"
)

// controlledAddrsFactory is installed before the listener opens, then frozen
// once from the held listener. libp2p callbacks only observe bounded immutable
// copies and never become an address authority of their own.
type controlledAddrsFactory struct {
	mu     sync.RWMutex
	seed   []ma.Multiaddr
	frozen []ma.Multiaddr
}

func newControlledAddrsFactory(seed []ma.Multiaddr) *controlledAddrsFactory {
	return &controlledAddrsFactory{seed: cloneMultiaddrs(seed)}
}

func (factory *controlledAddrsFactory) apply(_ []ma.Multiaddr) []ma.Multiaddr {
	factory.mu.RLock()
	defer factory.mu.RUnlock()
	if len(factory.frozen) != 0 {
		return cloneMultiaddrs(factory.frozen)
	}
	return cloneMultiaddrs(factory.seed)
}

func (factory *controlledAddrsFactory) freeze(candidates []ma.Multiaddr,
	port uint16,
) ([]ma.Multiaddr, error) {
	addresses := cloneMultiaddrs(factory.seed)
	if len(addresses) == 0 {
		addresses = selectAdvertisedAddrs(candidates, port)
	}
	if len(addresses) == 0 || len(addresses) > model.MaxMemberMultiaddrs {
		return nil, fmt.Errorf("%w: advertised address count is invalid", ErrMeshHost)
	}
	factory.mu.Lock()
	if len(factory.frozen) != 0 {
		factory.mu.Unlock()
		return nil, fmt.Errorf("%w: address factory was already frozen", ErrMeshHost)
	}
	factory.frozen = cloneMultiaddrs(addresses)
	factory.mu.Unlock()
	return cloneMultiaddrs(addresses), nil
}

func inspectRequestedListener(values []ma.Multiaddr) (ma.Multiaddr, uint16, error) {
	if len(values) != 1 || values[0] == nil {
		return nil, 0, fmt.Errorf("%w: exactly one listener is required", ErrMeshHost)
	}
	listener, port, err := inspectDirectTCPAddr(values[0], true, true)
	if err != nil {
		return nil, 0, err
	}
	return listener, port, nil
}

func inspectActualListener(requested ma.Multiaddr,
	values []ma.Multiaddr,
) (ma.Multiaddr, uint16, error) {
	if len(values) != 1 || values[0] == nil {
		return nil, 0, fmt.Errorf("%w: Host did not bind exactly one listener", ErrMeshHost)
	}
	actual, port, err := inspectDirectTCPAddr(values[0], true, false)
	if err != nil {
		return nil, 0, err
	}
	_, requestedPort, err := inspectDirectTCPAddr(requested, true, true)
	if err != nil || requestedPort != 0 && requestedPort != port {
		return nil, 0, fmt.Errorf("%w: actual listener port drifted", ErrMeshHost)
	}
	requestedParts, actualParts := ma.Split(requested), ma.Split(actual)
	if len(requestedParts) != 2 || len(actualParts) != 2 ||
		requestedParts[0].String() != actualParts[0].String() {
		return nil, 0, fmt.Errorf("%w: actual listener address drifted", ErrMeshHost)
	}
	return actual, port, nil
}

func inspectAdvertisedSeed(values []ma.Multiaddr, listenerPort uint16) ([]ma.Multiaddr, error) {
	if len(values) == 0 {
		return nil, nil
	}
	if listenerPort == 0 || len(values) > model.MaxMemberMultiaddrs {
		return nil, fmt.Errorf("%w: advertised seed requires one bounded nonzero-port listener", ErrMeshHost)
	}
	set := make(map[string]ma.Multiaddr, len(values))
	for _, value := range values {
		address, port, err := inspectDirectTCPAddr(value, false, false)
		if err != nil || port != listenerPort {
			return nil, errors.Join(err, fmt.Errorf("%w: advertised and listener ports differ", ErrMeshHost))
		}
		if _, duplicate := set[address.String()]; duplicate {
			return nil, fmt.Errorf("%w: duplicate advertised address", ErrMeshHost)
		}
		set[address.String()] = address
	}
	return sortedMultiaddrs(set), nil
}

func selectAdvertisedAddrs(values []ma.Multiaddr, listenerPort uint16) []ma.Multiaddr {
	set := make(map[string]ma.Multiaddr)
	for _, value := range values {
		address, port, err := inspectDirectTCPAddr(value, false, false)
		if err == nil && port == listenerPort {
			set[address.String()] = address
		}
	}
	addresses := sortedMultiaddrs(set)
	if len(addresses) > model.MaxMemberMultiaddrs {
		addresses = addresses[:model.MaxMemberMultiaddrs]
	}
	return addresses
}

func inspectDirectTCPAddr(value ma.Multiaddr, listener, allowZero bool) (
	ma.Multiaddr, uint16, error,
) {
	canonical, first, err := inspectDirectTCPShape(value, listener)
	if err != nil {
		return nil, 0, err
	}
	port, err := inspectDirectTCPPort(canonical, allowZero)
	if err != nil {
		return nil, 0, err
	}
	if err := inspectDirectTCPIPClass(canonical, first, listener); err != nil {
		return nil, 0, err
	}
	return canonical, port, nil
}

func inspectDirectTCPShape(value ma.Multiaddr, listener bool) (ma.Multiaddr, int, error) {
	if value == nil {
		return nil, 0, fmt.Errorf("%w: nil address", ErrMeshHost)
	}
	canonical, err := ma.NewMultiaddr(value.String())
	protocols := value.Protocols()
	if err != nil || canonical.String() != value.String() || len(protocols) != 2 ||
		protocols[1].Code != ma.P_TCP {
		return nil, 0, fmt.Errorf("%w: address must be canonical direct TCP", ErrMeshHost)
	}
	first := protocols[0].Code
	if !directTCPProtocolPermitted(first, listener) {
		return nil, 0, fmt.Errorf("%w: address protocol is not permitted", ErrMeshHost)
	}
	return canonical, first, nil
}

func directTCPProtocolPermitted(protocolCode int, listener bool) bool {
	if protocolCode == ma.P_IP4 || protocolCode == ma.P_IP6 {
		return true
	}
	return !listener && (protocolCode == ma.P_DNS4 || protocolCode == ma.P_DNS6)
}

func inspectDirectTCPPort(canonical ma.Multiaddr, allowZero bool) (uint16, error) {
	portText, err := canonical.ValueForProtocol(ma.P_TCP)
	parsed, parseErr := strconv.ParseUint(portText, 10, 16)
	if err != nil || parseErr != nil || parsed == 0 && !allowZero {
		return 0, fmt.Errorf("%w: TCP port is invalid", ErrMeshHost)
	}
	return uint16(parsed), nil
}

func inspectDirectTCPIPClass(canonical ma.Multiaddr, protocolCode int, listener bool) error {
	if protocolCode != ma.P_IP4 && protocolCode != ma.P_IP6 {
		return nil
	}
	hostValue, err := canonical.ValueForProtocol(protocolCode)
	ip := net.ParseIP(hostValue)
	if err != nil || ip == nil || !ip.IsGlobalUnicast() && !ip.IsLoopback() &&
		(!listener || !ip.IsUnspecified()) {
		return fmt.Errorf("%w: IP address class is not permitted", ErrMeshHost)
	}
	return nil
}

func verifyHostAdvertisement(nodeHost *NodeHost, want []string) error {
	got := meshHostAddrStrings(nodeHost.managedRuntimeHost().Addrs())
	if len(got) != len(want) {
		return fmt.Errorf("%w: controlled Host advertisement drifted", ErrMeshHost)
	}
	for index := range want {
		if got[index] != want[index] {
			return fmt.Errorf("%w: controlled Host advertisement drifted", ErrMeshHost)
		}
	}
	if _, err := model.AdvertisedAddressDigest(want); err != nil {
		return fmt.Errorf("%w: advertised snapshot: %v", ErrMeshHost, err)
	}
	return nil
}

func cloneMultiaddrs(values []ma.Multiaddr) []ma.Multiaddr {
	result := make([]ma.Multiaddr, 0, len(values))
	for _, value := range values {
		if value == nil {
			result = append(result, nil)
			continue
		}
		clone, err := ma.NewMultiaddrBytes(value.Bytes())
		if err != nil {
			result = append(result, nil)
			continue
		}
		result = append(result, clone)
	}
	return result
}

func sortedMultiaddrs(set map[string]ma.Multiaddr) []ma.Multiaddr {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]ma.Multiaddr, 0, len(keys))
	for _, key := range keys {
		result = append(result, set[key])
	}
	return result
}

func meshHostAddrStrings(values []ma.Multiaddr) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value != nil {
			result = append(result, value.String())
		}
	}
	sort.Strings(result)
	return result
}
