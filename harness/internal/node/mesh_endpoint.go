package node

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"slices"
	"sort"
	"strconv"

	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	ma "github.com/multiformats/go-multiaddr"
)

const maxMeshEndpointBytes = int64(4 << 10)

var (
	errMeshEndpointAuthority = errors.New("mnemond mesh endpoint authority is invalid")
	errMeshEndpointConflict  = errors.New("mnemond mesh endpoint authority conflicts with durable state")
)

type meshEndpointPendingSpec struct {
	PeerID          model.PeerID
	ListenAddrs     []string
	AdvertisedAddrs []string
}

type meshEndpointSpec struct {
	PeerID          model.PeerID
	ListenAddrs     []string
	AdvertisedAddrs []string
}

type meshEndpointValue struct {
	peerID          model.PeerID
	listenAddrs     []string
	advertisedAddrs []string
	canonical       []byte
}

type meshEndpointPending struct{ value meshEndpointValue }
type meshEndpoint struct{ value meshEndpointValue }

func newMeshEndpointPending(spec meshEndpointPendingSpec) (meshEndpointPending, error) {
	value, err := newMeshEndpointValue(spec.PeerID, spec.ListenAddrs, spec.AdvertisedAddrs, false)
	return meshEndpointPending{value: value}, err
}

func newMeshEndpoint(spec meshEndpointSpec) (meshEndpoint, error) {
	value, err := newMeshEndpointValue(spec.PeerID, spec.ListenAddrs, spec.AdvertisedAddrs, true)
	return meshEndpoint{value: value}, err
}

func (pending meshEndpointPending) peerIDValue() model.PeerID { return pending.value.peerID }
func (pending meshEndpointPending) listenAddresses() []string {
	return append([]string(nil), pending.value.listenAddrs...)
}
func (pending meshEndpointPending) advertisedAddresses() []string {
	return append([]string(nil), pending.value.advertisedAddrs...)
}
func (pending meshEndpointPending) canonicalJSON() []byte {
	return append([]byte(nil), pending.value.canonical...)
}
func (endpoint meshEndpoint) peerIDValue() model.PeerID { return endpoint.value.peerID }
func (endpoint meshEndpoint) listenAddresses() []string {
	return append([]string(nil), endpoint.value.listenAddrs...)
}
func (endpoint meshEndpoint) advertisedAddresses() []string {
	return append([]string(nil), endpoint.value.advertisedAddrs...)
}
func (endpoint meshEndpoint) canonicalJSON() []byte {
	return append([]byte(nil), endpoint.value.canonical...)
}

type meshEndpointStateKind uint8

const (
	meshEndpointStateAbsent meshEndpointStateKind = iota + 1
	meshEndpointStatePending
	meshEndpointStateFinal
	meshEndpointStateFinalWithPending
)

type meshEndpointState struct {
	kind    meshEndpointStateKind
	pending meshEndpointPending
	final   meshEndpoint
}

func (state meshEndpointState) stateKind() meshEndpointStateKind { return state.kind }
func (state meshEndpointState) pendingAuthority() (meshEndpointPending, bool) {
	return state.pending, state.kind == meshEndpointStatePending || state.kind == meshEndpointStateFinalWithPending
}
func (state meshEndpointState) finalAuthority() (meshEndpoint, bool) {
	return state.final, state.kind == meshEndpointStateFinal || state.kind == meshEndpointStateFinalWithPending
}

func inspectMeshEndpointState(nodeState string, expected model.PeerID) (meshEndpointState, error) {
	if !validMeshEndpointPeerID(expected) {
		return meshEndpointState{}, meshEndpointError("inspect", errors.New("expected PeerID is invalid"))
	}
	state, err := openIdentityNodeState(nodeState)
	if err != nil {
		return meshEndpointState{}, meshEndpointError("inspect", err)
	}
	defer state.close()
	if err := state.lock(); err != nil {
		return meshEndpointState{}, meshEndpointError("inspect", err)
	}
	defer state.unlock()
	return inspectMeshEndpointStateLocked(state, expected)
}

func inspectMeshEndpointStateLocked(state *identityNodeState,
	expected model.PeerID,
) (meshEndpointState, error) {
	if err := verifyMeshEndpointIdentityLocked(state, expected); err != nil {
		return meshEndpointState{}, err
	}
	if err := cleanupMeshEndpointStages(state); err != nil {
		return meshEndpointState{}, err
	}
	pendingValue, pending, err := readMeshEndpointFile(state, meshEndpointPendingName, false)
	if err != nil {
		return meshEndpointState{}, err
	}
	finalValue, final, err := readMeshEndpointFile(state, meshEndpointName, true)
	if err != nil {
		return meshEndpointState{}, err
	}
	if pending && pendingValue.peerID != expected || final && finalValue.peerID != expected {
		return meshEndpointState{}, meshEndpointError("inspect", errors.New("endpoint PeerID differs from Node identity"))
	}
	result := meshEndpointState{kind: meshEndpointStateAbsent}
	if pending {
		result.kind, result.pending = meshEndpointStatePending, meshEndpointPending{value: pendingValue}
	}
	if final {
		result.kind, result.final = meshEndpointStateFinal, meshEndpoint{value: finalValue}
	}
	if pending && final {
		if !meshEndpointAdvances(pendingValue, finalValue) {
			return meshEndpointState{}, meshEndpointError("inspect", errMeshEndpointConflict)
		}
		result.kind = meshEndpointStateFinalWithPending
	}
	return result, nil
}

func verifyMeshEndpointIdentityLocked(state *identityNodeState, expected model.PeerID) error {
	if err := state.cleanupStaging(); err != nil {
		return meshEndpointError("clean Node identity staging", err)
	}
	identity, err := state.load()
	if err != nil {
		return meshEndpointError("load Node identity", err)
	}
	if identity.PeerID() != expected {
		return meshEndpointError("bind Node identity", errors.New("endpoint PeerID differs from identity.key"))
	}
	return nil
}

type meshEndpointWire struct {
	AdvertisedAddrs []string `json:"advertised_addrs"`
	ListenAddrs     []string `json:"listen_addrs"`
	PeerID          string   `json:"peer_id"`
	SchemaVersion   int      `json:"schema_version"`
}

func decodeMeshEndpointValue(raw []byte, final bool) (meshEndpointValue, error) {
	canonical, err := model.CanonicalizeJSON(raw)
	if err != nil || !bytes.Equal(raw, canonical) {
		return meshEndpointValue{}, errors.Join(err, errors.New("endpoint JSON is not exact canonical JSON"))
	}
	var wire meshEndpointWire
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return meshEndpointValue{}, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF || wire.SchemaVersion != model.SchemaVersion {
		return meshEndpointValue{}, errors.New("endpoint JSON has trailing data or unsupported schema")
	}
	peerID, err := model.ParsePeerID(wire.PeerID)
	if err != nil {
		return meshEndpointValue{}, err
	}
	value, err := newMeshEndpointValue(peerID, wire.ListenAddrs, wire.AdvertisedAddrs, final)
	if err != nil || !bytes.Equal(value.canonical, raw) {
		return meshEndpointValue{}, errors.Join(err, errors.New("endpoint JSON projection is not canonical"))
	}
	return value, nil
}

func newMeshEndpointValue(peerID model.PeerID, listen, advertised []string,
	final bool,
) (meshEndpointValue, error) {
	if !validMeshEndpointPeerID(peerID) || len(listen) != 1 {
		return meshEndpointValue{}, meshEndpointError("construct", errors.New("PeerID and one listener are required"))
	}
	listeners, listenPort, err := normalizeMeshEndpointAddrs(listen, true, !final)
	if err != nil {
		return meshEndpointValue{}, err
	}
	if final && (len(advertised) == 0 || len(advertised) > model.MaxMemberMultiaddrs) ||
		!final && len(advertised) > model.MaxMemberMultiaddrs {
		return meshEndpointValue{}, meshEndpointError("construct", errors.New("advertised address count is invalid"))
	}
	addresses, _, err := normalizeMeshEndpointAddrs(advertised, false, false)
	if err != nil {
		return meshEndpointValue{}, err
	}
	if listenPort == 0 && len(addresses) != 0 {
		return meshEndpointValue{}, meshEndpointError("construct", errors.New("an automatic listener cannot predeclare addresses"))
	}
	for _, address := range addresses {
		_, port, _ := inspectMeshEndpointAddr(address, false, false)
		if port != listenPort {
			return meshEndpointValue{}, meshEndpointError("construct", errors.New("listen and advertised ports differ"))
		}
	}
	wire := meshEndpointWire{AdvertisedAddrs: addresses, ListenAddrs: listeners,
		PeerID: peerID.String(), SchemaVersion: model.SchemaVersion}
	canonical, err := model.CanonicalMarshal(wire)
	if err != nil || len(canonical) > int(maxMeshEndpointBytes) {
		return meshEndpointValue{}, meshEndpointError("construct", errors.Join(err, errors.New("endpoint encoding is invalid")))
	}
	return meshEndpointValue{peerID: peerID, listenAddrs: listeners,
		advertisedAddrs: addresses, canonical: canonical}, nil
}

func normalizeMeshEndpointAddrs(values []string, listener, allowZero bool) ([]string, int, error) {
	result := make([]string, len(values))
	port := 0
	for index, raw := range values {
		canonical, current, err := inspectMeshEndpointAddr(raw, listener, allowZero)
		if err != nil {
			return nil, 0, err
		}
		result[index], port = canonical, current
	}
	sort.Strings(result)
	for index := 1; index < len(result); index++ {
		if result[index] == result[index-1] {
			return nil, 0, meshEndpointError("construct", errors.New("duplicate address"))
		}
	}
	return result, port, nil
}

func inspectMeshEndpointAddr(raw string, listener, allowZero bool) (string, int, error) {
	address, err := ma.NewMultiaddr(raw)
	if err != nil || address.String() != raw {
		return "", 0, meshEndpointError("construct", errors.New("address is not canonical"))
	}
	protocols := address.Protocols()
	if !validMeshEndpointProtocols(protocols, listener) {
		return "", 0, meshEndpointError("construct", errors.New("address must be direct TCP"))
	}
	parsed, err := meshEndpointTCPPort(address, allowZero)
	if err != nil {
		return "", 0, meshEndpointError("construct", errors.New("TCP port is invalid"))
	}
	if !validMeshEndpointAddressClass(address, protocols[0].Code, listener) {
		return "", 0, meshEndpointError("construct", errors.New("IP address class is not permitted"))
	}
	return raw, parsed, nil
}

func validMeshEndpointProtocols(protocols []ma.Protocol, listener bool) bool {
	if len(protocols) != 2 || protocols[1].Code != ma.P_TCP {
		return false
	}
	if protocols[0].Code == ma.P_IP4 || protocols[0].Code == ma.P_IP6 {
		return true
	}
	return !listener && (protocols[0].Code == ma.P_DNS4 || protocols[0].Code == ma.P_DNS6)
}

func meshEndpointTCPPort(address ma.Multiaddr, allowZero bool) (int, error) {
	portText, err := address.ValueForProtocol(ma.P_TCP)
	parsed, parseErr := strconv.ParseUint(portText, 10, 16)
	if err != nil || parseErr != nil || parsed == 0 && !allowZero {
		return 0, errors.New("invalid TCP port")
	}
	return int(parsed), nil
}

func validMeshEndpointAddressClass(address ma.Multiaddr, protocol int, listener bool) bool {
	if protocol != ma.P_IP4 && protocol != ma.P_IP6 {
		return !listener
	}
	host, err := address.ValueForProtocol(protocol)
	ip := net.ParseIP(host)
	if err != nil || ip == nil {
		return false
	}
	if listener {
		return ip.IsUnspecified() || ip.IsGlobalUnicast() || ip.IsLoopback()
	}
	// GlobalUnicast deliberately includes private IPv4 and IPv6 ULA while
	// excluding unspecified, multicast, limited broadcast and link-local IPs.
	// Loopback remains a valid same-host T0 endpoint. A bare multiaddr has no
	// prefix with which to guess subnet-directed broadcast addresses.
	return ip.IsGlobalUnicast() || ip.IsLoopback()
}

func validMeshEndpointPeerID(peerID model.PeerID) bool {
	parsed, err := libp2ppeer.Decode(peerID.String())
	return err == nil && parsed.String() == peerID.String()
}

func meshEndpointAdvances(pending, final meshEndpointValue) bool {
	if pending.peerID != final.peerID || len(pending.listenAddrs) != 1 || len(final.listenAddrs) != 1 {
		return false
	}
	pendingAddress, _ := ma.NewMultiaddr(pending.listenAddrs[0])
	finalAddress, _ := ma.NewMultiaddr(final.listenAddrs[0])
	pendingParts, finalParts := ma.Split(pendingAddress), ma.Split(finalAddress)
	_, pendingPort, _ := inspectMeshEndpointAddr(pending.listenAddrs[0], true, true)
	_, finalPort, _ := inspectMeshEndpointAddr(final.listenAddrs[0], true, false)
	if len(pendingParts) != 2 || len(finalParts) != 2 ||
		pendingParts[0].String() != finalParts[0].String() || pendingPort != 0 && pendingPort != finalPort {
		return false
	}
	return len(pending.advertisedAddrs) == 0 ||
		slices.Equal(pending.advertisedAddrs, final.advertisedAddrs)
}

func validMeshEndpointValue(value meshEndpointValue, final bool) bool {
	if !validMeshEndpointPeerID(value.peerID) || len(value.canonical) == 0 {
		return false
	}
	rebuilt, err := newMeshEndpointValue(value.peerID, value.listenAddrs, value.advertisedAddrs, final)
	return err == nil && bytes.Equal(rebuilt.canonical, value.canonical)
}

func meshEndpointPublishOutcome(err error) (bool, error) {
	if err != nil {
		return false, err
	}
	return true, nil
}
