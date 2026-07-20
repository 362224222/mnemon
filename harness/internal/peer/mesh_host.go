package peer

import (
	"context"
	"errors"
	"fmt"
	"sync"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	ma "github.com/multiformats/go-multiaddr"
)

var ErrMeshHost = errors.New("Mnemon mesh Host bind")

// MeshHostBindSpec is the mechanical input for opening one libp2p listener.
// Durable endpoint authority remains owned by node; this type only carries the
// addresses that the caller has already selected.
type MeshHostBindSpec struct {
	ListenAddrs     []ma.Multiaddr
	AdvertisedAddrs []ma.Multiaddr
}

// MeshEndpointSnapshot is the immutable observation made while the listener
// is held. It is the only local address set exposed after Freeze.
type MeshEndpointSnapshot struct {
	peerID          model.PeerID
	listenAddrs     []string
	advertisedAddrs []string
}

func (snapshot MeshEndpointSnapshot) PeerID() model.PeerID { return snapshot.peerID }
func (snapshot MeshEndpointSnapshot) ListenAddrs() []string {
	return append([]string(nil), snapshot.listenAddrs...)
}
func (snapshot MeshEndpointSnapshot) AdvertisedAddrs() []string {
	return append([]string(nil), snapshot.advertisedAddrs...)
}

func (snapshot MeshEndpointSnapshot) clone() MeshEndpointSnapshot {
	return MeshEndpointSnapshot{peerID: snapshot.peerID, listenAddrs: snapshot.ListenAddrs(),
		advertisedAddrs: snapshot.AdvertisedAddrs()}
}

type meshHostState uint8

const (
	meshHostBound meshHostState = iota + 1
	meshHostFreezing
	meshHostClosing
	meshHostTransferred
	meshHostClosed
)

// BoundMeshHost owns one live NodeHost until Freeze transfers it to a
// MeshRuntime. Close may race with Freeze: it cancels the in-flight freeze,
// waits for cleanup, and never becomes a second transport owner.
type BoundMeshHost struct {
	mu             sync.Mutex
	state          meshHostState
	nodeHost       *NodeHost
	authority      *Authority
	endpoint       MeshEndpointSnapshot
	addressFactory *controlledAddrsFactory
	freezeCancel   context.CancelFunc
	closeRequested bool
	done           chan struct{}
	closeErr       error
}

// BindMeshHost opens exactly one Host and freezes its actual nonzero listener
// and controlled advertised-address projection before returning.
func BindMeshHost(ctx context.Context, privateKey libp2pcrypto.PrivKey,
	spec MeshHostBindSpec,
) (*BoundMeshHost, error) {
	if ctx == nil || ctx.Err() != nil || privateKey == nil ||
		privateKey.Type() != libp2pcrypto.Ed25519 {
		return nil, fmt.Errorf("%w: live context and Ed25519 identity are required", ErrMeshHost)
	}
	listener, requestedPort, err := inspectRequestedListener(spec.ListenAddrs)
	if err != nil {
		return nil, err
	}
	seed, err := inspectAdvertisedSeed(spec.AdvertisedAddrs, requestedPort)
	if err != nil {
		return nil, err
	}
	peerID, err := libp2ppeer.IDFromPrivateKey(privateKey)
	if err != nil {
		return nil, fmt.Errorf("%w: derive PeerID: %v", ErrMeshHost, err)
	}
	modelPeerID, err := model.ParsePeerID(peerID.String())
	if err != nil {
		return nil, fmt.Errorf("%w: parse PeerID: %v", ErrMeshHost, err)
	}
	authority, err := NewAuthority(modelPeerID)
	if err != nil {
		return nil, fmt.Errorf("%w: construct authority: %v", ErrMeshHost, err)
	}
	factory := newControlledAddrsFactory(seed)
	nodeHost, err := newNodeHost(privateKey, authority, []ma.Multiaddr{listener}, factory.apply)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMeshHost, err)
	}
	fail := func(cause error) (*BoundMeshHost, error) {
		return nil, errors.Join(cause, nodeHost.Close())
	}
	actual, port, err := inspectActualListener(listener, nodeHost.host.Network().ListenAddresses())
	if err != nil {
		return fail(err)
	}
	var candidates []ma.Multiaddr
	if len(seed) == 0 {
		candidates, err = nodeHost.host.Network().InterfaceListenAddresses()
		if err != nil {
			return fail(fmt.Errorf("%w: resolve listener interfaces: %v", ErrMeshHost, err))
		}
	}
	advertised, err := factory.freeze(candidates, port)
	if err != nil {
		return fail(err)
	}
	endpoint := MeshEndpointSnapshot{peerID: modelPeerID,
		listenAddrs: []string{actual.String()}, advertisedAddrs: meshHostAddrStrings(advertised)}
	if err := verifyHostAdvertisement(nodeHost, endpoint.advertisedAddrs); err != nil {
		return fail(err)
	}
	if err := ctx.Err(); err != nil {
		return fail(fmt.Errorf("%w: bind canceled: %w", ErrMeshHost, err))
	}
	return &BoundMeshHost{state: meshHostBound, nodeHost: nodeHost, authority: authority,
		endpoint: endpoint, addressFactory: factory, done: make(chan struct{})}, nil
}

// Endpoint returns a defensive copy of the observation made while this object
// held the listener. The snapshot remains immutable after transfer or close.
func (bound *BoundMeshHost) Endpoint() (MeshEndpointSnapshot, error) {
	if bound == nil {
		return MeshEndpointSnapshot{}, fmt.Errorf("%w: bound Host is unavailable", ErrMeshHost)
	}
	bound.mu.Lock()
	snapshot := bound.endpoint.clone()
	bound.mu.Unlock()
	if snapshot.peerID.IsZero() || len(snapshot.listenAddrs) != 1 ||
		len(snapshot.advertisedAddrs) == 0 {
		return MeshEndpointSnapshot{}, fmt.Errorf("%w: endpoint snapshot is unavailable", ErrMeshHost)
	}
	return snapshot, nil
}

// Freeze consumes this owner exactly once and transfers the same NodeHost into
// a fully reconciled MeshRuntime. Every failed or canceled attempt closes the
// listener before returning.
func (bound *BoundMeshHost) Freeze(ctx context.Context,
	mesh store.ChannelMeshAuthority,
) (*MeshRuntime, error) {
	if bound == nil {
		return nil, fmt.Errorf("%w: bound Host is unavailable", ErrMeshHost)
	}
	bound.mu.Lock()
	if bound.state != meshHostBound {
		bound.mu.Unlock()
		return nil, fmt.Errorf("%w: Host ownership was already consumed", ErrMeshHost)
	}
	parent, inputErr := ctx, error(nil)
	if parent == nil {
		parent = context.Background()
		inputErr = errors.New("context is nil")
	} else if err := parent.Err(); err != nil {
		inputErr = err
	}
	freezeCtx, cancel := context.WithCancel(parent)
	bound.state, bound.freezeCancel = meshHostFreezing, cancel
	nodeHost, authority, endpoint := bound.nodeHost, bound.authority, bound.endpoint.clone()
	bound.mu.Unlock()

	var runtime *MeshRuntime
	var err error
	if inputErr != nil {
		err = fmt.Errorf("%w: freeze requires a live context: %w", ErrMeshHost, inputErr)
	} else {
		runtime, err = freezeMeshRuntime(freezeCtx, nodeHost, authority, endpoint, mesh)
		if err != nil {
			err = fmt.Errorf("%w: construct MeshRuntime: %w", ErrMeshHost, err)
		}
	}
	bound.mu.Lock()
	ctxErr := freezeCtx.Err()
	closeRequested := bound.closeRequested
	if err == nil && ctxErr == nil && !closeRequested {
		runtime.cancel = cancel
		bound.state, bound.nodeHost, bound.authority = meshHostTransferred, nil, nil
		bound.freezeCancel = nil
		close(bound.done)
		bound.mu.Unlock()
		return runtime, nil
	}
	bound.mu.Unlock()

	var closeErr error
	if runtime != nil {
		closeErr = runtime.Close()
		cancel()
	} else {
		cancel()
		closeErr = nodeHost.Close()
	}
	if ctxErr != nil {
		err = errors.Join(err, fmt.Errorf("%w: freeze canceled: %w", ErrMeshHost, ctxErr))
	}
	if closeRequested {
		err = errors.Join(err, fmt.Errorf("%w: close won the Freeze race", ErrMeshHost))
	}
	if err == nil {
		err = fmt.Errorf("%w: freeze did not transfer ownership", ErrMeshHost)
	}
	bound.finishClose(closeErr)
	return nil, errors.Join(err, closeErr)
}

// Close releases a still-bound Host. Once Freeze succeeds, Close is a no-op
// because the returned MeshRuntime is the exclusive owner.
func (bound *BoundMeshHost) Close() error {
	if bound == nil {
		return nil
	}
	bound.mu.Lock()
	switch bound.state {
	case meshHostBound:
		bound.state = meshHostClosing
		nodeHost := bound.nodeHost
		bound.mu.Unlock()
		err := nodeHost.Close()
		bound.finishClose(err)
		return err
	case meshHostFreezing:
		bound.closeRequested = true
		cancel, done := bound.freezeCancel, bound.done
		bound.mu.Unlock()
		cancel()
		<-done
		return bound.closedError()
	case meshHostClosing:
		done := bound.done
		bound.mu.Unlock()
		<-done
		return bound.closedError()
	case meshHostTransferred:
		bound.mu.Unlock()
		return nil
	case meshHostClosed:
		err := bound.closeErr
		bound.mu.Unlock()
		return err
	default:
		bound.mu.Unlock()
		return fmt.Errorf("%w: invalid ownership state", ErrMeshHost)
	}
}

func (bound *BoundMeshHost) finishClose(err error) {
	bound.mu.Lock()
	bound.state, bound.nodeHost, bound.authority = meshHostClosed, nil, nil
	bound.freezeCancel, bound.closeErr = nil, err
	close(bound.done)
	bound.mu.Unlock()
}

func (bound *BoundMeshHost) closedError() error {
	bound.mu.Lock()
	err := bound.closeErr
	bound.mu.Unlock()
	return err
}
