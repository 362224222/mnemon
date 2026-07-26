package peer

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/libp2p/go-libp2p"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr"
)

var ErrNodeHost = errors.New("Mnemon libp2p Node Host")

// NodeHost owns the one production libp2p Host and its authority gate. It
// deliberately starts without listeners, registers disconnect observation,
// and only then exposes the configured addresses.
type NodeHost struct {
	host      host.Host
	managed   host.Host
	gater     *ConnectionGater
	closeOnce sync.Once
	closeDone chan struct{}
	closeMu   sync.Mutex
	closeErr  error
}

func NewNodeHost(privateKey libp2pcrypto.PrivKey, authority *Authority,
	listenAddrs []ma.Multiaddr,
) (*NodeHost, error) {
	if privateKey == nil || privateKey.Type() != libp2pcrypto.Ed25519 || authority == nil ||
		len(listenAddrs) == 0 {
		return nil, fmt.Errorf("%w: Ed25519 identity, authority and listen address are required", ErrNodeHost)
	}
	peerID, err := libp2ppeer.IDFromPrivateKey(privateKey)
	if err != nil || peerID != authority.LocalPeerID() {
		return nil, fmt.Errorf("%w: identity does not match authority", ErrNodeHost)
	}
	addresses := make([]ma.Multiaddr, len(listenAddrs))
	for index, address := range listenAddrs {
		if address == nil {
			return nil, fmt.Errorf("%w: nil listen address", ErrNodeHost)
		}
		addresses[index] = address
	}
	resourceManager, err := NewResourceManager()
	if err != nil {
		return nil, fmt.Errorf("%w: resource manager: %v", ErrNodeHost, err)
	}
	gater := NewConnectionGater(authority)
	nodeHost, err := libp2p.New(
		libp2p.Identity(privateKey),
		libp2p.NoListenAddrs,
		libp2p.DisableRelay(),
		libp2p.ResourceManager(resourceManager),
		libp2p.ConnectionGater(gater),
	)
	if err != nil {
		_ = resourceManager.Close()
		return nil, fmt.Errorf("%w: construct Host: %v", ErrNodeHost, err)
	}
	nodeHost.Network().Notify(gater)
	if err := nodeHost.Network().Listen(addresses...); err != nil {
		nodeHost.Network().StopNotify(gater)
		_ = nodeHost.Close()
		return nil, fmt.Errorf("%w: listen: %v", ErrNodeHost, err)
	}
	managed := &managedHost{Host: nodeHost, gater: gater}
	return &NodeHost{host: nodeHost, managed: managed, gater: gater,
		closeDone: make(chan struct{})}, nil
}

func (node *NodeHost) Host() host.Host {
	if node == nil {
		return nil
	}
	return node.managed
}

// managedHost is the only Host surface returned by NodeHost. It preserves
// libp2p control protocols while enforcing Mnemon's enrollment/data-plane
// split on every managed inbound handler and outbound stream.
type managedHost struct {
	host.Host
	gater *ConnectionGater
}

func (managed *managedHost) SetStreamHandler(protocolID protocol.ID,
	handler network.StreamHandler,
) {
	if managed == nil || managed.Host == nil {
		return
	}
	managed.Host.SetStreamHandler(protocolID, managed.wrapHandler(protocolID, handler))
}

func (managed *managedHost) SetStreamHandlerMatch(protocolID protocol.ID,
	match func(protocol.ID) bool, handler network.StreamHandler,
) {
	if managed == nil || managed.Host == nil {
		return
	}
	managed.Host.SetStreamHandlerMatch(protocolID, match, managed.wrapHandler(protocolID, handler))
}

func (managed *managedHost) wrapHandler(protocolID protocol.ID,
	handler network.StreamHandler,
) network.StreamHandler {
	return func(stream network.Stream) {
		actual := protocolID
		if stream != nil && stream.Protocol() != "" {
			actual = stream.Protocol()
		}
		if stream == nil || handler == nil || (managedProtocol(actual) &&
			!managed.gater.allowsProtocol(stream.Conn().RemotePeer(), actual,
				network.DirInbound, stream.Conn().ID())) {
			if stream != nil {
				_ = stream.Reset()
			}
			return
		}
		handler(stream)
	}
}

func (managed *managedHost) NewStream(ctx context.Context, peerID libp2ppeer.ID,
	protocolIDs ...protocol.ID,
) (network.Stream, error) {
	if managed == nil || managed.Host == nil || managed.gater == nil {
		return nil, fmt.Errorf("%w: managed Host is unavailable", ErrNodeHost)
	}
	for _, protocolID := range protocolIDs {
		if managedProtocol(protocolID) &&
			!managed.gater.allowsProtocol(peerID, protocolID, network.DirOutbound, "") {
			return nil, fmt.Errorf("%w: Peer lacks %s stream authority", ErrNodeHost, protocolID)
		}
	}
	stream, err := managed.Host.NewStream(ctx, peerID, protocolIDs...)
	if err != nil {
		return nil, err
	}
	// Close the install/scan/return race with authority reconciliation. If the
	// stream existed before a revoke scan, that scan observes it; if creation
	// completed afterwards, this second current-revision check resets it.
	for _, protocolID := range protocolIDs {
		if managedProtocol(protocolID) &&
			!managed.gater.allowsProtocol(peerID, protocolID, network.DirOutbound, "") {
			_ = stream.Reset()
			return nil, fmt.Errorf("%w: Peer lost %s stream authority", ErrNodeHost, protocolID)
		}
	}
	return stream, nil
}

func (node *NodeHost) Gater() *ConnectionGater {
	if node == nil {
		return nil
	}
	return node.gater
}

func (node *NodeHost) ReconcileConnections() error {
	if node == nil || node.host == nil || node.gater == nil {
		return fmt.Errorf("%w: Host is unavailable", ErrNodeHost)
	}
	if err := node.gater.ReconcileConnections(node.host.Network()); err != nil {
		return fmt.Errorf("%w: %v", ErrNodeHost, err)
	}
	return nil
}

func (node *NodeHost) Close() error {
	return node.CloseContext(context.Background())
}

// CloseContext starts the one owned libp2p close operation and waits only
// within the caller's deadline. libp2p Host.Close has no context surface and
// synchronously joins its transports and reference counts, so NodeHost keeps
// that operation behind one tracked completion channel that later callers can
// join instead of spawning detached waiter goroutines.
func (node *NodeHost) CloseContext(ctx context.Context) error {
	if node == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: shutdown context is unavailable", ErrNodeHost)
	}
	node.closeMu.Lock()
	if node.closeDone == nil {
		node.closeDone = make(chan struct{})
	}
	done := node.closeDone
	node.closeMu.Unlock()
	node.closeOnce.Do(func() {
		if node.host != nil && node.gater != nil {
			node.gater.shutdown()
			node.host.Network().StopNotify(node.gater)
		}
		go node.closeHost(done)
	})
	select {
	case <-done:
		node.closeMu.Lock()
		defer node.closeMu.Unlock()
		return node.closeErr
	default:
	}
	select {
	case <-done:
	case <-ctx.Done():
		return fmt.Errorf("%w: close Host: %w", ErrNodeHost, ctx.Err())
	}
	node.closeMu.Lock()
	defer node.closeMu.Unlock()
	return node.closeErr
}

func (node *NodeHost) closeHost(done chan struct{}) {
	var closeErr error
	if node.host != nil {
		closeErr = node.host.Close()
	}
	node.closeMu.Lock()
	node.closeErr = closeErr
	close(done)
	node.closeMu.Unlock()
}
