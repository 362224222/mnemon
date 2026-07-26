package node

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	ma "github.com/multiformats/go-multiaddr"
)

type daemonChannelRuntime struct {
	manager    *ChannelManager
	mesh       *peer.MeshRuntime
	dispatcher *peer.ChannelDispatcher
	data       *daemonDataPlaneRuntime
	cancel     context.CancelFunc
	meshCancel context.CancelFunc
}

func openDaemonChannelRuntime(ctx context.Context, st *store.Store, identity *Identity,
	clock Clock,
) (*daemonChannelRuntime, error) {
	if ctx == nil || ctx.Err() != nil || st == nil || identity == nil {
		return nil, errors.New("mnemond Channel runtime authority is unavailable")
	}
	meshAuthority, err := st.ReadChannelMeshAuthority(ctx)
	if err != nil {
		return nil, fmt.Errorf("read Channel mesh authority: %w", err)
	}
	listen, err := channelListenAddress(identity.PeerID())
	if err != nil {
		return nil, fmt.Errorf("construct Channel listen address: %w", err)
	}
	meshLifetime, meshCancel := context.WithCancel(context.Background())
	handlerLifetime, cancel := context.WithCancel(meshLifetime)
	mesh, err := peer.NewMeshRuntime(meshLifetime, identity.PrivateKey(), []ma.Multiaddr{listen}, meshAuthority)
	if err != nil {
		cancel()
		meshCancel()
		return nil, err
	}
	closeMesh := func(cause error) (*daemonChannelRuntime, error) {
		cancel()
		cause = errors.Join(cause, mesh.Close())
		meshCancel()
		return nil, cause
	}
	manager, err := NewChannelManager(ChannelManagerOptions{Store: st, Identity: identity,
		Runtime: mesh, Clock: clock})
	if err != nil {
		return closeMesh(err)
	}
	owner, err := peer.NewChannelEnrollmentOwner(peer.ChannelEnrollmentOwnerOptions{
		Store: manager.EnrollmentOwnerStore(), Signer: identity.PublicationSigner()})
	if err != nil {
		return closeMesh(err)
	}
	members, err := peer.NewChannelMemberService(peer.ChannelMemberServiceOptions{Controller: manager})
	if err != nil {
		return closeMesh(err)
	}
	dispatcher, err := peer.NewChannelDispatcher(handlerLifetime, mesh.Host(), peer.ChannelDispatcherOptions{
		Enrollment: owner, Member: members})
	if err != nil {
		return closeMesh(err)
	}
	data, err := openDaemonDataPlane(ctx, handlerLifetime, st, identity, clock, mesh, manager)
	if err != nil {
		return closeMesh(errors.Join(err, dispatcher.Close()))
	}
	return &daemonChannelRuntime{manager: manager, mesh: mesh, dispatcher: dispatcher,
		data: data, cancel: cancel, meshCancel: meshCancel}, nil
}

func channelListenAddress(peerID model.PeerID) (ma.Multiaddr, error) {
	if peerID.IsZero() {
		return nil, errors.New("Channel listener PeerID is unavailable")
	}
	digest := model.Sum([]byte("mnemon/r5/channel-listener/1\x00" + peerID.String())).Bytes()
	port := 20000 + int(binary.BigEndian.Uint16(digest[:2]))%30000
	return ma.NewMultiaddr(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port))
}

func (runtime *daemonChannelRuntime) CloseContext(ctx context.Context) error {
	if runtime == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("mnemond Channel shutdown context is unavailable")
	}
	// Broadcast the protocol lifetime cancellation before waiting for any
	// admitted dispatcher/Event/Artifact handler to drain.
	if runtime.cancel != nil {
		runtime.cancel()
	}
	var result error
	if runtime.data != nil {
		result = errors.Join(result, runtime.data.CloseContext(ctx))
	}
	if runtime.dispatcher != nil {
		result = errors.Join(result, runtime.dispatcher.CloseContext(ctx))
	}
	if runtime.mesh != nil {
		result = errors.Join(result, runtime.mesh.CloseContext(ctx))
	}
	if runtime.meshCancel != nil {
		runtime.meshCancel()
	}
	return errors.Join(result,
		gracefulShutdownDeadlineError(ctx, "close mnemond Channel runtime"))
}
