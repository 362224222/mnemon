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
	cancel     context.CancelFunc
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
	lifetime, cancel := context.WithCancel(context.Background())
	mesh, err := peer.NewMeshRuntime(lifetime, identity.PrivateKey(), []ma.Multiaddr{listen}, meshAuthority)
	if err != nil {
		cancel()
		return nil, err
	}
	closeMesh := func(cause error) (*daemonChannelRuntime, error) {
		cancel()
		return nil, errors.Join(cause, mesh.Close())
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
	dispatcher, err := peer.NewChannelDispatcher(lifetime, mesh.Host(), peer.ChannelDispatcherOptions{
		Enrollment: owner, Member: members})
	if err != nil {
		return closeMesh(err)
	}
	return &daemonChannelRuntime{manager: manager, mesh: mesh, dispatcher: dispatcher,
		cancel: cancel}, nil
}

func channelListenAddress(peerID model.PeerID) (ma.Multiaddr, error) {
	if peerID.IsZero() {
		return nil, errors.New("Channel listener PeerID is unavailable")
	}
	digest := model.Sum([]byte("mnemon/r5/channel-listener/1\x00" + peerID.String())).Bytes()
	port := 20000 + int(binary.BigEndian.Uint16(digest[:2]))%30000
	return ma.NewMultiaddr(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port))
}

func (runtime *daemonChannelRuntime) Close() error {
	if runtime == nil {
		return nil
	}
	if runtime.dispatcher != nil {
		_ = runtime.dispatcher.Close()
	}
	if runtime.cancel != nil {
		runtime.cancel()
	}
	if runtime.mesh != nil {
		return runtime.mesh.Close()
	}
	return nil
}
