package peer

import (
	"context"
	"errors"
	"fmt"

	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/multiformats/go-multiaddr"
)

type ChannelMeshLoader func(context.Context) (store.ChannelMeshAuthority, error)

// AdvertisedMultiaddrs returns the bounded concrete listener addresses that
// may be signed into owner and member records. Wildcard listen addresses are
// never returned by libp2p's Host.Addrs projection.
func (runtime *MeshRuntime) AdvertisedMultiaddrs() []string {
	if runtime == nil || runtime.Host() == nil {
		return nil
	}
	addresses := runtime.Host().Addrs()
	result := make([]string, 0, len(addresses))
	for _, address := range addresses {
		if address != nil && len(result) < model.MaxMemberMultiaddrs {
			result = append(result, address.String())
		}
	}
	return result
}

// EnrollChannel grants one temporary outbound owner permit, performs the
// bounded enrollment exchange, then replaces that permit with a fresh Store
// projection. The callback is invoked after every exchange outcome because a
// lost response can still mean the join committed durably.
func (runtime *MeshRuntime) EnrollChannel(ctx context.Context, before store.ChannelMeshAuthority,
	client *ChannelEnrollmentClient, spec JoinChannelSpec, load ChannelMeshLoader,
) (store.InstallJoinedChannelResult, error) {
	if runtime == nil || ctx == nil || client == nil || load == nil || spec.Token.IsZero() {
		return store.InstallJoinedChannelResult{}, fmt.Errorf("%w: enrollment inputs are incomplete", ErrMeshRuntime)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed || runtime.nodeHost == nil || runtime.gossip == nil {
		return store.InstallJoinedChannelResult{}, fmt.Errorf("%w: runtime is closed", ErrMeshRuntime)
	}
	owner, staged, snapshot, err := runtime.stageEnrollment(before, spec.Token)
	if err != nil {
		return store.InstallJoinedChannelResult{}, err
	}
	previous := cloneManagedAddresses(runtime.addresses)
	applyManagedAddresses(runtime.nodeHost.Host(), previous, staged)
	if err := runtime.gossip.Reconcile(snapshot); err != nil {
		applyManagedAddresses(runtime.nodeHost.Host(), staged, previous)
		return store.InstallJoinedChannelResult{}, fmt.Errorf("%w: stage enrollment: %v", ErrMeshRuntime, err)
	}
	result, joinErr := runtime.exchangeEnrollment(ctx, owner, client, spec)
	post, loadErr := load(ctx)
	reconcileErr := runtime.finishEnrollment(staged, post, loadErr)
	return result, errors.Join(joinErr, reconcileErr)
}

func (runtime *MeshRuntime) stageEnrollment(mesh store.ChannelMeshAuthority,
	token model.EnrollmentToken,
) (libp2ppeer.ID, map[libp2ppeer.ID][]multiaddr.Multiaddr, NetworkAuthoritySnapshot, error) {
	payload := token.Payload()
	owner, err := canonicalLibp2pID(payload.Descriptor().Descriptor().OwnerPeerID())
	if err != nil || owner == runtime.nodeHost.Host().ID() {
		return "", nil, NetworkAuthoritySnapshot{}, fmt.Errorf("%w: invalid enrollment owner", ErrMeshRuntime)
	}
	snapshot, addresses, err := projectMeshRuntime(mesh)
	if err != nil {
		return "", nil, NetworkAuthoritySnapshot{}, err
	}
	snapshot.OutboundEnrollmentPeers = []model.PeerID{payload.Descriptor().Descriptor().OwnerPeerID()}
	ownerAddresses := make([]multiaddr.Multiaddr, 0, len(payload.OwnerMultiaddrs()))
	for _, raw := range payload.OwnerMultiaddrs() {
		parsed, parseErr := canonicalPeerAddresses(owner, raw)
		if parseErr != nil {
			return "", nil, NetworkAuthoritySnapshot{}, parseErr
		}
		ownerAddresses = append(ownerAddresses, parsed...)
	}
	addresses[owner] = ownerAddresses
	return owner, addresses, snapshot, nil
}

func (runtime *MeshRuntime) exchangeEnrollment(ctx context.Context, owner libp2ppeer.ID,
	client *ChannelEnrollmentClient, spec JoinChannelSpec,
) (store.InstallJoinedChannelResult, error) {
	if err := runtime.nodeHost.Host().Connect(ctx, libp2ppeer.AddrInfo{ID: owner,
		Addrs: runtime.nodeHost.Host().Peerstore().Addrs(owner)}); err != nil {
		return store.InstallJoinedChannelResult{}, fmt.Errorf("%w: connect enrollment owner: %v", ErrMeshRuntime, err)
	}
	stream, err := runtime.nodeHost.Host().NewStream(ctx, owner, ChannelProtocol)
	if err != nil {
		return store.InstallJoinedChannelResult{}, fmt.Errorf("%w: open enrollment stream: %v", ErrMeshRuntime, err)
	}
	return client.Join(ctx, stream, spec)
}

func (runtime *MeshRuntime) finishEnrollment(staged map[libp2ppeer.ID][]multiaddr.Multiaddr,
	mesh store.ChannelMeshAuthority, loadErr error,
) error {
	if loadErr != nil {
		runtime.closed = true
		return errors.Join(fmt.Errorf("%w: reload durable authority: %v", ErrMeshRuntime, loadErr),
			runtime.gossip.Close())
	}
	snapshot, addresses, err := projectMeshRuntime(mesh)
	if err != nil {
		return err
	}
	applyManagedAddresses(runtime.nodeHost.Host(), staged, addresses)
	if err := runtime.gossip.Reconcile(snapshot); err != nil {
		runtime.closed = true
		return errors.Join(fmt.Errorf("%w: install enrollment authority: %v", ErrMeshRuntime, err),
			runtime.gossip.Close())
	}
	runtime.addresses = addresses
	return joinActiveChannels(runtime.gossip, snapshot)
}
