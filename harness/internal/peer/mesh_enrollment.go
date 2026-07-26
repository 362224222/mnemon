package peer

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/multiformats/go-multiaddr"
)

const channelEnrollmentMaxAttempts = 2
const channelEnrollmentAuthorityLoads = 2

type ChannelMeshLoader func(context.Context) (store.ChannelMeshAuthority, error)

type meshEnrollmentPermit struct {
	active    bool
	owner     model.PeerID
	ownerID   libp2ppeer.ID
	addresses []multiaddr.Multiaddr
}

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
// projection. Store is reloaded after every exchange outcome because a
// lost response can still mean the join committed durably.
func (runtime *MeshRuntime) EnrollChannel(ctx context.Context,
	client *ChannelEnrollmentClient, spec JoinChannelSpec, load ChannelMeshLoader,
) (store.InstallJoinedChannelResult, error) {
	if runtime == nil || ctx == nil || client == nil || load == nil || spec.Token.IsZero() {
		return store.InstallJoinedChannelResult{}, fmt.Errorf("%w: enrollment inputs are incomplete", ErrMeshRuntime)
	}
	slot, localHostID, err := runtime.claimEnrollmentSlot()
	if err != nil {
		return store.InstallJoinedChannelResult{}, err
	}
	localPeerID, localPublicKey, err := secureChannelPeer(localHostID)
	if err != nil {
		cause := fmt.Errorf("%w: local enrollment identity: %v", ErrMeshRuntime, err)
		return store.InstallJoinedChannelResult{}, runtime.clearEnrollmentSlot(slot, cause)
	}
	prepared, err := client.prepare(ctx, spec, localPeerID, localPublicKey)
	if err != nil {
		return store.InstallJoinedChannelResult{}, runtime.clearEnrollmentSlot(slot, err)
	}
	permit, err := prepareEnrollmentPermit(spec.Token, localHostID)
	if err != nil {
		client.release(prepared)
		return store.InstallJoinedChannelResult{}, runtime.clearEnrollmentSlot(slot, err)
	}
	if err := runtime.activateEnrollment(slot, permit); err != nil {
		client.release(prepared)
		return store.InstallJoinedChannelResult{}, err
	}
	var result store.InstallJoinedChannelResult
	var joinErr error
	for attempt := 1; attempt <= channelEnrollmentMaxAttempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, HermeticLimits().ChannelRequestTimeout)
		result, joinErr = runtime.exchangeEnrollment(attemptCtx, slot.ownerID, client, prepared)
		cancel()
		if joinErr == nil || attempt == channelEnrollmentMaxAttempts ||
			!retryableEnrollmentAttempt(joinErr) {
			break
		}
		if err := waitEnrollmentRetry(ctx, enrollmentRetryDelay(joinErr)); err != nil {
			joinErr = err
			break
		}
		closeEnrollmentConnections(runtime, slot.ownerID)
		prepared, joinErr = client.prepare(ctx, spec, localPeerID, localPublicKey)
		if joinErr != nil {
			break
		}
	}
	reconcileErr := runtime.finishEnrollment(ctx, slot, load)
	return result, errors.Join(joinErr, reconcileErr)
}

func (runtime *MeshRuntime) claimEnrollmentSlot() (*meshEnrollmentPermit, libp2ppeer.ID, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed || runtime.nodeHost == nil || runtime.nodeHost.Host() == nil ||
		runtime.gossip == nil {
		return nil, "", fmt.Errorf("%w: runtime is closed", ErrMeshRuntime)
	}
	if runtime.enrollment != nil {
		return nil, "", fmt.Errorf("%w: runtime enrollment is busy", ErrMeshRuntime)
	}
	slot := &meshEnrollmentPermit{}
	runtime.enrollment = slot
	runtime.revision++
	return slot, runtime.nodeHost.Host().ID(), nil
}

func retryableEnrollmentAttempt(err error) bool {
	var failure *ChannelProtocolFailure
	if errors.As(err, &failure) {
		return failure.Retryable()
	}
	return errors.Is(err, ErrChannelEnrollmentOutcomeUnknown) || errors.Is(err, ErrMeshRuntime)
}

func enrollmentRetryDelay(err error) time.Duration {
	var failure *ChannelProtocolFailure
	if errors.As(err, &failure) && failure.RetryAfter() > 0 {
		return failure.RetryAfter()
	}
	return channelEnrollmentGapRetry
}

func waitEnrollmentRetry(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func closeEnrollmentConnections(runtime *MeshRuntime, owner libp2ppeer.ID) {
	if runtime == nil || runtime.nodeHost == nil || owner == "" {
		return
	}
	for _, connection := range runtime.nodeHost.Host().Network().ConnsToPeer(owner) {
		_ = connection.Close()
	}
}

func prepareEnrollmentPermit(token model.EnrollmentToken,
	local libp2ppeer.ID,
) (meshEnrollmentPermit, error) {
	payload := token.Payload()
	owner, err := canonicalLibp2pID(payload.Descriptor().Descriptor().OwnerPeerID())
	if err != nil || owner == local || local == "" {
		return meshEnrollmentPermit{}, fmt.Errorf("%w: invalid enrollment owner", ErrMeshRuntime)
	}
	ownerAddresses := make([]multiaddr.Multiaddr, 0, len(payload.OwnerMultiaddrs()))
	for _, raw := range payload.OwnerMultiaddrs() {
		parsed, parseErr := canonicalPeerAddresses(owner, raw)
		if parseErr != nil {
			return meshEnrollmentPermit{}, parseErr
		}
		ownerAddresses = append(ownerAddresses, parsed...)
	}
	return meshEnrollmentPermit{owner: payload.Descriptor().Descriptor().OwnerPeerID(),
		ownerID: owner, addresses: ownerAddresses}, nil
}

func (runtime *MeshRuntime) activateEnrollment(slot *meshEnrollmentPermit,
	permit meshEnrollmentPermit,
) error {
	runtime.mu.Lock()
	if slot == nil || runtime.enrollment != slot {
		runtime.mu.Unlock()
		return fmt.Errorf("%w: runtime enrollment slot was fenced", ErrMeshRuntime)
	}
	if runtime.closed || runtime.nodeHost == nil || runtime.gossip == nil || slot.active {
		runtime.enrollment = nil
		runtime.revision++
		runtime.mu.Unlock()
		return fmt.Errorf("%w: runtime enrollment is unavailable", ErrMeshRuntime)
	}
	permit.active = true
	*slot = permit
	runtime.revision++
	runtime.mu.Unlock()
	if err := runtime.reconcileCurrentProjection(); err != nil {
		cause := fmt.Errorf("%w: stage enrollment: %v", ErrMeshRuntime, err)
		return runtime.clearEnrollmentSlot(slot, cause)
	}
	return nil
}

func (runtime *MeshRuntime) clearEnrollmentSlot(slot *meshEnrollmentPermit, cause error) error {
	runtime.mu.Lock()
	if slot == nil || runtime.enrollment != slot {
		runtime.mu.Unlock()
		return errors.Join(cause,
			fmt.Errorf("%w: enrollment slot was fenced", ErrMeshRuntime))
	}
	runtime.enrollment = nil
	runtime.revision++
	runtime.mu.Unlock()
	return cause
}

func overlayEnrollmentPermit(snapshot NetworkAuthoritySnapshot,
	addresses map[libp2ppeer.ID][]multiaddr.Multiaddr, permit *meshEnrollmentPermit,
) (NetworkAuthoritySnapshot, map[libp2ppeer.ID][]multiaddr.Multiaddr, error) {
	if permit == nil || !permit.active {
		return snapshot, addresses, nil
	}
	if permit.owner.IsZero() || permit.ownerID == "" ||
		len(snapshot.OutboundEnrollmentPeers) != 0 {
		return NetworkAuthoritySnapshot{}, nil,
			fmt.Errorf("%w: invalid active enrollment permit", ErrMeshRuntime)
	}
	snapshot.OutboundEnrollmentPeers = []model.PeerID{permit.owner}
	addresses = cloneManagedAddresses(addresses)
	combined := append([]multiaddr.Multiaddr(nil), addresses[permit.ownerID]...)
	seen := make(map[string]struct{}, len(combined)+len(permit.addresses))
	for _, address := range combined {
		seen[address.String()] = struct{}{}
	}
	for _, address := range permit.addresses {
		if _, exists := seen[address.String()]; !exists {
			combined = append(combined, address)
			seen[address.String()] = struct{}{}
		}
	}
	sort.Slice(combined, func(left, right int) bool {
		return combined[left].String() < combined[right].String()
	})
	addresses[permit.ownerID] = combined
	return snapshot, addresses, nil
}

func (runtime *MeshRuntime) exchangeEnrollment(ctx context.Context, owner libp2ppeer.ID,
	client *ChannelEnrollmentClient, prepared preparedChannelJoin,
) (store.InstallJoinedChannelResult, error) {
	if err := runtime.nodeHost.Host().Connect(ctx, libp2ppeer.AddrInfo{ID: owner,
		Addrs: runtime.nodeHost.Host().Peerstore().Addrs(owner)}); err != nil {
		client.release(prepared)
		return store.InstallJoinedChannelResult{}, fmt.Errorf("%w: connect enrollment owner: %v", ErrMeshRuntime, err)
	}
	stream, err := runtime.nodeHost.Host().NewStream(ctx, owner, ChannelProtocol)
	if err != nil {
		client.release(prepared)
		return store.InstallJoinedChannelResult{}, fmt.Errorf("%w: open enrollment stream: %v", ErrMeshRuntime, err)
	}
	return client.join(ctx, stream, prepared)
}

func (runtime *MeshRuntime) finishEnrollment(ctx context.Context, permit *meshEnrollmentPermit,
	load ChannelMeshLoader,
) error {
	for attempt := 0; attempt < channelEnrollmentAuthorityLoads; attempt++ {
		runtime.mu.Lock()
		if runtime.closed || runtime.enrollment != permit {
			runtime.mu.Unlock()
			return fmt.Errorf("%w: enrollment permit was fenced", ErrMeshRuntime)
		}
		revision := runtime.revision
		runtime.mu.Unlock()

		mesh, err := load(ctx)
		if err != nil {
			cause := fmt.Errorf("%w: reload durable authority: %w", ErrMeshRuntime, err)
			return runtime.failEnrollment(permit, cause)
		}
		if _, _, err := projectMeshRuntime(mesh); err != nil {
			return runtime.failEnrollment(permit, err)
		}

		runtime.mu.Lock()
		if runtime.closed || runtime.enrollment != permit {
			runtime.mu.Unlock()
			return fmt.Errorf("%w: enrollment permit was fenced", ErrMeshRuntime)
		}
		if runtime.revision != revision {
			runtime.mu.Unlock()
			continue
		}
		runtime.mesh = mesh
		runtime.enrollment = nil
		runtime.revision++
		settleRevision := runtime.revision
		runtime.mu.Unlock()
		err = runtime.reconcileProjectionOnce(settleRevision)
		if errors.Is(err, errMeshRuntimeRevision) {
			return runtime.reconcileCurrentProjection()
		}
		return err
	}
	cause := fmt.Errorf("%w: enrollment authority changed during reload", ErrMeshRuntime)
	return runtime.cancelEnrollment(permit, cause)
}

func (runtime *MeshRuntime) failEnrollment(permit *meshEnrollmentPermit, cause error) error {
	runtime.mu.Lock()
	if runtime.enrollment != permit {
		runtime.mu.Unlock()
		return errors.Join(cause, fmt.Errorf("%w: enrollment permit was fenced", ErrMeshRuntime))
	}
	runtime.enrollment = nil
	runtime.revision++
	runtime.mu.Unlock()
	return runtime.failClosed(cause)
}

func (runtime *MeshRuntime) cancelEnrollment(permit *meshEnrollmentPermit, cause error) error {
	runtime.mu.Lock()
	if runtime.enrollment != permit {
		runtime.mu.Unlock()
		return errors.Join(cause, fmt.Errorf("%w: enrollment permit was fenced", ErrMeshRuntime))
	}
	runtime.enrollment = nil
	runtime.revision++
	runtime.mu.Unlock()
	return errors.Join(cause, runtime.reconcileCurrentProjection())
}
