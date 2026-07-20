package peer

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	ma "github.com/multiformats/go-multiaddr"
)

var (
	ErrEnrollmentTransportPermit       = errors.New("invalid outbound enrollment transport permit")
	ErrEnrollmentTransportPermitExists = errors.New("outbound enrollment transport permit already exists")
	errEnrollmentTransportPermitBusy   = fmt.Errorf("%w: outbound enrollment transport budget is busy",
		ErrEnrollmentTransportPermit)
)

type outboundEnrollmentPermitToken struct {
	key        outboundEnrollmentPermitKey
	generation uint64
	addresses  []ma.Multiaddr
}

type outboundEnrollmentPermitRef struct {
	key        outboundEnrollmentPermitKey
	generation uint64
}

func (token outboundEnrollmentPermitToken) ref() outboundEnrollmentPermitRef {
	return outboundEnrollmentPermitRef{key: token.key, generation: token.generation}
}

func (token outboundEnrollmentPermitToken) allowsAddress(address ma.Multiaddr) bool {
	if address == nil {
		return false
	}
	for _, permitted := range token.addresses {
		if permitted != nil && permitted.Equal(address) {
			return true
		}
	}
	return false
}

type outboundEnrollmentPermit struct {
	key          outboundEnrollmentPermitKey
	generation   uint64
	addresses    map[string]struct{}
	expiresAt    time.Time
	claimed      bool
	connectionID string
	streamID     string
	resetStream  func() error
	onRelease    func(outboundEnrollmentPermitRef, error)
}

type outboundEnrollmentPermitRelease struct {
	ref         outboundEnrollmentPermitRef
	resetStream func() error
	callback    func(outboundEnrollmentPermitRef, error)
}

type outboundEnrollmentPermitState struct {
	permits        map[outboundEnrollmentPermitKey]*outboundEnrollmentPermit
	resolving      map[outboundEnrollmentPermitKey]*outboundEnrollmentResolution
	nextGeneration uint64
}

type outboundEnrollmentResolution struct {
	key       outboundEnrollmentPermitKey
	ctx       context.Context
	cancel    context.CancelFunc
	resolver  enrollmentMultiaddrResolver
	expiresAt time.Time
}

func newOutboundEnrollmentPermitState() outboundEnrollmentPermitState {
	return outboundEnrollmentPermitState{
		permits:   make(map[outboundEnrollmentPermitKey]*outboundEnrollmentPermit),
		resolving: make(map[outboundEnrollmentPermitKey]*outboundEnrollmentResolution),
	}
}

// acquireOutboundEnrollmentPermit reserves one slot from the shared unknown
// enrollment budget. onRelease may re-enter the gater and is invoked exactly
// once, outside the state mutex, on explicit release, expiry, or shutdown.
func (gater *ConnectionGater) acquireOutboundEnrollmentPermit(ctx context.Context,
	spec enrollmentTransportPermitSpec, onRelease func(outboundEnrollmentPermitRef, error),
) (outboundEnrollmentPermitToken, error) {
	if gater == nil || ctx == nil || ctx.Err() != nil || gater.authority == nil {
		return outboundEnrollmentPermitToken{}, fmt.Errorf("%w: live runtime and context are required",
			ErrEnrollmentTransportPermit)
	}
	key, signedAddresses, err := canonicalOutboundEnrollmentPermit(spec)
	if err != nil {
		return outboundEnrollmentPermitToken{}, err
	}
	resolution, callbacks, err := gater.reserveOutboundEnrollmentResolution(ctx, key)
	runEnrollmentPermitCallbacks(callbacks)
	if err != nil {
		return outboundEnrollmentPermitToken{}, err
	}
	defer resolution.cancel()
	addresses, resolveErr := resolveEnrollmentTransportAddresses(resolution.ctx,
		key.ownerPeerID, signedAddresses, resolution.resolver)
	return gater.completeOutboundEnrollmentResolution(resolution, addresses, onRelease, resolveErr)
}

func (gater *ConnectionGater) reserveOutboundEnrollmentResolution(ctx context.Context,
	key outboundEnrollmentPermitKey,
) (*outboundEnrollmentResolution, []outboundEnrollmentPermitRelease, error) {
	gater.mu.Lock()
	now := gater.now()
	gater.prunePendingLocked(now)
	callbacks := gater.pruneOutboundEnrollmentLocked(now)
	if gater.closed.Load() || gater.pendingTTL <= 0 || gater.dnsResolver == nil || ctx.Err() != nil {
		gater.mu.Unlock()
		return nil, callbacks, fmt.Errorf("%w: connection gate or context is closed",
			ErrEnrollmentTransportPermit)
	}
	if _, duplicate := gater.outbound.permits[key]; duplicate {
		gater.mu.Unlock()
		return nil, callbacks, ErrEnrollmentTransportPermitExists
	}
	if _, duplicate := gater.outbound.resolving[key]; duplicate {
		gater.mu.Unlock()
		return nil, callbacks, ErrEnrollmentTransportPermitExists
	}
	if gater.enrollmentSlotsLocked() >= gater.unknownMax {
		gater.mu.Unlock()
		return nil, callbacks, errEnrollmentTransportPermitBusy
	}
	if gater.outbound.nextGeneration == math.MaxUint64 {
		gater.mu.Unlock()
		return nil, callbacks, fmt.Errorf("%w: enrollment generation exhausted",
			ErrEnrollmentTransportPermit)
	}
	resolveContext, cancelResolve := context.WithTimeout(ctx, gater.pendingTTL)
	expiresAt := now.Add(gater.pendingTTL)
	if deadline, bounded := ctx.Deadline(); bounded && deadline.Before(expiresAt) {
		expiresAt = deadline
	}
	resolution := &outboundEnrollmentResolution{key: key, ctx: resolveContext,
		cancel: cancelResolve, resolver: gater.dnsResolver, expiresAt: expiresAt}
	gater.outbound.resolving[key] = resolution
	gater.resolutionWG.Add(1)
	gater.mu.Unlock()
	return resolution, callbacks, nil
}

func (gater *ConnectionGater) completeOutboundEnrollmentResolution(
	resolution *outboundEnrollmentResolution, addresses []ma.Multiaddr,
	onRelease func(outboundEnrollmentPermitRef, error), resolveErr error,
) (outboundEnrollmentPermitToken, error) {
	gater.mu.Lock()
	current := gater.outbound.resolving[resolution.key]
	if current != resolution {
		gater.mu.Unlock()
		gater.resolutionWG.Done()
		return outboundEnrollmentPermitToken{}, fmt.Errorf("%w: DNS reservation identity changed",
			ErrEnrollmentTransportPermit)
	}
	delete(gater.outbound.resolving, resolution.key)
	now := gater.now()
	callbacks := gater.pruneOutboundEnrollmentLocked(now)
	contextErr := resolution.ctx.Err()
	if resolveErr != nil || contextErr != nil || gater.closed.Load() || !resolution.expiresAt.After(now) {
		gater.mu.Unlock()
		gater.resolutionWG.Done()
		runEnrollmentPermitCallbacks(callbacks)
		if resolveErr != nil {
			return outboundEnrollmentPermitToken{}, resolveErr
		}
		if contextErr != nil {
			return outboundEnrollmentPermitToken{}, fmt.Errorf("%w: DNS context ended: %w",
				ErrEnrollmentTransportPermit, contextErr)
		}
		return outboundEnrollmentPermitToken{}, fmt.Errorf("%w: gate closed or deadline elapsed",
			ErrEnrollmentTransportPermit)
	}
	if gater.outbound.nextGeneration == math.MaxUint64 {
		gater.mu.Unlock()
		gater.resolutionWG.Done()
		runEnrollmentPermitCallbacks(callbacks)
		return outboundEnrollmentPermitToken{}, fmt.Errorf("%w: enrollment generation exhausted",
			ErrEnrollmentTransportPermit)
	}
	gater.outbound.nextGeneration++
	generation := gater.outbound.nextGeneration
	addressSet := make(map[string]struct{}, len(addresses))
	for _, address := range addresses {
		addressSet[address.String()] = struct{}{}
	}
	gater.outbound.permits[resolution.key] = &outboundEnrollmentPermit{key: resolution.key,
		generation: generation, addresses: addressSet, expiresAt: resolution.expiresAt,
		onRelease: onRelease}
	gater.ensureExpiryOwnerLocked()
	gater.signalExpiryOwnerLocked()
	gater.mu.Unlock()
	gater.resolutionWG.Done()
	runEnrollmentPermitCallbacks(callbacks)
	return outboundEnrollmentPermitToken{key: resolution.key, generation: generation,
		addresses: append([]ma.Multiaddr(nil), addresses...)}, nil
}

func (gater *ConnectionGater) releaseOutboundEnrollmentPermit(token outboundEnrollmentPermitToken) bool {
	if gater == nil || token.generation == 0 {
		return false
	}
	gater.mu.Lock()
	permit := gater.outbound.permits[token.key]
	if permit == nil || permit.generation != token.generation {
		gater.mu.Unlock()
		return false
	}
	delete(gater.outbound.permits, token.key)
	release := enrollmentPermitRelease(token.key, permit)
	gater.signalExpiryOwnerLocked()
	gater.mu.Unlock()
	runEnrollmentPermitCallbacks([]outboundEnrollmentPermitRelease{release})
	return true
}

// revokeOutboundEnrollmentPermits atomically removes every remaining
// capability. Exact stream resets and owner callbacks run after the mutex is
// released, so a callback may safely re-enter this method during fail-closed
// teardown.
func (gater *ConnectionGater) revokeOutboundEnrollmentPermits() {
	if gater == nil {
		return
	}
	gater.mu.Lock()
	callbacks := gater.retireOutboundEnrollmentLocked()
	gater.cancelOutboundEnrollmentResolutionsLocked()
	gater.signalExpiryOwnerLocked()
	gater.mu.Unlock()
	runEnrollmentPermitCallbacks(callbacks)
}

func (gater *ConnectionGater) cancelOutboundEnrollmentResolutionsLocked() {
	for _, resolution := range gater.outbound.resolving {
		resolution.cancel()
	}
}

// claimOutboundEnrollmentStream consumes the one stream opening authorized by
// a permit. A transport failure does not make the same capability reusable;
// the caller must release it and acquire a new bounded attempt.
func (gater *ConnectionGater) claimOutboundEnrollmentStream(token outboundEnrollmentPermitToken) bool {
	if gater == nil || token.generation == 0 {
		return false
	}
	gater.mu.Lock()
	callbacks := gater.pruneOutboundEnrollmentLocked(gater.now())
	permit := gater.outbound.permits[token.key]
	allowed := permit != nil && permit.generation == token.generation && !permit.claimed &&
		permit.key.protocol == string(ChannelProtocol) && permit.key.frameVersion == ChannelFrameVersion
	if allowed {
		permit.claimed = true
	}
	gater.mu.Unlock()
	runEnrollmentPermitCallbacks(callbacks)
	return allowed
}

// registerOutboundEnrollmentStream binds the claimed capability to the exact
// authenticated connection and locally initiated stream returned by libp2p.
// Release and expiry can then revoke that stream without closing a durable or
// separately permitted connection shared with it.
func (gater *ConnectionGater) registerOutboundEnrollmentStream(token outboundEnrollmentPermitToken,
	stream network.Stream,
) bool {
	if gater == nil || token.generation == 0 || stream == nil || stream.Protocol() != ChannelProtocol ||
		stream.Stat().Direction != network.DirOutbound || stream.Conn() == nil || stream.ID() == "" ||
		stream.Conn().ID() == "" || stream.Conn().RemotePeer() != token.key.ownerPeerID ||
		!token.allowsAddress(stream.Conn().RemoteMultiaddr()) {
		return false
	}
	gater.mu.Lock()
	callbacks := gater.pruneOutboundEnrollmentLocked(gater.now())
	permit := gater.outbound.permits[token.key]
	allowed := permit != nil && permit.generation == token.generation && permit.claimed &&
		permit.streamID == "" && (permit.connectionID == "" || permit.connectionID == stream.Conn().ID())
	if allowed {
		permit.connectionID = stream.Conn().ID()
		permit.streamID = stream.ID()
		permit.resetStream = stream.Reset
	}
	gater.mu.Unlock()
	runEnrollmentPermitCallbacks(callbacks)
	return allowed
}

func (gater *ConnectionGater) outboundEnrollmentPermitCurrent(token outboundEnrollmentPermitToken) bool {
	if gater == nil || token.generation == 0 {
		return false
	}
	gater.mu.Lock()
	callbacks := gater.pruneOutboundEnrollmentLocked(gater.now())
	permit := gater.outbound.permits[token.key]
	allowed := permit != nil && permit.generation == token.generation
	gater.mu.Unlock()
	runEnrollmentPermitCallbacks(callbacks)
	return allowed
}
