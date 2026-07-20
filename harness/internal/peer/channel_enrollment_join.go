package peer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type frozenJoinChannelSpec struct {
	token                model.EnrollmentToken
	displayLabel         string
	advertisedMultiaddrs []string
	localAlias           string
}

type channelJoinLocalIdentity struct {
	peerID    model.PeerID
	publicKey []byte
}

type channelJoinAttempt struct {
	session           ChannelJoinSession
	prepared          PreparedChannelJoin
	reservationActive bool
	release           bool
}

var errChannelJoinControllerInvariant = errors.New("Channel join controller invariant failed")

// JoinChannel is the sole outbound enrollment entry point. It owns token
// freezing, durable preparation, the exact short-lived transport capability,
// authenticated framing and retirement of that capability.
func (runtime *MeshRuntime) JoinChannel(ctx context.Context, spec JoinChannelSpec,
	session ChannelJoinSession,
) (ChannelJoinResult, error) {
	if ctx == nil || runtime == nil {
		return ChannelJoinResult{}, newChannelProtocolFailure(ChannelErrorInvalidToken, 0)
	}
	requestCtx, cancel := context.WithTimeout(ctx, HermeticLimits().ChannelRequestTimeout)
	defer cancel()
	client, err := newChannelEnrollmentClient(session, nil, nil)
	if err != nil {
		return ChannelJoinResult{}, err
	}
	return client.joinChannel(requestCtx, runtime, spec)
}

func (client *channelEnrollmentClient) joinChannel(ctx context.Context, runtime *MeshRuntime,
	spec JoinChannelSpec,
) (result ChannelJoinResult, resultErr error) {
	frozen, err := freezeJoinChannelSpec(spec)
	if err != nil {
		return ChannelJoinResult{}, newChannelProtocolFailure(ChannelErrorInvalidToken, 0)
	}
	local, err := runtime.channelJoinLocalIdentity()
	if err != nil {
		return ChannelJoinResult{}, enrollmentTransportFailure(err)
	}
	prepared, err := client.beginChannelJoin(ctx, frozen, local)
	if err != nil {
		return ChannelJoinResult{}, err
	}
	attempt := channelJoinAttempt{session: client.session, prepared: prepared,
		reservationActive: prepared.Reserved(),
		release:           prepared.Reserved() && !prepared.CommitUnknown()}
	defer func() {
		if cleanupErr := attempt.releaseIfRequired(); cleanupErr != nil {
			result = ChannelJoinResult{}
			resultErr = errors.Join(resultErr, cleanupErr)
		}
	}()
	if !stablePreparedChannelJoin(frozen, local, prepared) {
		return ChannelJoinResult{}, newChannelProtocolFailure(ChannelErrorRosterConflict, 0)
	}
	permit, err := runtime.acquireEnrollmentTransportPermit(ctx, enrollmentTransportPermitRequest{
		Token: frozen.token, EnrollmentRequestID: prepared.RequestID()})
	if err != nil {
		return ChannelJoinResult{}, enrollmentTransportFailure(err)
	}
	stream, openErr := runtime.openEnrollmentStream(ctx, permit)
	if openErr != nil {
		return finishChannelJoinPermit(ChannelJoinResult{}, enrollmentTransportFailure(openErr), permit)
	}
	result, joinErr := client.exchangeChannelEnrollment(ctx, stream, frozen, local, &attempt)
	result, joinErr = finishChannelJoinPermit(result, joinErr, permit)
	if errors.Is(joinErr, errChannelJoinControllerInvariant) {
		runtime.failClosedEnrollmentTransport(joinErr)
		return ChannelJoinResult{}, joinErr
	}
	return result, joinErr
}

func freezeJoinChannelSpec(spec JoinChannelSpec) (frozenJoinChannelSpec, error) {
	token, err := model.ParseEnrollmentToken(spec.Token.Reveal())
	if err != nil || model.VerifyEnrollmentToken(token) != nil {
		return frozenJoinChannelSpec{}, ErrChannelEnrollmentProtocol
	}
	return frozenJoinChannelSpec{token: token, displayLabel: spec.DisplayLabel,
		advertisedMultiaddrs: append([]string(nil), spec.AdvertisedMultiaddrs...),
		localAlias:           spec.LocalAlias}, nil
}

func (runtime *MeshRuntime) channelJoinLocalIdentity() (channelJoinLocalIdentity, error) {
	runtime.mu.Lock()
	if runtime.closed || runtime.nodeHost == nil || runtime.nodeHost.managedRuntimeHost() == nil {
		runtime.mu.Unlock()
		return channelJoinLocalIdentity{}, fmt.Errorf("%w: runtime is closed", ErrMeshRuntime)
	}
	localHost := runtime.nodeHost.managedRuntimeHost()
	runtime.mu.Unlock()
	peerID, publicKey, err := secureChannelPeer(localHost.ID())
	if err != nil {
		return channelJoinLocalIdentity{}, err
	}
	return channelJoinLocalIdentity{peerID: peerID, publicKey: publicKey}, nil
}

func (client *channelEnrollmentClient) beginChannelJoin(ctx context.Context,
	spec frozenJoinChannelSpec, local channelJoinLocalIdentity,
) (PreparedChannelJoin, error) {
	payload := spec.token.Payload()
	at := client.clock.Now()
	if at.IsZero() {
		return PreparedChannelJoin{}, fmt.Errorf("%w: join clock is unavailable",
			ErrChannelEnrollmentProtocol)
	}
	prepared, err := client.session.BeginChannelJoin(ctx, ChannelJoinPrepareControl{
		AuthenticatedLocalPeerID: local.peerID, LocalPublicKey: append([]byte(nil), local.publicKey...),
		Descriptor: payload.Descriptor(), GrantID: payload.GrantID(), LocalAlias: spec.localAlias, At: at,
	})
	if err != nil {
		return PreparedChannelJoin{}, channelJoinControlError(err)
	}
	if !prepared.claimOnce() {
		return PreparedChannelJoin{}, fmt.Errorf("%w: prepared join is invalid or already consumed",
			ErrChannelEnrollmentProtocol)
	}
	return prepared, nil
}

func stablePreparedChannelJoin(spec frozenJoinChannelSpec, local channelJoinLocalIdentity,
	prepared PreparedChannelJoin,
) bool {
	payload := spec.token.Payload()
	identity, err := model.EnrollmentJoinIdentityDigest(payload.Descriptor().Descriptor().ID(),
		payload.GrantID(), local.peerID, local.publicKey, prepared.OriginEpoch())
	if err != nil {
		return false
	}
	expected, err := model.EnrollmentRequestIDForJoinIdentity(identity)
	return err == nil && expected == prepared.RequestID()
}

func finishChannelJoinPermit(result ChannelJoinResult, joinErr error,
	permit *enrollmentTransportPermit,
) (ChannelJoinResult, error) {
	cleanupErr := permit.Close()
	if cleanupErr == nil {
		return result, joinErr
	}
	return ChannelJoinResult{}, errors.Join(joinErr,
		fmt.Errorf("%w: retire enrollment transport: %w", ErrChannelEnrollmentProtocol, cleanupErr))
}

func (client *channelEnrollmentClient) exchangeChannelEnrollment(ctx context.Context,
	stream network.Stream, spec frozenJoinChannelSpec, local channelJoinLocalIdentity,
	attempt *channelJoinAttempt,
) (ChannelJoinResult, error) {
	if stream == nil || stream.Conn() == nil || stream.Protocol() != ChannelProtocol {
		return ChannelJoinResult{}, enrollmentTransportFailure(ErrChannelEnrollmentProtocol)
	}
	deadline, ok := ctx.Deadline()
	if !ok || stream.SetDeadline(deadline) != nil {
		return ChannelJoinResult{}, enrollmentTransportFailure(ErrChannelEnrollmentProtocol)
	}
	stopCancellation := context.AfterFunc(ctx, func() { _ = stream.SetDeadline(time.Now()) })
	defer stopCancellation()
	ownerPeerID, _, err := secureChannelPeer(stream.Conn().RemotePeer())
	if err != nil || ownerPeerID != spec.token.Payload().Descriptor().Descriptor().OwnerPeerID() {
		return ChannelJoinResult{}, newChannelProtocolFailure(ChannelErrorWrongOwner, 0)
	}
	initiation, err := client.writeEnrollmentInit(ctx, stream, spec, attempt.prepared)
	if err != nil {
		return ChannelJoinResult{}, err
	}
	challenge, err := readEnrollmentChallenge(ctx, stream, initiation.frameRequestID)
	if err != nil {
		if stableChannelProtocolFailure(err) {
			attempt.allowRelease()
		}
		return ChannelJoinResult{}, err
	}
	transcript, proofFrame, err := enrollmentProofFrame(spec, local, ownerPeerID,
		attempt.prepared, initiation, challenge)
	if err != nil {
		return ChannelJoinResult{}, err
	}
	if err := attempt.markCommitUnknown(ctx, client.clock.Now()); err != nil {
		return ChannelJoinResult{}, err
	}
	if err := WriteChannelFrame(stream, proofFrame); err != nil {
		return ChannelJoinResult{}, enrollmentOutcomeUnknown(err)
	}
	accepted, err := readVerifiedChannelEnrollment(stream, initiation.frameRequestID,
		spec.token.Payload().Descriptor(), transcript, local.peerID, ownerPeerID)
	if err != nil {
		if stableChannelProtocolFailure(err) {
			attempt.allowRelease()
			return ChannelJoinResult{}, err
		}
		// Once proof transmission is possible, malformed, mismatched, or lost
		// accepted evidence cannot prove whether the owner committed. Preserve
		// commit_unknown and require a retry of the same stable request.
		return ChannelJoinResult{}, enrollmentOutcomeUnknown(err)
	}
	result, err := attempt.install(ctx, accepted, client.clock.Now())
	if err != nil {
		return ChannelJoinResult{}, err
	}
	if !validInstalledChannelJoinResult(accepted, result, spec.localAlias) {
		// The semantic controller may already have durably installed authority.
		// Misreporting that local invariant as a remote roster decision would make
		// callers abandon recovery. Keep it on the internal fail-closed surface.
		return ChannelJoinResult{}, fmt.Errorf("%w: %w", ErrChannelEnrollmentProtocol,
			errChannelJoinControllerInvariant)
	}
	return result, nil
}

type channelEnrollmentInitiation struct {
	frameRequestID ChannelRequestID
	joinerNonce    []byte
}

func (client *channelEnrollmentClient) writeEnrollmentInit(ctx context.Context,
	stream network.Stream, spec frozenJoinChannelSpec, prepared PreparedChannelJoin,
) (channelEnrollmentInitiation, error) {
	joinerNonce := make([]byte, model.EnrollmentNonceBytes)
	if _, err := io.ReadFull(client.random, joinerNonce); err != nil {
		return channelEnrollmentInitiation{}, fmt.Errorf("%w: joiner nonce unavailable",
			ErrChannelEnrollmentProtocol)
	}
	requestID, err := NewChannelRequestID(client.random)
	if err != nil {
		return channelEnrollmentInitiation{}, fmt.Errorf("%w: Channel request ID unavailable",
			ErrChannelEnrollmentProtocol)
	}
	payload := spec.token.Payload()
	init, err := NewEnrollInit(EnrollInitSpec{ChannelID: payload.Descriptor().Descriptor().ID(),
		GrantID: payload.GrantID(), EnrollmentRequestID: prepared.RequestID(), JoinerNonce: joinerNonce,
		SupportedVersions: []uint8{ChannelFrameVersion}, OriginEpoch: prepared.OriginEpoch(),
		DisplayLabel: spec.displayLabel, AdvertisedMultiaddrs: spec.advertisedMultiaddrs})
	if err != nil {
		return channelEnrollmentInitiation{}, newChannelProtocolFailure(ChannelErrorInvalidToken, 0)
	}
	frame, err := NewChannelFrame(requestID, init)
	if err != nil {
		return channelEnrollmentInitiation{}, ErrChannelEnrollmentProtocol
	}
	if err := WriteChannelFrame(stream, frame); err != nil {
		return channelEnrollmentInitiation{}, enrollmentPrecommitTransportFailure(ctx, err)
	}
	return channelEnrollmentInitiation{frameRequestID: requestID,
		joinerNonce: append([]byte(nil), joinerNonce...)}, nil
}
