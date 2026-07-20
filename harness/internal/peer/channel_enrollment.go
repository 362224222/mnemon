package peer

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"io"
	"time"
)

var (
	ErrChannelEnrollmentProtocol       = errors.New("Mnemon Channel enrollment protocol failed")
	ErrChannelEnrollmentOutcomeUnknown = errors.New("Mnemon Channel enrollment outcome is unknown; retry the same join")
)

const (
	channelEnrollmentBusyRetry = time.Second
	channelEnrollmentGapRetry  = 250 * time.Millisecond
)

// ChannelProtocolFailure is the closed, secret-free error surface returned by
// the enrollment client. Retry decisions are carried by stable protocol codes,
// never by remote diagnostics or Store error strings.
type ChannelProtocolFailure struct {
	code       ChannelProtocolErrorCode
	retryable  bool
	retryAfter time.Duration
}

func newChannelProtocolFailure(code ChannelProtocolErrorCode, retryAfter time.Duration) error {
	if !code.Valid() || retryAfter < 0 || retryAfter%time.Millisecond != 0 {
		return fmt.Errorf("%w: invalid stable failure", ErrChannelEnrollmentProtocol)
	}
	retryable := code.retryable()
	if !retryable {
		retryAfter = 0
	}
	return &ChannelProtocolFailure{code: code, retryable: retryable, retryAfter: retryAfter}
}

func (failure *ChannelProtocolFailure) Error() string {
	if failure == nil || !failure.code.Valid() {
		return ErrChannelEnrollmentProtocol.Error()
	}
	return fmt.Sprintf("%s: %s", ErrChannelEnrollmentProtocol, failure.code)
}

func (failure *ChannelProtocolFailure) Unwrap() error { return ErrChannelEnrollmentProtocol }
func (failure *ChannelProtocolFailure) Code() ChannelProtocolErrorCode {
	if failure == nil {
		return ""
	}
	return failure.code
}
func (failure *ChannelProtocolFailure) Retryable() bool {
	return failure != nil && failure.retryable
}
func (failure *ChannelProtocolFailure) RetryAfter() time.Duration {
	if failure == nil {
		return 0
	}
	return failure.retryAfter
}

type channelEnrollmentClock interface {
	Now() time.Time
}

type wallEnrollmentClock struct{}

func (wallEnrollmentClock) Now() time.Time { return time.Now() }

// readChannelStreamFrame applies the domain size fence before allocation and
// accounts the declared buffer against the libp2p stream scope for the entire
// decode. Small handshake messages never receive the 4 MiB roster-frame
// allowance merely because they share the same direct protocol.
func readChannelStreamFrame(stream network.Stream, maximum int) (ChannelFrame, func(), error) {
	if stream == nil || maximum <= 0 || maximum > maxChannelFrameBytes() {
		return ChannelFrame{}, nil, channelFrameError("invalid stream frame bound", nil)
	}
	var prefix [channelFrameLengthBytes]byte
	if _, err := io.ReadFull(stream, prefix[:]); err != nil {
		return ChannelFrame{}, nil, channelFrameError("read stream length prefix", err)
	}
	length := uint64(binary.BigEndian.Uint32(prefix[:]))
	if length == 0 || length > uint64(maximum) {
		return ChannelFrame{}, nil, channelFrameError("stream frame exceeds message bound", nil)
	}
	reserved := int(length)
	if err := stream.Scope().ReserveMemory(reserved, network.ReservationPriorityAlways); err != nil {
		return ChannelFrame{}, nil, channelFrameError("reserve stream frame memory", err)
	}
	release := func() { stream.Scope().ReleaseMemory(reserved) }
	raw := make([]byte, reserved)
	if _, err := io.ReadFull(stream, raw); err != nil {
		release()
		return ChannelFrame{}, nil, channelFrameError("read stream frame", err)
	}
	frame, err := ParseChannelFrame(raw)
	if err != nil {
		release()
		return ChannelFrame{}, nil, err
	}
	return frame, release, nil
}

func secureChannelPeer(peerID libp2ppeer.ID) (model.PeerID, []byte, error) {
	parsed, err := model.ParsePeerID(peerID.String())
	if err != nil {
		return model.PeerID{}, nil, fmt.Errorf("%w: secure PeerID", ErrChannelEnrollmentProtocol)
	}
	publicKey, err := peerID.ExtractPublicKey()
	if err != nil || publicKey == nil || publicKey.Type() != libp2pcrypto.Ed25519 {
		return model.PeerID{}, nil, fmt.Errorf("%w: secure PeerID lacks an Ed25519 key",
			ErrChannelEnrollmentProtocol)
	}
	raw, err := publicKey.Raw()
	if err != nil || len(raw) != 32 {
		return model.PeerID{}, nil, fmt.Errorf("%w: invalid secure Ed25519 key",
			ErrChannelEnrollmentProtocol)
	}
	return parsed, append([]byte(nil), raw...), nil
}

// Join executes the authenticated handshake on an already-open exact Channel
// stream and atomically installs only verified signed evidence. Dial authority
// and peerstore address admission remain the Node reconciler's responsibility.
func (client *ChannelEnrollmentClient) Join(ctx context.Context, stream network.Stream,
	spec JoinChannelSpec,
) (store.InstallJoinedChannelResult, error) {
	if stream == nil {
		return store.InstallJoinedChannelResult{}, newChannelProtocolFailure(ChannelErrorInvalidToken, 0)
	}
	completed := false
	defer func() {
		if completed {
			_ = stream.Close()
		} else {
			_ = stream.Reset()
		}
	}()
	if client == nil || client.store == nil || ctx == nil || stream.Conn() == nil ||
		stream.Protocol() != ChannelProtocol || model.VerifyEnrollmentToken(spec.Token) != nil {
		return store.InstallJoinedChannelResult{}, newChannelProtocolFailure(ChannelErrorInvalidToken, 0)
	}
	if err := ctx.Err(); err != nil {
		return store.InstallJoinedChannelResult{}, enrollmentTransportFailure(err)
	}
	deadline := time.Now().Add(HermeticLimits().ChannelRequestTimeout)
	if parentDeadline, ok := ctx.Deadline(); ok && parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}
	if err := stream.SetDeadline(deadline); err != nil {
		_ = stream.Reset()
		return store.InstallJoinedChannelResult{}, enrollmentTransportFailure(err)
	}
	requestCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	stopCancellation := context.AfterFunc(requestCtx, func() { _ = stream.SetDeadline(time.Now()) })
	defer stopCancellation()

	payload := spec.Token.Payload()
	descriptor := payload.Descriptor()
	wantOwner := descriptor.Descriptor().OwnerPeerID()
	ownerPeerID, _, err := secureChannelPeer(stream.Conn().RemotePeer())
	if err != nil || ownerPeerID != wantOwner {
		return store.InstallJoinedChannelResult{}, newChannelProtocolFailure(ChannelErrorWrongOwner, 0)
	}
	joinerPeerID, joinerPublicKey, err := secureChannelPeer(stream.Conn().LocalPeer())
	if err != nil || joinerPeerID == ownerPeerID {
		return store.InstallJoinedChannelResult{}, newChannelProtocolFailure(ChannelErrorInvalidToken, 0)
	}
	prepared, err := client.store.PrepareJoinedChannel(requestCtx, store.PrepareJoinedChannelSpec{
		AuthenticatedLocalPeerID: joinerPeerID, LocalPublicKey: joinerPublicKey,
		Descriptor: descriptor, GrantID: payload.GrantID(), LocalAlias: spec.LocalAlias,
		At: client.clock.Now(),
	})
	if err != nil {
		return store.InstallJoinedChannelResult{}, joinedChannelStoreFailure(err)
	}
	enrollmentRequestID := prepared.RequestID
	reservationActive := prepared.Reserved
	releaseReservation := prepared.Reserved && !prepared.CommitUnknown
	defer func() {
		if !reservationActive || !releaseReservation {
			return
		}
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), time.Second)
		defer releaseCancel()
		_ = client.store.ReleaseJoinedChannelReservation(releaseCtx, enrollmentRequestID, joinerPeerID,
			prepared.Attempt)
	}()

	joinerNonce := make([]byte, model.EnrollmentNonceBytes)
	if _, err := io.ReadFull(client.random, joinerNonce); err != nil {
		return store.InstallJoinedChannelResult{}, fmt.Errorf("%w: joiner nonce unavailable",
			ErrChannelEnrollmentProtocol)
	}
	frameRequestID, err := NewChannelRequestID(client.random)
	if err != nil {
		return store.InstallJoinedChannelResult{}, fmt.Errorf("%w: Channel request ID unavailable",
			ErrChannelEnrollmentProtocol)
	}
	init, err := NewEnrollInit(EnrollInitSpec{ChannelID: descriptor.Descriptor().ID(),
		GrantID: payload.GrantID(), EnrollmentRequestID: enrollmentRequestID, JoinerNonce: joinerNonce,
		SupportedVersions: []uint8{ChannelFrameVersion}, OriginEpoch: prepared.OriginEpoch,
		DisplayLabel: spec.DisplayLabel, AdvertisedMultiaddrs: spec.AdvertisedMultiaddrs})
	if err != nil {
		return store.InstallJoinedChannelResult{}, newChannelProtocolFailure(ChannelErrorInvalidToken, 0)
	}
	initFrame, err := NewChannelFrame(frameRequestID, init)
	if err != nil {
		return store.InstallJoinedChannelResult{}, fmt.Errorf("%w: invalid local enrollment frame",
			ErrChannelEnrollmentProtocol)
	}
	if err := WriteChannelFrame(stream, initFrame); err != nil {
		return store.InstallJoinedChannelResult{}, enrollmentPrecommitTransportFailure(requestCtx, err)
	}

	challengeFrame, releaseChallenge, err := readChannelStreamFrame(stream, model.MaxChannelRecordBytes)
	if err != nil {
		return store.InstallJoinedChannelResult{}, enrollmentPrecommitTransportFailure(requestCtx, err)
	}
	defer releaseChallenge()
	if failure := receivedChannelFailure(frameRequestID, challengeFrame); failure != nil {
		completed = true
		if reservationActive {
			releaseReservation = true
		}
		return store.InstallJoinedChannelResult{}, failure
	}
	if challengeFrame.RequestID() != frameRequestID ||
		challengeFrame.Type() != ChannelFrameEnrollChallenge {
		return store.InstallJoinedChannelResult{}, newChannelProtocolFailure(ChannelErrorRosterConflict, 0)
	}
	challenge, ok := challengeFrame.Payload().(EnrollChallenge)
	if !ok {
		return store.InstallJoinedChannelResult{}, newChannelProtocolFailure(ChannelErrorRosterConflict, 0)
	}
	transcript, err := model.NewEnrollmentTranscript(model.EnrollmentTranscriptSpec{
		ChannelID: descriptor.Descriptor().ID(), GrantID: payload.GrantID(), RequestID: enrollmentRequestID,
		OwnerPeerID: ownerPeerID, JoinerPeerID: joinerPeerID, OwnerNonce: challenge.OwnerNonce(),
		JoinerNonce: joinerNonce, SelectedVersion: challenge.SelectedVersion(), Limits: challenge.Limits(),
		JoinerOriginEpoch: prepared.OriginEpoch, JoinerDisplayLabel: spec.DisplayLabel,
		JoinerPublicKey: joinerPublicKey, AdvertisedMultiaddrs: spec.AdvertisedMultiaddrs,
		RosterHead: challenge.RosterHead(),
	})
	if err != nil {
		return store.InstallJoinedChannelResult{}, newChannelProtocolFailure(ChannelErrorIncompatibleProtocol, 0)
	}
	secret := payload.BearerSecret()
	verifier, verifierErr := model.VerifierForEnrollment(secret,
		descriptor.Descriptor().ID(), payload.GrantID())
	clear(secret)
	if verifierErr != nil {
		return store.InstallJoinedChannelResult{}, newChannelProtocolFailure(ChannelErrorInvalidToken, 0)
	}
	proof, err := model.ComputeEnrollmentProof(verifier, transcript)
	if err != nil {
		return store.InstallJoinedChannelResult{}, newChannelProtocolFailure(ChannelErrorInvalidToken, 0)
	}
	proofPayload, err := NewEnrollProof(proof)
	if err != nil {
		return store.InstallJoinedChannelResult{}, newChannelProtocolFailure(ChannelErrorInvalidToken, 0)
	}
	proofFrame, err := NewChannelFrame(frameRequestID, proofPayload)
	if err != nil {
		return store.InstallJoinedChannelResult{}, fmt.Errorf("%w: invalid local enrollment proof frame",
			ErrChannelEnrollmentProtocol)
	}
	if err := requestCtx.Err(); err != nil {
		return store.InstallJoinedChannelResult{}, enrollmentTransportFailure(err)
	}
	if reservationActive {
		if err := client.store.MarkJoinedChannelCommitUnknown(requestCtx, enrollmentRequestID,
			joinerPeerID, prepared.Attempt, client.clock.Now()); err != nil {
			return store.InstallJoinedChannelResult{}, joinedChannelStoreFailure(err)
		}
		releaseReservation = false
	}
	if err := WriteChannelFrame(stream, proofFrame); err != nil {
		return store.InstallJoinedChannelResult{}, enrollmentOutcomeUnknown(err)
	}

	acceptedFrame, releaseAccepted, err := readChannelStreamFrame(stream,
		channelFrameMaximum(ChannelFrameEnrollAccepted))
	if err != nil {
		return store.InstallJoinedChannelResult{}, enrollmentOutcomeUnknown(err)
	}
	defer releaseAccepted()
	if failure := receivedChannelFailure(frameRequestID, acceptedFrame); failure != nil {
		completed = true
		if reservationActive {
			releaseReservation = true
		}
		return store.InstallJoinedChannelResult{}, failure
	}
	if acceptedFrame.RequestID() != frameRequestID ||
		acceptedFrame.Type() != ChannelFrameEnrollAccepted {
		return store.InstallJoinedChannelResult{}, newChannelProtocolFailure(ChannelErrorRosterConflict, 0)
	}
	accepted, ok := acceptedFrame.Payload().(EnrollAccepted)
	if !ok {
		return store.InstallJoinedChannelResult{}, newChannelProtocolFailure(ChannelErrorRosterConflict, 0)
	}
	members := accepted.RosterSnapshot()
	roster, err := model.NewVerifiedRoster(descriptor, members)
	if err != nil || accepted.MemberRecord().PeerID() != joinerPeerID ||
		!validEnrollmentReceiptForStatus(descriptor, transcript, accepted) ||
		!validEnrollmentResultStatus(accepted.Status(), roster, joinerPeerID) {
		return store.InstallJoinedChannelResult{}, newChannelProtocolFailure(ChannelErrorRosterConflict, 0)
	}
	completed = true
	installed, err := client.store.InstallJoinedChannel(requestCtx,
		joinedChannelInstallSpec(ownerPeerID, spec.LocalAlias, descriptor,
			transcript, accepted, members, client.clock.Now()))
	if err != nil {
		return store.InstallJoinedChannelResult{}, joinedChannelStoreFailure(err)
	}
	reservationActive = false
	if (accepted.Status() == ChannelEnrollmentMemberRevoked &&
		installed.Status != store.ChannelEnrollmentMemberRevoked) ||
		(accepted.Status() == ChannelEnrollmentChannelClosed &&
			installed.Status != store.ChannelEnrollmentChannelClosed) {
		return store.InstallJoinedChannelResult{}, newChannelProtocolFailure(ChannelErrorRosterConflict, 0)
	}
	return installed, nil
}
