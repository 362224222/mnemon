package peer

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
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

// ChannelEnrollmentOwnerStore is the exact owner transaction surface used by
// the Channel handler. mnemond still owns the Store and signer; peer transport
// cannot construct durable membership evidence by itself.
type ChannelEnrollmentOwnerStore interface {
	PrepareChannelEnrollment(context.Context,
		store.PrepareChannelEnrollmentSpec,
	) (store.PrepareChannelEnrollmentResult, error)
	AcceptChannelEnrollment(context.Context,
		store.AcceptChannelEnrollmentSpec,
	) (store.AcceptChannelEnrollmentResult, error)
}

type ChannelEnrollmentOwnerOptions struct {
	Store  ChannelEnrollmentOwnerStore
	Signer store.ChannelAuthoritySigner
	Clock  channelEnrollmentClock
	Random io.Reader
}

// ChannelEnrollmentOwner serves the owner side of /mnemon/channel/1. Its
// bounded semaphore is deliberately independent of libp2p's outer resource
// manager: admitted streams must also have a fixed Store transaction budget.
type ChannelEnrollmentOwner struct {
	store  ChannelEnrollmentOwnerStore
	signer store.ChannelAuthoritySigner
	clock  channelEnrollmentClock
	random io.Reader
	budget chan struct{}
}

func NewChannelEnrollmentOwner(options ChannelEnrollmentOwnerOptions) (*ChannelEnrollmentOwner, error) {
	if options.Store == nil || options.Signer == nil {
		return nil, fmt.Errorf("%w: owner Store and signer are required", ErrChannelEnrollmentProtocol)
	}
	if options.Clock == nil {
		options.Clock = wallEnrollmentClock{}
	}
	if options.Random == nil {
		options.Random = rand.Reader
	}
	return &ChannelEnrollmentOwner{store: options.Store, signer: options.Signer,
		clock: options.Clock, random: options.Random,
		budget: make(chan struct{}, HermeticLimits().UnknownEnrollmentConnections)}, nil
}

// HandleChannelRequest serves an EnrollInit already admitted by the sole
// ChannelDispatcher. The dispatcher, not this sub-handler, owns the stream
// deadline, cancellation, first-frame reservation and final Close/Reset.
func (owner *ChannelEnrollmentOwner) HandleChannelRequest(ctx context.Context,
	stream network.Stream, initFrame ChannelFrame,
) error {
	if owner == nil || ctx == nil || stream == nil || stream.Protocol() != ChannelProtocol ||
		stream.Conn() == nil || initFrame.Type() != ChannelFrameEnrollInit {
		return ErrChannelEnrollmentProtocol
	}
	ownerPeerID, _, err := secureChannelPeer(stream.Conn().LocalPeer())
	if err != nil {
		return err
	}
	joinerPeerID, joinerPublicKey, err := secureChannelPeer(stream.Conn().RemotePeer())
	if err != nil || joinerPeerID == ownerPeerID {
		return ErrChannelEnrollmentProtocol
	}

	init, ok := initFrame.Payload().(EnrollInit)
	if !ok {
		return ErrChannelEnrollmentProtocol
	}
	if !supportsChannelFrameVersion(init.SupportedVersions(), ChannelFrameVersion) {
		return owner.writeFailure(stream, initFrame.RequestID(), ChannelErrorIncompatibleProtocol, 0)
	}

	select {
	case owner.budget <- struct{}{}:
		defer func() { <-owner.budget }()
	default:
		return owner.writeFailure(stream, initFrame.RequestID(), ChannelErrorBusy,
			channelEnrollmentBusyRetry)
	}

	prepared, err := owner.store.PrepareChannelEnrollment(ctx, store.PrepareChannelEnrollmentSpec{
		ChannelID: init.ChannelID(), GrantID: init.GrantID(), RequestID: init.EnrollmentRequestID(),
		AuthenticatedPeerID: joinerPeerID, JoinerOriginEpoch: init.OriginEpoch(),
		JoinerPublicKey: joinerPublicKey, At: owner.clock.Now(),
	})
	if err != nil {
		return owner.writeStoreFailure(stream, initFrame.RequestID(), err)
	}
	ownerNonce := make([]byte, model.EnrollmentNonceBytes)
	if _, err := io.ReadFull(owner.random, ownerNonce); err != nil {
		return err
	}
	challenge, err := NewEnrollChallenge(EnrollChallengeSpec{OwnerNonce: ownerNonce,
		SelectedVersion: ChannelFrameVersion, Limits: model.DefaultMemberLimits(),
		RosterHead: prepared.RosterHead})
	if err != nil {
		return err
	}
	challengeFrame, err := NewChannelFrame(initFrame.RequestID(), challenge)
	if err != nil || WriteChannelFrame(stream, challengeFrame) != nil {
		return ErrChannelEnrollmentProtocol
	}

	proofFrame, releaseProof, err := readChannelStreamFrame(stream, model.MaxChannelRecordBytes)
	if err != nil {
		return ErrChannelEnrollmentProtocol
	}
	defer releaseProof()
	if proofFrame.RequestID() != initFrame.RequestID() ||
		proofFrame.Type() != ChannelFrameEnrollProof {
		return ErrChannelEnrollmentProtocol
	}
	proof, ok := proofFrame.Payload().(EnrollProof)
	if !ok {
		return ErrChannelEnrollmentProtocol
	}
	transcript, err := model.NewEnrollmentTranscript(model.EnrollmentTranscriptSpec{
		ChannelID: init.ChannelID(), GrantID: init.GrantID(), RequestID: init.EnrollmentRequestID(),
		OwnerPeerID: ownerPeerID, JoinerPeerID: joinerPeerID, OwnerNonce: challenge.OwnerNonce(),
		JoinerNonce: init.JoinerNonce(), SelectedVersion: challenge.SelectedVersion(),
		Limits: challenge.Limits(), JoinerOriginEpoch: init.OriginEpoch(),
		JoinerDisplayLabel: init.DisplayLabel(), JoinerPublicKey: joinerPublicKey,
		AdvertisedMultiaddrs: init.AdvertisedMultiaddrs(), RosterHead: challenge.RosterHead(),
	})
	if err != nil {
		return ErrChannelEnrollmentProtocol
	}
	accepted, err := owner.store.AcceptChannelEnrollment(ctx, store.AcceptChannelEnrollmentSpec{
		AuthenticatedPeerID: joinerPeerID, Transcript: transcript,
		AdvertisedMultiaddrs: init.AdvertisedMultiaddrs(), Proof: proof.Proof(),
		Signer: owner.signer, At: owner.clock.Now(),
	})
	if err != nil {
		return owner.writeStoreFailure(stream, initFrame.RequestID(), err)
	}
	status, err := wireEnrollmentStatus(accepted.Status)
	if err != nil {
		return err
	}
	acceptedPayload, err := NewEnrollAccepted(status, accepted.Member,
		accepted.Roster.Members(), accepted.Receipt)
	if err != nil {
		return err
	}
	acceptedFrame, err := NewChannelFrame(initFrame.RequestID(), acceptedPayload)
	if err != nil {
		return err
	}
	// AcceptChannelEnrollment has committed before this write. Any write error
	// intentionally leaves the receipt durable for stable-request replay.
	if err := WriteChannelFrame(stream, acceptedFrame); err != nil {
		return err
	}
	// The final frame is complete. Handler closes only after frame reservations
	// are released by this call's deferred cleanup.
	return nil
}

func (owner *ChannelEnrollmentOwner) writeStoreFailure(stream network.Stream,
	requestID ChannelRequestID, cause error,
) error {
	code, retryAfter, ok := channelStoreFailure(cause)
	if !ok {
		return cause
	}
	return owner.writeFailure(stream, requestID, code, retryAfter)
}

func (owner *ChannelEnrollmentOwner) writeFailure(stream network.Stream,
	requestID ChannelRequestID, code ChannelProtocolErrorCode,
	retryAfter time.Duration,
) error {
	payload, err := NewProtocolError(ProtocolErrorSpec{Code: code,
		Retryable: code.retryable(), RetryAfter: retryAfter})
	if err != nil {
		return err
	}
	frame, err := NewChannelFrame(requestID, payload)
	if err != nil {
		return err
	}
	if err := WriteChannelFrame(stream, frame); err != nil {
		return err
	}
	return nil
}

func channelStoreFailure(cause error) (ChannelProtocolErrorCode, time.Duration, bool) {
	switch {
	case errors.Is(cause, store.ErrChannelEnrollmentOwner):
		return ChannelErrorWrongOwner, 0, true
	case errors.Is(cause, store.ErrChannelEnrollmentProof):
		return ChannelErrorBadProof, 0, true
	case errors.Is(cause, store.ErrChannelEnrollmentTokenExpired):
		return ChannelErrorTokenExpired, 0, true
	case errors.Is(cause, store.ErrChannelEnrollmentTokenClosed):
		return ChannelErrorTokenClosed, 0, true
	case errors.Is(cause, store.ErrChannelEnrollmentTokenExhausted):
		return ChannelErrorTokenExhausted, 0, true
	case errors.Is(cause, store.ErrChannelFull):
		return ChannelErrorChannelFull, 0, true
	case errors.Is(cause, store.ErrChannelEnrollmentChannelClosed):
		return ChannelErrorChannelClosed, 0, true
	case errors.Is(cause, store.ErrChannelEnrollmentMemberRevoked):
		return ChannelErrorMemberRevoked, 0, true
	case errors.Is(cause, store.ErrChannelEnrollmentStale):
		return ChannelErrorRosterGap, channelEnrollmentGapRetry, true
	case errors.Is(cause, store.ErrChannelEnrollmentConflict),
		errors.Is(cause, store.ErrChannelAuthorityInvariant):
		return ChannelErrorRosterConflict, 0, true
	case errors.Is(cause, store.ErrChannelEnrollmentUnavailable),
		errors.Is(cause, store.ErrChannelEnrollmentInput):
		return ChannelErrorInvalidToken, 0, true
	default:
		return "", 0, false
	}
}

func wireEnrollmentStatus(status store.ChannelEnrollmentStatus) (ChannelEnrollmentStatus, error) {
	switch status {
	case store.ChannelEnrollmentAccepted:
		return ChannelEnrollmentAccepted, nil
	case store.ChannelEnrollmentReplayed:
		return ChannelEnrollmentReplayed, nil
	case store.ChannelEnrollmentMemberRevoked:
		return ChannelEnrollmentMemberRevoked, nil
	case store.ChannelEnrollmentChannelClosed:
		return ChannelEnrollmentChannelClosed, nil
	default:
		return "", fmt.Errorf("%w: unknown durable enrollment status", ErrChannelEnrollmentProtocol)
	}
}

// ChannelEnrollmentJoinerStore is the joiner's one atomic replica boundary.
type ChannelEnrollmentJoinerStore interface {
	PrepareJoinedChannel(context.Context,
		store.PrepareJoinedChannelSpec,
	) (store.PrepareJoinedChannelResult, error)
	MarkJoinedChannelCommitUnknown(context.Context, model.EnrollmentRequestID,
		model.PeerID, uint64, time.Time,
	) error
	ReleaseJoinedChannelReservation(context.Context, model.EnrollmentRequestID,
		model.PeerID, uint64,
	) error
	InstallJoinedChannel(context.Context,
		store.InstallJoinedChannelSpec,
	) (store.InstallJoinedChannelResult, error)
}

type ChannelEnrollmentClientOptions struct {
	Store  ChannelEnrollmentJoinerStore
	Clock  channelEnrollmentClock
	Random io.Reader
}

type ChannelEnrollmentClient struct {
	store  ChannelEnrollmentJoinerStore
	clock  channelEnrollmentClock
	random io.Reader
}

func NewChannelEnrollmentClient(options ChannelEnrollmentClientOptions) (*ChannelEnrollmentClient, error) {
	if options.Store == nil {
		return nil, fmt.Errorf("%w: joiner Store is required", ErrChannelEnrollmentProtocol)
	}
	if options.Clock == nil {
		options.Clock = wallEnrollmentClock{}
	}
	if options.Random == nil {
		options.Random = rand.Reader
	}
	return &ChannelEnrollmentClient{store: options.Store, clock: options.Clock,
		random: options.Random}, nil
}

type JoinChannelSpec struct {
	Token                model.EnrollmentToken
	DisplayLabel         string
	AdvertisedMultiaddrs []string
	LocalAlias           string
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
		if reservationActive && !prepared.CommitUnknown {
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
		if reservationActive && !prepared.CommitUnknown {
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
	installed, err := client.store.InstallJoinedChannel(requestCtx, store.InstallJoinedChannelSpec{
		AuthenticatedOwnerPeerID: ownerPeerID, LocalAlias: spec.LocalAlias, Descriptor: descriptor,
		Transcript: transcript, Receipt: accepted.JoinReceipt(), Members: members, At: client.clock.Now(),
	})
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

func receivedChannelFailure(requestID ChannelRequestID, frame ChannelFrame) error {
	if frame.Type() != ChannelFrameProtocolError {
		return nil
	}
	if frame.RequestID() != requestID {
		return newChannelProtocolFailure(ChannelErrorRosterConflict, 0)
	}
	payload, ok := frame.Payload().(ProtocolError)
	if !ok {
		return newChannelProtocolFailure(ChannelErrorRosterConflict, 0)
	}
	return &ChannelProtocolFailure{code: payload.Code(), retryable: payload.Retryable(),
		retryAfter: payload.RetryAfter()}
}

func validEnrollmentReceiptForStatus(descriptor model.SignedChannelDescriptor,
	transcript model.EnrollmentTranscript, accepted EnrollAccepted,
) bool {
	receipt := accepted.JoinReceipt()
	member := accepted.MemberRecord()
	if accepted.Status() == ChannelEnrollmentAccepted {
		return model.VerifyEnrollmentReceipt(descriptor, member, transcript, receipt) == nil
	}
	identity, err := transcript.JoinIdentityDigest()
	previous, hasPrevious := member.PreviousDigest()
	return err == nil && receipt.RequestID() == transcript.RequestID() &&
		receipt.GrantID() == transcript.GrantID() && receipt.JoinIdentityDigest() == identity &&
		hasPrevious && member.Head().Revision() == transcript.RosterHead().Revision()+1 &&
		previous == transcript.RosterHead().Digest() &&
		model.VerifyEnrollmentReceiptEvidence(descriptor, member, receipt) == nil
}

func supportsChannelFrameVersion(versions []uint8, selected uint8) bool {
	for _, version := range versions {
		if version == selected {
			return true
		}
	}
	return false
}

func joinedChannelStoreFailure(cause error) error {
	switch {
	case errors.Is(cause, store.ErrNodeChannelLimit):
		return newChannelProtocolFailure(ChannelErrorNodeChannelLimit, 0)
	case errors.Is(cause, store.ErrChannelJoinConflict), errors.Is(cause, store.ErrChannelJoinInput),
		errors.Is(cause, store.ErrChannelAuthorityInvariant):
		return newChannelProtocolFailure(ChannelErrorRosterConflict, 0)
	case errors.Is(cause, context.Canceled), errors.Is(cause, context.DeadlineExceeded):
		return enrollmentTransportFailure(cause)
	default:
		return fmt.Errorf("%w: joined Channel Store unavailable", ErrChannelEnrollmentProtocol)
	}
}

func validEnrollmentResultStatus(status ChannelEnrollmentStatus, roster model.VerifiedRoster,
	joiner model.PeerID,
) bool {
	current, currentOK := roster.CurrentMember(joiner)
	owner, ownerOK := roster.CurrentMember(roster.Descriptor().Descriptor().OwnerPeerID())
	if !currentOK || !ownerOK {
		return false
	}
	if current.Status().Terminal() {
		return status == ChannelEnrollmentMemberRevoked
	}
	if owner.Status().Terminal() {
		return status == ChannelEnrollmentChannelClosed
	}
	return status == ChannelEnrollmentAccepted || status == ChannelEnrollmentReplayed
}

func enrollmentTransportFailure(cause error) error {
	if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		return cause
	}
	transport := errors.Is(cause, io.EOF) || errors.Is(cause, io.ErrUnexpectedEOF) ||
		errors.Is(cause, network.ErrReset)
	var netError net.Error
	transport = transport || errors.As(cause, &netError)
	if errors.Is(cause, ErrChannelFrame) && !transport {
		return newChannelProtocolFailure(ChannelErrorRosterConflict, 0)
	}
	if transport {
		return newChannelProtocolFailure(ChannelErrorOwnerUnreachable, channelEnrollmentBusyRetry)
	}
	return fmt.Errorf("%w: Channel transport unavailable", ErrChannelEnrollmentProtocol)
}

func enrollmentPrecommitTransportFailure(ctx context.Context, cause error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return enrollmentTransportFailure(cause)
}

func enrollmentOutcomeUnknown(_ error) error { return ErrChannelEnrollmentOutcomeUnknown }

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
