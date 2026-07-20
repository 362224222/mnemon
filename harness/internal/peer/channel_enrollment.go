package peer

import (
	"context"
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
