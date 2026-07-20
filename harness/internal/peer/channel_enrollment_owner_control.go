package peer

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var ErrChannelEnrollmentControlFailure = errors.New("Channel enrollment controller rejected request")

// ChannelEnrollmentControlFailure is the closed rejection surface a mnemond
// controller may return to the peer protocol. It deliberately carries no
// controller or Store diagnostics. Ordinary errors remain fail-closed internal
// failures and are never projected onto the wire.
type ChannelEnrollmentControlFailure struct{ code ChannelProtocolErrorCode }

func NewChannelEnrollmentControlFailure(
	code ChannelProtocolErrorCode,
) (*ChannelEnrollmentControlFailure, error) {
	if !channelEnrollmentOwnerFailureCode(code) {
		return nil, fmt.Errorf("%w: code is not valid for owner enrollment",
			ErrChannelEnrollmentProtocol)
	}
	return &ChannelEnrollmentControlFailure{code: code}, nil
}

func (failure *ChannelEnrollmentControlFailure) Error() string {
	if failure == nil || !channelEnrollmentOwnerFailureCode(failure.code) {
		return ErrChannelEnrollmentControlFailure.Error()
	}
	return fmt.Sprintf("%s: %s", ErrChannelEnrollmentControlFailure, failure.code)
}

func (failure *ChannelEnrollmentControlFailure) Unwrap() error {
	return ErrChannelEnrollmentControlFailure
}

func (failure *ChannelEnrollmentControlFailure) Code() ChannelProtocolErrorCode {
	if failure == nil {
		return ""
	}
	return failure.code
}

// ChannelEnrollmentChallengeControl is authenticated transport input for one
// owner challenge. AuthenticatedPeerID and JoinerPublicKey always come from the
// secure libp2p stream; EnrollInit fields are untrusted claims.
type ChannelEnrollmentChallengeControl struct {
	AuthenticatedPeerID model.PeerID
	ChannelID           model.ChannelID
	GrantID             model.GrantID
	RequestID           model.EnrollmentRequestID
	JoinerOriginEpoch   model.OriginEpoch
	JoinerPublicKey     []byte
	At                  time.Time
}

type ChannelEnrollmentChallengeAuthority struct {
	RosterHead model.RecordHead
}

// ChannelEnrollmentAcceptanceControl carries the exact transcript already
// reconstructed by the peer protocol. Proof remains untrusted evidence; the
// controller owns proof verification and the accepted authority transition.
type ChannelEnrollmentAcceptanceControl struct {
	AuthenticatedPeerID  model.PeerID
	Transcript           model.EnrollmentTranscript
	AdvertisedMultiaddrs []string
	Proof                model.Digest
	At                   time.Time
}

type ChannelEnrollmentAcceptanceAuthority struct {
	Status  ChannelEnrollmentStatus
	Member  model.Member
	Roster  []model.Member
	Receipt model.EnrollmentReceipt
}

// ChannelEnrollmentOwnerController is the narrow mnemond control boundary for
// the owner side of /mnemon/channel/1. Its method names intentionally do not
// mirror Store methods, so neither a raw Store nor a signer can accidentally
// satisfy it.
//
// PrepareEnrollmentChallenge may perform bounded durable reads. The peer calls
// AcceptEnrollmentAuthority only after the proof transcript is complete. That
// callback owns prepare, signing, fenced durable commit and runtime authority
// installation, and may return success only after all accepted authority is
// visible. The peer invokes callbacks without holding a peer lock and owns no
// Store transaction around them.
//
// A successful acceptance is durable before the peer writes EnrollAccepted.
// Therefore the controller must replay the same stable request after response
// loss without creating another member, grant use or receipt.
type ChannelEnrollmentOwnerController interface {
	PrepareEnrollmentChallenge(context.Context,
		ChannelEnrollmentChallengeControl,
	) (ChannelEnrollmentChallengeAuthority, error)
	AcceptEnrollmentAuthority(context.Context,
		ChannelEnrollmentAcceptanceControl,
	) (ChannelEnrollmentAcceptanceAuthority, error)
}

func channelEnrollmentControllerFailure(cause error) (ChannelProtocolErrorCode, time.Duration, bool) {
	var failure *ChannelEnrollmentControlFailure
	if !errors.As(cause, &failure) || failure == nil ||
		!channelEnrollmentOwnerFailureCode(failure.code) {
		return "", 0, false
	}
	return failure.code, channelEnrollmentOwnerRetryAfter(failure.code), true
}

func channelEnrollmentOwnerFailureCode(code ChannelProtocolErrorCode) bool {
	switch code {
	case ChannelErrorBusy, ChannelErrorInvalidToken, ChannelErrorWrongOwner,
		ChannelErrorTokenExpired, ChannelErrorTokenClosed, ChannelErrorTokenExhausted,
		ChannelErrorChannelFull, ChannelErrorBadProof, ChannelErrorMemberRevoked,
		ChannelErrorChannelClosed, ChannelErrorRosterGap, ChannelErrorRosterConflict:
		return true
	default:
		return false
	}
}

func channelEnrollmentOwnerRetryAfter(code ChannelProtocolErrorCode) time.Duration {
	switch code {
	case ChannelErrorBusy:
		return channelEnrollmentBusyRetry
	case ChannelErrorRosterGap:
		return channelEnrollmentGapRetry
	default:
		return 0
	}
}

func isNilChannelEnrollmentOwnerController(controller ChannelEnrollmentOwnerController) bool {
	if controller == nil {
		return true
	}
	value := reflect.ValueOf(controller)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
