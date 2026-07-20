package peer

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var ErrChannelJoinControlFailure = errors.New("Channel join controller rejected request")

// ChannelJoinControlFailure is the closed, secret-free failure surface of the
// joiner's mnemond callback. Ordinary controller diagnostics are never exposed
// as protocol decisions.
type ChannelJoinControlFailure struct{ code ChannelProtocolErrorCode }

func NewChannelJoinControlFailure(code ChannelProtocolErrorCode) (*ChannelJoinControlFailure, error) {
	if code != ChannelErrorNodeChannelLimit && code != ChannelErrorRosterConflict {
		return nil, fmt.Errorf("%w: code is not valid for joined Channel control",
			ErrChannelEnrollmentProtocol)
	}
	return &ChannelJoinControlFailure{code: code}, nil
}

func (failure *ChannelJoinControlFailure) Error() string {
	if failure == nil || (failure.code != ChannelErrorNodeChannelLimit &&
		failure.code != ChannelErrorRosterConflict) {
		return ErrChannelJoinControlFailure.Error()
	}
	return fmt.Sprintf("%s: %s", ErrChannelJoinControlFailure, failure.code)
}

func (failure *ChannelJoinControlFailure) Unwrap() error { return ErrChannelJoinControlFailure }

func (failure *ChannelJoinControlFailure) Code() ChannelProtocolErrorCode {
	if failure == nil {
		return ""
	}
	return failure.code
}

// ChannelJoinPrepareControl is the exact authenticated local identity and
// frozen token projection that mnemond must reserve before any dial occurs.
type ChannelJoinPrepareControl struct {
	AuthenticatedLocalPeerID model.PeerID
	LocalPublicKey           []byte
	AdvertisedMultiaddrs     []string
	Descriptor               model.SignedChannelDescriptor
	GrantID                  model.GrantID
	LocalAlias               string
	At                       time.Time
}

type preparedChannelJoinClaim struct{ used atomic.Bool }

// PreparedChannelJoin is a one-use, opaque durable reservation projection.
// The Store attempt remains private to the Node-owned session.
type PreparedChannelJoin struct {
	requestID     model.EnrollmentRequestID
	originEpoch   model.OriginEpoch
	reserved      bool
	commitUnknown bool
	claim         *preparedChannelJoinClaim
}

func NewPreparedChannelJoin(requestID model.EnrollmentRequestID, originEpoch model.OriginEpoch,
	reserved, commitUnknown bool,
) (PreparedChannelJoin, error) {
	if requestID.IsZero() || originEpoch.IsZero() || commitUnknown && !reserved {
		return PreparedChannelJoin{}, fmt.Errorf("%w: invalid prepared join",
			ErrChannelEnrollmentProtocol)
	}
	return PreparedChannelJoin{requestID: requestID, originEpoch: originEpoch,
		reserved: reserved, commitUnknown: commitUnknown, claim: &preparedChannelJoinClaim{}}, nil
}

func (prepared PreparedChannelJoin) RequestID() model.EnrollmentRequestID { return prepared.requestID }
func (prepared PreparedChannelJoin) OriginEpoch() model.OriginEpoch       { return prepared.originEpoch }
func (prepared PreparedChannelJoin) Reserved() bool                       { return prepared.reserved }
func (prepared PreparedChannelJoin) CommitUnknown() bool                  { return prepared.commitUnknown }

func (prepared PreparedChannelJoin) claimOnce() bool {
	return prepared.claim != nil && !prepared.requestID.IsZero() && !prepared.originEpoch.IsZero() &&
		(!prepared.commitUnknown || prepared.reserved) && prepared.claim.used.CompareAndSwap(false, true)
}

// VerifiedChannelEnrollment is immutable owner evidence accepted by the peer
// protocol. Only the Node session may convert it into durable replica state.
type VerifiedChannelEnrollment struct {
	owner      model.PeerID
	status     ChannelEnrollmentStatus
	descriptor model.SignedChannelDescriptor
	transcript model.EnrollmentTranscript
	receipt    model.EnrollmentReceipt
	roster     model.VerifiedRoster
}

func (accepted VerifiedChannelEnrollment) AuthenticatedOwnerPeerID() model.PeerID {
	return accepted.owner
}
func (accepted VerifiedChannelEnrollment) Status() ChannelEnrollmentStatus { return accepted.status }
func (accepted VerifiedChannelEnrollment) Descriptor() model.SignedChannelDescriptor {
	return accepted.descriptor
}
func (accepted VerifiedChannelEnrollment) Transcript() model.EnrollmentTranscript {
	return accepted.transcript
}
func (accepted VerifiedChannelEnrollment) Receipt() model.EnrollmentReceipt { return accepted.receipt }
func (accepted VerifiedChannelEnrollment) Roster() model.VerifiedRoster     { return accepted.roster }

type ChannelJoinResultSpec struct {
	Installed bool
	Status    ChannelEnrollmentStatus
	Channel   model.Channel
	Roster    model.VerifiedRoster
}

// ChannelJoinResult is the peer-native projection returned after the Node has
// durably committed and installed the accepted runtime authority.
type ChannelJoinResult struct{ spec ChannelJoinResultSpec }

func NewChannelJoinResult(spec ChannelJoinResultSpec) (ChannelJoinResult, error) {
	if !spec.Status.Valid() || spec.Roster.IsZero() {
		return ChannelJoinResult{}, fmt.Errorf("%w: invalid joined Channel result",
			ErrChannelEnrollmentProtocol)
	}
	if !spec.Channel.ID().IsZero() && (spec.Channel.Descriptor().Descriptor().ID() !=
		spec.Roster.Descriptor().Descriptor().ID() || spec.Channel.RosterHead() != spec.Roster.Head()) {
		return ChannelJoinResult{}, fmt.Errorf("%w: joined Channel result authority differs",
			ErrChannelEnrollmentProtocol)
	}
	return ChannelJoinResult{spec: spec}, nil
}

func (result ChannelJoinResult) Installed() bool                 { return result.spec.Installed }
func (result ChannelJoinResult) Status() ChannelEnrollmentStatus { return result.spec.Status }
func (result ChannelJoinResult) Channel() model.Channel          { return result.spec.Channel }
func (result ChannelJoinResult) Roster() model.VerifiedRoster    { return result.spec.Roster }

// ChannelJoinSession is a one-use semantic adapter owned by mnemond. Begin must
// atomically claim the session. Every later method is bound to that prepared
// request and accepts no caller-selected request, Peer or attempt identity.
// Callbacks may block, run without peer locks or Store transactions, and must
// never reacquire the Node's already-held Channel authority token.
type ChannelJoinSession interface {
	BeginChannelJoin(context.Context, ChannelJoinPrepareControl) (PreparedChannelJoin, error)
	MarkChannelJoinCommitUnknown(context.Context, time.Time) error
	ReleaseChannelJoinReservation(context.Context) error
	InstallAcceptedChannelJoin(context.Context, VerifiedChannelEnrollment, time.Time) (ChannelJoinResult, error)
}

type JoinChannelSpec struct {
	Token        model.EnrollmentToken
	DisplayLabel string
	LocalAlias   string
}

type channelEnrollmentClient struct {
	session ChannelJoinSession
	clock   channelEnrollmentClock
	random  io.Reader
}

func newChannelEnrollmentClient(session ChannelJoinSession, clock channelEnrollmentClock,
	random io.Reader,
) (*channelEnrollmentClient, error) {
	if isNilChannelJoinSession(session) {
		return nil, fmt.Errorf("%w: join session is required", ErrChannelEnrollmentProtocol)
	}
	if clock == nil {
		clock = wallEnrollmentClock{}
	}
	if random == nil {
		random = rand.Reader
	}
	return &channelEnrollmentClient{session: session, clock: clock, random: random}, nil
}

func isNilChannelJoinSession(session ChannelJoinSession) bool {
	if session == nil {
		return true
	}
	value := reflect.ValueOf(session)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func channelJoinControlError(cause error) error {
	if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		return cause
	}
	var failure *ChannelJoinControlFailure
	if errors.As(cause, &failure) && failure != nil {
		return newChannelProtocolFailure(failure.Code(), 0)
	}
	return fmt.Errorf("%w: Channel join controller unavailable", ErrChannelEnrollmentProtocol)
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
	if errors.Is(cause, errEnrollmentTransportPermitBusy) ||
		errors.Is(cause, ErrEnrollmentTransportPermitExists) ||
		errors.Is(cause, network.ErrResourceLimitExceeded) {
		return newChannelProtocolFailure(ChannelErrorBusy, channelEnrollmentBusyRetry)
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
