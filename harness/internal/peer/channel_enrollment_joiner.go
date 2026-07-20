package peer

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"io"
	"net"
	"time"
)

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

func joinedChannelInstallSpec(ownerPeerID model.PeerID, localAlias string,
	descriptor model.SignedChannelDescriptor, transcript model.EnrollmentTranscript,
	accepted EnrollAccepted, members []model.Member, at time.Time,
) store.InstallJoinedChannelSpec {
	return store.InstallJoinedChannelSpec{AuthenticatedOwnerPeerID: ownerPeerID,
		OwnerOutcome: store.ChannelEnrollmentStatus(accepted.Status()), LocalAlias: localAlias,
		Descriptor: descriptor, Transcript: transcript, Receipt: accepted.JoinReceipt(),
		Members: members, At: at}
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
