package peer

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"io"
	"time"
)

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

func supportsChannelFrameVersion(versions []uint8, selected uint8) bool {
	for _, version := range versions {
		if version == selected {
			return true
		}
	}
	return false
}
