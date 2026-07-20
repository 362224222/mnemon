package peer

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestPreparedChannelJoinIsValidatedAndConsumedOnce(t *testing.T) {
	requestID, _ := model.ParseEnrollmentRequestID("request-peer-prepared-once")
	epoch, _ := model.ParseOriginEpoch("epoch-peer-prepared-once")
	for _, input := range []struct {
		request model.EnrollmentRequestID
		epoch   model.OriginEpoch
		reserve bool
		unknown bool
	}{{epoch: epoch}, {request: requestID}, {request: requestID, epoch: epoch, unknown: true}} {
		if prepared, err := NewPreparedChannelJoin(input.request, input.epoch,
			input.reserve, input.unknown); err == nil || prepared.claimOnce() {
			t.Fatalf("invalid prepared join = (%#v,%v)", prepared, err)
		}
	}
	prepared, err := NewPreparedChannelJoin(requestID, epoch, true, true)
	if err != nil || prepared.RequestID() != requestID || prepared.OriginEpoch() != epoch ||
		!prepared.Reserved() || !prepared.CommitUnknown() {
		t.Fatalf("valid prepared join = (%#v,%v)", prepared, err)
	}
	copies := []PreparedChannelJoin{prepared, prepared}
	start := make(chan struct{})
	results := make(chan bool, len(copies))
	var workers sync.WaitGroup
	for _, copyValue := range copies {
		workers.Add(1)
		go func(candidate PreparedChannelJoin) {
			defer workers.Done()
			<-start
			results <- candidate.claimOnce()
		}(copyValue)
	}
	close(start)
	workers.Wait()
	close(results)
	claimed := 0
	for result := range results {
		if result {
			claimed++
		}
	}
	if claimed != 1 {
		t.Fatalf("prepared join claims = %d, want one", claimed)
	}
}

func TestChannelJoinControlSurfaceIsSemanticAndClosed(t *testing.T) {
	if _, ok := any(&store.Store{}).(ChannelJoinSession); ok {
		t.Fatal("raw Store unexpectedly satisfies semantic ChannelJoinSession")
	}
	if client, err := newChannelEnrollmentClient(nil, nil, nil); client != nil || err == nil {
		t.Fatalf("missing session client = (%#v,%v)", client, err)
	}
	for _, code := range []ChannelProtocolErrorCode{ChannelErrorNodeChannelLimit,
		ChannelErrorRosterConflict} {
		failure, err := NewChannelJoinControlFailure(code)
		if err != nil || failure.Code() != code || !errors.Is(failure, ErrChannelJoinControlFailure) {
			t.Fatalf("join control failure %s = (%#v,%v)", code, failure, err)
		}
	}
	if failure, err := NewChannelJoinControlFailure(ChannelErrorWrongOwner); failure != nil || err == nil {
		t.Fatalf("invalid join control failure = (%#v,%v)", failure, err)
	}
}

func TestChannelJoinResultRejectsCrossAuthorityProjection(t *testing.T) {
	fixture := testkit.NewSignedChannel(t, "peer-join-result")
	roster, err := model.NewVerifiedRoster(fixture.Descriptor(),
		[]model.Member{fixture.OwnerMember().Member()})
	if err != nil {
		t.Fatal(err)
	}
	result, err := NewChannelJoinResult(ChannelJoinResultSpec{Status: ChannelEnrollmentAccepted,
		Channel: fixture.Channel(), Roster: roster})
	if err != nil || result.Status() != ChannelEnrollmentAccepted ||
		result.Channel().ID() != fixture.Channel().ID() || result.Roster().Head() != roster.Head() {
		t.Fatalf("valid Channel join result = (%#v,%v)", result, err)
	}
	other := testkit.NewSignedChannel(t, "peer-join-result-other")
	if result, err := NewChannelJoinResult(ChannelJoinResultSpec{Status: ChannelEnrollmentAccepted,
		Channel: other.Channel(), Roster: roster}); err == nil || result.Status().Valid() {
		t.Fatalf("cross-authority join result = (%#v,%v)", result, err)
	}
}

func TestChannelEnrollmentFailureCategoriesRemainClosed(t *testing.T) {
	var failure *ChannelProtocolFailure
	for _, cause := range []error{errEnrollmentTransportPermitBusy,
		ErrEnrollmentTransportPermitExists, network.ErrResourceLimitExceeded} {
		err := enrollmentTransportFailure(cause)
		if !errors.As(err, &failure) || failure.Code() != ChannelErrorBusy ||
			!failure.Retryable() || failure.RetryAfter() != channelEnrollmentBusyRetry {
			t.Fatalf("bounded transport failure %v = %#v", cause, err)
		}
	}
	err := enrollmentTransportFailure(io.EOF)
	if !errors.As(err, &failure) || failure.Code() != ChannelErrorOwnerUnreachable ||
		!failure.Retryable() || failure.RetryAfter() != channelEnrollmentBusyRetry ||
		!errors.Is(err, ErrChannelEnrollmentProtocol) {
		t.Fatalf("transport failure = %#v", err)
	}
	err = enrollmentTransportFailure(channelFrameError("authenticated malformed response", nil))
	if !errors.As(err, &failure) || failure.Code() != ChannelErrorRosterConflict || failure.Retryable() {
		t.Fatalf("malformed authenticated response = %#v", err)
	}
	if err := enrollmentPrecommitTransportFailure(canceledEnrollmentContext(), io.EOF); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled precommit transport = %v", err)
	}
	if err := enrollmentOutcomeUnknown(network.ErrReset); !errors.Is(err, ErrChannelEnrollmentOutcomeUnknown) {
		t.Fatalf("post-proof response loss = %v", err)
	}
}
