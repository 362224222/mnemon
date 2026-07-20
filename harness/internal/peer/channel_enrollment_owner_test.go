package peer

import (
	"bytes"
	"context"
	"errors"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
	"testing"
	"time"
)

func TestChannelEnrollmentOwnerRejectsUnsupportedVersionBeforeStore(t *testing.T) {
	fixture := testkit.NewSignedChannel(t, "peer-enrollment-version")
	joiner := testkit.NewIdentity(t, "peer-enrollment-version-joiner")
	ownerHost := newEnrollmentTestHost(t, fixture.Owner())
	defer ownerHost.Close()
	joinerHost := newEnrollmentTestHost(t, joiner)
	defer joinerHost.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := joinerHost.Connect(ctx, libp2ppeer.AddrInfo{ID: ownerHost.ID(),
		Addrs: ownerHost.Addrs()}); err != nil {
		t.Fatal(err)
	}
	called := make(chan struct{}, 1)
	ownerProtocol, err := NewChannelEnrollmentOwner(ChannelEnrollmentOwnerOptions{
		Store:  unexpectedEnrollmentOwnerStore{called: called},
		Signer: enrollmentTestSigner{privateKey: enrollmentPrivateKey(t, fixture.Owner())},
	})
	if err != nil {
		t.Fatal(err)
	}
	registerEnrollmentTestDispatcher(t, ctx, ownerHost, ownerProtocol)
	grantID, _ := model.ParseGrantID("grant-peer-enrollment-version")
	identity, err := model.EnrollmentJoinIdentityDigest(fixture.Channel().ID(), grantID,
		joiner.PeerID(), joiner.PublicKey(), joiner.OriginEpoch())
	if err != nil {
		t.Fatal(err)
	}
	requestID, err := model.EnrollmentRequestIDForJoinIdentity(identity)
	if err != nil {
		t.Fatal(err)
	}
	init, err := NewEnrollInit(EnrollInitSpec{ChannelID: fixture.Channel().ID(), GrantID: grantID,
		EnrollmentRequestID: requestID,
		JoinerNonce:         bytes.Repeat([]byte{0x55}, model.EnrollmentNonceBytes),
		SupportedVersions:   []uint8{2}, OriginEpoch: joiner.OriginEpoch(),
		DisplayLabel: joiner.DisplayName(), AdvertisedMultiaddrs: joiner.Multiaddrs()})
	if err != nil {
		t.Fatal(err)
	}
	frameRequestID, err := ParseChannelRequestID("channel-request-303132333435363738393a3b3c3d3e3f")
	if err != nil {
		t.Fatal(err)
	}
	frame, err := NewChannelFrame(frameRequestID, init)
	if err != nil {
		t.Fatal(err)
	}
	stream := openEnrollmentTestStream(t, ctx, joinerHost, ownerHost.ID())
	defer stream.Close()
	if err := WriteChannelFrame(stream, frame); err != nil {
		t.Fatal(err)
	}
	response, err := ReadChannelFrame(stream)
	if err != nil {
		t.Fatal(err)
	}
	failure, ok := response.Payload().(ProtocolError)
	if response.RequestID() != frameRequestID || !ok || failure.Code() != ChannelErrorIncompatibleProtocol ||
		failure.Retryable() {
		t.Fatalf("unsupported-version response = %#v", response)
	}
	select {
	case <-called:
		t.Fatal("unsupported version reached owner Store")
	default:
	}
}

func TestChannelEnrollmentMapsOnlyTypedStableStoreFailures(t *testing.T) {
	tests := []struct {
		cause      error
		code       ChannelProtocolErrorCode
		retryAfter time.Duration
	}{
		{store.ErrChannelEnrollmentOwner, ChannelErrorWrongOwner, 0},
		{store.ErrChannelEnrollmentProof, ChannelErrorBadProof, 0},
		{store.ErrChannelEnrollmentTokenExpired, ChannelErrorTokenExpired, 0},
		{store.ErrChannelEnrollmentTokenClosed, ChannelErrorTokenClosed, 0},
		{store.ErrChannelEnrollmentTokenExhausted, ChannelErrorTokenExhausted, 0},
		{store.ErrChannelFull, ChannelErrorChannelFull, 0},
		{store.ErrChannelEnrollmentChannelClosed, ChannelErrorChannelClosed, 0},
		{store.ErrChannelEnrollmentMemberRevoked, ChannelErrorMemberRevoked, 0},
		{store.ErrChannelEnrollmentStale, ChannelErrorRosterGap, channelEnrollmentGapRetry},
		{store.ErrChannelEnrollmentConflict, ChannelErrorRosterConflict, 0},
		{store.ErrChannelEnrollmentUnavailable, ChannelErrorInvalidToken, 0},
	}
	for _, test := range tests {
		code, retryAfter, ok := channelStoreFailure(fmtWrappedEnrollmentError{test.cause})
		if !ok || code != test.code || retryAfter != test.retryAfter {
			t.Errorf("channelStoreFailure(%v) = (%q,%s,%t)", test.cause, code, retryAfter, ok)
		}
	}
	if code, retryAfter, ok := channelStoreFailure(errors.New("sqlite path and secret detail")); ok || code != "" || retryAfter != 0 {
		t.Fatalf("untyped Store failure leaked to wire = (%q,%s,%t)", code, retryAfter, ok)
	}
	if code, retryAfter, ok := channelStoreFailure(context.DeadlineExceeded); ok || code != "" || retryAfter != 0 {
		t.Fatalf("ambiguous Store timeout leaked as a rejection = (%q,%s,%t)", code, retryAfter, ok)
	}
}
