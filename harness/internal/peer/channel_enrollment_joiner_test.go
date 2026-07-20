package peer

import (
	"bytes"
	"context"
	"errors"
	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
	"io"
	"testing"
	"time"
)

func TestChannelEnrollmentClientRejectsWrongSecureOwner(t *testing.T) {
	createdAt := time.Date(2026, 7, 18, 2, 0, 0, 0, time.UTC)
	ownerFixture := testkit.NewSignedChannelAt(t, "peer-enrollment-owner", createdAt)
	joiner := testkit.NewIdentity(t, "peer-enrollment-owner-joiner")
	wrongOwner := testkit.NewIdentity(t, "peer-enrollment-wrong-owner")
	joinerStore := openEnrollmentTestStore(t, joiner, createdAt)
	grantID, _ := model.ParseGrantID("grant-peer-enrollment-owner")
	token := enrollmentTestToken(t, ownerFixture, grantID, "peer-enrollment-owner")
	joinerHost := newEnrollmentTestHost(t, joiner)
	defer joinerHost.Close()
	wrongHost := newEnrollmentTestHost(t, wrongOwner)
	defer wrongHost.Close()
	wrongHost.SetStreamHandler(ChannelProtocol, func(stream network.Stream) {
		defer stream.Close()
		_, _ = io.Copy(io.Discard, stream)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := joinerHost.Connect(ctx, libp2ppeer.AddrInfo{ID: wrongHost.ID(),
		Addrs: wrongHost.Addrs()}); err != nil {
		t.Fatal(err)
	}
	client, err := NewChannelEnrollmentClient(ChannelEnrollmentClientOptions{Store: joinerStore,
		Clock:  fixedEnrollmentClock{at: createdAt.Add(time.Minute)},
		Random: bytes.NewReader(bytes.Repeat([]byte{0x51}, model.EnrollmentNonceBytes))})
	if err != nil {
		t.Fatal(err)
	}
	stream := openEnrollmentTestStream(t, ctx, joinerHost, wrongHost.ID())
	_, err = client.Join(ctx, stream, JoinChannelSpec{Token: token, DisplayLabel: joiner.DisplayName(),
		AdvertisedMultiaddrs: joiner.Multiaddrs(), LocalAlias: "wrong-owner"})
	var failure *ChannelProtocolFailure
	if !errors.As(err, &failure) || failure.Code() != ChannelErrorWrongOwner || failure.Retryable() {
		t.Fatalf("wrong secure owner error = %#v", err)
	}
}

func TestChannelEnrollmentConstructorsRejectMissingControlAuthority(t *testing.T) {
	if owner, err := NewChannelEnrollmentOwner(ChannelEnrollmentOwnerOptions{}); owner != nil || err == nil {
		t.Fatalf("empty owner = (%#v,%v)", owner, err)
	}
	if client, err := NewChannelEnrollmentClient(ChannelEnrollmentClientOptions{}); client != nil || err == nil {
		t.Fatalf("empty client = (%#v,%v)", client, err)
	}
	var failure *ChannelProtocolFailure
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
