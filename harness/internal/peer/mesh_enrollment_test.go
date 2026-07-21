package peer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestMeshRuntimeAdvertisesConcreteBoundedAddresses(t *testing.T) {
	t.Parallel()
	owner := testkit.NewIdentity(t, "mesh-enrollment-addresses")
	st := openPeerMeshStore(t, owner, peerMeshTime(t, "2026-07-18T08:00:00Z"))
	runtime := newTestMeshRuntime(t, context.Background(), owner, readMeshRuntimeAuthority(t, st))
	addresses := runtime.AdvertisedMultiaddrs()
	if len(addresses) == 0 {
		t.Fatal("Mesh runtime advertised no concrete listener address")
	}
	for _, address := range addresses {
		if address == "/ip4/0.0.0.0/tcp/0" || address == "/ip6/::/tcp/0" {
			t.Fatalf("Mesh runtime advertised wildcard address %q", address)
		}
	}
}

func TestEnrollmentRetryPolicyKeepsOnlyStableRetryableFailures(t *testing.T) {
	t.Parallel()
	busy := newChannelProtocolFailure(ChannelErrorBusy, channelEnrollmentBusyRetry)
	if !retryableEnrollmentAttempt(busy) || enrollmentRetryDelay(busy) != channelEnrollmentBusyRetry {
		t.Fatalf("busy enrollment failure was not retryable with wire delay: %v", busy)
	}
	if !retryableEnrollmentAttempt(ErrChannelEnrollmentOutcomeUnknown) ||
		enrollmentRetryDelay(ErrChannelEnrollmentOutcomeUnknown) != channelEnrollmentGapRetry {
		t.Fatal("outcome-unknown enrollment failure did not use bounded replay retry")
	}
	if !retryableEnrollmentAttempt(errors.Join(ErrMeshRuntime, errors.New("open enrollment stream"))) {
		t.Fatal("enrollment transport failure was not retryable")
	}
	permanent := newChannelProtocolFailure(ChannelErrorMemberRevoked, 0)
	if retryableEnrollmentAttempt(permanent) || enrollmentRetryDelay(permanent) <= 0 {
		t.Fatalf("permanent enrollment failure retry policy = retryable %v delay %s",
			retryableEnrollmentAttempt(permanent), enrollmentRetryDelay(permanent))
	}
}

func TestWaitEnrollmentRetryHonorsContextCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	if err := waitEnrollmentRetry(ctx, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled enrollment retry wait error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("cancelled enrollment retry waited %s", elapsed)
	}
}
