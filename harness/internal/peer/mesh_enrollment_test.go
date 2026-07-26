package peer

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
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

func TestMeshEnrollmentAuthorityLoadFailureClosesStaleRuntime(t *testing.T) {
	local := testkit.NewIdentity(t, "mesh-enrollment-load-failure-local")
	remote := testkit.NewIdentity(t, "mesh-enrollment-load-failure-remote")
	st := openPeerMeshStore(t, local, peerMeshTime(t, "2026-07-18T08:30:00Z"))
	runtime := newTestMeshRuntime(t, context.Background(), local,
		readMeshRuntimeAuthority(t, st))
	remoteID := meshRuntimeLibp2pID(t, remote.PeerID())
	permit := &meshEnrollmentPermit{owner: remote.PeerID(), ownerID: remoteID}
	runtime.mu.Lock()
	runtime.enrollment = permit
	runtime.revision++
	runtime.mu.Unlock()

	loadErr := errors.New("durable authority unavailable")
	err := runtime.finishEnrollment(context.Background(), permit,
		func(context.Context) (store.ChannelMeshAuthority, error) {
			return store.ChannelMeshAuthority{}, loadErr
		})
	if !errors.Is(err, loadErr) {
		t.Fatalf("authority load failure = %v", err)
	}
	runtime.mu.Lock()
	closed := runtime.closed
	runtime.mu.Unlock()
	if !closed {
		t.Fatal("runtime remained open with an unknown post-enrollment authority")
	}
}

func TestMeshEnrollmentExchangeAndAuthorityLoadDoNotHoldRuntimeLock(t *testing.T) {
	fixture := newEnrollmentHandshakeFixture(t, "mesh-enrollment-real-lock",
		time.Date(2026, 7, 18, 9, 0, 0, 0, time.UTC), false)
	if err := fixture.joinerHost.Close(); err != nil {
		t.Fatal(err)
	}
	runtime := newTestMeshRuntime(t, fixture.ctx, fixture.joinerIdentity,
		readMeshRuntimeAuthority(t, fixture.joinerStore))
	runtime.Host().Peerstore().AddAddrs(fixture.ownerHost.ID(), fixture.ownerHost.Addrs(),
		peerstore.PermanentAddrTTL)

	barrier := &committedEnrollmentOwnerStore{delegate: fixture.ownerStore,
		committed: make(chan struct{}), resume: make(chan struct{})}
	var releaseOwner sync.Once
	cleanupOwner := func() { releaseOwner.Do(func() { close(barrier.resume) }) }
	t.Cleanup(cleanupOwner)
	ownerProtocol, err := NewChannelEnrollmentOwner(ChannelEnrollmentOwnerOptions{
		Store: barrier,
		Signer: enrollmentTestSigner{
			privateKey: enrollmentPrivateKey(t, fixture.ownerIdentity),
		},
		Clock:  fixedEnrollmentClock{at: fixture.acceptedAt},
		Random: bytes.NewReader(bytes.Repeat([]byte{0x35}, model.EnrollmentNonceBytes*4)),
	})
	if err != nil {
		t.Fatal(err)
	}
	registerEnrollmentTestDispatcher(t, fixture.ctx, fixture.ownerHost, ownerProtocol)
	client, err := NewChannelEnrollmentClient(ChannelEnrollmentClientOptions{
		Store: fixture.joinerStore, Clock: fixedEnrollmentClock{at: fixture.acceptedAt},
		Random: bytes.NewReader(bytes.Repeat([]byte{0x45},
			(model.EnrollmentNonceBytes+channelRequestIDBytes)*2)),
	})
	if err != nil {
		t.Fatal(err)
	}
	spec := JoinChannelSpec{Token: fixture.token,
		DisplayLabel:         fixture.joinerIdentity.DisplayName(),
		AdvertisedMultiaddrs: runtime.AdvertisedMultiaddrs(), LocalAlias: "real-lock"}

	loadStarted, releaseLoad := make(chan struct{}), make(chan struct{})
	var releaseLoader sync.Once
	cleanupLoader := func() { releaseLoader.Do(func() { close(releaseLoad) }) }
	t.Cleanup(cleanupLoader)
	loadCalls := 0
	load := func(ctx context.Context) (store.ChannelMeshAuthority, error) {
		mesh, loadErr := fixture.joinerStore.ReadChannelMeshAuthority(ctx)
		loadCalls++
		if loadCalls == 1 {
			close(loadStarted)
			<-releaseLoad
		}
		return mesh, loadErr
	}
	type enrollmentResult struct {
		result store.InstallJoinedChannelResult
		err    error
	}
	resultC := make(chan enrollmentResult, 1)
	go func() {
		result, enrollErr := runtime.EnrollChannel(fixture.ctx, client, spec, load)
		resultC <- enrollmentResult{result: result, err: enrollErr}
	}()

	select {
	case <-barrier.committed:
	case <-fixture.ctx.Done():
		t.Fatal(fixture.ctx.Err())
	}
	if err := runtime.Reconcile(readMeshRuntimeAuthority(t, fixture.joinerStore)); err != nil {
		t.Fatalf("reconcile during real enrollment exchange: %v", err)
	}
	ownerID, err := canonicalLibp2pID(fixture.ownerIdentity.PeerID())
	if err != nil || !runtime.authority.CanDial(ownerID) {
		t.Fatalf("concurrent reconcile dropped enrollment permit: owner=%q err=%v", ownerID, err)
	}
	releaseOwner.Do(func() { close(barrier.resume) })

	select {
	case <-loadStarted:
	case <-fixture.ctx.Done():
		t.Fatal(fixture.ctx.Err())
	}
	if err := runtime.Reconcile(readMeshRuntimeAuthority(t, fixture.joinerStore)); err != nil {
		t.Fatalf("reconcile during durable authority load: %v", err)
	}
	releaseLoader.Do(func() { close(releaseLoad) })
	enrolled := <-resultC
	if enrolled.err != nil || !enrolled.result.Installed {
		t.Fatalf("real enrollment result = (%#v, %v)", enrolled.result, enrolled.err)
	}
	if loadCalls != 2 {
		t.Fatalf("authority loads = %d, want one bounded conflict retry", loadCalls)
	}
	_, enrollmentRetained := runtime.authority.state.Load().outboundEnrollment[ownerID]
	if enrollmentRetained || !runtime.HasCurrentSession(fixture.ownerFixture.Channel().ID()) {
		t.Fatal("settled enrollment retained permit or omitted joined Channel session")
	}
}
