package peer

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"strings"
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

func TestMeshEnrollmentPreparingSlotRejectsConcurrentJoinWithoutTouchingReservation(t *testing.T) {
	exercise := newPreparingSlotEnrollmentExercise(t)
	first := exercise.start()
	exercise.blocked.waitPrepared(t)
	if reservations := channelJoinReservationCount(
		t, exercise.fixture.joinerStore.Path()); reservations != 1 {
		t.Fatalf("first durable reservations = %d, want one", reservations)
	}
	exercise.assertConcurrentBusy(t, first)
	exercise.assertFirstSettles(t, first)
	exercise.assertPrepareFailureClearsSlot(t)
}

type preparingSlotEnrollmentExercise struct {
	fixture enrollmentHandshakeFixture
	runtime *MeshRuntime
	blocked *preparingSlotJoinStore
	client  *ChannelEnrollmentClient
	spec    JoinChannelSpec

	loadMu    sync.Mutex
	loadCalls int
}

func newPreparingSlotEnrollmentExercise(t *testing.T) *preparingSlotEnrollmentExercise {
	t.Helper()
	fixture := newEnrollmentHandshakeFixture(t, "mesh-enrollment-preparing-slot",
		time.Date(2026, 7, 18, 8, 45, 0, 0, time.UTC), true)
	if err := fixture.joinerHost.Close(); err != nil {
		t.Fatal(err)
	}
	runtime := newTestMeshRuntime(t, fixture.ctx, fixture.joinerIdentity,
		readMeshRuntimeAuthority(t, fixture.joinerStore))
	runtime.Host().Peerstore().AddAddrs(fixture.ownerHost.ID(), fixture.ownerHost.Addrs(),
		peerstore.PermanentAddrTTL)
	ownerProtocol, err := NewChannelEnrollmentOwner(ChannelEnrollmentOwnerOptions{
		Store: fixture.ownerStore,
		Signer: enrollmentTestSigner{
			privateKey: enrollmentPrivateKey(t, fixture.ownerIdentity),
		},
		Clock:  fixedEnrollmentClock{at: fixture.acceptedAt},
		Random: bytes.NewReader(bytes.Repeat([]byte{0x36}, model.EnrollmentNonceBytes*4)),
	})
	if err != nil {
		t.Fatal(err)
	}
	registerEnrollmentTestDispatcher(t, fixture.ctx, fixture.ownerHost, ownerProtocol)

	blocked := newPreparingSlotJoinStore(fixture.joinerStore)
	t.Cleanup(blocked.releasePrepare)
	client, err := NewChannelEnrollmentClient(ChannelEnrollmentClientOptions{
		Store: blocked, Clock: fixedEnrollmentClock{at: fixture.acceptedAt},
		Random: bytes.NewReader(bytes.Repeat([]byte{0x46},
			(model.EnrollmentNonceBytes+channelRequestIDBytes)*3)),
	})
	if err != nil {
		t.Fatal(err)
	}
	spec := JoinChannelSpec{Token: fixture.token,
		DisplayLabel:         fixture.joinerIdentity.DisplayName(),
		AdvertisedMultiaddrs: runtime.AdvertisedMultiaddrs(), LocalAlias: "preparing-slot"}
	return &preparingSlotEnrollmentExercise{fixture: fixture, runtime: runtime,
		blocked: blocked, client: client, spec: spec}
}

func (exercise *preparingSlotEnrollmentExercise) load(
	ctx context.Context,
) (store.ChannelMeshAuthority, error) {
	exercise.loadMu.Lock()
	exercise.loadCalls++
	exercise.loadMu.Unlock()
	return exercise.fixture.joinerStore.ReadChannelMeshAuthority(ctx)
}

func (exercise *preparingSlotEnrollmentExercise) loadCount() int {
	exercise.loadMu.Lock()
	defer exercise.loadMu.Unlock()
	return exercise.loadCalls
}

func (exercise *preparingSlotEnrollmentExercise) start() <-chan meshEnrollmentResult {
	first := make(chan meshEnrollmentResult, 1)
	go func() {
		result, enrollErr := exercise.runtime.EnrollChannel(
			exercise.fixture.ctx, exercise.client, exercise.spec, exercise.load)
		first <- meshEnrollmentResult{result: result, err: enrollErr}
	}()
	return first
}

func (exercise *preparingSlotEnrollmentExercise) assertConcurrentBusy(t *testing.T,
	first <-chan meshEnrollmentResult,
) {
	t.Helper()
	secondCtx, cancelSecond := context.WithTimeout(exercise.fixture.ctx, 500*time.Millisecond)
	second := make(chan error, 1)
	go func() {
		_, enrollErr := exercise.runtime.EnrollChannel(
			secondCtx, exercise.client, exercise.spec, exercise.load)
		second <- enrollErr
	}()
	select {
	case secondErr := <-second:
		if !errors.Is(secondErr, ErrMeshRuntime) ||
			!strings.Contains(secondErr.Error(), "busy") {
			t.Fatalf("concurrent enrollment error = %v, want busy", secondErr)
		}
	case <-time.After(250 * time.Millisecond):
		exercise.blocked.releasePrepare()
		<-first
		<-second
		t.Fatal("concurrent enrollment waited for the preparing attempt")
	}
	cancelSecond()
	prepares, releases := exercise.blocked.snapshot()
	if prepares != 1 || releases != 0 || exercise.loadCount() != 0 {
		t.Fatalf("busy enrollment touched first attempt: prepares %d releases %d loads %d",
			prepares, releases, exercise.loadCount())
	}
	if reservations := channelJoinReservationCount(
		t, exercise.fixture.joinerStore.Path()); reservations != 1 {
		t.Fatalf("reservation count after busy enrollment = %d, want one", reservations)
	}
}

func (exercise *preparingSlotEnrollmentExercise) assertFirstSettles(t *testing.T,
	first <-chan meshEnrollmentResult,
) {
	t.Helper()
	exercise.blocked.releasePrepare()
	enrolled := <-first
	if enrolled.err != nil || !enrolled.result.Installed {
		t.Fatalf("first enrollment after busy contender = (%#v,%v)", enrolled.result, enrolled.err)
	}
	prepares, releases := exercise.blocked.snapshot()
	if prepares != 1 || releases != 0 {
		t.Fatalf("successful first enrollment Store calls = prepares %d releases %d",
			prepares, releases)
	}
	if reservations := channelJoinReservationCount(
		t, exercise.fixture.joinerStore.Path()); reservations != 0 {
		t.Fatalf("settled durable reservations = %d, want zero", reservations)
	}
	exercise.runtime.mu.Lock()
	slot := exercise.runtime.enrollment
	exercise.runtime.mu.Unlock()
	if slot != nil ||
		!exercise.runtime.HasCurrentSession(exercise.fixture.ownerFixture.Channel().ID()) {
		t.Fatal("successful enrollment retained its slot or omitted the Channel session")
	}
}

func (exercise *preparingSlotEnrollmentExercise) assertPrepareFailureClearsSlot(t *testing.T) {
	t.Helper()
	cancelledCtx, cancel := context.WithCancel(exercise.fixture.ctx)
	cancel()
	if _, err := exercise.runtime.EnrollChannel(
		cancelledCtx, exercise.client, exercise.spec, exercise.load); err == nil {
		t.Fatal("cancelled enrollment unexpectedly succeeded")
	}
	exercise.runtime.mu.Lock()
	slot := exercise.runtime.enrollment
	exercise.runtime.mu.Unlock()
	prepares, _ := exercise.blocked.snapshot()
	if slot != nil || prepares != 1 {
		t.Fatalf("prepare failure retained slot or touched Store: slot %p prepares %d",
			slot, prepares)
	}
}

func TestMeshEnrollmentExchangeAndAuthorityLoadAllowRuntimeMuReentry(t *testing.T) {
	exercise := startMeshEnrollmentLockExercise(t)
	exercise.assertExchangeAllowsRuntimeMuReentry(t)
	exercise.assertAuthorityLoadAllowsRuntimeMuReentry(t)
}

type meshEnrollmentResult struct {
	result store.InstallJoinedChannelResult
	err    error
}

type meshEnrollmentLockExercise struct {
	fixture       enrollmentHandshakeFixture
	runtime       *MeshRuntime
	barrier       *committedEnrollmentOwnerStore
	releaseOwner  sync.Once
	loadStarted   chan struct{}
	releaseLoad   chan struct{}
	releaseLoader sync.Once
	loadCalls     int
	result        <-chan meshEnrollmentResult
}

func startMeshEnrollmentLockExercise(t *testing.T) *meshEnrollmentLockExercise {
	t.Helper()
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
	exercise := &meshEnrollmentLockExercise{fixture: fixture, runtime: runtime,
		barrier: barrier, loadStarted: loadStarted, releaseLoad: releaseLoad}
	t.Cleanup(func() {
		exercise.releaseOwner.Do(func() { close(barrier.resume) })
		exercise.releaseLoader.Do(func() { close(releaseLoad) })
	})
	load := func(ctx context.Context) (store.ChannelMeshAuthority, error) {
		mesh, loadErr := fixture.joinerStore.ReadChannelMeshAuthority(ctx)
		exercise.loadCalls++
		if exercise.loadCalls == 1 {
			close(loadStarted)
			<-releaseLoad
		}
		return mesh, loadErr
	}
	resultC := make(chan meshEnrollmentResult, 1)
	exercise.result = resultC
	go func() {
		result, enrollErr := runtime.EnrollChannel(fixture.ctx, client, spec, load)
		resultC <- meshEnrollmentResult{result: result, err: enrollErr}
	}()
	return exercise
}

func (exercise *meshEnrollmentLockExercise) assertExchangeAllowsRuntimeMuReentry(t *testing.T) {
	t.Helper()
	select {
	case <-exercise.barrier.committed:
	case <-exercise.fixture.ctx.Done():
		t.Fatal(exercise.fixture.ctx.Err())
	}
	if err := exercise.runtime.Reconcile(
		readMeshRuntimeAuthority(t, exercise.fixture.joinerStore)); err != nil {
		t.Fatalf("reconcile during real enrollment exchange: %v", err)
	}
	ownerID, err := canonicalLibp2pID(exercise.fixture.ownerIdentity.PeerID())
	if err != nil || !exercise.runtime.authority.CanDial(ownerID) {
		t.Fatalf("concurrent reconcile dropped enrollment permit: owner=%q err=%v", ownerID, err)
	}
	exercise.releaseOwner.Do(func() { close(exercise.barrier.resume) })
}

func (exercise *meshEnrollmentLockExercise) assertAuthorityLoadAllowsRuntimeMuReentry(t *testing.T) {
	t.Helper()
	select {
	case <-exercise.loadStarted:
	case <-exercise.fixture.ctx.Done():
		t.Fatal(exercise.fixture.ctx.Err())
	}
	if err := exercise.runtime.Reconcile(
		readMeshRuntimeAuthority(t, exercise.fixture.joinerStore)); err != nil {
		t.Fatalf("reconcile during durable authority load: %v", err)
	}
	exercise.releaseLoader.Do(func() { close(exercise.releaseLoad) })
	enrolled := <-exercise.result
	if enrolled.err != nil || !enrolled.result.Installed {
		t.Fatalf("real enrollment result = (%#v, %v)", enrolled.result, enrolled.err)
	}
	if exercise.loadCalls != 2 {
		t.Fatalf("authority loads = %d, want one bounded conflict retry", exercise.loadCalls)
	}
	ownerID, err := canonicalLibp2pID(exercise.fixture.ownerIdentity.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	_, enrollmentRetained := exercise.runtime.authority.state.Load().outboundEnrollment[ownerID]
	if enrollmentRetained ||
		!exercise.runtime.HasCurrentSession(exercise.fixture.ownerFixture.Channel().ID()) {
		t.Fatal("settled enrollment retained permit or omitted joined Channel session")
	}
}

type preparingSlotJoinStore struct {
	ChannelEnrollmentJoinerStore
	prepared chan struct{}
	resume   chan struct{}

	mu          sync.Mutex
	prepares    int
	releases    int
	prepareOnce sync.Once
	resumeOnce  sync.Once
}

func newPreparingSlotJoinStore(delegate ChannelEnrollmentJoinerStore) *preparingSlotJoinStore {
	return &preparingSlotJoinStore{ChannelEnrollmentJoinerStore: delegate,
		prepared: make(chan struct{}), resume: make(chan struct{})}
}

func (blocked *preparingSlotJoinStore) PrepareJoinedChannel(ctx context.Context,
	spec store.PrepareJoinedChannelSpec,
) (store.PrepareJoinedChannelResult, error) {
	result, err := blocked.ChannelEnrollmentJoinerStore.PrepareJoinedChannel(ctx, spec)
	blocked.mu.Lock()
	blocked.prepares++
	first := blocked.prepares == 1
	blocked.mu.Unlock()
	if first {
		blocked.prepareOnce.Do(func() { close(blocked.prepared) })
		<-blocked.resume
	}
	return result, err
}

func (blocked *preparingSlotJoinStore) ReleaseJoinedChannelReservation(ctx context.Context,
	requestID model.EnrollmentRequestID, peerID model.PeerID, attempt uint64,
) error {
	blocked.mu.Lock()
	blocked.releases++
	blocked.mu.Unlock()
	return blocked.ChannelEnrollmentJoinerStore.ReleaseJoinedChannelReservation(
		ctx, requestID, peerID, attempt)
}

func (blocked *preparingSlotJoinStore) waitPrepared(t *testing.T) {
	t.Helper()
	select {
	case <-blocked.prepared:
	case <-time.After(time.Second):
		t.Fatal("first enrollment did not reserve its durable attempt")
	}
}

func (blocked *preparingSlotJoinStore) releasePrepare() {
	blocked.resumeOnce.Do(func() { close(blocked.resume) })
}

func (blocked *preparingSlotJoinStore) snapshot() (int, int) {
	blocked.mu.Lock()
	defer blocked.mu.Unlock()
	return blocked.prepares, blocked.releases
}

func channelJoinReservationCount(t *testing.T, path string) int {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM channel_join_reservations`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
