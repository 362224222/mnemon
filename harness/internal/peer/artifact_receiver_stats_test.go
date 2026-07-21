package peer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

type artifactReceiverLeaseFixture struct {
	at      time.Time
	backend *artifactReceiverTestBackend
	content artifactReceiverTestContent
}

func TestArtifactReceiverNilUseLeaseFailsClosed(t *testing.T) {
	fixture := newArtifactReceiverLeaseFixture(t, "lifecycle-nil", []byte("nil lifecycle"))
	client := &artifactReceiverTestClient{manifest: func(context.Context, model.PeerID,
		GetManifest,
	) (Manifest, error) {
		t.Fatal("nil CAS use lease reached the Artifact client")
		return Manifest{}, ErrArtifactClientTransport
	}}
	cas := &artifactReceiverNilUseCAS{CAS: newArtifactReceiverTestCAS(t)}
	receiver := newArtifactReceiverForTest(t, fixture.backend, client, cas, 10*time.Second, 1,
		&artifactReceiverTestClock{at: fixture.at})
	err := <-runArtifactReceiverForTest(receiver, context.Background())
	if !errors.Is(err, ErrArtifactReceiverInvariant) ||
		receiver.Snapshot().FatalCode != ArtifactReceiverFatalCASInvariant {
		t.Fatalf("nil CAS use lease = %v, snapshot %#v", err, receiver.Snapshot())
	}
}

func TestArtifactReceiverCASUseLeaseCoversReadySettlement(t *testing.T) {
	fixture := newArtifactReceiverLeaseFixture(t, "lifecycle-ready", []byte("ready lifecycle"))
	releaseReady := make(chan struct{})
	backend := &artifactReceiverReadyGateBackend{artifactReceiverTestBackend: fixture.backend,
		entered: make(chan struct{}), release: releaseReady}
	cas := newArtifactReceiverTestCAS(t)
	receiver := newArtifactReceiverForTest(t, backend,
		artifactReceiverContentClient(t, fixture.content), cas, 10*time.Second, 1,
		&artifactReceiverTestClock{at: fixture.at})
	done := make(chan error, 1)
	go func() { done <- receiver.runCycle(context.Background()) }()
	waitArtifactReceiverSignal(t, backend.entered, "blocked ready settlement")
	exclusive := startArtifactReceiverExclusive(t, cas)
	assertArtifactReceiverExclusiveBlocked(t, exclusive)
	close(releaseReady)
	if err := waitArtifactReceiverResult(t, done); err != nil {
		t.Fatal(err)
	}
	releaseArtifactReceiverExclusive(t, exclusive)
	if receiver.Snapshot().Ready != 1 {
		t.Fatalf("ready lifecycle snapshot = %#v", receiver.Snapshot())
	}
}

func TestArtifactReceiverCASUseLeaseCoversFatalReceiveFailure(t *testing.T) {
	fixture := newArtifactReceiverLeaseFixture(t, "lifecycle-fatal", []byte("fatal lifecycle"))
	entered := make(chan struct{})
	releaseFailure := make(chan struct{})
	client := &artifactReceiverTestClient{manifest: func(context.Context, model.PeerID,
		GetManifest,
	) (Manifest, error) {
		close(entered)
		<-releaseFailure
		return Manifest{}, errors.New("injected fatal receiver client failure")
	}}
	cas := newArtifactReceiverTestCAS(t)
	receiver := newArtifactReceiverForTest(t, fixture.backend, client, cas, 10*time.Second, 1,
		&artifactReceiverTestClock{at: fixture.at})
	done := make(chan error, 1)
	go func() { done <- receiver.runCycle(context.Background()) }()
	waitArtifactReceiverSignal(t, entered, "blocked fatal receive")
	exclusive := startArtifactReceiverExclusive(t, cas)
	assertArtifactReceiverExclusiveBlocked(t, exclusive)
	close(releaseFailure)
	if err := waitArtifactReceiverResult(t, done); err == nil {
		t.Fatal("fatal receiver client failure unexpectedly succeeded")
	}
	releaseArtifactReceiverExclusive(t, exclusive)
}

func TestArtifactReceiverCASUseLeaseCoversCancellation(t *testing.T) {
	fixture := newArtifactReceiverLeaseFixture(t, "lifecycle-cancel", []byte("cancel lifecycle"))
	backend := &artifactReceiverReadyGateBackend{artifactReceiverTestBackend: fixture.backend,
		entered: make(chan struct{}), release: make(chan struct{})}
	cas := newArtifactReceiverTestCAS(t)
	receiver := newArtifactReceiverForTest(t, backend,
		artifactReceiverContentClient(t, fixture.content), cas, 10*time.Second, 1,
		&artifactReceiverTestClock{at: fixture.at})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- receiver.runCycle(ctx) }()
	waitArtifactReceiverSignal(t, backend.entered, "blocked cancelled ready settlement")
	exclusive := startArtifactReceiverExclusive(t, cas)
	assertArtifactReceiverExclusiveBlocked(t, exclusive)
	cancel()
	if err := waitArtifactReceiverResult(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled ready receiver = %v", err)
	}
	releaseArtifactReceiverExclusive(t, exclusive)
}

func newArtifactReceiverLeaseFixture(t testing.TB, name string,
	content []byte,
) artifactReceiverLeaseFixture {
	t.Helper()
	at := artifactReceiverTestTime()
	origin := testkit.NewIdentity(t, "artifact-receiver-"+name+"-origin")
	channelID := artifactReceiverChannelID(t, name)
	artifactContent := artifactReceiverContentForTest(t, name, content)
	backend := &artifactReceiverTestBackend{claims: []artifactReceiverClaim{
		artifactReceiverClaimForTest(t, name, origin, channelID,
			[]model.Digest{artifactContent.manifest.RootDigest()}),
	}}
	return artifactReceiverLeaseFixture{at: at, backend: backend, content: artifactContent}
}

type artifactReceiverReadyGateBackend struct {
	*artifactReceiverTestBackend
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (backend *artifactReceiverReadyGateBackend) ready(ctx context.Context,
	fence artifactReceiverFence, at time.Time,
) error {
	backend.once.Do(func() { close(backend.entered) })
	select {
	case <-backend.release:
		return backend.artifactReceiverTestBackend.ready(ctx, fence, at)
	case <-ctx.Done():
		return ctx.Err()
	}
}

type artifactReceiverExclusiveResult struct {
	lease *artifactdomain.CASLease
	err   error
}

func startArtifactReceiverExclusive(t testing.TB,
	cas *artifactdomain.CAS,
) <-chan artifactReceiverExclusiveResult {
	t.Helper()
	attempted := make(chan struct{})
	result := make(chan artifactReceiverExclusiveResult, 1)
	go func() {
		close(attempted)
		lease, err := cas.AcquireExclusive()
		result <- artifactReceiverExclusiveResult{lease: lease, err: err}
	}()
	waitArtifactReceiverSignal(t, attempted, "CAS exclusive attempt")
	return result
}

func assertArtifactReceiverExclusiveBlocked(t testing.TB,
	result <-chan artifactReceiverExclusiveResult,
) {
	t.Helper()
	select {
	case acquired := <-result:
		if acquired.lease != nil {
			acquired.lease.Release()
		}
		t.Fatalf("CAS exclusive entered during receiver use lease: %v", acquired.err)
	case <-time.After(30 * time.Millisecond):
	}
}

func releaseArtifactReceiverExclusive(t testing.TB,
	result <-chan artifactReceiverExclusiveResult,
) {
	t.Helper()
	select {
	case acquired := <-result:
		if acquired.err != nil || acquired.lease == nil {
			t.Fatalf("CAS exclusive after receiver release = (%#v, %v)", acquired.lease, acquired.err)
		}
		acquired.lease.Release()
	case <-time.After(3 * time.Second):
		t.Fatal("CAS exclusive remained blocked after receiver released use lease")
	}
}
