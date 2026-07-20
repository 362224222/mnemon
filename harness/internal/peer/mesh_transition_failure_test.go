package peer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
	ma "github.com/multiformats/go-multiaddr"
)

type meshTransitionBeginResult struct {
	transition *MeshAuthorityTransition
	err        error
}

func TestMeshAuthorityTransitionFailsClosedWithoutInstallingCandidate(t *testing.T) {
	t.Parallel()
	fixture := newMeshTransitionFailureFixture(t, "mesh-transition-fail",
		"2026-07-18T08:00:00Z")

	remote := fixture.channel.AppendActive(t, "mesh-transition-fail-remote")
	mergePeerMeshRoster(t, fixture.store, fixture.channel, remote.Member(), remote.Member().CreatedAt())
	transition, err := fixture.runtime.BeginAuthorityTransition(readMeshRuntimeAuthority(t, fixture.store))
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("durable authority diverged")
	terminal := fixture.runtime.terminalSignal()
	if err := transition.FailClosed(cause); !errors.Is(err, cause) {
		t.Fatalf("fail closed error = %v", err)
	}
	select {
	case <-terminal:
	default:
		t.Fatal("FailClosed did not publish authority termination")
	}
	if err := fixture.runtime.terminalError(); !errors.Is(err, ErrMeshRuntime) ||
		!errors.Is(err, cause) {
		t.Fatalf("FailClosed terminal error = %v", err)
	}
	if err := transition.FailClosed(cause); !errors.Is(err, cause) {
		t.Fatalf("fail closed replay error = %v", err)
	}
	if transition.Wait() == nil || fixture.runtime.HasCurrentSession(fixture.channel.Channel().ID()) {
		t.Fatal("failed transition retained a live Channel session")
	}
	if _, err := fixture.runtime.Session(fixture.channel.Channel().ID()); !errors.Is(err, ErrMeshRuntime) {
		t.Fatalf("session after fail closed error = %v", err)
	}
	if transition.snapshot.Channels[0].RosterHead != remote.Member().Head() {
		t.Fatal("test candidate did not contain the unproved roster generation")
	}
	if fixture.runtime.authority.CanConnect(meshRuntimeLibp2pID(t, remote.Identity().PeerID())) {
		t.Fatal("fail closed installed the unproved candidate authority")
	}
	if phase := transition.phase.Load(); phase != meshTransitionFailed {
		t.Fatalf("transition phase = %d, want failed", phase)
	}
	if fixture.runtime.authority.LocalPeerID().String() != fixture.owner.PeerID().String() {
		t.Fatal("fail closed corrupted immutable local identity")
	}
}

func TestMeshAuthorityTransitionInstallFailureClosesWholeHost(t *testing.T) {
	t.Parallel()
	fixture := newMeshTransitionFailureFixture(t, "mesh-install-failure",
		"2026-07-18T09:00:00Z")
	remote := fixture.channel.AppendActive(t, "mesh-install-failure-remote")
	mergePeerMeshRoster(t, fixture.store, fixture.channel, remote.Member(), remote.Member().CreatedAt())
	transition, err := fixture.runtime.BeginAuthorityTransition(readMeshRuntimeAuthority(t, fixture.store))
	if err != nil {
		t.Fatal(err)
	}
	// Inject loss of the Gossip transition token after the durable caller would
	// have committed. MeshRuntime must close the Host, not merely the topic
	// router, because no further control stream may survive an uninstalled
	// durable authority generation.
	fixture.runtime.gossip.mu.Lock()
	fixture.runtime.gossip.transition = nil
	fixture.runtime.gossip.mu.Unlock()
	terminal := fixture.runtime.terminalSignal()
	if err := transition.Install(); !errors.Is(err, ErrMeshRuntime) ||
		!errors.Is(err, ErrGossipTopic) {
		t.Fatalf("injected Mesh install failure = %v", err)
	}
	select {
	case <-terminal:
	default:
		t.Fatal("failed Install did not publish authority termination")
	}
	if err := fixture.runtime.terminalError(); !errors.Is(err, ErrMeshRuntime) ||
		!errors.Is(err, ErrGossipTopic) {
		t.Fatalf("failed Install terminal error = %v", err)
	}
	if transition.phase.Load() != meshTransitionFailed || !fixture.runtime.closed ||
		!fixture.runtime.nodeHost.gater.closed.Load() {
		t.Fatalf("failed install retained Host authority: phase=%d runtime_closed=%v gater_closed=%v",
			transition.phase.Load(), fixture.runtime.closed, fixture.runtime.nodeHost.gater.closed.Load())
	}
	if _, err := fixture.runtime.Session(fixture.channel.Channel().ID()); !errors.Is(err, ErrMeshRuntime) {
		t.Fatalf("Session after failed install error = %v", err)
	}
}

func TestMeshAuthorityTransitionAbortFailureSignalsInnerCause(t *testing.T) {
	t.Parallel()
	fixture := newMeshTransitionFailureFixture(t, "mesh-abort-failure",
		"2026-07-20T10:30:00Z")
	remote := fixture.channel.AppendActive(t, "mesh-abort-failure-remote")
	mergePeerMeshRoster(t, fixture.store, fixture.channel, remote.Member(), remote.Member().CreatedAt())
	transition, err := fixture.runtime.BeginAuthorityTransition(
		readMeshRuntimeAuthority(t, fixture.store))
	if err != nil {
		t.Fatal(err)
	}
	// Consume the inner token as installed while the outer durable owner still
	// believes it can abort. The outer Abort must treat that contradiction as
	// terminal instead of reopening either authority generation.
	if err := transition.gossipTransition.Install(); err != nil {
		t.Fatal(err)
	}
	terminal := fixture.runtime.terminalSignal()
	if err := transition.Abort(); !errors.Is(err, ErrMeshRuntime) ||
		!errors.Is(err, ErrGossipTransitionFinalized) {
		t.Fatalf("injected Mesh abort failure = %v", err)
	}
	select {
	case <-terminal:
	default:
		t.Fatal("failed Abort did not publish authority termination")
	}
	if err := fixture.runtime.terminalError(); !errors.Is(err, ErrMeshRuntime) ||
		!errors.Is(err, ErrGossipTransitionFinalized) {
		t.Fatalf("failed Abort terminal error = %v", err)
	}
	if transition.phase.Load() != meshTransitionFailed || !fixture.runtime.closed ||
		!fixture.runtime.nodeHost.gater.closed.Load() {
		t.Fatalf("failed Abort retained Host authority: phase=%d runtime_closed=%v gater_closed=%v",
			transition.phase.Load(), fixture.runtime.closed,
			fixture.runtime.nodeHost.gater.closed.Load())
	}
}

func TestMeshRuntimeNormalTerminalCauseSurvivesLateFatal(t *testing.T) {
	fixture := newMeshTransitionFailureFixture(t, "mesh-terminal-normal-first",
		"2026-07-20T10:45:00Z")
	signal := fixture.runtime.terminalSignal()
	if err := fixture.runtime.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-signal:
	default:
		t.Fatal("normal Close did not publish authority termination")
	}
	terminal := fixture.runtime.terminalError()
	if terminal != errMeshRuntimeAuthorityTerminated {
		t.Fatalf("normal terminal cause = %v, want stable generic sentinel", terminal)
	}

	lateFatal := errors.New("late enrollment transport failure")
	fixture.runtime.failClosedEnrollmentTransport(lateFatal)
	if current := fixture.runtime.terminalError(); current != terminal ||
		errors.Is(current, lateFatal) {
		t.Fatalf("late fatal changed frozen terminal cause: before=%v after=%v",
			terminal, current)
	}
	if err := fixture.runtime.Close(); !errors.Is(err, lateFatal) {
		t.Fatalf("Close aggregate after late fatal = %v", err)
	}
	if signal != fixture.runtime.terminalSignal() {
		t.Fatal("late fatal replaced the one terminal signal")
	}
}

func TestMeshRuntimeFatalTerminalCauseSurvivesNormalClose(t *testing.T) {
	fixture := newMeshTransitionFailureFixture(t, "mesh-terminal-fatal-first",
		"2026-07-20T10:50:00Z")
	signal := fixture.runtime.terminalSignal()
	fatal := errors.New("first enrollment transport failure")
	fixture.runtime.failClosedEnrollmentTransport(fatal)
	select {
	case <-signal:
	default:
		t.Fatal("fatal termination did not publish authority termination")
	}
	terminal := fixture.runtime.terminalError()
	if !errors.Is(terminal, ErrMeshRuntime) || !errors.Is(terminal, fatal) {
		t.Fatalf("fatal terminal cause = %v", terminal)
	}
	if err := fixture.runtime.Close(); !errors.Is(err, fatal) {
		t.Fatalf("normal Close aggregate after fatal = %v", err)
	}
	if current := fixture.runtime.terminalError(); current != terminal {
		t.Fatalf("normal Close changed frozen terminal cause: before=%v after=%v",
			terminal, current)
	}
}

func TestEnrollmentTransportFailClosedFinalizesPreparedTransitionAndConcurrentClose(t *testing.T) {
	fixture := newMeshTransitionFailureFixture(t, "mesh-permit-prepared-failure",
		"2026-07-20T07:00:00Z")
	remote := testkit.NewIdentity(t, "mesh-permit-prepared-failure-remote")
	remoteHost := newEnrollmentTestHost(t, remote)
	defer remoteHost.Close()
	permit, err := fixture.runtime.acquireEnrollmentTransportPermit(context.Background(),
		meshEnrollmentTransportRequest(t, remote, remoteHost, "prepared-failure"))
	if err != nil {
		t.Fatal(err)
	}
	transition, err := fixture.runtime.BeginAuthorityTransition(
		readMeshRuntimeAuthority(t, fixture.store))
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("injected enrollment transport reconciliation failure")
	failDone := make(chan struct{})
	closeDone := make(chan error, 1)
	go func() {
		fixture.runtime.failClosedEnrollmentTransport(cause)
		close(failDone)
	}()
	go func() { closeDone <- fixture.runtime.Close() }()
	select {
	case <-failDone:
	case <-time.After(3 * time.Second):
		t.Fatal("prepared-transition transport failure did not return")
	}
	select {
	case closeErr := <-closeDone:
		if !errors.Is(closeErr, cause) {
			t.Fatalf("concurrent Close error = %v, want terminal transport cause", closeErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("concurrent Close waited forever on prepared transition")
	}
	if err := transition.Wait(); !errors.Is(err, cause) ||
		transition.phase.Load() != meshTransitionFailed ||
		fixture.runtime.nodeHost.gater.UnknownEnrollmentSlots() != 0 ||
		fixture.runtime.nodeHost.gater.outboundEnrollmentPermitCurrent(permit.token) {
		t.Fatalf("prepared fail-closed result=%v phase=%d slots=%d current=%v", err,
			transition.phase.Load(), fixture.runtime.nodeHost.gater.UnknownEnrollmentSlots(),
			fixture.runtime.nodeHost.gater.outboundEnrollmentPermitCurrent(permit.token))
	}
}

func TestEnrollmentTransportFailClosedCancelsPreparingTransitionAndConcurrentClose(t *testing.T) {
	fixture := newMeshTransitionFailureFixture(t, "mesh-permit-preparing-failure",
		"2026-07-20T08:00:00Z")
	remote := fixture.channel.AppendActive(t, "mesh-permit-preparing-failure-remote")
	mergePeerMeshRoster(t, fixture.store, fixture.channel, remote.Member(), remote.Member().CreatedAt())
	fixture.runtime.gossip.mu.Lock()
	gate := fixture.runtime.gossip.gates[fixture.channel.Channel().ID()]
	fixture.runtime.gossip.mu.Unlock()
	if gate == nil || !gate.tryAcquire() {
		t.Fatal("hold one Channel admission to block transition preparation")
	}
	candidate := readMeshRuntimeAuthority(t, fixture.store)
	beginDone := make(chan meshTransitionBeginResult, 1)
	go func() {
		transition, beginErr := fixture.runtime.BeginAuthorityTransition(candidate)
		beginDone <- meshTransitionBeginResult{transition: transition, err: beginErr}
	}()
	if !waitForMeshTransitionPreparation(fixture.runtime, 3*time.Second) {
		gate.release()
		t.Fatal("transition did not enter the preparing/draining window")
	}
	cause := errors.New("injected failure during transition preparation")
	failDone := make(chan struct{})
	closeDone := make(chan error, 1)
	go func() {
		fixture.runtime.failClosedEnrollmentTransport(cause)
		close(failDone)
	}()
	go func() { closeDone <- fixture.runtime.Close() }()
	if !waitForClosed(failDone, 3*time.Second) {
		gate.release()
		t.Fatal("preparing-transition transport failure did not return")
	}
	gate.release()
	result, began := waitForMeshTransitionBegin(beginDone, 3*time.Second)
	if !began {
		t.Fatal("preparing Begin did not observe fail-closed cancellation")
	}
	if result.transition != nil || result.err == nil {
		t.Fatalf("preparing Begin result = (%v, %v), want terminal error", result.transition, result.err)
	}
	closeErr, closed := waitForRuntimeClose(closeDone, 3*time.Second)
	if !closed {
		t.Fatal("concurrent Close waited forever on preparing transition")
	}
	if !errors.Is(closeErr, cause) {
		t.Fatalf("preparing concurrent Close error = %v", closeErr)
	}
	fixture.runtime.mu.Lock()
	active := fixture.runtime.transition
	fixture.runtime.mu.Unlock()
	if active != nil || fixture.runtime.nodeHost.gater.UnknownEnrollmentSlots() != 0 {
		t.Fatal("preparing fail-closed retained transition or enrollment slots")
	}
}

func waitForMeshTransitionPreparation(runtime *MeshRuntime, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		runtime.mu.Lock()
		meshTransition := runtime.transition
		meshPreparing := meshTransition != nil && meshTransition.gossipTransition == nil
		runtime.mu.Unlock()
		runtime.gossip.mu.Lock()
		gossipPreparing := runtime.gossip.transition != nil
		runtime.gossip.mu.Unlock()
		if meshPreparing && gossipPreparing {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

func waitForClosed(done <-chan struct{}, timeout time.Duration) bool {
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

func waitForMeshTransitionBegin(done <-chan meshTransitionBeginResult,
	timeout time.Duration,
) (meshTransitionBeginResult, bool) {
	select {
	case result := <-done:
		return result, true
	case <-time.After(timeout):
		return meshTransitionBeginResult{}, false
	}
}

func waitForRuntimeClose(done <-chan error, timeout time.Duration) (error, bool) {
	select {
	case err := <-done:
		return err, true
	case <-time.After(timeout):
		return nil, false
	}
}

func TestEnrollmentTransportExpiryOwnerFailClosedDoesNotSelfJoin(t *testing.T) {
	fixture := newMeshTransitionFailureFixture(t, "mesh-permit-expiry-owner-failure",
		"2026-07-20T09:00:00Z")
	remote := testkit.NewIdentity(t, "mesh-permit-expiry-owner-failure-remote")
	remoteHost := newEnrollmentTestHost(t, remote)
	defer remoteHost.Close()
	gater := fixture.runtime.nodeHost.gater
	start := time.Date(2026, 7, 20, 9, 30, 0, 0, time.UTC)
	gater.pendingTTL = time.Minute
	var clock atomic.Value
	clock.Store(start)
	gater.now = func() time.Time { return clock.Load().(time.Time) }
	permit, err := fixture.runtime.acquireEnrollmentTransportPermit(context.Background(),
		meshEnrollmentTransportRequest(t, remote, remoteHost, "expiry-owner-failure"))
	if err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("injected exact stream reset failure")
	gater.mu.Lock()
	gater.outbound.permits[permit.token.key].resetStream = func() error { return sentinel }
	gater.mu.Unlock()
	clock.Store(start.Add(gater.pendingTTL + time.Nanosecond))
	gater.mu.Lock()
	gater.signalExpiryOwnerLocked()
	gater.mu.Unlock()
	select {
	case <-fixture.runtime.terminalSignal():
	case <-time.After(3 * time.Second):
		t.Fatal("expiry owner did not publish transport fail-closed")
	}
	if err := fixture.runtime.terminalError(); !errors.Is(err, ErrMeshRuntime) ||
		!errors.Is(err, sentinel) {
		t.Fatalf("expiry transport terminal error = %v", err)
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- fixture.runtime.Close() }()
	select {
	case closeErr := <-closeDone:
		if !errors.Is(closeErr, sentinel) {
			t.Fatalf("Close error = %v, want exact reset sentinel", closeErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close self-joined the expiry owner after transport failure")
	}
	gater.mu.Lock()
	expiryRunning := gater.expiryRunning
	gater.mu.Unlock()
	if !gater.closed.Load() || expiryRunning || gater.UnknownEnrollmentSlots() != 0 {
		t.Fatalf("expiry fail-closed retained lifecycle state: closed=%v running=%v slots=%d",
			gater.closed.Load(), expiryRunning, gater.UnknownEnrollmentSlots())
	}
}

type meshTransitionFailureFixture struct {
	owner   testkit.Identity
	store   *store.Store
	channel *testkit.SignedChannel
	runtime *MeshRuntime
}

func newMeshTransitionFailureFixture(t *testing.T, seed, at string) meshTransitionFailureFixture {
	t.Helper()
	owner := testkit.NewIdentity(t, seed+"-owner")
	st := openPeerMeshStore(t, owner, peerMeshTime(t, at))
	channel := testkit.NewSignedChannelForOwnerAt(t, seed, owner, peerMeshTime(t, at))
	createPeerMeshChannel(t, st, channel, seed)
	key, err := owner.Libp2pPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	listen, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/0")
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewMeshRuntime(context.Background(), key, []ma.Multiaddr{listen},
		readMeshRuntimeAuthority(t, st))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	return meshTransitionFailureFixture{owner: owner, store: st, channel: channel, runtime: runtime}
}
