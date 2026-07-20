package peer

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestMeshRuntimeEnrollmentTransportInstallsAndReleasesExactAddressSource(t *testing.T) {
	runtime, remote, remoteHost, mesh := newMeshEnrollmentTransportFixture(t, "mesh-permit-source")
	spec := meshEnrollmentTransportRequest(t, remote, remoteHost, "source")
	permit, err := runtime.acquireEnrollmentTransportPermit(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	remoteID := meshRuntimeLibp2pID(t, remote.PeerID())
	if len(runtime.managedRuntimeHost().Peerstore().Addrs(remoteID)) == 0 ||
		runtime.nodeHost.gater.UnknownEnrollmentSlots() != 1 {
		t.Fatal("permit did not install one bounded address source and gater slot")
	}
	transition, err := runtime.BeginAuthorityTransition(mesh)
	if err != nil {
		t.Fatal(err)
	}
	if len(runtime.managedRuntimeHost().Peerstore().Addrs(remoteID)) == 0 {
		t.Fatal("whole authority staging erased the independent permit source")
	}
	if err := transition.Abort(); err != nil {
		t.Fatal(err)
	}
	if len(runtime.managedRuntimeHost().Peerstore().Addrs(remoteID)) == 0 {
		t.Fatal("whole authority abort erased the independent permit source")
	}
	if err := permit.Close(); err != nil || permit.Close() != nil {
		t.Fatalf("idempotent permit close = %v", err)
	}
	if runtime.nodeHost.gater.UnknownEnrollmentSlots() != 0 ||
		len(runtime.managedRuntimeHost().Peerstore().Addrs(remoteID)) != 0 {
		t.Fatal("permit close retained its slot or sole address source")
	}
}

func TestMeshRuntimeEnrollmentTransportUsesOnlyExactPrivateOpener(t *testing.T) {
	runtime, remote, remoteHost, _ := newMeshEnrollmentTransportFixture(t, "mesh-permit-open")
	remoteDone := make(chan error, 1)
	remoteHost.SetStreamHandler(ChannelProtocol, func(stream network.Stream) {
		_, _ = stream.Write([]byte{'m'})
		one := make([]byte, 1)
		_, err := stream.Read(one)
		remoteDone <- err
		_ = stream.Close()
	})
	spec := meshEnrollmentTransportRequest(t, remote, remoteHost, "open")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	permit, err := runtime.acquireEnrollmentTransportPermit(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	remoteID := meshRuntimeLibp2pID(t, remote.PeerID())
	if stream, err := runtime.managedRuntimeHost().NewStream(ctx, remoteID, ChannelProtocol); stream != nil ||
		!errors.Is(err, ErrNodeHost) {
		t.Fatalf("generic runtime Host stream = (%v, %v), want durable-authority rejection", stream, err)
	}
	stream, err := runtime.openEnrollmentStream(ctx, permit)
	if err != nil {
		t.Fatal(err)
	}
	one := make([]byte, 1)
	if count, err := stream.Read(one); err != nil || count != 1 || one[0] != 'm' {
		t.Fatalf("private exact enrollment response = (%q, %v)", one[:count], err)
	}
	if err := permit.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-remoteDone:
		if err == nil {
			t.Fatal("remote enrollment stream ended cleanly instead of being retired")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("permit release did not retire the claimed stream")
	}
	waitPeerDisconnected(t, runtime.managedRuntimeHost(), remoteID)
	if replay, err := runtime.openEnrollmentStream(ctx, permit); replay != nil || !errors.Is(err, ErrNodeHost) {
		t.Fatalf("private opener replay = (%v, %v), want single-use rejection", replay, err)
	}
}

func TestMeshRuntimeEnrollmentTransportExpiryRetiresLiveConnection(t *testing.T) {
	runtime, remote, remoteHost, _ := newMeshEnrollmentTransportFixture(t, "mesh-permit-expiry")
	gater := runtime.nodeHost.gater
	gater.pendingTTL = time.Minute
	start := time.Date(2026, 7, 20, 3, 0, 0, 0, time.UTC)
	var clock atomic.Value
	clock.Store(start)
	gater.now = func() time.Time { return clock.Load().(time.Time) }
	remoteDone := make(chan error, 1)
	remoteHost.SetStreamHandler(ChannelProtocol, func(stream network.Stream) {
		_, _ = stream.Write([]byte{'x'})
		one := make([]byte, 1)
		_, err := stream.Read(one)
		remoteDone <- err
		_ = stream.Close()
	})
	request := meshEnrollmentTransportRequest(t, remote, remoteHost, "expiry")
	permit, err := runtime.acquireEnrollmentTransportPermit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	defer permit.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := runtime.openEnrollmentStream(ctx, permit)
	if err != nil {
		t.Fatal(err)
	}
	one := make([]byte, 1)
	if count, err := stream.Read(one); err != nil || count != 1 || one[0] != 'x' {
		t.Fatalf("expiry enrollment response = (%q, %v)", one[:count], err)
	}
	clock.Store(start.Add(gater.pendingTTL + time.Nanosecond))
	gater.mu.Lock()
	gater.signalExpiryOwnerLocked()
	gater.mu.Unlock()
	select {
	case err := <-remoteDone:
		if err == nil {
			t.Fatal("expired remote enrollment stream ended cleanly")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("permit expiry did not retire the claimed stream")
	}
	remoteID := meshRuntimeLibp2pID(t, remote.PeerID())
	waitPeerDisconnected(t, runtime.managedRuntimeHost(), remoteID)
	if gater.UnknownEnrollmentSlots() != 0 ||
		len(runtime.managedRuntimeHost().Peerstore().Addrs(remoteID)) != 0 {
		t.Fatal("expired transport retained its budget or address source")
	}
}

func TestMeshRuntimeEnrollmentTransportRejectsUnverifiedCredential(t *testing.T) {
	runtime, _, _, _ := newMeshEnrollmentTransportFixture(t, "mesh-permit-invalid")
	requestID, _ := model.ParseEnrollmentRequestID("request-mesh-permit-invalid")
	permit, err := runtime.acquireEnrollmentTransportPermit(context.Background(),
		enrollmentTransportPermitRequest{EnrollmentRequestID: requestID})
	if permit != nil || !errors.Is(err, ErrMeshRuntime) ||
		runtime.nodeHost.gater.UnknownEnrollmentSlots() != 0 {
		t.Fatalf("unverified credential acquisition = (%v, %v)", permit, err)
	}
}

func TestMeshRuntimeEnrollmentTransportCapabilityDoesNotSurviveRestart(t *testing.T) {
	local := testkit.NewIdentity(t, "mesh-permit-restart-local")
	st := openPeerMeshStore(t, local, peerMeshTime(t, "2026-07-20T01:30:00Z"))
	channel := testkit.NewSignedChannelForOwnerAt(t, "mesh-permit-restart-channel", local,
		peerMeshTime(t, "2026-07-20T01:30:00Z"))
	createPeerMeshChannel(t, st, channel, "mesh-permit-restart")
	mesh := readMeshRuntimeAuthority(t, st)
	runtime := newTestMeshRuntime(t, context.Background(), local, mesh)
	remote := testkit.NewIdentity(t, "mesh-permit-restart-remote")
	remoteHost := newEnrollmentTestHost(t, remote)
	defer remoteHost.Close()
	permit, err := runtime.acquireEnrollmentTransportPermit(context.Background(),
		meshEnrollmentTransportRequest(t, remote, remoteHost, "restart"))
	if err != nil {
		t.Fatal(err)
	}
	remoteID := meshRuntimeLibp2pID(t, remote.PeerID())
	if runtime.nodeHost.gater.UnknownEnrollmentSlots() != 1 ||
		len(runtime.managedRuntimeHost().Peerstore().Addrs(remoteID)) == 0 {
		t.Fatal("restart fixture did not hold an active transport capability")
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	restarted := newTestMeshRuntime(t, context.Background(), local, readMeshRuntimeAuthority(t, st))
	if restarted.nodeHost.gater.UnknownEnrollmentSlots() != 0 ||
		len(restarted.managedRuntimeHost().Peerstore().Addrs(remoteID)) != 0 ||
		restarted.nodeHost.gater.InterceptPeerDial(remoteID) {
		t.Fatal("restart reconstructed an in-memory enrollment capability or address source")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if stream, openErr := restarted.openEnrollmentStream(ctx, permit); stream != nil ||
		!errors.Is(openErr, ErrMeshRuntime) {
		t.Fatalf("restarted runtime accepted old permit = (%v, %v)", stream, openErr)
	}
	if err := permit.Close(); err != nil || runtime.nodeHost.gater.UnknownEnrollmentSlots() != 0 {
		t.Fatalf("old permit close after restart = %v, slots %d",
			err, runtime.nodeHost.gater.UnknownEnrollmentSlots())
	}
}

func TestMeshRuntimeTwoPermitsResetOnlyTheirExactStreams(t *testing.T) {
	runtime, remote, remoteHost, _ := newMeshEnrollmentTransportFixture(t, "mesh-two-permits")
	remoteStreams := make(chan network.Stream, 2)
	remoteHost.SetStreamHandler(ChannelProtocol, func(stream network.Stream) {
		_, _ = stream.Read(make([]byte, 1))
		remoteStreams <- stream
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	permitA, err := runtime.acquireEnrollmentTransportPermit(ctx,
		meshEnrollmentTransportRequest(t, remote, remoteHost, "two-a"))
	if err != nil {
		t.Fatal(err)
	}
	permitB, err := runtime.acquireEnrollmentTransportPermit(ctx,
		meshEnrollmentTransportRequest(t, remote, remoteHost, "two-b"))
	if err != nil {
		t.Fatal(err)
	}
	streamA, err := runtime.openEnrollmentStream(ctx, permitA)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := streamA.Write([]byte{'a'}); err != nil {
		t.Fatal(err)
	}
	remoteA := <-remoteStreams
	streamB, err := runtime.openEnrollmentStream(ctx, permitB)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := streamB.Write([]byte{'b'}); err != nil {
		t.Fatal(err)
	}
	remoteB := <-remoteStreams
	if streamA.Conn().ID() != streamB.Conn().ID() {
		t.Fatal("same owner/address permits did not reuse the authenticated connection fixture")
	}
	if err := permitA.Close(); err != nil {
		t.Fatal(err)
	}
	_ = remoteA.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := remoteA.Read(make([]byte, 1)); err == nil {
		t.Fatal("permit A release did not reset stream A")
	}
	if _, err := streamB.Write([]byte{'c'}); err != nil {
		t.Fatalf("permit A release reset stream B: %v", err)
	}
	_ = remoteB.SetReadDeadline(time.Now().Add(2 * time.Second))
	one := make([]byte, 1)
	if count, err := remoteB.Read(one); err != nil || count != 1 || one[0] != 'c' {
		t.Fatalf("stream B after permit A release = (%q, %v)", one[:count], err)
	}
	remoteID := meshRuntimeLibp2pID(t, remote.PeerID())
	if runtime.nodeHost.gater.UnknownEnrollmentSlots() != 1 ||
		len(runtime.managedRuntimeHost().Network().ConnsToPeer(remoteID)) == 0 ||
		len(runtime.managedRuntimeHost().Peerstore().Addrs(remoteID)) == 0 {
		t.Fatal("permit A release retired permit B connection, slot, or address source")
	}
	if err := permitB.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMeshRuntimeEnrollmentOpenReleaseRaceNeverReturnsLiveStream(t *testing.T) {
	runtime, remote, remoteHost, _ := newMeshEnrollmentTransportFixture(t, "mesh-permit-open-release")
	remoteHost.SetStreamHandler(ChannelProtocol, func(stream network.Stream) {
		_ = stream.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, _ = stream.Read(make([]byte, 1))
		_ = stream.Close()
	})
	type openResult struct {
		stream network.Stream
		err    error
	}
	for attempt := 0; attempt < 24; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		permit, err := runtime.acquireEnrollmentTransportPermit(ctx,
			meshEnrollmentTransportRequest(t, remote, remoteHost, fmt.Sprintf("race-%02d", attempt)))
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		start := make(chan struct{})
		opened := make(chan openResult, 1)
		released := make(chan struct{})
		go func() {
			<-start
			stream, openErr := runtime.openEnrollmentStream(ctx, permit)
			opened <- openResult{stream: stream, err: openErr}
		}()
		go func() {
			<-start
			_ = permit.Close()
			close(released)
		}()
		close(start)
		result := <-opened
		<-released
		if result.stream != nil {
			if _, writeErr := result.stream.Write([]byte{'x'}); writeErr == nil {
				t.Fatalf("attempt %d returned a live stream after release won/joined", attempt)
			}
			_ = result.stream.Close()
		} else if result.err == nil {
			t.Fatalf("attempt %d returned neither stream nor error", attempt)
		}
		cancel()
		if slots := runtime.nodeHost.gater.UnknownEnrollmentSlots(); slots != 0 {
			t.Fatalf("attempt %d retained %d enrollment slots", attempt, slots)
		}
	}
}

func newMeshEnrollmentTransportFixture(t *testing.T, seed string) (*MeshRuntime,
	testkit.Identity, host.Host, store.ChannelMeshAuthority,
) {
	t.Helper()
	owner := testkit.NewIdentity(t, seed+"-local")
	st := openPeerMeshStore(t, owner, peerMeshTime(t, "2026-07-20T01:00:00Z"))
	channel := testkit.NewSignedChannelForOwnerAt(t, seed+"-channel", owner,
		peerMeshTime(t, "2026-07-20T01:00:00Z"))
	createPeerMeshChannel(t, st, channel, seed)
	mesh := readMeshRuntimeAuthority(t, st)
	runtime := newTestMeshRuntime(t, context.Background(), owner, mesh)
	remote := testkit.NewIdentity(t, seed+"-remote")
	remoteHost := newEnrollmentTestHost(t, remote)
	t.Cleanup(func() {
		if err := remoteHost.Close(); err != nil {
			t.Errorf("close remote enrollment Host: %v", err)
		}
	})
	return runtime, remote, remoteHost, mesh
}

func meshEnrollmentTransportRequest(t *testing.T, owner testkit.Identity,
	ownerHost host.Host, suffix string,
) enrollmentTransportPermitRequest {
	t.Helper()
	addresses := ownerHost.Addrs()
	sort.Slice(addresses, func(left, right int) bool {
		return addresses[left].String() < addresses[right].String()
	})
	raw := make([]string, len(addresses))
	for index, address := range addresses {
		raw[index] = address.String()
	}
	return meshEnrollmentTransportRequestForAddresses(t, owner, raw, suffix)
}

func meshEnrollmentTransportRequestForAddresses(t *testing.T, owner testkit.Identity,
	raw []string, suffix string,
) enrollmentTransportPermitRequest {
	t.Helper()
	raw = append([]string(nil), raw...)
	sort.Strings(raw)
	grantID, _ := model.ParseGrantID("grant-mesh-permit-" + suffix)
	requestID, _ := model.ParseEnrollmentRequestID("request-mesh-permit-" + suffix)
	channel := testkit.NewSignedChannelForOwnerAt(t, "mesh-permit-token-"+suffix, owner,
		peerMeshTime(t, "2026-07-20T01:00:00Z"))
	secret := model.Sum([]byte("mesh-enrollment-transport-secret:" + suffix)).Bytes()
	payload, err := model.NewEnrollmentTokenPayload(model.EnrollmentTokenSpec{
		Descriptor: channel.Descriptor(), OwnerMultiaddrs: raw, GrantID: grantID,
		BearerSecret: secret, ExpiresAt: channel.Channel().CreatedAt().Add(time.Hour),
		MaxUses:            channel.Channel().MemberLimit() - 1,
		ProtocolMinVersion: model.EnrollmentProtocolMinVersion,
		ProtocolMaxVersion: model.EnrollmentProtocolMaxVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	message, err := model.EnrollmentTokenSigningMessage(channel.Channel().ID(), payload.Digest())
	if err != nil {
		t.Fatal(err)
	}
	token, err := model.AttachEnrollmentTokenSignature(payload,
		ed25519.Sign(enrollmentPrivateKey(t, owner), message))
	if err != nil {
		t.Fatal(err)
	}
	return enrollmentTransportPermitRequest{Token: token, EnrollmentRequestID: requestID}
}
