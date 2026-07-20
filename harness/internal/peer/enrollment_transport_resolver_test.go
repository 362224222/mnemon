package peer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	ma "github.com/multiformats/go-multiaddr"
	madns "github.com/multiformats/go-multiaddr-dns"
)

func TestEnrollmentTransportResolverSupportsDNS4DNS6AndDNSAddr(t *testing.T) {
	owner := testAuthorityPeer(t, "resolver-protocol-owner")
	records := newMutableEnrollmentDNS()
	records.setIPs("v4.test", "127.0.0.1")
	records.setIPs("v6.test", "::1")
	records.setTXT("_dnsaddr.addr.test", "dnsaddr=/ip4/127.0.0.3/tcp/45003/p2p/"+
		owner.libp2pID.String())
	resolver := newEnrollmentDNSResolver(t, records)
	signed := []ma.Multiaddr{
		ma.StringCast("/dns4/v4.test/tcp/45001"),
		ma.StringCast("/dns6/v6.test/tcp/45002"),
		ma.StringCast("/dnsaddr/addr.test/p2p/" + owner.libp2pID.String()),
	}
	resolved, err := resolveEnrollmentTransportAddresses(context.Background(), owner.libp2pID,
		signed, resolver)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"/ip4/127.0.0.1/tcp/45001", "/ip4/127.0.0.3/tcp/45003",
		"/ip6/::1/tcp/45002"}
	if got := multiaddrStrings(resolved); !equalStrings(got, want) {
		t.Fatalf("resolved DNS transports = %v, want %v", got, want)
	}
}

func TestEnrollmentTransportResolverRejectsRecursiveAndWorkOverflow(t *testing.T) {
	owner := testAuthorityPeer(t, "resolver-bounds-owner")
	signed := []ma.Multiaddr{ma.StringCast("/dns4/bounds.test/tcp/45011")}
	cases := []struct {
		name     string
		resolver enrollmentMultiaddrResolver
	}{
		{name: "recursion", resolver: enrollmentResolverFunc(func(_ context.Context,
			address ma.Multiaddr,
		) ([]ma.Multiaddr, error) {
			return []ma.Multiaddr{address}, nil
		})},
		{name: "work", resolver: enrollmentResolverFunc(func(_ context.Context,
			address ma.Multiaddr,
		) ([]ma.Multiaddr, error) {
			return []ma.Multiaddr{address, address, address, address,
				address, address, address, address}, nil
		})},
		{name: "non-transport", resolver: enrollmentResolverFunc(func(context.Context,
			ma.Multiaddr,
		) ([]ma.Multiaddr, error) {
			return []ma.Multiaddr{ma.StringCast("/unix/tmp/mnemon-resolver.sock")}, nil
		})},
	}
	for _, testCase := range cases {
		if _, err := resolveEnrollmentTransportAddresses(context.Background(), owner.libp2pID,
			signed, testCase.resolver); !errors.Is(err, ErrEnrollmentTransportPermit) {
			t.Fatalf("%s bound error = %v", testCase.name, err)
		}
	}
}

func TestConnectionGaterDNSPermitFreezesOneResolutionSnapshot(t *testing.T) {
	local := testAuthorityPeer(t, "resolver-drift-local")
	owner := testAuthorityPeer(t, "resolver-drift-owner")
	authority, _ := NewAuthority(local.modelID)
	gater := newTestConnectionGater(t, authority)
	records := newMutableEnrollmentDNS()
	records.setIPs("drift.test", "127.0.0.1")
	gater.dnsResolver = newEnrollmentDNSResolver(t, records)
	spec := testEnrollmentTransportPermitSpec(t, owner,
		[]string{"/dns4/drift.test/tcp/45101"}, "dns-drift")
	old := acquireClaimedEnrollmentPermit(t, gater, spec)
	assertEnrollmentPermitAddresses(t, gater, old, owner.libp2pID,
		"/ip4/127.0.0.1/tcp/45101", "/dns4/drift.test/tcp/45101", "/ip4/127.0.0.2/tcp/45101")
	records.setIPs("drift.test", "127.0.0.2")
	assertEnrollmentPermitAddresses(t, gater, old, owner.libp2pID,
		"/ip4/127.0.0.1/tcp/45101", "/dns4/drift.test/tcp/45101", "/ip4/127.0.0.2/tcp/45101")
	if !gater.releaseOutboundEnrollmentPermit(old) {
		t.Fatal("release old DNS snapshot permit")
	}
	current := acquireClaimedEnrollmentPermit(t, gater, spec)
	defer gater.releaseOutboundEnrollmentPermit(current)
	assertEnrollmentPermitAddresses(t, gater, current, owner.libp2pID,
		"/ip4/127.0.0.2/tcp/45101", "/dns4/drift.test/tcp/45101", "/ip4/127.0.0.1/tcp/45101")
	if calls := records.ipCallCount("drift.test"); calls != 2 {
		t.Fatalf("DNS lookups = %d, want exactly one per acquisition", calls)
	}
}

func TestConnectionGaterResolvingReservationsShareEightSlotBudget(t *testing.T) {
	local := testAuthorityPeer(t, "resolver-budget-local")
	owner := testAuthorityPeer(t, "resolver-budget-owner")
	unknown := testAuthorityPeer(t, "resolver-budget-inbound")
	authority, _ := NewAuthority(local.modelID)
	gater := newTestConnectionGater(t, authority)
	resolver := newBlockingEnrollmentResolver()
	gater.dnsResolver = resolver
	ctx, cancel := context.WithCancel(context.Background())
	results := startBlockedEnrollmentAcquisitions(t, ctx, gater, owner, 32)
	limit := HermeticLimits().UnknownEnrollmentConnections
	waitForResolverEntries(t, resolver.entered, limit)
	// Drain every acquisition that did not reserve a DNS slot before probing
	// the mutex. Otherwise TryLock races with those short budget checks and can
	// falsely attribute their lock ownership to the blocked resolver calls.
	assertEnrollmentAcquisitionErrors(t, results, 32-limit, errEnrollmentTransportPermitBusy)
	if slots := gater.UnknownEnrollmentSlots(); slots != limit {
		t.Fatalf("resolving slots = %d, want shared limit", slots)
	}
	if gater.InterceptSecured(network.DirInbound, unknown.libp2pID, testConnectionAddresses()) {
		t.Fatal("inbound reservation bypassed the resolving enrollment budget")
	}
	if !gater.mu.TryLock() {
		t.Fatal("DNS resolution held the gater state mutex")
	}
	gater.mu.Unlock()
	cancel()
	assertEnrollmentAcquisitionErrors(t, results, limit, ErrEnrollmentTransportPermit)
	if resolver.calls.Load() != int32(limit) ||
		gater.UnknownEnrollmentSlots() != 0 {
		t.Fatalf("resolver calls/remaining slots = %d/%d, want 8/0",
			resolver.calls.Load(), gater.UnknownEnrollmentSlots())
	}
}

func TestConnectionGaterCanceledResolutionCannotConvertToPermit(t *testing.T) {
	local := testAuthorityPeer(t, "resolver-cancel-linearization-local")
	owner := testAuthorityPeer(t, "resolver-cancel-linearization-owner")
	authority, _ := NewAuthority(local.modelID)
	gater := newTestConnectionGater(t, authority)
	spec := testEnrollmentTransportPermitSpec(t, owner,
		[]string{"/dns4/cancel.test/tcp/45201"}, "cancel-linearization")
	key, _, err := canonicalOutboundEnrollmentPermit(spec)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	resolution, callbacks, err := gater.reserveOutboundEnrollmentResolution(ctx, key)
	runEnrollmentPermitCallbacks(callbacks)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	token, err := gater.completeOutboundEnrollmentResolution(resolution,
		[]ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/45201")}, nil, nil)
	resolution.cancel()
	if token.generation != 0 || !errors.Is(err, context.Canceled) ||
		gater.UnknownEnrollmentSlots() != 0 || gater.outboundEnrollmentPermits() != 0 {
		t.Fatalf("canceled conversion = (%+v, %v), slots/permits %d/%d",
			token, err, gater.UnknownEnrollmentSlots(), gater.outboundEnrollmentPermits())
	}
}

func TestConnectionGaterResolutionDoesNotRenewTransportTTL(t *testing.T) {
	local := testAuthorityPeer(t, "resolver-ttl-local")
	owner := testAuthorityPeer(t, "resolver-ttl-owner")
	authority, _ := NewAuthority(local.modelID)
	gater := newTestConnectionGater(t, authority)
	start := time.Date(2026, 7, 20, 8, 0, 0, 0, time.UTC)
	gater.pendingTTL = time.Minute
	gater.now = func() time.Time { return start }
	spec := testEnrollmentTransportPermitSpec(t, owner,
		[]string{"/dns4/ttl.test/tcp/45211"}, "resolution-ttl")
	key, _, err := canonicalOutboundEnrollmentPermit(spec)
	if err != nil {
		t.Fatal(err)
	}
	resolution, callbacks, err := gater.reserveOutboundEnrollmentResolution(context.Background(), key)
	runEnrollmentPermitCallbacks(callbacks)
	if err != nil {
		t.Fatal(err)
	}
	gater.now = func() time.Time { return start.Add(time.Minute + time.Nanosecond) }
	token, err := gater.completeOutboundEnrollmentResolution(resolution,
		[]ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/45211")}, nil, nil)
	resolution.cancel()
	if token.generation != 0 || !errors.Is(err, ErrEnrollmentTransportPermit) ||
		gater.UnknownEnrollmentSlots() != 0 {
		t.Fatalf("post-resolution renewed permit = (%+v, %v), slots %d",
			token, err, gater.UnknownEnrollmentSlots())
	}
}

func TestMeshRuntimeDNSResolutionFailuresLeaveNoTransportState(t *testing.T) {
	runtime, owner, _, _ := newMeshEnrollmentTransportFixture(t, "resolver-failure")
	other := testAuthorityPeer(t, "resolver-wrong-peer")
	sentinel := errors.New("lookup failed")
	cases := []struct {
		name     string
		resolver enrollmentMultiaddrResolver
	}{
		{name: "empty", resolver: enrollmentResolverFunc(func(context.Context, ma.Multiaddr) ([]ma.Multiaddr, error) {
			return nil, nil
		})},
		{name: "over-limit", resolver: enrollmentResolverFunc(func(context.Context, ma.Multiaddr) ([]ma.Multiaddr, error) {
			return distinctEnrollmentAddresses(9), nil
		})},
		{name: "wrong-peer", resolver: enrollmentResolverFunc(func(context.Context, ma.Multiaddr) ([]ma.Multiaddr, error) {
			return []ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/45301/p2p/" + other.libp2pID.String())}, nil
		})},
		{name: "lookup-failure", resolver: enrollmentResolverFunc(func(context.Context, ma.Multiaddr) ([]ma.Multiaddr, error) {
			return nil, sentinel
		})},
	}
	for index, testCase := range cases {
		runtime.nodeHost.gater.dnsResolver = testCase.resolver
		request := meshEnrollmentTransportRequestForAddresses(t, owner,
			[]string{fmt.Sprintf("/dns4/%s.test/tcp/453%02d", testCase.name, index)},
			"dns-failure-"+testCase.name)
		permit, err := runtime.acquireEnrollmentTransportPermit(context.Background(), request)
		if permit != nil || !errors.Is(err, ErrEnrollmentTransportPermit) {
			t.Fatalf("%s resolution = (%v, %v)", testCase.name, permit, err)
		}
		assertNoEnrollmentTransportState(t, runtime, owner.PeerID())
	}
}

func TestMeshRuntimeBlockedDNSResolutionDoesNotHoldLocksAndShutdownDrains(t *testing.T) {
	runtime, owner, _, _ := newMeshEnrollmentTransportFixture(t, "resolver-shutdown")
	resolver := newBlockingEnrollmentResolver()
	runtime.nodeHost.gater.dnsResolver = resolver
	request := meshEnrollmentTransportRequestForAddresses(t, owner,
		[]string{"/dns4/shutdown.test/tcp/45401"}, "dns-shutdown")
	result := make(chan error, 1)
	go func() {
		_, err := runtime.acquireEnrollmentTransportPermit(context.Background(), request)
		result <- err
	}()
	waitForResolverEntries(t, resolver.entered, 1)
	assertDNSResolutionLocksAvailable(t, runtime)
	closed := make(chan error, 1)
	go func() { closed <- runtime.Close() }()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("blocked acquisition shutdown error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not cancel the blocked resolver")
	}
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("runtime shutdown did not join DNS resolution")
	}
	assertNoEnrollmentTransportState(t, runtime, owner.PeerID())
}

func TestMeshRuntimeDNSPermitOpensRealLibp2pTransportAndClosesCleanly(t *testing.T) {
	runtime, owner, ownerHost, mesh := newMeshEnrollmentTransportFixture(t, "resolver-real")
	records := newMutableEnrollmentDNS()
	records.setIPs("owner.test", "127.0.0.1")
	runtime.nodeHost.gater.dnsResolver = newEnrollmentDNSResolver(t, records)
	port := enrollmentHostTCPPort(t, ownerHost)
	request := meshEnrollmentTransportRequestForAddresses(t, owner,
		[]string{"/dns4/owner.test/tcp/" + port}, "dns-real")
	ownerHost.SetStreamHandler(ChannelProtocol, func(stream network.Stream) {
		_, _ = stream.Write([]byte{'d'})
		_ = stream.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	permit, err := runtime.acquireEnrollmentTransportPermit(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := runtime.openEnrollmentStream(ctx, permit)
	if err != nil {
		t.Fatal(err)
	}
	one := make([]byte, 1)
	if count, readErr := stream.Read(one); readErr != nil || count != 1 || one[0] != 'd' {
		t.Fatalf("DNS-backed stream response = (%q, %v)", one[:count], readErr)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if err := permit.Close(); err != nil {
		t.Fatal(err)
	}
	runtime.mu.Lock()
	closed, terminalErr := runtime.closed, runtime.terminalErr
	runtime.mu.Unlock()
	if closed || terminalErr != nil || !runtime.HasCurrentSession(mesh.Channels()[0].Channel().ID()) {
		t.Fatalf("clean stream retirement failed closed runtime: closed=%v err=%v",
			closed, terminalErr)
	}
}

type enrollmentResolverFunc func(context.Context, ma.Multiaddr) ([]ma.Multiaddr, error)

func (resolve enrollmentResolverFunc) Resolve(ctx context.Context,
	address ma.Multiaddr,
) ([]ma.Multiaddr, error) {
	return resolve(ctx, address)
}

type mutableEnrollmentDNS struct {
	mu      sync.Mutex
	ips     map[string][]net.IPAddr
	txt     map[string][]string
	ipCalls map[string]int
}

func newMutableEnrollmentDNS() *mutableEnrollmentDNS {
	return &mutableEnrollmentDNS{ips: make(map[string][]net.IPAddr),
		txt: make(map[string][]string), ipCalls: make(map[string]int)}
}

func (records *mutableEnrollmentDNS) LookupIPAddr(ctx context.Context,
	name string,
) ([]net.IPAddr, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	records.mu.Lock()
	defer records.mu.Unlock()
	records.ipCalls[name]++
	return append([]net.IPAddr(nil), records.ips[name]...), nil
}

func (records *mutableEnrollmentDNS) LookupTXT(ctx context.Context, name string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	records.mu.Lock()
	defer records.mu.Unlock()
	return append([]string(nil), records.txt[name]...), nil
}

func (records *mutableEnrollmentDNS) setIPs(name string, values ...string) {
	records.mu.Lock()
	defer records.mu.Unlock()
	records.ips[name] = make([]net.IPAddr, len(values))
	for index, value := range values {
		records.ips[name][index] = net.IPAddr{IP: net.ParseIP(value)}
	}
}

func (records *mutableEnrollmentDNS) setTXT(name string, values ...string) {
	records.mu.Lock()
	defer records.mu.Unlock()
	records.txt[name] = append([]string(nil), values...)
}

func (records *mutableEnrollmentDNS) ipCallCount(name string) int {
	records.mu.Lock()
	defer records.mu.Unlock()
	return records.ipCalls[name]
}

type blockingEnrollmentResolver struct {
	entered chan struct{}
	calls   atomic.Int32
}

func newBlockingEnrollmentResolver() *blockingEnrollmentResolver {
	return &blockingEnrollmentResolver{entered: make(chan struct{}, 32)}
}

func (resolver *blockingEnrollmentResolver) Resolve(ctx context.Context,
	_ ma.Multiaddr,
) ([]ma.Multiaddr, error) {
	resolver.calls.Add(1)
	resolver.entered <- struct{}{}
	<-ctx.Done()
	return nil, ctx.Err()
}

func newEnrollmentDNSResolver(t *testing.T, records *mutableEnrollmentDNS) *madns.Resolver {
	t.Helper()
	resolver, err := madns.NewResolver(madns.WithDefaultResolver(records))
	if err != nil {
		t.Fatal(err)
	}
	return resolver
}

func acquireClaimedEnrollmentPermit(t *testing.T, gater *ConnectionGater,
	spec enrollmentTransportPermitSpec,
) outboundEnrollmentPermitToken {
	t.Helper()
	token, err := gater.acquireOutboundEnrollmentPermit(context.Background(), spec, nil)
	if err != nil || !gater.claimOutboundEnrollmentStream(token) {
		t.Fatalf("acquire/claim DNS permit = %v", err)
	}
	return token
}

func assertEnrollmentPermitAddresses(t *testing.T, gater *ConnectionGater,
	token outboundEnrollmentPermitToken, owner libp2ppeer.ID, allowed string, rejected ...string,
) {
	t.Helper()
	if len(token.addresses) != 1 || token.addresses[0].String() != allowed ||
		!gater.InterceptAddrDial(owner, ma.StringCast(allowed)) {
		t.Fatalf("frozen addresses = %v, want only %s", multiaddrStrings(token.addresses), allowed)
	}
	for _, raw := range rejected {
		if gater.InterceptAddrDial(owner, ma.StringCast(raw)) {
			t.Fatalf("frozen permit accepted %s", raw)
		}
	}
}

func startBlockedEnrollmentAcquisitions(t *testing.T, ctx context.Context,
	gater *ConnectionGater, owner authorityTestPeer, count int,
) <-chan error {
	t.Helper()
	start := make(chan struct{})
	results := make(chan error, count)
	for index := 0; index < count; index++ {
		index := index
		go func() {
			<-start
			spec := testEnrollmentTransportPermitSpec(t, owner,
				[]string{fmt.Sprintf("/dns4/budget.test/tcp/%d", 46000+index)},
				fmt.Sprintf("dns-budget-%02d", index))
			_, err := gater.acquireOutboundEnrollmentPermit(ctx, spec, nil)
			results <- err
		}()
	}
	close(start)
	return results
}

func waitForResolverEntries(t *testing.T, entered <-chan struct{}, count int) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for index := 0; index < count; index++ {
		select {
		case <-entered:
		case <-deadline.C:
			t.Fatalf("resolver entries = %d, want %d", index, count)
		}
	}
}

func assertEnrollmentAcquisitionErrors(t *testing.T, results <-chan error, count int,
	want error,
) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for index := 0; index < count; index++ {
		select {
		case err := <-results:
			if !errors.Is(err, want) {
				t.Fatalf("acquisition %d error = %v, want %v", index, err, want)
			}
		case <-deadline.C:
			t.Fatalf("acquisitions returned = %d, want %d", index, count)
		}
	}
}

func assertDNSResolutionLocksAvailable(t *testing.T, runtime *MeshRuntime) {
	t.Helper()
	if !runtime.mu.TryLock() {
		t.Fatal("DNS resolution held the runtime state mutex")
	}
	runtime.mu.Unlock()
	if !runtime.addressSources.mu.TryLock() {
		t.Fatal("DNS resolution held the address-source mutex")
	}
	runtime.addressSources.mu.Unlock()
	if !runtime.nodeHost.gater.mu.TryLock() {
		t.Fatal("DNS resolution held the gater state mutex")
	}
	runtime.nodeHost.gater.mu.Unlock()
}

func assertNoEnrollmentTransportState(t *testing.T, runtime *MeshRuntime, owner model.PeerID) {
	t.Helper()
	peerID, err := libp2ppeer.Decode(owner.String())
	if err != nil {
		t.Fatal(err)
	}
	if runtime.nodeHost.gater.UnknownEnrollmentSlots() != 0 ||
		len(runtime.managedRuntimeHost().Peerstore().Addrs(peerID)) != 0 {
		t.Fatalf("failed DNS acquisition retained slots/addresses = %d/%v",
			runtime.nodeHost.gater.UnknownEnrollmentSlots(),
			runtime.managedRuntimeHost().Peerstore().Addrs(peerID))
	}
}

func distinctEnrollmentAddresses(count int) []ma.Multiaddr {
	addresses := make([]ma.Multiaddr, count)
	for index := range addresses {
		addresses[index] = ma.StringCast(fmt.Sprintf("/ip4/127.0.0.%d/tcp/45301", index+1))
	}
	return addresses
}

func enrollmentHostTCPPort(t *testing.T, ownerHost interface{ Addrs() []ma.Multiaddr }) string {
	t.Helper()
	for _, address := range ownerHost.Addrs() {
		if value, err := address.ValueForProtocol(ma.P_TCP); err == nil {
			return value
		}
	}
	t.Fatal("owner Host has no TCP listen address")
	return ""
}

func multiaddrStrings(addresses []ma.Multiaddr) []string {
	result := make([]string, len(addresses))
	for index, address := range addresses {
		result[index] = address.String()
	}
	return result
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
