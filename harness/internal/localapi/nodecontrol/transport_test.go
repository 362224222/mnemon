package nodecontrol

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

func TestControlProvidersMapNodeObservationsExactly(t *testing.T) {
	t.Parallel()
	profile := nodecontrolTestProfile(t, model.Sum([]byte("provider-credential")),
		model.Sum([]byte("provider-assets")).String())
	authority := nodecontrolTestAuthority(t)
	observer := &controlObserverStub{
		health: node.DaemonHealth{AssetRevision: authority.AssetRevision, Ready: true},
		status: node.ControlStatus{AssetRevision: authority.AssetRevision,
			ActivationReady: true, Runtime: node.RuntimeStatus{Running: true, Ready: true, Healthy: true}},
		authority: authority,
	}
	metadata := localapi.RequestMetadata{Profile: profile}

	health, apiErr := (healthProvider{observer: observer}).Health(context.Background(), metadata)
	if apiErr != nil || health != (localapi.HealthSnapshot{
		AssetRevision: authority.AssetRevision, WorkersReady: true,
	}) {
		t.Fatalf("Health() = (%#v, %#v)", health, apiErr)
	}
	status, apiErr := (statusProvider{observer: observer}).Status(context.Background(), metadata)
	wantStatus := localapi.StatusSnapshot{AssetRevision: authority.AssetRevision,
		ActivationReady: true, Runtime: localapi.RuntimeStatusSnapshot{
			Running: true, Ready: true, Healthy: true,
		}}
	if apiErr != nil || status != wantStatus {
		t.Fatalf("Status() = (%#v, %#v), want %#v", status, apiErr, wantStatus)
	}
	gotAuthority, apiErr := (authorityProvider{observer: observer}).Authority(context.Background(), metadata)
	wantAuthority := localapi.AuthoritySnapshot{Host: authority.Host, Runtime: authority.Runtime,
		Enabled: authority.Enabled, AssetRevision: authority.AssetRevision,
		UpdatedAt: authority.UpdatedAt, PeerID: authority.PeerID,
		ActiveAssetRevision: authority.ActiveAssetRevision}
	if apiErr != nil || gotAuthority != wantAuthority {
		t.Fatalf("Authority() = (%#v, %#v), want %#v", gotAuthority, apiErr, wantAuthority)
	}
	if observer.healthCalls.Load() != 1 || observer.statusCalls.Load() != 1 ||
		observer.authorityCalls.Load() != 1 || observer.lastProfile != profile.ID() {
		t.Fatalf("provider calls/profile = (%d, %d, %d, %s)", observer.healthCalls.Load(),
			observer.statusCalls.Load(), observer.authorityCalls.Load(), observer.lastProfile.String())
	}
}

func TestControlProvidersMapControlErrorsAndFailClosedOnInvalidAuthority(t *testing.T) {
	t.Parallel()
	profile := nodecontrolTestProfile(t, model.Sum([]byte("error-credential")),
		model.Sum([]byte("error-assets")).String())
	metadata := localapi.RequestMetadata{Profile: profile}
	controlErr := &node.ControlError{Code: node.ControlCodeMnemondUnavailable,
		Retryable: true, Message: "managed controller unavailable"}
	tests := []struct {
		name string
		call func(*controlObserverStub) *localapi.APIError
	}{
		{name: "health", call: func(observer *controlObserverStub) *localapi.APIError {
			observer.healthErr = controlErr
			_, apiErr := (healthProvider{observer: observer}).Health(context.Background(), metadata)
			return apiErr
		}},
		{name: "status", call: func(observer *controlObserverStub) *localapi.APIError {
			observer.statusErr = controlErr
			_, apiErr := (statusProvider{observer: observer}).Status(context.Background(), metadata)
			return apiErr
		}},
		{name: "authority", call: func(observer *controlObserverStub) *localapi.APIError {
			observer.authorityErr = controlErr
			_, apiErr := (authorityProvider{observer: observer}).Authority(context.Background(), metadata)
			return apiErr
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			apiErr := test.call(&controlObserverStub{})
			if apiErr == nil || apiErr.Code != localapi.CodeMnemondUnavailable || !apiErr.Retryable ||
				apiErr.Message != controlErr.Message {
				t.Fatalf("provider control error = %#v", apiErr)
			}
		})
	}

	_, apiErr := (authorityProvider{observer: &controlObserverStub{}}).Authority(context.Background(), metadata)
	if apiErr == nil || apiErr.Code != localapi.CodeInternal || apiErr.Retryable {
		t.Fatalf("invalid authority error = %#v", apiErr)
	}
}

func TestMutationPreparerReleasesAdmissionOnEveryRejectedPreparation(t *testing.T) {
	t.Parallel()
	profile := nodecontrolTestProfile(t, model.Sum([]byte("mutation-credential")), model.Sum([]byte("mutation-assets")).String())
	for _, controlErr := range []*node.ControlError{nil, {Code: node.ControlCodeInternal, Message: "mutation preparation failed"}} {
		var releases atomic.Int32
		controller := &mutationControllerStub{release: func() { releases.Add(1) }, controlErr: controlErr}
		snapshot, release, apiErr := (mutationShutdownPreparer{controller: controller}).PrepareMutationShutdown(
			context.Background(), localapi.RequestMetadata{Profile: profile})
		if apiErr == nil || apiErr.Code != localapi.CodeInternal || release != nil ||
			snapshot != (localapi.AuthoritySnapshot{}) || releases.Load() != 1 || controller.calls.Load() != 1 {
			t.Fatalf("rejected mutation = snapshot=%#v release=%v error=%#v releases=%d calls=%d",
				snapshot, release != nil, apiErr, releases.Load(), controller.calls.Load())
		}
	}
}

func TestFactoryPrepareBindsDormantThenServesAuthenticatedLifecycle(t *testing.T) {
	nodeState, options, bindings, observer, lifecycleCalls := newFactoryFixture(t)
	transport, err := (Factory{}).Prepare(context.Background(), options, bindings)
	if err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(nodeState, "control.sock")
	assertOwnerSocket(t, socketPath)
	assertPreparedTransportDormant(t, nodeState, transport, observer)
	runPreparedAuthenticatedLifecycle(t, nodeState, transport, observer, lifecycleCalls)
	if _, err := os.Lstat(socketPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("control socket after lifecycle close = %v", err)
	}
}

func assertPreparedTransportDormant(t *testing.T, nodeState string, transport node.PreparedControlTransport,
	observer *controlObserverStub) {
	t.Helper()
	connection, err := net.DialTimeout("unix", filepath.Join(nodeState, "control.sock"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(connection, "GET /v1/health HTTP/1.1\r\nHost: mnemond\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	if err := connection.SetReadDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 1)
	if _, err := connection.Read(buffer); err == nil {
		t.Fatal("dormant prepared transport responded before Run")
	} else if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		t.Fatalf("dormant prepared transport read error = %v, want timeout", err)
	}
	if calls := observer.healthCalls.Load(); calls != 0 {
		t.Fatalf("dormant transport health calls = %d, want 0", calls)
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
}

func runPreparedAuthenticatedLifecycle(t *testing.T, nodeState string, transport node.PreparedControlTransport,
	observer *controlObserverStub, lifecycleCalls *atomic.Int32) {
	t.Helper()
	runDone := make(chan error, 1)
	go func() { runDone <- transport.Run(context.Background()) }()
	readyCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := transport.Readiness(readyCtx); err != nil {
		t.Fatalf("authenticated Readiness() error = %v", err)
	}
	client, err := localapi.NewClient(nodeState)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := AuthorityResponse(observer.authority)
	if err != nil {
		t.Fatal(err)
	}
	response, apiErr := client.Shutdown(readyCtx, wire)
	if apiErr != nil || response.SchemaVersion != localapi.SchemaVersion ||
		response.Status != "stopping" || lifecycleCalls.Load() != 1 {
		t.Fatalf("authenticated Shutdown() = (%#v, %#v), lifecycle calls=%d",
			response, apiErr, lifecycleCalls.Load())
	}
	if err := transport.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run() after Shutdown = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not stop prepared transport")
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFactoryPrepareRejectsInvalidAndTypedNilBindingsWithoutSocket(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*context.Context, *node.ControlTransportOptions, *node.ControlBindings)
	}{
		{name: "nil context", mutate: func(ctx *context.Context, _ *node.ControlTransportOptions,
			_ *node.ControlBindings) {
			*ctx = nil
		}},
		{name: "cancelled context", mutate: func(ctx *context.Context, _ *node.ControlTransportOptions,
			_ *node.ControlBindings) {
			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			*ctx = cancelled
		}},
		{name: "relative node state", mutate: func(_ *context.Context,
			options *node.ControlTransportOptions, _ *node.ControlBindings) {
			options.NodeState = "relative"
		}},
		{name: "zero connection bound", mutate: func(_ *context.Context,
			options *node.ControlTransportOptions, _ *node.ControlBindings) {
			options.MaxConnections = 0
		}},
		{name: "invalid asset revision", mutate: func(_ *context.Context,
			options *node.ControlTransportOptions, _ *node.ControlBindings) {
			options.AssetRevision = "invalid"
		}},
		{name: "nil authenticator", mutate: func(_ *context.Context, _ *node.ControlTransportOptions,
			bindings *node.ControlBindings) {
			bindings.Authenticator = nil
		}},
		{name: "typed nil authenticator", mutate: func(_ *context.Context,
			_ *node.ControlTransportOptions, bindings *node.ControlBindings) {
			var value *profileAuthenticatorStub
			bindings.Authenticator = value
		}},
		{name: "nil agent", mutate: func(_ *context.Context, _ *node.ControlTransportOptions,
			bindings *node.ControlBindings) {
			bindings.Agent = nil
		}},
		{name: "typed nil agent", mutate: func(_ *context.Context, _ *node.ControlTransportOptions,
			bindings *node.ControlBindings) {
			var value *noopManagedService
			bindings.Agent = value
		}},
		{name: "nil observer", mutate: func(_ *context.Context, _ *node.ControlTransportOptions,
			bindings *node.ControlBindings) {
			bindings.Observer = nil
		}},
		{name: "typed nil observer", mutate: func(_ *context.Context, _ *node.ControlTransportOptions,
			bindings *node.ControlBindings) {
			var value *controlObserverStub
			bindings.Observer = value
		}},
		{name: "nil mutation controller", mutate: func(_ *context.Context,
			_ *node.ControlTransportOptions, bindings *node.ControlBindings) {
			bindings.Mutation = nil
		}},
		{name: "typed nil mutation controller", mutate: func(_ *context.Context,
			_ *node.ControlTransportOptions, bindings *node.ControlBindings) {
			var value *mutationControllerStub
			bindings.Mutation = value
		}},
		{name: "nil shutdown", mutate: func(_ *context.Context, _ *node.ControlTransportOptions,
			bindings *node.ControlBindings) {
			bindings.Shutdown = nil
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			nodeState, options, bindings, _, _ := newFactoryFixture(t)
			test.mutate(&ctx, &options, &bindings)
			transport, err := (Factory{}).Prepare(ctx, options, bindings)
			if err == nil || transport != nil {
				t.Fatalf("Prepare() = (%T, %v), want nil transport and error", transport, err)
			}
			if _, err := os.Lstat(filepath.Join(nodeState, "control.sock")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid Prepare left a control socket: %v", err)
			}
		})
	}
}

func TestFactoryPrepareRecoversStaleSocketAndCloseDeletesReplacement(t *testing.T) {
	nodeState, options, bindings, _, _ := newFactoryFixture(t)
	socketPath := filepath.Join(nodeState, "control.sock")
	stale, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	stale.SetUnlinkOnClose(false)
	if err := os.Chmod(socketPath, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := stale.Close(); err != nil {
		t.Fatal(err)
	}
	assertOwnerSocket(t, socketPath)

	transport, err := (Factory{}).Prepare(context.Background(), options, bindings)
	if err != nil {
		t.Fatal(err)
	}
	assertOwnerSocket(t, socketPath)
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(socketPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replacement socket after Close = %v", err)
	}
}

func TestFactoryPreparePreservesActiveOwnerSocket(t *testing.T) {
	nodeState, options, bindings, _, _ := newFactoryFixture(t)
	socketPath := filepath.Join(nodeState, "control.sock")
	active, err := localapi.ListenOwnerUnix(socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer active.Close()
	transport, err := (Factory{}).Prepare(context.Background(), options, bindings)
	if err == nil || transport != nil {
		t.Fatalf("Prepare against active socket = (%T, %v)", transport, err)
	}
	assertOwnerSocket(t, socketPath)
}

func TestConnectionAdmissionListenerBoundsAcceptBeforeNetHTTP(t *testing.T) {
	t.Parallel()
	raw := newScriptedListener()
	listener, err := newConnectionAdmissionListener(raw, 2)
	if err != nil {
		t.Fatal(err)
	}
	serverEnds := make([]net.Conn, 3)
	clientEnds := make([]net.Conn, 3)
	for index := range serverEnds {
		serverEnds[index], clientEnds[index] = net.Pipe()
		raw.accepts <- serverEnds[index]
		defer clientEnds[index].Close()
	}
	first := mustAcceptConnection(t, listener)
	<-raw.called
	second := mustAcceptConnection(t, listener)
	<-raw.called
	thirdStarted := make(chan struct{})
	thirdResult := make(chan net.Conn, 1)
	thirdError := make(chan error, 1)
	go func() {
		close(thirdStarted)
		connection, acceptErr := listener.Accept()
		thirdResult <- connection
		thirdError <- acceptErr
	}()
	<-thirdStarted
	select {
	case <-raw.called:
		t.Fatal("underlying Accept ran while the connection budget was full")
	case <-time.After(25 * time.Millisecond):
	}
	mustCloseConnection(t, first)
	select {
	case <-raw.called:
	case <-time.After(time.Second):
		t.Fatal("releasing a connection did not resume underlying Accept")
	}
	var third net.Conn
	select {
	case third = <-thirdResult:
		if err := <-thirdError; err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("third connection was not admitted after capacity returned")
	}
	mustCloseConnection(t, second)
	mustCloseConnection(t, third)
	mustCloseConnection(t, listener)
}

func TestConnectionAdmissionListenerCloseUnblocksCapacityWaiter(t *testing.T) {
	t.Parallel()
	raw := newScriptedListener()
	listener, err := newConnectionAdmissionListener(raw, 1)
	if err != nil {
		t.Fatal(err)
	}
	server, client := net.Pipe()
	defer client.Close()
	raw.accepts <- server
	accepted, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	<-raw.called
	waiting := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(waiting)
		_, acceptErr := listener.Accept()
		done <- acceptErr
	}()
	<-waiting
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("blocked Accept error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock capacity-limited Accept")
	}
	if err := accepted.Close(); err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("repeated Close() = %v", err)
	}
	if calls := raw.closeCalls.Load(); calls != 1 {
		t.Fatalf("underlying listener Close calls = %d, want 1", calls)
	}
}

func TestConnectionAdmissionListenerRequiresFiniteAuthority(t *testing.T) {
	t.Parallel()
	if _, err := newConnectionAdmissionListener(nil, 1); err == nil {
		t.Fatal("nil listener was accepted")
	}
	raw := newScriptedListener()
	if _, err := newConnectionAdmissionListener(raw, 0); err == nil {
		t.Fatal("zero connection limit was accepted")
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAdmittedConnectionClosesUnderlyingAndReleasesExactlyOnce(t *testing.T) {
	t.Parallel()
	raw := newScriptedListener()
	listener, err := newConnectionAdmissionListener(raw, 1)
	if err != nil {
		t.Fatal(err)
	}
	server, client := net.Pipe()
	defer client.Close()
	counted := &countingConnection{Conn: server}
	raw.accepts <- counted
	accepted := mustAcceptConnection(t, listener)
	<-raw.called

	closed := make(chan error, 1)
	go func() {
		if err := accepted.Close(); err != nil {
			closed <- err
			return
		}
		closed <- accepted.Close()
	}()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("repeated admitted connection Close blocked while releasing capacity")
	}
	if calls := counted.closeCalls.Load(); calls != 1 {
		t.Fatalf("underlying connection Close calls = %d, want 1", calls)
	}

	nextServer, nextClient := net.Pipe()
	defer nextClient.Close()
	raw.accepts <- nextServer
	next := mustAcceptConnection(t, listener)
	<-raw.called
	mustCloseConnection(t, next)
	mustCloseConnection(t, listener)
}

func TestRequestTrackerRejectsNewHandlersAndDrainsEnteredHandler(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{})
	release := make(chan struct{})
	tracker := newRequestTracker(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(entered)
		<-release
	}))
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		tracker.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("tracked handler did not start")
	}
	drained := tracker.seal()
	select {
	case <-drained:
		t.Fatal("tracker drained while entered handler was active")
	default:
	}
	rejected := httptest.NewRecorder()
	tracker.ServeHTTP(rejected, httptest.NewRequest(http.MethodGet, "/", nil))
	if rejected.Code != http.StatusServiceUnavailable {
		t.Fatalf("post-seal status = %d", rejected.Code)
	}
	close(release)
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("entered handler did not return")
	}
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("tracker did not publish complete drain")
	}
}

func TestPreparedTransportAcceptsOnlyDuringRunAndShutsDownCleanly(t *testing.T) {
	t.Parallel()
	raw := newScriptedListener()
	listener, err := newConnectionAdmissionListener(raw, 1)
	if err != nil {
		t.Fatal(err)
	}
	requests := newRequestTracker(http.NotFoundHandler())
	transport := &preparedTransport{
		listener:           listener,
		requests:           requests,
		server:             &http.Server{Handler: requests},
		shutdownTimeout:    time.Second,
		forcedDrainTimeout: time.Second,
	}
	select {
	case <-raw.called:
		t.Fatal("prepared transport accepted before Run")
	default:
	}
	runDone := make(chan error, 1)
	go func() { runDone <- transport.Run(context.Background()) }()
	select {
	case <-raw.called:
	case <-time.After(time.Second):
		t.Fatal("Run did not begin accepting on the prepared listener")
	}
	if err := transport.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run after graceful shutdown = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("graceful shutdown did not stop Run")
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	if err := transport.Close(); err != nil {
		t.Fatalf("repeated prepared transport Close() = %v", err)
	}
	if calls := raw.closeCalls.Load(); calls != 1 {
		t.Fatalf("underlying listener Close calls = %d, want 1", calls)
	}
}

func TestPreparedTransportBoundsStubbornHandlerAndRetainsNodeAuthority(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{})
	release := make(chan struct{})
	requestDone := make(chan struct{})
	requests := newRequestTracker(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(entered)
		<-release
	}))
	go func() {
		defer close(requestDone)
		requests.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("stubborn handler did not enter")
	}
	transport := &preparedTransport{requests: requests, server: &http.Server{Handler: requests},
		shutdownTimeout: 20 * time.Millisecond, forcedDrainTimeout: 20 * time.Millisecond}
	started := time.Now()
	err := transport.Shutdown(context.Background())
	if !errors.Is(err, node.ErrControlTransportUndrained) {
		t.Fatalf("Shutdown() error = %v, want undrained authority signal", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("bounded Shutdown() took %s", elapsed)
	}
	close(release)
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("stubborn handler did not release during test cleanup")
	}
}

type scriptedListener struct {
	accepts    chan net.Conn
	called     chan struct{}
	closed     chan struct{}
	closeOnce  sync.Once
	closeCalls atomic.Int32
}

func newScriptedListener() *scriptedListener {
	return &scriptedListener{accepts: make(chan net.Conn, 8), called: make(chan struct{}, 8),
		closed: make(chan struct{})}
}

func (listener *scriptedListener) Accept() (net.Conn, error) {
	listener.called <- struct{}{}
	select {
	case connection := <-listener.accepts:
		return connection, nil
	case <-listener.closed:
		return nil, net.ErrClosed
	}
}

func (listener *scriptedListener) Close() error {
	listener.closeCalls.Add(1)
	listener.closeOnce.Do(func() { close(listener.closed) })
	return nil
}

func (*scriptedListener) Addr() net.Addr { return scriptedAddress("nodecontrol-test") }

type scriptedAddress string

func (address scriptedAddress) Network() string { return "scripted" }

func (address scriptedAddress) String() string { return string(address) }

type countingConnection struct {
	net.Conn
	closeCalls atomic.Int32
}

func (connection *countingConnection) Close() error {
	connection.closeCalls.Add(1)
	return connection.Conn.Close()
}

func mustAcceptConnection(t *testing.T, listener net.Listener) net.Conn {
	t.Helper()
	connection, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	return connection
}

func mustCloseConnection(t *testing.T, closer io.Closer) {
	t.Helper()
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
}

type profileAuthenticatorStub struct {
	want    model.Digest
	profile model.Profile
}

func (authenticator *profileAuthenticatorStub) AuthenticateProfile(_ context.Context,
	credential model.Digest) (model.Profile, error) {
	if credential != authenticator.want {
		return model.Profile{}, errors.New("profile credential was rejected")
	}
	return authenticator.profile, nil
}

type noopManagedService struct{ node.ManagedControlService }

type controlObserverStub struct {
	health         node.DaemonHealth
	status         node.ControlStatus
	authority      node.Authority
	healthErr      *node.ControlError
	statusErr      *node.ControlError
	authorityErr   *node.ControlError
	healthCalls    atomic.Int32
	statusCalls    atomic.Int32
	authorityCalls atomic.Int32
	lastProfile    model.ProfileID
}

func (observer *controlObserverStub) ObserveControlHealth(_ context.Context,
	profile model.Profile) (node.DaemonHealth, *node.ControlError) {
	observer.healthCalls.Add(1)
	observer.lastProfile = profile.ID()
	return observer.health, observer.healthErr
}

func (observer *controlObserverStub) ObserveControlStatus(_ context.Context,
	profile model.Profile) (node.ControlStatus, *node.ControlError) {
	observer.statusCalls.Add(1)
	observer.lastProfile = profile.ID()
	return observer.status, observer.statusErr
}

func (observer *controlObserverStub) ObserveControlAuthority(_ context.Context,
	profile model.Profile) (node.Authority, *node.ControlError) {
	observer.authorityCalls.Add(1)
	observer.lastProfile = profile.ID()
	return observer.authority, observer.authorityErr
}

type mutationControllerStub struct {
	authority  node.Authority
	release    func()
	controlErr *node.ControlError
	calls      atomic.Int32
}

func (controller *mutationControllerStub) PrepareMutationShutdown(_ context.Context,
	_ model.Profile) (node.Authority, func(), *node.ControlError) {
	controller.calls.Add(1)
	return controller.authority, controller.release, controller.controlErr
}

func newFactoryFixture(t *testing.T) (string, node.ControlTransportOptions, node.ControlBindings,
	*controlObserverStub, *atomic.Int32) {
	t.Helper()
	nodeState, err := os.MkdirTemp("/tmp", "mn-nc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(nodeState) })
	credential, created, err := localapi.EnsureProfileCredential(nodeState)
	if err != nil || !created {
		t.Fatalf("EnsureProfileCredential() = (%s, %t, %v)", credential.String(), created, err)
	}
	authority := nodecontrolTestAuthority(t)
	profile := nodecontrolTestProfile(t, credential, authority.AssetRevision)
	observer := &controlObserverStub{
		health: node.DaemonHealth{AssetRevision: authority.AssetRevision, Ready: true},
		status: node.ControlStatus{AssetRevision: authority.AssetRevision,
			ActivationReady: true, Runtime: node.RuntimeStatus{Running: true, Ready: true, Healthy: true}},
		authority: authority,
	}
	mutation := &mutationControllerStub{authority: authority, release: func() {}}
	var lifecycleCalls atomic.Int32
	bindings := node.ControlBindings{
		Authenticator: &profileAuthenticatorStub{want: credential, profile: profile},
		Agent:         &noopManagedService{},
		Observer:      observer,
		Mutation:      mutation,
		Shutdown:      func() { lifecycleCalls.Add(1) },
	}
	options := node.ControlTransportOptions{NodeState: nodeState,
		AssetRevision: authority.AssetRevision, MaxConnections: 4}
	return nodeState, options, bindings, observer, &lifecycleCalls
}

func nodecontrolTestProfile(t *testing.T, credential model.Digest, revision string) model.Profile {
	t.Helper()
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	profile, err := model.NewProfile(model.ProfileSpec{
		ID: model.TeamworkProfileID(), Principal: "nodecontrol-test",
		WorkspaceRoot: t.TempDir(), Host: model.HostCodex, Runtime: model.RuntimeCodexAppServer,
		CredentialHash: credential, ActiveAssetRevision: revision,
		HandlingBudget: model.DefaultHandlingBudget().JSON(), Enabled: true,
		CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

func assertOwnerSocket(t *testing.T, path string) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode = %v, want owner-only Unix socket", info.Mode())
	}
}
