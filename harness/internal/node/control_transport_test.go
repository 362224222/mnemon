package node

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	testControlReadHeaderTimeout  = 5 * time.Second
	testControlShutdownTimeout    = 5 * time.Second
	testControlForcedDrainTimeout = time.Second
)

func TestControllerTransportPreparationFailureClosesResourcesAndRetainsFence(t *testing.T) {
	errPrepare := errors.New("test control transport preparation failed")
	for _, test := range []struct {
		name       string
		prepare    func(*controllerPreparedTransportStub) (PreparedControlTransport, error)
		wantCause  error
		wantCloses int
	}{
		{name: "factory error", prepare: func(*controllerPreparedTransportStub) (PreparedControlTransport, error) {
			return nil, errPrepare
		}, wantCause: errPrepare},
		{name: "resource and factory error", prepare: func(transport *controllerPreparedTransportStub) (PreparedControlTransport, error) {
			return transport, errPrepare
		}, wantCause: errPrepare, wantCloses: 1},
		{name: "nil transport", prepare: func(*controllerPreparedTransportStub) (PreparedControlTransport, error) {
			return nil, nil
		}},
		{name: "typed nil transport", prepare: func(*controllerPreparedTransportStub) (PreparedControlTransport, error) {
			return (*controllerPreparedTransportStub)(nil), nil
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newDaemonFixture(t, true)
			transport := &controllerPreparedTransportStub{}
			prepares := 0
			factory := ControlTransportFactoryFunc(func(_ context.Context,
				options ControlTransportOptions, bindings ControlBindings,
			) (PreparedControlTransport, error) {
				prepares++
				if options.NodeState != fixture.nodeState || options.AssetRevision != fixture.revision ||
					options.MaxConnections != controllerControlConnectionLimit ||
					isNilNodeInterface(bindings.Authenticator) || isNilNodeInterface(bindings.Agent) ||
					isNilNodeInterface(bindings.Observer) || isNilNodeInterface(bindings.Mutation) ||
					bindings.Shutdown == nil {
					t.Fatalf("Prepare() options/bindings = (%#v, %#v)", options, bindings)
				}
				return test.prepare(transport)
			})
			releases := 0
			controller, closeStore := newControllerWithControlFactory(t, fixture, factory, func() error {
				releases++
				return nil
			})
			defer closeStore()
			err := controller.Serve(context.Background())
			if err == nil || test.wantCause != nil && !errors.Is(err, test.wantCause) ||
				prepares != 1 || releases != 0 || transport.closes != test.wantCloses ||
				transport.runs != 0 || transport.readiness != 0 || transport.shutdowns != 0 {
				t.Fatalf("Serve() = %v; prepares=%d releases=%d transport=%#v",
					err, prepares, releases, transport)
			}
			if err := controller.releaseBeforeAccept(); err != nil || releases != 1 {
				t.Fatalf("owner release = %v, calls=%d", err, releases)
			}
		})
	}
}

type controllerTypedNilControlFactory struct{}

func (*controllerTypedNilControlFactory) Prepare(context.Context, ControlTransportOptions,
	ControlBindings,
) (PreparedControlTransport, error) {
	panic("typed-nil control factory must be rejected before invocation")
}

type controllerPreparedTransportStub struct {
	runs      int
	readiness int
	shutdowns int
	closes    int
}

func (transport *controllerPreparedTransportStub) Run(context.Context) error {
	transport.runs++
	return errors.New("unexpected test control transport Run")
}

func (transport *controllerPreparedTransportStub) Readiness(context.Context) error {
	transport.readiness++
	return errors.New("unexpected test control transport Readiness")
}

func (transport *controllerPreparedTransportStub) Shutdown(context.Context) error {
	transport.shutdowns++
	return errors.New("unexpected test control transport Shutdown")
}

func (transport *controllerPreparedTransportStub) Close() error {
	if transport != nil {
		transport.closes++
	}
	return nil
}

func newControllerWithControlFactory(t *testing.T, fixture daemonFixture,
	factory ControlTransportFactory, beforeAccept func() error,
) (*Controller, func()) {
	t.Helper()
	authority, err := openExistingDaemonAuthority(context.Background(), fixture.workspace,
		fixture.nodeState, testProfileCredentials{})
	if err != nil {
		t.Fatal(err)
	}
	controller, err := NewController(context.Background(), ControllerOptions{
		NodeState: fixture.nodeState, Workspace: fixture.workspace, Store: authority.store,
		ArtifactCAS: newControllerTestCAS(t, fixture.nodeState),
		Profile:     authority.authority.Profile, Signer: authority.identity.PublicationSigner(),
		Clock: controllerTestClock{fixture.profile.UpdatedAt()}, Install: fixture.install,
		Control: factory, BeforeAccept: beforeAccept,
	})
	if err != nil {
		_ = authority.store.Close()
		t.Fatal(err)
	}
	return controller, func() {
		if err := authority.store.Close(); err != nil {
			t.Errorf("close Controller Store: %v", err)
		}
	}
}

// testLocalControlTransportFactory is deliberately test-only. It keeps the
// Node package's end-to-end lifecycle oracles on the real authenticated Unix
// HTTP protocol without making the production Node package depend on that
// protocol implementation.
type testLocalControlTransportFactory struct{}

func newTestControlTransportFactory() ControlTransportFactory {
	return testLocalControlTransportFactory{}
}

func (testLocalControlTransportFactory) Prepare(ctx context.Context,
	options ControlTransportOptions, bindings ControlBindings,
) (PreparedControlTransport, error) {
	if ctx == nil || ctx.Err() != nil || options.NodeState == "" ||
		!filepath.IsAbs(options.NodeState) || filepath.Clean(options.NodeState) != options.NodeState ||
		options.MaxConnections == 0 || isNilNodeInterface(bindings.Authenticator) ||
		isNilNodeInterface(bindings.Agent) || isNilNodeInterface(bindings.Observer) ||
		isNilNodeInterface(bindings.Mutation) || bindings.Shutdown == nil {
		return nil, errors.New("test local control transport requires complete bounded bindings")
	}
	if _, err := model.ParseDigest(options.AssetRevision); err != nil {
		return nil, errors.New("test local control transport asset revision is invalid")
	}
	server, err := localapi.NewServerWithStatusLifecycle(bindings.Authenticator,
		testLocalControlService{next: bindings.Agent},
		testLocalControlHealth{observer: bindings.Observer},
		testLocalControlStatus{observer: bindings.Observer},
		testLocalControlAuthority{observer: bindings.Observer},
		localapi.LifecycleFunc(bindings.Shutdown),
		testLocalControlMutation{controller: bindings.Mutation})
	if err != nil {
		return nil, err
	}
	socketPath := filepath.Join(options.NodeState, controlSocketName)
	if _, err := localapi.RemoveStaleOwnerUnix(ctx, socketPath); err != nil {
		return nil, err
	}
	listener, err := localapi.ListenOwnerUnix(socketPath)
	if err != nil {
		return nil, err
	}
	admitted, err := newTestControlAdmissionListener(listener, options.MaxConnections)
	if err != nil {
		return nil, errors.Join(err, listener.Close())
	}
	requests := newTestControlRequestTracker(server.Handler())
	return &testPreparedControlTransport{nodeState: options.NodeState,
		assetRevision: options.AssetRevision, listener: admitted, requests: requests,
		server: &http.Server{Handler: requests, ReadHeaderTimeout: testControlReadHeaderTimeout}}, nil
}

type testPreparedControlTransport struct {
	nodeState     string
	assetRevision string
	listener      *testControlAdmissionListener
	requests      *testControlRequestTracker
	server        *http.Server
	closeOnce     sync.Once
	closeErr      error
}

func (transport *testPreparedControlTransport) Run(ctx context.Context) error {
	if transport == nil || transport.server == nil || transport.listener == nil || ctx == nil {
		return errors.New("test local control transport is unavailable")
	}
	err := transport.server.Serve(transport.listener)
	if errors.Is(err, http.ErrServerClosed) || ctx.Err() != nil && errors.Is(err, os.ErrClosed) {
		return nil
	}
	return err
}

func (transport *testPreparedControlTransport) Readiness(ctx context.Context) error {
	if transport == nil || ctx == nil || ctx.Err() != nil {
		return errors.New("test local control transport startup was cancelled")
	}
	client, err := localapi.NewClient(transport.nodeState)
	if err != nil {
		return err
	}
	probeCtx, cancel := context.WithTimeout(ctx, testControlReadHeaderTimeout)
	defer cancel()
	health, apiErr := client.ProbeHealth(probeCtx)
	if apiErr != nil || health.SchemaVersion != localapi.SchemaVersion ||
		health.AssetRevision != transport.assetRevision ||
		(health.Status != "ready" && health.Status != "not_ready") {
		return errors.New("test local control authenticated startup proof failed")
	}
	return nil
}

func (transport *testPreparedControlTransport) Shutdown(ctx context.Context) error {
	if transport == nil || transport.server == nil || transport.requests == nil {
		return errors.New("test local control transport lifecycle is unavailable")
	}
	drained := transport.requests.seal()
	if ctx == nil {
		ctx = context.Background()
	}
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), testControlShutdownTimeout)
	shutdownErr := transport.server.Shutdown(shutdownCtx)
	cancel()
	if shutdownErr != nil {
		closeErr := transport.server.Close()
		if errors.Is(closeErr, http.ErrServerClosed) {
			closeErr = nil
		}
		return errors.Join(shutdownErr, closeErr, waitTestControlDrain(drained))
	}
	return waitTestControlDrain(drained)
}

func waitTestControlDrain(drained <-chan struct{}) error {
	timer := time.NewTimer(testControlForcedDrainTimeout)
	defer timer.Stop()
	select {
	case <-drained:
		return nil
	case <-timer.C:
		return ErrControlTransportUndrained
	}
}

func (transport *testPreparedControlTransport) Close() error {
	if transport == nil {
		return nil
	}
	transport.closeOnce.Do(func() {
		if transport.listener != nil {
			transport.closeErr = transport.listener.Close()
		}
	})
	return transport.closeErr
}

type testLocalControlService struct{ next ManagedControlService }

func (adapter testLocalControlService) HookCheck(ctx context.Context,
	metadata localapi.RequestMetadata, _ localapi.HookCheckRequest,
) (localapi.HookCheckResponse, *localapi.APIError) {
	response, controlErr := adapter.next.HookCheck(ctx, testControlMetadata(metadata))
	if controlErr != nil {
		return localapi.HookCheckResponse{}, testControlAPIError(controlErr)
	}
	return localapi.HookCheckResponse{SchemaVersion: localapi.SchemaVersion,
		Pending: response.Pending}, nil
}

func (adapter testLocalControlService) AgentCurrent(ctx context.Context,
	metadata localapi.RequestMetadata, _ localapi.AgentCurrentRequest,
) (localapi.AgentCurrentResponse, *localapi.APIError) {
	response, controlErr := adapter.next.AgentCurrent(ctx, testControlMetadata(metadata))
	if controlErr != nil {
		return localapi.AgentCurrentResponse{}, testControlAPIError(controlErr)
	}
	return localapi.AgentCurrentResponse{SchemaVersion: localapi.SchemaVersion,
		Status: response.Status, RunID: response.RunID, ClaimSecret: response.ClaimSecret,
		Projection: append(response.Projection[:0:0], response.Projection...)}, nil
}

func (adapter testLocalControlService) TeamworkAction(ctx context.Context,
	metadata localapi.RequestMetadata, request localapi.TeamworkActionRequest,
) (localapi.OperationResponse, *localapi.APIError) {
	response, controlErr := adapter.next.TeamworkAction(ctx, testControlMetadata(metadata),
		TeamworkActionRequest{Action: request.Action, Channel: request.Channel, To: request.To,
			Deadline: request.Deadline, Content: request.Content,
			Artifacts: append([]string(nil), request.Artifacts...)})
	if controlErr != nil {
		return localapi.OperationResponse{}, testControlAPIError(controlErr)
	}
	return testControlOperationResponse(response), nil
}

func (adapter testLocalControlService) AgentResolve(ctx context.Context,
	metadata localapi.RequestMetadata, request localapi.AgentResolveRequest,
) (localapi.OperationResponse, *localapi.APIError) {
	response, controlErr := adapter.next.AgentResolve(ctx, testControlMetadata(metadata),
		AgentResolveRequest{Decision: request.Decision, Content: request.Content})
	if controlErr != nil {
		return localapi.OperationResponse{}, testControlAPIError(controlErr)
	}
	return testControlOperationResponse(response), nil
}

func testControlMetadata(metadata localapi.RequestMetadata) ControlMetadata {
	return ControlMetadata{Profile: metadata.Profile, OperationKeyHash: metadata.OperationKeyHash,
		HasOperationKey: metadata.HasOperationKey, ClaimContextHash: metadata.ClaimContextHash,
		HasClaimContext: metadata.HasClaimContext, RunAttachmentHash: metadata.RunAttachmentHash,
		HasRunAttachment: metadata.HasRunAttachment}
}

func testControlOperationResponse(response OperationResponse) localapi.OperationResponse {
	return localapi.OperationResponse{SchemaVersion: response.SchemaVersion,
		Status: response.Status, Action: response.Action, OperationID: response.OperationID,
		Replayed: response.Replayed, Handling: testControlHandlingReceipt(response.Handling),
		Results: testControlOperationResults(response.Results), Receipt: response.Receipt}
}

func testControlHandlingReceipt(receipt *HandlingReceipt) *localapi.HandlingReceipt {
	if receipt == nil {
		return nil
	}
	return &localapi.HandlingReceipt{Status: receipt.Status}
}

func testControlOperationResults(results []OperationResult) []localapi.OperationResult {
	if results == nil {
		return nil
	}
	converted := make([]localapi.OperationResult, len(results))
	for index, result := range results {
		converted[index] = localapi.OperationResult{EventID: result.EventID,
			EventType: result.EventType, Work: localapi.WorkReceipt{Ref: result.Work.Ref,
				Version: result.Work.Version, State: result.Work.State}}
	}
	return converted
}

type testLocalControlHealth struct{ observer ControlObserver }

func (provider testLocalControlHealth) Health(ctx context.Context,
	metadata localapi.RequestMetadata,
) (localapi.HealthSnapshot, *localapi.APIError) {
	health, controlErr := provider.observer.ObserveControlHealth(ctx, metadata.Profile)
	if controlErr != nil {
		return localapi.HealthSnapshot{}, testControlAPIError(controlErr)
	}
	return localapi.HealthSnapshot{AssetRevision: health.AssetRevision,
		WorkersReady: health.Ready}, nil
}

type testLocalControlStatus struct{ observer ControlObserver }

func (provider testLocalControlStatus) Status(ctx context.Context,
	metadata localapi.RequestMetadata,
) (localapi.StatusSnapshot, *localapi.APIError) {
	status, controlErr := provider.observer.ObserveControlStatus(ctx, metadata.Profile)
	if controlErr != nil {
		return localapi.StatusSnapshot{}, testControlAPIError(controlErr)
	}
	return localapi.StatusSnapshot{AssetRevision: status.AssetRevision,
		ActivationReady: status.ActivationReady, ActivationIssue: status.ActivationIssue,
		Runtime: localapi.RuntimeStatusSnapshot{Running: status.Runtime.Running,
			Ready: status.Runtime.Ready, Healthy: status.Runtime.Healthy,
			Recovering: status.Runtime.Recovering, Issue: status.Runtime.Issue}}, nil
}

type testLocalControlAuthority struct{ observer ControlObserver }

func (provider testLocalControlAuthority) Authority(ctx context.Context,
	metadata localapi.RequestMetadata,
) (localapi.AuthoritySnapshot, *localapi.APIError) {
	authority, controlErr := provider.observer.ObserveControlAuthority(ctx, metadata.Profile)
	if controlErr != nil {
		return localapi.AuthoritySnapshot{}, testControlAPIError(controlErr)
	}
	return testControlAuthoritySnapshot(authority)
}

type testLocalControlMutation struct{ controller MutationShutdownController }

func (preparer testLocalControlMutation) PrepareMutationShutdown(ctx context.Context,
	metadata localapi.RequestMetadata,
) (localapi.AuthoritySnapshot, localapi.AdmissionReleaseFunc, *localapi.APIError) {
	authority, release, controlErr := preparer.controller.PrepareMutationShutdown(ctx, metadata.Profile)
	if controlErr != nil {
		return localapi.AuthoritySnapshot{}, nil, testControlAPIError(controlErr)
	}
	if release == nil {
		return localapi.AuthoritySnapshot{}, nil,
			localapi.NewAPIError(localapi.CodeInternal, "mutation shutdown release is unavailable")
	}
	snapshot, apiErr := testControlAuthoritySnapshot(authority)
	if apiErr != nil {
		release()
		return localapi.AuthoritySnapshot{}, nil, apiErr
	}
	return snapshot, localapi.AdmissionReleaseFunc(release), nil
}

func testControlAuthoritySnapshot(authority Authority) (localapi.AuthoritySnapshot, *localapi.APIError) {
	if err := authority.Validate(); err != nil {
		return localapi.AuthoritySnapshot{},
			localapi.NewAPIError(localapi.CodeInternal, "durable authority is invalid")
	}
	return localapi.AuthoritySnapshot{Host: authority.Host, Runtime: authority.Runtime,
		Enabled: authority.Enabled, AssetRevision: authority.AssetRevision,
		UpdatedAt: authority.UpdatedAt, PeerID: authority.PeerID,
		ActiveAssetRevision: authority.ActiveAssetRevision}, nil
}

func testControlAPIError(controlErr *ControlError) *localapi.APIError {
	if controlErr == nil {
		return nil
	}
	code, known := testControlErrorCode(controlErr.Code)
	if !known || controlErr.Retryable != controlErr.Code.Retryable() ||
		controlErr.Replayed && controlErr.OperationID == nil {
		return localapi.NewAPIError(localapi.CodeInternal, "internal control error")
	}
	if controlErr.OperationID != nil {
		if _, err := model.ParseOperationID(*controlErr.OperationID); err != nil {
			return localapi.NewAPIError(localapi.CodeInternal, "internal control error")
		}
	}
	mapped := localapi.NewAPIError(code, controlErr.Message)
	mapped.Replayed = controlErr.Replayed
	if controlErr.OperationID != nil {
		operationID := *controlErr.OperationID
		mapped.OperationID = &operationID
	}
	return mapped
}

func testControlErrorCode(code ControlErrorCode) (localapi.ErrorCode, bool) {
	for _, mapping := range [...]struct {
		domain    ControlErrorCode
		transport localapi.ErrorCode
	}{
		{ControlCodeInvalidArgument, localapi.CodeInvalidArgument},
		{ControlCodeContentRequired, localapi.CodeContentRequired},
		{ControlCodeContentTooLarge, localapi.CodeContentTooLarge},
		{ControlCodeArtifactInvalid, localapi.CodeArtifactInvalid},
		{ControlCodeArtifactTooLarge, localapi.CodeArtifactTooLarge},
		{ControlCodeAmbiguousChannel, localapi.CodeAmbiguousChannel},
		{ControlCodeAmbiguousParticipant, localapi.CodeAmbiguousParticipant},
		{ControlCodeUnknownAction, localapi.CodeUnknownAction},
		{ControlCodeAuthenticationFailed, localapi.CodeAuthenticationFailed},
		{ControlCodeContextRequired, localapi.CodeContextRequired},
		{ControlCodeContextInvalid, localapi.CodeContextInvalid},
		{ControlCodeContextStale, localapi.CodeContextStale},
		{ControlCodeAssetRevisionMismatch, localapi.CodeAssetRevisionMismatch},
		{ControlCodeActionNotAllowed, localapi.CodeActionNotAllowed},
		{ControlCodeCurrentTooLarge, localapi.CodeCurrentTooLarge},
		{ControlCodeOperationMismatch, localapi.CodeOperationMismatch},
		{ControlCodeWorkConflict, localapi.CodeWorkConflict},
		{ControlCodeWorkExpired, localapi.CodeWorkExpired},
		{ControlCodeProfileHostMismatch, localapi.CodeProfileHostMismatch},
		{ControlCodeOperationPending, localapi.CodeOperationPending},
		{ControlCodePeerUnavailable, localapi.CodePeerUnavailable},
		{ControlCodeMnemondUnavailable, localapi.CodeMnemondUnavailable},
		{ControlCodeInternal, localapi.CodeInternal},
	} {
		if mapping.domain == code {
			return mapping.transport, true
		}
	}
	return localapi.CodeInternal, false
}

type testControlRequestTracker struct {
	next      http.Handler
	mu        sync.Mutex
	accepting bool
	active    uint64
	drained   chan struct{}
}

func newTestControlRequestTracker(next http.Handler) *testControlRequestTracker {
	return &testControlRequestTracker{next: next, accepting: true, drained: make(chan struct{})}
}

func (tracker *testControlRequestTracker) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	tracker.mu.Lock()
	if !tracker.accepting {
		tracker.mu.Unlock()
		http.Error(writer, "mnemond is stopping", http.StatusServiceUnavailable)
		return
	}
	tracker.active++
	tracker.mu.Unlock()
	defer func() {
		tracker.mu.Lock()
		tracker.active--
		if !tracker.accepting && tracker.active == 0 {
			close(tracker.drained)
		}
		tracker.mu.Unlock()
	}()
	tracker.next.ServeHTTP(writer, request)
}

func (tracker *testControlRequestTracker) seal() <-chan struct{} {
	tracker.mu.Lock()
	if tracker.accepting {
		tracker.accepting = false
		if tracker.active == 0 {
			close(tracker.drained)
		}
	}
	drained := tracker.drained
	tracker.mu.Unlock()
	return drained
}

type testControlAdmissionListener struct {
	net.Listener
	permits   chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func newTestControlAdmissionListener(listener net.Listener,
	limit uint32,
) (*testControlAdmissionListener, error) {
	if listener == nil || limit == 0 {
		return nil, errors.New("test control listener requires a finite connection limit")
	}
	return &testControlAdmissionListener{Listener: listener, permits: make(chan struct{}, limit),
		closed: make(chan struct{})}, nil
}

func (listener *testControlAdmissionListener) Accept() (net.Conn, error) {
	if listener == nil || listener.Listener == nil {
		return nil, net.ErrClosed
	}
	select {
	case listener.permits <- struct{}{}:
	case <-listener.closed:
		return nil, net.ErrClosed
	}
	connection, err := listener.Listener.Accept()
	if err != nil {
		<-listener.permits
		return nil, err
	}
	return &testControlAdmittedConnection{Conn: connection,
		release: func() { <-listener.permits }}, nil
}

func (listener *testControlAdmissionListener) Close() error {
	if listener == nil || listener.Listener == nil {
		return nil
	}
	listener.closeOnce.Do(func() {
		close(listener.closed)
		listener.closeErr = listener.Listener.Close()
	})
	return listener.closeErr
}

type testControlAdmittedConnection struct {
	net.Conn
	release   func()
	closeOnce sync.Once
	closeErr  error
}

func (connection *testControlAdmittedConnection) Close() error {
	if connection == nil || connection.Conn == nil {
		return nil
	}
	connection.closeOnce.Do(func() {
		connection.closeErr = connection.Conn.Close()
		connection.release()
	})
	return connection.closeErr
}

func TestControlTransportFactoryFuncRejectsNil(t *testing.T) {
	t.Parallel()
	var factory ControlTransportFactoryFunc
	transport, err := factory.Prepare(context.Background(), ControlTransportOptions{}, ControlBindings{})
	if err == nil || transport != nil {
		t.Fatalf("nil factory = (%#v, %v)", transport, err)
	}
}

var _ ControlTransportFactory = testLocalControlTransportFactory{}
var _ PreparedControlTransport = (*testPreparedControlTransport)(nil)
var _ localapi.Service = testLocalControlService{}
