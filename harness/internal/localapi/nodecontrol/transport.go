package nodecontrol

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

const (
	controlReadHeaderTimeout  = 5 * time.Second
	controlShutdownTimeout    = 5 * time.Second
	controlForcedDrainTimeout = time.Second
)

type Factory struct{}

func (Factory) Prepare(ctx context.Context, options node.ControlTransportOptions,
	bindings node.ControlBindings,
) (node.PreparedControlTransport, error) {
	if ctx == nil || ctx.Err() != nil || options.NodeState == "" ||
		!filepath.IsAbs(options.NodeState) || filepath.Clean(options.NodeState) != options.NodeState ||
		options.MaxConnections == 0 || nilInterface(bindings.Authenticator) ||
		nilInterface(bindings.Agent) || nilInterface(bindings.Observer) ||
		nilInterface(bindings.Mutation) || bindings.Shutdown == nil {
		return nil, errors.New("local control transport requires complete bounded Node bindings")
	}
	if _, err := model.ParseDigest(options.AssetRevision); err != nil {
		return nil, errors.New("local control transport asset revision is invalid")
	}
	service := newServiceAdapter(bindings.Agent)
	health := healthProvider{observer: bindings.Observer}
	status := statusProvider{observer: bindings.Observer}
	authority := authorityProvider{observer: bindings.Observer}
	mutation := mutationShutdownPreparer{controller: bindings.Mutation}
	server, err := localapi.NewServerWithStatusLifecycle(bindings.Authenticator, service,
		health, status, authority, localapi.LifecycleFunc(bindings.Shutdown), mutation)
	if err != nil {
		return nil, err
	}
	socketPath := filepath.Join(options.NodeState, "control.sock")
	if _, err := localapi.RemoveStaleOwnerUnix(ctx, socketPath); err != nil {
		return nil, err
	}
	listener, err := localapi.ListenOwnerUnix(socketPath)
	if err != nil {
		return nil, err
	}
	admitted, err := newConnectionAdmissionListener(listener, options.MaxConnections)
	if err != nil {
		return nil, errors.Join(err, listener.Close())
	}
	requests := newRequestTracker(server.Handler())
	return &preparedTransport{nodeState: options.NodeState, assetRevision: options.AssetRevision,
		listener: admitted, requests: requests,
		server:          &http.Server{Handler: requests, ReadHeaderTimeout: controlReadHeaderTimeout},
		shutdownTimeout: controlShutdownTimeout, forcedDrainTimeout: controlForcedDrainTimeout}, nil
}

type preparedTransport struct {
	nodeState          string
	assetRevision      string
	listener           *connectionAdmissionListener
	requests           *requestTracker
	server             *http.Server
	shutdownTimeout    time.Duration
	forcedDrainTimeout time.Duration
	closeOnce          sync.Once
	closeErr           error
}

func (transport *preparedTransport) Run(ctx context.Context) error {
	if transport == nil || transport.server == nil || transport.listener == nil || ctx == nil {
		return errors.New("local control transport is unavailable")
	}
	err := transport.server.Serve(transport.listener)
	if errors.Is(err, http.ErrServerClosed) || ctx.Err() != nil && errors.Is(err, os.ErrClosed) {
		return nil
	}
	return err
}

func (transport *preparedTransport) Readiness(ctx context.Context) error {
	if transport == nil || ctx == nil || ctx.Err() != nil {
		return errors.New("local control transport startup was cancelled")
	}
	client, err := localapi.NewClient(transport.nodeState)
	if err != nil {
		return errors.New("local control startup authority is unavailable")
	}
	probeCtx, cancel := context.WithTimeout(ctx, controlReadHeaderTimeout)
	defer cancel()
	health, apiErr := client.ProbeHealth(probeCtx)
	if apiErr != nil || health.SchemaVersion != localapi.SchemaVersion ||
		health.AssetRevision != transport.assetRevision ||
		(health.Status != "ready" && health.Status != "not_ready") {
		return errors.New("local control authenticated startup proof failed")
	}
	return nil
}

func (transport *preparedTransport) Shutdown(ctx context.Context) error {
	if transport == nil || transport.server == nil || transport.requests == nil {
		return errors.New("local control transport lifecycle is unavailable")
	}
	drained := transport.requests.seal()
	if ctx == nil {
		ctx = context.Background()
	}
	if transport.shutdownTimeout <= 0 || transport.forcedDrainTimeout <= 0 {
		return errors.Join(node.ErrControlTransportUndrained, transport.server.Close())
	}
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), transport.shutdownTimeout)
	shutdownErr := transport.server.Shutdown(shutdownCtx)
	cancel()
	if shutdownErr != nil {
		closeErr := transport.server.Close()
		if errors.Is(closeErr, http.ErrServerClosed) {
			closeErr = nil
		}
		return errors.Join(shutdownErr, closeErr,
			waitForRequestDrain(drained, transport.forcedDrainTimeout))
	}
	return waitForRequestDrain(drained, transport.forcedDrainTimeout)
}

func waitForRequestDrain(drained <-chan struct{}, timeout time.Duration) error {
	if drained == nil || timeout <= 0 {
		return node.ErrControlTransportUndrained
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-drained:
		return nil
	case <-timer.C:
		return node.ErrControlTransportUndrained
	}
}

func (transport *preparedTransport) Close() error {
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

type healthProvider struct{ observer node.ControlObserver }

func (provider healthProvider) Health(ctx context.Context,
	metadata localapi.RequestMetadata,
) (localapi.HealthSnapshot, *localapi.APIError) {
	health, controlErr := provider.observer.ObserveControlHealth(ctx, metadata.Profile)
	if controlErr != nil {
		return localapi.HealthSnapshot{}, controlAPIError(controlErr)
	}
	return localapi.HealthSnapshot{AssetRevision: health.AssetRevision,
		WorkersReady: health.Ready}, nil
}

type statusProvider struct{ observer node.ControlObserver }

func (provider statusProvider) Status(ctx context.Context,
	metadata localapi.RequestMetadata,
) (localapi.StatusSnapshot, *localapi.APIError) {
	status, controlErr := provider.observer.ObserveControlStatus(ctx, metadata.Profile)
	if controlErr != nil {
		return localapi.StatusSnapshot{}, controlAPIError(controlErr)
	}
	return localapi.StatusSnapshot{AssetRevision: status.AssetRevision,
		ActivationReady: status.ActivationReady, ActivationIssue: status.ActivationIssue,
		Runtime: localapi.RuntimeStatusSnapshot{Running: status.Runtime.Running,
			Ready: status.Runtime.Ready, Healthy: status.Runtime.Healthy,
			Recovering: status.Runtime.Recovering, Issue: status.Runtime.Issue}}, nil
}

type authorityProvider struct{ observer node.ControlObserver }

func (provider authorityProvider) Authority(ctx context.Context,
	metadata localapi.RequestMetadata,
) (localapi.AuthoritySnapshot, *localapi.APIError) {
	authority, controlErr := provider.observer.ObserveControlAuthority(ctx, metadata.Profile)
	if controlErr != nil {
		return localapi.AuthoritySnapshot{}, controlAPIError(controlErr)
	}
	return authoritySnapshot(authority)
}

type mutationShutdownPreparer struct {
	controller node.MutationShutdownController
}

func (preparer mutationShutdownPreparer) PrepareMutationShutdown(ctx context.Context,
	metadata localapi.RequestMetadata,
) (localapi.AuthoritySnapshot, localapi.AdmissionReleaseFunc, *localapi.APIError) {
	authority, release, controlErr := preparer.controller.PrepareMutationShutdown(ctx, metadata.Profile)
	if controlErr != nil {
		if release != nil {
			release()
		}
		return localapi.AuthoritySnapshot{}, nil, controlAPIError(controlErr)
	}
	if release == nil {
		return localapi.AuthoritySnapshot{}, nil,
			localapi.NewAPIError(localapi.CodeInternal, "mutation shutdown release is unavailable")
	}
	snapshot, apiErr := authoritySnapshot(authority)
	if apiErr != nil {
		release()
		return localapi.AuthoritySnapshot{}, nil, apiErr
	}
	return snapshot, localapi.AdmissionReleaseFunc(release), nil
}

func authoritySnapshot(authority node.Authority) (localapi.AuthoritySnapshot, *localapi.APIError) {
	if err := authority.Validate(); err != nil {
		return localapi.AuthoritySnapshot{}, localapi.NewAPIError(localapi.CodeInternal,
			"durable authority is invalid")
	}
	return localapi.AuthoritySnapshot{Host: authority.Host, Runtime: authority.Runtime,
		Enabled: authority.Enabled, AssetRevision: authority.AssetRevision,
		UpdatedAt: authority.UpdatedAt, PeerID: authority.PeerID,
		ActiveAssetRevision: authority.ActiveAssetRevision}, nil
}

type requestTracker struct {
	next      http.Handler
	mu        sync.Mutex
	accepting bool
	active    uint64
	drained   chan struct{}
}

func newRequestTracker(next http.Handler) *requestTracker {
	return &requestTracker{next: next, accepting: true, drained: make(chan struct{})}
}

func (tracker *requestTracker) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
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

func (tracker *requestTracker) seal() <-chan struct{} {
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

type connectionAdmissionListener struct {
	net.Listener
	permits   chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func newConnectionAdmissionListener(listener net.Listener,
	limit uint32,
) (*connectionAdmissionListener, error) {
	if listener == nil || limit == 0 {
		return nil, errors.New("local control listener requires a finite connection limit")
	}
	return &connectionAdmissionListener{Listener: listener, permits: make(chan struct{}, limit),
		closed: make(chan struct{})}, nil
}

func (listener *connectionAdmissionListener) Accept() (net.Conn, error) {
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
		listener.release()
		return nil, err
	}
	return &admittedConnection{Conn: connection, release: listener.release}, nil
}

func (listener *connectionAdmissionListener) Close() error {
	if listener == nil || listener.Listener == nil {
		return nil
	}
	listener.closeOnce.Do(func() {
		close(listener.closed)
		listener.closeErr = listener.Listener.Close()
	})
	return listener.closeErr
}

func (listener *connectionAdmissionListener) release() { <-listener.permits }

type admittedConnection struct {
	net.Conn
	release   func()
	closeOnce sync.Once
	closeErr  error
}

func (connection *admittedConnection) Close() error {
	if connection == nil || connection.Conn == nil {
		return nil
	}
	connection.closeOnce.Do(func() {
		connection.closeErr = connection.Conn.Close()
		connection.release()
	})
	return connection.closeErr
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

var _ node.ControlTransportFactory = Factory{}
var _ node.PreparedControlTransport = (*preparedTransport)(nil)
