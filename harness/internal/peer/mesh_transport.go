package peer

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/libp2p/go-libp2p/core/host"
)

var ErrMeshTransport = errors.New("Mnemon managed mesh transport")

// MeshTransportOptions omits the one frozen Host so it cannot escape the package.
type MeshTransportOptions struct {
	Enrollment    ChannelEnrollmentOwnerOptions
	Member        ChannelMemberServiceOptions
	EventSource   EventSourceStore
	EventClock    EventServerClock
	ArtifactStore ArtifactStoreSource
	ArtifactCAS   ArtifactCAS
}

type meshTransportState uint8

const (
	meshTransportPrepared meshTransportState = iota + 1
	meshTransportStarting
	meshTransportRunning
	meshTransportStopping
	meshTransportStopped
)

// MeshTransport solely owns the direct transports on one managed Host.
type MeshTransport struct {
	runtime *MeshRuntime
	host    host.Host

	enrollment     *ChannelEnrollmentOwner
	member         *ChannelMemberService
	memberClient   *ChannelMemberClient
	eventClient    *EventClient
	artifactClient *ArtifactClient
	eventSource    EventSourceStore
	eventClock     EventServerClock
	artifactSource ArtifactServerSource
	artifactCAS    ArtifactCAS
	mu             sync.Mutex
	state          meshTransportState
	runCtx         context.Context
	runCancel      context.CancelFunc
	ready          chan struct{}
	done           chan struct{}
	runErr         error
	closeErr       error
	active         sync.WaitGroup
	readyOnce      sync.Once
	doneOnce       sync.Once
}

func NewMeshTransport(runtime *MeshRuntime, options MeshTransportOptions) (*MeshTransport, error) {
	nodeHost, err := meshTransportHost(runtime)
	if err != nil {
		return nil, err
	}
	if isNilEventSourceStore(options.EventSource) ||
		isNilArtifactStoreSource(options.ArtifactStore) || isNilArtifactCAS(options.ArtifactCAS) {
		return nil, fmt.Errorf("%w: Event source, Artifact Store and CAS are required",
			ErrMeshTransport)
	}
	artifactSource, err := NewArtifactServerStoreSource(options.ArtifactStore)
	if err != nil {
		return nil, fmt.Errorf("%w: prepare Artifact source: %w", ErrMeshTransport, err)
	}
	enrollment, err := NewChannelEnrollmentOwner(options.Enrollment)
	if err != nil {
		return nil, fmt.Errorf("%w: prepare Channel enrollment: %w", ErrMeshTransport, err)
	}
	member, err := NewChannelMemberService(options.Member)
	if err != nil {
		return nil, fmt.Errorf("%w: prepare Channel member service: %w", ErrMeshTransport, err)
	}
	memberClient, err := NewChannelMemberClient(ChannelMemberClientOptions{Host: nodeHost})
	if err != nil {
		return nil, fmt.Errorf("%w: prepare Channel member client: %w", ErrMeshTransport, err)
	}
	eventClient, err := NewEventClient(EventClientOptions{Host: nodeHost})
	if err != nil {
		return nil, fmt.Errorf("%w: prepare Event client: %w", ErrMeshTransport, err)
	}
	artifactClient, err := NewArtifactClient(ArtifactClientOptions{Host: nodeHost})
	if err != nil {
		return nil, fmt.Errorf("%w: prepare Artifact client: %w", ErrMeshTransport, err)
	}
	return &MeshTransport{runtime: runtime, host: nodeHost, enrollment: enrollment,
		member: member, memberClient: memberClient, eventClient: eventClient,
		artifactClient: artifactClient, eventSource: options.EventSource,
		eventClock: options.EventClock, artifactSource: artifactSource,
		artifactCAS: options.ArtifactCAS, state: meshTransportPrepared,
		ready: make(chan struct{}), done: make(chan struct{})}, nil
}

func meshTransportHost(runtime *MeshRuntime) (host.Host, error) {
	if runtime == nil {
		return nil, fmt.Errorf("%w: frozen MeshRuntime is required", ErrMeshTransport)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	nodeHost := runtime.managedRuntimeHost()
	if runtime.closed || nodeHost == nil || nodeHost.ID() == "" {
		return nil, fmt.Errorf("%w: MeshRuntime is closed", ErrMeshTransport)
	}
	return nodeHost, nil
}

func (transport *MeshTransport) Run(ctx context.Context) error {
	runCtx, err := transport.claimRun(ctx)
	if err != nil {
		return err
	}
	dispatcher, eventServer, artifactServer, err := transport.startServers(runCtx)
	if err != nil {
		err = errors.Join(err, transport.terminalCauseNow())
		transport.finishRun(err)
		return err
	}
	if !transport.publishReady() {
		err = errors.Join(transport.waitStopCause(runCtx),
			closeMeshTransportServers(dispatcher, eventServer, artifactServer))
		transport.finishRun(err)
		return err
	}
	cause := transport.waitStopCause(runCtx)
	_, fenceErr := transport.fenceAdmission(false)
	err = errors.Join(cause, fenceErr,
		closeMeshTransportServers(dispatcher, eventServer, artifactServer))
	transport.active.Wait()
	transport.finishRun(err)
	return err
}

func (transport *MeshTransport) claimRun(ctx context.Context) (context.Context, error) {
	if transport == nil || ctx == nil || ctx.Err() != nil {
		return nil, fmt.Errorf("%w: live Run context is required", ErrMeshTransport)
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if transport.state != meshTransportPrepared {
		return nil, fmt.Errorf("%w: Run ownership was already consumed", ErrMeshTransport)
	}
	if !transport.runtimeLive() {
		return nil, errors.Join(fmt.Errorf("%w: MeshRuntime is unavailable", ErrMeshTransport),
			transport.terminalCauseNow())
	}
	runCtx, cancel := context.WithCancel(ctx)
	transport.state, transport.runCtx, transport.runCancel = meshTransportStarting, runCtx, cancel
	return runCtx, nil
}

func (transport *MeshTransport) startServers(ctx context.Context) (
	*ChannelDispatcher, *EventServer, *ArtifactServer, error,
) {
	dispatcher, err := NewChannelDispatcher(ctx, transport.host, ChannelDispatcherOptions{
		Enrollment: transport.enrollment, Member: transport.member})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("%w: start Channel dispatcher: %w", ErrMeshTransport, err)
	}
	eventServer, err := NewEventServer(ctx, EventServerOptions{Host: transport.host,
		Source: transport.eventSource, Clock: transport.eventClock})
	if err != nil {
		return nil, nil, nil, errors.Join(
			fmt.Errorf("%w: start Event server: %w", ErrMeshTransport, err), dispatcher.Close())
	}
	artifactServer, err := NewArtifactServer(ctx, ArtifactServerOptions{Host: transport.host,
		Source: transport.artifactSource, CAS: transport.artifactCAS})
	if err != nil {
		return nil, nil, nil, errors.Join(
			fmt.Errorf("%w: start Artifact server: %w", ErrMeshTransport, err),
			eventServer.Close(), dispatcher.Close())
	}
	return dispatcher, eventServer, artifactServer, nil
}

func (transport *MeshTransport) publishReady() bool {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if transport.state != meshTransportStarting || transport.runCtx.Err() != nil ||
		!transport.runtimeLive() {
		return false
	}
	transport.state = meshTransportRunning
	transport.readyOnce.Do(func() { close(transport.ready) })
	return true
}

func (transport *MeshTransport) waitStopCause(ctx context.Context) error {
	if cause := transport.terminalCauseNow(); cause != nil {
		return cause
	}
	select {
	case <-transport.runtime.terminalSignal():
		return transport.runtime.terminalError()
	case <-ctx.Done():
		return transport.terminalCauseNow()
	}
}

func (transport *MeshTransport) terminalCauseNow() error {
	select {
	case <-transport.runtime.terminalSignal():
		return transport.runtime.terminalError()
	default:
		return nil
	}
}

func closeMeshTransportServers(dispatcher *ChannelDispatcher, eventServer *EventServer,
	artifactServer *ArtifactServer,
) error {
	var result error
	if artifactServer != nil {
		result = errors.Join(result, artifactServer.Close())
	}
	if eventServer != nil {
		result = errors.Join(result, eventServer.Close())
	}
	if dispatcher != nil {
		result = errors.Join(result, dispatcher.Close())
	}
	return result
}

func (transport *MeshTransport) finishRun(runErr error) {
	transport.mu.Lock()
	if transport.runCancel != nil {
		transport.runCancel()
		transport.runCancel = nil
	}
	transport.state = meshTransportStopped
	transport.runErr = runErr
	transport.closeErr = errors.Join(transport.closeErr, runErr)
	transport.readyOnce.Do(func() { close(transport.ready) })
	transport.doneOnce.Do(func() { close(transport.done) })
	transport.mu.Unlock()
}

func (transport *MeshTransport) Readiness(ctx context.Context) error {
	if transport == nil || ctx == nil {
		return fmt.Errorf("%w: readiness context is required", ErrMeshTransport)
	}
	select {
	case <-transport.ready:
	case <-ctx.Done():
		return ctx.Err()
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if transport.state != meshTransportRunning || transport.runCtx == nil ||
		transport.runCtx.Err() != nil || !transport.runtimeLive() {
		return errors.Join(fmt.Errorf("%w: transport is not ready", ErrMeshTransport),
			transport.runErr)
	}
	return nil
}

func (transport *MeshTransport) Close() error {
	if transport == nil {
		return nil
	}
	done, err := transport.fenceAdmission(true)
	if err != nil {
		return err
	}
	<-done
	transport.mu.Lock()
	defer transport.mu.Unlock()
	return transport.closeErr
}

func (transport *MeshTransport) fenceAdmission(closePrepared bool) (<-chan struct{}, error) {
	transport.mu.Lock()
	if transport.done == nil {
		transport.mu.Unlock()
		return nil, fmt.Errorf("%w: transport is incomplete", ErrMeshTransport)
	}
	var cancel context.CancelFunc
	switch transport.state {
	case meshTransportPrepared:
		if closePrepared {
			transport.state = meshTransportStopped
			transport.readyOnce.Do(func() { close(transport.ready) })
			transport.doneOnce.Do(func() { close(transport.done) })
		}
	case meshTransportStarting, meshTransportRunning:
		transport.state = meshTransportStopping
		cancel = transport.runCancel
	}
	done := transport.done
	transport.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return done, nil
}

func (transport *MeshTransport) beginCall(ctx context.Context) (context.Context, func(), error) {
	if transport == nil || ctx == nil || ctx.Err() != nil {
		return nil, nil, fmt.Errorf("%w: live operation context is required", ErrMeshTransport)
	}
	transport.mu.Lock()
	if transport.state != meshTransportRunning || transport.runCtx == nil ||
		transport.runCtx.Err() != nil || !transport.runtimeLive() {
		transport.mu.Unlock()
		return nil, nil, fmt.Errorf("%w: transport is not ready", ErrMeshTransport)
	}
	runCtx := transport.runCtx
	transport.active.Add(1)
	transport.mu.Unlock()
	callCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(runCtx, cancel)
	return callCtx, func() {
		stop()
		cancel()
		transport.active.Done()
	}, nil
}

func (transport *MeshTransport) runtimeLive() bool {
	if transport.runtime == nil {
		return false
	}
	runtime := transport.runtime
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	managed := runtime.managedRuntimeHost()
	return !runtime.closed && managed != nil && transport.host != nil &&
		managed.ID() == transport.host.ID()
}
