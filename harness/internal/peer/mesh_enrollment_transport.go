package peer

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// enrollmentTransportPermit is an internal, single-stream transport fence. It
// does not by itself prove that a future EnrollInit matches the Node-prepared
// request; MeshRuntime.JoinChannel owns that higher-level binding.
type enrollmentTransportPermit struct {
	runtime *MeshRuntime
	token   outboundEnrollmentPermitToken

	closeOnce  sync.Once
	closeDone  chan struct{}
	retireOnce sync.Once
	retired    chan struct{}
	resultMu   sync.Mutex
	retireErr  error
	closeErr   error
}

type enrollmentTransportPermitRequest struct {
	Token               model.EnrollmentToken
	EnrollmentRequestID model.EnrollmentRequestID
}

func (runtime *MeshRuntime) acquireEnrollmentTransportPermit(ctx context.Context,
	request enrollmentTransportPermitRequest,
) (*enrollmentTransportPermit, error) {
	if runtime == nil || ctx == nil || ctx.Err() != nil {
		return nil, fmt.Errorf("%w: live runtime and context are required", ErrMeshRuntime)
	}
	spec, err := enrollmentTransportPermitSpecFromRequest(request)
	if err != nil {
		return nil, err
	}
	runtime.mu.Lock()
	if runtime.closed || runtime.nodeHost == nil || runtime.addressSources == nil {
		runtime.mu.Unlock()
		return nil, fmt.Errorf("%w: runtime is closed", ErrMeshRuntime)
	}
	nodeHost := runtime.nodeHost
	addressSources := runtime.addressSources
	runtime.mu.Unlock()
	permit := &enrollmentTransportPermit{runtime: runtime, closeDone: make(chan struct{}),
		retired: make(chan struct{})}
	token, err := nodeHost.gater.acquireOutboundEnrollmentPermit(ctx, spec, permit.retire)
	if err != nil {
		return nil, fmt.Errorf("%w: acquire enrollment transport: %w", ErrMeshRuntime, err)
	}
	permit.token = token
	if err := addressSources.addPermit(token); err != nil {
		cleanupErr := permit.Close()
		return nil, errors.Join(fmt.Errorf("%w: install enrollment addresses: %w",
			ErrMeshRuntime, err), cleanupErr)
	}
	if !nodeHost.gater.outboundEnrollmentPermitCurrent(token) {
		addressSources.removePermit(token.ref())
		cleanupErr := permit.Close()
		return nil, errors.Join(fmt.Errorf("%w: enrollment transport expired during installation",
			ErrMeshRuntime), cleanupErr)
	}
	runtime.mu.Lock()
	closed := runtime.closed
	runtime.mu.Unlock()
	if closed {
		cleanupErr := permit.Close()
		return nil, errors.Join(fmt.Errorf("%w: runtime closed during enrollment transport installation",
			ErrMeshRuntime), cleanupErr)
	}
	return permit, nil
}

func enrollmentTransportPermitSpecFromRequest(request enrollmentTransportPermitRequest) (
	enrollmentTransportPermitSpec, error,
) {
	if request.EnrollmentRequestID.IsZero() || model.VerifyEnrollmentToken(request.Token) != nil {
		return enrollmentTransportPermitSpec{},
			fmt.Errorf("%w: verified token and stable request identity are required", ErrMeshRuntime)
	}
	payload := request.Token.Payload()
	descriptor := payload.Descriptor().Descriptor()
	return enrollmentTransportPermitSpec{OwnerPeerID: descriptor.OwnerPeerID(),
		OwnerMultiaddrs: payload.OwnerMultiaddrs(), ChannelID: descriptor.ID(),
		GrantID: payload.GrantID(), EnrollmentRequestID: request.EnrollmentRequestID}, nil
}

func (runtime *MeshRuntime) retireEnrollmentTransportPermit(ref outboundEnrollmentPermitRef,
	resetErr error,
) error {
	if runtime == nil || runtime.addressSources == nil {
		return fmt.Errorf("%w: enrollment transport owner is unavailable", ErrMeshRuntime)
	}
	runtime.addressSources.removePermit(ref)
	if runtime.nodeHost == nil {
		return fmt.Errorf("%w: enrollment Host is unavailable", ErrMeshRuntime)
	}
	// The gater has already removed the exception. Reconciliation therefore
	// retains a promoted durable binding but closes an otherwise-authorityless
	// exact enrollment connection. Permit retirement is the sole exact-stream
	// reset owner; callers must observe any failure before reporting success.
	if resetErr != nil {
		resetErr = fmt.Errorf("reset exact enrollment stream: %w", resetErr)
	}
	reconcileErr := runtime.nodeHost.ReconcileConnections()
	result := errors.Join(resetErr, reconcileErr)
	if result != nil {
		runtime.failClosedEnrollmentTransport(result)
	}
	return result
}

func (permit *enrollmentTransportPermit) retire(ref outboundEnrollmentPermitRef, resetErr error) {
	if permit == nil {
		return
	}
	permit.retireOnce.Do(func() {
		result := permit.runtime.retireEnrollmentTransportPermit(ref, resetErr)
		permit.resultMu.Lock()
		permit.retireErr = result
		permit.resultMu.Unlock()
		close(permit.retired)
	})
}

// openEnrollmentStream is package-private so only the reviewed Channel client
// can turn this capability into transport. Callers cannot select another Peer,
// protocol, frame version, or address set.
func (runtime *MeshRuntime) openEnrollmentStream(ctx context.Context,
	permit *enrollmentTransportPermit,
) (network.Stream, error) {
	if runtime == nil || permit == nil || permit.runtime != runtime {
		return nil, fmt.Errorf("%w: enrollment transport capability is unavailable", ErrMeshRuntime)
	}
	runtime.mu.Lock()
	if runtime.closed || runtime.nodeHost == nil {
		runtime.mu.Unlock()
		return nil, fmt.Errorf("%w: runtime is closed", ErrMeshRuntime)
	}
	nodeHost := runtime.nodeHost
	runtime.mu.Unlock()
	stream, err := nodeHost.openEnrollmentStream(ctx, permit.token)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrMeshRuntime, err)
	}
	return stream, nil
}

func (permit *enrollmentTransportPermit) Close() error {
	if permit == nil {
		return nil
	}
	if permit.closeDone == nil || permit.retired == nil {
		return fmt.Errorf("%w: enrollment transport capability is incomplete", ErrMeshRuntime)
	}
	permit.closeOnce.Do(func() {
		defer close(permit.closeDone)
		if permit.runtime == nil || permit.runtime.nodeHost == nil ||
			permit.runtime.nodeHost.gater == nil || permit.token.generation == 0 {
			permit.closeErr = fmt.Errorf("%w: enrollment transport capability is incomplete",
				ErrMeshRuntime)
			return
		}
		permit.runtime.nodeHost.gater.releaseOutboundEnrollmentPermit(permit.token)
		<-permit.retired
		permit.resultMu.Lock()
		permit.closeErr = permit.retireErr
		permit.resultMu.Unlock()
	})
	<-permit.closeDone
	return permit.closeErr
}
