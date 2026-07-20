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
// does not by itself prove that a future EnrollInit matches the Store-prepared
// request; the unified Channel enrollment coordinator must perform that
// binding before this primitive is integrated into its client path.
type enrollmentTransportPermit struct {
	runtime *MeshRuntime
	token   outboundEnrollmentPermitToken

	closeOnce sync.Once
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
	token, err := nodeHost.gater.acquireOutboundEnrollmentPermit(ctx, spec,
		runtime.retireEnrollmentTransportPermit)
	if err != nil {
		return nil, fmt.Errorf("%w: acquire enrollment transport: %w", ErrMeshRuntime, err)
	}
	if err := addressSources.addPermit(token); err != nil {
		nodeHost.gater.releaseOutboundEnrollmentPermit(token)
		return nil, fmt.Errorf("%w: install enrollment addresses: %w", ErrMeshRuntime, err)
	}
	if !nodeHost.gater.outboundEnrollmentPermitCurrent(token) {
		addressSources.removePermit(token.ref())
		return nil, fmt.Errorf("%w: enrollment transport expired during installation", ErrMeshRuntime)
	}
	runtime.mu.Lock()
	closed := runtime.closed
	runtime.mu.Unlock()
	if closed {
		nodeHost.gater.releaseOutboundEnrollmentPermit(token)
		return nil, fmt.Errorf("%w: runtime closed during enrollment transport installation", ErrMeshRuntime)
	}
	return &enrollmentTransportPermit{runtime: runtime, token: token}, nil
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
) {
	if runtime == nil || runtime.addressSources == nil {
		return
	}
	runtime.addressSources.removePermit(ref)
	if runtime.nodeHost != nil {
		// The gater has already removed the exception. Reconciliation therefore
		// retains a promoted durable binding but closes an otherwise-authorityless
		// exact enrollment connection. Managed stream gates remain fail-closed
		// even if the transport reports a best-effort close error.
		if resetErr != nil {
			resetErr = fmt.Errorf("reset exact enrollment stream: %w", resetErr)
		}
		reconcileErr := runtime.nodeHost.ReconcileConnections()
		if resetErr != nil || reconcileErr != nil {
			runtime.failClosedEnrollmentTransport(errors.Join(resetErr, reconcileErr))
		}
	}
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
	permit.closeOnce.Do(func() {
		if permit.runtime != nil && permit.runtime.nodeHost != nil {
			permit.runtime.nodeHost.gater.releaseOutboundEnrollmentPermit(permit.token)
		}
	})
	return nil
}
