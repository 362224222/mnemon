package peer

import (
	"context"
	"fmt"
)

func (server *AgencyServer) Close() error {
	return server.CloseContext(context.Background())
}

// CloseContext seals both protocol handlers, cancels every admitted callback,
// and joins the handlers within the caller's bound. A timed-out caller may
// retry the wait; shutdown itself is started exactly once.
func (server *AgencyServer) CloseContext(ctx context.Context) error {
	if server == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: shutdown context is unavailable", ErrAgencyServer)
	}
	server.closeOnce.Do(func() {
		agencyServerConstructorMu.Lock()
		server.mu.Lock()
		server.closed = true
		server.cancel()
		server.host.RemoveStreamHandler(AgencyDeliveryProtocol)
		server.host.RemoveStreamHandler(AgencyObjectProtocol)
		server.mu.Unlock()
		agencyServerConstructorMu.Unlock()
	})
	server.mu.Lock()
	drained := server.handlersDone
	server.mu.Unlock()
	if drained == nil {
		return nil
	}
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%w: drain active handlers: %w", ErrAgencyServer, ctx.Err())
	}
}
