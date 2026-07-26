package peer

import (
	"context"
	"fmt"
)

func (server *ArtifactServer) Close() error {
	return server.CloseContext(context.Background())
}

// CloseContext seals handler admission, broadcasts cancellation and waits for
// every handler admitted before the seal. A caller may retry the wait with a
// later context after an earlier deadline expires.
func (server *ArtifactServer) CloseContext(ctx context.Context) error {
	if server == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: shutdown context is unavailable", ErrArtifactServer)
	}
	server.closeOnce.Do(func() {
		artifactServerConstructorMu.Lock()
		server.mu.Lock()
		server.closed = true
		if server.cancel != nil {
			server.cancel()
		}
		if server.host != nil {
			server.host.RemoveStreamHandler(ArtifactsProtocol)
		}
		server.mu.Unlock()
		artifactServerConstructorMu.Unlock()
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
		return fmt.Errorf("%w: drain active handlers: %w", ErrArtifactServer, ctx.Err())
	}
}
