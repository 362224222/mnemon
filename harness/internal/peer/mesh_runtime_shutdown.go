package peer

import (
	"context"
	"errors"
	"fmt"
)

func (runtime *MeshRuntime) Close() error {
	return runtime.CloseContext(context.Background())
}

// CloseContext seals the logical runtime and passes one caller deadline through
// Gossip and the libp2p Host. Repeated calls resume any drain that outlived an
// earlier deadline.
func (runtime *MeshRuntime) CloseContext(ctx context.Context) error {
	if runtime == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: shutdown context is unavailable", ErrMeshRuntime)
	}
	runtime.mu.Lock()
	if !runtime.closed {
		runtime.closed = true
		runtime.revision++
	}
	gossip, nodeHost := runtime.gossip, runtime.nodeHost
	runtime.mu.Unlock()

	var result error
	if gossip != nil {
		result = errors.Join(result, gossip.CloseContext(ctx))
	}
	if nodeHost == nil {
		return result
	}
	if contextErr := ctx.Err(); contextErr != nil {
		result = errors.Join(result,
			fmt.Errorf("%w: close mesh runtime before Host drain: %w", ErrMeshRuntime, contextErr))
	}
	return errors.Join(result, nodeHost.CloseContext(ctx))
}
