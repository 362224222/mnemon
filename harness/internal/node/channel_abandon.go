package node

import (
	"context"
	"errors"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

// ChannelAbandon is the daemon-owned recovery boundary. Store installs the
// terminal durable gate first; the mesh then reconciles from that complete
// snapshot, closing only the affected topic and Channel-scoped streams.
func (manager *ChannelManager) ChannelAbandon(ctx context.Context,
	metadata localapi.RequestMetadata, request localapi.ChannelAbandonRequest,
) (localapi.ChannelAbandonResponse, *localapi.APIError) {
	if apiErr := manager.validateCall(ctx, metadata); apiErr != nil {
		return localapi.ChannelAbandonResponse{}, apiErr
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	result, err := manager.store.AbandonChannel(ctx, store.AbandonChannelSpec{
		ChannelAlias: request.Channel, ConfirmedAlias: request.ConfirmChannel,
		Force: request.Force, At: manager.clock.Now()})
	if err != nil {
		return localapi.ChannelAbandonResponse{}, channelAPIError(err)
	}
	mesh, err := manager.store.ReadChannelMeshAuthority(ctx)
	if err == nil {
		err = manager.runtime.ReconcileWithCommit(mesh, func() error { return nil })
	}
	if err != nil {
		// The Store gate already rejects new scoped work. If its runtime
		// projection cannot be installed, close the mesh so no stale protocol
		// callback survives until the supervisor restarts this Node.
		return localapi.ChannelAbandonResponse{}, channelAPIError(errors.Join(err, manager.runtime.Close()))
	}
	manager.triggerMemberReconcile()
	return localapi.ChannelAbandonResponse{SchemaVersion: localapi.SchemaVersion,
		Status: "abandoned", Channel: result.Alias, Replayed: result.Replayed,
		TransitionedAt: result.TransitionedAt.UTC().Format(time.RFC3339Nano),
		Evidence: localapi.ChannelForensicCounts{
			Bindings: result.Evidence.Bindings, Conflicts: result.Evidence.Conflicts,
			Cursors: result.Evidence.Cursors, Deliveries: result.Evidence.Deliveries,
			Events: result.Evidence.Events, Inboxes: result.Evidence.Inboxes,
			MemberRecords: result.Evidence.MemberRecords, Publications: result.Evidence.Publications,
			PullACKs: result.Evidence.PullACKs, Works: result.Evidence.Works,
		}}, nil
}
