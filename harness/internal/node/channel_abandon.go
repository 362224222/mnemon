package node

import (
	"context"
	"errors"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

// ChannelAbandon is the daemon-owned recovery boundary. Store installs the
// terminal durable gate first; the mesh then reconciles from that complete
// snapshot, closing only the affected topic and Channel-scoped streams.
func (manager *ChannelManager) ChannelAbandon(ctx context.Context,
	metadata RequestMetadata, request ChannelAbandonRequest,
) (ChannelAbandonResponse, *APIError) {
	if apiErr := manager.validateCall(ctx, metadata); apiErr != nil {
		return ChannelAbandonResponse{}, apiErr
	}
	at := manager.clock.Now()
	manager.mu.Lock()
	result, err := manager.store.AbandonChannel(ctx, store.AbandonChannelSpec{
		ChannelAlias: request.Channel, ConfirmedAlias: request.ConfirmChannel,
		Force: request.Force, At: at})
	manager.mu.Unlock()
	if err != nil {
		return ChannelAbandonResponse{}, channelAPIError(err)
	}
	mesh, err := manager.store.ReadChannelMeshAuthority(ctx)
	if err == nil {
		err = manager.runtime.Reconcile(mesh)
	}
	if err != nil {
		// The Store gate already rejects new scoped work. If its runtime
		// projection cannot be installed, close the mesh so no stale protocol
		// callback survives until the supervisor restarts this Node.
		return ChannelAbandonResponse{}, channelAPIError(errors.Join(err, manager.runtime.Close()))
	}
	manager.triggerMemberReconcileChannel(mesh, result.ChannelID)
	return ChannelAbandonResponse{SchemaVersion: SchemaVersion,
		Status: "abandoned", Channel: result.Alias, Replayed: result.Replayed,
		TransitionedAt: result.TransitionedAt.UTC().Format(time.RFC3339Nano),
		Evidence: ChannelForensicCounts{
			Bindings: result.Evidence.Bindings, Conflicts: result.Evidence.Conflicts,
			Cursors: result.Evidence.Cursors, Deliveries: result.Evidence.Deliveries,
			Events: result.Evidence.Events, Inboxes: result.Evidence.Inboxes,
			MemberRecords: result.Evidence.MemberRecords, Publications: result.Evidence.Publications,
			PullACKs: result.Evidence.PullACKs, Works: result.Evidence.Works,
		}}, nil
}

func (manager *ChannelManager) triggerMemberReconcileChannel(mesh store.ChannelMeshAuthority,
	channelID model.ChannelID,
) {
	for _, channel := range mesh.Channels() {
		if channel.Channel().ID() != channelID {
			continue
		}
		for _, member := range channel.Roster().Members() {
			current, ok := channel.Roster().CurrentMember(member.PeerID())
			if ok && current.Head() == member.Head() && current.Status() == model.MemberActive &&
				current.PeerID() != mesh.LocalPeerID() {
				manager.triggerMemberReconcileScope(channelID, current.PeerID())
			}
		}
		return
	}
}
