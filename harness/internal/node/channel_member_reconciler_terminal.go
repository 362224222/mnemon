package node

import (
	"context"

	"github.com/mnemon-dev/mnemon/harness/internal/peer"
)

func (worker *ChannelMemberReconciler) mergeTerminalMemberHello(ctx context.Context,
	target channelMemberTarget, ack peer.MemberHelloAck,
) (bool, error) {
	records := ack.MissingRecords()
	if len(records) == 0 {
		return false, nil
	}
	terminal := records[len(records)-1]
	if terminal.PeerID() != target.localMember.PeerID() || !terminal.Status().Terminal() ||
		terminal.OriginEpoch() != target.localMember.OriginEpoch() ||
		ack.RosterHead() != terminal.Head() {
		return false, nil
	}
	if err := worker.backend.merge(ctx, target, records, ack.RosterHead(), worker.now()); err != nil {
		return true, err
	}
	worker.recordMerge()
	worker.signal()
	return true, nil
}
