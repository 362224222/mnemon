package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestChannelLeaveRuntimeGatesFailClosedWhenAuthorityIsUnavailable(t *testing.T) {
	t.Parallel()
	manager := &ChannelManager{}
	if _, err := manager.AcceptMemberLeaveGate(context.Background(),
		peer.ChannelMemberLeaveControl{}); !errors.Is(err, peer.ErrChannelMemberRosterConflict) {
		t.Fatalf("unavailable owner leave gate error = %v", err)
	}
	if err := manager.SettleMemberLeaveRuntimeGate(context.Background(),
		model.ChannelLeaveRequestID{}, model.SignedChannelLeaveReceipt{},
		time.Time{}); !errors.Is(err, ErrChannelMemberReconciler) {
		t.Fatalf("unavailable requester settlement gate error = %v", err)
	}
	if err := channelMemberStoreError(store.ErrChannelLeaveAuthority); !errors.Is(err,
		peer.ErrChannelMemberNotMember) {
		t.Fatalf("leave authority protocol mapping = %v", err)
	}
}
