package node

import (
	"errors"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestChannelMemberStoreErrorsRemainClosedProtocolCodes(t *testing.T) {
	t.Parallel()
	if err := channelMemberStoreError(store.ErrChannelBaselineEpochMismatch); !errors.Is(err, peer.ErrChannelMemberEpochMismatch) {
		t.Fatalf("channelMemberStoreError() = %v", err)
	}
}
