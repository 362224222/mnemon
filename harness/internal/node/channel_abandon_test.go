package node

import (
	"context"
	"testing"
)

func TestChannelAbandonFailsClosedWithoutRuntimeAuthority(t *testing.T) {
	t.Parallel()
	manager := &ChannelManager{}
	_, apiErr := manager.ChannelAbandon(context.Background(), RequestMetadata{},
		ChannelAbandonRequest{Channel: "alpha", ConfirmChannel: "alpha", Force: true})
	if apiErr == nil || apiErr.Code != CodeMnemondUnavailable {
		t.Fatalf("ChannelAbandon() error = %#v", apiErr)
	}
}
