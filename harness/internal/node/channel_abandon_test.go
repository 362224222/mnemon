package node

import (
	"context"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
)

func TestChannelAbandonFailsClosedWithoutRuntimeAuthority(t *testing.T) {
	t.Parallel()
	manager := &ChannelManager{}
	_, apiErr := manager.ChannelAbandon(context.Background(), localapi.RequestMetadata{},
		localapi.ChannelAbandonRequest{Channel: "alpha", ConfirmChannel: "alpha", Force: true})
	if apiErr == nil || apiErr.Code != localapi.CodeMnemondUnavailable {
		t.Fatalf("ChannelAbandon() error = %#v", apiErr)
	}
}
