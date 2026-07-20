package node

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
)

func TestControllerChannelServiceSatisfiesClosedLocalBoundary(t *testing.T) {
	t.Parallel()
	var service localapi.ChannelService = controllerChannelService{}
	if service == nil {
		t.Fatal("controller Channel service is nil")
	}
}
