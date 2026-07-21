package node

import (
	"testing"
)

func TestControllerChannelServiceSatisfiesClosedLocalBoundary(t *testing.T) {
	t.Parallel()
	var service ChannelService = controllerChannelService{}
	if service == nil {
		t.Fatal("controller Channel service is nil")
	}
}
