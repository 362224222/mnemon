package node

import (
	"context"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestDaemonChannelRuntimeRejectsMissingAuthority(t *testing.T) {
	t.Parallel()
	if runtime, err := openDaemonChannelRuntime(context.Background(), nil, nil, nil); err == nil || runtime != nil {
		t.Fatalf("openDaemonChannelRuntime() = (%#v, %v)", runtime, err)
	}
}

func TestChannelListenAddressIsIdentityStable(t *testing.T) {
	t.Parallel()
	identity := testkit.NewIdentity(t, "daemon-channel-listener")
	first, err := channelListenAddress(identity.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	second, err := channelListenAddress(identity.PeerID())
	if err != nil || first.String() != second.String() || first.String() == "/ip4/0.0.0.0/tcp/0" {
		t.Fatalf("Channel listener = (%v, %v, %v)", first, second, err)
	}
}
