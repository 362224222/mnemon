package model

import (
	"errors"
	"testing"
	"time"
)

func TestPeerBindingRejectsSelfAndNormalizesLists(t *testing.T) {
	t.Parallel()

	local := mustPeer(t, "peer-local")
	remote := mustPeer(t, "peer-remote")
	channel, _ := ParseChannelID("channel-a")
	epoch, _ := ParseOriginEpoch("epoch-a")
	head, _ := NewRecordHead(2, Sum([]byte("member")))
	limits, _ := NewJSON([]byte(`{"frame_bytes":65536}`))
	now := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	spec := PeerBindingSpec{channel, remote, epoch, "reviewer", []byte("public-key"),
		[]string{"/ip4/127.0.0.2/tcp/4001", "/ip4/127.0.0.1/tcp/4001"},
		[]string{"/mnemon/events/1", "/mnemon/artifacts/1"}, limits, head,
		BindingPending, ReachabilityUnknown, now, nil}

	binding, err := NewPeerBinding(local, spec)
	if err != nil {
		t.Fatalf("NewPeerBinding() error = %v", err)
	}
	if got := binding.Multiaddrs(); got[0] != "/ip4/127.0.0.1/tcp/4001" {
		t.Fatalf("multiaddrs not canonicalized: %v", got)
	}
	copyKey := binding.PublicKey()
	copyKey[0] = 'x'
	if string(binding.PublicKey()) != "public-key" {
		t.Fatalf("PeerBinding public key is mutable")
	}

	spec.PeerID = local
	if _, err := NewPeerBinding(local, spec); !errors.Is(err, ErrInvariant) {
		t.Fatalf("self binding error = %v, want ErrInvariant", err)
	}
	spec.PeerID = remote
	spec.Multiaddrs = []string{"/ip4/127.0.0.1/tcp/4001", "/ip4/127.0.0.1/tcp/4001"}
	if _, err := NewPeerBinding(local, spec); !errors.Is(err, ErrInvalid) {
		t.Fatalf("duplicate address error = %v", err)
	}
}

func TestPeerBindingEnumsAreClosed(t *testing.T) {
	t.Parallel()

	if !BindingPending.Valid() || !BindingActive.Valid() || !BindingRevoked.Valid() || BindingState("ready").Valid() {
		t.Fatalf("BindingState closed enum mismatch")
	}
	if !ReachabilityUnknown.Valid() || !ReachabilityReachable.Valid() || !ReachabilityUnreachable.Valid() || Reachability("online").Valid() {
		t.Fatalf("Reachability closed enum mismatch")
	}
}
