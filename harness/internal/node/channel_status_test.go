package node

import (
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestInviteViewExpiresWithoutPersistingBearerAuthority(t *testing.T) {
	t.Parallel()
	expires := time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)
	view := inviteView(expires, 7, 2, "open", expires)
	if view.Status != "expired" || view.RemainingUses != 5 || view.ExpiresAt != "2026-07-20T10:00:00Z" {
		t.Fatalf("inviteView() = %#v", view)
	}
}

func TestChannelBaselinesReadyRequiresBothDirectionsForLiveBindings(t *testing.T) {
	t.Parallel()
	ready := store.ChannelPeerReadiness{BindingState: model.BindingActive,
		InboundReady: true, OutboundReady: true}
	if !channelBaselinesReady([]store.ChannelPeerReadiness{ready,
		{BindingState: model.BindingRevoked}}) {
		t.Fatal("complete live baseline and terminal binding were not ready")
	}
	for _, incomplete := range []store.ChannelPeerReadiness{
		{BindingState: model.BindingPending},
		{BindingState: model.BindingActive, InboundReady: true},
		{BindingState: model.BindingActive, OutboundReady: true},
	} {
		if channelBaselinesReady([]store.ChannelPeerReadiness{incomplete}) {
			t.Fatalf("incomplete baseline was ready: %#v", incomplete)
		}
	}
}
