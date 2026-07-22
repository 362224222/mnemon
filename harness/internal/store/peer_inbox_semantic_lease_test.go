package store

import (
	"context"
	"testing"
	"time"
)

func TestPeerInboxSemanticProcessingLeaseBoundsAbandonedRecovery(t *testing.T) {
	t.Parallel()
	fixture, _, readyAt := newReadyPeerInboxSemantic(t, "semantic-lease-bound")
	claimAt := readyAt.Add(time.Second)
	first := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-lease-bound-first", claimAt)
	lease := first.Fence().LeaseUntil().Sub(claimAt)
	if lease <= 0 || lease > 30*time.Second {
		t.Fatalf("semantic processing lease = %s, want nonzero and <= 30s", lease)
	}
	before, err := fixture.store.ClaimPeerInboxSemantic(context.Background(),
		ClaimPeerInboxSemanticSpec{LeaseOwner: "semantic-lease-bound-before",
			At: first.Fence().LeaseUntil().Add(-time.Nanosecond)})
	if err != nil || before.Found() {
		t.Fatalf("pre-expiry reclaim = (found %t,%v), want none", before.Found(), err)
	}
	second := mustClaimPeerInboxSemantic(t, fixture.store,
		"semantic-lease-bound-second", first.Fence().LeaseUntil())
	if second.Fence().Attempt() != first.Fence().Attempt()+1 {
		t.Fatalf("reclaim attempt = %d, want %d", second.Fence().Attempt(),
			first.Fence().Attempt()+1)
	}
}
