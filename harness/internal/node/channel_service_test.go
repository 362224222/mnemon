package node

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestChannelAliasIsStableSanitizedAndCollisionAware(t *testing.T) {
	t.Parallel()
	if got := channelAlias("  Review / Team  "); got != "review-team" {
		t.Fatalf("channelAlias() = %q", got)
	}
	if got := channelAlias("Проект Réview"); got != "r-view" {
		t.Fatalf("non-ASCII channelAlias() = %q", got)
	}
	if got := uniqueChannelAlias("review-team", store.ChannelControlAuthority{}); got != "review-team" {
		t.Fatalf("uniqueChannelAlias() = %q", got)
	}
}
