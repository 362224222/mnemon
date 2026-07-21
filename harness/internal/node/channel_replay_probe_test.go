package node

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestChannelMutationCountsViewPreservesDurableCounts(t *testing.T) {
	counts := channelMutationCountsView(store.ChannelMutationCounts{Events: 3, Works: 2})
	if counts.Events != 3 {
		t.Fatalf("events = %d, want 3", counts.Events)
	}
	if counts.Works != 2 {
		t.Fatalf("works = %d, want 2", counts.Works)
	}
}
