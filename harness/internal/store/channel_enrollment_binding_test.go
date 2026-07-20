package store

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestSortedMemberPeerIDsIsDeterministicAndExcludesLocal(t *testing.T) {
	t.Parallel()
	local := testkit.NewIdentity(t, "binding-order-local").PeerID()
	first := testkit.NewIdentity(t, "binding-order-first").PeerID()
	second := testkit.NewIdentity(t, "binding-order-second").PeerID()
	members := map[model.PeerID]model.Member{local: {}, first: {}, second: {}}
	peers := sortedMemberPeerIDs(members, local)
	if len(peers) != 2 || peers[0] == local || peers[1] == local ||
		peers[0].String() >= peers[1].String() {
		t.Fatalf("sorted peers = %#v", peers)
	}
}
