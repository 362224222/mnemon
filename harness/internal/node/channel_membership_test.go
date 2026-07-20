package node

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestMemberAliasNeverExposesPeerID(t *testing.T) {
	t.Parallel()
	peerID, err := model.ParsePeerID("12D3KooWLAqWKpkXGjWSV1weBG6o2QfCToBcznNdrEweC1YcHq9X")
	if err != nil {
		t.Fatal(err)
	}
	if alias := memberAlias(peerID); alias == peerID.String() || len(alias) != len("member-")+8 {
		t.Fatalf("memberAlias() = %q", alias)
	}
}
