package store

import (
	"context"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestReadChannelControlAuthorityIncludesOnlyPublicOpenGrantMetadata(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	fixture := testkit.NewSignedChannel(t, "control-authority")
	insertChannelTestNode(t, st.db, fixture.Owner(), fixture.Channel().CreatedAt())
	grantID, _ := model.ParseGrantID("grant-control-authority")
	token := storeTestEnrollmentToken(t, fixture.Descriptor(), fixture.Owner(), grantID,
		"control-authority", fixture.Channel().CreatedAt(), model.MaxMembersPerChannel-1)
	if _, err := st.CreateChannel(context.Background(), CreateChannelSpec{Channel: fixture.Channel(),
		Genesis: fixture.OwnerMember().Member(), Token: token}); err != nil {
		t.Fatal(err)
	}
	authority, err := st.ReadChannelControlAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	channels := authority.Channels()
	if len(channels) != 1 {
		t.Fatalf("control authority Channel count = %d", len(channels))
	}
	grant, ok := channels[0].OpenGrant()
	if authority.LocalPeerID() != fixture.Owner().PeerID() || !ok ||
		channels[0].Channel().ID() != fixture.Channel().ID() || grant.ID() != grantID ||
		grant.UsedUses() != 0 || grant.MaxUses() != model.MaxMembersPerChannel-1 || grant.Status() != "open" {
		t.Fatalf("control authority = %#v, grant=%#v present=%v", authority, grant, ok)
	}
}
