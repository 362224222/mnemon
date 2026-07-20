package store

import (
	"context"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestChannelAuthorityPlanFingerprintIsStableAndClosed(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	fixture := testkit.NewSignedChannel(t, "authority-plan-fingerprint")
	insertChannelTestNode(t, st.db, fixture.Owner(), fixture.Channel().CreatedAt())
	insertSignedChannelFixture(t, st.db, fixture, model.TopicNotJoined)
	mesh, err := st.ReadChannelMeshAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	first, err := fingerprintChannelMeshAuthority(mesh)
	if err != nil || first.IsZero() {
		t.Fatalf("first fingerprint = (%s, %v)", first.String(), err)
	}
	channels := mesh.Channels()
	channels[0] = ChannelMeshChannel{}
	second, err := fingerprintChannelMeshAuthority(mesh)
	if err != nil || second != first {
		t.Fatalf("defensive fingerprint = (%s, %v), want %s", second.String(), err, first.String())
	}
	for _, resolution := range []ChannelAuthorityPlanResolution{
		ChannelAuthorityPlanUnchanged, ChannelAuthorityPlanCandidate, ChannelAuthorityPlanDiverged,
	} {
		if !resolution.Valid() {
			t.Fatalf("closed resolution %q is invalid", resolution)
		}
	}
	if ChannelAuthorityPlanResolution("other").Valid() {
		t.Fatal("unknown authority-plan resolution became valid")
	}
}
