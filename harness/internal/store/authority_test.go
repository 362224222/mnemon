package store

import (
	"context"
	"errors"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestReadLocalAuthorityReturnsOneConsistentSnapshot(t *testing.T) {
	st := openTestStore(t)
	node, profile := bootstrapValues(t, "peer-authority", "principal-authority", t.TempDir())
	if _, err := st.InitializeNode(context.Background(), node, profile); err != nil {
		t.Fatal(err)
	}
	snapshot, err := st.ReadLocalAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Node.PeerID() != node.PeerID() || snapshot.Node.OriginEpoch() != node.OriginEpoch() ||
		snapshot.Profile.ID() != profile.ID() || snapshot.Profile.Principal() != profile.Principal() ||
		snapshot.Node.ActiveAssetRevision() != snapshot.Profile.ActiveAssetRevision() ||
		snapshot.Profile.Enabled() {
		t.Fatalf("ReadLocalAuthority() = %#v", snapshot)
	}
}

func TestReadLocalAuthorityRejectsMissingAndInconsistentState(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		st := openTestStore(t)
		if _, err := st.ReadLocalAuthority(context.Background()); !errors.Is(err, ErrLocalAuthority) {
			t.Fatalf("ReadLocalAuthority() error = %v", err)
		}
	})
	t.Run("asset mismatch", func(t *testing.T) {
		st := openTestStore(t)
		node, profile := bootstrapValues(t, "peer-authority-drift", "principal-authority-drift", t.TempDir())
		if _, err := st.InitializeNode(context.Background(), node, profile); err != nil {
			t.Fatal(err)
		}
		if _, err := st.db.ExecContext(context.Background(),
			"UPDATE node SET active_asset_rev = ? WHERE singleton = 1", model.Sum([]byte("drift")).String()); err != nil {
			t.Fatal(err)
		}
		if _, err := st.ReadLocalAuthority(context.Background()); !errors.Is(err, ErrLocalAuthority) {
			t.Fatalf("ReadLocalAuthority() error = %v", err)
		}
	})
}
