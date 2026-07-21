package node

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestInspectAuthorityReadsActiveAndDisabledExistingState(t *testing.T) {
	t.Parallel()
	for _, enabled := range []bool{false, true} {
		enabled := enabled
		t.Run(map[bool]string{false: "disabled", true: "active"}[enabled], func(t *testing.T) {
			fixture := newDaemonFixture(t, enabled)
			snapshot, err := InspectAuthority(context.Background(), fixture.workspace)
			if err != nil || snapshot.Enabled != enabled || snapshot.Host != fixture.profile.Host() ||
				snapshot.Runtime != fixture.profile.Runtime() ||
				snapshot.AssetRevision != fixture.profile.ActiveAssetRevision() ||
				!snapshot.UpdatedAt.Equal(fixture.profile.UpdatedAt()) ||
				snapshot.PeerID != fixture.identity.PeerID() ||
				snapshot.ActiveAssetRevision != fixture.revision {
				t.Fatalf("InspectAuthority() = (%#v, %v)", snapshot, err)
			}
			st, err := store.OpenExisting(context.Background(), filepath.Join(fixture.nodeState, "node.db"))
			if err != nil {
				t.Fatalf("inspection retained Store writer: %v", err)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestInspectAuthorityRejectsActiveWriterExplicitly(t *testing.T) {
	t.Parallel()
	fixture := newDaemonFixture(t, true)
	st, err := store.OpenExisting(context.Background(), filepath.Join(fixture.nodeState, "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = InspectAuthority(context.Background(), fixture.workspace)
	if !errors.Is(err, ErrAuthorityInspection) || !errors.Is(err, store.ErrWriterActive) {
		t.Fatalf("active writer inspection error = %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := InspectAuthority(context.Background(), fixture.workspace); err != nil {
		t.Fatalf("inspection after writer release: %v", err)
	}
}

func TestInspectAuthorityRejectsIdentityAndCredentialDriftAndReleasesStore(t *testing.T) {
	t.Parallel()
	t.Run("credential", func(t *testing.T) {
		fixture := newDaemonFixture(t, false)
		writeDaemonToken(t, fixture.nodeState, bytes.Repeat([]byte{0x29}, 32), true)
		if _, err := InspectAuthority(context.Background(), fixture.workspace); !errors.Is(err, ErrAuthorityInspection) {
			t.Fatalf("credential drift error = %v", err)
		}
		st, err := store.OpenExisting(context.Background(), filepath.Join(fixture.nodeState, "node.db"))
		if err != nil {
			t.Fatalf("failed inspection retained Store writer: %v", err)
		}
		_ = st.Close()
	})
	t.Run("identity", func(t *testing.T) {
		fixture := newDaemonFixture(t, false)
		if err := os.Remove(filepath.Join(fixture.nodeState, identityKeyName)); err != nil {
			t.Fatal(err)
		}
		replacement, err := EnsureIdentity(fixture.nodeState)
		if err != nil || replacement.PeerID() == fixture.identity.PeerID() {
			t.Fatalf("replacement identity = (%v, %v)", replacement, err)
		}
		if _, err := InspectAuthority(context.Background(), fixture.workspace); !errors.Is(err, ErrAuthorityInspection) {
			t.Fatalf("identity drift error = %v", err)
		}
	})
}

func TestInspectAuthorityNeverCreatesOrRepairsStateAndHonorsContext(t *testing.T) {
	t.Parallel()
	workspace := newDaemonWorkspace(t)
	if _, err := InspectAuthority(context.Background(), workspace); !errors.Is(err, ErrAuthorityInspection) {
		t.Fatalf("missing authority error = %v", err)
	}
	if _, err := os.Lstat(filepath.Join(workspace, ".mnemon")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("inspection created workspace state: %v", err)
	}

	fixture := newDaemonFixture(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := InspectAuthority(ctx, fixture.workspace); !errors.Is(err, context.Canceled) ||
		!errors.Is(err, ErrAuthorityInspection) {
		t.Fatalf("cancelled inspection error = %v", err)
	}
	if _, err := InspectAuthority(context.Background(), "."); !errors.Is(err, ErrAuthorityInspection) {
		t.Fatalf("relative inspection error = %v", err)
	}
	if snapshot, err := InspectAuthority(context.Background(), fixture.workspace); err != nil ||
		snapshot.PeerID.IsZero() {
		t.Fatalf("post-cancellation inspection = (%#v, %v)", snapshot, err)
	}
}
