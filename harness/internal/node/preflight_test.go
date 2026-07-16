package node

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestDaemonPreflightNeverInitializesOrRewritesNodeDatabase(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		path := filepath.Join(fixture.nodeState, "node.db")
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		preflight := newFixtureDaemonPreflight(t, fixture, fixture.install, fixture.revision)
		if err := preflight.Verify(context.Background()); !errors.Is(err, ErrDaemonPreflight) {
			t.Fatalf("Verify() error = %v", err)
		}
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("strict preflight created node.db: %v", err)
		}
	})

	t.Run("empty", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		path := filepath.Join(fixture.nodeState, "node.db")
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		preflight := newFixtureDaemonPreflight(t, fixture, fixture.install, fixture.revision)
		if err := preflight.Verify(context.Background()); !errors.Is(err, store.ErrUnsupportedSchema) {
			t.Fatalf("Verify(empty) error = %v", err)
		}
		info, err := os.Stat(path)
		if err != nil || info.Size() != 0 {
			t.Fatalf("empty node.db changed: (%v, %v)", info, err)
		}
	})

	t.Run("unknown schema", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		path := filepath.Join(fixture.nodeState, "node.db")
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("CREATE TABLE legacy(id INTEGER PRIMARY KEY); PRAGMA user_version = 99"); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o600); err != nil {
			t.Fatal(err)
		}
		before, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		preflight := newFixtureDaemonPreflight(t, fixture, fixture.install, fixture.revision)
		if err := preflight.Verify(context.Background()); !errors.Is(err, store.ErrUnsupportedSchema) {
			t.Fatalf("Verify(unknown) error = %v", err)
		}
		after, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(after, before) {
			t.Fatalf("unknown node.db was rewritten: equal=%t error=%v", bytes.Equal(after, before), err)
		}
	})
}

func TestDaemonPreflightRejectsWriterAndDurableAuthorityDrift(t *testing.T) {
	t.Run("writer active", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		st, err := store.OpenExisting(context.Background(), filepath.Join(fixture.nodeState, "node.db"))
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		preflight := newFixtureDaemonPreflight(t, fixture, fixture.install, fixture.revision)
		if err := preflight.Verify(context.Background()); !errors.Is(err, store.ErrWriterActive) {
			t.Fatalf("Verify() error = %v, want ErrWriterActive", err)
		}
	})

	t.Run("disabled Profile", func(t *testing.T) {
		fixture := newDaemonFixture(t, false)
		preflight := newFixtureDaemonPreflight(t, fixture, fixture.install, fixture.revision)
		if err := preflight.Verify(context.Background()); !errors.Is(err, ErrDaemonAuthority) {
			t.Fatalf("Verify() error = %v", err)
		}
	})

	t.Run("identity mismatch", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		if err := os.Remove(filepath.Join(fixture.nodeState, identityKeyName)); err != nil {
			t.Fatal(err)
		}
		if _, err := EnsureIdentity(fixture.nodeState); err != nil {
			t.Fatal(err)
		}
		preflight := newFixtureDaemonPreflight(t, fixture, fixture.install, fixture.revision)
		if err := preflight.Verify(context.Background()); !errors.Is(err, ErrDaemonAuthority) {
			t.Fatalf("Verify() error = %v", err)
		}
	})

	t.Run("workspace mismatch", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		renamed := fixture.workspace + "-renamed"
		if err := os.Rename(fixture.workspace, renamed); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(renamed) })
		fixture.workspace = renamed
		fixture.nodeState = filepath.Join(renamed, ".mnemon", "harness", "node")
		install := InstallationVerifierFunc(func(model.Profile) error { return nil })
		preflight := newFixtureDaemonPreflight(t, fixture, install, fixture.revision)
		if err := preflight.Verify(context.Background()); !errors.Is(err, ErrDaemonAuthority) {
			t.Fatalf("Verify() error = %v", err)
		}
	})

	t.Run("credential mismatch", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		writeDaemonToken(t, fixture.nodeState, bytes.Repeat([]byte{0x91}, 32), true)
		preflight := newFixtureDaemonPreflight(t, fixture, fixture.install, fixture.revision)
		if err := preflight.Verify(context.Background()); !errors.Is(err, ErrDaemonAuthority) {
			t.Fatalf("Verify() error = %v", err)
		}
		assertDaemonPreflightWriterReleased(t, fixture.nodeState)
	})

	t.Run("asset mismatch", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		other := model.Sum([]byte("other-assets")).String()
		preflight := newFixtureDaemonPreflight(t, fixture, fixture.install, other)
		if err := preflight.Verify(context.Background()); !errors.Is(err, ErrDaemonPreflight) {
			t.Fatalf("Verify() error = %v", err)
		}
	})

	t.Run("database mode", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		path := filepath.Join(fixture.nodeState, "node.db")
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatal(err)
		}
		preflight := newFixtureDaemonPreflight(t, fixture, fixture.install, fixture.revision)
		if err := preflight.Verify(context.Background()); !errors.Is(err, ErrDaemonAuthority) {
			t.Fatalf("Verify() error = %v", err)
		}
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != 0o644 {
			t.Fatalf("strict preflight repaired node.db: (%v, %v)", info, err)
		}
	})
}

func TestDaemonPreflightVerifiesInstallationAndReleasesWriterAuthority(t *testing.T) {
	t.Run("installation drift", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		drift := errors.New("projection drift")
		install := InstallationVerifierFunc(func(profile model.Profile) error {
			if profile.ID() != model.TeamworkProfileID() {
				t.Fatalf("Verify profile = %#v", profile)
			}
			return drift
		})
		preflight := newFixtureDaemonPreflight(t, fixture, install, fixture.revision)
		if err := preflight.Verify(context.Background()); !errors.Is(err, drift) {
			t.Fatalf("Verify() error = %v", err)
		}
		assertDaemonPreflightWriterReleased(t, fixture.nodeState)
	})

	t.Run("success", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		called := false
		install := InstallationVerifierFunc(func(profile model.Profile) error {
			called = true
			if profile.ID() != model.TeamworkProfileID() || profile.WorkspaceRoot() != fixture.workspace ||
				profile.ActiveAssetRevision() != fixture.revision || !profile.Enabled() {
				t.Fatalf("Verify profile = %#v", profile)
			}
			return fixture.install.Verify(profile)
		})
		preflight := newFixtureDaemonPreflight(t, fixture, install, fixture.revision)
		if err := preflight.Verify(context.Background()); err != nil {
			t.Fatalf("Verify() error = %v", err)
		}
		if !called {
			t.Fatal("installation verifier was not called")
		}
		assertDaemonPreflightWriterReleased(t, fixture.nodeState)
	})

	t.Run("cancelled", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		called := false
		install := InstallationVerifierFunc(func(model.Profile) error {
			called = true
			return nil
		})
		preflight := newFixtureDaemonPreflight(t, fixture, install, fixture.revision)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := preflight.Verify(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Verify(cancelled) error = %v", err)
		}
		if called {
			t.Fatal("cancelled preflight reached installation verifier")
		}
	})

	t.Run("stalled installation verifier", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		started := make(chan struct{})
		release := make(chan struct{})
		defer close(release)
		install := InstallationVerifierFunc(func(model.Profile) error {
			close(started)
			<-release
			return nil
		})
		preflight := newFixtureDaemonPreflight(t, fixture, install, fixture.revision)
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() { result <- preflight.Verify(ctx) }()
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			cancel()
			t.Fatal("installation verifier did not start")
		}
		cancel()
		var err error
		select {
		case err = <-result:
		case <-time.After(2 * time.Second):
			t.Fatal("cancelled installation verifier retained preflight")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Verify(stalled installation) error = %v", err)
		}
		assertDaemonPreflightWriterReleased(t, fixture.nodeState)
	})
}

func newFixtureDaemonPreflight(t *testing.T, fixture daemonFixture, install InstallationVerifier,
	revision string,
) *DaemonPreflight {
	t.Helper()
	preflight, err := NewDaemonPreflight(DaemonPreflightOptions{Workspace: fixture.workspace,
		NodeState: fixture.nodeState, AssetRevision: revision, Install: install})
	if err != nil {
		t.Fatal(err)
	}
	return preflight
}

func assertDaemonPreflightWriterReleased(t *testing.T, nodeState string) {
	t.Helper()
	st, err := store.OpenExisting(context.Background(), filepath.Join(nodeState, "node.db"))
	if err != nil {
		t.Fatalf("preflight retained writer authority: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
}
