package node

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestConfirmOfflineAuthorityRemovesOnlyStaleSocketWhileHoldingWriter(t *testing.T) {
	for _, withSocket := range []bool{false, true} {
		withSocket := withSocket
		t.Run(map[bool]string{false: "absent", true: "stale"}[withSocket], func(t *testing.T) {
			fixture := newDaemonFixture(t, true)
			socketPath := filepath.Join(fixture.nodeState, controlSocketName)
			if withSocket {
				createOfflineStaleSocket(t, socketPath)
			}
			expected := daemonFixtureAuthorityResponse(t, fixture)
			digest, err := localapi.AuthorityDigest(expected)
			if err != nil {
				t.Fatal(err)
			}
			writerObserved := false
			response, err := confirmOfflineAuthority(context.Background(), fixture.workspace, digest,
				func(ctx context.Context, path string) (bool, error) {
					competing, writerErr := store.OpenExisting(context.Background(),
						filepath.Join(fixture.nodeState, "node.db"))
					if competing != nil {
						_ = competing.Close()
						return false, errors.New("stale recovery did not retain the Store writer")
					}
					if !errors.Is(writerErr, store.ErrWriterActive) {
						return false, errors.New("stale recovery Store writer was not observable")
					}
					writerObserved = true
					return localapi.RemoveStaleOwnerUnix(ctx, path)
				})
			if err != nil || response != expected {
				t.Fatalf("ConfirmOfflineAuthority() = (%#v, %v)", response, err)
			}
			if !writerObserved {
				t.Fatal("stale socket recovery did not execute under the Store writer")
			}
			if _, err := os.Lstat(socketPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("confirmed socket remains: %v", err)
			}
			st, err := store.OpenExisting(context.Background(), filepath.Join(fixture.nodeState, "node.db"))
			if err != nil {
				t.Fatalf("confirmation retained Store writer: %v", err)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestConfirmOfflineAuthorityFailsClosedForWriterGenerationAndSocketConflict(t *testing.T) {
	t.Run("writer active", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		path := filepath.Join(fixture.nodeState, controlSocketName)
		createOfflineStaleSocket(t, path)
		st, err := store.OpenExisting(context.Background(), filepath.Join(fixture.nodeState, "node.db"))
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		_, err = ConfirmOfflineAuthority(context.Background(), fixture.workspace,
			mustOfflineAuthorityDigest(t, daemonFixtureAuthorityResponse(t, fixture)))
		if !errors.Is(err, ErrOfflineAuthority) || !errors.Is(err, ErrOfflineAuthorityActive) ||
			!errors.Is(err, store.ErrWriterActive) {
			t.Fatalf("writer-active confirmation error = %v", err)
		}
		if info, statErr := os.Lstat(path); statErr != nil || info.Mode()&os.ModeSocket == 0 {
			t.Fatalf("writer-active confirmation changed socket: (%v, %v)", info, statErr)
		}
	})

	t.Run("authority mismatch", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		other := newDaemonFixture(t, true)
		path := filepath.Join(fixture.nodeState, controlSocketName)
		createOfflineStaleSocket(t, path)
		_, err := ConfirmOfflineAuthority(context.Background(), fixture.workspace,
			mustOfflineAuthorityDigest(t, daemonFixtureAuthorityResponse(t, other)))
		if !errors.Is(err, ErrOfflineAuthority) {
			t.Fatalf("authority-mismatch confirmation error = %v", err)
		}
		if info, statErr := os.Lstat(path); statErr != nil || info.Mode()&os.ModeSocket == 0 {
			t.Fatalf("authority mismatch changed socket: (%v, %v)", info, statErr)
		}
		assertOfflineWriterReleased(t, fixture)
	})

	t.Run("active socket", func(t *testing.T) {
		fixture := newDaemonFixture(t, false)
		path := filepath.Join(fixture.nodeState, controlSocketName)
		listener, err := localapi.ListenOwnerUnix(path)
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		_, err = ConfirmOfflineAuthority(context.Background(), fixture.workspace,
			mustOfflineAuthorityDigest(t, daemonFixtureAuthorityResponse(t, fixture)))
		if !errors.Is(err, ErrOfflineAuthority) || !errors.Is(err, localapi.ErrOwnerUnixActive) {
			t.Fatalf("active-socket confirmation error = %v", err)
		}
		if info, statErr := os.Lstat(path); statErr != nil || info.Mode()&os.ModeSocket == 0 {
			t.Fatalf("active socket was changed: (%v, %v)", info, statErr)
		}
		assertOfflineWriterReleased(t, fixture)
	})

	t.Run("unsafe socket path", func(t *testing.T) {
		fixture := newDaemonFixture(t, false)
		path := filepath.Join(fixture.nodeState, controlSocketName)
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := ConfirmOfflineAuthority(context.Background(), fixture.workspace,
			mustOfflineAuthorityDigest(t, daemonFixtureAuthorityResponse(t, fixture)))
		if !errors.Is(err, ErrOfflineAuthority) {
			t.Fatalf("unsafe-socket confirmation error = %v", err)
		}
		if info, statErr := os.Lstat(path); statErr != nil || !info.Mode().IsRegular() {
			t.Fatalf("unsafe socket path was changed: (%v, %v)", info, statErr)
		}
		assertOfflineWriterReleased(t, fixture)
	})
}

func TestConfirmOfflineAuthorityRejectsInvalidInputWithoutMutation(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	path := filepath.Join(fixture.nodeState, controlSocketName)
	createOfflineStaleSocket(t, path)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, test := range []struct {
		name      string
		ctx       context.Context
		workspace string
		digest    model.Digest
	}{
		{name: "nil context", workspace: fixture.workspace,
			digest: mustOfflineAuthorityDigest(t, daemonFixtureAuthorityResponse(t, fixture))},
		{name: "cancelled", ctx: ctx, workspace: fixture.workspace,
			digest: mustOfflineAuthorityDigest(t, daemonFixtureAuthorityResponse(t, fixture))},
		{name: "zero digest", ctx: context.Background(), workspace: fixture.workspace},
		{name: "relative workspace", ctx: context.Background(), workspace: ".",
			digest: mustOfflineAuthorityDigest(t, daemonFixtureAuthorityResponse(t, fixture))},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ConfirmOfflineAuthority(test.ctx, test.workspace, test.digest); !errors.Is(err, ErrOfflineAuthority) {
				t.Fatalf("ConfirmOfflineAuthority() error = %v", err)
			}
			if info, err := os.Lstat(path); err != nil || info.Mode()&os.ModeSocket == 0 {
				t.Fatalf("invalid request changed socket: (%v, %v)", info, err)
			}
		})
	}
}

func createOfflineStaleSocket(t *testing.T, path string) {
	t.Helper()
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	listener.SetUnlinkOnClose(false)
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
}

func mustOfflineAuthorityDigest(t *testing.T, response localapi.AuthorityResponse) model.Digest {
	t.Helper()
	digest, err := localapi.AuthorityDigest(response)
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func assertOfflineWriterReleased(t *testing.T, fixture daemonFixture) {
	t.Helper()
	st, err := store.OpenExisting(context.Background(), filepath.Join(fixture.nodeState, "node.db"))
	if err != nil {
		t.Fatalf("failed confirmation retained Store writer: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
}
