package node

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestMeshEndpointFilesAdvancePendingFinalAndRetirement(t *testing.T) {
	fixture := newMeshEndpointFileFixture(t, "lifecycle")
	assertMeshEndpointFileState(t, fixture, meshEndpointStateAbsent)

	created, err := publishMeshEndpointPending(fixture.nodeState, fixture.pending)
	if err != nil || !created {
		t.Fatalf("publishMeshEndpointPending() = (%t,%v)", created, err)
	}
	assertMeshEndpointAuthorityFile(t, fixture.nodeState, meshEndpointPendingName,
		fixture.pending.canonicalJSON())
	assertMeshEndpointFileState(t, fixture, meshEndpointStatePending)
	if created, err = publishMeshEndpointPending(fixture.nodeState, fixture.pending); err != nil || created {
		t.Fatalf("pending replay = (%t,%v)", created, err)
	}
	different := mustMeshEndpointPending(t, fixture.peerID, "/ip6/::/tcp/0", nil)
	if _, err := publishMeshEndpointPending(fixture.nodeState, different); !errors.Is(err, errMeshEndpointConflict) {
		t.Fatalf("different pending error = %v", err)
	}

	created, err = publishMeshEndpointFinal(fixture.nodeState, fixture.pending, fixture.final)
	if err != nil || !created {
		t.Fatalf("publishMeshEndpointFinal() = (%t,%v)", created, err)
	}
	assertMeshEndpointAuthorityFile(t, fixture.nodeState, meshEndpointName, fixture.final.canonicalJSON())
	assertMeshEndpointFileState(t, fixture, meshEndpointStateFinalWithPending)
	if created, err = publishMeshEndpointFinal(fixture.nodeState, fixture.pending, fixture.final); err != nil || created {
		t.Fatalf("final replay = (%t,%v)", created, err)
	}
	compatibleWrong := mustMeshEndpointPending(t, fixture.peerID, "/ip4/0.0.0.0/tcp/4401", nil)
	if _, err := publishMeshEndpointFinal(fixture.nodeState, compatibleWrong, fixture.final); !errors.Is(err, errMeshEndpointConflict) {
		t.Fatalf("final replay with wrong live pending error = %v", err)
	}

	wrongPending := mustMeshEndpointPending(t, fixture.peerID, "/ip4/0.0.0.0/tcp/4501", nil)
	if err := retireMeshEndpointPending(fixture.nodeState, compatibleWrong, fixture.final); !errors.Is(err, errMeshEndpointConflict) {
		t.Fatalf("wrong retirement error = %v", err)
	}
	if _, err := os.Lstat(filepath.Join(fixture.nodeState, meshEndpointPendingName)); err != nil {
		t.Fatalf("wrong retirement removed pending: %v", err)
	}
	if err := retireMeshEndpointPending(fixture.nodeState, fixture.pending, fixture.final); err != nil {
		t.Fatalf("retireMeshEndpointPending() error = %v", err)
	}
	assertMeshEndpointFileState(t, fixture, meshEndpointStateFinal)
	if err := retireMeshEndpointPending(fixture.nodeState, fixture.pending, fixture.final); err != nil {
		t.Fatalf("retirement replay error = %v", err)
	}
	if err := retireMeshEndpointPending(fixture.nodeState, wrongPending, fixture.final); !errors.Is(err, errMeshEndpointAuthority) {
		t.Fatalf("retired final accepted incompatible expected authority: %v", err)
	}
	if created, err = publishMeshEndpointFinal(fixture.nodeState, compatibleWrong, fixture.final); err != nil || created {
		t.Fatalf("retired final replay = (%t,%v)", created, err)
	}
	if _, err := publishMeshEndpointPending(fixture.nodeState, fixture.pending); !errors.Is(err, errMeshEndpointConflict) {
		t.Fatalf("pending after final error = %v", err)
	}
}

func TestMeshEndpointPublishFailureNeverReportsCreated(t *testing.T) {
	fixture := newMeshEndpointFileFixture(t, "publish-outcome")
	writeMeshEndpointTestFile(t, fixture, meshEndpointPendingName,
		fixture.pending.canonicalJSON(), meshEndpointFileMode)
	state, err := openIdentityNodeState(fixture.nodeState)
	if err != nil {
		t.Fatal(err)
	}
	defer state.close()
	if err := state.lock(); err != nil {
		t.Fatal(err)
	}
	defer state.unlock()
	created, err := meshEndpointPublishOutcome(publishMeshEndpointFile(state,
		meshEndpointPendingName, fixture.pending.canonicalJSON(), false))
	if created || !errors.Is(err, errMeshEndpointConflict) {
		t.Fatalf("pre-link failure outcome = (%t,%v)", created, err)
	}
}

func TestMeshEndpointFilesRequireCanonicalIdentityBeforeReadOrWrite(t *testing.T) {
	authorityPeer := testkit.NewIdentity(t, "mesh-endpoint-unbound-authority").PeerID()
	tests := []struct {
		name  string
		setup func(*testing.T, string)
	}{
		{name: "missing identity"},
		{name: "corrupt identity", setup: func(t *testing.T, nodeState string) {
			if err := os.WriteFile(filepath.Join(nodeState, identityKeyName),
				[]byte("not-a-canonical-private-key"), identityKeyMode); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "wrong canonical identity", setup: func(t *testing.T, nodeState string) {
			identity, err := EnsureIdentity(nodeState)
			if err != nil {
				t.Fatal(err)
			}
			if identity.PeerID() == authorityPeer {
				t.Fatal("test identities unexpectedly match")
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nodeState := newMeshEndpointNodeState(t)
			if test.setup != nil {
				test.setup(t, nodeState)
			}
			pending := mustMeshEndpointPending(t, authorityPeer, "/ip4/0.0.0.0/tcp/0", nil)
			if _, err := inspectMeshEndpointState(nodeState, authorityPeer); !errors.Is(err, errMeshEndpointAuthority) {
				t.Fatalf("inspectMeshEndpointState() error = %v", err)
			}
			if created, err := publishMeshEndpointPending(nodeState, pending); created || !errors.Is(err, errMeshEndpointAuthority) {
				t.Fatalf("publishMeshEndpointPending() = (%t,%v)", created, err)
			}
			assertNoMeshEndpointAuthorityFiles(t, nodeState)
		})
	}

	t.Run("identity corruption fences an existing pending before final write", func(t *testing.T) {
		fixture := newMeshEndpointFileFixture(t, "identity-corruption")
		if _, err := publishMeshEndpointPending(fixture.nodeState, fixture.pending); err != nil {
			t.Fatal(err)
		}
		pendingBefore, err := os.ReadFile(filepath.Join(fixture.nodeState, meshEndpointPendingName))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(fixture.nodeState, identityKeyName),
			[]byte("corrupt-after-pending"), identityKeyMode); err != nil {
			t.Fatal(err)
		}
		if _, err := inspectMeshEndpointState(fixture.nodeState, fixture.peerID); !errors.Is(err, errMeshEndpointAuthority) {
			t.Fatalf("inspectMeshEndpointState() error = %v", err)
		}
		if created, err := publishMeshEndpointFinal(fixture.nodeState, fixture.pending, fixture.final); created || !errors.Is(err, errMeshEndpointAuthority) {
			t.Fatalf("publishMeshEndpointFinal() = (%t,%v)", created, err)
		}
		pendingAfter, err := os.ReadFile(filepath.Join(fixture.nodeState, meshEndpointPendingName))
		if err != nil || !bytes.Equal(pendingBefore, pendingAfter) {
			t.Fatalf("pending changed after identity failure: (%q,%v)", pendingAfter, err)
		}
		if _, err := os.Lstat(filepath.Join(fixture.nodeState, meshEndpointName)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("final endpoint was written without identity authority: %v", err)
		}
	})
}

func TestMeshEndpointFilesConvergeCrashStagesWithoutLosingAuthority(t *testing.T) {
	fixture := newMeshEndpointFileFixture(t, "crash-stages")
	identityStage := filepath.Join(fixture.nodeState, ".identity-44444444444444444444444444444444.tmp")
	identityPath := filepath.Join(fixture.nodeState, identityKeyName)
	if err := os.Link(identityPath, identityStage); err != nil {
		t.Fatal(err)
	}
	assertMeshEndpointFileState(t, fixture, meshEndpointStateAbsent)
	if _, err := os.Lstat(identityStage); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("linked identity crash stage survived cleanup: %v", err)
	}
	assertSingleMeshEndpointLink(t, identityPath)

	stale := filepath.Join(fixture.nodeState, ".mesh-endpoint-00000000000000000000000000000000.tmp")
	if err := os.WriteFile(stale, []byte("unpublished"), meshEndpointFileMode); err != nil {
		t.Fatal(err)
	}
	assertMeshEndpointFileState(t, fixture, meshEndpointStateAbsent)
	if _, err := os.Lstat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("safe unlinked stage survived cleanup: %v", err)
	}

	if _, err := publishMeshEndpointPending(fixture.nodeState, fixture.pending); err != nil {
		t.Fatal(err)
	}
	pendingPath := filepath.Join(fixture.nodeState, meshEndpointPendingName)
	linkedPending := filepath.Join(fixture.nodeState, ".mesh-endpoint-11111111111111111111111111111111.tmp")
	if err := os.Link(pendingPath, linkedPending); err != nil {
		t.Fatal(err)
	}
	assertMeshEndpointFileState(t, fixture, meshEndpointStatePending)
	assertSingleMeshEndpointLink(t, pendingPath)

	if _, err := publishMeshEndpointFinal(fixture.nodeState, fixture.pending, fixture.final); err != nil {
		t.Fatal(err)
	}
	finalPath := filepath.Join(fixture.nodeState, meshEndpointName)
	linkedFinal := filepath.Join(fixture.nodeState, ".mesh-endpoint-22222222222222222222222222222222.tmp")
	if err := os.Link(finalPath, linkedFinal); err != nil {
		t.Fatal(err)
	}
	assertMeshEndpointFileState(t, fixture, meshEndpointStateFinalWithPending)
	assertSingleMeshEndpointLink(t, finalPath)

	unsafe := filepath.Join(fixture.nodeState, ".mesh-endpoint-33333333333333333333333333333333.tmp")
	if err := os.Symlink(finalPath, unsafe); err != nil {
		t.Fatal(err)
	}
	if _, err := inspectMeshEndpointState(fixture.nodeState, fixture.peerID); !errors.Is(err, errMeshEndpointAuthority) {
		t.Fatalf("unsafe stage error = %v", err)
	}
	if info, err := os.Lstat(unsafe); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("unsafe stage was followed or removed: (%v,%v)", info, err)
	}
}

func TestMeshEndpointFilesFailClosedOnUnsafeOrNoncanonicalAuthority(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, meshEndpointFileFixture)
	}{
		{name: "malformed", mutate: func(t *testing.T, f meshEndpointFileFixture) {
			writeMeshEndpointTestFile(t, f, meshEndpointPendingName, []byte(`{}`), meshEndpointFileMode)
		}},
		{name: "noncanonical whitespace", mutate: func(t *testing.T, f meshEndpointFileFixture) {
			writeMeshEndpointTestFile(t, f, meshEndpointPendingName,
				append(f.pending.canonicalJSON(), '\n'), meshEndpointFileMode)
		}},
		{name: "unknown field", mutate: func(t *testing.T, f meshEndpointFileFixture) {
			raw := fmt.Sprintf(`{"advertised_addrs":[],"listen_addrs":["/ip4/0.0.0.0/tcp/0"],"peer_id":%q,"schema_version":1,"unknown":true}`,
				f.peerID.String())
			writeMeshEndpointTestFile(t, f, meshEndpointPendingName, []byte(raw), meshEndpointFileMode)
		}},
		{name: "schema drift", mutate: func(t *testing.T, f meshEndpointFileFixture) {
			raw := bytes.Replace(f.pending.canonicalJSON(), []byte(`"schema_version":1`),
				[]byte(`"schema_version":2`), 1)
			writeMeshEndpointTestFile(t, f, meshEndpointPendingName, raw, meshEndpointFileMode)
		}},
		{name: "unsorted addresses", mutate: func(t *testing.T, f meshEndpointFileFixture) {
			raw := fmt.Sprintf(`{"advertised_addrs":["/ip4/127.0.0.1/tcp/4401","/dns4/node-a/tcp/4401"],"listen_addrs":["/ip4/0.0.0.0/tcp/4401"],"peer_id":%q,"schema_version":1}`,
				f.peerID.String())
			writeMeshEndpointTestFile(t, f, meshEndpointName, []byte(raw), meshEndpointFileMode)
		}},
		{name: "oversized", mutate: func(t *testing.T, f meshEndpointFileFixture) {
			writeMeshEndpointTestFile(t, f, meshEndpointPendingName,
				bytes.Repeat([]byte("x"), int(maxMeshEndpointBytes)+1), meshEndpointFileMode)
		}},
		{name: "wrong mode", mutate: func(t *testing.T, f meshEndpointFileFixture) {
			writeMeshEndpointTestFile(t, f, meshEndpointPendingName, f.pending.canonicalJSON(), 0o644)
		}},
		{name: "directory", mutate: func(t *testing.T, f meshEndpointFileFixture) {
			if err := os.Mkdir(filepath.Join(f.nodeState, meshEndpointPendingName), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "symlink", mutate: func(t *testing.T, f meshEndpointFileFixture) {
			target := filepath.Join(t.TempDir(), "outside")
			if err := os.WriteFile(target, f.pending.canonicalJSON(), meshEndpointFileMode); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, filepath.Join(f.nodeState, meshEndpointPendingName)); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "hardlink", mutate: func(t *testing.T, f meshEndpointFileFixture) {
			path := writeMeshEndpointTestFile(t, f, meshEndpointPendingName,
				f.pending.canonicalJSON(), meshEndpointFileMode)
			if err := os.Link(path, filepath.Join(f.nodeState, "endpoint-alias")); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "wrong peer", mutate: func(t *testing.T, f meshEndpointFileFixture) {
			other := testkit.NewIdentity(t, "mesh-endpoint-other").PeerID()
			value := mustMeshEndpointPending(t, other, "/ip4/0.0.0.0/tcp/0", nil)
			writeMeshEndpointTestFile(t, f, meshEndpointPendingName, value.canonicalJSON(), meshEndpointFileMode)
		}},
		{name: "inconsistent crash pair", mutate: func(t *testing.T, f meshEndpointFileFixture) {
			writeMeshEndpointTestFile(t, f, meshEndpointPendingName,
				f.pending.canonicalJSON(), meshEndpointFileMode)
			wrong := mustMeshEndpoint(t, f.peerID, "/ip4/127.0.0.1/tcp/4401",
				[]string{"/ip4/127.0.0.1/tcp/4401"})
			writeMeshEndpointTestFile(t, f, meshEndpointName, wrong.canonicalJSON(), meshEndpointFileMode)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newMeshEndpointFileFixture(t, "unsafe-"+test.name)
			test.mutate(t, fixture)
			if _, err := inspectMeshEndpointState(fixture.nodeState, fixture.peerID); !errors.Is(err, errMeshEndpointAuthority) {
				t.Fatalf("inspectMeshEndpointState() error = %v", err)
			}
		})
	}
}

func TestMeshEndpointFilesSerializeConcurrentExactAndConflictingCreators(t *testing.T) {
	fixture := newMeshEndpointFileFixture(t, "concurrent")
	alternative := mustMeshEndpointPending(t, fixture.peerID, "/ip6/::/tcp/0", nil)
	type result struct {
		created bool
		err     error
	}
	results := make(chan result, 24)
	var wait sync.WaitGroup
	for index := range 24 {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			candidate := fixture.pending
			if index%2 == 1 {
				candidate = alternative
			}
			created, err := publishMeshEndpointPending(fixture.nodeState, candidate)
			results <- result{created: created, err: err}
		}(index)
	}
	wait.Wait()
	close(results)
	created, conflicts := 0, 0
	for result := range results {
		if result.created {
			created++
		}
		if errors.Is(result.err, errMeshEndpointConflict) {
			conflicts++
		} else if result.err != nil {
			t.Fatalf("creator error = %v", result.err)
		}
	}
	if created != 1 || conflicts != 12 {
		t.Fatalf("creators = created %d conflicts %d, want 1 and 12", created, conflicts)
	}
	state, err := inspectMeshEndpointState(fixture.nodeState, fixture.peerID)
	if err != nil || state.stateKind() != meshEndpointStatePending {
		t.Fatalf("concurrent state = (%d,%v)", state.stateKind(), err)
	}
}

func TestMeshEndpointFileReaderRejectsNodeStatePathSwap(t *testing.T) {
	fixture := newMeshEndpointFileFixture(t, "path-swap")
	state, err := openIdentityNodeState(fixture.nodeState)
	if err != nil {
		t.Fatal(err)
	}
	defer state.close()
	moved := fixture.nodeState + "-moved"
	if err := os.Rename(fixture.nodeState, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(fixture.nodeState, identityDirectoryMode); err != nil {
		t.Fatal(err)
	}
	if _, err := inspectMeshEndpointStateLocked(state, fixture.peerID); !errors.Is(err, errMeshEndpointAuthority) {
		t.Fatalf("path swap error = %v", err)
	}
}

type meshEndpointFileFixture struct {
	nodeState string
	peerID    model.PeerID
	pending   meshEndpointPending
	final     meshEndpoint
}

func newMeshEndpointFileFixture(t *testing.T, _ string) meshEndpointFileFixture {
	t.Helper()
	nodeState := newMeshEndpointNodeState(t)
	identity, err := EnsureIdentity(nodeState)
	if err != nil {
		t.Fatal(err)
	}
	peerID := identity.PeerID()
	return meshEndpointFileFixture{nodeState: nodeState, peerID: peerID,
		pending: mustMeshEndpointPending(t, peerID, "/ip4/0.0.0.0/tcp/0", nil),
		final: mustMeshEndpoint(t, peerID, "/ip4/0.0.0.0/tcp/4401",
			[]string{"/dns4/node-a/tcp/4401", "/ip4/127.0.0.1/tcp/4401"})}
}

func newMeshEndpointNodeState(t *testing.T) string {
	t.Helper()
	nodeState := filepath.Join(t.TempDir(), "node")
	if err := os.Mkdir(nodeState, identityDirectoryMode); err != nil {
		t.Fatal(err)
	}
	return nodeState
}

func assertNoMeshEndpointAuthorityFiles(t *testing.T, nodeState string) {
	t.Helper()
	for _, name := range []string{meshEndpointPendingName, meshEndpointName} {
		if _, err := os.Lstat(filepath.Join(nodeState, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("unexpected endpoint authority %s: %v", name, err)
		}
	}
}

func assertMeshEndpointFileState(t *testing.T, fixture meshEndpointFileFixture,
	want meshEndpointStateKind,
) {
	t.Helper()
	state, err := inspectMeshEndpointState(fixture.nodeState, fixture.peerID)
	if err != nil || state.stateKind() != want {
		t.Fatalf("inspectMeshEndpointState() = (%d,%v), want %d", state.stateKind(), err, want)
	}
}

func assertMeshEndpointAuthorityFile(t *testing.T, nodeState, name string, want []byte) {
	t.Helper()
	path := filepath.Join(nodeState, name)
	raw, err := os.ReadFile(path)
	info, statErr := os.Lstat(path)
	if err != nil || statErr != nil || !bytes.Equal(raw, want) || info.Mode().Perm() != meshEndpointFileMode {
		t.Fatalf("endpoint file %s = (%q,%v,%v)", name, raw, info, errors.Join(err, statErr))
	}
	assertSingleMeshEndpointLink(t, path)
}

func assertSingleMeshEndpointLink(t *testing.T, path string) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || uint64(stat.Nlink) != 1 {
		t.Fatalf("link count for %s = %#v", path, stat)
	}
}

func writeMeshEndpointTestFile(t *testing.T, fixture meshEndpointFileFixture, name string,
	raw []byte, mode os.FileMode,
) string {
	t.Helper()
	path := filepath.Join(fixture.nodeState, name)
	if err := os.WriteFile(path, raw, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}
