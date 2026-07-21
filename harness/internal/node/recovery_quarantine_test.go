package node

import (
	"bytes"
	"context"
	"errors"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRecoveryQuarantineAtomicallyMovesWholeNodeAfterExactQuiescence(t *testing.T) {
	t.Parallel()
	fixture := newDaemonFixture(t, false)
	expected := daemonFixtureAuthorityResponse(t, fixture)
	before, err := os.Lstat(fixture.nodeState)
	if err != nil {
		t.Fatal(err)
	}
	database, err := os.ReadFile(filepath.Join(fixture.nodeState, "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	identity, err := os.ReadFile(filepath.Join(fixture.nodeState, identityKeyName))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := AcquireRecoveryLifecycle(context.Background(), DaemonLifecycleOptions{
		Workspace: fixture.workspace, NodeState: fixture.nodeState,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lease.Quarantine(context.Background(), expected,
		time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)); !errors.Is(err, ErrRecoveryQuarantine) {
		t.Fatalf("unquiesced Quarantine() = %v", err)
	}
	if _, err := os.Lstat(fixture.nodeState); err != nil {
		t.Fatalf("unquiesced attempt changed Node: %v", err)
	}
	confirmer := DaemonOfflineConfirmerFunc(func(ctx context.Context,
		expected AuthorityResponse,
	) (AuthorityResponse, error) {
		digest, err := AuthorityDigest(expected)
		if err != nil {
			return AuthorityResponse{}, err
		}
		return ConfirmRecoveryOfflineAuthority(ctx, fixture.workspace, digest)
	})
	if quiesced, err := lease.Quiesce(context.Background(),
		daemonFixtureLifecycleClient(t, fixture), confirmer, expected); err != nil || quiesced != expected {
		t.Fatalf("recovery Quiesce() = (%#v, %v)", quiesced, err)
	}
	at := time.Date(2026, 7, 21, 12, 0, 0, 123456789, time.UTC)
	result, err := lease.Quarantine(context.Background(), expected, at)
	if err != nil {
		t.Fatal(err)
	}
	assertRecoveryQuarantinePreserved(t, fixture, lease, result, at,
		recoveryQuarantineEvidence{before: before, database: database, identity: identity})
}

type recoveryQuarantineEvidence struct {
	before   os.FileInfo
	database []byte
	identity []byte
}

func assertRecoveryQuarantinePreserved(t *testing.T, fixture daemonFixture,
	lease *DaemonLifecycleLease, result RecoveryQuarantineResult, at time.Time,
	evidence recoveryQuarantineEvidence,
) {
	t.Helper()
	want := filepath.Join(fixture.workspace, ".mnemon", "harness", "recovery",
		"20260721T120000.123456789Z-"+fixture.identity.PeerID().String(), "node")
	if result.NodePath != want || result.PeerID != fixture.identity.PeerID().String() ||
		!result.RenamedAt.Equal(at) {
		t.Fatalf("Quarantine() = %#v, want path %q", result, want)
	}
	if _, err := os.Lstat(fixture.nodeState); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old Node path remains: %v", err)
	}
	after, err := os.Lstat(want)
	if err != nil || !os.SameFile(evidence.before, after) {
		t.Fatalf("renamed Node identity = (%v, %v)", after, err)
	}
	for path, wantBytes := range map[string][]byte{
		filepath.Join(want, "node.db"):       evidence.database,
		filepath.Join(want, identityKeyName): evidence.identity,
	} {
		got, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(got, wantBytes) {
			t.Fatalf("preserved %s = %d bytes, error %v", path, len(got), err)
		}
	}
	if err := lease.Close(); err != nil {
		t.Fatalf("Close() after rename = %v", err)
	}
	st, err := store.OpenExisting(context.Background(), filepath.Join(want, "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	if authority, err := st.ReadLocalAuthority(context.Background()); err != nil ||
		authority.Node.PeerID() != fixture.identity.PeerID() {
		t.Fatalf("preserved authority = (%#v, %v)", authority, err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
}
