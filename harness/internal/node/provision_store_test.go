package node

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestProvisionClosesStoreWriterBeforeCredentialProjection(t *testing.T) {
	workspace := newProvisionWorkspace(t)
	options := provisionTestOptions(t, workspace, model.HostCodex)
	options.Credentials = observingProvisionCredentials{}
	first, err := Provision(context.Background(), options)
	if err != nil || !first.Created {
		t.Fatalf("first Provision() = (%#v, %v)", first, err)
	}
	second, err := Provision(context.Background(), options)
	if err != nil || second.Created || second.Node.PeerID() != first.Node.PeerID() {
		t.Fatalf("replayed Provision() = (%#v, %v)", second, err)
	}
}

func TestProvisionExistingStoreDoesNotRepairAMissingWriterGuard(t *testing.T) {
	workspace := newProvisionWorkspace(t)
	options := provisionTestOptions(t, workspace, model.HostCodex)
	first, err := Provision(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	guard := filepath.Join(first.NodeState, "node.db.writer.lock")
	if err := os.Remove(guard); err != nil {
		t.Fatal(err)
	}
	result, err := Provision(context.Background(), options)
	if result != (ProvisionResult{}) || !errors.Is(err, ErrProvision) {
		t.Fatalf("Provision() = (%#v, %v)", result, err)
	}
	if _, err := os.Lstat(guard); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Provision recreated missing writer guard: %v", err)
	}
}

func TestProvisionRejectsUnsafeFreshWriterGuardBeforeProjectionMutation(t *testing.T) {
	workspace := newProvisionWorkspace(t)
	nodeState, err := PrepareNodeState(workspace)
	if err != nil {
		t.Fatal(err)
	}
	guard := filepath.Join(nodeState, "node.db.writer.lock")
	if err := os.WriteFile(guard, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(guard, 0o644); err != nil {
		t.Fatal(err)
	}
	credentials := &countingProvisionCredentials{}
	options := provisionTestOptions(t, workspace, model.HostCodex)
	options.Credentials = credentials
	result, err := Provision(context.Background(), options)
	if result != (ProvisionResult{}) || !errors.Is(err, ErrProvision) || credentials.calls != 0 {
		t.Fatalf("Provision() = (%#v, %v), credential calls=%d", result, err, credentials.calls)
	}
	if info, err := os.Lstat(guard); err != nil || info.Mode().Perm() != 0o644 {
		t.Fatalf("unsafe writer guard changed = (%v, %v)", info, err)
	}
	for _, name := range []string{"node.db", identityKeyName, "profiles"} {
		if _, err := os.Lstat(filepath.Join(nodeState, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("unsafe guard created %s: %v", name, err)
		}
	}
}

func TestProvisionRejectsStoreFileReplacementBetweenSections(t *testing.T) {
	for _, name := range []string{"node.db", "node.db.writer.lock"} {
		name := name
		t.Run(name, func(t *testing.T) {
			workspace := newProvisionWorkspace(t)
			replacer := &replacingProvisionCredentials{name: name}
			options := provisionTestOptions(t, workspace, model.HostCodex)
			options.Credentials = replacer
			result, err := Provision(context.Background(), options)
			if result != (ProvisionResult{}) || !errors.Is(err, ErrProvision) ||
				!errors.Is(err, store.ErrInitializationConflict) {
				t.Fatalf("Provision() = (%#v, %v)", result, err)
			}
			if replacer.before == nil || replacer.after == nil || os.SameFile(replacer.before, replacer.after) {
				t.Fatal("replacement fixture did not change file identity")
			}
			identity, loadErr := LoadIdentity(filepath.Join(workspace, ".mnemon", "harness", "node"))
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			state, inspectErr := inspectMeshEndpointState(filepath.Join(workspace, ".mnemon", "harness", "node"),
				identity.PeerID())
			if inspectErr != nil || state.stateKind() != meshEndpointStateAbsent {
				t.Fatalf("replacement published endpoint = (%d, %v)", state.stateKind(), inspectErr)
			}
		})
	}
}

func TestProvisionRejectsFreshStoreWithFinalEndpointWithoutChangingState(t *testing.T) {
	for _, kind := range []meshEndpointStateKind{meshEndpointStateFinalWithPending, meshEndpointStateFinal} {
		kind := kind
		t.Run(map[meshEndpointStateKind]string{
			meshEndpointStateFinalWithPending: "final with pending",
			meshEndpointStateFinal:            "final",
		}[kind], func(t *testing.T) {
			workspace, nodeState, before, identityBefore, credentialBefore :=
				prepareFreshProvisionFinalState(t, kind)
			result, err := Provision(context.Background(), provisionTestOptions(t, workspace, model.HostCodex))
			if result != (ProvisionResult{}) || !errors.Is(err, ErrProvision) ||
				!errors.Is(err, store.ErrInitializationConflict) {
				t.Fatalf("Provision() = (%#v, %v)", result, err)
			}
			assertFreshProvisionStateUnchanged(t, nodeState, before, identityBefore, credentialBefore)
		})
	}
}

func TestProvisionRequiresPristineStoreOnlyUntilFinalEndpointExists(t *testing.T) {
	tests := []struct {
		name      string
		kind      meshEndpointStateKind
		wantError bool
	}{
		{name: "absent", kind: meshEndpointStateAbsent, wantError: true},
		{name: "pending", kind: meshEndpointStatePending, wantError: true},
		{name: "final with pending", kind: meshEndpointStateFinalWithPending},
		{name: "final", kind: meshEndpointStateFinal},
	}
	for index, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			testProvisionRequiresPristineStore(t, index, tc.kind, tc.wantError)
		})
	}
}

func testProvisionRequiresPristineStore(t *testing.T, index int, kind meshEndpointStateKind,
	wantError bool,
) {
	t.Helper()
	workspace := newProvisionWorkspace(t)
	options := provisionTestOptions(t, workspace, model.HostCodex)
	first, err := Provision(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	desired := mustMeshEndpointPending(t, first.Node.PeerID(), defaultProvisionMeshListener, nil)
	port := 4501 + index
	final := mustMeshEndpoint(t, first.Node.PeerID(), fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port),
		[]string{fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port)})
	setProvisionEndpointState(t, first.NodeState, desired, final, kind)
	insertProvisionMeshEvidence(t, first)
	options.Credentials = verifyOnlyProvisionCredentials{}
	result, err := Provision(context.Background(), options)
	if wantError && (result != (ProvisionResult{}) || !errors.Is(err, ErrProvision) ||
		!errors.Is(err, store.ErrMeshNotPristine)) {
		t.Fatalf("non-pristine Provision() = (%#v, %v)", result, err)
	}
	if !wantError && (err != nil || result.Node.PeerID() != first.Node.PeerID()) {
		t.Fatalf("final non-pristine Provision() = (%#v, %v)", result, err)
	}
	state, inspectErr := inspectMeshEndpointState(first.NodeState, first.Node.PeerID())
	if inspectErr != nil || state.stateKind() != kind {
		t.Fatalf("non-pristine endpoint state = (%d, %v), want %d", state.stateKind(), inspectErr, kind)
	}
}

type verifyOnlyProvisionCredentials struct{}

func (verifyOnlyProvisionCredentials) Ensure(string) (model.Digest, bool, error) {
	return model.Digest{}, false, errors.New("existing Provision called credential Ensure")
}

func (verifyOnlyProvisionCredentials) Verify(nodeState string, expected model.Digest) error {
	return localapi.VerifyProfileCredential(nodeState, expected)
}

type observingProvisionCredentials struct{}

func (observingProvisionCredentials) Ensure(nodeState string) (model.Digest, bool, error) {
	if err := observeProvisionStoreState(nodeState, store.NodeInitializationFresh); err != nil {
		return model.Digest{}, false, err
	}
	return testProfileCredentials{}.Ensure(nodeState)
}

func (observingProvisionCredentials) Verify(nodeState string, expected model.Digest) error {
	if err := observeProvisionStoreState(nodeState, store.NodeInitializationExisting); err != nil {
		return err
	}
	return localapi.VerifyProfileCredential(nodeState, expected)
}

func observeProvisionStoreState(nodeState string, want store.NodeInitializationState) (err error) {
	st, err := store.OpenExisting(context.Background(), filepath.Join(nodeState, "node.db"))
	if err != nil {
		return fmt.Errorf("credential projection overlapped Store writer: %w", err)
	}
	defer func() { err = errors.Join(err, st.Close()) }()
	got, err := st.ClassifyNodeInitialization(context.Background())
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("credential projection observed Store state %d, want %d", got, want)
	}
	return nil
}

type countingProvisionCredentials struct{ calls int }

func (credentials *countingProvisionCredentials) Ensure(string) (model.Digest, bool, error) {
	credentials.calls++
	return model.Digest{}, false, errors.New("unexpected credential Ensure")
}

func (credentials *countingProvisionCredentials) Verify(string, model.Digest) error {
	credentials.calls++
	return errors.New("unexpected credential Verify")
}

type replacingProvisionCredentials struct {
	name          string
	before, after os.FileInfo
}

func (credentials *replacingProvisionCredentials) Ensure(nodeState string) (model.Digest, bool, error) {
	digest, created, err := testProfileCredentials{}.Ensure(nodeState)
	if err == nil {
		err = credentials.replace(nodeState)
	}
	return digest, created, err
}

func (credentials *replacingProvisionCredentials) Verify(nodeState string, expected model.Digest) error {
	if err := localapi.VerifyProfileCredential(nodeState, expected); err != nil {
		return err
	}
	return credentials.replace(nodeState)
}

func (credentials *replacingProvisionCredentials) replace(nodeState string) error {
	path := filepath.Join(nodeState, credentials.name)
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	credentials.before, err = os.Lstat(path)
	if err != nil {
		return err
	}
	if err := os.Rename(path, path+".before-replacement"); err != nil {
		return err
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return err
	}
	credentials.after, err = os.Lstat(path)
	return err
}

func prepareFreshProvisionFinalState(t *testing.T, kind meshEndpointStateKind) (
	string, string, meshEndpointState, os.FileInfo, os.FileInfo,
) {
	t.Helper()
	workspace := newProvisionWorkspace(t)
	nodeState, err := PrepareNodeState(workspace)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(context.Background(), filepath.Join(nodeState, "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	identity, err := EnsureIdentity(nodeState)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := (testProfileCredentials{}).Ensure(nodeState); err != nil {
		t.Fatal(err)
	}
	pending := mustMeshEndpointPending(t, identity.PeerID(), defaultProvisionMeshListener, nil)
	publishProvisionPending(t, nodeState, pending)
	final := mustMeshEndpoint(t, identity.PeerID(), "/ip4/0.0.0.0/tcp/4701",
		[]string{"/ip4/127.0.0.1/tcp/4701"})
	publishProvisionFinal(t, nodeState, pending, final)
	if kind == meshEndpointStateFinal {
		if err := retireMeshEndpointPending(nodeState, pending, final); err != nil {
			t.Fatal(err)
		}
	}
	before, err := inspectMeshEndpointState(nodeState, identity.PeerID())
	if err != nil || before.stateKind() != kind {
		t.Fatalf("prepared endpoint = (%d, %v), want %d", before.stateKind(), err, kind)
	}
	identityInfo, _ := os.Lstat(filepath.Join(nodeState, identityKeyName))
	credentialInfo, _ := os.Lstat(filepath.Join(nodeState, "profiles",
		model.TeamworkProfileID().String()+".token"))
	return workspace, nodeState, before, identityInfo, credentialInfo
}

func assertFreshProvisionStateUnchanged(t *testing.T, nodeState string, endpoint meshEndpointState,
	identityBefore, credentialBefore os.FileInfo,
) {
	t.Helper()
	st, err := store.OpenExisting(context.Background(), filepath.Join(nodeState, "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	classification, classifyErr := st.ClassifyNodeInitialization(context.Background())
	closeErr := st.Close()
	if classifyErr != nil || closeErr != nil || classification != store.NodeInitializationFresh {
		t.Fatalf("fresh Store changed = (%d, %v, close %v)", classification, classifyErr, closeErr)
	}
	identity, err := LoadIdentity(nodeState)
	if err != nil {
		t.Fatal(err)
	}
	current, inspectErr := inspectMeshEndpointState(nodeState, identity.PeerID())
	if inspectErr != nil || !sameProvisionMeshEndpointState(current, endpoint) {
		t.Fatalf("endpoint changed = (%d, %v)", current.stateKind(), inspectErr)
	}
	identityAfter, _ := os.Lstat(filepath.Join(nodeState, identityKeyName))
	credentialAfter, _ := os.Lstat(filepath.Join(nodeState, "profiles",
		model.TeamworkProfileID().String()+".token"))
	if !os.SameFile(identityBefore, identityAfter) || !os.SameFile(credentialBefore, credentialAfter) {
		t.Fatal("fresh conflict replaced identity or credential")
	}
}

func insertProvisionMeshEvidence(t *testing.T, provisioned ProvisionResult) {
	t.Helper()
	path := filepath.Join(provisioned.NodeState, "node.db")
	st, err := store.OpenExisting(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := model.NewJSON([]byte(`{"entries":[],"kind":"provision-test","total_bytes":0}`))
	if err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	at := provisioned.Profile.CreatedAt().Add(time.Second)
	root := store.VerifiedArtifactRoot{RootDigest: model.Sum([]byte("provision-mesh-evidence")),
		Manifest: manifest, ManifestDigest: model.Sum(manifest.Bytes()), CreatedAt: at, VerifiedAt: at}
	if _, err := st.CheckpointVerifiedArtifactRoot(context.Background(), root); err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	encodedAt := at.UTC().Format("2006-01-02T15:04:05.000000000Z")
	_, insertErr := db.Exec(`INSERT INTO artifact_pins(root_digest,owner_kind,owner_id,created_at)
		VALUES(?,'handling','provision-test',?)`, root.RootDigest.String(), encodedAt)
	closeErr := db.Close()
	if insertErr != nil || closeErr != nil {
		t.Fatal(errors.Join(insertErr, closeErr))
	}
}
