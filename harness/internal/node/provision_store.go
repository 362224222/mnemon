package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type provisionStoreSnapshot struct {
	state       store.NodeInitializationState
	authority   store.LocalAuthority
	database    os.FileInfo
	writerGuard os.FileInfo
}

// readProvisionStoreSnapshot is the sole create boundary. A missing node.db
// may become a fresh Store; every existing path is opened existing-only so
// Provision cannot repair or reinterpret damaged durable authority.
func readProvisionStoreSnapshot(ctx context.Context, nodeState string) (snapshot provisionStoreSnapshot, err error) {
	path := filepath.Join(nodeState, "node.db")
	_, inspectErr := os.Lstat(path)
	var st *store.Store
	switch {
	case errors.Is(inspectErr, os.ErrNotExist):
		if err := validateProvisionFreshWriterGuard(path + ".writer.lock"); err != nil {
			return provisionStoreSnapshot{}, err
		}
		st, err = store.Open(ctx, path)
	case inspectErr != nil:
		return provisionStoreSnapshot{}, inspectErr
	default:
		st, err = store.OpenExisting(ctx, path)
	}
	if err != nil {
		return provisionStoreSnapshot{}, err
	}
	defer func() { err = errors.Join(err, st.Close()) }()
	snapshot.state, err = st.ClassifyNodeInitialization(ctx)
	if err != nil {
		return provisionStoreSnapshot{}, err
	}
	if snapshot.state == store.NodeInitializationExisting {
		snapshot.authority, err = st.ReadLocalAuthority(ctx)
	}
	if err == nil && snapshot.state != store.NodeInitializationFresh &&
		snapshot.state != store.NodeInitializationExisting {
		err = errors.New("Store returned an unknown initialization state")
	}
	if err == nil {
		snapshot.database, snapshot.writerGuard, err = freezeProvisionStoreFiles(path)
	}
	return snapshot, err
}

func validateProvisionFreshWriterGuard(path string) error {
	_, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	_, err = inspectProvisionStoreFile(path, nil)
	return err
}

func settleProvisionStore(ctx context.Context, nodeState string, first provisionStoreSnapshot,
	candidate provisionCandidate, endpoint meshEndpointState,
) (result ProvisionResult, err error) {
	path := filepath.Join(nodeState, "node.db")
	if err := verifyProvisionStoreFiles(path, first); err != nil {
		return ProvisionResult{}, err
	}
	st, err := store.OpenExisting(ctx, path)
	if err != nil {
		return ProvisionResult{}, err
	}
	defer func() { err = errors.Join(err, st.Close()) }()
	if err := verifyProvisionStoreFiles(path, first); err != nil {
		return ProvisionResult{}, err
	}
	current, err := st.ClassifyNodeInitialization(ctx)
	if err != nil {
		return ProvisionResult{}, err
	}
	if current != first.state {
		return ProvisionResult{}, fmt.Errorf("%w: initialization state changed", store.ErrInitializationConflict)
	}
	if current == store.NodeInitializationFresh && endpoint.stateKind() != meshEndpointStateAbsent &&
		endpoint.stateKind() != meshEndpointStatePending {
		return ProvisionResult{}, fmt.Errorf("%w: fresh Store has final mesh endpoint authority",
			store.ErrInitializationConflict)
	}

	authority, err := settleProvisionAuthority(ctx, st, current, candidate)
	if err != nil {
		return ProvisionResult{}, err
	}
	if !sameProvisionAuthority(authority, candidate.authority) ||
		first.state == store.NodeInitializationExisting && !sameProvisionAuthority(authority, first.authority) {
		return ProvisionResult{}, fmt.Errorf("%w: durable authority changed", store.ErrInitializationConflict)
	}
	if endpoint.stateKind() == meshEndpointStateAbsent || endpoint.stateKind() == meshEndpointStatePending {
		proof, proofErr := st.ReadMeshPristineAuthority(ctx)
		if proofErr != nil {
			return ProvisionResult{}, proofErr
		}
		if !sameProvisionAuthority(authority,
			store.LocalAuthority{Node: proof.Node(), Profile: proof.Profile()}) {
			return ProvisionResult{}, fmt.Errorf("%w: mesh-pristine authority changed", store.ErrInitializationConflict)
		}
	}
	if err := verifyProvisionStoreFiles(path, first); err != nil {
		return ProvisionResult{}, err
	}
	return ProvisionResult{NodeState: nodeState, Node: authority.Node, Profile: authority.Profile,
		Created:           current == store.NodeInitializationFresh,
		CredentialCreated: candidate.credentialCreated}, nil
}

func settleProvisionAuthority(ctx context.Context, st *store.Store,
	state store.NodeInitializationState, candidate provisionCandidate,
) (store.LocalAuthority, error) {
	switch state {
	case store.NodeInitializationFresh:
		initialized, err := st.InitializeNode(ctx, candidate.authority.Node, candidate.authority.Profile)
		if err != nil {
			return store.LocalAuthority{}, err
		}
		if !initialized.Created {
			return store.LocalAuthority{}, fmt.Errorf("%w: fresh initialization replayed",
				store.ErrInitializationConflict)
		}
		return store.LocalAuthority{Node: initialized.Node, Profile: initialized.Profile}, nil
	case store.NodeInitializationExisting:
		return st.ReadLocalAuthority(ctx)
	default:
		return store.LocalAuthority{}, errors.New("Store returned an unknown initialization state")
	}
}

func sameProvisionAuthority(left, right store.LocalAuthority) bool {
	return left.Node.Spec() == right.Node.Spec() && left.Profile.Spec() == right.Profile.Spec()
}

func freezeProvisionStoreFiles(path string) (os.FileInfo, os.FileInfo, error) {
	database, err := inspectProvisionStoreFile(path, nil)
	if err != nil {
		return nil, nil, err
	}
	guard, err := inspectProvisionStoreFile(path+".writer.lock", nil)
	return database, guard, err
}

func verifyProvisionStoreFiles(path string, frozen provisionStoreSnapshot) error {
	if _, err := inspectProvisionStoreFile(path, frozen.database); err != nil {
		return fmt.Errorf("%w: node.db file fence: %v", store.ErrInitializationConflict, err)
	}
	if _, err := inspectProvisionStoreFile(path+".writer.lock", frozen.writerGuard); err != nil {
		return fmt.Errorf("%w: writer guard file fence: %v", store.ErrInitializationConflict, err)
	}
	return nil
}

func inspectProvisionStoreFile(path string, expected os.FileInfo) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if _, err := validateIdentityOwnerPath(info, 0o600, false); err != nil {
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		return nil, errors.New("private Store file must have exactly one filesystem link")
	}
	if expected != nil && !os.SameFile(expected, info) {
		return nil, errors.New("private Store file identity changed")
	}
	return info, nil
}
