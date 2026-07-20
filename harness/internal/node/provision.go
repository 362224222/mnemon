package node

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

var ErrProvision = errors.New("provision mnemond Node")

type ProvisionOptions struct {
	Workspace     string
	Host          model.HostKind
	AssetRevision string
	Clock         Clock
	Credentials   ProfileCredentialProvisioner
}

type ProvisionResult struct {
	NodeState         string
	Node              model.Node
	Profile           model.Profile
	Created           bool
	CredentialCreated bool
}

// PrepareNodeState creates and validates only the owner-controlled directory
// skeleton required to serialize first setup. It deliberately does not create
// or open identity keys, Profile credentials, node.db, projections or any
// other canonical Node authority. Provision remains the sole initializer for
// those objects after the caller has acquired its setup transaction lock.
func PrepareNodeState(workspace string) (string, error) {
	validated, err := validateDaemonWorkspace(workspace)
	if err != nil {
		return "", fmt.Errorf("%w: prepare Node state: %v", ErrProvision, err)
	}
	nodeState, err := ensureProvisionState(validated)
	if err != nil {
		return "", fmt.Errorf("%w: prepare Node state: %v", ErrProvision, err)
	}
	return nodeState, nil
}

// Provision initializes only durable inactive Node authority. Host projection,
// adapter checks, activation and daemon launch are separate setup gates, so a
// failure after this call cannot accidentally grant managed Agent authority.
func Provision(ctx context.Context, options ProvisionOptions) (result ProvisionResult, err error) {
	if err := validateProvisionAuthority(ctx, options.Credentials); err != nil {
		return ProvisionResult{}, err
	}
	workspace, err := validateDaemonWorkspace(options.Workspace)
	if err != nil {
		return ProvisionResult{}, fmt.Errorf("%w: %v", ErrProvision, err)
	}
	runtimeKind, ok := model.RuntimeForHost(options.Host)
	if _, digestErr := model.ParseDigest(options.AssetRevision); !ok || digestErr != nil {
		return ProvisionResult{}, fmt.Errorf("%w: Host or asset revision is invalid", ErrProvision)
	}
	if options.Clock == nil {
		options.Clock = wallClock{}
	}
	at := options.Clock.Now().Round(0).UTC()
	if at.IsZero() || at.UnixNano() <= 0 || !time.Unix(0, at.UnixNano()).UTC().Equal(at) {
		return ProvisionResult{}, fmt.Errorf("%w: clock is invalid", ErrProvision)
	}
	nodeState, err := ensureProvisionState(workspace)
	if err != nil {
		return ProvisionResult{}, err
	}
	identity, err := EnsureIdentity(nodeState)
	if err != nil {
		return ProvisionResult{}, fmt.Errorf("%w: %v", ErrProvision, err)
	}
	credential, credentialCreated, err := options.Credentials.Ensure(nodeState)
	if err != nil {
		return ProvisionResult{}, fmt.Errorf("%w: %v", ErrProvision, err)
	}
	st, err := store.Open(ctx, filepath.Join(nodeState, "node.db"))
	if err != nil {
		return ProvisionResult{}, fmt.Errorf("%w: %v", ErrProvision, err)
	}
	defer func() {
		if closeErr := st.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("%w: close Store: %v", ErrProvision, closeErr))
		}
	}()

	if authority, readErr := st.ReadLocalAuthority(ctx); readErr == nil {
		if authority.Node.PeerID() != identity.PeerID() ||
			authority.Profile.WorkspaceRoot() != workspace ||
			authority.Profile.CredentialHash() != credential {
			return ProvisionResult{}, fmt.Errorf("%w: durable identity differs from workspace projections", ErrProvision)
		}
		if authority.Profile.Enabled() && authority.Profile.Host() != options.Host {
			return ProvisionResult{}, fmt.Errorf("%w: enabled Profile Host is %s, not %s",
				ErrProvision, authority.Profile.Host(), options.Host)
		}
		return ProvisionResult{NodeState: nodeState, Node: authority.Node, Profile: authority.Profile,
			CredentialCreated: credentialCreated}, nil
	}

	epoch, err := model.ParseOriginEpoch(derivedProvisionIdentifier("epoch", identity.PublicKey(), workspace))
	if err != nil {
		return ProvisionResult{}, fmt.Errorf("%w: derive origin epoch: %v", ErrProvision, err)
	}
	principal := derivedProvisionIdentifier("principal", identity.PublicKey(), workspace)
	nodeValue, err := model.NewNode(model.NodeSpec{PeerID: identity.PeerID(), OriginEpoch: epoch,
		NextOriginSequence: 1, ActiveAssetRevision: options.AssetRevision, CreatedAt: at, UpdatedAt: at})
	if err != nil {
		return ProvisionResult{}, fmt.Errorf("%w: create Node value: %v", ErrProvision, err)
	}
	profile, err := model.NewProfile(model.ProfileSpec{ID: model.TeamworkProfileID(), Principal: principal,
		WorkspaceRoot: workspace, Host: options.Host, Runtime: runtimeKind, CredentialHash: credential,
		ActiveAssetRevision: options.AssetRevision, HandlingBudget: model.DefaultHandlingBudget().JSON(),
		CreatedAt: at, UpdatedAt: at})
	if err != nil {
		return ProvisionResult{}, fmt.Errorf("%w: create Profile value: %v", ErrProvision, err)
	}
	initialized, err := st.InitializeNode(ctx, nodeValue, profile)
	if err != nil {
		return ProvisionResult{}, fmt.Errorf("%w: %v", ErrProvision, err)
	}
	return ProvisionResult{NodeState: nodeState, Node: initialized.Node, Profile: initialized.Profile,
		Created: initialized.Created, CredentialCreated: credentialCreated}, nil
}

func validateProvisionAuthority(ctx context.Context, credentials ProfileCredentialProvisioner) error {
	if ctx == nil || isNilNodeInterface(credentials) {
		return fmt.Errorf("%w: context or credential authority is unavailable", ErrProvision)
	}
	return nil
}

func ensureProvisionState(workspace string) (string, error) {
	mnemonDir := filepath.Join(workspace, ".mnemon")
	if err := ensureProvisionDirectory(mnemonDir, 0o700, false); err != nil {
		return "", err
	}
	harnessDir := filepath.Join(mnemonDir, "harness")
	if err := ensureProvisionDirectory(harnessDir, 0o700, true); err != nil {
		return "", err
	}
	nodeState := filepath.Join(harnessDir, "node")
	if err := ensureProvisionDirectory(nodeState, 0o700, true); err != nil {
		return "", err
	}
	return nodeState, nil
}

func ensureProvisionDirectory(path string, createMode os.FileMode, exactMode bool) error {
	created := false
	if err := os.Mkdir(path, createMode); err == nil {
		created = true
	} else if !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("%w: create %s: %v", ErrProvision, path, err)
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !ownedByEffectiveUser(info) ||
		info.Mode().Perm()&0o022 != 0 || exactMode && info.Mode().Perm() != createMode {
		return fmt.Errorf("%w: %s is not a safe owner directory", ErrProvision, path)
	}
	if created {
		parent, err := os.Open(filepath.Dir(path))
		if err != nil {
			return fmt.Errorf("%w: open parent of %s: %v", ErrProvision, path, err)
		}
		syncErr := parent.Sync()
		closeErr := parent.Close()
		if syncErr != nil || closeErr != nil {
			return fmt.Errorf("%w: persist %s: %v", ErrProvision, path, errors.Join(syncErr, closeErr))
		}
	}
	return nil
}

func derivedProvisionIdentifier(kind string, publicKey []byte, workspace string) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte("mnemon-r5-provision-" + kind))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(publicKey)
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(workspace))
	sum := hash.Sum(nil)
	return kind + "-" + base64.RawURLEncoding.EncodeToString(sum[:18])
}
