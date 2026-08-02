package node

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/mnemon-dev/mnemon/harness/internal/agency"
	r7authority "github.com/mnemon-dev/mnemon/harness/internal/authority"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"os"
	"path/filepath"
	"time"
)

var ErrProvision = errors.New("provision mnemond Node")

type ProvisionOptions struct {
	Workspace     string
	Host          model.HostKind
	AssetRevision string
	Clock         Clock
	Credentials   ProfileCredentialAuthority
}

type ProvisionResult struct {
	NodeState         string
	Node              model.Node
	Profile           model.Profile
	Created           bool
	CredentialCreated bool
}

type provisionPlan struct {
	workspace string
	runtime   model.RuntimeKind
	at        time.Time
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
	if ctx == nil {
		return ProvisionResult{}, fmt.Errorf("%w: context is unavailable", ErrProvision)
	}
	plan, err := prepareProvision(&options)
	if err != nil {
		return ProvisionResult{}, err
	}
	nodeState, err := ensureProvisionState(plan.workspace)
	if err != nil {
		return ProvisionResult{}, err
	}
	identity, err := EnsureIdentity(nodeState)
	if err != nil {
		return ProvisionResult{}, fmt.Errorf("%w: %v", ErrProvision, err)
	}
	credentials, err := requireProfileCredentialAuthority(options.Credentials)
	if err != nil {
		return ProvisionResult{}, fmt.Errorf("%w: %w", ErrProvision, err)
	}
	credential, credentialCreated, err := credentials.EnsureProfileCredential(nodeState)
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

	var provisioned ProvisionResult
	if durable, readErr := st.ReadLocalAuthority(ctx); readErr == nil {
		provisioned, err = replayProvision(nodeState, identity, credential, credentialCreated,
			durable, options.Host, plan.workspace)
	} else {
		provisioned, err = createProvisionAuthority(ctx, st, nodeState, identity, credential,
			credentialCreated, options, plan)
	}
	if err != nil {
		return ProvisionResult{}, err
	}
	if err := ensureProvisionAgencyAuthority(ctx, nodeState,
		provisioned.Profile.Principal(), options.Clock); err != nil {
		return ProvisionResult{}, err
	}
	return provisioned, nil
}

// ensureProvisionAgencyAuthority is the setup-only initializer for agency.db.
// node.db and agency.db cannot share a SQLite transaction; Provision therefore
// uses idempotent replay as its recovery contract. Daemon startup never calls
// this helper and refuses either missing half.
func ensureProvisionAgencyAuthority(ctx context.Context, nodeState string,
	profilePrincipal string, clock Clock,
) (err error) {
	if clock == nil {
		return fmt.Errorf("%w: R7 authority clock is unavailable", ErrProvision)
	}
	principal, err := agencyPrincipalForValue(profilePrincipal)
	if err != nil {
		return fmt.Errorf("%w: derive R7 Principal: %v", ErrProvision, err)
	}
	st, err := r7authority.OpenWithClock(ctx, filepath.Join(nodeState, "agency.db"), clock.Now)
	if err != nil {
		return fmt.Errorf("%w: open R7 authority: %v", ErrProvision, err)
	}
	defer func() {
		if closeErr := st.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("%w: close R7 authority: %v", ErrProvision, closeErr))
		}
	}()
	if err := st.EnrollPrincipal(ctx, principal); err != nil {
		return fmt.Errorf("%w: enroll R7 Principal: %v", ErrProvision, err)
	}
	if err := st.RequirePrincipal(ctx, principal); err != nil {
		return fmt.Errorf("%w: verify R7 Principal: %v", ErrProvision, err)
	}
	return nil
}

func agencyPrincipalForValue(principal string) (agency.AgentPrincipalID, error) {
	if principal == "" {
		return agency.AgentPrincipalID{}, errors.New("durable Profile Principal is unavailable")
	}
	digest := sha256.Sum256([]byte("mnemon/r7/agent-principal/1\x00" + principal))
	return agency.NewAgentPrincipalID("principal:r7:" + hex.EncodeToString(digest[:16]))
}

func prepareProvision(options *ProvisionOptions) (provisionPlan, error) {
	workspace, err := validateDaemonWorkspace(options.Workspace)
	if err != nil {
		return provisionPlan{}, fmt.Errorf("%w: %v", ErrProvision, err)
	}
	runtimeKind, ok := model.RuntimeForHost(options.Host)
	if _, digestErr := model.ParseDigest(options.AssetRevision); !ok || digestErr != nil {
		return provisionPlan{}, fmt.Errorf("%w: Host or asset revision is invalid", ErrProvision)
	}
	if options.Clock == nil {
		options.Clock = wallClock{}
	}
	at, ok := canonicalAuthorityTime(options.Clock.Now())
	if !ok {
		return provisionPlan{}, fmt.Errorf("%w: clock is invalid", ErrProvision)
	}
	return provisionPlan{workspace: workspace, runtime: runtimeKind, at: at}, nil
}

func replayProvision(nodeState string, identity *Identity, credential model.Digest, credentialCreated bool,
	authority store.LocalAuthority, host model.HostKind, workspace string,
) (ProvisionResult, error) {
	if authority.Node.PeerID() != identity.PeerID() ||
		authority.Profile.WorkspaceRoot() != workspace || authority.Profile.CredentialHash() != credential {
		return ProvisionResult{}, fmt.Errorf("%w: durable identity differs from workspace projections", ErrProvision)
	}
	if authority.Profile.Enabled() && authority.Profile.Host() != host {
		return ProvisionResult{}, fmt.Errorf("%w: enabled Profile Host is %s, not %s",
			ErrProvision, authority.Profile.Host(), host)
	}
	return ProvisionResult{NodeState: nodeState, Node: authority.Node, Profile: authority.Profile,
		CredentialCreated: credentialCreated}, nil
}

func createProvisionAuthority(ctx context.Context, st *store.Store, nodeState string, identity *Identity,
	credential model.Digest, credentialCreated bool, options ProvisionOptions, plan provisionPlan,
) (ProvisionResult, error) {
	epoch, err := model.ParseOriginEpoch(derivedProvisionIdentifier("epoch", identity.PublicKey(), plan.workspace))
	if err != nil {
		return ProvisionResult{}, fmt.Errorf("%w: derive origin epoch: %v", ErrProvision, err)
	}
	principal := derivedProvisionIdentifier("principal", identity.PublicKey(), plan.workspace)
	nodeValue, err := model.NewNode(model.NodeSpec{PeerID: identity.PeerID(), OriginEpoch: epoch,
		NextOriginSequence: 1, ActiveAssetRevision: options.AssetRevision, CreatedAt: plan.at, UpdatedAt: plan.at})
	if err != nil {
		return ProvisionResult{}, fmt.Errorf("%w: create Node value: %v", ErrProvision, err)
	}
	profile, err := model.NewProfile(model.ProfileSpec{ID: model.TeamworkProfileID(), Principal: principal,
		WorkspaceRoot: plan.workspace, Host: options.Host, Runtime: plan.runtime, CredentialHash: credential,
		ActiveAssetRevision: options.AssetRevision, HandlingBudget: model.DefaultHandlingBudget().JSON(),
		CreatedAt: plan.at, UpdatedAt: plan.at})
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
