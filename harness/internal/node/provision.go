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

const (
	defaultProvisionMeshListener = "/ip4/0.0.0.0/tcp/0"
	provisionEnsureDeadline      = 30 * time.Second
)

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

type provisionCandidate struct {
	identity          *Identity
	authority         store.LocalAuthority
	credentialCreated bool
}

type provisionPlan struct {
	options              ProvisionOptions
	workspace, nodeState string
	runtime              model.RuntimeKind
	at                   time.Time
}

type provisionEnsureOwner interface {
	acquire(context.Context, string) (*ensureLock, error)
	close(*ensureLock) error
}

type systemProvisionEnsureOwner struct{}

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

// Provision initializes or strictly replays the inactive Node authority and
// its private mesh bootstrap preimage. ensure.lock is the outer lifecycle
// lease; each Store writer is closed before identity, credential, or endpoint
// filesystem authority is inspected or mutated.
func Provision(ctx context.Context, options ProvisionOptions) (ProvisionResult, error) {
	return provision(ctx, options, systemProvisionEnsureOwner{})
}

func provision(ctx context.Context, options ProvisionOptions,
	owner provisionEnsureOwner,
) (result ProvisionResult, err error) {
	if owner == nil {
		return ProvisionResult{}, fmt.Errorf("%w: ensure lock owner is unavailable", ErrProvision)
	}
	plan, err := prepareProvision(ctx, options)
	if err != nil {
		return ProvisionResult{}, err
	}
	lock, err := owner.acquire(ctx, plan.nodeState)
	if err != nil {
		return ProvisionResult{}, err
	}
	defer func() {
		if closeErr := owner.close(lock); closeErr != nil {
			result = ProvisionResult{}
			err = errors.Join(err, fmt.Errorf("%w: release ensure lock: %w", ErrProvision, closeErr))
		}
	}()
	first, err := readProvisionStoreSnapshot(ctx, plan.nodeState)
	if err != nil {
		return ProvisionResult{}, fmt.Errorf("%w: inspect Store: %w", ErrProvision, err)
	}
	candidate, err := stageProvisionFilesystem(plan.nodeState, plan.workspace, plan.runtime, plan.at,
		plan.options, first)
	if err != nil {
		return ProvisionResult{}, fmt.Errorf("%w: stage projections: %w", ErrProvision, err)
	}
	if err := provisionContextError(ctx, "stage projections"); err != nil {
		return ProvisionResult{}, err
	}
	desired, frozen, err := inspectProvisionMeshEndpoint(plan.nodeState, candidate.identity.PeerID())
	if err != nil {
		return ProvisionResult{}, fmt.Errorf("%w: inspect mesh endpoint: %w", ErrProvision, err)
	}
	result, err = settleProvisionStore(ctx, plan.nodeState, first, candidate, frozen)
	if err != nil {
		return ProvisionResult{}, fmt.Errorf("%w: settle Store: %w", ErrProvision, err)
	}
	if err := provisionContextError(ctx, "publish mesh endpoint"); err != nil {
		return ProvisionResult{}, err
	}
	if err := reconcileProvisionMeshEndpoint(plan.nodeState, desired, frozen); err != nil {
		return ProvisionResult{}, fmt.Errorf("%w: reconcile mesh endpoint: %w", ErrProvision, err)
	}
	if err := provisionContextError(ctx, "complete"); err != nil {
		return ProvisionResult{}, err
	}
	return result, nil
}

func prepareProvision(ctx context.Context, options ProvisionOptions) (provisionPlan, error) {
	if err := validateProvisionAuthority(ctx, options.Credentials); err != nil {
		return provisionPlan{}, err
	}
	workspace, err := validateDaemonWorkspace(options.Workspace)
	if err != nil {
		return provisionPlan{}, fmt.Errorf("%w: %v", ErrProvision, err)
	}
	runtime, ok := model.RuntimeForHost(options.Host)
	if _, digestErr := model.ParseDigest(options.AssetRevision); !ok || digestErr != nil {
		return provisionPlan{}, fmt.Errorf("%w: Host or asset revision is invalid", ErrProvision)
	}
	if options.Clock == nil {
		options.Clock = wallClock{}
	}
	at := options.Clock.Now().Round(0).UTC()
	if at.IsZero() || at.UnixNano() <= 0 || !time.Unix(0, at.UnixNano()).UTC().Equal(at) {
		return provisionPlan{}, fmt.Errorf("%w: clock is invalid", ErrProvision)
	}
	if err := provisionContextError(ctx, "validate"); err != nil {
		return provisionPlan{}, err
	}
	nodeState, err := ensureProvisionState(workspace)
	return provisionPlan{options: options, workspace: workspace, nodeState: nodeState,
		runtime: runtime, at: at}, err
}

func stageProvisionFilesystem(nodeState, workspace string, runtime model.RuntimeKind, at time.Time,
	options ProvisionOptions, first provisionStoreSnapshot,
) (provisionCandidate, error) {
	if first.state == store.NodeInitializationExisting {
		identity, err := LoadIdentity(nodeState)
		if err != nil {
			return provisionCandidate{}, err
		}
		profile := first.authority.Profile
		if err := options.Credentials.Verify(nodeState, profile.CredentialHash()); err != nil {
			return provisionCandidate{}, err
		}
		if first.authority.Node.PeerID() != identity.PeerID() || profile.WorkspaceRoot() != workspace {
			return provisionCandidate{}, errors.New("durable identity differs from workspace projections")
		}
		if profile.Enabled() && profile.Host() != options.Host {
			return provisionCandidate{}, fmt.Errorf("enabled Profile Host is %s, not %s", profile.Host(), options.Host)
		}
		return provisionCandidate{identity: identity, authority: first.authority}, nil
	}
	identity, err := EnsureIdentity(nodeState)
	if err != nil {
		return provisionCandidate{}, err
	}
	credential, created, err := options.Credentials.Ensure(nodeState)
	if err != nil {
		return provisionCandidate{}, err
	}
	epoch, err := model.ParseOriginEpoch(derivedProvisionIdentifier("epoch", identity.PublicKey(), workspace))
	if err != nil {
		return provisionCandidate{}, err
	}
	nodeValue, err := model.NewNode(model.NodeSpec{PeerID: identity.PeerID(), OriginEpoch: epoch,
		NextOriginSequence: 1, ActiveAssetRevision: options.AssetRevision, CreatedAt: at, UpdatedAt: at})
	if err != nil {
		return provisionCandidate{}, err
	}
	profile, err := model.NewProfile(model.ProfileSpec{ID: model.TeamworkProfileID(),
		Principal: derivedProvisionIdentifier("principal", identity.PublicKey(), workspace), WorkspaceRoot: workspace,
		Host: options.Host, Runtime: runtime, CredentialHash: credential, ActiveAssetRevision: options.AssetRevision,
		HandlingBudget: model.DefaultHandlingBudget().JSON(), CreatedAt: at, UpdatedAt: at})
	return provisionCandidate{identity: identity, authority: store.LocalAuthority{Node: nodeValue, Profile: profile},
		credentialCreated: created}, err
}

func acquireProvisionEnsureLock(ctx context.Context, nodeState string) (*ensureLock, error) {
	if err := provisionContextError(ctx, "acquire ensure lock"); err != nil {
		return nil, err
	}
	bounded, cancel := context.WithTimeout(ctx, provisionEnsureDeadline)
	defer cancel()
	lock, err := acquireEnsureLock(bounded, nodeState, daemonLifecyclePoll)
	if err != nil {
		if cause := context.Cause(bounded); cause != nil {
			err = errors.Join(err, bounded.Err(), cause)
		}
		return nil, fmt.Errorf("%w: acquire ensure lock: %w", ErrProvision, err)
	}
	if cause := context.Cause(bounded); cause != nil {
		closeErr := lock.close()
		return nil, fmt.Errorf("%w: acquire ensure lock: %w", ErrProvision,
			errors.Join(bounded.Err(), cause, closeErr))
	}
	return lock, nil
}

func (systemProvisionEnsureOwner) acquire(ctx context.Context, nodeState string) (*ensureLock, error) {
	return acquireProvisionEnsureLock(ctx, nodeState)
}

func (systemProvisionEnsureOwner) close(lock *ensureLock) error { return lock.close() }

func validateProvisionAuthority(ctx context.Context, credentials ProfileCredentialProvisioner) error {
	if ctx == nil || isNilNodeInterface(credentials) {
		return fmt.Errorf("%w: context or credential authority is unavailable", ErrProvision)
	}
	return provisionContextError(ctx, "validate")
}

func provisionContextError(ctx context.Context, operation string) error {
	if cause := context.Cause(ctx); cause != nil {
		return fmt.Errorf("%w: %s: %w", ErrProvision, operation, errors.Join(ctx.Err(), cause))
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
