package cli

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi/nodecontrol"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestSetupFreshClassificationAcceptsOnlySafeAbsentStoreStates(t *testing.T) {
	_, nodeState := newSetupClassificationState(t)
	if allowed, err := setupCanInitialize(nodeState); err != nil || !allowed {
		t.Fatalf("missing database fresh classification = (%t, %v)", allowed, err)
	}
	profiles := filepath.Join(nodeState, "profiles")
	if err := os.Mkdir(profiles, 0o700); err != nil {
		t.Fatal(err)
	}
	credential := filepath.Join(profiles, model.TeamworkProfileID().String()+".token")
	if err := os.WriteFile(credential, []byte("corrupt\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if allowed, err := setupCanInitialize(nodeState); err != nil || allowed {
		t.Fatalf("corrupt credential fresh classification = (%t, %v)", allowed, err)
	}
	if err := os.Remove(credential); err != nil {
		t.Fatal(err)
	}
	if _, _, err := localapi.EnsureProfileCredential(nodeState); err != nil {
		t.Fatal(err)
	}
	if allowed, err := setupCanInitialize(nodeState); err != nil || !allowed {
		t.Fatalf("valid partial credential classification = (%t, %v)", allowed, err)
	}
}

func TestSetupFreshClassificationRequiresAnExistingWriterGuard(t *testing.T) {
	_, nodeState := newSetupClassificationState(t)
	databasePath := filepath.Join(nodeState, "node.db")
	st, err := store.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if allowed, err := setupCanInitialize(nodeState); err != nil || !allowed {
		t.Fatalf("exact owner-only empty Store classification = (%t, %v)", allowed, err)
	}
	if err := os.Remove(databasePath + ".writer.lock"); err != nil {
		t.Fatal(err)
	}
	if allowed, err := setupCanInitialize(nodeState); err == nil || allowed {
		t.Fatalf("missing writer guard classification = (%t, %v)", allowed, err)
	}
	if _, err := os.Lstat(databasePath + ".writer.lock"); !os.IsNotExist(err) {
		t.Fatalf("classification recreated missing writer guard: %v", err)
	}
}

func TestSetupFreshClassificationValidatesCredentialWithAnExactFreshStore(t *testing.T) {
	_, nodeState := newSetupClassificationState(t)
	st, err := store.Open(context.Background(), filepath.Join(nodeState, "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := localapi.EnsureProfileCredential(nodeState); err != nil {
		t.Fatal(err)
	}
	if allowed, err := setupCanInitialize(nodeState); err != nil || !allowed {
		t.Fatalf("fresh Store with valid credential = (%t, %v)", allowed, err)
	}
	credential := filepath.Join(nodeState, "profiles",
		model.TeamworkProfileID().String()+".token")
	if err := os.WriteFile(credential, []byte("corrupt\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if allowed, err := setupCanInitialize(nodeState); err != nil || allowed {
		t.Fatalf("fresh Store with corrupt credential = (%t, %v)", allowed, err)
	}
}

func TestSetupFreshClassificationRejectsInitializedAuthority(t *testing.T) {
	workspace, nodeState := newSetupClassificationState(t)
	if _, err := node.Provision(context.Background(), node.ProvisionOptions{
		Workspace: workspace, Host: model.HostCodex,
		AssetRevision: model.Sum([]byte("initialized setup classifier")).String(),
		Credentials:   nodecontrol.ProfileCredentials{},
	}); err != nil {
		t.Fatal(err)
	}
	if allowed, err := setupCanInitialize(nodeState); err != nil || allowed {
		t.Fatalf("initialized Store classification = (%t, %v)", allowed, err)
	}
}

func TestSetupFreshClassificationRejectsPartialUserVersionZero(t *testing.T) {
	_, nodeState := newSetupClassificationState(t)
	databasePath := filepath.Join(nodeState, "node.db")
	raw, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec("CREATE TABLE partial_authority (value TEXT NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(databasePath, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(databasePath+".writer.lock", nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if allowed, err := setupCanInitialize(nodeState); err == nil || allowed {
		t.Fatalf("partial user_version=0 classification = (%t, %v)", allowed, err)
	}
}

func TestSetupInitializesARealEmptyStoreThroughTheMainPath(t *testing.T) {
	fixture := newSetupFixture(t, assets.HostCodex, false)
	nodeState, err := node.PrepareNodeState(fixture.workspace)
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
	fixture.clientFailures = 1
	companion := &setupProvisionReplayCompanion{fixture: fixture}
	app := fixture.app()
	app.deps.canInitialize = func(path string) (bool, error) {
		fixture.record("can-initialize")
		return setupCanInitialize(path)
	}
	app.deps.newCompanion = func(ctx context.Context, workspace, version string) (setupCompanion, error) {
		fixture.record("new-companion")
		if ctx == nil || workspace != fixture.workspace || version != "test-r5" {
			t.Fatalf("companion boundary = (%v, %q, %q)", ctx, workspace, version)
		}
		return companion, nil
	}
	if exit := app.run(context.Background(), nil); exit != 0 || fixture.stderr.String() != "" {
		t.Fatalf("empty Store setup = exit %d stdout %q stderr %q", exit,
			fixture.stdout.String(), fixture.stderr.String())
	}
	if companion.initializeCalls != 1 {
		t.Fatalf("initialization calls = %d, want 1", companion.initializeCalls)
	}
	st, err = store.OpenExisting(context.Background(), filepath.Join(nodeState, "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	classification, classifyErr := st.ClassifyNodeInitialization(context.Background())
	closeErr := st.Close()
	if classifyErr != nil || closeErr != nil || classification != store.NodeInitializationExisting {
		t.Fatalf("initialized Store = (%d, %v, close %v)", classification, classifyErr, closeErr)
	}
	if info, err := os.Lstat(filepath.Join(nodeState, "mesh-endpoint.pending.json")); err != nil ||
		!info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("initialized pending endpoint = (%v, %v)", info, err)
	}
}

func newSetupClassificationState(t *testing.T) (string, string) {
	t.Helper()
	workspace, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	if err := os.MkdirAll(nodeState, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{filepath.Join(workspace, ".mnemon"),
		filepath.Join(workspace, ".mnemon", "harness")} {
		if err := os.Chmod(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return workspace, nodeState
}

func TestSetupAuthorityProvenanceReplaysOnlyOfflineAndRequiresExactAuthority(t *testing.T) {
	t.Run("online authority never initializes", func(t *testing.T) {
		fixture := newSetupFixture(t, assets.HostCodex, false)
		companion := &setupAuthorityProbeCompanion{fixture: fixture}
		observed, apiErr := fixture.app().observeSetupAuthority(context.Background(),
			filepath.Join(fixture.workspace, ".mnemon", "harness", "node"), companion)
		if apiErr != nil || !observed.found || observed.source != setupAuthorityOnline ||
			companion.initializeCalls != 0 || companion.inspectCalls != 0 {
			t.Fatalf("online observation = (%#v, %v), initialize=%d inspect=%d",
				observed, apiErr, companion.initializeCalls, companion.inspectCalls)
		}
	})

	for _, tc := range []struct {
		drift        string
		inspectCalls int
	}{
		{drift: "host", inspectCalls: 2},
		{drift: "revision", inspectCalls: 2},
		{drift: "receipt schema", inspectCalls: 1},
		{drift: "receipt status", inspectCalls: 1},
		{drift: "receipt created", inspectCalls: 1},
	} {
		tc := tc
		t.Run("offline rejects "+tc.drift+" drift", func(t *testing.T) {
			fixture := newSetupFixture(t, assets.HostCodex, false)
			fixture.readError = localapi.NewAPIError(localapi.CodeMnemondUnavailable,
				"injected offline daemon")
			companion := &setupAuthorityProbeCompanion{fixture: fixture, drift: tc.drift}
			observed, apiErr := fixture.app().observeSetupAuthority(context.Background(),
				filepath.Join(fixture.workspace, ".mnemon", "harness", "node"), companion)
			if apiErr == nil || observed.found || companion.initializeCalls != 1 ||
				companion.inspectCalls != tc.inspectCalls {
				t.Fatalf("offline drift observation = (%#v, %v), initialize=%d inspect=%d",
					observed, apiErr, companion.initializeCalls, companion.inspectCalls)
			}
		})
	}
}

func TestSetupOfflineAuthorityReplaysProvisionBeforeRecoveringAnAbsentEndpoint(t *testing.T) {
	fixture := newSetupFixture(t, assets.HostCodex, false)
	provisioned, err := node.Provision(context.Background(), node.ProvisionOptions{
		Workspace: fixture.workspace, Host: model.HostCodex,
		AssetRevision: fixture.revision, Credentials: nodecontrol.ProfileCredentials{},
	})
	if err != nil {
		t.Fatal(err)
	}
	pendingPath := filepath.Join(provisioned.NodeState, "mesh-endpoint.pending.json")
	if err := os.Remove(pendingPath); err != nil {
		t.Fatal(err)
	}
	durable, err := node.InspectAuthority(context.Background(), fixture.workspace,
		nodecontrol.ProfileCredentials{})
	if err != nil {
		t.Fatal(err)
	}
	fixture.authority, err = nodecontrol.AuthorityResponse(durable)
	if err != nil {
		t.Fatal(err)
	}
	fixture.readError = localapi.NewAPIError(localapi.CodeMnemondUnavailable,
		"injected offline daemon")
	companion := &setupProvisionReplayCompanion{fixture: fixture}
	app := fixture.app()
	app.deps.newCompanion = func(ctx context.Context, workspace,
		version string,
	) (setupCompanion, error) {
		fixture.record("new-companion")
		if ctx == nil || workspace != fixture.workspace || version != "test-r5" {
			t.Fatalf("companion boundary = (%v, %q, %q)", ctx, workspace, version)
		}
		return companion, nil
	}

	exit := app.run(context.Background(), nil)
	if exit != 0 || fixture.stderr.String() != "" || fixture.stdout.String() == "" {
		t.Fatalf("crash replay setup = exit %d stdout %q stderr %q", exit,
			fixture.stdout.String(), fixture.stderr.String())
	}
	if companion.initializeCalls != 1 {
		t.Fatalf("Provision replays = %d, want 1", companion.initializeCalls)
	}
	if info, err := os.Lstat(pendingPath); err != nil || !info.Mode().IsRegular() ||
		info.Mode().Perm() != 0o600 {
		t.Fatalf("recovered pending endpoint = (%v, %v)", info, err)
	}
	fixture.wantOrder(t,
		"cwd", "load-bundle", "new-preflight", "new-companion", "bootstrap", "lock",
		"new-client", "read-authority", "inspect", "initialize:codex", "inspect",
		"inspect-host:codex", "install-bundle", "verify-absent:claude-code",
		"install-projection:codex", "verify-projection:codex", "verify-activation:codex",
		"new-gate", "activate:codex", "ensure", "unlock")
}

type setupAuthorityProbeCompanion struct {
	fixture         *setupFixture
	drift           string
	initializeCalls int
	inspectCalls    int
}

func (companion *setupAuthorityProbeCompanion) Initialize(_ context.Context,
	host model.HostKind, revision string,
) (companionInitializeReceipt, error) {
	companion.initializeCalls++
	receipt := companionInitializeReceipt{AssetRevision: revision, Host: string(host),
		SchemaVersion: model.SchemaVersion, Status: "initialized"}
	switch companion.drift {
	case "receipt schema":
		receipt.SchemaVersion++
	case "receipt status":
		receipt.Status = "ready"
	case "receipt created":
		receipt.Created = true
	}
	return receipt, nil
}

func (companion *setupAuthorityProbeCompanion) Inspect(context.Context) (
	localapi.AuthorityResponse, error,
) {
	companion.inspectCalls++
	if companion.inspectCalls != 2 {
		return companion.fixture.authority, nil
	}
	switch companion.drift {
	case "host":
		return setupTestAuthority(companion.fixture.t, assets.HostClaudeCode, false,
			companion.fixture.authority.AssetRevision), nil
	case "revision":
		return setupTestAuthority(companion.fixture.t, assets.HostCodex, false,
			model.Sum([]byte("drifted offline revision")).String()), nil
	default:
		return companion.fixture.authority, nil
	}
}

func (companion *setupAuthorityProbeCompanion) ConfirmOffline(ctx context.Context,
	expected node.Authority,
) (node.Authority, error) {
	return (&fakeSetupCompanion{fixture: companion.fixture}).ConfirmOffline(ctx, expected)
}

func (companion *setupAuthorityProbeCompanion) Activate(ctx context.Context,
	host model.HostKind, revision string, expectedUpdatedAt time.Time,
) (companionLifecycleReceipt, error) {
	return (&fakeSetupCompanion{fixture: companion.fixture}).Activate(ctx, host, revision,
		expectedUpdatedAt)
}

func (companion *setupAuthorityProbeCompanion) Deactivate(ctx context.Context,
	host model.HostKind, revision string, expectedUpdatedAt time.Time,
) (companionLifecycleReceipt, error) {
	return (&fakeSetupCompanion{fixture: companion.fixture}).Deactivate(ctx, host, revision,
		expectedUpdatedAt)
}

type setupProvisionReplayCompanion struct {
	fixture         *setupFixture
	initializeCalls int
}

func (companion *setupProvisionReplayCompanion) Initialize(ctx context.Context,
	host model.HostKind, revision string,
) (companionInitializeReceipt, error) {
	companion.fixture.record("initialize:" + string(host))
	companion.initializeCalls++
	result, err := node.Provision(ctx, node.ProvisionOptions{Workspace: companion.fixture.workspace,
		Host: host, AssetRevision: revision, Credentials: nodecontrol.ProfileCredentials{}})
	if err != nil {
		return companionInitializeReceipt{}, err
	}
	return companionInitializeReceipt{AssetRevision: result.Profile.ActiveAssetRevision(),
		Created: result.Created, Host: string(result.Profile.Host()),
		SchemaVersion: model.SchemaVersion, Status: "initialized"}, nil
}

func (companion *setupProvisionReplayCompanion) Inspect(ctx context.Context) (
	localapi.AuthorityResponse, error,
) {
	companion.fixture.record("inspect")
	authority, err := node.InspectAuthority(ctx, companion.fixture.workspace,
		nodecontrol.ProfileCredentials{})
	if err != nil {
		return localapi.AuthorityResponse{}, err
	}
	response, err := nodecontrol.AuthorityResponse(authority)
	if err == nil {
		companion.fixture.authority = response
	}
	return response, err
}

func (companion *setupProvisionReplayCompanion) ConfirmOffline(ctx context.Context,
	expected node.Authority,
) (node.Authority, error) {
	return (&fakeSetupCompanion{fixture: companion.fixture}).ConfirmOffline(ctx, expected)
}

func (companion *setupProvisionReplayCompanion) Activate(_ context.Context,
	host model.HostKind, revision string, expectedUpdatedAt time.Time,
) (companionLifecycleReceipt, error) {
	companion.fixture.record("activate:" + string(host))
	updatedAt := expectedUpdatedAt.Add(time.Second)
	companion.fixture.authority = setupTestAuthorityAt(companion.fixture.t, assets.Host(host),
		true, revision, updatedAt)
	return companionLifecycleReceipt{AssetRevision: revision, Changed: true,
		Host: string(host), SchemaVersion: model.SchemaVersion, Status: "active",
		UpdatedAt: updatedAt.Format(time.RFC3339Nano)}, nil
}

func (companion *setupProvisionReplayCompanion) Deactivate(_ context.Context,
	host model.HostKind, revision string, expectedUpdatedAt time.Time,
) (companionLifecycleReceipt, error) {
	companion.fixture.record("deactivate:" + string(host))
	updatedAt := expectedUpdatedAt.Add(time.Second)
	companion.fixture.authority = setupTestAuthorityAt(companion.fixture.t, assets.Host(host),
		false, revision, updatedAt)
	return companionLifecycleReceipt{AssetRevision: revision, Changed: true,
		Host: string(host), SchemaVersion: model.SchemaVersion, Status: "inactive",
		UpdatedAt: updatedAt.Format(time.RFC3339Nano)}, nil
}
