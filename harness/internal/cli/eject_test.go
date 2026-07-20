package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/integration"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi/nodecontrol"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

func TestEjectActiveProfileUsesTheClosedLifecycleAndEmitsCanonicalReceipt(t *testing.T) {
	fixture := newEjectFixture(t, true)
	fixture.projection = integration.EjectHostProjectionReceipt{Host: assets.HostCodex,
		Revision: fixture.authority.AssetRevision, RemovedFiles: 3,
		RegistrationRemoved: true}

	exit, stdout, stderr := fixture.run()
	if exit != 0 || stderr != "" {
		t.Fatalf("eject = exit %d stdout %q stderr %q", exit, stdout, stderr)
	}
	want := ejectReceipt{AssetRevision: fixture.initialRevision, Host: "codex",
		PeerID: fixture.authority.PeerID, RegistrationRemoved: true, RemovedFiles: 3,
		SchemaVersion: localapi.SchemaVersion, Status: "ejected"}
	raw, err := model.CanonicalMarshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if stdout != string(raw)+"\n" || strings.Count(stdout, "\n") != 1 ||
		strings.Contains(strings.ToLower(stdout), "token") {
		t.Fatalf("eject receipt = %q, want %q", stdout, string(raw)+"\n")
	}
	fixture.wantOrder(t, "cwd", "load-bundle", "new-companion", "lock", "new-client",
		"read-authority", "install-bundle", "acquire-lifecycle", "quiesce",
		"deactivate:codex", "eject-projection:codex", "verify-absent:codex",
		"close-lifecycle", "unlock")
	if fixture.authority.Enabled || fixture.authority.AssetRevision != fixture.initialRevision {
		t.Fatalf("eject did not retain disabled durable authority: %#v", fixture.authority)
	}
}

func TestEjectDisabledAbsentProjectionReplaysWithoutActivationOrHostBinary(t *testing.T) {
	fixture := newEjectFixture(t, false)
	fixture.clientOffline = true
	fixture.projection = integration.EjectHostProjectionReceipt{Host: assets.HostCodex,
		Revision: fixture.authority.AssetRevision, Replayed: true}

	exit, stdout, stderr := fixture.run()
	if exit != 0 || stderr != "" || !strings.Contains(stdout, `"replayed":true`) ||
		!strings.Contains(stdout, `"status":"ejected"`) {
		t.Fatalf("replayed eject = exit %d stdout %q stderr %q", exit, stdout, stderr)
	}
	fixture.wantOrder(t, "cwd", "load-bundle", "new-companion", "lock", "new-client",
		"read-authority", "inspect", "install-bundle", "acquire-lifecycle", "quiesce",
		"eject-projection:codex", "verify-absent:codex", "close-lifecycle", "unlock")
	if fixture.called("deactivate:codex") {
		t.Fatalf("disabled replay mutated authority: %v", fixture.order)
	}
}

func TestEjectRejectsAnotherHostBeforeLifecycleOrProjectionMutation(t *testing.T) {
	fixture := newEjectFixture(t, true)
	exit, stdout, stderr := fixture.run("--host", "claude-code")
	if exit != 4 || stdout != "" || stderr !=
		"profile_host_mismatch: managed Profile is bound to another Host\n" {
		t.Fatalf("Host mismatch = exit %d stdout %q stderr %q", exit, stdout, stderr)
	}
	fixture.wantOrder(t, "cwd", "load-bundle", "new-companion", "lock", "new-client",
		"read-authority", "unlock")
}

func TestEjectFailureBoundariesKeepAuthorityAndAssetsFailClosed(t *testing.T) {
	t.Run("busy admission keeps active daemon and assets", func(t *testing.T) {
		fixture := newEjectFixture(t, true)
		fixture.fail["quiesce"] = fmt.Errorf("%w: %w", node.ErrDaemonLifecycle,
			localapi.NewAPIError(localapi.CodeOperationPending, "Agent authority is active"))
		exit, stdout, stderr := fixture.run()
		if exit != 5 || stdout != "" || stderr !=
			"operation_pending: managed Profile has active Agent work; retry eject after it becomes idle\n" {
			t.Fatalf("busy eject = exit %d stdout %q stderr %q", exit, stdout, stderr)
		}
		if !fixture.authority.Enabled || fixture.called("deactivate:codex") ||
			fixture.called("eject-projection:codex") || fixture.count("close-lifecycle") != 1 {
			t.Fatalf("busy eject escaped lifecycle: authority=%#v order=%v",
				fixture.authority, fixture.order)
		}
	})

	t.Run("projection drift disables Agent and preserves Host assets", func(t *testing.T) {
		fixture := newEjectFixture(t, true)
		fixture.projectionErr = fmt.Errorf("%w: injected managed file drift",
			integration.ErrProjectionConflict)
		exit, stdout, stderr := fixture.run()
		if exit != 3 || stdout != "" || stderr !=
			"asset_revision_mismatch: managed Agent is disabled, but changed Host assets were preserved; run doctor before switching Host\n" {
			t.Fatalf("drifted eject = exit %d stdout %q stderr %q", exit, stdout, stderr)
		}
		if fixture.authority.Enabled || fixture.count("deactivate:codex") != 1 ||
			fixture.count("eject-projection:codex") != 1 ||
			fixture.called("activate:codex") {
			t.Fatalf("drifted eject was not forward-only: authority=%#v order=%v",
				fixture.authority, fixture.order)
		}
	})

	t.Run("deactivation response loss stops before Host mutation", func(t *testing.T) {
		fixture := newEjectFixture(t, true)
		fixture.fail["deactivate-after-commit"] = errors.New("injected response loss")
		exit, stdout, stderr := fixture.run()
		if exit == 0 || stdout != "" || stderr == "" || fixture.authority.Enabled ||
			fixture.called("eject-projection:codex") || fixture.count("close-lifecycle") != 1 {
			t.Fatalf("response-loss eject = exit %d stdout %q stderr %q authority=%#v order=%v",
				exit, stdout, stderr, fixture.authority, fixture.order)
		}
	})

	t.Run("final absence proof fails after safe forward removal", func(t *testing.T) {
		fixture := newEjectFixture(t, true)
		fixture.projection = integration.EjectHostProjectionReceipt{Host: assets.HostCodex,
			Revision: fixture.authority.AssetRevision, RemovedFiles: 3,
			RegistrationRemoved: true}
		fixture.fail["verify-absent"] = errors.New("injected residual managed registration")
		exit, stdout, stderr := fixture.run()
		if exit != 3 || stdout != "" || stderr !=
			"asset_revision_mismatch: managed Agent is disabled, but Host projection absence could not be proved; run doctor before switching Host\n" ||
			fixture.authority.Enabled || fixture.count("eject-projection:codex") != 1 ||
			fixture.count("verify-absent:codex") != 1 {
			t.Fatalf("absence failure = exit %d stdout %q stderr %q authority=%#v order=%v",
				exit, stdout, stderr, fixture.authority, fixture.order)
		}
	})
}

func TestEjectParsingAndClosedDependencyFailures(t *testing.T) {
	for _, args := range [][]string{{"--host"}, {"--host", "other"}, {"--project-root"},
		{"--host", "auto", "--host", "codex"}, {"--json"}} {
		if _, apiErr := parseEjectRequest(args); apiErr == nil ||
			apiErr.Code != localapi.CodeInvalidArgument || apiErr.ExitStatus() != 2 {
			t.Fatalf("parseEjectRequest(%v) = %#v", args, apiErr)
		}
	}
	request, apiErr := parseEjectRequest(nil)
	if apiErr != nil || request.host != "auto" || request.projectRoot != "" {
		t.Fatalf("default eject request = (%#v, %#v)", request, apiErr)
	}

	fixture := newEjectFixture(t, true)
	fixture.fail["lock"] = errors.New("missing managed Node")
	exit, stdout, stderr := fixture.run()
	if exit != 3 || stdout != "" || stderr !=
		"authentication_failed: managed Node is not initialized or is unsafe\n" ||
		fixture.called("new-client") {
		t.Fatalf("missing Node eject = exit %d stdout %q stderr %q order=%v",
			exit, stdout, stderr, fixture.order)
	}
}

type ejectFixture struct {
	t               *testing.T
	workspace       string
	bundle          assets.Bundle
	authority       localapi.AuthorityResponse
	initialRevision string
	clientOffline   bool
	projection      integration.EjectHostProjectionReceipt
	projectionErr   error
	fail            map[string]error
	order           []string
	stdout          bytes.Buffer
	stderr          bytes.Buffer
}

func newEjectFixture(t *testing.T, enabled bool) *ejectFixture {
	t.Helper()
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	authority := ejectTestAuthority(t, enabled, bundle.Manifest().AssetRevision,
		time.Date(2026, time.July, 17, 6, 0, 0, 0, time.UTC))
	return &ejectFixture{t: t, workspace: workspace, bundle: bundle, authority: authority,
		initialRevision: authority.AssetRevision, fail: make(map[string]error)}
}

func (fixture *ejectFixture) app() *ejectApp {
	t := fixture.t
	companion := &fakeEjectCompanion{fixture: fixture}
	return &ejectApp{stdout: &fixture.stdout, stderr: &fixture.stderr, version: "test-r5",
		deps: ejectDependencies{
			workingDirectory: func() (string, error) {
				fixture.record("cwd")
				return fixture.workspace, fixture.fail["cwd"]
			},
			loadBundle: func() (assets.Bundle, error) {
				fixture.record("load-bundle")
				return fixture.bundle, fixture.fail["load-bundle"]
			},
			newCompanion: func(ctx context.Context, workspace, version string) (
				ejectCompanion, error,
			) {
				fixture.record("new-companion")
				if ctx == nil || workspace != fixture.workspace || version != "test-r5" {
					t.Fatalf("eject companion composition = (%v, %q, %q)", ctx, workspace, version)
				}
				return companion, fixture.fail["new-companion"]
			},
			acquireLock: func(ctx context.Context, nodeState string) (io.Closer, error) {
				fixture.record("lock")
				fixture.wantNodeState(nodeState)
				if err := fixture.fail["lock"]; err != nil {
					return nil, err
				}
				return fakeEjectCloser{fixture: fixture, stage: "unlock"}, nil
			},
			newClient: func(nodeState string) (ejectAuthorityClient, error) {
				fixture.record("new-client")
				fixture.wantNodeState(nodeState)
				if err := fixture.fail["new-client"]; err != nil {
					return nil, err
				}
				return &fakeEjectClient{fixture: fixture}, nil
			},
			installBundle: func(nodeState string, _ assets.Bundle) error {
				fixture.record("install-bundle")
				fixture.wantNodeState(nodeState)
				return fixture.fail["install-bundle"]
			},
			ejectProjection: func(workspace, nodeState string, host assets.Host,
				_ assets.Bundle,
			) (integration.EjectHostProjectionReceipt, error) {
				fixture.record("eject-projection:" + string(host))
				if workspace != fixture.workspace {
					t.Fatalf("eject workspace = %q", workspace)
				}
				fixture.wantNodeState(nodeState)
				return fixture.projection, fixture.projectionErr
			},
			verifyAbsent: func(workspace, nodeState string, host assets.Host,
				_ assets.Bundle,
			) error {
				fixture.record("verify-absent:" + string(host))
				if workspace != fixture.workspace {
					t.Fatalf("absence workspace = %q", workspace)
				}
				fixture.wantNodeState(nodeState)
				return fixture.fail["verify-absent"]
			},
			acquireLifecycle: func(ctx context.Context,
				options node.DaemonLifecycleOptions,
			) (setupDaemonLifecycle, error) {
				fixture.record("acquire-lifecycle")
				if ctx == nil || options.Workspace != fixture.workspace {
					t.Fatalf("eject lifecycle composition = (%v, %#v)", ctx, options)
				}
				fixture.wantNodeState(options.NodeState)
				if err := fixture.fail["acquire-lifecycle"]; err != nil {
					return nil, err
				}
				return &fakeEjectLifecycle{fixture: fixture}, nil
			},
		}}
}

func (fixture *ejectFixture) run(args ...string) (int, string, string) {
	fixture.t.Helper()
	exit := fixture.app().run(context.Background(), args)
	return exit, fixture.stdout.String(), fixture.stderr.String()
}

func (fixture *ejectFixture) record(stage string) { fixture.order = append(fixture.order, stage) }

func (fixture *ejectFixture) called(stage string) bool { return fixture.count(stage) != 0 }

func (fixture *ejectFixture) count(stage string) int {
	count := 0
	for _, value := range fixture.order {
		if value == stage {
			count++
		}
	}
	return count
}

func (fixture *ejectFixture) wantOrder(t *testing.T, want ...string) {
	t.Helper()
	if !reflect.DeepEqual(fixture.order, want) {
		t.Fatalf("eject order =\n%v\nwant\n%v", fixture.order, want)
	}
}

func (fixture *ejectFixture) wantNodeState(path string) {
	fixture.t.Helper()
	want := filepath.Join(fixture.workspace, ".mnemon", "harness", "node")
	if path != want {
		fixture.t.Fatalf("eject Node state = %q, want %q", path, want)
	}
}

type fakeEjectClient struct{ fixture *ejectFixture }

func (client *fakeEjectClient) ReadAuthority(context.Context) (localapi.AuthorityResponse,
	*localapi.APIError,
) {
	client.fixture.record("read-authority")
	if client.fixture.clientOffline {
		return localapi.AuthorityResponse{}, localapi.NewAPIError(
			localapi.CodeMnemondUnavailable, "offline")
	}
	return client.fixture.authority, nil
}

func (*fakeEjectClient) ShutdownForMutation(context.Context,
	localapi.AuthorityResponse,
) (localapi.ShutdownResponse, *localapi.APIError) {
	return localapi.ShutdownResponse{}, localapi.NewAPIError(localapi.CodeInternal,
		"fake lifecycle owns mutation shutdown")
}

type fakeEjectCompanion struct{ fixture *ejectFixture }

func (companion *fakeEjectCompanion) Inspect(context.Context) (localapi.AuthorityResponse,
	error,
) {
	companion.fixture.record("inspect")
	return companion.fixture.authority, companion.fixture.fail["inspect"]
}

func (companion *fakeEjectCompanion) ConfirmOffline(_ context.Context,
	expected node.Authority,
) (node.Authority, error) {
	if expected != ejectTestNodeAuthority(companion.fixture.t, companion.fixture.authority) {
		return node.Authority{}, errors.New("offline authority mismatch")
	}
	return expected, companion.fixture.fail["confirm-offline"]
}

func (companion *fakeEjectCompanion) Deactivate(_ context.Context, host model.HostKind,
	revision string, expectedUpdatedAt time.Time,
) (companionLifecycleReceipt, error) {
	companion.fixture.record("deactivate:" + string(host))
	if err := companion.fixture.fail["deactivate"]; err != nil {
		return companionLifecycleReceipt{}, err
	}
	wantAt, err := parseSetupAuthorityTime(companion.fixture.authority.UpdatedAt)
	if err != nil || expectedUpdatedAt != wantAt || revision != companion.fixture.authority.AssetRevision {
		return companionLifecycleReceipt{}, errors.New("eject deactivation authority mismatch")
	}
	deactivatedAt := expectedUpdatedAt.Add(time.Second)
	companion.fixture.authority = ejectTestAuthority(companion.fixture.t, false, revision,
		deactivatedAt)
	if err := companion.fixture.fail["deactivate-after-commit"]; err != nil {
		return companionLifecycleReceipt{}, err
	}
	return companionLifecycleReceipt{AssetRevision: revision, Changed: true, Host: string(host),
		SchemaVersion: model.SchemaVersion, Status: "inactive",
		UpdatedAt: deactivatedAt.Format(time.RFC3339Nano)}, nil
}

type fakeEjectLifecycle struct {
	fixture *ejectFixture
	closed  bool
}

func (lifecycle *fakeEjectLifecycle) Quiesce(ctx context.Context,
	client node.DaemonLifecycleClient, confirmer node.DaemonOfflineConfirmer,
	expected node.Authority,
) (node.Authority, error) {
	lifecycle.fixture.record("quiesce")
	if lifecycle.closed || ctx == nil || client == nil || confirmer == nil ||
		expected != ejectTestNodeAuthority(lifecycle.fixture.t, lifecycle.fixture.authority) {
		return node.Authority{}, errors.New("invalid eject quiescence")
	}
	if err := lifecycle.fixture.fail["quiesce"]; err != nil {
		return node.Authority{}, err
	}
	return expected, nil
}

func (*fakeEjectLifecycle) Ensure(context.Context,
	node.DaemonEnsureOptions,
) (node.DaemonEnsureResult, error) {
	return node.DaemonEnsureResult{}, errors.New("eject must never ensure a daemon")
}

func (lifecycle *fakeEjectLifecycle) Close() error {
	lifecycle.fixture.record("close-lifecycle")
	if lifecycle.closed {
		return nil
	}
	lifecycle.closed = true
	return lifecycle.fixture.fail["close-lifecycle"]
}

type fakeEjectCloser struct {
	fixture *ejectFixture
	stage   string
}

func (closer fakeEjectCloser) Close() error {
	closer.fixture.record(closer.stage)
	return closer.fixture.fail[closer.stage]
}

func ejectTestAuthority(t *testing.T, enabled bool, revision string,
	updatedAt time.Time,
) localapi.AuthorityResponse {
	t.Helper()
	peerID, err := model.ParsePeerID("peer-eject-test")
	if err != nil {
		t.Fatal(err)
	}
	response, err := localapi.NewAuthorityResponse(localapi.AuthoritySnapshot{
		Host: model.HostCodex, Runtime: model.RuntimeCodexAppServer, Enabled: enabled,
		AssetRevision: revision, ActiveAssetRevision: revision, UpdatedAt: updatedAt,
		PeerID: peerID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func ejectTestNodeAuthority(t *testing.T, response localapi.AuthorityResponse) node.Authority {
	t.Helper()
	authority, err := nodecontrol.Authority(response)
	if err != nil {
		t.Fatal(err)
	}
	return authority
}
