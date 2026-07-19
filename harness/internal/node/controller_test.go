package node

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/integration"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type controllerTestClock struct{ now time.Time }

func (clock controllerTestClock) Now() time.Time { return clock.now }

func TestControllerServesOwnerOnlyManagedRoutesFromOneStore(t *testing.T) {
	workspace, err := os.MkdirTemp("/tmp", "mnemon-r5-controller-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(workspace) })
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	if err := os.MkdirAll(nodeState, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(nodeState, 0o700); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(nodeState, "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := integration.InstallNodeBundle(nodeState, bundle); err != nil {
		t.Fatal(err)
	}
	if _, err := integration.InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 7, 17, 3, 0, 0, 0, time.UTC)
	peerID, _ := model.ParsePeerID("peer-controller-local")
	epoch, _ := model.ParseOriginEpoch("epoch-controller-local")
	nodeValue, err := model.NewNode(model.NodeSpec{PeerID: peerID, OriginEpoch: epoch,
		NextOriginSequence: 1, ActiveAssetRevision: bundle.Manifest().AssetRevision,
		CreatedAt: at, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	credential := bytes.Repeat([]byte{0x65}, 32)
	disabled, err := model.NewProfile(model.ProfileSpec{ID: model.TeamworkProfileID(),
		Principal: "principal-controller", WorkspaceRoot: workspace, Host: model.HostCodex,
		Runtime: model.RuntimeCodexAppServer, CredentialHash: model.Sum(credential),
		ActiveAssetRevision: bundle.Manifest().AssetRevision, HandlingBudget: model.DefaultHandlingBudget().JSON(),
		CreatedAt: at, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.InitializeNode(ctx, nodeValue, disabled); err != nil {
		t.Fatal(err)
	}
	enabledSpec := disabled.Spec()
	enabledSpec.Enabled = true
	enabledSpec.UpdatedAt = at.Add(time.Second)
	enabled, _ := model.NewProfile(enabledSpec)
	if _, err := st.ActivateProfile(ctx, enabled, disabled.UpdatedAt(), enabled.UpdatedAt()); err != nil {
		t.Fatal(err)
	}
	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := event.NewEd25519Signer(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	controller, err := NewController(context.Background(), ControllerOptions{NodeState: nodeState, Workspace: workspace,
		Store: st, Profile: enabled, Signer: signer, Clock: controllerTestClock{enabled.UpdatedAt()},
		Install: testInstallationVerifier(workspace, nodeState, bundle)})
	if err != nil {
		t.Fatal(err)
	}
	writeControllerProfileToken(t, nodeState, credential)
	serveCtx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- controller.Serve(serveCtx) }()
	waitControllerSocket(t, filepath.Join(nodeState, "control.sock"), served)
	client, err := localapi.NewClient(nodeState)
	if err != nil {
		t.Fatal(err)
	}
	health, apiErr := client.ProbeHealth(context.Background())
	if apiErr != nil || health.Status != "ready" ||
		health.AssetRevision != bundle.Manifest().AssetRevision {
		t.Fatalf("ProbeHealth() = (%#v, %v)", health, apiErr)
	}
	status, apiErr := client.ReadStatus(context.Background())
	if apiErr != nil || status.Status != "degraded" || status.Activation.State != "ready" ||
		status.Activation.Issue != "none" || status.Runtime.State != "starting" ||
		status.Runtime.Issue != "none" || status.AssetRevision != bundle.Manifest().AssetRevision {
		t.Fatalf("ReadStatus() = (%#v, %v)", status, apiErr)
	}
	authority, apiErr := client.ReadAuthority(context.Background())
	if apiErr != nil || !authority.Enabled || authority.Host != string(model.HostCodex) ||
		authority.Runtime != string(model.RuntimeCodexAppServer) ||
		authority.AssetRevision != bundle.Manifest().AssetRevision ||
		authority.ActiveAssetRevision != bundle.Manifest().AssetRevision ||
		authority.PeerID != peerID.String() ||
		authority.UpdatedAt != enabled.UpdatedAt().Format(time.RFC3339Nano) {
		t.Fatalf("ReadAuthority() = (%#v, %v)", authority, apiErr)
	}
	hook, apiErr := client.HookCheck(context.Background())
	if apiErr != nil || hook.Pending {
		t.Fatalf("HookCheck() = (%#v, %v)", hook, apiErr)
	}
	current, apiErr := client.AgentCurrent(context.Background())
	if apiErr != nil || current.Status != "none" || current.RunID != "" || current.ClaimSecret != "" {
		t.Fatalf("AgentCurrent() = (%#v, %v)", current, apiErr)
	}
	projection, err := localapi.ParseInitiationProjection(current.Projection)
	if err != nil || len(projection.InitiationContext.Channels) != 0 {
		t.Fatalf("initiation projection = %s, %v", current.Projection, err)
	}
	deactivated, err := st.DeactivateProfile(context.Background(), enabled, at.Add(2*time.Second))
	if err != nil || deactivated.Profile.Enabled() {
		t.Fatalf("deactivate controller authority = (%#v, %v)", deactivated, err)
	}
	authority, apiErr = client.ReadAuthority(context.Background())
	if apiErr != nil || authority.Enabled ||
		authority.UpdatedAt != deactivated.Profile.UpdatedAt().Format(time.RFC3339Nano) {
		t.Fatalf("disabled ReadAuthority() = (%#v, %v)", authority, apiErr)
	}
	reactivated, err := st.ActivateProfile(context.Background(), enabled,
		deactivated.Profile.UpdatedAt(), at.Add(3*time.Second))
	if err != nil || !reactivated.Profile.Enabled() {
		t.Fatalf("reactivate controller authority = (%#v, %v)", reactivated, err)
	}
	authority, apiErr = client.ReadAuthority(context.Background())
	if apiErr != nil || !authority.Enabled ||
		authority.UpdatedAt != reactivated.Profile.UpdatedAt().Format(time.RFC3339Nano) {
		t.Fatalf("reactivated ReadAuthority() = (%#v, %v)", authority, apiErr)
	}
	guidePath := filepath.Join(workspace, ".agents", "skills", "mnemon-harness", "guides", "teamwork", "GUIDE.md")
	if err := os.Remove(guidePath); err != nil {
		t.Fatal(err)
	}
	health, apiErr = client.ProbeHealth(context.Background())
	if apiErr != nil || health.Status != "not_ready" {
		t.Fatalf("drifted ProbeHealth() = (%#v, %v)", health, apiErr)
	}
	status, apiErr = client.ReadStatus(context.Background())
	if apiErr != nil || status.Status != "degraded" || status.Activation.State != "failed" ||
		status.Activation.Issue != "durable_authority_mismatch" || status.Runtime.State != "starting" {
		t.Fatalf("drifted ReadStatus() = (%#v, %v)", status, apiErr)
	}
	if _, apiErr := client.HookCheck(context.Background()); apiErr == nil ||
		apiErr.Code != localapi.CodeAssetRevisionMismatch {
		t.Fatalf("drifted HookCheck() error = %v", apiErr)
	}
	cancel()
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("Serve() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Controller did not stop after cancellation")
	}
	if _, err := os.Lstat(filepath.Join(nodeState, "control.sock")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("control socket remained after shutdown: %v", err)
	}
}

func TestControllerAuthenticatedShutdownCompletesResponseThenReturnsAndCleansSocket(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
		Clock: controllerTestClock{fixture.profile.UpdatedAt()}, Install: fixture.install})
	if err != nil {
		t.Fatal(err)
	}
	defer daemon.Close()
	serveDone := make(chan error, 1)
	go func() { serveDone <- daemon.Serve(context.Background()) }()
	socketPath := filepath.Join(fixture.nodeState, "control.sock")
	waitControllerSocket(t, socketPath, serveDone)
	client, err := localapi.NewClient(fixture.nodeState)
	if err != nil {
		t.Fatal(err)
	}
	authority, apiErr := client.ReadAuthority(context.Background())
	if apiErr != nil {
		t.Fatalf("ReadAuthority() before shutdown = %#v", apiErr)
	}
	expectedDigest, err := localapi.AuthorityDigest(authority)
	if err != nil {
		t.Fatal(err)
	}
	response, apiErr := client.Shutdown(context.Background(), authority)
	if apiErr != nil || response.SchemaVersion != localapi.SchemaVersion || response.Status != "stopping" ||
		response.AuthorityDigest != expectedDigest.String() {
		t.Fatalf("Shutdown() = (%#v, %#v)", response, apiErr)
	}
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("Serve() after authenticated shutdown = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Controller did not stop after authenticated shutdown")
	}
	if _, err := os.Lstat(socketPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("control socket remained after authenticated shutdown: %v", err)
	}
	if err := daemon.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.OpenExisting(context.Background(), filepath.Join(fixture.nodeState, "node.db"))
	if err != nil {
		t.Fatalf("daemon shutdown retained Store writer: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestControllerMutationShutdownBusyKeepsDaemonReadyAndReopensAdmission(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	insertControllerBusyRun(t, fixture)
	daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
		Clock: controllerTestClock{fixture.profile.UpdatedAt()}, Install: fixture.install})
	if err != nil {
		t.Fatal(err)
	}
	defer daemon.Close()
	serveDone := make(chan error, 1)
	go func() { serveDone <- daemon.Serve(context.Background()) }()
	waitControllerSocket(t, filepath.Join(fixture.nodeState, "control.sock"), serveDone)
	client, err := localapi.NewClient(fixture.nodeState)
	if err != nil {
		t.Fatal(err)
	}
	authority, apiErr := client.ReadAuthority(context.Background())
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	if response, apiErr := client.ShutdownForMutation(context.Background(), authority); apiErr == nil ||
		apiErr.Code != localapi.CodeOperationPending || response != (localapi.ShutdownResponse{}) {
		t.Fatalf("busy ShutdownForMutation() = (%#v, %#v)", response, apiErr)
	}
	select {
	case err := <-serveDone:
		t.Fatalf("busy mutation shutdown stopped daemon: %v", err)
	default:
	}
	if health, apiErr := client.ProbeHealth(context.Background()); apiErr != nil ||
		health.Status != "ready" {
		t.Fatalf("health after busy mutation shutdown = (%#v, %#v)", health, apiErr)
	}
	if hook, apiErr := client.HookCheck(context.Background()); apiErr != nil || hook.Pending {
		t.Fatalf("HookCheck after busy mutation shutdown = (%#v, %#v)", hook, apiErr)
	}
	if _, apiErr := client.Shutdown(context.Background(), authority); apiErr != nil {
		t.Fatalf("ordinary cleanup Shutdown() = %#v", apiErr)
	}
	if err := <-serveDone; err != nil {
		t.Fatalf("Serve() after ordinary cleanup = %v", err)
	}
}

func TestControllerMutationShutdownGenerationMismatchReopensAdmission(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
		Clock: controllerTestClock{fixture.profile.UpdatedAt()}, Install: fixture.install})
	if err != nil {
		t.Fatal(err)
	}
	defer daemon.Close()
	serveDone := make(chan error, 1)
	go func() { serveDone <- daemon.Serve(context.Background()) }()
	waitControllerSocket(t, filepath.Join(fixture.nodeState, "control.sock"), serveDone)
	client, err := localapi.NewClient(fixture.nodeState)
	if err != nil {
		t.Fatal(err)
	}
	authority, apiErr := client.ReadAuthority(context.Background())
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	drifted := authority
	generation, err := time.Parse(time.RFC3339Nano, authority.UpdatedAt)
	if err != nil {
		t.Fatal(err)
	}
	drifted.UpdatedAt = generation.Add(time.Nanosecond).Format(time.RFC3339Nano)
	if _, apiErr := client.ShutdownForMutation(context.Background(), drifted); apiErr == nil ||
		apiErr.Code != localapi.CodeOperationMismatch {
		t.Fatalf("drifted ShutdownForMutation() error = %#v", apiErr)
	}
	if hook, apiErr := client.HookCheck(context.Background()); apiErr != nil || hook.Pending {
		t.Fatalf("HookCheck after mutation mismatch = (%#v, %#v)", hook, apiErr)
	}
	if _, apiErr := client.Shutdown(context.Background(), authority); apiErr != nil {
		t.Fatal(apiErr)
	}
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
}

func insertControllerBusyRun(t *testing.T, fixture daemonFixture) {
	t.Helper()
	database, err := sql.Open("sqlite", "file:"+filepath.Join(fixture.nodeState, "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`INSERT INTO agent_runs(run_id, profile_id, cause_json, launcher,
		runtime_kind, launcher_diagnostic_json, runtime_ids_json, status, started_at)
		VALUES('run-controller-mutation-busy', ?, '{}', 'test', ?, '{}', '{}', 'starting', ?)`,
		fixture.profile.ID().String(), string(fixture.profile.Runtime()),
		fixture.profile.UpdatedAt().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
}

func TestControllerShutdownSignalIsConcurrentAndIdempotent(t *testing.T) {
	t.Parallel()
	controller := &Controller{shutdownRequested: make(chan struct{})}
	const callers = 64
	var wait sync.WaitGroup
	start := make(chan struct{})
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			controller.requestShutdown()
		}()
	}
	close(start)
	wait.Wait()
	select {
	case <-controller.shutdownRequested:
	default:
		t.Fatal("concurrent lifecycle requests did not close the shutdown signal")
	}
	controller.requestShutdown()
}

func TestControllerManagedWorkerGatesReadinessAndStopsBeforeHTTP(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	worker := newControllerTestWakeWorker()
	controller, closeStore := newControllerWithTestWakeWorker(t, fixture, worker)
	defer closeStore()

	serveDone := make(chan error, 1)
	go func() { serveDone <- controller.Serve(context.Background()) }()
	select {
	case <-worker.started:
	case err := <-serveDone:
		t.Fatalf("Controller exited before worker startup: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Controller did not start managed worker")
	}
	client, err := localapi.NewClient(fixture.nodeState)
	if err != nil {
		t.Fatal(err)
	}
	if health, apiErr := client.ProbeHealth(context.Background()); apiErr != nil ||
		health.Status != "not_ready" {
		t.Fatalf("recovering worker health = (%#v, %#v)", health, apiErr)
	}
	if status, apiErr := client.ReadStatus(context.Background()); apiErr != nil ||
		status.Status != "degraded" || status.Activation.State != "ready" ||
		status.Runtime.State != "recovering" || status.Runtime.Issue != "none" {
		t.Fatalf("recovering worker status = (%#v, %#v)", status, apiErr)
	}
	close(worker.allowReady)
	waitControllerHealth(t, client, "ready")
	if status, apiErr := client.ReadStatus(context.Background()); apiErr != nil ||
		status.Status != "ready" || status.Activation.State != "ready" ||
		status.Runtime.State != "ready" || status.Runtime.Issue != "none" {
		t.Fatalf("ready worker status = (%#v, %#v)", status, apiErr)
	}

	controller.requestShutdown()
	select {
	case <-worker.cancelling:
	case err := <-serveDone:
		t.Fatalf("Controller stopped HTTP before worker cleanup: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not observe shutdown")
	}
	if _, err := os.Lstat(filepath.Join(fixture.nodeState, "control.sock")); err != nil {
		t.Fatalf("control socket closed before worker cleanup: %v", err)
	}
	select {
	case err := <-serveDone:
		t.Fatalf("Controller returned before worker cleanup: %v", err)
	default:
	}
	close(worker.allowStop)
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("Controller shutdown = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Controller did not stop after worker cleanup")
	}
	if _, err := os.Lstat(filepath.Join(fixture.nodeState, "control.sock")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("control socket remained after worker cleanup: %v", err)
	}
}

func TestControllerStatusDistinguishesInstallationDriftFromRuntimeHealth(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	worker := newControllerTestWakeWorker()
	controller, closeStore := newControllerWithTestWakeWorker(t, fixture, worker)
	defer closeStore()

	serveDone := make(chan error, 1)
	go func() { serveDone <- controller.Serve(context.Background()) }()
	select {
	case <-worker.started:
	case err := <-serveDone:
		t.Fatalf("Controller exited before worker startup: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Controller did not start managed worker")
	}
	client, err := localapi.NewClient(fixture.nodeState)
	if err != nil {
		t.Fatal(err)
	}
	close(worker.allowReady)
	waitControllerHealth(t, client, "ready")
	guide := filepath.Join(fixture.workspace, ".agents", "skills", "mnemon-harness",
		"guides", "teamwork", "GUIDE.md")
	if err := os.Remove(guide); err != nil {
		t.Fatal(err)
	}
	if status, apiErr := client.ReadStatus(context.Background()); apiErr != nil ||
		status.Status != "degraded" || status.Activation.State != "failed" ||
		status.Activation.Issue != "asset_revision_mismatch" || status.Runtime.State != "ready" ||
		status.Runtime.Issue != "none" {
		t.Fatalf("installation-drift status = (%#v, %#v)", status, apiErr)
	}
	if health, apiErr := client.ProbeHealth(context.Background()); apiErr != nil ||
		health.Status != "not_ready" {
		t.Fatalf("installation-drift health = (%#v, %#v)", health, apiErr)
	}
	controller.requestShutdown()
	select {
	case <-worker.cancelling:
	case err := <-serveDone:
		t.Fatalf("Controller stopped before worker cancellation: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not observe shutdown")
	}
	close(worker.allowStop)
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Controller did not stop after worker cleanup")
	}
}

func TestControllerManagedWorkerFailureStaysReachableAndFailsClosed(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	worker := newControllerTestWakeWorker()
	controller, closeStore := newControllerWithTestWakeWorker(t, fixture, worker)
	defer closeStore()

	serveDone := make(chan error, 1)
	go func() { serveDone <- controller.Serve(context.Background()) }()
	select {
	case <-worker.started:
	case err := <-serveDone:
		t.Fatalf("Controller exited before worker startup: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Controller did not start managed worker")
	}
	client, err := localapi.NewClient(fixture.nodeState)
	if err != nil {
		t.Fatal(err)
	}
	close(worker.allowReady)
	waitControllerHealth(t, client, "ready")
	close(worker.fail)
	select {
	case <-worker.stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("failed worker did not return")
	}
	waitControllerHealth(t, client, "not_ready")
	if status, apiErr := client.ReadStatus(context.Background()); apiErr != nil ||
		status.Status != "degraded" || status.Activation.State != "ready" ||
		status.Runtime.State != "failed" ||
		status.Runtime.Issue != "durable_runtime_transition_failed" {
		t.Fatalf("failed worker status = (%#v, %#v)", status, apiErr)
	}
	if _, apiErr := client.HookCheck(context.Background()); apiErr == nil ||
		apiErr.Code != localapi.CodeMnemondUnavailable {
		t.Fatalf("HookCheck after worker failure = %#v", apiErr)
	}
	select {
	case err := <-serveDone:
		t.Fatalf("worker failure stopped Controller: %v", err)
	default:
	}
	controller.requestShutdown()
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Controller did not stop after explicit shutdown")
	}
}

func TestControllerRequestTrackerRejectsNewHandlersAndDrainsEnteredHandler(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	tracker := newControllerRequestTracker(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(entered)
		<-release
	}))
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		tracker.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("tracked handler did not start")
	}
	drained := tracker.seal()
	select {
	case <-drained:
		t.Fatal("tracker drained while entered handler was active")
	default:
	}
	rejected := httptest.NewRecorder()
	tracker.ServeHTTP(rejected, httptest.NewRequest(http.MethodGet, "/", nil))
	if rejected.Code != http.StatusServiceUnavailable {
		t.Fatalf("post-seal status = %d", rejected.Code)
	}
	close(release)
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("entered handler did not return")
	}
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("tracker did not publish complete drain")
	}
}

type controllerTestWakeWorker struct {
	mu         sync.Mutex
	snapshot   agent.WakeWorkerSnapshot
	started    chan struct{}
	allowReady chan struct{}
	fail       chan struct{}
	cancelling chan struct{}
	allowStop  chan struct{}
	stopped    chan struct{}
}

func newControllerTestWakeWorker() *controllerTestWakeWorker {
	return &controllerTestWakeWorker{snapshot: agent.WakeWorkerSnapshot{Healthy: true},
		started: make(chan struct{}), allowReady: make(chan struct{}), fail: make(chan struct{}),
		cancelling: make(chan struct{}), allowStop: make(chan struct{}), stopped: make(chan struct{})}
}

func (worker *controllerTestWakeWorker) Run(ctx context.Context) error {
	worker.setSnapshot(agent.WakeWorkerSnapshot{Running: true, Healthy: true, Recovering: true})
	close(worker.started)
	select {
	case <-worker.allowReady:
		worker.setSnapshot(agent.WakeWorkerSnapshot{Running: true, Ready: true, Healthy: true})
	case <-ctx.Done():
		close(worker.cancelling)
		<-worker.allowStop
		worker.setSnapshot(agent.WakeWorkerSnapshot{Healthy: true})
		close(worker.stopped)
		return nil
	}
	select {
	case <-worker.fail:
		worker.setSnapshot(agent.WakeWorkerSnapshot{Healthy: false,
			LastError: "durable_runtime_transition_failed"})
		close(worker.stopped)
		return errors.New("managed wake worker failed")
	case <-ctx.Done():
		close(worker.cancelling)
		<-worker.allowStop
		worker.setSnapshot(agent.WakeWorkerSnapshot{Healthy: true})
		close(worker.stopped)
		return nil
	}
}

func (worker *controllerTestWakeWorker) Snapshot() agent.WakeWorkerSnapshot {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	return worker.snapshot
}

func (worker *controllerTestWakeWorker) setSnapshot(snapshot agent.WakeWorkerSnapshot) {
	worker.mu.Lock()
	worker.snapshot = snapshot
	worker.mu.Unlock()
}

func newControllerWithTestWakeWorker(t *testing.T, fixture daemonFixture,
	worker managedWakeWorker,
) (*Controller, func()) {
	t.Helper()
	authority, err := openExistingDaemonAuthority(context.Background(), fixture.workspace,
		fixture.nodeState)
	if err != nil {
		t.Fatal(err)
	}
	controller, err := NewController(context.Background(), ControllerOptions{NodeState: fixture.nodeState,
		Workspace: fixture.workspace, Store: authority.store, Profile: authority.authority.Profile,
		Signer: authority.identity.PublicationSigner(), Clock: controllerTestClock{fixture.profile.UpdatedAt()},
		Install: fixture.install, wakeWorker: worker})
	if err != nil {
		_ = authority.store.Close()
		t.Fatal(err)
	}
	return controller, func() {
		if err := authority.store.Close(); err != nil {
			t.Errorf("close Controller Store: %v", err)
		}
	}
}

func waitControllerHealth(t *testing.T, client *localapi.Client, status string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		health, apiErr := client.ProbeHealth(context.Background())
		if apiErr == nil && health.Status == status {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Controller health did not become %q", status)
}

func testInstallationVerifier(workspace, nodeState string, bundle assets.Bundle) InstallationVerifier {
	verify := InstallationVerifierFunc(func(_ context.Context, profile model.Profile) error {
		host := assets.Host(profile.Host())
		if !host.Valid() || profile.ActiveAssetRevision() != bundle.Manifest().AssetRevision {
			return errors.New("Profile does not select the canonical Host assets")
		}
		if err := integration.VerifyNodeBundle(nodeState, bundle); err != nil {
			return err
		}
		return integration.VerifyHostProjection(workspace, nodeState, host, bundle)
	})
	return testInstallationWithActions(verify, bundle)
}

func writeControllerProfileToken(t *testing.T, nodeState string, credential []byte) {
	t.Helper()
	profiles := filepath.Join(nodeState, "profiles")
	if err := os.Mkdir(profiles, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(profiles, model.TeamworkProfileID().String()+".token")
	raw := append([]byte(base64.RawURLEncoding.EncodeToString(credential)), '\n')
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func waitControllerSocket(t *testing.T, path string, served <-chan error) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-served:
			t.Fatalf("Controller exited before its socket became ready: %v", err)
		default:
		}
		if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSocket != 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("control socket %s did not become ready", path)
}
