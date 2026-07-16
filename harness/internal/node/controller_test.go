package node

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

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
	if _, err := st.ActivateProfile(ctx, enabled, enabled.UpdatedAt()); err != nil {
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
	controller, err := NewController(ControllerOptions{NodeState: nodeState, Workspace: workspace,
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
	reactivated, err := st.ActivateProfile(context.Background(), enabled, at.Add(3*time.Second))
	if err != nil || !reactivated.Profile.Enabled() {
		t.Fatalf("reactivate controller authority = (%#v, %v)", reactivated, err)
	}
	authority, apiErr = client.ReadAuthority(context.Background())
	if apiErr != nil || !authority.Enabled ||
		authority.UpdatedAt != reactivated.Profile.UpdatedAt().Format(time.RFC3339Nano) {
		t.Fatalf("reactivated ReadAuthority() = (%#v, %v)", authority, apiErr)
	}
	guidePath := filepath.Join(workspace, ".codex", "skills", "mnemon-harness", "guides", "teamwork", "GUIDE.md")
	if err := os.Remove(guidePath); err != nil {
		t.Fatal(err)
	}
	health, apiErr = client.ProbeHealth(context.Background())
	if apiErr != nil || health.Status != "not_ready" {
		t.Fatalf("drifted ProbeHealth() = (%#v, %v)", health, apiErr)
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
	response, apiErr := client.Shutdown(context.Background())
	if apiErr != nil || response.SchemaVersion != localapi.SchemaVersion || response.Status != "stopping" {
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

func testInstallationVerifier(workspace, nodeState string, bundle assets.Bundle) InstallationVerifier {
	return InstallationVerifierFunc(func(profile model.Profile) error {
		host := assets.Host(profile.Host())
		if !host.Valid() || profile.ActiveAssetRevision() != bundle.Manifest().AssetRevision {
			return errors.New("Profile does not select the canonical Host assets")
		}
		if err := integration.VerifyNodeBundle(nodeState, bundle); err != nil {
			return err
		}
		return integration.VerifyHostProjection(workspace, nodeState, host, bundle)
	})
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
