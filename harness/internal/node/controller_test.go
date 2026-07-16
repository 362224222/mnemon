package node

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
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
		Store: st, Profile: enabled, Signer: signer, Clock: controllerTestClock{enabled.UpdatedAt()}})
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
