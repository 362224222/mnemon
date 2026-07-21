package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

func TestResetRequiresExactPeerThenPreservesLostKeyNodeAndAllowsFreshIdentity(t *testing.T) {
	fixture := newResetTestFixture(t)
	workspace, nodeState, peerID := fixture.workspace, fixture.nodeState, fixture.peerID
	wrong := "12D3KooWLzW3XvRNG5Jv84reMiXzrU1QpkwQCrw4EP8AVSv4GDKJ"
	deps := productionResetDependencies()
	deps.workingDirectory = func() (string, error) { return workspace, nil }
	fixed := time.Date(2026, 7, 21, 14, 15, 16, 123456789, time.UTC)
	deps.now = func() time.Time { return fixed }
	var stdout, stderr bytes.Buffer
	app := &resetApp{stdout: &stdout, stderr: &stderr, deps: deps}
	if exit := app.run(context.Background(), []string{"--force", "--confirm-peer", wrong}); exit == 0 || stdout.Len() != 0 {
		t.Fatalf("wrong-peer reset = exit %d stdout=%q stderr=%q", exit,
			stdout.String(), stderr.String())
	}
	if _, err := os.Lstat(nodeState); err != nil {
		t.Fatalf("wrong-peer reset changed Node: %v", err)
	}
	credential := filepath.Join(nodeState, "profiles", model.TeamworkProfileID().String()+".token")
	for _, path := range []string{filepath.Join(nodeState, "identity.key"), credential} {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	stdout.Reset()
	stderr.Reset()
	if exit := app.run(context.Background(), []string{"--confirm-peer", peerID, "--force"}); exit != 0 || stderr.Len() != 0 {
		t.Fatalf("confirmed reset = exit %d stdout=%q stderr=%q", exit,
			stdout.String(), stderr.String())
	}
	assertResetReceiptAndFreshIdentity(t, fixture, fixed, stdout.Bytes())
}

type resetTestFixture struct {
	workspace string
	nodeState string
	revision  string
	peerID    string
}

func newResetTestFixture(t *testing.T) resetTestFixture {
	t.Helper()
	workspace, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeState, err := node.PrepareNodeState(workspace)
	if err != nil {
		t.Fatal(err)
	}
	revision := model.Sum([]byte("reset-test-assets")).String()
	provisioned, err := node.Provision(context.Background(), node.ProvisionOptions{
		Workspace: workspace, Host: model.HostCodex, AssetRevision: revision,
		Credentials: localapi.NodeRuntime{},
	})
	if err != nil {
		t.Fatal(err)
	}
	peerID := provisioned.Node.PeerID().String()
	return resetTestFixture{workspace: workspace, nodeState: nodeState,
		revision: revision, peerID: peerID}
}

func assertResetReceiptAndFreshIdentity(t *testing.T, fixture resetTestFixture,
	fixed time.Time, raw []byte,
) {
	t.Helper()
	var receipt resetReceipt
	if err := json.Unmarshal(bytes.TrimSpace(raw), &receipt); err != nil {
		t.Fatal(err)
	}
	wantPath := filepath.Join(fixture.workspace, ".mnemon", "harness", "recovery",
		"20260721T141516.123456789Z-"+fixture.peerID, "node")
	if receipt.Status != "reset" || receipt.SchemaVersion != localapi.SchemaVersion ||
		receipt.PeerID != fixture.peerID || receipt.RecoveryPath != wantPath ||
		receipt.RenamedAt != fixed.Format(time.RFC3339Nano) {
		t.Fatalf("reset receipt = %#v", receipt)
	}
	if _, err := os.Lstat(fixture.nodeState); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old Node remains: %v", err)
	}
	if info, err := os.Lstat(filepath.Join(wantPath, "node.db")); err != nil ||
		!info.Mode().IsRegular() {
		t.Fatalf("forensic database = (%v, %v)", info, err)
	}
	if _, err := node.PrepareNodeState(fixture.workspace); err != nil {
		t.Fatal(err)
	}
	fresh, err := node.Provision(context.Background(), node.ProvisionOptions{
		Workspace: fixture.workspace, Host: model.HostCodex, AssetRevision: fixture.revision,
		Credentials: localapi.NodeRuntime{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Node.PeerID().String() == fixture.peerID {
		t.Fatal("setup after reset reused quarantined Node identity")
	}
}

func TestResetRejectsOpenOrAmbiguousGrammarWithoutDependencies(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"--force"},
		{"--confirm-peer", "peer"},
		{"--force", "--force", "--confirm-peer", "peer"},
		{"--force", "--confirm-peer", "peer", "--project-root", "."},
	} {
		if _, apiErr := parseResetRequest(args); apiErr == nil || apiErr.Code != localapi.CodeInvalidArgument {
			t.Fatalf("parseResetRequest(%v) = %#v", args, apiErr)
		}
	}
}

func TestResetProcessBoundaryRenamesNodeTree(t *testing.T) {
	workspace, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.PrepareNodeState(workspace); err != nil {
		t.Fatal(err)
	}
	revision := model.Sum([]byte("reset-process-assets")).String()
	provisioned, err := node.Provision(context.Background(), node.ProvisionOptions{
		Workspace: workspace, Host: model.HostCodex, AssetRevision: revision,
		Credentials: localapi.NodeRuntime{},
	})
	if err != nil {
		t.Fatal(err)
	}
	peerID := provisioned.Node.PeerID().String()
	command := exec.Command(os.Args[0], "-test.run=^TestResetProcessHelper$")
	command.Dir = workspace
	command.Env = append(os.Environ(), "MNEMON_RESET_PROCESS_HELPER=1",
		"MNEMON_RESET_PROCESS_PEER="+peerID)
	raw, err := command.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			t.Fatalf("reset helper = %v stderr=%q", err, string(exitErr.Stderr))
		}
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"status":"reset"`) ||
		!strings.Contains(string(raw), `"peer_id":"`+peerID+`"`) {
		t.Fatalf("reset process receipt = %q", string(raw))
	}
	if _, err := os.Lstat(filepath.Join(workspace, ".mnemon", "harness", "node")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reset process retained old Node: %v", err)
	}
}

func TestResetProcessHelper(t *testing.T) {
	if os.Getenv("MNEMON_RESET_PROCESS_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	peerID := os.Getenv("MNEMON_RESET_PROCESS_PEER")
	exit := RunReset(context.Background(), []string{"--force", "--confirm-peer", peerID},
		os.Stdout, os.Stderr, "test")
	os.Exit(exit)
}
