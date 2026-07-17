//go:build darwin || linux

package process_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/integration"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
	"golang.org/x/sys/unix"
)

const (
	concurrentSetups = 20
	commandOutputMax = 16 << 10
	buildOutputMax   = 64 << 10
	runAttachmentEnv = "MNEMON_HARNESS_RUN_ATTACHMENT"
	launchPermitEnv  = "MNEMON_HARNESS_INTERNAL_MNEMOND_ENSURE_FD"
)

type setupProcessReceipt struct {
	AssetRevision string `json:"asset_revision"`
	Host          string `json:"host"`
	PeerID        string `json:"peer_id"`
	Replayed      bool   `json:"replayed"`
	SchemaVersion int    `json:"schema_version"`
	Started       bool   `json:"started"`
	Status        string `json:"status"`
}

type ejectProcessReceipt struct {
	AssetRevision       string `json:"asset_revision"`
	Host                string `json:"host"`
	PeerID              string `json:"peer_id"`
	RegistrationRemoved bool   `json:"registration_removed"`
	RemovedFiles        int    `json:"removed_files"`
	Replayed            bool   `json:"replayed"`
	SchemaVersion       int    `json:"schema_version"`
	Status              string `json:"status"`
}

type setupProcessPID struct {
	SchemaVersion int    `json:"schema_version"`
	Instance      string `json:"instance"`
	PID           int    `json:"pid"`
	Executable    string `json:"executable"`
	Workspace     string `json:"workspace"`
	NodeState     string `json:"node_state"`
}

type setupProcessResult struct {
	stdout   []byte
	stderr   []byte
	overflow bool
	err      error
}

type setupProcessPIDSnapshot struct {
	record setupProcessPID
	raw    []byte
	info   os.FileInfo
}

type setupProcessCleanup struct {
	root         string
	nodeState    string
	client       *localapi.Client
	autoMayRun   bool
	directChild  *exec.Cmd
	directPermit *os.File
	offline      setupProcessOfflineProbe
}

type setupProcessOfflineProbe struct {
	executable  string
	workspace   string
	environment []string
}

// TestPublicSetupSerializesProcessesAndRecoversAKilledDaemon is deliberately
// black-box at the setup boundary: it builds the two public commands and
// invokes only mnemon-harness setup. The automatically managed daemon is
// stopped only through authenticated local control. The crash target is a
// separate mnemond child whose exact exec.Cmd ownership remains with the test.
func TestPublicSetupSerializesProcessesAndRecoversAKilledDaemon(t *testing.T) {
	repository := setupProcessRepositoryRoot(t)
	root := setupProcessPhysicalTempDir(t)
	bin := filepath.Join(root, "bin")
	workspace := filepath.Join(root, "work")
	harnessExecutable := filepath.Join(bin, "mnemon-harness")
	mnemondExecutable := filepath.Join(bin, "mnemond")
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	cleanup := &setupProcessCleanup{root: root, nodeState: nodeState}
	t.Cleanup(func() { cleanup.run(t) })
	for _, path := range []string{bin, workspace} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatalf("create process-test directory: %v", err)
		}
	}

	buildCtx, cancelBuild := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancelBuild()
	setupProcessBuild(t, buildCtx, repository, harnessExecutable,
		"./harness/cmd/mnemon-harness")
	setupProcessBuild(t, buildCtx, repository, mnemondExecutable,
		"./harness/cmd/mnemond")
	setupProcessFakeCodex(t, filepath.Join(bin, "codex"))
	// A hostile host attachment proves the runtime environment is constructed
	// closed and does not inherit Agent capability or unrelated host secrets.
	t.Setenv(runAttachmentEnv, filepath.Join(root, "host-attachment-secret"))
	environment := setupProcessEnvironment(bin, workspace, root)
	if setupProcessHasEnvironment(environment, runAttachmentEnv) {
		t.Fatal("closed process environment inherited the managed Run attachment")
	}
	cleanup.offline = setupProcessOfflineProbe{executable: mnemondExecutable,
		workspace: workspace, environment: append([]string(nil), environment...)}

	setupCtx, cancelSetups := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancelSetups()
	cleanup.autoMayRun = true
	start := make(chan struct{})
	results := make(chan struct {
		index  int
		result setupProcessResult
	}, concurrentSetups)
	for index := range concurrentSetups {
		go func() {
			<-start
			results <- struct {
				index  int
				result setupProcessResult
			}{index: index, result: setupProcessRunSetup(setupCtx, harnessExecutable,
				workspace, environment)}
		}()
	}
	close(start)
	concurrentResults := make([]setupProcessResult, concurrentSetups)
	for range concurrentSetups {
		result := <-results
		concurrentResults[result.index] = result.result
	}

	firstPID, err := setupProcessReadPID(workspace, mnemondExecutable)
	if err != nil {
		t.Fatalf("read daemon lifecycle after concurrent setup: %v", err)
	}
	client, err := localapi.NewClient(nodeState)
	if err != nil {
		t.Fatal("construct authenticated local control client")
	}
	cleanup.client = client

	receipts := make([]setupProcessReceipt, concurrentSetups)
	for index, result := range concurrentResults {
		receipt, err := setupProcessParseReceipt(result)
		if err != nil {
			t.Errorf("setup process %d failed: %v", index, err)
			continue
		}
		receipts[index] = receipt
	}
	if t.Failed() {
		t.FailNow()
	}
	setupProcessAssertConcurrentReceipts(t, receipts)
	baseline := receipts[0]
	healthCtx, cancelHealth := context.WithTimeout(context.Background(), 5*time.Second)
	if err := setupProcessWaitReady(healthCtx, client, baseline.AssetRevision); err != nil {
		cancelHealth()
		t.Fatalf("concurrent setup did not leave authenticated ready health: %v", err)
	}
	cancelHealth()
	setupProcessAssertCodexProjectionLayout(t, workspace, true)

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	if err := setupProcessShutdown(shutdownCtx, client, nodeState, cleanup.offline); err != nil {
		cancelShutdown()
		t.Fatalf("authenticated shutdown of setup-managed daemon: %v", err)
	}
	cancelShutdown()
	cleanup.autoMayRun = false
	afterShutdown, err := setupProcessReadPID(workspace, mnemondExecutable)
	if err != nil || afterShutdown.record.Instance != firstPID.record.Instance ||
		!bytes.Equal(afterShutdown.raw, firstPID.raw) ||
		!os.SameFile(afterShutdown.info, firstPID.info) {
		t.Fatalf("graceful shutdown changed released lifecycle diagnostics: %v", err)
	}

	directPermit, err := setupProcessAcquireLaunchPermit(nodeState)
	if err != nil {
		t.Fatalf("acquire owned direct launch permit: %v", err)
	}
	cleanup.directPermit = directPermit
	directOutput := newSetupProcessOutput(commandOutputMax)
	direct := exec.Command(mnemondExecutable, "serve", "--project-root", workspace)
	direct.Dir = workspace
	direct.Env = append(append([]string(nil), environment...), launchPermitEnv+"=3")
	direct.ExtraFiles = []*os.File{directPermit}
	direct.Stdin = nil
	direct.Stdout = directOutput
	direct.Stderr = directOutput
	direct.WaitDelay = 250 * time.Millisecond
	if err := direct.Start(); err != nil {
		t.Fatalf("start owned direct mnemond child: %v", err)
	}
	cleanup.directChild = direct
	directReadyCtx, cancelDirectReady := context.WithTimeout(context.Background(), 5*time.Second)
	if err := setupProcessWaitReady(directReadyCtx, client, baseline.AssetRevision); err != nil {
		cancelDirectReady()
		t.Fatalf("owned direct mnemond did not become ready: %v", err)
	}
	cancelDirectReady()
	if err := directPermit.Close(); err != nil {
		t.Fatalf("release owned direct launch permit after ready: %v", err)
	}
	cleanup.directPermit = nil
	crashErr := setupProcessCrashOwnedChild(direct)
	if direct.ProcessState != nil {
		cleanup.directChild = nil
	}
	if crashErr != nil || directOutput.overflowed() {
		t.Fatalf("crash owned direct mnemond: %v output=%s", crashErr,
			setupProcessFingerprint(directOutput.bytes()))
	}
	crashedCtx, cancelCrashed := context.WithTimeout(context.Background(), 5*time.Second)
	if err := setupProcessWaitCrashed(crashedCtx, client, nodeState); err != nil {
		cancelCrashed()
		t.Fatalf("owned child crash was not externally observable: %v", err)
	}
	cancelCrashed()
	stalePID, err := setupProcessReadPID(workspace, mnemondExecutable)
	if err != nil || stalePID.record.Instance != firstPID.record.Instance ||
		!bytes.Equal(stalePID.raw, firstPID.raw) || !os.SameFile(stalePID.info, firstPID.info) {
		t.Fatalf("owned child crash changed the prior stale lifecycle generation: %v", err)
	}

	recoveryCtx, cancelRecovery := context.WithTimeout(context.Background(), 20*time.Second)
	cleanup.autoMayRun = true
	recovery := setupProcessRunSetup(recoveryCtx, harnessExecutable, workspace, environment)
	cancelRecovery()
	secondPID, pidErr := setupProcessReadPID(workspace, mnemondExecutable)
	recoveryReceipt, receiptErr := setupProcessParseReceipt(recovery)
	if receiptErr != nil {
		t.Fatalf("ordinary setup did not recover the killed daemon: %v", receiptErr)
	}
	if pidErr != nil {
		t.Fatalf("read recovered daemon lifecycle: %v", pidErr)
	}
	if !recoveryReceipt.Started || !recoveryReceipt.Replayed ||
		recoveryReceipt.PeerID != baseline.PeerID ||
		recoveryReceipt.AssetRevision != baseline.AssetRevision {
		t.Fatalf("recovery receipt did not preserve replay authority: %#v", recoveryReceipt)
	}
	if secondPID.record.Instance == firstPID.record.Instance ||
		bytes.Equal(secondPID.raw, firstPID.raw) || os.SameFile(secondPID.info, firstPID.info) {
		t.Fatal("stale recovery did not publish a new lifecycle inode generation")
	}
	recoveredHealthCtx, cancelRecoveredHealth := context.WithTimeout(context.Background(), 5*time.Second)
	if err := setupProcessWaitReady(recoveredHealthCtx, client, baseline.AssetRevision); err != nil {
		cancelRecoveredHealth()
		t.Fatalf("recovered daemon lacks authenticated ready health: %v", err)
	}
	cancelRecoveredHealth()
}

// TestPublicSetupUpgradesAnActiveRevisionUnderLifecycleLease composes a real
// old-authority Store, controller, Unix socket and applied Host projection,
// then invokes only the public setup command for the desired embedded bundle.
// The old daemon uses the current controller protocol with an injected old
// installation verifier; setup must still perform the real companion-owned
// offline mutations and launch a new managed daemon before returning.
func TestPublicSetupUpgradesAnActiveRevisionUnderLifecycleLease(t *testing.T) {
	repository := setupProcessRepositoryRoot(t)
	root := setupProcessPhysicalTempDir(t)
	bin := filepath.Join(root, "bin")
	workspace := filepath.Join(root, "work")
	harnessExecutable := filepath.Join(bin, "mnemon-harness")
	mnemondExecutable := filepath.Join(bin, "mnemond")
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	cleanup := &setupProcessCleanup{root: root, nodeState: nodeState}
	t.Cleanup(func() { cleanup.run(t) })
	for _, path := range []string{bin, workspace} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatalf("create revision-upgrade directory: %v", err)
		}
	}

	buildCtx, cancelBuild := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancelBuild()
	setupProcessBuild(t, buildCtx, repository, harnessExecutable,
		"./harness/cmd/mnemon-harness")
	setupProcessBuild(t, buildCtx, repository, mnemondExecutable,
		"./harness/cmd/mnemond")
	setupProcessFakeCodex(t, filepath.Join(bin, "codex"))
	environment := setupProcessEnvironment(bin, workspace, root)
	cleanup.offline = setupProcessOfflineProbe{executable: mnemondExecutable,
		workspace: workspace, environment: append([]string(nil), environment...)}

	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	newRevision := bundle.Manifest().AssetRevision
	oldRevision := model.Sum([]byte("process active revision before upgrade")).String()
	createdAt := time.Date(2020, time.January, 2, 8, 0, 0, 0, time.UTC)
	provisioned, err := node.Provision(context.Background(), node.ProvisionOptions{
		Workspace: workspace, Host: model.HostCodex, AssetRevision: oldRevision,
		Clock: setupProcessClock{at: createdAt},
	})
	if err != nil || provisioned.NodeState != nodeState || provisioned.Profile.Enabled() {
		t.Fatalf("provision old revision = (%#v, %v)", provisioned, err)
	}
	if _, err := integration.InstallNodeBundle(nodeState, bundle); err != nil {
		t.Fatal(err)
	}
	projection, err := integration.InstallHostProjection(workspace, nodeState,
		assets.HostCodex, bundle)
	if err != nil {
		t.Fatal(err)
	}
	setupProcessRewriteAppliedProjectionRevision(t, projection.OwnershipPath,
		newRevision, oldRevision)
	oldInstall := node.InstallationVerifierFunc(func(profile model.Profile) error {
		if profile.Host() != model.HostCodex || profile.ActiveAssetRevision() != oldRevision {
			return errors.New("old process fixture installation authority differs")
		}
		return nil
	})
	activated, err := node.Activate(context.Background(), node.ActivateOptions{
		Workspace: workspace, Host: model.HostCodex, AssetRevision: oldRevision,
		ExpectedUpdatedAt: provisioned.Profile.UpdatedAt(),
		Clock:             setupProcessClock{at: createdAt.Add(time.Second)},
		Install:           oldInstall,
	})
	if err != nil || !activated.Changed || !activated.Profile.Enabled() {
		t.Fatalf("activate old revision = (%#v, %v)", activated, err)
	}

	oldDaemon, err := node.OpenDaemon(context.Background(), node.DaemonOptions{
		Workspace: workspace, Install: oldInstall,
	})
	if err != nil {
		t.Fatal(err)
	}
	oldServeCtx, cancelOldServe := context.WithCancel(context.Background())
	defer cancelOldServe()
	defer oldDaemon.Close()
	oldDone := make(chan error, 1)
	go func() {
		oldDone <- errors.Join(oldDaemon.Serve(oldServeCtx), oldDaemon.Close())
	}()
	client, err := localapi.NewClient(nodeState)
	if err != nil {
		t.Fatal(err)
	}
	cleanup.client = client
	cleanup.autoMayRun = true
	oldReadyCtx, cancelOldReady := context.WithTimeout(context.Background(), 5*time.Second)
	if err := setupProcessWaitReady(oldReadyCtx, client, oldRevision); err != nil {
		cancelOldReady()
		t.Fatalf("old revision daemon did not become ready: %v", err)
	}
	cancelOldReady()

	upgradeCtx, cancelUpgrade := context.WithTimeout(context.Background(), 30*time.Second)
	result := setupProcessRunSetup(upgradeCtx, harnessExecutable, workspace, environment)
	cancelUpgrade()
	receipt, err := setupProcessParseReceipt(result)
	if err != nil {
		t.Fatalf("public active revision upgrade failed: %v", err)
	}
	if receipt.AssetRevision != newRevision || receipt.Host != "codex" ||
		receipt.PeerID != provisioned.Node.PeerID().String() || receipt.Replayed ||
		!receipt.Started || receipt.Status != "ready" {
		t.Fatalf("active revision upgrade receipt = %#v", receipt)
	}
	select {
	case err := <-oldDone:
		if err != nil {
			t.Fatalf("old daemon lifecycle completion = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("old daemon did not release its Store writer")
	}
	if err := integration.VerifyHostProjection(workspace, nodeState,
		assets.HostCodex, bundle); err != nil {
		t.Fatalf("new projection did not converge: %v", err)
	}
	setupProcessAssertCodexProjectionLayout(t, workspace, true)
	newReadyCtx, cancelNewReady := context.WithTimeout(context.Background(), 5*time.Second)
	if err := setupProcessWaitReady(newReadyCtx, client, newRevision); err != nil {
		cancelNewReady()
		t.Fatalf("new revision daemon did not become ready: %v", err)
	}
	cancelNewReady()
	authority, apiErr := client.ReadAuthority(context.Background())
	if apiErr != nil || authority.AssetRevision != newRevision ||
		authority.ActiveAssetRevision != newRevision || !authority.Enabled ||
		authority.PeerID != receipt.PeerID {
		t.Fatalf("new durable authority = (%#v, %#v)", authority, apiErr)
	}
}

func TestPublicEjectPreservesNodeAndAuthorizesOneExplicitHostSwitch(t *testing.T) {
	repository := setupProcessRepositoryRoot(t)
	root := setupProcessPhysicalTempDir(t)
	bin := filepath.Join(root, "bin")
	workspace := filepath.Join(root, "work")
	harnessExecutable := filepath.Join(bin, "mnemon-harness")
	mnemondExecutable := filepath.Join(bin, "mnemond")
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	cleanup := &setupProcessCleanup{root: root, nodeState: nodeState}
	t.Cleanup(func() { cleanup.run(t) })
	for _, path := range []string{bin, workspace} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatalf("create eject process-test directory: %v", err)
		}
	}

	buildCtx, cancelBuild := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancelBuild()
	setupProcessBuild(t, buildCtx, repository, harnessExecutable,
		"./harness/cmd/mnemon-harness")
	setupProcessBuild(t, buildCtx, repository, mnemondExecutable,
		"./harness/cmd/mnemond")
	setupProcessFakeCodex(t, filepath.Join(bin, "codex"))
	setupProcessFakeClaude(t, filepath.Join(bin, "claude"))
	environment := setupProcessEnvironment(bin, workspace, root)
	cleanup.offline = setupProcessOfflineProbe{executable: mnemondExecutable,
		workspace: workspace, environment: append([]string(nil), environment...)}

	setupCtx, cancelSetup := context.WithTimeout(context.Background(), 20*time.Second)
	cleanup.autoMayRun = true
	installed := setupProcessRunSetup(setupCtx, harnessExecutable, workspace, environment)
	cancelSetup()
	setupReceipt, err := setupProcessParseReceipt(installed)
	if err != nil || setupReceipt.Host != "codex" || setupReceipt.Replayed {
		t.Fatalf("initial public setup = (%#v, %v)", setupReceipt, err)
	}
	client, err := localapi.NewClient(nodeState)
	if err != nil {
		t.Fatal(err)
	}
	cleanup.client = client
	readyCtx, cancelReady := context.WithTimeout(context.Background(), 5*time.Second)
	if err := setupProcessWaitReady(readyCtx, client, setupReceipt.AssetRevision); err != nil {
		cancelReady()
		t.Fatalf("initial setup readiness: %v", err)
	}
	cancelReady()
	databaseInfo, _ := setupProcessSnapshotFile(t, filepath.Join(nodeState, "node.db"), 0)
	identityInfo, identityRaw := setupProcessSnapshotFile(t,
		filepath.Join(nodeState, "identity.key"), 4096)
	credentialInfo, credentialRaw := setupProcessSnapshotFile(t,
		filepath.Join(nodeState, "profiles", model.TeamworkProfileID().String()+".token"), 4096)

	ejectCtx, cancelEject := context.WithTimeout(context.Background(), 20*time.Second)
	ejected := setupProcessRunEject(ejectCtx, harnessExecutable, workspace, environment)
	cancelEject()
	ejectReceipt, err := setupProcessParseEjectReceipt(ejected)
	if err != nil || ejectReceipt.AssetRevision != setupReceipt.AssetRevision ||
		ejectReceipt.Host != "codex" || ejectReceipt.PeerID != setupReceipt.PeerID ||
		ejectReceipt.RemovedFiles != 3 || !ejectReceipt.RegistrationRemoved ||
		ejectReceipt.Replayed || ejectReceipt.Status != "ejected" {
		t.Fatalf("public eject receipt = (%#v, %v)", ejectReceipt, err)
	}
	offlineCtx, cancelOffline := context.WithTimeout(context.Background(), 5*time.Second)
	if err := setupProcessWaitOffline(offlineCtx, client, nodeState, cleanup.offline); err != nil {
		cancelOffline()
		t.Fatalf("eject did not leave the Node offline: %v", err)
	}
	cancelOffline()
	cleanup.autoMayRun = false
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	if err := integration.VerifyHostProjectionAbsent(workspace, nodeState,
		assets.HostCodex, bundle); err != nil {
		t.Fatalf("Codex projection remains after eject: %v", err)
	}
	setupProcessAssertCodexProjectionLayout(t, workspace, false)
	if err := integration.VerifyNodeBundle(nodeState, bundle); err != nil {
		t.Fatalf("eject removed the immutable Node bundle: %v", err)
	}
	setupProcessAssertPreservedFile(t, filepath.Join(nodeState, "node.db"), databaseInfo, nil)
	setupProcessAssertPreservedFile(t, filepath.Join(nodeState, "identity.key"),
		identityInfo, identityRaw)
	setupProcessAssertPreservedFile(t, filepath.Join(nodeState, "profiles",
		model.TeamworkProfileID().String()+".token"), credentialInfo, credentialRaw)

	replayCtx, cancelReplay := context.WithTimeout(context.Background(), 15*time.Second)
	replayed := setupProcessRunEject(replayCtx, harnessExecutable, workspace, environment)
	cancelReplay()
	replayReceipt, err := setupProcessParseEjectReceipt(replayed)
	if err != nil || !replayReceipt.Replayed || replayReceipt.RemovedFiles != 0 ||
		replayReceipt.RegistrationRemoved || replayReceipt.PeerID != setupReceipt.PeerID {
		t.Fatalf("replayed public eject = (%#v, %v)", replayReceipt, err)
	}

	switchCtx, cancelSwitch := context.WithTimeout(context.Background(), 20*time.Second)
	cleanup.autoMayRun = true
	switched := setupProcessRunHarness(switchCtx, harnessExecutable, workspace, environment,
		"setup", "--host", "claude-code", "--project-root", workspace)
	cancelSwitch()
	switchReceipt, err := setupProcessParseReceipt(switched)
	if err != nil || switchReceipt.Host != "claude-code" || switchReceipt.Replayed ||
		!switchReceipt.Started || switchReceipt.PeerID != setupReceipt.PeerID ||
		switchReceipt.AssetRevision != setupReceipt.AssetRevision {
		t.Fatalf("explicit Host switch setup = (%#v, %v)", switchReceipt, err)
	}
	switchedReadyCtx, cancelSwitchedReady := context.WithTimeout(context.Background(), 5*time.Second)
	if err := setupProcessWaitReady(switchedReadyCtx, client,
		switchReceipt.AssetRevision); err != nil {
		cancelSwitchedReady()
		t.Fatalf("switched Host daemon did not become ready: %v", err)
	}
	cancelSwitchedReady()
	if err := integration.VerifyHostProjectionAbsent(workspace, nodeState,
		assets.HostCodex, bundle); err != nil {
		t.Fatalf("Host switch recreated the old Codex projection: %v", err)
	}
	setupProcessAssertCodexProjectionLayout(t, workspace, false)
	if err := integration.VerifyHostProjection(workspace, nodeState,
		assets.HostClaudeCode, bundle); err != nil {
		t.Fatalf("Host switch did not install the Claude projection: %v", err)
	}
	setupProcessAssertPreservedFile(t, filepath.Join(nodeState, "node.db"), databaseInfo, nil)
	setupProcessAssertPreservedFile(t, filepath.Join(nodeState, "identity.key"),
		identityInfo, identityRaw)
	setupProcessAssertPreservedFile(t, filepath.Join(nodeState, "profiles",
		model.TeamworkProfileID().String()+".token"), credentialInfo, credentialRaw)
}

func setupProcessAssertConcurrentReceipts(t *testing.T, receipts []setupProcessReceipt) {
	t.Helper()
	if len(receipts) != concurrentSetups {
		t.Fatalf("setup receipt count = %d, want %d", len(receipts), concurrentSetups)
	}
	baseline := receipts[0]
	started := 0
	fresh := 0
	for index, receipt := range receipts {
		if receipt.SchemaVersion != 1 || receipt.Status != "ready" || receipt.Host != "codex" ||
			receipt.PeerID == "" || !setupProcessDigest(receipt.AssetRevision) {
			t.Errorf("setup receipt %d has invalid public authority: %#v", index, receipt)
		}
		if receipt.PeerID != baseline.PeerID || receipt.AssetRevision != baseline.AssetRevision {
			t.Errorf("setup receipt %d diverged from the shared Node authority", index)
		}
		if receipt.Started {
			started++
		}
		if !receipt.Replayed {
			fresh++
		}
		if receipt.Started != !receipt.Replayed {
			t.Errorf("setup receipt %d has non-serialized started/replayed state: %#v",
				index, receipt)
		}
	}
	if started != 1 || fresh != 1 {
		t.Errorf("concurrent setup published started=%d fresh=%d, want one of each", started, fresh)
	}
}

func setupProcessAssertCodexProjectionLayout(t *testing.T, workspace string, present bool) {
	t.Helper()
	paths := []string{
		filepath.Join(workspace, ".agents", "skills", "mnemon-harness", "SKILL.md"),
		filepath.Join(workspace, ".agents", "skills", "mnemon-harness", "guides", "teamwork", "GUIDE.md"),
		filepath.Join(workspace, ".codex", "hooks", "mnemon-harness", "hook.sh"),
	}
	for _, path := range paths {
		info, err := os.Lstat(path)
		if present {
			if err != nil || !info.Mode().IsRegular() {
				t.Fatalf("required Codex projection %s = (%#v, %v)", path, info, err)
			}
			continue
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("ejected Codex projection %s remains: %v", path, err)
		}
	}
	if _, err := os.Lstat(filepath.Join(workspace, ".codex", "skills")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("legacy Codex Skill surface exists: %v", err)
	}
}

func setupProcessRunSetup(ctx context.Context, executable, workspace string,
	environment []string,
) setupProcessResult {
	return setupProcessRunHarness(ctx, executable, workspace, environment,
		"setup", "--project-root", workspace)
}

func setupProcessRunEject(ctx context.Context, executable, workspace string,
	environment []string,
) setupProcessResult {
	return setupProcessRunHarness(ctx, executable, workspace, environment,
		"eject", "--project-root", workspace)
}

func setupProcessRunHarness(ctx context.Context, executable, workspace string,
	environment []string, args ...string,
) setupProcessResult {
	stdout := newSetupProcessOutput(commandOutputMax)
	stderr := newSetupProcessOutput(commandOutputMax)
	command := exec.CommandContext(ctx, executable, args...)
	command.Dir = workspace
	command.Env = environment
	command.Stdin = nil
	command.Stdout = stdout
	command.Stderr = stderr
	command.WaitDelay = 250 * time.Millisecond
	err := command.Run()
	return setupProcessResult{stdout: stdout.bytes(), stderr: stderr.bytes(),
		overflow: stdout.overflowed() || stderr.overflowed(), err: err}
}

func setupProcessParseReceipt(result setupProcessResult) (setupProcessReceipt, error) {
	if result.err != nil {
		return setupProcessReceipt{}, fmt.Errorf("exit=%v stderr=%s stdout=%s", result.err,
			setupProcessFingerprint(result.stderr), setupProcessFingerprint(result.stdout))
	}
	if result.overflow {
		return setupProcessReceipt{}, errors.New("command output exceeded its test bound")
	}
	if len(result.stderr) != 0 {
		return setupProcessReceipt{}, fmt.Errorf("unexpected stderr %s",
			setupProcessFingerprint(result.stderr))
	}
	if bytes.Contains(bytes.ToLower(result.stdout), []byte("token")) ||
		bytes.Contains(bytes.ToLower(result.stdout), []byte("credential")) ||
		bytes.Contains(bytes.ToLower(result.stdout), []byte("secret")) {
		return setupProcessReceipt{}, errors.New("public setup output contains a secret-bearing field")
	}
	decoder := json.NewDecoder(bytes.NewReader(result.stdout))
	decoder.DisallowUnknownFields()
	var receipt setupProcessReceipt
	if err := decoder.Decode(&receipt); err != nil {
		return setupProcessReceipt{}, fmt.Errorf("decode receipt %s: %v",
			setupProcessFingerprint(result.stdout), err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return setupProcessReceipt{}, errors.New("setup receipt has trailing content")
	}
	canonical, err := json.Marshal(receipt)
	if err != nil || !bytes.Equal(result.stdout, append(canonical, '\n')) {
		return setupProcessReceipt{}, errors.New("setup receipt is not one canonical JSON line")
	}
	return receipt, nil
}

func setupProcessParseEjectReceipt(result setupProcessResult) (ejectProcessReceipt, error) {
	if result.err != nil {
		return ejectProcessReceipt{}, fmt.Errorf("exit=%v stderr=%s stdout=%s", result.err,
			setupProcessFingerprint(result.stderr), setupProcessFingerprint(result.stdout))
	}
	if result.overflow || len(result.stderr) != 0 ||
		bytes.Contains(bytes.ToLower(result.stdout), []byte("token")) ||
		bytes.Contains(bytes.ToLower(result.stdout), []byte("credential")) ||
		bytes.Contains(bytes.ToLower(result.stdout), []byte("secret")) {
		return ejectProcessReceipt{}, errors.New("eject returned an invalid or secret-bearing envelope")
	}
	decoder := json.NewDecoder(bytes.NewReader(result.stdout))
	decoder.DisallowUnknownFields()
	var receipt ejectProcessReceipt
	if err := decoder.Decode(&receipt); err != nil {
		return ejectProcessReceipt{}, fmt.Errorf("decode eject receipt %s: %v",
			setupProcessFingerprint(result.stdout), err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ejectProcessReceipt{}, errors.New("eject receipt has trailing content")
	}
	canonical, err := json.Marshal(receipt)
	if err != nil || !bytes.Equal(result.stdout, append(canonical, '\n')) {
		return ejectProcessReceipt{}, errors.New("eject receipt is not one canonical JSON line")
	}
	if receipt.SchemaVersion != 1 || receipt.Status != "ejected" ||
		!setupProcessDigest(receipt.AssetRevision) || receipt.Host == "" || receipt.PeerID == "" ||
		receipt.RemovedFiles < 0 || receipt.RemovedFiles > 3 {
		return ejectProcessReceipt{}, errors.New("eject receipt has invalid public authority")
	}
	return receipt, nil
}

func setupProcessReadPID(workspace, executable string) (setupProcessPIDSnapshot, error) {
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	path := filepath.Join(nodeState, "mnemond.pid")
	info, err := os.Lstat(path)
	if err != nil {
		return setupProcessPIDSnapshot{}, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 ||
		info.Size() <= 0 || info.Size() > 4096 {
		return setupProcessPIDSnapshot{}, errors.New("unsafe PID lifecycle file")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return setupProcessPIDSnapshot{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var record setupProcessPID
	if err := decoder.Decode(&record); err != nil {
		return setupProcessPIDSnapshot{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return setupProcessPIDSnapshot{}, errors.New("PID lifecycle has trailing content")
	}
	canonical, err := json.Marshal(record)
	instance, instanceErr := hex.DecodeString(record.Instance)
	if err != nil || !bytes.Equal(raw, append(canonical, '\n')) || record.SchemaVersion != 1 ||
		record.PID <= 0 || instanceErr != nil || len(instance) != 16 ||
		hex.EncodeToString(instance) != record.Instance || record.Executable != executable ||
		record.Workspace != workspace || record.NodeState != nodeState {
		return setupProcessPIDSnapshot{}, errors.New("PID lifecycle authority is invalid")
	}
	confirmed, err := os.Lstat(path)
	if err != nil || !os.SameFile(info, confirmed) {
		return setupProcessPIDSnapshot{}, errors.New("PID lifecycle identity changed while reading")
	}
	return setupProcessPIDSnapshot{record: record, raw: raw, info: confirmed}, nil
}

func setupProcessBuild(t *testing.T, ctx context.Context, repository, output,
	packagePath string,
) {
	t.Helper()
	combined := newSetupProcessOutput(buildOutputMax)
	command := exec.CommandContext(ctx, "go", "build", "-trimpath", "-o", output, packagePath)
	command.Dir = repository
	command.Stdout = combined
	command.Stderr = combined
	command.WaitDelay = 250 * time.Millisecond
	if err := command.Run(); err != nil {
		t.Fatalf("build %s: %v, output %s", packagePath, err,
			setupProcessFingerprint(combined.bytes()))
	}
	if combined.overflowed() {
		t.Fatalf("build %s exceeded its output bound", packagePath)
	}
	physical, err := filepath.EvalSymlinks(output)
	info, statErr := os.Lstat(output)
	if err != nil || physical != output || statErr != nil || !info.Mode().IsRegular() ||
		info.Mode().Perm()&0o111 == 0 || info.Mode().Perm()&0o022 != 0 {
		t.Fatalf("build %s did not produce a safe physical executable", packagePath)
	}
}

func setupProcessFakeCodex(t *testing.T, path string) {
	t.Helper()
	contents := []byte("#!/bin/sh\nset -eu\n" +
		"case \"$*\" in\n" +
		"  --version) printf '%s\\n' 'codex process-test' ;;\n" +
		"  'app-server --help') printf '%s\\n' 'Usage: codex app-server' ;;\n" +
		"  *) exit 64 ;;\n" +
		"esac\n")
	if err := os.WriteFile(path, contents, 0o700); err != nil {
		t.Fatalf("write fake Codex executable: %v", err)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatalf("protect fake Codex executable: %v", err)
	}
}

func setupProcessFakeClaude(t *testing.T, path string) {
	t.Helper()
	contents := []byte("#!/bin/sh\nset -eu\n" +
		"case \"$*\" in\n" +
		"  --version) printf '%s\\n' 'claude process-test' ;;\n" +
		"  --help) printf '%s\\n' 'Usage: claude' ;;\n" +
		"  *) exit 64 ;;\n" +
		"esac\n")
	if err := os.WriteFile(path, contents, 0o700); err != nil {
		t.Fatalf("write fake Claude executable: %v", err)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatalf("protect fake Claude executable: %v", err)
	}
}

func setupProcessRewriteAppliedProjectionRevision(t *testing.T, ownershipPath,
	currentRevision, previousRevision string,
) {
	t.Helper()
	raw, err := os.ReadFile(ownershipPath)
	if err != nil {
		t.Fatal(err)
	}
	current := []byte(`"asset_revision":"` + currentRevision + `"`)
	previous := []byte(`"asset_revision":"` + previousRevision + `"`)
	if len(current) != len(previous) || bytes.Count(raw, current) != 1 {
		t.Fatal("applied projection has no unique replaceable revision authority")
	}
	rewritten := bytes.Replace(raw, current, previous, 1)
	file, err := os.OpenFile(ownershipPath, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(rewritten); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	directory, err := os.Open(filepath.Dir(ownershipPath))
	if err != nil {
		t.Fatal(err)
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		t.Fatal(err)
	}
	if err := directory.Close(); err != nil {
		t.Fatal(err)
	}
}

type setupProcessClock struct{ at time.Time }

func (clock setupProcessClock) Now() time.Time { return clock.at }

func setupProcessSnapshotFile(t *testing.T, path string,
	maxBytes int64,
) (os.FileInfo, []byte) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("snapshot managed file %s: %v", filepath.Base(path), err)
	}
	var raw []byte
	if maxBytes > 0 {
		if info.Size() <= 0 || info.Size() > maxBytes {
			t.Fatalf("managed file %s size is outside its test bound", filepath.Base(path))
		}
		raw, err = os.ReadFile(path)
		if err != nil {
			t.Fatalf("read managed file %s: %v", filepath.Base(path), err)
		}
	}
	return info, raw
}

func setupProcessAssertPreservedFile(t *testing.T, path string, before os.FileInfo,
	wantRaw []byte,
) {
	t.Helper()
	after, err := os.Lstat(path)
	if err != nil || before == nil || !os.SameFile(before, after) ||
		!after.Mode().IsRegular() || after.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("managed file %s was replaced or removed: %v", filepath.Base(path), err)
	}
	if wantRaw != nil {
		raw, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(raw, wantRaw) {
			t.Fatalf("managed file %s content changed: %v", filepath.Base(path), err)
		}
	}
}

func setupProcessEnvironment(bin, workspace, temporaryRoot string) []string {
	// Construct from nothing: in particular, never copy
	// MNEMON_HARNESS_RUN_ATTACHMENT or any host credential into mnemond.
	return []string{
		"HOME=" + workspace,
		"LANG=C",
		"LC_ALL=C",
		"PATH=" + bin + string(os.PathListSeparator) + "/usr/bin:/bin",
		"TMPDIR=" + temporaryRoot,
	}
}

func setupProcessHasEnvironment(environment []string, name string) bool {
	prefix := name + "="
	for _, entry := range environment {
		if strings.HasPrefix(entry, prefix) {
			return true
		}
	}
	return false
}

func setupProcessDigest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil && len(decoded) == sha256.Size
}

func setupProcessWaitReady(ctx context.Context, client *localapi.Client,
	assetRevision string,
) error {
	for {
		health, apiErr := client.ProbeHealth(ctx)
		if apiErr == nil {
			if health.SchemaVersion != 1 || health.Status != "ready" ||
				health.AssetRevision != assetRevision {
				return errors.New("authenticated health authority differs from setup")
			}
			return nil
		}
		if apiErr.Code != localapi.CodeMnemondUnavailable {
			return fmt.Errorf("authenticated health failed with code %s", apiErr.Code)
		}
		if err := setupProcessPoll(ctx); err != nil {
			return err
		}
	}
}

func setupProcessShutdown(ctx context.Context, client *localapi.Client, nodeState string,
	offline setupProcessOfflineProbe,
) error {
	if client == nil {
		return errors.New("authenticated local control is unavailable")
	}
	_, apiErr := client.ProbeHealth(ctx)
	if apiErr == nil {
		authority, authorityErr := client.ReadAuthority(ctx)
		if authorityErr != nil {
			return fmt.Errorf("read shutdown authority failed with code %s", authorityErr.Code)
		}
		response, shutdownErr := client.Shutdown(ctx, authority)
		if shutdownErr != nil {
			return fmt.Errorf("shutdown failed with code %s", shutdownErr.Code)
		}
		digest, digestErr := localapi.AuthorityDigest(authority)
		if digestErr != nil || response.SchemaVersion != 1 || response.Status != "stopping" ||
			response.AuthorityDigest != digest.String() {
			return errors.New("shutdown returned an invalid lifecycle response")
		}
	} else if apiErr.Code != localapi.CodeMnemondUnavailable {
		return fmt.Errorf("pre-shutdown health failed with code %s", apiErr.Code)
	}
	return setupProcessWaitOffline(ctx, client, nodeState, offline)
}

func setupProcessWaitOffline(ctx context.Context, client *localapi.Client, nodeState string,
	offline setupProcessOfflineProbe,
) error {
	socket := filepath.Join(nodeState, "control.sock")
	for {
		_, statErr := os.Lstat(socket)
		socketAbsent := errors.Is(statErr, os.ErrNotExist)
		_, apiErr := client.ProbeHealth(ctx)
		unavailable := apiErr != nil && apiErr.Code == localapi.CodeMnemondUnavailable
		if socketAbsent && unavailable {
			writerOffline, err := setupProcessProbeOfflineWriter(ctx, offline)
			if err != nil {
				return err
			}
			if writerOffline {
				return nil
			}
		}
		if statErr != nil && !socketAbsent {
			return errors.New("local control socket cannot be inspected")
		}
		if apiErr != nil && !unavailable {
			return fmt.Errorf("offline health failed with code %s", apiErr.Code)
		}
		if err := setupProcessPoll(ctx); err != nil {
			return err
		}
	}
}

func setupProcessProbeOfflineWriter(ctx context.Context,
	offline setupProcessOfflineProbe,
) (bool, error) {
	if offline.executable == "" || offline.workspace == "" || len(offline.environment) == 0 {
		return false, errors.New("offline writer probe is unavailable")
	}
	stdout := newSetupProcessOutput(commandOutputMax)
	stderr := newSetupProcessOutput(commandOutputMax)
	command := exec.CommandContext(ctx, offline.executable, "inspect", "--project-root",
		offline.workspace)
	command.Dir = offline.workspace
	command.Env = append([]string(nil), offline.environment...)
	command.Stdin = nil
	command.Stdout = stdout
	command.Stderr = stderr
	command.WaitDelay = 250 * time.Millisecond
	err := command.Run()
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if stdout.overflowed() || stderr.overflowed() {
		return false, errors.New("offline writer probe exceeded its output bound")
	}
	if err != nil {
		return false, nil
	}
	if len(stderr.bytes()) != 0 || len(stdout.bytes()) == 0 {
		return false, errors.New("offline writer probe returned an invalid process envelope")
	}
	return true, nil
}

func setupProcessWaitCrashed(ctx context.Context, client *localapi.Client, nodeState string) error {
	socket := filepath.Join(nodeState, "control.sock")
	for {
		info, statErr := os.Lstat(socket)
		_, apiErr := client.ProbeHealth(ctx)
		if statErr == nil && info.Mode()&os.ModeSocket != 0 && apiErr != nil &&
			apiErr.Code == localapi.CodeMnemondUnavailable {
			return nil
		}
		if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
			return errors.New("crashed local control socket cannot be inspected")
		}
		if apiErr != nil && apiErr.Code != localapi.CodeMnemondUnavailable {
			return fmt.Errorf("crashed health failed with code %s", apiErr.Code)
		}
		if err := setupProcessPoll(ctx); err != nil {
			return err
		}
	}
}

func setupProcessPoll(ctx context.Context) error {
	timer := time.NewTimer(20 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func setupProcessCrashOwnedChild(command *exec.Cmd) error {
	if command == nil || command.Process == nil {
		return errors.New("owned direct child is unavailable")
	}
	killErr := command.Process.Kill()
	waitErr := command.Wait()
	if command.ProcessState == nil {
		return errors.Join(killErr, waitErr, errors.New("owned direct child exit is unproven"))
	}
	if killErr != nil {
		return fmt.Errorf("kill owned direct child: %w", killErr)
	}
	if waitErr == nil || command.ProcessState.Success() {
		return errors.New("owned direct child did not exit from the requested crash")
	}
	return nil
}

func setupProcessAcquireLaunchPermit(nodeState string) (*os.File, error) {
	path := filepath.Join(nodeState, "ensure.lock")
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode().Perm() != 0o600 {
		return nil, errors.New("managed ensure lock is unavailable")
	}
	stat, ok := before.Sys().(*syscall.Stat_t)
	if !ok || uint32(stat.Uid) != uint32(os.Geteuid()) || stat.Nlink != 1 {
		return nil, errors.New("managed ensure lock authority is unsafe")
	}
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("managed ensure lock descriptor is unavailable")
	}
	fail := func(cause error) (*os.File, error) {
		_ = file.Close()
		return nil, cause
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return fail(errors.New("managed ensure lock identity changed"))
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return fail(fmt.Errorf("managed ensure lock is busy: %w", err))
	}
	current, err := os.Lstat(path)
	if err != nil || !os.SameFile(opened, current) {
		return fail(errors.New("managed ensure lock identity changed after acquisition"))
	}
	return file, nil
}

func setupProcessStopOwnedChild(command *exec.Cmd) error {
	if command == nil || command.ProcessState != nil {
		return nil
	}
	if command.Process == nil {
		return errors.New("owned direct child process is unavailable")
	}
	_ = command.Process.Kill()
	_ = command.Wait()
	if command.ProcessState == nil {
		return errors.New("owned direct child stop is unproven")
	}
	return nil
}

func (cleanup *setupProcessCleanup) run(t *testing.T) {
	t.Helper()
	stopped := true
	if err := setupProcessStopOwnedChild(cleanup.directChild); err != nil {
		t.Errorf("stop owned direct child: %v", err)
		stopped = false
	}
	if cleanup.directPermit != nil {
		if err := cleanup.directPermit.Close(); err != nil {
			t.Errorf("release owned direct launch permit: %v", err)
			stopped = false
		}
		cleanup.directPermit = nil
	}
	if cleanup.autoMayRun {
		client := cleanup.client
		if client == nil {
			client, _ = localapi.NewClient(cleanup.nodeState)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := setupProcessShutdown(ctx, client, cleanup.nodeState, cleanup.offline)
		cancel()
		if err != nil {
			t.Errorf("authenticated cleanup shutdown: %v", err)
			stopped = false
		}
	}
	if !stopped {
		t.Logf("preserving process-test root because stop proof is incomplete: %s", cleanup.root)
		return
	}
	if err := os.RemoveAll(cleanup.root); err != nil {
		t.Errorf("remove process-test directory: %v", err)
	}
}

func setupProcessPhysicalTempDir(t *testing.T) string {
	t.Helper()
	created, err := os.MkdirTemp("/tmp", "mnr5-")
	if err != nil {
		t.Fatalf("create short process-test root: %v", err)
	}
	physical, err := filepath.EvalSymlinks(created)
	if err != nil || !filepath.IsAbs(physical) || filepath.Clean(physical) != physical {
		_ = os.RemoveAll(created)
		t.Fatalf("resolve short physical process-test root: %v", err)
	}
	return physical
}

func setupProcessRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve process-test source")
	}
	for directory := filepath.Dir(source); ; directory = filepath.Dir(directory) {
		contents, err := os.ReadFile(filepath.Join(directory, "go.mod"))
		if err == nil && bytes.Contains(contents,
			[]byte("module github.com/mnemon-dev/mnemon\n")) {
			physical, resolveErr := filepath.EvalSymlinks(directory)
			if resolveErr != nil {
				t.Fatalf("resolve repository root: %v", resolveErr)
			}
			return physical
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("repository root is unavailable")
		}
	}
}

func setupProcessFingerprint(raw []byte) string {
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("len=%d sha256=%s", len(raw), hex.EncodeToString(sum[:]))
}

type setupProcessOutput struct {
	mu       sync.Mutex
	limit    int
	data     []byte
	overflow bool
}

func newSetupProcessOutput(limit int) *setupProcessOutput {
	return &setupProcessOutput{limit: limit}
}

func (output *setupProcessOutput) Write(value []byte) (int, error) {
	output.mu.Lock()
	defer output.mu.Unlock()
	remaining := output.limit - len(output.data)
	if remaining < len(value) {
		output.overflow = true
	}
	if remaining > len(value) {
		remaining = len(value)
	}
	if remaining > 0 {
		output.data = append(output.data, value[:remaining]...)
	}
	return len(value), nil
}

func (output *setupProcessOutput) bytes() []byte {
	output.mu.Lock()
	defer output.mu.Unlock()
	return append([]byte(nil), output.data...)
}

func (output *setupProcessOutput) overflowed() bool {
	output.mu.Lock()
	defer output.mu.Unlock()
	return output.overflow
}
