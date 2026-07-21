//go:build darwin || linux

package process_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/integration"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type setupProcessPaths struct {
	repository        string
	root              string
	bin               string
	workspace         string
	harnessExecutable string
	mnemondExecutable string
	nodeState         string
}

func TestPublicEjectPreservesNodeAndSwitchesToObservableClaudeHost(t *testing.T) {
	fixture := newSetupHostSwitchFixture(t)
	setupReceipt := fixture.installCodex(t)
	snapshots := fixture.snapshotNodeFiles(t)
	fixture.ejectCodex(t, setupReceipt, snapshots)
	fixture.assertEjectReplay(t, setupReceipt)
	fixture.switchToClaude(t, setupReceipt)
	fixture.ejectClaude(t, setupReceipt)
	fixture.reactivateCodex(t, setupReceipt, snapshots)
}

type setupHostSwitchFixture struct {
	root              string
	bin               string
	workspace         string
	harnessExecutable string
	mnemondExecutable string
	nodeState         string
	environment       []string
	cleanup           *setupProcessCleanup
	client            *localapi.Client
	bundle            assets.Bundle
}

type setupHostSwitchSnapshots struct {
	databaseInfo   os.FileInfo
	identityInfo   os.FileInfo
	identityRaw    []byte
	credentialInfo os.FileInfo
	credentialRaw  []byte
}

func newSetupHostSwitchFixture(t *testing.T) *setupHostSwitchFixture {
	t.Helper()
	paths := newSetupProcessPaths(t)
	cleanup := &setupProcessCleanup{root: paths.root, nodeState: paths.nodeState}
	t.Cleanup(func() { cleanup.run(t) })
	setupProcessPrepareDirectories(t, paths, "eject")
	setupProcessBuildHarnessCommands(t, paths)
	setupProcessFakeCodex(t, filepath.Join(paths.bin, "codex"))
	setupProcessFakeClaude(t, filepath.Join(paths.bin, "claude"))
	environment := setupProcessEnvironment(paths.bin, paths.workspace, paths.root)
	cleanup.offline = setupProcessOfflineProbe{executable: paths.mnemondExecutable,
		workspace: paths.workspace, environment: append([]string(nil), environment...)}
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	return &setupHostSwitchFixture{root: paths.root, bin: paths.bin, workspace: paths.workspace,
		harnessExecutable: paths.harnessExecutable, mnemondExecutable: paths.mnemondExecutable,
		nodeState: paths.nodeState, environment: environment, cleanup: cleanup, bundle: bundle}
}

func newSetupProcessPaths(t *testing.T) setupProcessPaths {
	t.Helper()
	repository := setupProcessRepositoryRoot(t)
	root := setupProcessPhysicalTempDir(t)
	bin := filepath.Join(root, "bin")
	workspace := filepath.Join(root, "work")
	return setupProcessPaths{repository: repository, root: root, bin: bin, workspace: workspace,
		harnessExecutable: filepath.Join(bin, "mnemon-harness"),
		mnemondExecutable: filepath.Join(bin, "mnemond"),
		nodeState:         filepath.Join(workspace, ".mnemon", "harness", "node")}
}

func setupProcessPrepareDirectories(t *testing.T, paths setupProcessPaths, name string) {
	t.Helper()
	for _, path := range []string{paths.bin, paths.workspace} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatalf("create %s process-test directory: %v", name, err)
		}
	}
}

func setupProcessBuildHarnessCommands(t *testing.T, paths setupProcessPaths) {
	t.Helper()
	buildCtx, cancelBuild := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancelBuild()
	setupProcessBuild(t, buildCtx, paths.repository, paths.harnessExecutable,
		"./harness/cmd/mnemon-harness")
	setupProcessBuild(t, buildCtx, paths.repository, paths.mnemondExecutable,
		"./harness/cmd/mnemond")
}

func (fixture *setupHostSwitchFixture) installCodex(t *testing.T) setupProcessReceipt {
	t.Helper()
	setupCtx, cancelSetup := context.WithTimeout(context.Background(), 20*time.Second)
	fixture.cleanup.autoMayRun = true
	installed := setupProcessRunSetup(setupCtx, fixture.harnessExecutable, fixture.workspace,
		fixture.environment)
	cancelSetup()
	setupReceipt, err := setupProcessParseReceipt(installed)
	if err != nil || setupReceipt.Host != "codex" || setupReceipt.Replayed {
		t.Fatalf("initial public setup = (%#v, %v)", setupReceipt, err)
	}
	client, err := localapi.NewClient(fixture.nodeState)
	if err != nil {
		t.Fatal(err)
	}
	fixture.client = client
	fixture.cleanup.client = client
	readyCtx, cancelReady := context.WithTimeout(context.Background(), 5*time.Second)
	if err := setupProcessWaitReady(readyCtx, client, setupReceipt.AssetRevision); err != nil {
		cancelReady()
		t.Fatalf("initial setup readiness: %v", err)
	}
	cancelReady()
	return setupReceipt
}

func (fixture *setupHostSwitchFixture) snapshotNodeFiles(t *testing.T) setupHostSwitchSnapshots {
	t.Helper()
	databaseInfo, _ := setupProcessSnapshotFile(t, filepath.Join(fixture.nodeState, "node.db"), 0)
	identityInfo, identityRaw := setupProcessSnapshotFile(t,
		filepath.Join(fixture.nodeState, "identity.key"), 4096)
	credentialInfo, credentialRaw := setupProcessSnapshotFile(t,
		fixture.credentialPath(), 4096)
	return setupHostSwitchSnapshots{databaseInfo: databaseInfo, identityInfo: identityInfo,
		identityRaw: identityRaw, credentialInfo: credentialInfo, credentialRaw: credentialRaw}
}

func (fixture *setupHostSwitchFixture) ejectCodex(t *testing.T,
	setupReceipt setupProcessReceipt, snapshots setupHostSwitchSnapshots,
) {
	t.Helper()
	ejectCtx, cancelEject := context.WithTimeout(context.Background(), 20*time.Second)
	ejected := setupProcessRunEject(ejectCtx, fixture.harnessExecutable, fixture.workspace,
		fixture.environment)
	cancelEject()
	ejectReceipt, err := setupProcessParseEjectReceipt(ejected)
	if err != nil || ejectReceipt.AssetRevision != setupReceipt.AssetRevision ||
		ejectReceipt.Host != "codex" || ejectReceipt.PeerID != setupReceipt.PeerID ||
		ejectReceipt.RemovedFiles != 3 || !ejectReceipt.RegistrationRemoved ||
		ejectReceipt.Replayed || ejectReceipt.Status != "ejected" {
		t.Fatalf("public eject receipt = (%#v, %v)", ejectReceipt, err)
	}
	offlineCtx, cancelOffline := context.WithTimeout(context.Background(), 5*time.Second)
	if err := setupProcessWaitOffline(offlineCtx, fixture.client, fixture.nodeState,
		fixture.cleanup.offline); err != nil {
		cancelOffline()
		t.Fatalf("eject did not leave the Node offline: %v", err)
	}
	cancelOffline()
	fixture.cleanup.autoMayRun = false
	if err := integration.VerifyHostProjectionAbsent(fixture.workspace, fixture.nodeState,
		assets.HostCodex, fixture.bundle); err != nil {
		t.Fatalf("Codex projection remains after eject: %v", err)
	}
	setupProcessAssertCodexProjectionLayout(t, fixture.workspace, false)
	if err := integration.VerifyNodeBundle(fixture.nodeState, fixture.bundle); err != nil {
		t.Fatalf("eject removed the immutable Node bundle: %v", err)
	}
	fixture.assertPreservedNodeFiles(t, snapshots)
}

func (fixture *setupHostSwitchFixture) assertEjectReplay(t *testing.T,
	setupReceipt setupProcessReceipt,
) {
	t.Helper()
	replayCtx, cancelReplay := context.WithTimeout(context.Background(), 15*time.Second)
	replayed := setupProcessRunEject(replayCtx, fixture.harnessExecutable, fixture.workspace,
		fixture.environment)
	cancelReplay()
	replayReceipt, err := setupProcessParseEjectReceipt(replayed)
	if err != nil || !replayReceipt.Replayed || replayReceipt.RemovedFiles != 0 ||
		replayReceipt.RegistrationRemoved || replayReceipt.PeerID != setupReceipt.PeerID {
		t.Fatalf("replayed public eject = (%#v, %v)", replayReceipt, err)
	}
}

func (fixture *setupHostSwitchFixture) switchToClaude(t *testing.T,
	setupReceipt setupProcessReceipt,
) setupProcessReceipt {
	t.Helper()
	switchCtx, cancelSwitch := context.WithTimeout(context.Background(), 20*time.Second)
	fixture.cleanup.autoMayRun = true
	switched := setupProcessRunHarness(switchCtx, fixture.harnessExecutable, fixture.workspace,
		fixture.environment, "setup", "--host", "claude-code", "--project-root",
		fixture.workspace)
	cancelSwitch()
	switchReceipt, err := setupProcessParseReceipt(switched)
	if err != nil || switchReceipt.Host != "claude-code" || switchReceipt.Replayed ||
		!switchReceipt.Started || switchReceipt.PeerID != setupReceipt.PeerID ||
		switchReceipt.AssetRevision != setupReceipt.AssetRevision {
		t.Fatalf("observable Claude Host switch = (%#v, %v)", switchReceipt, err)
	}
	switchReadyCtx, cancelSwitchReady := context.WithTimeout(context.Background(), 5*time.Second)
	if err := setupProcessWaitReady(switchReadyCtx, fixture.client,
		switchReceipt.AssetRevision); err != nil {
		cancelSwitchReady()
		t.Fatalf("Claude daemon did not become ready: %v", err)
	}
	cancelSwitchReady()
	if err := integration.VerifyHostProjection(fixture.workspace, fixture.nodeState,
		assets.HostClaudeCode, fixture.bundle); err != nil {
		t.Fatalf("Claude switch did not install its projection: %v", err)
	}
	if err := integration.VerifyHostProjectionAbsent(fixture.workspace, fixture.nodeState,
		assets.HostCodex, fixture.bundle); err != nil {
		t.Fatalf("Claude switch recreated the Codex projection: %v", err)
	}
	setupProcessAssertCodexProjectionLayout(t, fixture.workspace, false)
	authority, authorityErr := fixture.client.ReadAuthority(context.Background())
	if authorityErr != nil || !authority.Enabled || authority.Host != string(model.HostClaudeCode) ||
		authority.PeerID != setupReceipt.PeerID || authority.Runtime != string(model.RuntimeClaudeCLI) {
		t.Fatalf("Claude durable authority = (%#v, %#v)", authority, authorityErr)
	}
	return switchReceipt
}

func (fixture *setupHostSwitchFixture) ejectClaude(t *testing.T,
	setupReceipt setupProcessReceipt,
) {
	t.Helper()
	claudeEjectCtx, cancelClaudeEject := context.WithTimeout(context.Background(), 20*time.Second)
	claudeEjected := setupProcessRunEject(claudeEjectCtx, fixture.harnessExecutable,
		fixture.workspace, fixture.environment)
	cancelClaudeEject()
	claudeEjectReceipt, err := setupProcessParseEjectReceipt(claudeEjected)
	if err != nil || claudeEjectReceipt.Host != "claude-code" ||
		claudeEjectReceipt.PeerID != setupReceipt.PeerID || claudeEjectReceipt.Replayed ||
		claudeEjectReceipt.RemovedFiles != 3 || !claudeEjectReceipt.RegistrationRemoved {
		t.Fatalf("Claude eject receipt = (%#v, %v)", claudeEjectReceipt, err)
	}
	claudeOfflineCtx, cancelClaudeOffline := context.WithTimeout(context.Background(), 5*time.Second)
	if err := setupProcessWaitOffline(claudeOfflineCtx, fixture.client, fixture.nodeState,
		fixture.cleanup.offline); err != nil {
		cancelClaudeOffline()
		t.Fatalf("Claude eject did not leave the Node offline: %v", err)
	}
	cancelClaudeOffline()
	fixture.cleanup.autoMayRun = false
	if err := integration.VerifyHostProjectionAbsent(fixture.workspace, fixture.nodeState,
		assets.HostClaudeCode, fixture.bundle); err != nil {
		t.Fatalf("Claude projection remains after eject: %v", err)
	}
}

func (fixture *setupHostSwitchFixture) reactivateCodex(t *testing.T,
	setupReceipt setupProcessReceipt, snapshots setupHostSwitchSnapshots,
) {
	t.Helper()
	reactivateCtx, cancelReactivate := context.WithTimeout(context.Background(), 20*time.Second)
	fixture.cleanup.autoMayRun = true
	reactivated := setupProcessRunHarness(reactivateCtx, fixture.harnessExecutable, fixture.workspace,
		fixture.environment, "setup", "--host", "codex", "--project-root",
		fixture.workspace)
	cancelReactivate()
	reactivateReceipt, err := setupProcessParseReceipt(reactivated)
	if err != nil || reactivateReceipt.Host != "codex" || reactivateReceipt.Replayed ||
		!reactivateReceipt.Started || reactivateReceipt.PeerID != setupReceipt.PeerID ||
		reactivateReceipt.AssetRevision != setupReceipt.AssetRevision {
		t.Fatalf("explicit Codex reactivation = (%#v, %v)", reactivateReceipt, err)
	}
	reactivatedReadyCtx, cancelReactivatedReady := context.WithTimeout(context.Background(), 5*time.Second)
	if err := setupProcessWaitReady(reactivatedReadyCtx, fixture.client,
		reactivateReceipt.AssetRevision); err != nil {
		cancelReactivatedReady()
		t.Fatalf("reactivated Codex daemon did not become ready: %v", err)
	}
	cancelReactivatedReady()
	if err := integration.VerifyHostProjection(fixture.workspace, fixture.nodeState,
		assets.HostCodex, fixture.bundle); err != nil {
		t.Fatalf("Codex reactivation did not restore its projection: %v", err)
	}
	setupProcessAssertCodexProjectionLayout(t, fixture.workspace, true)
	fixture.assertPreservedNodeFiles(t, snapshots)
}

func (fixture *setupHostSwitchFixture) credentialPath() string {
	return filepath.Join(fixture.nodeState, "profiles",
		model.TeamworkProfileID().String()+".token")
}

func (fixture *setupHostSwitchFixture) assertPreservedNodeFiles(t *testing.T,
	snapshots setupHostSwitchSnapshots,
) {
	t.Helper()
	setupProcessAssertPreservedFile(t, filepath.Join(fixture.nodeState, "node.db"),
		snapshots.databaseInfo, nil)
	setupProcessAssertPreservedFile(t, filepath.Join(fixture.nodeState, "identity.key"),
		snapshots.identityInfo, snapshots.identityRaw)
	setupProcessAssertPreservedFile(t, fixture.credentialPath(), snapshots.credentialInfo,
		snapshots.credentialRaw)
}
