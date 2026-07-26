//go:build darwin || linux

package process_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const channelProcessConvergenceTimeout = 30 * time.Second

type channelProcessNode struct {
	name        string
	workspace   string
	nodeState   string
	environment []string
	peerID      string
	client      *localapi.Client
	autoMayRun  bool
	offline     setupProcessOfflineProbe
}

type channelProcessClusterCleanup struct {
	root  string
	nodes []*channelProcessNode
}

type channelProcessFixture struct {
	harnessExecutable string
	nodes             map[string]*channelProcessNode
}

type channelProcessInitialState struct {
	alpha map[string]localapi.ChannelView
	beta  map[string]localapi.ChannelView
}

// TestPublicChannelRemovalIsolatesOnlyOneOfTwoOverlappingChannels exercises
// three ordinary mnemond processes only through mnemon-harness. It treats the
// public signed roster, binding, baseline and topic projections as its oracle;
// no Store handle or test-only protocol surface is used.
func TestPublicChannelRemovalIsolatesOnlyOneOfTwoOverlappingChannels(t *testing.T) {
	fixture := channelProcessSetupFixture(t)
	initial := channelProcessEstablishOverlappingChannels(t, fixture)
	channelProcessRemoveAlphaMember(t, fixture, initial)
}

func channelProcessSetupFixture(t *testing.T) *channelProcessFixture {
	t.Helper()
	repository := setupProcessRepositoryRoot(t)
	root := setupProcessPhysicalTempDir(t)
	cleanup := &channelProcessClusterCleanup{root: root}
	t.Cleanup(func() { cleanup.run(t) })
	harnessExecutable, mnemondExecutable, bin := channelProcessBuildExecutables(t, repository, root)
	nodes := channelProcessDefineNodes(t, root, bin, mnemondExecutable, cleanup)
	for _, name := range []string{"A", "B", "C"} {
		channelProcessSetupNode(t, harnessExecutable, nodes[name])
	}
	channelProcessAssertDistinctIdentities(t, nodes)
	return &channelProcessFixture{harnessExecutable: harnessExecutable, nodes: nodes}
}

func channelProcessBuildExecutables(t *testing.T, repository, root string) (string, string, string) {
	t.Helper()
	bin := filepath.Join(root, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatalf("create Channel process-test bin: %v", err)
	}
	harnessExecutable := filepath.Join(bin, "mnemon-harness")
	mnemondExecutable := filepath.Join(bin, "mnemond")
	buildCtx, cancelBuild := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancelBuild()
	setupProcessBuild(t, buildCtx, repository, harnessExecutable,
		"./harness/cmd/mnemon-harness")
	setupProcessBuild(t, buildCtx, repository, mnemondExecutable,
		"./harness/cmd/mnemond")
	setupProcessFakeCodex(t, filepath.Join(bin, "codex"))
	return harnessExecutable, mnemondExecutable, bin
}

func channelProcessDefineNodes(t *testing.T, root, bin, mnemondExecutable string,
	cleanup *channelProcessClusterCleanup,
) map[string]*channelProcessNode {
	t.Helper()
	nodes := make(map[string]*channelProcessNode, 3)
	for _, name := range []string{"A", "B", "C"} {
		workspace := filepath.Join(root, "node-"+strings.ToLower(name))
		if err := os.Mkdir(workspace, 0o700); err != nil {
			t.Fatalf("create Node %s workspace: %v", name, err)
		}
		node := &channelProcessNode{name: name, workspace: workspace,
			nodeState:   filepath.Join(workspace, ".mnemon", "harness", "node"),
			environment: setupProcessEnvironment(bin, workspace, root)}
		node.offline = setupProcessOfflineProbe{executable: mnemondExecutable,
			workspace: workspace, environment: append([]string(nil), node.environment...)}
		nodes[name] = node
		cleanup.nodes = append(cleanup.nodes, node)
	}
	return nodes
}

func channelProcessSetupNode(t *testing.T, harnessExecutable string, node *channelProcessNode) {
	t.Helper()
	setupCtx, cancelSetup := context.WithTimeout(context.Background(), 30*time.Second)
	node.autoMayRun = true
	result := setupProcessRunSetup(setupCtx, harnessExecutable, node.workspace, node.environment)
	cancelSetup()
	receipt, err := setupProcessParseReceipt(result)
	if err != nil || receipt.SchemaVersion != 1 || receipt.Status != "ready" ||
		receipt.PeerID == "" {
		t.Fatalf("public setup for Node %s = (%#v, %v)", node.name, receipt, err)
	}
	node.peerID = receipt.PeerID
	node.client, err = localapi.NewClient(node.nodeState)
	if err != nil {
		t.Fatalf("construct authenticated cleanup client for Node %s: %v", node.name, err)
	}
}

func channelProcessAssertDistinctIdentities(t *testing.T, nodes map[string]*channelProcessNode) {
	t.Helper()
	if nodes["A"].peerID == nodes["B"].peerID || nodes["A"].peerID == nodes["C"].peerID ||
		nodes["B"].peerID == nodes["C"].peerID {
		t.Fatal("public setup did not create three distinct Node identities")
	}
}

func channelProcessEstablishOverlappingChannels(t *testing.T,
	fixture *channelProcessFixture,
) channelProcessInitialState {
	t.Helper()
	nodes := fixture.nodes
	executable := fixture.harnessExecutable
	alpha := channelProcessCreate(t, executable, nodes["A"], "Alpha", "alpha")
	channelProcessJoinWithToken(t, executable, nodes["B"], "alpha", alpha.InviteToken)
	channelProcessJoinWithToken(t, executable, nodes["C"], "alpha", alpha.InviteToken)
	beta := channelProcessCreate(t, executable, nodes["B"], "Beta", "beta")
	channelProcessJoinWithToken(t, executable, nodes["C"], "beta", beta.InviteToken)
	return channelProcessWaitInitialState(t, fixture)
}

func channelProcessCreate(t *testing.T, executable string, owner *channelProcessNode,
	name, alias string,
) localapi.ChannelCreateResponse {
	t.Helper()
	result := channelProcessRun(t, executable, owner, "channel", "create", name, "--json")
	receipt, err := channelProcessDecode[localapi.ChannelCreateResponse](result)
	if err != nil || receipt.SchemaVersion != 1 || receipt.Status != "created" ||
		receipt.Channel.Alias != alias || receipt.Channel.Name != name {
		t.Fatalf("public %s create receipt = (status=%q alias=%q name=%q, %v)",
			name, receipt.Status, receipt.Channel.Alias, receipt.Channel.Name, err)
	}
	return receipt
}

func channelProcessWaitInitialState(t *testing.T,
	fixture *channelProcessFixture,
) channelProcessInitialState {
	t.Helper()
	nodes := fixture.nodes
	executable := fixture.harnessExecutable
	alphaPeers := []string{nodes["A"].peerID, nodes["B"].peerID, nodes["C"].peerID}
	betaPeers := []string{nodes["B"].peerID, nodes["C"].peerID}
	alpha := channelProcessWaitReadyViews(t, executable, nodes, []string{"A", "B", "C"},
		"A", "alpha", "Alpha", alphaPeers)
	beta := channelProcessWaitReadyViews(t, executable, nodes, []string{"B", "C"},
		"B", "beta", "Beta", betaPeers)
	channelProcessAssertSharedAuthority(t, "initial Alpha", alpha["A"], alpha["B"], alpha["C"])
	channelProcessAssertSharedAuthority(t, "initial Beta", beta["B"], beta["C"])
	if alpha["A"].ChannelIDDigest == beta["B"].ChannelIDDigest {
		t.Fatal("Alpha and Beta expose the same public Channel identity")
	}
	channelProcessAssertAliases(t, executable, nodes["B"], []string{"alpha", "beta"})
	channelProcessAssertAliases(t, executable, nodes["C"], []string{"alpha", "beta"})
	return channelProcessInitialState{alpha: alpha, beta: beta}
}

func channelProcessWaitReadyViews(t *testing.T, executable string,
	nodes map[string]*channelProcessNode, names []string, ownerName, alias, channelName string,
	peers []string,
) map[string]localapi.ChannelView {
	t.Helper()
	views := make(map[string]localapi.ChannelView, len(names))
	for _, name := range names {
		views[name] = channelProcessWaitChannel(t, executable, nodes[name], alias,
			func(view localapi.ChannelView) error {
				return channelProcessAssertReady(view, nodes[name].peerID, nodes[ownerName].peerID,
					alias, channelName, peers)
			})
	}
	return views
}

func channelProcessRemoveAlphaMember(t *testing.T, fixture *channelProcessFixture,
	initial channelProcessInitialState,
) {
	t.Helper()
	nodes := fixture.nodes
	executable := fixture.harnessExecutable
	cAlias, err := channelProcessMemberAlias(initial.alpha["A"], nodes["C"].peerID)
	if err != nil {
		t.Fatal(err)
	}
	removed := channelProcessRun(t, executable, nodes["A"], "channel", "remove",
		"--channel", "alpha", cAlias, "--json")
	removeReceipt, err := channelProcessDecode[localapi.ChannelRemoveResponse](removed)
	if err != nil || removeReceipt.SchemaVersion != 1 || removeReceipt.Status != "removed" ||
		removeReceipt.Channel.Alias != "alpha" ||
		removeReceipt.Channel.RosterRevision != initial.alpha["A"].RosterRevision+1 {
		t.Fatalf("public Alpha removal receipt = (status=%q alias=%q revision=%d, %v)",
			removeReceipt.Status, removeReceipt.Channel.Alias,
			removeReceipt.Channel.RosterRevision, err)
	}
	channelProcessAssertRemovalOutcome(t, fixture, initial, removeReceipt.Channel.RosterRevision)
}

func channelProcessAssertRemovalOutcome(t *testing.T, fixture *channelProcessFixture,
	initial channelProcessInitialState, terminalRevision uint64,
) {
	t.Helper()
	nodes := fixture.nodes
	executable := fixture.harnessExecutable
	alphaPeers := []string{nodes["A"].peerID, nodes["B"].peerID, nodes["C"].peerID}
	betaPeers := []string{nodes["B"].peerID, nodes["C"].peerID}
	finalAlpha := make(map[string]localapi.ChannelView, 3)
	finalAlpha["A"] = channelProcessWaitChannel(t, executable, nodes["A"], "alpha",
		func(view localapi.ChannelView) error {
			return channelProcessAssertAlphaSurvivor(view, nodes["A"].peerID, nodes["A"].peerID,
				alphaPeers, nodes["C"].peerID, terminalRevision)
		})
	finalAlpha["B"] = channelProcessWaitChannel(t, executable, nodes["B"], "alpha",
		func(view localapi.ChannelView) error {
			return channelProcessAssertAlphaTerminalRoster(view, nodes["B"].peerID,
				nodes["A"].peerID, alphaPeers, nodes["C"].peerID, terminalRevision)
		})
	finalBeta := channelProcessWaitUnchangedBeta(t, executable, nodes, betaPeers, initial.beta)
	channelProcessAssertSharedAuthority(t, "surviving Beta", finalBeta["B"], finalBeta["C"])
	finalAlpha["C"] = channelProcessWaitChannel(t, executable, nodes["C"], "alpha",
		func(view localapi.ChannelView) error {
			return channelProcessAssertAlphaRemoved(view, nodes["C"].peerID, alphaPeers,
				terminalRevision)
		})
	channelProcessAssertSharedAuthority(t, "terminal Alpha", finalAlpha["A"],
		finalAlpha["B"], finalAlpha["C"])
	channelProcessAssertAliases(t, executable, nodes["B"], []string{"alpha", "beta"})
	channelProcessAssertAliases(t, executable, nodes["C"], []string{"alpha", "beta"})
}

func channelProcessWaitUnchangedBeta(t *testing.T, executable string,
	nodes map[string]*channelProcessNode, peers []string,
	initial map[string]localapi.ChannelView,
) map[string]localapi.ChannelView {
	t.Helper()
	views := make(map[string]localapi.ChannelView, 2)
	for _, name := range []string{"B", "C"} {
		views[name] = channelProcessWaitChannel(t, executable, nodes[name], "beta",
			func(view localapi.ChannelView) error {
				if err := channelProcessAssertReady(view, nodes[name].peerID, nodes["B"].peerID,
					"beta", "Beta", peers); err != nil {
					return err
				}
				if view.RosterHead != initial[name].RosterHead ||
					view.ChannelIDDigest != initial[name].ChannelIDDigest {
					return errors.New("Beta authority changed during Alpha removal")
				}
				return nil
			})
	}
	return views
}

func channelProcessRun(t *testing.T, executable string, node *channelProcessNode,
	arguments ...string,
) setupProcessResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return setupProcessRunHarness(ctx, executable, node.workspace, node.environment, arguments...)
}

func channelProcessJoinWithToken(t *testing.T, executable string, node *channelProcessNode,
	wantAlias, token string,
) {
	t.Helper()
	if token == "" || strings.TrimSpace(token) != token || len(token) > 64<<10 {
		t.Fatal("public create receipt returned an invalid bounded invite token")
	}
	path := filepath.Join(node.workspace, ".join-"+wantAlias)
	if err := os.WriteFile(path, []byte(token+"\n"), 0o600); err != nil {
		t.Fatalf("write protected %s invite file: %v", wantAlias, err)
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("%s invite file is not owner-only: %v", wantAlias, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), channelProcessConvergenceTimeout)
	defer cancel()
	var receipt localapi.ChannelJoinResponse
	var decodeErr error
	for {
		result := setupProcessRunHarness(ctx, executable, node.workspace, node.environment,
			"channel", "join", "--file", path, "--json")
		receipt, decodeErr = channelProcessDecode[localapi.ChannelJoinResponse](result)
		if decodeErr == nil && receipt.SchemaVersion == 1 && receipt.Status == "joined" &&
			receipt.Channel.Alias == wantAlias && receipt.Channel.Membership == "active" {
			break
		}
		if !channelProcessRetryable(result) {
			t.Fatalf("public %s join on Node %s = (status=%q alias=%q membership=%q, %v)",
				wantAlias, node.name, receipt.Status, receipt.Channel.Alias,
				receipt.Channel.Membership, decodeErr)
		}
		if err := setupProcessPoll(ctx); err != nil {
			t.Fatalf("public %s join on Node %s did not converge: %v", wantAlias,
				node.name, decodeErr)
		}
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove consumed %s invite file: %v", wantAlias, err)
	}
}

func channelProcessRetryable(result setupProcessResult) bool {
	var exitError *exec.ExitError
	if !errors.As(result.err, &exitError) || exitError.ExitCode() != 5 || result.overflow ||
		len(result.stdout) != 0 || len(result.stderr) == 0 || len(result.stderr) > 512 {
		return false
	}
	for _, prefix := range []string{"busy:", "owner_unreachable:", "roster_gap:"} {
		if bytes.HasPrefix(result.stderr, []byte(prefix)) {
			return true
		}
	}
	return false
}

func channelProcessDecode[T any](result setupProcessResult) (T, error) {
	var zero T
	if result.err != nil || result.overflow || len(result.stderr) != 0 || len(result.stdout) < 2 ||
		result.stdout[len(result.stdout)-1] != '\n' {
		return zero, fmt.Errorf("invalid process envelope: exit=%v stdout=%s stderr=%s overflow=%t",
			result.err, setupProcessFingerprint(result.stdout), setupProcessFingerprint(result.stderr),
			result.overflow)
	}
	decoder := json.NewDecoder(bytes.NewReader(result.stdout))
	decoder.DisallowUnknownFields()
	var response T
	if err := decoder.Decode(&response); err != nil {
		return zero, fmt.Errorf("decode public Channel response %s: %w",
			setupProcessFingerprint(result.stdout), err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return zero, errors.New("public Channel response has trailing content")
	}
	canonical, err := model.CanonicalMarshal(response)
	if err != nil || !bytes.Equal(result.stdout, append(canonical, '\n')) {
		return zero, errors.New("public Channel response is not one canonical JSON line")
	}
	return response, nil
}

func channelProcessWaitChannel(t *testing.T, executable string, node *channelProcessNode,
	alias string, accept func(localapi.ChannelView) error,
) localapi.ChannelView {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), channelProcessConvergenceTimeout)
	defer cancel()
	var last localapi.ChannelView
	var lastErr error
	for {
		result := setupProcessRunHarness(ctx, executable, node.workspace, node.environment,
			"channel", "status", alias, "--json")
		response, err := channelProcessDecode[localapi.ChannelStatusResponse](result)
		if err == nil && response.SchemaVersion == 1 && response.Status == "ok" &&
			len(response.Channels) == 1 {
			last = response.Channels[0]
			err = accept(last)
		}
		if ctx.Err() == nil {
			lastErr = err
		}
		if err == nil && len(response.Channels) == 1 {
			return response.Channels[0]
		}
		if err := setupProcessPoll(ctx); err != nil {
			t.Fatalf("Node %s Channel %s did not converge: last=%s error=%v",
				node.name, alias, channelProcessStatusSummary(last), lastErr)
		}
	}
}

func channelProcessAssertReady(view localapi.ChannelView, localPeer, ownerPeer, alias, name string,
	peers []string,
) error {
	return errors.Join(
		channelProcessAssertReadyProjection(view, alias, name, len(peers)),
		channelProcessAssertOwner(view, localPeer, ownerPeer),
		channelProcessAssertReadyMembers(view, localPeer, peers),
	)
}

func channelProcessAssertReadyProjection(view localapi.ChannelView, alias, name string,
	memberCount int,
) error {
	if view.Alias != alias || view.Name != name || view.Membership != "active" ||
		view.Topic.Status != "joined" || int(view.Topic.TotalMembers) != memberCount ||
		int(view.Topic.ReadyMembers) != memberCount || view.RosterRevision != uint64(memberCount) ||
		view.RosterHead.Revision != view.RosterRevision {
		return errors.New("Channel is not joined with a complete ready signed roster")
	}
	return nil
}

func channelProcessAssertReadyMembers(view localapi.ChannelView, localPeer string,
	peers []string,
) error {
	members, err := channelProcessMembers(view, peers)
	if err != nil {
		return err
	}
	for peer, member := range members {
		if err := channelProcessAssertReadyMember(peer, member, localPeer); err != nil {
			return err
		}
	}
	return nil
}

func channelProcessAssertReadyMember(peer string, member localapi.ChannelMemberView,
	localPeer string,
) error {
	if member.Status != "active" || !member.BaselineReady {
		return fmt.Errorf("member %s is not active with a bidirectional baseline", peer)
	}
	if peer == localPeer && (member.Binding != "self" || member.Reachability != "self") {
		return errors.New("local member projection is not self-authoritative")
	}
	if peer != localPeer && (member.Binding != "active" || member.Reachability != "reachable") {
		return fmt.Errorf("remote member %s is not active and reachable", peer)
	}
	return nil
}

func channelProcessAssertAlphaSurvivor(view localapi.ChannelView, localPeer, ownerPeer string,
	peers []string, removedPeer string, revision uint64,
) error {
	if view.Alias != "alpha" || view.Name != "Alpha" || view.Membership != "active" ||
		view.Topic.Status != "joined" || view.Topic.TotalMembers != 3 ||
		view.Topic.ReadyMembers != 2 || view.RosterRevision != revision ||
		view.RosterHead.Revision != revision {
		return errors.New("surviving Alpha projection has not converged to the terminal roster")
	}
	if err := channelProcessAssertOwner(view, localPeer, ownerPeer); err != nil {
		return err
	}
	members, err := channelProcessMembers(view, peers)
	if err != nil {
		return err
	}
	for peer, member := range members {
		if peer == removedPeer {
			if member.Status != "revoked" ||
				(member.Binding != "revoked" && member.Binding != "none") || member.BaselineReady {
				return errors.New("removed Alpha member is not terminal and baseline-isolated")
			}
			continue
		}
		if member.Status != "active" || !member.BaselineReady {
			return errors.New("surviving Alpha member lost active baseline readiness")
		}
	}
	return nil
}

func channelProcessAssertAlphaTerminalRoster(view localapi.ChannelView, localPeer, ownerPeer string,
	peers []string, removedPeer string, revision uint64,
) error {
	if view.Alias != "alpha" || view.Name != "Alpha" || view.Membership != "active" ||
		(view.Topic.Status != "joined" && view.Topic.Status != "converging") ||
		view.RosterRevision != revision || view.RosterHead.Revision != revision {
		return errors.New("Alpha observer lacks the terminal signed roster")
	}
	if err := channelProcessAssertOwner(view, localPeer, ownerPeer); err != nil {
		return err
	}
	members, err := channelProcessMembers(view, peers)
	if err != nil {
		return err
	}
	for peer, member := range members {
		if peer == removedPeer {
			if member.Status != "revoked" || member.Binding == "active" || member.BaselineReady {
				return errors.New("Alpha observer has not isolated the removed member")
			}
			continue
		}
		if member.Status != "active" {
			return errors.New("Alpha observer changed a surviving member terminally")
		}
	}
	return nil
}

func channelProcessStatusSummary(view localapi.ChannelView) string {
	members := make([]string, 0, len(view.Members))
	for _, member := range view.Members {
		members = append(members, fmt.Sprintf("%s/%s/%t", member.Status, member.Binding,
			member.BaselineReady))
	}
	return fmt.Sprintf("alias=%s membership=%s revision=%d topic=%s ready=%d/%d members=%v",
		view.Alias, view.Membership, view.RosterRevision, view.Topic.Status,
		view.Topic.ReadyMembers, view.Topic.TotalMembers, members)
}

func channelProcessAssertAlphaRemoved(view localapi.ChannelView, localPeer string,
	peers []string, revision uint64,
) error {
	if view.Alias != "alpha" || view.Name != "Alpha" || view.Membership != "left" ||
		view.Topic.Status != "left" || view.RosterRevision != revision ||
		view.RosterHead.Revision != revision {
		return errors.New("removed Node has not entered terminal Alpha isolation")
	}
	members, err := channelProcessMembers(view, peers)
	if err != nil {
		return err
	}
	self := members[localPeer]
	if self.Status != "revoked" || self.Binding != "self" || self.Reachability != "self" ||
		self.BaselineReady {
		return errors.New("removed Node lacks terminal self evidence for Alpha")
	}
	return nil
}

func channelProcessAssertOwner(view localapi.ChannelView, localPeer, ownerPeer string) error {
	wantLocal := localPeer == ownerPeer
	if view.RosterHead.OwnerPeerID != ownerPeer || view.Owner.Local != wantLocal {
		return errors.New("public owner projection differs from signed roster authority")
	}
	wantReachability := "reachable"
	if wantLocal {
		wantReachability = "self"
	}
	if view.Owner.Reachability != wantReachability {
		return errors.New("public owner reachability is not converged")
	}
	return nil
}

func channelProcessMembers(view localapi.ChannelView,
	peers []string,
) (map[string]localapi.ChannelMemberView, error) {
	if len(view.Members) != len(peers) {
		return nil, fmt.Errorf("member count = %d, want %d", len(view.Members), len(peers))
	}
	want := make(map[string]struct{}, len(peers))
	for _, peer := range peers {
		want[peer] = struct{}{}
	}
	members := make(map[string]localapi.ChannelMemberView, len(view.Members))
	for _, member := range view.Members {
		if _, exists := want[member.PeerID]; !exists {
			return nil, errors.New("public roster contains an unexpected PeerID")
		}
		if _, duplicate := members[member.PeerID]; duplicate {
			return nil, errors.New("public roster contains a duplicate PeerID")
		}
		members[member.PeerID] = member
	}
	return members, nil
}

func channelProcessMemberAlias(view localapi.ChannelView, peerID string) (string, error) {
	for _, member := range view.Members {
		if member.PeerID == peerID && member.Status == "active" && member.Alias != "" {
			return member.Alias, nil
		}
	}
	return "", errors.New("public Alpha roster has no active alias for Node C")
}

func channelProcessAssertSharedAuthority(t *testing.T, label string,
	views ...localapi.ChannelView,
) {
	t.Helper()
	if len(views) < 2 {
		t.Fatalf("%s lacks independent Node observations", label)
	}
	for _, view := range views[1:] {
		if view.ChannelIDDigest != views[0].ChannelIDDigest ||
			view.RosterHead != views[0].RosterHead || view.RosterRevision != views[0].RosterRevision {
			t.Fatalf("%s signed authority differs across public Node views", label)
		}
	}
}

func channelProcessAssertAliases(t *testing.T, executable string, node *channelProcessNode,
	want []string,
) {
	t.Helper()
	result := channelProcessRun(t, executable, node, "channel", "status", "--json")
	response, err := channelProcessDecode[localapi.ChannelStatusResponse](result)
	if err != nil || response.SchemaVersion != 1 || response.Status != "ok" {
		t.Fatalf("Node %s public Channel list: %v", node.name, err)
	}
	aliases := make([]string, len(response.Channels))
	for index, channel := range response.Channels {
		aliases[index] = channel.Alias
	}
	if !slices.Equal(aliases, want) {
		t.Fatalf("Node %s Channel aliases = %v, want %v", node.name, aliases, want)
	}
}

func (cleanup *channelProcessClusterCleanup) run(t *testing.T) {
	t.Helper()
	stopped := true
	for _, node := range cleanup.nodes {
		if !node.autoMayRun {
			continue
		}
		client := node.client
		if client == nil {
			client, _ = localapi.NewClient(node.nodeState)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := setupProcessShutdown(ctx, client, node.nodeState, node.offline)
		cancel()
		if err != nil {
			t.Errorf("authenticated cleanup shutdown for Node %s: %v", node.name, err)
			stopped = false
		}
	}
	if !stopped {
		t.Logf("preserving Channel process-test root because stop proof is incomplete: %s", cleanup.root)
		return
	}
	if err := os.RemoveAll(cleanup.root); err != nil {
		t.Errorf("remove Channel process-test directory: %v", err)
	}
}
