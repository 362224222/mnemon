package peer

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestGossipAuthorityTransitionDrainsWithoutHoldingBroadLocks(t *testing.T) {
	fixture := newGossipAuthorityTransitionFixture(t)
	transition := beginDrainedGossipAuthorityTransition(t, fixture)
	session, gossip := fixture.session, fixture.gossip
	if session.gate.tryAcquire() {
		session.gate.release()
		t.Fatal("affected Channel admitted work during durable window")
	}
	if !gossip.mu.TryLock() {
		t.Fatal("durable window retained Gossip mutex")
	}
	gossip.mu.Unlock()
	if !fixture.authority.updateMu.TryLock() {
		t.Fatal("durable window retained Authority update mutex")
	}
	fixture.authority.updateMu.Unlock()
	active, err := gossip.BeginAuthorityTransition(fixture.snapshot)
	if active != transition || !errors.Is(err, ErrGossipTransitionInProgress) {
		t.Fatalf("transition reentry = (%p, %v), want active %p", active, err, transition)
	}
	waitingSession := make(chan *TopicSession, 1)
	waitingError := make(chan error, 1)
	go func() {
		current, joinErr := gossip.Join(fixture.channel.ChannelID)
		waitingSession <- current
		waitingError <- joinErr
	}()
	select {
	case current := <-waitingSession:
		t.Fatalf("Session returned inside durable window: %p", current)
	case <-time.After(100 * time.Millisecond):
	}
	if err := transition.Abort(); err != nil || transition.Wait() != nil {
		t.Fatalf("abort transition = %v / wait %v", err, transition.Wait())
	}
	if current, joinErr := <-waitingSession, <-waitingError; joinErr != nil || current != session {
		t.Fatalf("Session after abort = (%p, %v), want old %p", current, joinErr, session)
	}
	if session.closed.Load() || !session.IsCurrent() || !gossip.HasCurrentSession(fixture.channel.ChannelID) {
		t.Fatal("abort changed the old runtime authority")
	}

	assertGossipTransitionInstallWaitsForSessionClose(t, gossip, session, fixture.snapshot)
}

func TestGossipAuthorityTransitionInstallFailureStaysFailClosed(t *testing.T) {
	fixture := newGossipAuthorityTransitionFixture(t)
	transition, err := fixture.gossip.BeginAuthorityTransition(fixture.snapshot)
	if err != nil {
		t.Fatal(err)
	}
	gate := fixture.session.gate
	// Simulate an internal token loss after the caller's durable CAS committed.
	// Install must stop the router instead of restoring the pre-CAS authority.
	fixture.gossip.mu.Lock()
	fixture.gossip.transition = nil
	fixture.gossip.mu.Unlock()
	installErr := transition.Install()
	if !errors.Is(installErr, ErrGossipTopic) ||
		!errors.Is(transition.Wait(), ErrGossipTopic) {
		t.Fatalf("early Install failure = %v / wait %v", installErr, transition.Wait())
	}
	fixture.gossip.mu.Lock()
	gateCount := len(fixture.gossip.gates)
	closed := fixture.gossip.closed
	fixture.gossip.mu.Unlock()
	if !closed || gateCount != 0 || !gate.isRetired() || !fixture.session.closed.Load() {
		t.Fatalf("failed Install reopened old runtime: closed=%v gates=%d retired=%v session_closed=%v",
			closed, gateCount, gate.isRetired(), fixture.session.closed.Load())
	}
	if gate.tryAcquire() {
		gate.release()
		t.Fatal("failed Install reopened old Channel admission")
	}
	if _, err := fixture.gossip.Join(fixture.channel.ChannelID); !errors.Is(err, ErrGossipTopic) {
		t.Fatalf("Join after failed Install error = %v", err)
	}
	if err := transition.Abort(); !errors.Is(err, ErrGossipTransitionFinalized) {
		t.Fatalf("Abort after failed Install error = %v", err)
	}
}

type gossipAuthorityTransitionFixture struct {
	authority *Authority
	gossip    *Gossip
	session   *TopicSession
	channel   ChannelAuthoritySnapshot
	snapshot  NetworkAuthoritySnapshot
}

func newGossipAuthorityTransitionFixture(t *testing.T) gossipAuthorityTransitionFixture {
	t.Helper()
	local := testAuthorityPeer(t, "gossip-commit-local")
	remote := testAuthorityPeer(t, "gossip-commit-remote")
	channel := testAuthorityChannel(t, "gossip-commit-channel", model.BindingActive, local, remote)
	authority, _ := NewAuthority(local.modelID)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{channel}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	nodeHost, err := libp2p.New(libp2p.Identity(local.libp2pPrivate),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	gossip, err := NewGossip(ctx, nodeHost, authority)
	if err != nil {
		t.Fatal(err)
	}
	session, err := gossip.Join(channel.ChannelID)
	if err != nil {
		t.Fatal(err)
	}
	candidate := nextTopicAuthorityGeneration(t, channel, "gossip-commit-roster-3")
	snapshot := NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{candidate}}
	t.Cleanup(func() {
		cancel()
		_ = gossip.Close()
		_ = nodeHost.Close()
	})
	return gossipAuthorityTransitionFixture{authority: authority, gossip: gossip,
		session: session, channel: channel, snapshot: snapshot}
}

type gossipAuthorityTransitionResult struct {
	transition *GossipAuthorityTransition
	err        error
}

func beginDrainedGossipAuthorityTransition(t *testing.T,
	fixture gossipAuthorityTransitionFixture,
) *GossipAuthorityTransition {
	t.Helper()
	requireGateAdmission(t, fixture.session.gate, context.Background())
	begun := make(chan gossipAuthorityTransitionResult, 1)
	go func() {
		transition, beginErr := fixture.gossip.BeginAuthorityTransition(fixture.snapshot)
		begun <- gossipAuthorityTransitionResult{transition: transition, err: beginErr}
	}()
	select {
	case result := <-begun:
		fixture.session.gate.release()
		t.Fatalf("transition returned before existing admission drained: %#v", result)
	case <-time.After(100 * time.Millisecond):
	}
	fixture.session.gate.release()
	result := <-begun
	if result.err != nil || result.transition == nil {
		t.Fatalf("begin transition = (%v, %v)", result.transition, result.err)
	}
	return result.transition
}

func assertGossipTransitionInstallWaitsForSessionClose(t *testing.T, gossip *Gossip,
	session *TopicSession, snapshot NetworkAuthoritySnapshot,
) {
	t.Helper()
	transition, err := gossip.BeginAuthorityTransition(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- session.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Session.Close returned inside authority transition: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := transition.Install(); err != nil {
		t.Fatalf("install transition: %v", err)
	}
	if err := <-closed; err != nil {
		t.Fatalf("Session.Close after install: %v", err)
	}
	if !session.closed.Load() || session.IsCurrent() {
		t.Fatal("installed authority did not retire the old session")
	}
	replacement, err := gossip.Join(session.channelID)
	if err != nil || replacement == session || !replacement.IsCurrent() ||
		!gossip.HasCurrentSession(session.channelID) {
		t.Fatalf("post-commit TopicSession = (%p,%v), previous %p", replacement, err, session)
	}
}

func TestGossipRetiresTerminalAndAbsentGatesUnderChannelChurn(t *testing.T) {
	local := testAuthorityPeer(t, "gossip-gate-churn-local")
	remote := testAuthorityPeer(t, "gossip-gate-churn-remote")
	authority, _ := NewAuthority(local.modelID)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nodeHost, err := libp2p.New(libp2p.Identity(local.libp2pPrivate),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer nodeHost.Close()
	gossip, err := NewGossip(ctx, nodeHost, authority)
	if err != nil {
		t.Fatal(err)
	}
	defer gossip.Close()

	for iteration := 0; iteration < model.MaxChannelsPerNode*3; iteration++ {
		assertGossipRetiredGateIteration(t, gossip, local, remote, iteration)
	}
}

func assertGossipRetiredGateIteration(t *testing.T, gossip *Gossip,
	local, remote authorityTestPeer, iteration int,
) {
	t.Helper()
	channel := testAuthorityChannel(t, fmt.Sprintf("gossip-gate-churn-%d", iteration),
		model.BindingActive, local, remote)
	active := NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{channel}}
	if err := gossip.Reconcile(active); err != nil {
		t.Fatal(err)
	}
	session, err := gossip.Join(channel.ChannelID)
	if err != nil {
		t.Fatal(err)
	}
	gate := session.gate
	validator := gossip.validator(session)
	topicName, _ := TopicName(channel.ChannelID)
	message := testPubSubMessage(topicName,
		testPeerPublication(t, channel, local, remote, fmt.Sprintf("stale-%d", iteration)),
		local.libp2pID, remote.libp2pID)

	candidate := NetworkAuthoritySnapshot{LocalPeerID: local.modelID}
	if iteration%2 == 0 {
		terminal := channel
		terminal.Status = model.ChannelClosed
		candidate.Channels = []ChannelAuthoritySnapshot{terminal}
	}
	if err := gossip.Reconcile(candidate); err != nil {
		t.Fatal(err)
	}
	gossip.mu.Lock()
	gateCount := len(gossip.gates)
	_, retained := gossip.gates[channel.ChannelID]
	gossip.mu.Unlock()
	if gateCount > model.MaxChannelsPerNode || retained || !gate.isRetired() ||
		!session.closed.Load() {
		t.Fatalf("iteration %d retained terminal gate: count=%d retained=%v retired=%v closed=%v",
			iteration, gateCount, retained, gate.isRetired(), session.closed.Load())
	}
	validated := make(chan pubsub.ValidationResult, 1)
	go func() {
		validated <- validator(context.Background(), remote.libp2pID, message)
	}()
	select {
	case result := <-validated:
		if result != pubsub.ValidationReject {
			t.Fatalf("iteration %d late validator result = %v", iteration, result)
		}
	case <-time.After(time.Second):
		t.Fatalf("iteration %d late validator blocked on retired gate", iteration)
	}
}

func nextTopicAuthorityGeneration(t *testing.T, channel ChannelAuthoritySnapshot,
	seed string,
) ChannelAuthoritySnapshot {
	t.Helper()
	head, _ := model.NewRecordHead(channel.RosterHead.Revision()+1, model.Sum([]byte(seed)))
	candidate := channel
	candidate.RosterHead = head
	candidate.VerifiedRosterHeads = append(
		append([]model.RecordHead(nil), channel.VerifiedRosterHeads...), head)
	member := channel.Members[len(channel.Members)-1]
	member.Head = head
	candidate.Members = append(append([]MemberAuthoritySnapshot(nil), channel.Members...), member)
	return candidate
}
