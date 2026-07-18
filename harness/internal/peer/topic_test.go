package peer

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestPublicationMessageIDUsesBoundedValidTupleAndInvalidFallback(t *testing.T) {
	t.Parallel()

	local := testAuthorityPeer(t, "message-id-local")
	remote := testAuthorityPeer(t, "message-id-remote")
	channel := testAuthorityChannel(t, "message-id-channel", model.BindingActive, local, remote)
	publication := testPeerPublication(t, channel, local, remote, "original")
	topic, _ := TopicName(channel.ChannelID)
	message := &pb.Message{From: []byte(local.libp2pID), Topic: stringPointer(topic),
		Data: publication.WireJSON().Bytes()}
	want := PublicationMessageID(message)
	if want == "" || PublicationMessageID(message) != want {
		t.Fatal("message ID is empty or unstable")
	}
	retry := *message
	retry.Seqno = []byte("transport-retry-sequence")
	if PublicationMessageID(&retry) != want {
		t.Fatal("transport seqno changed a stable original-author message ID")
	}
	wrongTopic := *message
	wrongTopic.Topic = stringPointer(topic + "-wrong")
	if PublicationMessageID(&wrongTopic) == want {
		t.Fatal("message ID did not bind the exact topic")
	}
	reauthored := *message
	reauthored.From = []byte(remote.libp2pID)
	if PublicationMessageID(&reauthored) == want {
		t.Fatal("message ID allowed a re-authored copy to poison the original author's seen entry")
	}
	resigned := *message
	resigned.Data = tamperPublicationSignature(t, publication).WireJSON().Bytes()
	if PublicationMessageID(&resigned) != want {
		t.Fatal("structurally valid publication ID departed from the frozen header tuple")
	}
	changed := *message
	changed.Data = append([]byte(nil), message.Data...)
	changed.Data[len(changed.Data)-2] ^= 1
	if PublicationMessageID(&changed) == want {
		t.Fatal("message ID did not bind the exact publication bytes")
	}
	malformed := &pb.Message{From: []byte(local.libp2pID), Topic: stringPointer(topic),
		Data: []byte(`{"not":"a publication"}`)}
	malformedID := PublicationMessageID(malformed)
	malformedAuthor := *malformed
	malformedAuthor.From = []byte(remote.libp2pID)
	if PublicationMessageID(&malformedAuthor) != malformedID {
		t.Fatal("malformed fallback unexpectedly trusted an unvalidated author")
	}
	malformedTopic := *malformed
	malformedTopic.Topic = stringPointer(topic + "-wrong")
	malformedRaw := *malformed
	malformedRaw.Data = []byte(`{"not":"the same publication"}`)
	if PublicationMessageID(&malformedTopic) == malformedID ||
		PublicationMessageID(&malformedRaw) == malformedID || PublicationMessageID(nil) == "" {
		t.Fatal("malformed fallback did not bind topic and raw bytes")
	}
}

func TestAuthoritySubscriptionFilterIsChannelScopedAndBounded(t *testing.T) {
	t.Parallel()

	local := testAuthorityPeer(t, "filter-local")
	remote := testAuthorityPeer(t, "filter-remote")
	authority, _ := NewAuthority(local.modelID)
	active := testAuthorityChannel(t, "filter-active", model.BindingActive, local, remote)
	pending := testAuthorityChannel(t, "filter-pending", model.BindingPending, local, remote)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{active, pending}}); err != nil {
		t.Fatal(err)
	}
	activeTopic, _ := TopicName(active.ChannelID)
	pendingTopic, _ := TopicName(pending.ChannelID)
	filter := authoritySubscriptionFilter{authority: authority}
	subscribe, unsubscribe := true, false
	subscriptions := []*pb.RPC_SubOpts{
		{Subscribe: &subscribe, Topicid: &activeTopic},
		{Subscribe: &subscribe, Topicid: &pendingTopic},
		{Subscribe: &unsubscribe, Topicid: &pendingTopic},
	}
	accepted, err := filter.FilterIncomingSubscriptions(remote.libp2pID, subscriptions)
	if err != nil || len(accepted) != 2 {
		t.Fatalf("FilterIncomingSubscriptions() = (%v, %v)", accepted, err)
	}
	for _, item := range accepted {
		if item.GetSubscribe() && item.GetTopicid() != activeTopic {
			t.Fatalf("unauthorized subscription survived: %v", item)
		}
	}
	tooMany := make([]*pb.RPC_SubOpts, model.MaxChannelsPerNode+1)
	if _, err := filter.FilterIncomingSubscriptions(remote.libp2pID, tooMany); !errors.Is(err, pubsub.ErrTooManySubscriptions) {
		t.Fatalf("oversized subscription batch error = %v", err)
	}
}

func TestAuthorityRPCInspectorRejectsCrossChannelBeforePublicationParsing(t *testing.T) {
	t.Parallel()

	local := testAuthorityPeer(t, "rpc-inspector-local")
	remote := testAuthorityPeer(t, "rpc-inspector-remote")
	alphaPeer := testAuthorityPeer(t, "rpc-inspector-alpha-peer")
	beta := testAuthorityChannel(t, "rpc-inspector-beta", model.BindingActive, local, remote)
	alpha := testAuthorityChannel(t, "rpc-inspector-alpha", model.BindingActive, local, alphaPeer)
	authority, _ := NewAuthority(local.modelID)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{alpha, beta}}); err != nil {
		t.Fatal(err)
	}
	alphaTopic, _ := TopicName(alpha.ChannelID)
	betaTopic, _ := TopicName(beta.ChannelID)
	malformed := []byte("deliberately not a publication")
	inspector := authorityRPCInspector(authority)
	if err := inspector(remote.libp2pID, &pubsub.RPC{RPC: pb.RPC{Publish: []*pb.Message{
		{Topic: &betaTopic, Data: malformed},
	}}}); err != nil {
		t.Fatalf("authorized exact-topic malformed payload was parsed too early: %v", err)
	}
	if err := inspector(remote.libp2pID, &pubsub.RPC{RPC: pb.RPC{Publish: []*pb.Message{
		{Topic: &alphaTopic, Data: malformed},
	}}}); !errors.Is(err, ErrGossipTopic) {
		t.Fatalf("cross-Channel RPC inspection error = %v", err)
	}
	if err := inspector(remote.libp2pID, &pubsub.RPC{RPC: pb.RPC{Publish: []*pb.Message{
		{Topic: &betaTopic, Data: malformed}, {Topic: &alphaTopic, Data: malformed},
	}}}); !errors.Is(err, ErrGossipTopic) {
		t.Fatalf("mixed authorized/cross-Channel RPC inspection error = %v", err)
	}
	if err := inspector("", &pubsub.RPC{}); !errors.Is(err, ErrGossipTopic) {
		t.Fatalf("unauthenticated RPC inspection error = %v", err)
	}
}

func TestGossipNegotiatesNoProtocolFallback(t *testing.T) {
	tests := []struct {
		name      string
		id        string
		protocols []protocol.ID
		flood     bool
	}{
		{name: "v1.3 only", id: "v13", protocols: []protocol.ID{pubsub.GossipSubID_v13}},
		{name: "v1.1 only", id: "v11", protocols: []protocol.ID{pubsub.GossipSubID_v11}},
		{name: "FloodSub only", id: "flood", flood: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			local := testAuthorityPeer(t, "protocol-local-"+test.id)
			remote := testAuthorityPeer(t, "protocol-remote-"+test.id)
			channel := testAuthorityChannel(t, "protocol-channel-"+test.id,
				model.BindingActive, local, remote)
			authority, _ := NewAuthority(local.modelID)
			if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
				Channels: []ChannelAuthoritySnapshot{channel}}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			localHost, err := libp2p.New(libp2p.Identity(local.libp2pPrivate),
				libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
			if err != nil {
				t.Fatal(err)
			}
			defer localHost.Close()
			remoteHost, err := libp2p.New(libp2p.Identity(remote.libp2pPrivate),
				libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
			if err != nil {
				t.Fatal(err)
			}
			defer remoteHost.Close()
			gossip, err := NewGossip(ctx, localHost, authority)
			if err != nil {
				t.Fatal(err)
			}
			defer gossip.Close()
			session, err := gossip.Join(channel.ChannelID)
			if err != nil {
				t.Fatal(err)
			}
			var remoteRouter *pubsub.PubSub
			if test.flood {
				remoteRouter, err = pubsub.NewFloodSub(ctx, remoteHost)
			} else {
				remoteRouter, err = pubsub.NewGossipSub(ctx, remoteHost,
					pubsub.WithGossipSubProtocols(test.protocols, pubsub.GossipSubDefaultFeatures))
			}
			if err != nil {
				t.Fatal(err)
			}
			topic, err := remoteRouter.Join(session.Name())
			if err != nil {
				t.Fatal(err)
			}
			defer topic.Close()
			subscription, err := topic.Subscribe()
			if err != nil {
				t.Fatal(err)
			}
			defer subscription.Cancel()
			if err := localHost.Connect(ctx, peer.AddrInfo{ID: remoteHost.ID(), Addrs: remoteHost.Addrs()}); err != nil {
				t.Fatal(err)
			}
			time.Sleep(300 * time.Millisecond)
			assertNoPubSubProtocol(t, localHost, remoteHost.ID())
			if len(session.Peers()) != 0 {
				t.Fatal("incompatible PubSub-only Peer entered the R5 topic")
			}
		})
	}
}

func TestGossipValidatorChecksRelayAuthorScopeRosterAndSignature(t *testing.T) {
	t.Parallel()

	local := testAuthorityPeer(t, "validator-local")
	author := testAuthorityPeer(t, "validator-author")
	relay := testAuthorityPeer(t, "validator-relay")
	channel := testThreePeerAuthorityChannel(t, "validator-channel", local, author, relay)
	authority, _ := NewAuthority(local.modelID)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{channel}}); err != nil {
		t.Fatal(err)
	}
	gossip := &Gossip{authority: authority}
	topic, _ := TopicName(channel.ChannelID)
	publication := testPeerPublication(t, channel, author, local, "valid")
	message := testPubSubMessage(topic, publication, author.libp2pID, relay.libp2pID)
	gate := &channelGate{}
	gate.deliverable.Store(true)
	session := &TopicSession{gossip: gossip, channelID: channel.ChannelID, name: topic, gate: gate}
	validator := gossip.validator(session)
	if result := validator(context.Background(), relay.libp2pID, message); result != pubsub.ValidationAccept {
		t.Fatalf("valid relayed publication result = %v", result)
	}
	if validated, ok := message.ValidatorData.(model.SignedPublication); !ok ||
		validated.Digest() != publication.Digest() {
		t.Fatal("validator did not attach the parsed publication")
	}

	nonmember := testAuthorityPeer(t, "validator-nonmember")
	tests := map[string]struct {
		transport peer.ID
		message   *pubsub.Message
	}{
		"nonmember relay": {nonmember.libp2pID,
			testPubSubMessage(topic, publication, author.libp2pID, nonmember.libp2pID)},
		"wrong original author": {relay.libp2pID,
			testPubSubMessage(topic, publication, relay.libp2pID, relay.libp2pID)},
		"wrong topic": {relay.libp2pID,
			testPubSubMessage(topic+"-wrong", publication, author.libp2pID, relay.libp2pID)},
		"bad origin signature": {relay.libp2pID,
			testPubSubMessage(topic, tamperPublicationSignature(t, publication), author.libp2pID, relay.libp2pID)},
	}
	for name, test := range tests {
		name, test := name, test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if result := validator(context.Background(), test.transport, test.message); result != pubsub.ValidationReject {
				t.Fatalf("result = %v, want reject", result)
			}
		})
	}
}

func TestGossipTopicsDeliverOnceAndPreserveConflictChallenger(t *testing.T) {
	local := testAuthorityPeer(t, "gossip-a")
	remote := testAuthorityPeer(t, "gossip-b")
	channelA := testAuthorityChannel(t, "gossip-shared", model.BindingActive, local, remote)
	channelB := channelA
	channelB.Bindings = []BindingAuthoritySnapshot{{PeerID: local.modelID, State: model.BindingActive}}
	authorityA, _ := NewAuthority(local.modelID)
	authorityB, _ := NewAuthority(remote.modelID)
	if err := authorityA.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{channelA}}); err != nil {
		t.Fatal(err)
	}
	if err := authorityB.Replace(NetworkAuthoritySnapshot{LocalPeerID: remote.modelID,
		Channels: []ChannelAuthoritySnapshot{channelB}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hostA, err := libp2p.New(libp2p.Identity(local.libp2pPrivate),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer hostA.Close()
	hostB, err := libp2p.New(libp2p.Identity(remote.libp2pPrivate),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer hostB.Close()
	gossipA, err := NewGossip(ctx, hostA, authorityA)
	if err != nil {
		t.Fatal(err)
	}
	defer gossipA.Close()
	gossipB, err := NewGossip(ctx, hostB, authorityB)
	if err != nil {
		t.Fatal(err)
	}
	defer gossipB.Close()
	sessionA, err := gossipA.Join(channelA.ChannelID)
	if err != nil {
		t.Fatal(err)
	}
	sessionB, err := gossipB.Join(channelB.ChannelID)
	if err != nil {
		t.Fatal(err)
	}
	if err := hostA.Connect(ctx, peer.AddrInfo{ID: hostB.ID(), Addrs: hostB.Addrs()}); err != nil {
		t.Fatal(err)
	}
	waitTopicPeer(t, sessionA, hostB.ID())
	waitTopicPeer(t, sessionB, hostA.ID())
	// ListPeers proves the signed subscription exchange; the first GossipSub
	// heartbeat installs the mesh edge shortly afterwards.
	time.Sleep(300 * time.Millisecond)

	original := testPeerPublication(t, channelA, local, remote, "original")
	if err := sessionA.Publish(ctx, original); err != nil {
		t.Fatal(err)
	}
	received := nextTopicPublication(t, sessionB, 5*time.Second)
	if received.Publication.Digest() != original.Digest() || received.OriginalAuthor != hostA.ID() {
		t.Fatalf("received publication = %#v", received)
	}
	if err := sessionA.Publish(ctx, original); err != nil {
		t.Fatal(err)
	}
	duplicateContext, duplicateCancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer duplicateCancel()
	if _, err := sessionB.Next(duplicateContext); err == nil {
		t.Fatal("same exact publication was delivered twice")
	}

	challenger := testPeerPublication(t, channelA, local, remote, "challenger")
	if challenger.Key() != original.Key() || challenger.Digest() == original.Digest() {
		t.Fatal("test challenger does not exercise same-key digest conflict")
	}
	if err := sessionA.Publish(ctx, challenger); err != nil {
		t.Fatal(err)
	}
	received = nextTopicPublication(t, sessionB, 5*time.Second)
	if received.Publication.Digest() != challenger.Digest() {
		t.Fatal("changed publication bytes were hidden by Gossip dedupe")
	}

	rotationHead, _ := model.NewRecordHead(3, model.Sum([]byte("gossip-b-roster-3")))
	rotatedAuthorityB := channelB
	rotatedAuthorityB.RosterHead = rotationHead
	rotatedAuthorityB.VerifiedRosterHeads = append(
		append([]model.RecordHead(nil), channelB.VerifiedRosterHeads...), rotationHead)
	rotatedAuthorRecord := channelB.Members[0]
	rotatedAuthorRecord.Head = rotationHead
	rotatedAuthorityB.Members = append(
		append([]MemberAuthoritySnapshot(nil), channelB.Members...), rotatedAuthorRecord)
	boundary := testPeerPublication(t, channelA, local, remote, "rotation-boundary")
	sessionB.gate.RLock()
	reconciled := make(chan error, 1)
	go func() {
		reconciled <- gossipB.Reconcile(NetworkAuthoritySnapshot{LocalPeerID: remote.modelID,
			Channels: []ChannelAuthoritySnapshot{rotatedAuthorityB}})
	}()
	writerDeadline := time.Now().Add(2 * time.Second)
	writerPending := false
	for time.Now().Before(writerDeadline) {
		if !sessionB.gate.TryRLock() {
			writerPending = true
			break
		}
		sessionB.gate.RUnlock()
		runtime.Gosched()
	}
	boundaryPublishErr := sessionA.Publish(ctx, boundary)
	time.Sleep(300 * time.Millisecond)
	sessionB.gate.RUnlock()
	reconcileErr := <-reconciled
	if !writerPending || boundaryPublishErr != nil || reconcileErr != nil {
		t.Fatalf("rotation boundary = pending %v, publish %v, reconcile %v",
			writerPending, boundaryPublishErr, reconcileErr)
	}
	sessionB, err = gossipB.Join(channelB.ChannelID)
	if err != nil {
		t.Fatal(err)
	}
	received = nextTopicPublication(t, sessionB, 5*time.Second)
	if received.Publication.Digest() != boundary.Digest() {
		t.Fatal("old validator handoff lost a seen-cache-marked publication")
	}
	if err := sessionB.Close(); err != nil {
		t.Fatal(err)
	}
	if sessionB.Peers() != nil {
		t.Fatal("closed Channel session still exposed topic peers")
	}
}

func TestNewGossipRejectsDuplicateWithoutReplacingStreamHandler(t *testing.T) {
	local := testAuthorityPeer(t, "gossip-single-runtime-a")
	remote := testAuthorityPeer(t, "gossip-single-runtime-b")
	channelA := testAuthorityChannel(t, "gossip-single-runtime", model.BindingActive, local, remote)
	channelB := channelA
	channelB.Bindings = []BindingAuthoritySnapshot{{PeerID: local.modelID, State: model.BindingActive}}
	authorityA, _ := NewAuthority(local.modelID)
	authorityB, _ := NewAuthority(remote.modelID)
	if err := authorityA.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{channelA}}); err != nil {
		t.Fatal(err)
	}
	if err := authorityB.Replace(NetworkAuthoritySnapshot{LocalPeerID: remote.modelID,
		Channels: []ChannelAuthoritySnapshot{channelB}}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hostA, err := libp2p.New(libp2p.Identity(local.libp2pPrivate),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer hostA.Close()
	hostB, err := libp2p.New(libp2p.Identity(remote.libp2pPrivate),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer hostB.Close()
	gossipA, err := NewGossip(ctx, hostA, authorityA)
	if err != nil {
		t.Fatal(err)
	}
	defer gossipA.Close()
	gossipB, err := NewGossip(ctx, hostB, authorityB)
	if err != nil {
		t.Fatal(err)
	}
	defer gossipB.Close()
	sessionA, err := gossipA.Join(channelA.ChannelID)
	if err != nil {
		t.Fatal(err)
	}
	sessionB, err := gossipB.Join(channelB.ChannelID)
	if err != nil {
		t.Fatal(err)
	}
	if err := hostA.Connect(ctx, peer.AddrInfo{ID: hostB.ID(), Addrs: hostB.Addrs()}); err != nil {
		t.Fatal(err)
	}
	waitTopicPeer(t, sessionA, hostB.ID())
	waitTopicPeer(t, sessionB, hostA.ID())

	duplicate, duplicateErr := NewGossip(ctx, hostA, authorityA)
	if duplicate != nil || !errors.Is(duplicateErr, ErrGossipTopic) {
		t.Fatalf("duplicate NewGossip() = (%p, %v)", duplicate, duplicateErr)
	}
	distinctAuthority, _ := NewAuthority(local.modelID)
	if err := distinctAuthority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{channelA}}); err != nil {
		t.Fatal(err)
	}
	distinctDuplicate, distinctErr := NewGossip(ctx, hostA, distinctAuthority)
	if distinctDuplicate != nil || !errors.Is(distinctErr, ErrGossipTopic) {
		t.Fatalf("same-Host distinct-Authority NewGossip() = (%p, %v)",
			distinctDuplicate, distinctErr)
	}
	if err := hostA.Network().ClosePeer(hostB.ID()); err != nil {
		t.Fatal(err)
	}
	waitPeerDisconnected(t, hostA, hostB.ID())
	waitTopicPeerAbsent(t, sessionA, hostB.ID())
	waitTopicPeerAbsent(t, sessionB, hostA.ID())
	// Dial toward A so delivery depends on the original inbound v1.2 handler
	// still being installed there after the rejected duplicate constructor.
	if err := hostB.Connect(ctx, peer.AddrInfo{ID: hostA.ID(), Addrs: hostA.Addrs()}); err != nil {
		t.Fatal(err)
	}
	assertExactGossipSubV12(t, hostA, hostB.ID())
	assertExactGossipSubV12(t, hostB, hostA.ID())
	waitTopicPeer(t, sessionA, hostB.ID())
	waitTopicPeer(t, sessionB, hostA.ID())
	time.Sleep(300 * time.Millisecond)

	publication := testPeerPublication(t, channelB, remote, local, "after-duplicate-reconnect")
	if err := sessionB.Publish(ctx, publication); err != nil {
		t.Fatal(err)
	}
	localCopy := nextTopicPublication(t, sessionB, 5*time.Second)
	remoteCopy := nextTopicPublication(t, sessionA, 5*time.Second)
	if localCopy.Publication.Digest() != publication.Digest() ||
		remoteCopy.Publication.Digest() != publication.Digest() ||
		remoteCopy.OriginalAuthor != hostB.ID() {
		t.Fatal("live Gossip router stopped delivering after a rejected duplicate constructor")
	}
}

func TestGossipDataPlaneStreamRejectsPendingAndRevokedPeers(t *testing.T) {
	for _, binding := range []model.BindingState{model.BindingPending, model.BindingRevoked} {
		binding := binding
		label := string(binding)
		t.Run(label, func(t *testing.T) {
			local := testAuthorityPeer(t, "gossip-stream-local-"+label)
			remote := testAuthorityPeer(t, "gossip-stream-remote-"+label)
			channel := testAuthorityChannel(t, "gossip-stream-"+label, binding, local, remote)
			authority, _ := NewAuthority(local.modelID)
			if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
				Channels: []ChannelAuthoritySnapshot{channel}}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			localHost, err := libp2p.New(libp2p.Identity(local.libp2pPrivate),
				libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
			if err != nil {
				t.Fatal(err)
			}
			defer localHost.Close()
			remoteHost, err := libp2p.New(libp2p.Identity(remote.libp2pPrivate),
				libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
			if err != nil {
				t.Fatal(err)
			}
			defer remoteHost.Close()
			outboundOpened := make(chan struct{}, 1)
			remoteHost.SetStreamHandler(GossipProtocol, func(stream network.Stream) {
				select {
				case outboundOpened <- struct{}{}:
				default:
				}
				_ = stream.Reset()
			})
			gossip, err := NewGossip(ctx, localHost, authority)
			if err != nil {
				t.Fatal(err)
			}
			defer gossip.Close()
			session, err := gossip.Join(channel.ChannelID)
			if err != nil {
				t.Fatal(err)
			}
			if err := remoteHost.Connect(ctx, peer.AddrInfo{ID: localHost.ID(),
				Addrs: localHost.Addrs()}); err != nil {
				t.Fatal(err)
			}
			time.Sleep(300 * time.Millisecond)
			select {
			case <-outboundOpened:
				t.Fatal("pending/revoked Peer received an outbound Gossip data-plane stream")
			default:
			}
			stream, streamErr := remoteHost.NewStream(ctx, localHost.ID(), GossipProtocol)
			if streamErr == nil {
				_ = stream.SetDeadline(time.Now().Add(time.Second))
				buffer := make([]byte, 1)
				if _, readErr := stream.Read(buffer); readErr == nil {
					t.Fatal("pending/revoked inbound Gossip stream was not reset")
				}
				_ = stream.Close()
			}
			for _, peerID := range session.Peers() {
				if peerID == remoteHost.ID() {
					t.Fatal("pending/revoked Peer entered the Gossip topic")
				}
			}
		})
	}
}

func TestGossipPendingToActiveRefreshesExistingPhysicalConnection(t *testing.T) {
	local := testAuthorityPeer(t, "gossip-promotion-a")
	remote := testAuthorityPeer(t, "gossip-promotion-b")
	pendingA := testAuthorityChannel(t, "gossip-promotion", model.BindingPending, local, remote)
	pendingB := pendingA
	pendingB.Bindings = []BindingAuthoritySnapshot{{PeerID: local.modelID, State: model.BindingPending}}
	authorityA, _ := NewAuthority(local.modelID)
	authorityB, _ := NewAuthority(remote.modelID)
	if err := authorityA.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{pendingA}}); err != nil {
		t.Fatal(err)
	}
	if err := authorityB.Replace(NetworkAuthoritySnapshot{LocalPeerID: remote.modelID,
		Channels: []ChannelAuthoritySnapshot{pendingB}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hostA, err := libp2p.New(libp2p.Identity(local.libp2pPrivate),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer hostA.Close()
	hostB, err := libp2p.New(libp2p.Identity(remote.libp2pPrivate),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer hostB.Close()
	refreshHostA := &failOnceConnectHost{Host: hostA, failed: make(chan struct{}, 1)}
	gossipA, err := NewGossip(ctx, refreshHostA, authorityA)
	if err != nil {
		t.Fatal(err)
	}
	defer gossipA.Close()
	gossipB, err := NewGossip(ctx, hostB, authorityB)
	if err != nil {
		t.Fatal(err)
	}
	defer gossipB.Close()
	if _, err := gossipA.Join(pendingA.ChannelID); err != nil {
		t.Fatal(err)
	}
	if _, err := gossipB.Join(pendingB.ChannelID); err != nil {
		t.Fatal(err)
	}
	if err := hostA.Connect(ctx, peer.AddrInfo{ID: hostB.ID(), Addrs: hostB.Addrs()}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	assertNoPubSubProtocol(t, hostA, hostB.ID())
	if hostA.Network().Connectedness(hostB.ID()) != network.Connected {
		t.Fatal("pending binding did not retain its physical enrollment connection")
	}

	activeA := pendingA
	activeA.Bindings = []BindingAuthoritySnapshot{{PeerID: remote.modelID, State: model.BindingActive}}
	activeB := pendingB
	activeB.Bindings = []BindingAuthoritySnapshot{{PeerID: local.modelID, State: model.BindingActive}}
	injectedConnectErr := errors.New("injected promoted Peer reconnect failure")
	refreshHostA.setFailure(injectedConnectErr)
	activeSnapshotA := NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{activeA}}
	if err := gossipA.Reconcile(activeSnapshotA); err != nil {
		t.Fatal(err)
	}
	// Reachability does not fail the committed authority transition. The
	// background refresh retains its intent and retries without replaying the
	// authority snapshot.
	select {
	case <-refreshHostA.failed:
	case <-time.After(2 * time.Second):
		t.Fatal("background promoted-Peer refresh did not exercise the injected failure")
	}
	if err := gossipB.Reconcile(NetworkAuthoritySnapshot{LocalPeerID: remote.modelID,
		Channels: []ChannelAuthoritySnapshot{activeB}}); err != nil {
		t.Fatal(err)
	}
	sessionA, err := gossipA.Join(activeA.ChannelID)
	if err != nil {
		t.Fatal(err)
	}
	sessionB, err := gossipB.Join(activeB.ChannelID)
	if err != nil {
		t.Fatal(err)
	}
	waitTopicPeer(t, sessionA, hostB.ID())
	waitTopicPeer(t, sessionB, hostA.ID())
	assertExactGossipSubV12(t, hostA, hostB.ID())
	time.Sleep(1200 * time.Millisecond)
	publication := testPeerPublication(t, activeA, local, remote, "after-pending-promotion")
	if err := sessionA.Publish(ctx, publication); err != nil {
		t.Fatal(err)
	}
	received := nextTopicPublication(t, sessionB, 5*time.Second)
	if received.Publication.Digest() != publication.Digest() {
		t.Fatal("promoted physical connection never entered the Gossip data plane")
	}
}

func TestGossipRefreshCompletionCannotOverwriteRepromotedGeneration(t *testing.T) {
	t.Parallel()

	remote := testAuthorityPeer(t, "gossip-refresh-aba")
	oldGeneration := &peerRefresh{running: true, attempt: 7}
	newGeneration := &peerRefresh{running: true, attempt: 2,
		info: peer.AddrInfo{ID: remote.libp2pID}}
	gossip := &Gossip{refresh: map[peer.ID]*peerRefresh{remote.libp2pID: newGeneration}}
	gossip.finishRefresh(remote.libp2pID, peer.AddrInfo{ID: remote.libp2pID}, oldGeneration)
	if gossip.refresh[remote.libp2pID] != newGeneration || !newGeneration.running ||
		newGeneration.attempt != 2 {
		t.Fatal("an obsolete refresh attempt mutated the re-promoted Peer generation")
	}
}

func TestGossipDataPlaneHealthRequiresLocalOutboundWriter(t *testing.T) {
	local := testAuthorityPeer(t, "gossip-health-local")
	remote := testAuthorityPeer(t, "gossip-health-remote")
	localHost := newBarePeerHost(t, local)
	defer localHost.Close()
	remoteHost := newBarePeerHost(t, remote)
	defer remoteHost.Close()
	localAccepted := make(chan struct{}, 1)
	remoteAccepted := make(chan struct{}, 1)
	releaseLocal := make(chan struct{})
	releaseRemote := make(chan struct{})
	localHost.SetStreamHandler(GossipProtocol, func(stream network.Stream) {
		localAccepted <- struct{}{}
		<-releaseLocal
		_ = stream.Close()
	})
	remoteHost.SetStreamHandler(GossipProtocol, func(stream network.Stream) {
		remoteAccepted <- struct{}{}
		<-releaseRemote
		_ = stream.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := remoteHost.Connect(ctx, peer.AddrInfo{ID: localHost.ID(), Addrs: localHost.Addrs()}); err != nil {
		t.Fatal(err)
	}
	inbound, err := remoteHost.NewStream(ctx, localHost.ID(), GossipProtocol)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := inbound.Write([]byte{'i'}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-localAccepted:
	case <-time.After(2 * time.Second):
		t.Fatal("inbound Gossip stream was not established")
	}
	gossip := &Gossip{host: localHost}
	if gossip.hasHealthyDataPlaneStream(remoteHost.ID()) {
		t.Fatal("remote-to-local inbound Gossip stream was mistaken for the local outbound writer")
	}
	outbound, err := localHost.NewStream(ctx, remoteHost.ID(), GossipProtocol)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := outbound.Write([]byte{'o'}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-remoteAccepted:
	case <-time.After(2 * time.Second):
		t.Fatal("outbound Gossip stream was not established")
	}
	if !gossip.hasHealthyDataPlaneStream(remoteHost.ID()) {
		t.Fatal("local outbound Gossip writer was not recognized as healthy")
	}
	_ = inbound.Reset()
	_ = outbound.Reset()
	close(releaseLocal)
	close(releaseRemote)
}

func TestGossipConcurrentCloseWaitsForRefreshQuiescence(t *testing.T) {
	local := testAuthorityPeer(t, "gossip-close-refresh-local")
	remote := testAuthorityPeer(t, "gossip-close-refresh-remote")
	pending := testAuthorityChannel(t, "gossip-close-refresh", model.BindingPending, local, remote)
	authority, _ := NewAuthority(local.modelID)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{pending}}); err != nil {
		t.Fatal(err)
	}
	localHost := newBarePeerHost(t, local)
	defer localHost.Close()
	remoteHost := newBarePeerHost(t, remote)
	defer remoteHost.Close()
	blocking := &cancelBlockingConnectHost{Host: localHost, entered: make(chan struct{}),
		exited: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gossip, err := NewGossip(ctx, blocking, authority)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gossip.Join(pending.ChannelID); err != nil {
		t.Fatal(err)
	}
	if err := localHost.Connect(ctx, peer.AddrInfo{ID: remoteHost.ID(), Addrs: remoteHost.Addrs()}); err != nil {
		t.Fatal(err)
	}
	active := pending
	active.Bindings = []BindingAuthoritySnapshot{{PeerID: remote.modelID, State: model.BindingActive}}
	if err := gossip.Reconcile(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{active}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-blocking.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh worker did not enter the controlled dial")
	}
	closed := make(chan error, 2)
	go func() { closed <- gossip.Close() }()
	go func() { closed <- gossip.Close() }()
	for iteration := 0; iteration < 2; iteration++ {
		select {
		case err := <-closed:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("concurrent Gossip Close did not reach refresh quiescence")
		}
	}
	select {
	case <-blocking.exited:
	default:
		t.Fatal("Gossip Close returned before the refresh attempt exited")
	}
}

type failOnceConnectHost struct {
	host.Host
	mu     sync.Mutex
	fail   error
	failed chan struct{}
}

type cancelBlockingConnectHost struct {
	host.Host
	entered chan struct{}
	exited  chan struct{}
	once    sync.Once
}

func (nodeHost *cancelBlockingConnectHost) Connect(ctx context.Context, _ peer.AddrInfo) error {
	nodeHost.once.Do(func() { close(nodeHost.entered) })
	<-ctx.Done()
	select {
	case <-nodeHost.exited:
	default:
		close(nodeHost.exited)
	}
	return ctx.Err()
}

func (nodeHost *failOnceConnectHost) setFailure(err error) {
	nodeHost.mu.Lock()
	nodeHost.fail = err
	nodeHost.mu.Unlock()
}

func (nodeHost *failOnceConnectHost) Connect(ctx context.Context, info peer.AddrInfo) error {
	nodeHost.mu.Lock()
	if nodeHost.fail != nil {
		err := nodeHost.fail
		nodeHost.fail = nil
		nodeHost.mu.Unlock()
		select {
		case nodeHost.failed <- struct{}{}:
		default:
		}
		return err
	}
	nodeHost.mu.Unlock()
	return nodeHost.Host.Connect(ctx, info)
}

func TestGossipReconcileRevokesOneChannelAndPreservesOverlap(t *testing.T) {
	local := testAuthorityPeer(t, "gossip-overlap-a")
	remote := testAuthorityPeer(t, "gossip-overlap-c")
	alphaA := testAuthorityChannel(t, "gossip-overlap-alpha", model.BindingActive, local, remote)
	betaA := testAuthorityChannel(t, "gossip-overlap-beta", model.BindingActive, local, remote)
	alphaC := alphaA
	alphaC.Bindings = []BindingAuthoritySnapshot{{PeerID: local.modelID, State: model.BindingActive}}
	betaC := betaA
	betaC.Bindings = []BindingAuthoritySnapshot{{PeerID: local.modelID, State: model.BindingActive}}
	authorityA, _ := NewAuthority(local.modelID)
	authorityC, _ := NewAuthority(remote.modelID)
	if err := authorityA.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{alphaA, betaA}}); err != nil {
		t.Fatal(err)
	}
	if err := authorityC.Replace(NetworkAuthoritySnapshot{LocalPeerID: remote.modelID,
		Channels: []ChannelAuthoritySnapshot{alphaC, betaC}}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hostA, err := libp2p.New(libp2p.Identity(local.libp2pPrivate),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer hostA.Close()
	hostC, err := libp2p.New(libp2p.Identity(remote.libp2pPrivate),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer hostC.Close()
	gossipA, err := NewGossip(ctx, hostA, authorityA)
	if err != nil {
		t.Fatal(err)
	}
	defer gossipA.Close()
	gossipC, err := NewGossip(ctx, hostC, authorityC)
	if err != nil {
		t.Fatal(err)
	}
	defer gossipC.Close()
	alphaSessionA, err := gossipA.Join(alphaA.ChannelID)
	if err != nil {
		t.Fatal(err)
	}
	betaSessionA, err := gossipA.Join(betaA.ChannelID)
	if err != nil {
		t.Fatal(err)
	}
	alphaSessionC, err := gossipC.Join(alphaC.ChannelID)
	if err != nil {
		t.Fatal(err)
	}
	betaSessionC, err := gossipC.Join(betaC.ChannelID)
	if err != nil {
		t.Fatal(err)
	}
	if err := hostA.Connect(ctx, peer.AddrInfo{ID: hostC.ID(), Addrs: hostC.Addrs()}); err != nil {
		t.Fatal(err)
	}
	waitTopicPeer(t, alphaSessionA, hostC.ID())
	waitTopicPeer(t, betaSessionA, hostC.ID())
	waitTopicPeer(t, alphaSessionC, hostA.ID())
	waitTopicPeer(t, betaSessionC, hostA.ID())
	assertExactGossipSubV12(t, hostA, hostC.ID())
	assertExactGossipSubV12(t, hostC, hostA.ID())
	time.Sleep(300 * time.Millisecond)

	revokedAlpha := alphaA
	revokedAlpha.Bindings = []BindingAuthoritySnapshot{{PeerID: remote.modelID, State: model.BindingRevoked}}
	betaDuringRotation := testPeerPublication(t, betaA, local, remote, "beta-during-alpha-rotation")
	alphaSessionA.gate.RLock()
	reconciled := make(chan error, 1)
	go func() {
		reconciled <- gossipA.Reconcile(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
			Channels: []ChannelAuthoritySnapshot{revokedAlpha, betaA}})
	}()
	writerDeadline := time.Now().Add(2 * time.Second)
	writerPending := false
	for time.Now().Before(writerDeadline) {
		if !alphaSessionA.gate.TryRLock() {
			writerPending = true
			break
		}
		alphaSessionA.gate.RUnlock()
		runtime.Gosched()
	}
	betaPublishErr := betaSessionA.Publish(ctx, betaDuringRotation)
	betaContext, betaCancel := context.WithTimeout(ctx, 2*time.Second)
	betaReceived, betaReceiveErr := betaSessionC.Next(betaContext)
	betaCancel()
	alphaSessionA.gate.RUnlock()
	reconcileErr := <-reconciled
	if !writerPending {
		t.Fatal("Alpha reconciliation did not reach its Channel write gate")
	}
	if betaPublishErr != nil || betaReceiveErr != nil ||
		betaReceived.Publication.Digest() != betaDuringRotation.Digest() {
		t.Fatalf("unaffected Beta stopped during Alpha rotation: publish=%v receive=%v", betaPublishErr, betaReceiveErr)
	}
	if reconcileErr != nil {
		t.Fatal(reconcileErr)
	}
	if !alphaSessionA.closed.Load() || betaSessionA.closed.Load() {
		t.Fatal("authority reconciliation did not isolate rotation to the affected Channel")
	}
	alphaSessionA, err = gossipA.Join(alphaA.ChannelID)
	if err != nil {
		t.Fatal(err)
	}
	unchangedBeta, err := gossipA.Join(betaA.ChannelID)
	if err != nil || unchangedBeta != betaSessionA {
		t.Fatalf("unaffected Beta session changed: %p/%p, %v", unchangedBeta, betaSessionA, err)
	}
	waitTopicPeer(t, betaSessionA, hostC.ID())
	waitTopicPeer(t, betaSessionC, hostA.ID())
	if hostA.Network().Connectedness(hostC.ID()) != network.Connected {
		t.Fatal("scoped Channel revoke dropped the overlapping physical connection")
	}
	for _, peerID := range alphaSessionA.Peers() {
		if peerID == hostC.ID() {
			t.Fatal("revoked Peer remained visible in the rotated Alpha topic")
		}
	}
	// Allow a full heartbeat after rejoin so the assertion exercises the
	// steady-state mesh rather than only the subscription transition.
	time.Sleep(1100 * time.Millisecond)
	alphaPublication := testPeerPublication(t, revokedAlpha, local, remote, "revoked-alpha")
	if err := alphaSessionA.Publish(ctx, alphaPublication); err != nil {
		t.Fatal(err)
	}
	alphaContext, alphaCancel := context.WithTimeout(ctx, 1200*time.Millisecond)
	defer alphaCancel()
	if received, err := alphaSessionC.Next(alphaContext); err == nil {
		t.Fatalf("revoked Alpha Peer received publication %s", received.Publication.Digest().String())
	}

	betaPublication := testPeerPublication(t, betaA, local, remote, "active-beta")
	if err := betaSessionA.Publish(ctx, betaPublication); err != nil {
		t.Fatal(err)
	}
	received := nextTopicPublication(t, betaSessionC, 5*time.Second)
	if received.Publication.Digest() != betaPublication.Digest() {
		t.Fatal("overlapping Beta Channel stopped after scoped Alpha revoke")
	}
}

func TestGossipSessionCloseSerializesValidatorRecreation(t *testing.T) {
	local := testAuthorityPeer(t, "gossip-lifecycle-local")
	remote := testAuthorityPeer(t, "gossip-lifecycle-remote")
	channel := testAuthorityChannel(t, "gossip-lifecycle-channel", model.BindingActive, local, remote)
	authority, _ := NewAuthority(local.modelID)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{channel}}); err != nil {
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
	session, err := gossip.Join(channel.ChannelID)
	if err != nil {
		t.Fatal(err)
	}
	for iteration := 0; iteration < 32; iteration++ {
		closed := make(chan error, 1)
		go func(current *TopicSession) { closed <- current.Close() }(session)
		deadline := time.Now().Add(2 * time.Second)
		for !session.closed.Load() && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if !session.closed.Load() {
			t.Fatal("session close did not begin")
		}
		replacement, joinErr := gossip.Join(channel.ChannelID)
		if closeErr := <-closed; closeErr != nil {
			t.Fatal(closeErr)
		}
		if joinErr != nil || replacement == session || replacement.closed.Load() {
			t.Fatalf("serialized Join() = (%p, %v), previous %p", replacement, joinErr, session)
		}
		session = replacement
	}

	newHead, _ := model.NewRecordHead(3, model.Sum([]byte("gossip-lifecycle-roster-3")))
	rosterChanged := channel
	rosterChanged.RosterHead = newHead
	rosterChanged.VerifiedRosterHeads = append(
		append([]model.RecordHead(nil), channel.VerifiedRosterHeads...), newHead)
	newRemoteRecord := channel.Members[1]
	newRemoteRecord.Head = newHead
	rosterChanged.Members = append(
		append([]MemberAuthoritySnapshot(nil), channel.Members...), newRemoteRecord)
	firstGeneration := session
	stableGate := session.gate
	oldValidator := gossip.validator(firstGeneration)
	publication := testPeerPublication(t, channel, local, remote, "publish-at-roster-rotation")
	session.gate.RLock()
	reconciled := make(chan error, 1)
	go func() {
		reconciled <- gossip.Reconcile(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
			Channels: []ChannelAuthoritySnapshot{rosterChanged}})
	}()
	deadline := time.Now().Add(2 * time.Second)
	writerPending := false
	for time.Now().Before(deadline) {
		if !session.gate.TryRLock() {
			writerPending = true
			break
		}
		session.gate.RUnlock()
		runtime.Gosched()
	}
	published := make(chan error, 1)
	go func() { published <- session.publishUnderGate(ctx, publication) }()
	var publishErr error
	select {
	case publishErr = <-published:
	case <-time.After(2 * time.Second):
		publishErr = errors.New("local validator recursively blocked behind the pending Channel writer")
	}
	session.gate.RUnlock()
	reconcileErr := <-reconciled
	if !writerPending || publishErr != nil || reconcileErr != nil {
		t.Fatalf("Publish/roster Reconcile = pending %v, publish %v, reconcile %v",
			writerPending, publishErr, reconcileErr)
	}
	if !session.closed.Load() {
		t.Fatal("roster-head-only authority change did not rotate the Channel session")
	}
	session, err = gossip.Join(channel.ChannelID)
	if err != nil {
		t.Fatal(err)
	}
	if session.gate != stableGate || !firstGeneration.handoff.Load() {
		t.Fatal("successful authority rotation did not preserve the Channel gate and validator handoff")
	}
	conflicted := rosterChanged
	conflicted.Status = model.ChannelConflicted
	if err := gossip.Reconcile(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{conflicted}}); err != nil {
		t.Fatal(err)
	}
	if !session.closed.Load() {
		t.Fatal("Channel status change did not close the active session")
	}
	if _, err := gossip.Join(channel.ChannelID); !errors.Is(err, ErrGossipTopic) {
		t.Fatalf("conflicted Channel Join() error = %v", err)
	}
	if stableGate.deliverable.Load() {
		t.Fatal("Channel without a current subscription remained deliverable")
	}

	// A Channel with no current session still has validator closures from
	// successful older generations. Reactivation must therefore drain its
	// lifetime gate even though there is no TopicSession to rotate. Once the
	// authority becomes active, the queued ancestor still rejects until a
	// complete successor subscription is installed.
	topicName, _ := TopicName(channel.ChannelID)
	reactivationPublication := testPeerPublication(t, channel, local, remote,
		"validator-during-sessionless-reactivation")
	reactivationMessage := testPubSubMessage(topicName, reactivationPublication,
		local.libp2pID, remote.libp2pID)
	stableGate.RLock()
	reactivated := make(chan error, 1)
	go func() {
		reactivated <- gossip.Reconcile(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
			Channels: []ChannelAuthoritySnapshot{rosterChanged}})
	}()
	deadline = time.Now().Add(2 * time.Second)
	reactivationWriterPending := false
	for time.Now().Before(deadline) {
		if !stableGate.TryRLock() {
			reactivationWriterPending = true
			break
		}
		stableGate.RUnlock()
		runtime.Gosched()
	}
	reactivationValidated := make(chan pubsub.ValidationResult, 1)
	go func() {
		reactivationValidated <- oldValidator(context.Background(), remote.libp2pID,
			reactivationMessage)
	}()
	select {
	case result := <-reactivationValidated:
		stableGate.RUnlock()
		t.Fatalf("ancestor validator bypassed the sessionless authority gate: %v", result)
	case <-time.After(100 * time.Millisecond):
	}
	stableGate.RUnlock()
	reactivateErr := <-reactivated
	reactivationResult := <-reactivationValidated
	if !reactivationWriterPending || reactivateErr != nil ||
		reactivationResult != pubsub.ValidationReject {
		t.Fatalf("sessionless reactivation = pending %v, reconcile %v, old validation %v",
			reactivationWriterPending, reactivateErr, reactivationResult)
	}
	session, err = gossip.Join(channel.ChannelID)
	if err != nil {
		t.Fatal(err)
	}
	if session.gate != stableGate {
		t.Fatal("inactive Channel reactivation replaced its lifetime validator gate")
	}

	newerHead, _ := model.NewRecordHead(4, model.Sum([]byte("gossip-lifecycle-roster-4")))
	rosterChangedAgain := rosterChanged
	rosterChangedAgain.RosterHead = newerHead
	rosterChangedAgain.VerifiedRosterHeads = append(
		append([]model.RecordHead(nil), rosterChanged.VerifiedRosterHeads...), newerHead)
	newerRemoteRecord := channel.Members[1]
	newerRemoteRecord.Head = newerHead
	rosterChangedAgain.Members = append(
		append([]MemberAuthoritySnapshot(nil), rosterChanged.Members...), newerRemoteRecord)
	delayedPublication := testPeerPublication(t, channel, local, remote, "delayed-old-validator")
	delayedMessage := testPubSubMessage(topicName, delayedPublication, local.libp2pID, remote.libp2pID)

	stableGate.RLock()
	secondReconcile := make(chan error, 1)
	go func() {
		secondReconcile <- gossip.Reconcile(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
			Channels: []ChannelAuthoritySnapshot{rosterChangedAgain}})
	}()
	deadline = time.Now().Add(2 * time.Second)
	secondWriterPending := false
	for time.Now().Before(deadline) {
		if !stableGate.TryRLock() {
			secondWriterPending = true
			break
		}
		stableGate.RUnlock()
		runtime.Gosched()
	}
	validated := make(chan pubsub.ValidationResult, 1)
	go func() {
		validated <- oldValidator(context.Background(), remote.libp2pID, delayedMessage)
	}()
	select {
	case result := <-validated:
		stableGate.RUnlock()
		t.Fatalf("old validator bypassed the lifetime gate before revision rotation: %v", result)
	case <-time.After(100 * time.Millisecond):
	}
	stableGate.RUnlock()
	secondReconcileErr := <-secondReconcile
	validationResult := <-validated
	if !secondWriterPending || secondReconcileErr != nil || validationResult != pubsub.ValidationAccept {
		t.Fatalf("second rotation = pending %v, reconcile %v, old validation %v",
			secondWriterPending, secondReconcileErr, validationResult)
	}
	if !session.closed.Load() {
		t.Fatal("second roster rotation did not close the reactivated generation")
	}
	session, err = gossip.Join(channel.ChannelID)
	if err != nil {
		t.Fatal(err)
	}
	if session.gate != stableGate {
		t.Fatal("multi-rotation successor replaced its lifetime validator gate")
	}
}

func testThreePeerAuthorityChannel(t *testing.T, id string,
	local, author, relay authorityTestPeer,
) ChannelAuthoritySnapshot {
	t.Helper()
	channel := testAuthorityChannel(t, id, model.BindingActive, local, author)
	relayHead, _ := model.NewRecordHead(3, model.Sum([]byte(id+"-relay")))
	relayEpoch, _ := model.ParseOriginEpoch("epoch-" + id + "-relay")
	channel.RosterHead = relayHead
	channel.VerifiedRosterHeads = append(channel.VerifiedRosterHeads, relayHead)
	channel.Bindings = append(channel.Bindings,
		BindingAuthoritySnapshot{PeerID: relay.modelID, State: model.BindingActive})
	channel.Members = append(channel.Members, MemberAuthoritySnapshot{PeerID: relay.modelID,
		OriginEpoch: relayEpoch, Head: relayHead, PublicKey: relay.publicKey})
	return channel
}

func testPeerPublication(t *testing.T, channel ChannelAuthoritySnapshot,
	author, audience authorityTestPeer, summary string,
) model.SignedPublication {
	t.Helper()
	var authorMember MemberAuthoritySnapshot
	for _, member := range channel.Members {
		if member.PeerID == author.modelID {
			authorMember = member
			break
		}
	}
	workID, _ := model.ParseWorkID("work-gossip")
	workRef, _ := model.NewWorkRef(author.modelID, workID)
	scope, err := model.NewEventScope(channel.ChannelID, author.modelID, authorMember.OriginEpoch,
		1, 1, authorMember.Head, channel.RosterHead, workRef)
	if err != nil {
		t.Fatal(err)
	}
	eventID, _ := model.ParseEventID("event-gossip")
	audienceValue, _ := model.NewAudience([]model.PeerID{audience.modelID})
	payload, _ := model.JSONFrom(map[string]any{"content": summary})
	at := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	event, err := model.NewEvent(model.EventSpec{ID: eventID, Scope: scope, Source: model.EventSourceLocal,
		ActorPrincipal: "principal-gossip", Type: model.EventReviewOffered, Audience: audienceValue,
		Summary: summary, Payload: payload, CreatedAt: at, AcceptedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	body, err := model.NewPublicationBody(event)
	if err != nil {
		t.Fatal(err)
	}
	message, _ := model.PublicationSigningMessage(channel.ChannelID, body.Digest())
	publication, err := model.AttachSignature(body, ed25519.Sign(author.privateKey, message))
	if err != nil {
		t.Fatal(err)
	}
	return publication
}

func testPubSubMessage(topic string, publication model.SignedPublication,
	author, receivedFrom peer.ID,
) *pubsub.Message {
	return &pubsub.Message{Message: &pb.Message{From: []byte(author), Topic: &topic,
		Data: publication.WireJSON().Bytes()}, ReceivedFrom: receivedFrom}
}

func tamperPublicationSignature(t *testing.T,
	publication model.SignedPublication,
) model.SignedPublication {
	t.Helper()
	var outer map[string]any
	if err := json.Unmarshal(publication.WireJSON().Bytes(), &outer); err != nil {
		t.Fatal(err)
	}
	signature := publication.OriginSignature()
	signature[0] ^= 0xff
	outer["origin_signature"] = signature
	raw, _ := json.Marshal(outer)
	canonical, _ := model.NewJSON(raw)
	tampered, err := model.ParseSignedPublication(canonical.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	return tampered
}

func waitTopicPeer(t *testing.T, session *TopicSession, expected peer.ID) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, current := range session.Peers() {
			if current == expected {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("topic %s did not converge with %s", session.Name(), expected)
}

func waitTopicPeerAbsent(t *testing.T, session *TopicSession, expected peer.ID) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		found := false
		for _, current := range session.Peers() {
			if current == expected {
				found = true
				break
			}
		}
		if !found {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("topic %s retained disconnected Peer %s", session.Name(), expected)
}

func waitPeerDisconnected(t *testing.T, nodeHost host.Host, expected peer.ID) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if nodeHost.Network().Connectedness(expected) == network.NotConnected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Peer %s did not disconnect", expected)
}

func nextTopicPublication(t *testing.T, session *TopicSession, timeout time.Duration) ReceivedPublication {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	received, err := session.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return received
}

func assertExactGossipSubV12(t *testing.T, nodeHost host.Host, remote peer.ID) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		found := false
		for _, connection := range nodeHost.Network().ConnsToPeer(remote) {
			for _, stream := range connection.GetStreams() {
				negotiated := stream.Protocol()
				if negotiated == pubsub.GossipSubID_v12 {
					found = true
					continue
				}
				if negotiated == pubsub.FloodSubID || strings.HasPrefix(string(negotiated), "/meshsub/") {
					t.Fatalf("unexpected PubSub protocol %q", negotiated)
				}
			}
		}
		if found {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no GossipSub v1.2 stream negotiated with %s", remote)
}

func assertNoPubSubProtocol(t *testing.T, nodeHost host.Host, remote peer.ID) {
	t.Helper()
	for _, connection := range nodeHost.Network().ConnsToPeer(remote) {
		for _, stream := range connection.GetStreams() {
			negotiated := stream.Protocol()
			if negotiated == pubsub.FloodSubID || strings.HasPrefix(string(negotiated), "/meshsub/") {
				t.Fatalf("negotiated forbidden PubSub fallback %q", negotiated)
			}
		}
	}
}

func stringPointer(value string) *string { return &value }
