package peer

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
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

func TestGossipMessageIDBindsUnsupportedHeaderBeforeStrictRejection(t *testing.T) {
	t.Parallel()

	local := testAuthorityPeer(t, "message-id-unsupported-local")
	author := testAuthorityPeer(t, "message-id-unsupported-author")
	relay := testAuthorityPeer(t, "message-id-unsupported-relay")
	channel := testThreePeerAuthorityChannel(t, "message-id-unsupported-channel", local, author, relay)
	authority, _ := NewAuthority(local.modelID)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{channel}}); err != nil {
		t.Fatal(err)
	}
	gossip := &Gossip{authority: authority}
	topic, _ := TopicName(channel.ChannelID)
	publication := testPeerPublication(t, channel, author, local, "valid-after-unsupported")
	unsupportedRaw := resignEventFramePublication(t, publication.WireJSON().Bytes(), author.privateKey,
		func(body map[string]any) {
			body["schema_version"] = json.Number("2")
			body["future_semantics"] = map[string]any{"mode": "opaque"}
		})
	evidence, err := model.ParsePublicationEvidence(unsupportedRaw)
	if err != nil || evidence.IsSupported() {
		t.Fatalf("unsupported stable evidence = (%#v, %v)", evidence, err)
	}
	unsupportedWire := &pb.Message{From: []byte(author.libp2pID), Topic: stringPointer(topic),
		Data: unsupportedRaw}
	unsupportedID := PublicationMessageID(unsupportedWire)
	reauthored := *unsupportedWire
	reauthored.From = []byte(relay.libp2pID)
	if PublicationMessageID(&reauthored) == unsupportedID {
		t.Fatal("unsupported stable header fell back to an author-free raw MessageID")
	}
	gate := &channelGate{}
	gate.deliverable.Store(true)
	session := &TopicSession{gossip: gossip, channelID: channel.ChannelID, name: topic, gate: gate}
	validator := gossip.validator(session)
	unsupported := &pubsub.Message{Message: unsupportedWire, ReceivedFrom: relay.libp2pID}
	if result := validator(context.Background(), relay.libp2pID, unsupported); result != pubsub.ValidationReject {
		t.Fatalf("unsupported live publication result = %v, want reject", result)
	}
	valid := testPubSubMessage(topic, publication, author.libp2pID, relay.libp2pID)
	if PublicationMessageID(valid.Message) == unsupportedID {
		t.Fatal("later supported publication shared the unsupported seen-cache identity")
	}
	if result := validator(context.Background(), relay.libp2pID, valid); result != pubsub.ValidationAccept {
		t.Fatalf("valid publication after unsupported challenger result = %v", result)
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
