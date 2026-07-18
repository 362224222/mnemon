package peer

import (
	"errors"
	"testing"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestProtocolIDsAndChannelTopicRoundTrip(t *testing.T) {
	t.Parallel()

	if GossipProtocol != pubsub.GossipSubID_v12 || ChannelProtocol != "/mnemon/channel/1" ||
		EventsProtocol != "/mnemon/events/1" ||
		ArtifactsProtocol != "/mnemon/artifacts/1" {
		t.Fatal("R5 transport protocol IDs changed")
	}
	if !managedProtocol(GossipProtocol) || !managedProtocol(ChannelProtocol) ||
		!managedProtocol(EventsProtocol) || !managedProtocol(ArtifactsProtocol) ||
		managedProtocol("/mnemon/unknown/1") {
		t.Fatal("managed protocol boundary changed")
	}
	channelID, _ := model.ParseChannelID("channel-alpha_7")
	topic, err := TopicName(channelID)
	if err != nil || topic != "/mnemon/channel/channel-alpha_7/events/1" {
		t.Fatalf("TopicName() = (%q, %v)", topic, err)
	}
	parsed, err := ParseTopicName(topic)
	if err != nil || parsed != channelID {
		t.Fatalf("ParseTopicName() = (%q, %v)", parsed.String(), err)
	}
}

func TestChannelTopicRejectsInjectionAndAlternateShapes(t *testing.T) {
	t.Parallel()

	unsafeIDs := []string{"channel/alpha", `channel\alpha`}
	for _, value := range unsafeIDs {
		channelID, _ := model.ParseChannelID(value)
		if _, err := TopicName(channelID); !errors.Is(err, ErrProtocolScope) {
			t.Fatalf("TopicName(%q) error = %v", value, err)
		}
	}
	invalidTopics := []string{
		"", "/mnemon/channel//events/1", "/mnemon/channel/channel-a/events/2",
		"/mnemon/channel/channel-a/events/1/", "/mnemon/channel/channel/a/events/1",
		"/other/channel-a/events/1", "mnemon/channel/channel-a/events/1",
	}
	for _, topic := range invalidTopics {
		if _, err := ParseTopicName(topic); !errors.Is(err, ErrProtocolScope) {
			t.Fatalf("ParseTopicName(%q) error = %v", topic, err)
		}
	}
}
