package peer

import (
	"errors"
	"fmt"
	"strings"

	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	GossipProtocol         protocol.ID = "/meshsub/1.2.0"
	ChannelProtocol        protocol.ID = "/mnemon/channel/1"
	EventsProtocol         protocol.ID = "/mnemon/events/1"
	ArtifactsProtocol      protocol.ID = "/mnemon/artifacts/1"
	AgencyDeliveryProtocol protocol.ID = "/mnemon/agency/delivery/1"
	AgencyObjectProtocol   protocol.ID = "/mnemon/agency/object/1"

	channelTopicPrefix = "/mnemon/channel/"
	channelTopicSuffix = "/events/1"
)

func managedProtocol(protocolID protocol.ID) bool {
	switch protocolID {
	case GossipProtocol, ChannelProtocol, EventsProtocol, ArtifactsProtocol,
		AgencyDeliveryProtocol, AgencyObjectProtocol:
		return true
	default:
		return false
	}
}

func agencyProtocol(protocolID protocol.ID) bool {
	return protocolID == AgencyDeliveryProtocol || protocolID == AgencyObjectProtocol
}

var ErrProtocolScope = errors.New("invalid Mnemon peer protocol scope")

// TopicName derives the one GossipSub topic for a Channel. Channel identity is
// an opaque path segment, not an authorization credential, so separators and
// escape-like alternate spellings are rejected rather than normalized.
func TopicName(channelID model.ChannelID) (string, error) {
	if channelID.IsZero() || strings.ContainsAny(channelID.String(), `/\`) {
		return "", fmt.Errorf("%w: ChannelID is not one canonical topic segment", ErrProtocolScope)
	}
	return channelTopicPrefix + channelID.String() + channelTopicSuffix, nil
}

// ParseTopicName accepts only the exact R5 Channel Event topic shape.
func ParseTopicName(topic string) (model.ChannelID, error) {
	if !strings.HasPrefix(topic, channelTopicPrefix) || !strings.HasSuffix(topic, channelTopicSuffix) {
		return model.ChannelID{}, fmt.Errorf("%w: unknown topic", ErrProtocolScope)
	}
	value := strings.TrimSuffix(strings.TrimPrefix(topic, channelTopicPrefix), channelTopicSuffix)
	channelID, err := model.ParseChannelID(value)
	if err != nil {
		return model.ChannelID{}, fmt.Errorf("%w: %v", ErrProtocolScope, err)
	}
	canonical, err := TopicName(channelID)
	if err != nil || canonical != topic {
		return model.ChannelID{}, fmt.Errorf("%w: noncanonical topic", ErrProtocolScope)
	}
	return channelID, nil
}
