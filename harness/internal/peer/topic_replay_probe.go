package peer

import (
	"context"
	"fmt"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const WrongTopicReplayRejection = "wrong_topic"

type WrongTopicReplayProbeResult struct {
	Rejected bool
	Reason   string
}

// ProbeWrongTopicReplay runs the same Topic validator used by GossipSub against
// exact signed publication bytes while binding the outer message to this
// session's target topic. It is a read-only adversarial probe: the rejected
// message is never handed to a subscription or durable Store path.
func (session *TopicSession) ProbeWrongTopicReplay(ctx context.Context,
	publication model.SignedPublication,
) (WrongTopicReplayProbeResult, error) {
	if session == nil || session.gossip == nil || session.gossip.authority == nil ||
		session.gate == nil || ctx == nil || ctx.Err() != nil ||
		publication.Event().ID().IsZero() {
		return WrongTopicReplayProbeResult{}, fmt.Errorf("%w: replay probe is unavailable",
			ErrGossipTopic)
	}
	local := session.gossip.authority.LocalPeerID()
	if local == "" || publication.Event().Scope().OriginPeerID().String() != local.String() {
		return WrongTopicReplayProbeResult{}, fmt.Errorf("%w: replay probe requires a local-origin publication",
			ErrGossipTopic)
	}
	if publication.Key().ChannelID() == session.channelID {
		return WrongTopicReplayProbeResult{}, fmt.Errorf("%w: replay probe requires distinct Channels",
			ErrGossipTopic)
	}
	if session.closed.Load() || !session.gate.deliverable.Load() {
		return WrongTopicReplayProbeResult{}, fmt.Errorf("%w: target topic is unavailable",
			ErrGossipTopic)
	}
	message := &pubsub.Message{Message: &pb.Message{From: []byte(local), Topic: &session.name,
		Data: publication.WireJSON().Bytes()}, ReceivedFrom: local}
	session.localPublishes.Add(1)
	defer session.localPublishes.Add(-1)
	result := session.gossip.validator(session)(ctx, local, message)
	switch result {
	case pubsub.ValidationReject:
		return WrongTopicReplayProbeResult{Rejected: true, Reason: WrongTopicReplayRejection}, nil
	case pubsub.ValidationAccept:
		return WrongTopicReplayProbeResult{Reason: "accepted"}, nil
	default:
		return WrongTopicReplayProbeResult{Reason: "ignored"}, nil
	}
}
