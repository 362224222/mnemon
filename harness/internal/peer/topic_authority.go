package peer

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const publicationMessageIDDomain = "mnemon/r5/gossipsub-message-id/1"

// PublicationMessageID implements the frozen valid-header tuple plus the
// PubSub original author. The latter prevents another active member from
// re-publishing identical application bytes under its own transport signature
// and poisoning the seen cache before validation rejects it. ReceivedFrom and
// transport seqno are deliberately excluded so relays and retries are stable.
//
// Only a frame whose stable publication header cannot be parsed and validated
// uses a separate topic+raw fallback. Current-semantic support belongs to the
// strict validator, not this seen-cache key: an unsupported but stable header must
// still bind From and the complete publication identity tuple. Parsing is
// bounded by the publication cap, performs no I/O and is total for every frame
// admitted by GossipSub's outer wire limit.
func PublicationMessageID(message *pb.Message) string {
	digest := sha256.New()
	if message == nil {
		writeMessageIDFields(digest, []byte(publicationMessageIDDomain), []byte("invalid"), nil, nil)
		return "r5:" + hex.EncodeToString(digest.Sum(nil))
	}
	publication, err := model.ParsePublicationEvidence(message.GetData())
	if err != nil {
		writeMessageIDFields(digest, []byte(publicationMessageIDDomain), []byte("invalid"),
			[]byte(message.GetTopic()), message.GetData())
		return "r5:" + hex.EncodeToString(digest.Sum(nil))
	}
	sequence := make([]byte, 8)
	binary.BigEndian.PutUint64(sequence, publication.ChannelSequence())
	writeMessageIDFields(digest,
		[]byte(publicationMessageIDDomain), []byte("valid"), []byte(message.GetTopic()), message.GetFrom(),
		[]byte(publication.ChannelID().String()), []byte(publication.OriginPeerID().String()),
		[]byte(publication.OriginEpoch().String()), sequence, []byte(publication.EventID().String()),
		publication.EventDigest().Bytes(), publication.Digest().Bytes(),
	)
	return "r5:" + hex.EncodeToString(digest.Sum(nil))
}

func writeMessageIDFields(digest hash.Hash, fields ...[]byte) {
	var length [8]byte
	for _, field := range fields {
		binary.BigEndian.PutUint64(length[:], uint64(len(field)))
		_, _ = digest.Write(length[:])
		_, _ = digest.Write(field)
	}
}

type authoritySubscriptionFilter struct{ authority *Authority }

var _ pubsub.SubscriptionFilter = authoritySubscriptionFilter{}

func (filter authoritySubscriptionFilter) CanSubscribe(topic string) bool {
	return filter.authority != nil && filter.authority.CanSubscribe(topic)
}

func (filter authoritySubscriptionFilter) FilterIncomingSubscriptions(from libp2ppeer.ID,
	subscriptions []*pb.RPC_SubOpts,
) ([]*pb.RPC_SubOpts, error) {
	if len(subscriptions) > model.MaxChannelsPerNode {
		return nil, pubsub.ErrTooManySubscriptions
	}
	accepted := make(map[string]*pb.RPC_SubOpts, len(subscriptions))
	for _, subscription := range subscriptions {
		if subscription == nil {
			continue
		}
		topic := subscription.GetTopicid()
		if !filter.CanSubscribe(topic) {
			continue
		}
		// Unsubscribe never grants access and remains admissible for cleanup
		// after a scoped revoke. Subscribe requires exact active authority.
		if subscription.GetSubscribe() && !filter.authority.CanUseTopic(from, topic) {
			continue
		}
		if previous, duplicate := accepted[topic]; duplicate {
			if previous.GetSubscribe() != subscription.GetSubscribe() {
				delete(accepted, topic)
			}
			continue
		}
		accepted[topic] = subscription
	}
	result := make([]*pb.RPC_SubOpts, 0, len(accepted))
	for _, subscription := range accepted {
		result = append(result, subscription)
	}
	return result, nil
}

func authorityRPCInspector(authority *Authority) func(libp2ppeer.ID, *pubsub.RPC) error {
	return func(from libp2ppeer.ID, rpc *pubsub.RPC) error {
		if authority == nil || from == "" || rpc == nil {
			return fmt.Errorf("%w: invalid incoming RPC", ErrGossipTopic)
		}
		// This runs before transport signature verification, MessageID parsing and
		// the global validation queue. Inspect only authenticated transport Peer +
		// exact raw topic; full publication authority remains in the validator.
		for _, message := range rpc.GetPublish() {
			if message == nil || !authority.CanUseTopic(from, message.GetTopic()) {
				return fmt.Errorf("%w: incoming publish is outside Peer Channel authority", ErrGossipTopic)
			}
		}
		return nil
	}
}
