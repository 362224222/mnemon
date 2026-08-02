package peer

import (
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const HermeticProfileName = model.HermeticNetworkProfileName

// NetworkLimits is the one production and Hermetic resource profile. Keeping
// it as a value (rather than test-only knobs) makes every transport boundary
// inspectable without exposing mutable limiter configuration.
type NetworkLimits struct {
	NodeConnections              int
	PeerConnections              int
	UnknownEnrollmentConnections int
	NodeStreams                  int
	PeerStreams                  int
	ApplicationProtocolStreams   int
	ResourceMemoryBytes          int64
	ResourceFileDescriptors      int
	GossipWireBytes              int
	PublicationBytes             int
	ValidateWorkers              int
	ValidateThrottle             int
	ValidateQueue                int
	PeerOutboundQueue            int
	SubscriptionBuffer           int
	ChannelPublishItems          int
	ChannelPublishBytes          int
	PullPagePublications         int
	PullPageBytes                int
	ArtifactManifestBytes        int
	ArtifactBlockBytes           int
	DirectFrameBytes             int
	NodeArtifactPulls            int
	PeerArtifactPulls            int
	NodeInboxPendingBytes        int64
	ChannelInboxPendingBytes     int64
	InboxWorkers                 int
	ChannelRequestTimeout        time.Duration
	EventRequestTimeout          time.Duration
	ArtifactRequestTimeout       time.Duration
}

// HermeticLimits returns a copy of the fixed shared peer transport profile.
func HermeticLimits() NetworkLimits {
	return NetworkLimits{
		NodeConnections: 64, PeerConnections: 8, UnknownEnrollmentConnections: 8,
		NodeStreams: 256, PeerStreams: 32, ApplicationProtocolStreams: 8,
		ResourceMemoryBytes: 256 << 20, ResourceFileDescriptors: 128,
		GossipWireBytes: 1 << 20, PublicationBytes: model.MaxPublicationBytes,
		ValidateWorkers: 8, ValidateThrottle: 32, ValidateQueue: 128,
		PeerOutboundQueue: 64, SubscriptionBuffer: 128,
		ChannelPublishItems: 256, ChannelPublishBytes: 16 << 20,
		PullPagePublications: 32, PullPageBytes: 1 << 20,
		ArtifactManifestBytes: 4 << 20, ArtifactBlockBytes: 1 << 20, DirectFrameBytes: 8 << 20,
		NodeArtifactPulls: 16, PeerArtifactPulls: 4,
		NodeInboxPendingBytes: 256 << 20, ChannelInboxPendingBytes: 64 << 20, InboxWorkers: 4,
		ChannelRequestTimeout: 10 * time.Second, EventRequestTimeout: 15 * time.Second,
		ArtifactRequestTimeout: 30 * time.Second,
	}
}

// NewResourceManager builds the fixed libp2p limiter used by every mnemond
// host. Domain frame and queue limits remain enforced by protocol handlers;
// this manager is the outer connection, stream, memory and descriptor fence.
func NewResourceManager() (network.ResourceManager, error) {
	config := resourceLimitConfig()
	return rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(config))
}

func resourceLimitConfig() rcmgr.ConcreteLimitConfig {
	limits := HermeticLimits()
	node := rcmgr.ResourceLimits{
		Conns: rcmgr.LimitVal(limits.NodeConnections), ConnsInbound: rcmgr.LimitVal(limits.NodeConnections),
		ConnsOutbound: rcmgr.LimitVal(limits.NodeConnections), Streams: rcmgr.LimitVal(limits.NodeStreams),
		StreamsInbound: rcmgr.LimitVal(limits.NodeStreams), StreamsOutbound: rcmgr.LimitVal(limits.NodeStreams),
		Memory: rcmgr.LimitVal64(limits.ResourceMemoryBytes), FD: rcmgr.LimitVal(limits.ResourceFileDescriptors),
	}
	perPeer := rcmgr.ResourceLimits{
		Conns: rcmgr.LimitVal(limits.PeerConnections), ConnsInbound: rcmgr.LimitVal(limits.PeerConnections),
		ConnsOutbound: rcmgr.LimitVal(limits.PeerConnections), Streams: rcmgr.LimitVal(limits.PeerStreams),
		StreamsInbound: rcmgr.LimitVal(limits.PeerStreams), StreamsOutbound: rcmgr.LimitVal(limits.PeerStreams),
		Memory: 32 << 20, FD: rcmgr.LimitVal(limits.PeerConnections),
	}
	perApplicationProtocol := rcmgr.ResourceLimits{
		Streams:         rcmgr.LimitVal(limits.ApplicationProtocolStreams),
		StreamsInbound:  rcmgr.LimitVal(limits.ApplicationProtocolStreams),
		StreamsOutbound: rcmgr.LimitVal(limits.ApplicationProtocolStreams),
		Memory:          32 << 20,
	}
	protocolPeer := make(map[protocol.ID]rcmgr.ResourceLimits, 6)
	for _, id := range []protocol.ID{GossipProtocol, ChannelProtocol, EventsProtocol, ArtifactsProtocol,
		AgencyDeliveryProtocol, AgencyObjectProtocol} {
		protocolPeer[id] = perApplicationProtocol
	}
	partial := rcmgr.PartialLimitConfig{
		System: node, Transient: node, AllowlistedSystem: node, AllowlistedTransient: node,
		PeerDefault: perPeer, ProtocolPeer: protocolPeer,
		Conn: rcmgr.ResourceLimits{Conns: 1, ConnsInbound: 1, ConnsOutbound: 1,
			Memory: 32 << 20, FD: 1},
		Stream: rcmgr.ResourceLimits{Streams: 1, StreamsInbound: 1, StreamsOutbound: 1,
			Memory: rcmgr.LimitVal64(limits.DirectFrameBytes)},
	}
	return partial.Build(rcmgr.DefaultLimits.AutoScale())
}
