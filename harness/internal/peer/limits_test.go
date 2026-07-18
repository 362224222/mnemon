package peer

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/protocol"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestHermeticNetworkLimitsAreExact(t *testing.T) {
	t.Parallel()

	limits := HermeticLimits()
	if HermeticProfileName != "r5-hermetic-v1" || limits.NodeConnections != 64 ||
		limits.PeerConnections != 8 || limits.UnknownEnrollmentConnections != 8 ||
		limits.NodeStreams != 256 || limits.PeerStreams != 32 ||
		limits.ApplicationProtocolStreams != 8 || limits.ResourceMemoryBytes != 256<<20 ||
		limits.ResourceFileDescriptors != 128 ||
		limits.GossipWireBytes != 1<<20 || limits.PublicationBytes != model.MaxPublicationBytes ||
		limits.ValidateWorkers != 8 || limits.ValidateThrottle != 32 || limits.ValidateQueue != 128 ||
		limits.PeerOutboundQueue != 64 || limits.SubscriptionBuffer != 128 ||
		limits.ChannelPublishItems != 256 || limits.ChannelPublishBytes != 16<<20 ||
		limits.PullPagePublications != 32 || limits.PullPageBytes != 1<<20 ||
		limits.ArtifactManifestBytes != 4<<20 || limits.ArtifactBlockBytes != 1<<20 ||
		limits.DirectFrameBytes != 8<<20 || limits.NodeArtifactPulls != 16 || limits.PeerArtifactPulls != 4 ||
		limits.NodeInboxPendingBytes != 256<<20 || limits.ChannelInboxPendingBytes != 64<<20 ||
		limits.InboxWorkers != 4 ||
		limits.ChannelRequestTimeout != 10*time.Second || limits.EventRequestTimeout != 15*time.Second ||
		limits.ArtifactRequestTimeout != 30*time.Second {
		t.Fatalf("unexpected %s limits: %#v", HermeticProfileName, limits)
	}
	if limits.GossipWireBytes <= limits.PublicationBytes {
		t.Fatal("Gossip wire compatibility cap was collapsed into the domain publication cap")
	}
}

func TestResourceLimitConfigFencesNodePeerAndApplicationProtocols(t *testing.T) {
	t.Parallel()

	limits := HermeticLimits()
	partial := resourceLimitConfig().ToPartialLimitConfig()
	assertResourceLimits(t, "system", partial.System, limits.NodeConnections, limits.NodeStreams,
		limits.ResourceMemoryBytes, limits.ResourceFileDescriptors)
	assertResourceLimits(t, "transient", partial.Transient, limits.NodeConnections, limits.NodeStreams,
		limits.ResourceMemoryBytes, limits.ResourceFileDescriptors)
	assertResourceLimits(t, "allowlisted system", partial.AllowlistedSystem, limits.NodeConnections,
		limits.NodeStreams, limits.ResourceMemoryBytes, limits.ResourceFileDescriptors)
	assertResourceLimits(t, "allowlisted transient", partial.AllowlistedTransient, limits.NodeConnections,
		limits.NodeStreams, limits.ResourceMemoryBytes, limits.ResourceFileDescriptors)
	assertResourceLimits(t, "peer", partial.PeerDefault, limits.PeerConnections, limits.PeerStreams,
		32<<20, limits.PeerConnections)
	for _, id := range []string{string(GossipProtocol), string(ChannelProtocol), string(EventsProtocol),
		string(ArtifactsProtocol)} {
		value, ok := partial.ProtocolPeer[protocol.ID(id)]
		if !ok {
			t.Fatalf("missing application protocol limit for %s", id)
		}
		assertResourceLimits(t, id, value, 0, limits.ApplicationProtocolStreams, 32<<20, 0)
	}
	assertResourceLimits(t, "connection", partial.Conn, 1, 0, 32<<20, 1)
	assertResourceLimits(t, "stream", partial.Stream, 0, 1, int64(limits.DirectFrameBytes), 0)
	manager, err := NewResourceManager()
	if err != nil {
		t.Fatalf("NewResourceManager() error = %v", err)
	}
	if err := manager.Close(); err != nil {
		t.Fatalf("resource manager Close() error = %v", err)
	}
}

func assertResourceLimits(t *testing.T, name string, value rcmgr.ResourceLimits,
	connections, streams int, memory int64, fileDescriptors int,
) {
	t.Helper()
	if value.Conns.Build(0) != connections || value.ConnsInbound.Build(0) != connections ||
		value.ConnsOutbound.Build(0) != connections || value.Streams.Build(0) != streams ||
		value.StreamsInbound.Build(0) != streams || value.StreamsOutbound.Build(0) != streams ||
		value.Memory.Build(0) != memory || value.FD.Build(0) != fileDescriptors {
		t.Fatalf("%s resource limits = %#v", name, value)
	}
}
