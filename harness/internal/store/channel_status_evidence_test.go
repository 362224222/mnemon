package store

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestChannelObservationCopiesMutableSlices(t *testing.T) {
	t.Parallel()
	peerID, err := model.ParsePeerID("peer-status-copy")
	if err != nil {
		t.Fatal(err)
	}
	observation := ChannelObservation{localPeerID: peerID, channels: []ChannelObservationChannel{{
		rosterHead: ChannelObservationRosterHead{ownerSignature: []byte("signature")},
		progress:   ChannelStatusProgress{readiness: []ChannelPeerReadiness{{PeerID: peerID}}},
	}}}
	first := observation.Channels()
	first[0].rosterHead.ownerSignature[0] = 'x'
	first[0].progress.readiness[0].PeerID = model.PeerID{}
	second := observation.Channels()[0]
	if string(second.RosterHead().OwnerSignature()) != "signature" ||
		second.Progress().Readiness()[0].PeerID != peerID {
		t.Fatal("Channel observation leaked mutable backing storage")
	}
}
