package store

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestChannelStatusEvidenceCopiesMutableSlices(t *testing.T) {
	t.Parallel()
	peerID, err := model.ParsePeerID("peer-status-copy")
	if err != nil {
		t.Fatal(err)
	}
	authority := ChannelStatusAuthority{localPeerID: peerID, channels: []ChannelStatusChannel{{
		rosterHead:   ChannelStatusRosterHead{ownerSignature: []byte("signature")},
		publications: []ChannelStatusPublication{{audiencePeerIDs: []model.PeerID{peerID}}},
		progress:     ChannelStatusProgress{readiness: []ChannelPeerReadiness{{PeerID: peerID}}},
	}}}
	first := authority.Channels()
	first[0].rosterHead.ownerSignature[0] = 'x'
	first[0].publications[0].audiencePeerIDs[0] = model.PeerID{}
	first[0].progress.readiness[0].PeerID = model.PeerID{}
	second := authority.Channels()[0]
	if string(second.RosterHead().OwnerSignature()) != "signature" ||
		second.Publications()[0].AudiencePeerIDs()[0] != peerID ||
		second.Progress().Readiness()[0].PeerID != peerID {
		t.Fatal("Channel status evidence leaked mutable backing storage")
	}
}
