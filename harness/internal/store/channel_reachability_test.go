package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestSetPeerReachabilityRejectsRevokedBinding(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	channel := testkit.NewSignedChannel(t, "reachability-revoked")
	remote := channel.AppendActive(t, "reachability-revoked-remote")
	terminal := channel.AppendTerminal(t, remote.Identity().PeerID(), model.MemberRevoked)
	insertChannelTestNode(t, st.db, channel.Owner(), channel.Channel().CreatedAt())
	insertSignedChannelFixture(t, st.db, channel, model.TopicJoining)
	insertSignedPeerBinding(t, st.db, channel.Channel().ID(), terminal, "former",
		model.BindingRevoked, model.ReachabilityUnknown, remote.Member().CreatedAt())
	_, err := st.SetPeerReachability(context.Background(), SetPeerReachabilitySpec{
		ChannelID: channel.Channel().ID(), PeerID: remote.Identity().PeerID(),
		OriginEpoch: remote.Identity().OriginEpoch(), ExpectedRosterHead: channel.Roster().Head(),
		Reachability: model.ReachabilityReachable, At: channel.Channel().UpdatedAt().Add(time.Hour)})
	if !errors.Is(err, ErrChannelRuntimeAuthority) {
		t.Fatalf("revoked reachability error = %v", err)
	}
	read, err := st.ReadPeerReachability(context.Background(), channel.Channel().ID(),
		remote.Identity().PeerID())
	if err != nil || read.BindingState != model.BindingRevoked || read.HasLastSeen {
		t.Fatalf("revoked read projection = (%#v,%v)", read, err)
	}
}
