package peer

import (
	"context"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestTopicSessionProbeWrongTopicReplayRejectsExactLocalPublication(t *testing.T) {
	t.Parallel()

	local := testAuthorityPeer(t, "wrong-topic-probe-local")
	remote := testAuthorityPeer(t, "wrong-topic-probe-remote")
	source := testAuthorityChannel(t, "wrong-topic-probe-source",
		model.BindingActive, local, remote)
	target := testAuthorityChannel(t, "wrong-topic-probe-target",
		model.BindingActive, local, remote)
	authority, _ := NewAuthority(local.modelID)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{source, target}}); err != nil {
		t.Fatal(err)
	}
	targetTopic, _ := TopicName(target.ChannelID)
	gate := &channelGate{}
	gate.deliverable.Store(true)
	session := &TopicSession{gossip: &Gossip{authority: authority}, channelID: target.ChannelID,
		name: targetTopic, gate: gate}
	publication := testPeerPublication(t, source, local, remote, "source-on-target")
	result, err := session.ProbeWrongTopicReplay(context.Background(), publication)
	if err != nil || !result.Rejected || result.Reason != WrongTopicReplayRejection {
		t.Fatalf("ProbeWrongTopicReplay() = (%#v, %v), want wrong-topic rejection",
			result, err)
	}
}
