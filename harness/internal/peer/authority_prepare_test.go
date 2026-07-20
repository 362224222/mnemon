package peer

import (
	"errors"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestAuthorityInvalidReplacementKeepsPreviousRevision(t *testing.T) {
	t.Parallel()

	local := testAuthorityPeer(t, "replace-local")
	remote := testAuthorityPeer(t, "replace-remote")
	authority, _ := NewAuthority(local.modelID)
	valid := testAuthorityChannel(t, "channel-valid", model.BindingActive, local, remote)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{valid}}); err != nil {
		t.Fatal(err)
	}
	topic, _ := TopicName(valid.ChannelID)
	invalid := valid
	invalid.Members = append([]MemberAuthoritySnapshot(nil), valid.Members...)
	invalid.Members[1].PublicKey = append([]byte(nil), local.publicKey...)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{invalid}}); !errors.Is(err, ErrNetworkAuthority) {
		t.Fatalf("invalid replacement error = %v", err)
	}
	if !authority.CanUseTopic(remote.libp2pID, topic) {
		t.Fatal("invalid replacement partially changed the visible authority")
	}
}

func TestAuthorityRejectsRosterGapAndSharedMemberHead(t *testing.T) {
	t.Parallel()

	local := testAuthorityPeer(t, "roster-local")
	remote := testAuthorityPeer(t, "roster-remote")
	authority, _ := NewAuthority(local.modelID)
	valid := testAuthorityChannel(t, "roster-channel", model.BindingActive, local, remote)
	gap := valid
	gap.VerifiedRosterHeads = []model.RecordHead{valid.RosterHead}
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{gap}}); !errors.Is(err, ErrNetworkAuthority) {
		t.Fatalf("roster gap error = %v", err)
	}
	shared := valid
	shared.Members = append([]MemberAuthoritySnapshot(nil), valid.Members...)
	shared.Members[1].Head = shared.Members[0].Head
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{shared}}); !errors.Is(err, ErrNetworkAuthority) {
		t.Fatalf("shared member head error = %v", err)
	}
}
