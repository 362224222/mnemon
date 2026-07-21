package node

import (
	"sort"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type channelSessionObserver interface {
	channelSessionReady(model.ChannelID) bool
}

func (manager *ChannelManager) channelSessionReady(channelID model.ChannelID) bool {
	return manager != nil && manager.runtime != nil && manager.runtime.HasCurrentSession(channelID)
}

func projectStatusChannels(authority store.ChannelStatusAuthority,
	sessions channelSessionObserver,
) []localapi.StatusChannelSnapshot {
	channels := authority.Channels()
	result := make([]localapi.StatusChannelSnapshot, 0, len(channels))
	for _, durable := range channels {
		sessionReady := sessions != nil && sessions.channelSessionReady(durable.Channel().ID())
		result = append(result, projectStatusChannel(authority.LocalPeerID(), durable, sessionReady))
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Alias < result[right].Alias })
	return result
}

func projectStatusChannel(local model.PeerID, durable store.ChannelStatusChannel,
	sessionReady bool,
) localapi.StatusChannelSnapshot {
	channel, progress := durable.Channel(), durable.Progress()
	topic := projectStatusChannelTopic(local, durable, sessionReady)
	commit, publication := progress.Commit(), progress.Publication()
	cursor, inbox := progress.Cursor(), progress.Inbox()
	artifact, runtime := progress.Artifact(), progress.Runtime()
	return localapi.StatusChannelSnapshot{Alias: channel.LocalAlias(), Membership: string(channel.Status()),
		RosterRevision: channel.RosterHead().Revision(), Topic: topic,
		LocalCommit: localapi.StatusChannelCommit{Accepted: commit.Accepted},
		Publication: localapi.StatusChannelPublication{Queued: publication.Queued,
			Published: publication.Published, Blocked: publication.Blocked,
			RemotePending:      publication.RemotePending,
			RemoteAcknowledged: publication.RemoteAcknowledged,
			RemoteBlocked:      publication.RemoteBlocked},
		Cursor: localapi.StatusChannelCursor{InboundOrigins: cursor.InboundOrigins,
			InboundCaughtUp: cursor.InboundCaughtUp, InboundPending: cursor.InboundPending,
			InboundTerminal: cursor.InboundTerminal, InboundGapped: cursor.InboundGapped,
			OutboundPeers: cursor.OutboundPeers, OutboundAcknowledged: cursor.OutboundAcknowledged,
			OutboundPending: cursor.OutboundPending},
		Inbox: localapi.StatusChannelInbox{Durable: inbox.Durable, Pending: inbox.Pending,
			WaitingArtifact: inbox.WaitingArtifact, Accepted: inbox.Accepted,
			Rejected: inbox.Rejected, Conflicted: inbox.Conflicted, Ignored: inbox.Ignored,
			Quarantined: inbox.Quarantined},
		Artifact: localapi.StatusChannelArtifact{PinnedRoots: artifact.PinnedRoots,
			VerifiedRoots: artifact.VerifiedRoots},
		Runtime: localapi.StatusChannelRuntime{HandlingPending: runtime.HandlingPending,
			HandlingClaimed: runtime.HandlingClaimed, HandlingCompleted: runtime.HandlingCompleted,
			HandlingRejected: runtime.HandlingRejected, HandlingDead: runtime.HandlingDead,
			RunActive: runtime.RunActive, RunCompleted: runtime.RunCompleted,
			RunRetry: runtime.RunRetry, RunRejected: runtime.RunRejected, RunFailed: runtime.RunFailed}}
}

func projectStatusChannelTopic(local model.PeerID, durable store.ChannelStatusChannel,
	sessionReady bool,
) localapi.StatusChannelTopic {
	channel := durable.Channel()
	readiness := make(map[model.PeerID]bool)
	for _, remote := range durable.Progress().Readiness() {
		readiness[remote.PeerID] = remote.Ready()
	}
	bindings := make(map[model.PeerID]model.PeerBinding)
	for _, binding := range durable.Bindings() {
		bindings[binding.PeerID()] = binding
	}
	latest := make(map[model.PeerID]model.Member)
	for _, member := range durable.Roster().Members() {
		latest[member.PeerID()] = member
	}
	ready, unreachable := uint8(0), uint8(0)
	for peerID, member := range latest {
		if member.Status() != model.MemberActive {
			continue
		}
		if peerID == local {
			if channel.TopicState() == model.TopicJoined && sessionReady {
				ready++
			}
			continue
		}
		binding, present := bindings[peerID]
		if !present || binding.Reachability() != model.ReachabilityReachable {
			unreachable++
		}
		if present && readiness[peerID] {
			ready++
		}
	}
	state := observedChannelTopicState(channel, sessionReady)
	if state == "joined" && ready < activeChannelMembers(latest) {
		state = "converging"
	}
	return localapi.StatusChannelTopic{State: state, ReadyMembers: ready,
		TotalMembers: uint8(len(latest)), UnreachableMembers: unreachable}
}

func activeChannelMembers(members map[model.PeerID]model.Member) uint8 {
	active := uint8(0)
	for _, member := range members {
		if member.Status() == model.MemberActive {
			active++
		}
	}
	return active
}

func observedChannelTopicState(channel model.Channel, sessionReady bool) string {
	if channel.Status().Terminal() || channel.TopicState() == model.TopicLeft {
		return "left"
	}
	if channel.Status() == model.ChannelConflicted || channel.TopicState() == model.TopicBlocked {
		return "blocked"
	}
	if channel.TopicState() == model.TopicJoined && sessionReady {
		return "joined"
	}
	return "converging"
}
