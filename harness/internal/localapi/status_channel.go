package localapi

import (
	"errors"
	"reflect"
	"sort"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	statusChannelReady    = "ready"
	statusChannelQueued   = "queued"
	statusChannelDegraded = "degraded"
	statusChannelTerminal = "terminal"
)

// StatusChannelSnapshot is controller input assembled from one durable
// ChannelStatusAuthority and one adjacent topic-session observation. It has no
// caller-selected aggregate state; the local API derives that closed result.
type StatusChannelSnapshot struct {
	Alias          string
	Membership     string
	RosterRevision uint64
	Topic          StatusChannelTopic
	LocalCommit    StatusChannelCommit
	Publication    StatusChannelPublication
	Cursor         StatusChannelCursor
	Inbox          StatusChannelInbox
	Artifact       StatusChannelArtifact
	Runtime        StatusChannelRuntime
}

type StatusChannelTopic struct {
	ReadyMembers       uint8  `json:"ready_members"`
	State              string `json:"state"`
	TotalMembers       uint8  `json:"total_members"`
	UnreachableMembers uint8  `json:"unreachable_members"`
}

type StatusChannelCommit struct {
	Accepted uint64 `json:"accepted"`
}

type StatusChannelPublication struct {
	Blocked            uint64 `json:"blocked"`
	Published          uint64 `json:"published"`
	Queued             uint64 `json:"queued"`
	RemoteAcknowledged uint64 `json:"remote_acknowledged"`
	RemoteBlocked      uint64 `json:"remote_blocked"`
	RemotePending      uint64 `json:"remote_pending"`
}

type StatusChannelCursor struct {
	InboundCaughtUp      uint64 `json:"inbound_caught_up"`
	InboundGapped        uint64 `json:"inbound_gapped"`
	InboundOrigins       uint64 `json:"inbound_origins"`
	InboundPending       uint64 `json:"inbound_pending"`
	InboundTerminal      uint64 `json:"inbound_terminal"`
	OutboundAcknowledged uint64 `json:"outbound_acknowledged"`
	OutboundPeers        uint64 `json:"outbound_peers"`
	OutboundPending      uint64 `json:"outbound_pending"`
}

type StatusChannelInbox struct {
	Accepted        uint64 `json:"accepted"`
	Conflicted      uint64 `json:"conflicted"`
	Durable         uint64 `json:"durable"`
	Ignored         uint64 `json:"ignored"`
	Pending         uint64 `json:"pending"`
	Quarantined     uint64 `json:"quarantined"`
	Rejected        uint64 `json:"rejected"`
	WaitingArtifact uint64 `json:"waiting_artifact"`
}

type StatusChannelArtifact struct {
	PinnedRoots   uint64 `json:"pinned_roots"`
	VerifiedRoots uint64 `json:"verified_roots"`
	WaitingInbox  uint64 `json:"waiting_inbox"`
}

type StatusChannelRuntime struct {
	HandlingClaimed   uint64 `json:"handling_claimed"`
	HandlingCompleted uint64 `json:"handling_completed"`
	HandlingDead      uint64 `json:"handling_dead"`
	HandlingPending   uint64 `json:"handling_pending"`
	HandlingRejected  uint64 `json:"handling_rejected"`
	RunActive         uint64 `json:"run_active"`
	RunCompleted      uint64 `json:"run_completed"`
	RunFailed         uint64 `json:"run_failed"`
	RunRejected       uint64 `json:"run_rejected"`
	RunRetry          uint64 `json:"run_retry"`
}

type StatusChannelSemantic struct {
	Accepted    uint64 `json:"accepted"`
	Conflicted  uint64 `json:"conflicted"`
	Ignored     uint64 `json:"ignored"`
	Originated  uint64 `json:"originated"`
	Pending     uint64 `json:"pending"`
	Quarantined uint64 `json:"quarantined"`
	Rejected    uint64 `json:"rejected"`
}

// StatusChannel is the compact public progress projection. It deliberately
// omits PeerIDs, Event identities, diagnostics, paths, and lease material.
type StatusChannel struct {
	Alias          string                   `json:"alias"`
	Artifact       StatusChannelArtifact    `json:"artifact"`
	Cursor         StatusChannelCursor      `json:"cursor"`
	Inbox          StatusChannelInbox       `json:"inbox"`
	LocalCommit    StatusChannelCommit      `json:"local_commit"`
	Membership     string                   `json:"membership"`
	Publication    StatusChannelPublication `json:"publication"`
	RosterRevision uint64                   `json:"roster_revision"`
	Runtime        StatusChannelRuntime     `json:"runtime"`
	Semantic       StatusChannelSemantic    `json:"semantic_outcome"`
	State          string                   `json:"state"`
	Topic          StatusChannelTopic       `json:"topic"`
}

func newStatusChannels(snapshots []StatusChannelSnapshot) ([]StatusChannel, error) {
	if len(snapshots) > model.MaxChannelsPerNode {
		return nil, errors.New("local API: status Channel count exceeds its bound")
	}
	ordered := append([]StatusChannelSnapshot(nil), snapshots...)
	sort.Slice(ordered, func(left, right int) bool { return ordered[left].Alias < ordered[right].Alias })
	result := make([]StatusChannel, len(ordered))
	for index, snapshot := range ordered {
		if index > 0 && snapshot.Alias == ordered[index-1].Alias {
			return nil, errors.New("local API: status Channel alias is duplicated")
		}
		channel, err := newStatusChannel(snapshot)
		if err != nil {
			return nil, err
		}
		result[index] = channel
	}
	return result, nil
}

func newStatusChannel(snapshot StatusChannelSnapshot) (StatusChannel, error) {
	if !validChannelAlias(snapshot.Alias) || !model.ChannelStatus(snapshot.Membership).Valid() ||
		snapshot.RosterRevision == 0 || !validStatusChannelTopic(snapshot) ||
		!validStatusChannelCardinality(snapshot) {
		return StatusChannel{}, errors.New("local API: status Channel progress is invalid")
	}
	semantic := StatusChannelSemantic{Originated: snapshot.LocalCommit.Accepted,
		Pending: snapshot.Inbox.Pending, Accepted: snapshot.Inbox.Accepted,
		Rejected: snapshot.Inbox.Rejected, Conflicted: snapshot.Inbox.Conflicted,
		Ignored: snapshot.Inbox.Ignored, Quarantined: snapshot.Inbox.Quarantined}
	snapshot.Artifact.WaitingInbox = snapshot.Inbox.WaitingArtifact
	return StatusChannel{Alias: snapshot.Alias, Artifact: snapshot.Artifact, Cursor: snapshot.Cursor,
		Inbox: snapshot.Inbox, LocalCommit: snapshot.LocalCommit, Membership: snapshot.Membership,
		Publication: snapshot.Publication, RosterRevision: snapshot.RosterRevision,
		Runtime: snapshot.Runtime, Semantic: semantic, State: statusChannelState(snapshot),
		Topic: snapshot.Topic}, nil
}

func validStatusChannelTopic(snapshot StatusChannelSnapshot) bool {
	topic := snapshot.Topic
	if topic.TotalMembers == 0 || topic.TotalMembers > model.MaxMembersPerChannel ||
		topic.ReadyMembers > topic.TotalMembers || topic.UnreachableMembers >= topic.TotalMembers {
		return false
	}
	status := model.ChannelStatus(snapshot.Membership)
	switch topic.State {
	case "joined", "converging":
		return status == model.ChannelActive
	case "blocked":
		return status == model.ChannelConflicted
	case "left":
		return status != model.ChannelActive
	default:
		return false
	}
}

func validStatusChannelCardinality(snapshot StatusChannelSnapshot) bool {
	publication := snapshot.Publication
	cursor, inbox := snapshot.Cursor, snapshot.Inbox
	return snapshot.LocalCommit.Accepted == publication.Queued+publication.Published+publication.Blocked &&
		cursor.InboundOrigins == cursor.InboundCaughtUp+cursor.InboundPending+cursor.InboundTerminal &&
		cursor.OutboundPeers == cursor.OutboundAcknowledged+cursor.OutboundPending &&
		inbox.Durable == inbox.Pending+inbox.Accepted+inbox.Rejected+inbox.Conflicted+
			inbox.Ignored+inbox.Quarantined && inbox.WaitingArtifact <= inbox.Pending &&
		snapshot.Artifact.VerifiedRoots <= snapshot.Artifact.PinnedRoots
}

func statusChannelState(snapshot StatusChannelSnapshot) string {
	status := model.ChannelStatus(snapshot.Membership)
	if status == model.ChannelLeft || status == model.ChannelClosed {
		return statusChannelTerminal
	}
	if statusChannelIsDegraded(snapshot) {
		return statusChannelDegraded
	}
	if statusChannelIsQueued(snapshot) {
		return statusChannelQueued
	}
	return statusChannelReady
}

func statusChannelIsDegraded(snapshot StatusChannelSnapshot) bool {
	status := model.ChannelStatus(snapshot.Membership)
	return status == model.ChannelConflicted || status == model.ChannelAbandoned ||
		snapshot.Topic.State == "blocked" || snapshot.Publication.Blocked > 0 ||
		snapshot.Publication.RemoteBlocked > 0 ||
		snapshot.Cursor.InboundTerminal > 0 || snapshot.Inbox.Conflicted > 0 ||
		snapshot.Inbox.Quarantined > 0 || snapshot.Runtime.HandlingDead > 0 ||
		snapshot.Runtime.RunFailed > 0
}

func statusChannelIsQueued(snapshot StatusChannelSnapshot) bool {
	return model.ChannelStatus(snapshot.Membership) == model.ChannelLeaving ||
		snapshot.Topic.State != "joined" || snapshot.Topic.UnreachableMembers > 0 ||
		snapshot.Publication.Queued > 0 || snapshot.Publication.RemotePending > 0 ||
		snapshot.Cursor.InboundPending > 0 || snapshot.Cursor.InboundGapped > 0 ||
		snapshot.Cursor.OutboundPending > 0 || snapshot.Inbox.Pending > 0 ||
		snapshot.Artifact.VerifiedRoots < snapshot.Artifact.PinnedRoots ||
		snapshot.Runtime.HandlingPending > 0 || snapshot.Runtime.HandlingClaimed > 0 ||
		snapshot.Runtime.RunActive > 0 || snapshot.Runtime.RunRetry > 0
}

func statusChannelsExit(channels []StatusChannel) int {
	exit := 0
	for _, channel := range channels {
		switch channel.State {
		case statusChannelDegraded:
			return CodeInternal.ExitStatus()
		case statusChannelQueued:
			exit = CodeMnemondUnavailable.ExitStatus()
		}
	}
	return exit
}

func statusChannelSnapshots(channels []StatusChannel) []StatusChannelSnapshot {
	result := make([]StatusChannelSnapshot, len(channels))
	for index, channel := range channels {
		artifact := channel.Artifact
		artifact.WaitingInbox = 0
		result[index] = StatusChannelSnapshot{Alias: channel.Alias, Membership: channel.Membership,
			RosterRevision: channel.RosterRevision, Topic: channel.Topic, LocalCommit: channel.LocalCommit,
			Publication: channel.Publication, Cursor: channel.Cursor, Inbox: channel.Inbox,
			Artifact: artifact, Runtime: channel.Runtime}
	}
	return result
}

// ValidateStatusChannels re-closes an already decoded public projection. It is
// shared by status and doctor so the two public commands cannot drift into
// independent Channel state vocabularies.
func ValidateStatusChannels(channels []StatusChannel) error {
	want, err := newStatusChannels(statusChannelSnapshots(channels))
	if err != nil || !reflect.DeepEqual(want, channels) {
		return errors.New("local API: status Channels are not a closed observation")
	}
	return nil
}
