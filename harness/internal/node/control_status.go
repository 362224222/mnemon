package node

import "context"

// MaxStatusResponseBytes bounds eight compact Channel aggregates plus the
// closed activation and Runtime checks. Status contains no Event, Peer, queue
// lease, filesystem, payload, or transport identity.
const MaxStatusResponseBytes = 32 << 10

// StatusProvider observes the current controller authorities without
// creating work, probing claims, settling leases, or modifying durable state.
type StatusProvider interface {
	Status(context.Context, RequestMetadata) (StatusSnapshot, *APIError)
}

// StatusProviderFunc adapts one controller-owned readonly observation.
type StatusProviderFunc func(context.Context, RequestMetadata) (StatusSnapshot, *APIError)

func (provider StatusProviderFunc) Status(ctx context.Context,
	metadata RequestMetadata,
) (StatusSnapshot, *APIError) {
	if provider == nil {
		return StatusSnapshot{}, NewAPIError(CodeInternal, "status provider is unavailable")
	}
	return provider(ctx, metadata)
}

// RuntimeStatusSnapshot is the identity-free portion of a managed worker
// snapshot. Issue may contain an internal worker value; the local API admits
// only a fixed public allowlist and replaces everything else.
type RuntimeStatusSnapshot struct {
	Running    bool
	Ready      bool
	Healthy    bool
	Recovering bool
	Issue      string
}

// StatusSnapshot combines the exact activation observation and the live
// managed Runtime worker. AssetRevision is the controller-bound canonical
// revision, never a caller-provided value.
type StatusSnapshot struct {
	AssetRevision   string
	ActivationReady bool
	ActivationIssue string
	Runtime         RuntimeStatusSnapshot
	Channels        []StatusChannelSnapshot
}

// StatusChannelSnapshot is controller input assembled from one durable
// ChannelObservation and one adjacent topic-session observation. It has no
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
	Leave          StatusChannelLeave
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

// StatusChannelLeave is the bounded public projection of the local member's
// durable leave request. A failed request is recovered only by an explicit
// `channel leave [alias]` invocation, represented by recovery=channel_leave.
type StatusChannelLeave struct {
	Attempts   uint64 `json:"attempts"`
	Diagnostic string `json:"diagnostic"`
	Recovery   string `json:"recovery"`
	Status     string `json:"status"`
}
