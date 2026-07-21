package node

import "context"

const MaxChannelResponseBytes = 1 << 20

type ChannelCreateRequest struct {
	Name string `json:"name,omitempty"`
}

type ChannelJoinRequest struct {
	Token string `json:"token"`
}

type ChannelInviteRequest struct {
	Channel        string `json:"channel,omitempty"`
	ExpiresSeconds int64  `json:"expires_seconds,omitempty"`
	Uses           uint8  `json:"uses,omitempty"`
}

type ChannelInviteCloseRequest struct {
	Channel string `json:"channel,omitempty"`
}

type ChannelRemoveRequest struct {
	Channel string `json:"channel,omitempty"`
	Member  string `json:"member"`
}

type ChannelLeaveRequest struct {
	Channel string `json:"channel,omitempty"`
}

type ChannelAbandonRequest struct {
	Channel        string `json:"channel"`
	ConfirmChannel string `json:"confirm_channel"`
	Force          bool   `json:"force"`
}

type ChannelReplayProbeRequest struct {
	SourceChannel string `json:"source_channel"`
	TargetChannel string `json:"target_channel"`
}

type ChannelTopicView struct {
	ReadyMembers uint8  `json:"ready_members"`
	Status       string `json:"status"`
	TotalMembers uint8  `json:"total_members"`
}

type ChannelOwnerView struct {
	Local        bool   `json:"local"`
	Reachability string `json:"reachability"`
}

type ChannelMemberView struct {
	Alias         string `json:"alias"`
	BaselineReady bool   `json:"baseline_ready"`
	Binding       string `json:"binding"`
	PeerID        string `json:"peer_id"`
	Reachability  string `json:"reachability"`
	Status        string `json:"status"`
}

type ChannelInviteView struct {
	ExpiresAt     string `json:"expires_at"`
	RemainingUses uint8  `json:"remaining_uses"`
	Status        string `json:"status"`
}

type ChannelRosterHeadView struct {
	Digest         string `json:"digest"`
	OwnerPeerID    string `json:"owner_peer_id"`
	OwnerSignature string `json:"owner_signature"`
	Revision       uint64 `json:"revision"`
}

// ChannelPublicationRefView is deliberately structured: Channel, Peer and
// epoch identifiers may themselves contain separators, so concatenation is
// not an unambiguous public reference.
type ChannelPublicationRefView struct {
	ChannelSequence uint64 `json:"channel_sequence"`
	OriginEpoch     string `json:"origin_epoch"`
	OriginPeerID    string `json:"origin_peer_id"`
}

type ChannelEventKeyView struct {
	EventID      string `json:"event_id"`
	OriginEpoch  string `json:"origin_epoch"`
	OriginPeerID string `json:"origin_peer_id"`
}

type ChannelPublicationView struct {
	Arrival                    string                    `json:"arrival"`
	ArtifactDirectSourcePeerID *string                   `json:"artifact_direct_source_peer_id"`
	AudiencePeerIDs            []string                  `json:"audience_peer_ids"`
	CausalityEventKey          *ChannelEventKeyView      `json:"causality_event_key"`
	ChannelIDDigest            string                    `json:"channel_id_digest"`
	EventDigest                string                    `json:"event_digest"`
	EventKey                   ChannelEventKeyView       `json:"event_key"`
	IgnoredPeerIDs             []string                  `json:"ignored_peer_ids"`
	ImmediateTransportPeerID   string                    `json:"immediate_transport_peer_id"`
	OriginPeerID               string                    `json:"origin_peer_id"`
	PublicationDigest          string                    `json:"publication_digest"`
	PublicationRef             ChannelPublicationRefView `json:"publication_ref"`
	SemanticOutcome            string                    `json:"semantic_outcome"`
}

type ChannelView struct {
	Alias           string                   `json:"alias"`
	ChannelIDDigest string                   `json:"channel_id_digest"`
	Invite          *ChannelInviteView       `json:"invite,omitempty"`
	Members         []ChannelMemberView      `json:"members"`
	Membership      string                   `json:"membership"`
	Name            string                   `json:"name"`
	Owner           ChannelOwnerView         `json:"owner"`
	Publications    []ChannelPublicationView `json:"publications"`
	RosterHead      ChannelRosterHeadView    `json:"roster_head"`
	RosterRevision  uint64                   `json:"roster_revision"`
	Topic           ChannelTopicView         `json:"topic"`
}

type ChannelCreateResponse struct {
	Channel       ChannelView       `json:"channel"`
	Invite        ChannelInviteView `json:"invite"`
	InviteToken   string            `json:"invite_token"`
	SchemaVersion int               `json:"schema_version"`
	Status        string            `json:"status"`
}

type ChannelJoinResponse struct {
	Channel       ChannelView `json:"channel"`
	SchemaVersion int         `json:"schema_version"`
	Status        string      `json:"status"`
}

type ChannelInviteResponse struct {
	Channel       ChannelView       `json:"channel"`
	Invite        ChannelInviteView `json:"invite"`
	InviteToken   string            `json:"invite_token"`
	SchemaVersion int               `json:"schema_version"`
	Status        string            `json:"status"`
}

type ChannelInviteCloseResponse struct {
	Channel       ChannelView `json:"channel"`
	SchemaVersion int         `json:"schema_version"`
	Status        string      `json:"status"`
}

type ChannelRemoveResponse struct {
	Channel       ChannelView `json:"channel"`
	SchemaVersion int         `json:"schema_version"`
	Status        string      `json:"status"`
}

type ChannelLeaveResponse struct {
	Channel       ChannelView `json:"channel"`
	SchemaVersion int         `json:"schema_version"`
	Status        string      `json:"status"`
}

type ChannelForensicCounts struct {
	Bindings      uint64 `json:"bindings"`
	Conflicts     uint64 `json:"conflicts"`
	Cursors       uint64 `json:"cursors"`
	Deliveries    uint64 `json:"deliveries"`
	Events        uint64 `json:"events"`
	Inboxes       uint64 `json:"inboxes"`
	MemberRecords uint64 `json:"member_records"`
	Publications  uint64 `json:"publications"`
	PullACKs      uint64 `json:"pull_acks"`
	Works         uint64 `json:"works"`
}

type ChannelMutationCounts struct {
	Events uint64 `json:"events"`
	Works  uint64 `json:"works"`
}

type ChannelAbandonResponse struct {
	Channel        string                `json:"channel"`
	Evidence       ChannelForensicCounts `json:"evidence"`
	Replayed       bool                  `json:"replayed"`
	SchemaVersion  int                   `json:"schema_version"`
	Status         string                `json:"status"`
	TransitionedAt string                `json:"transitioned_at"`
}

type ChannelReplayProbeResponse struct {
	EventDigest              string                `json:"event_digest"`
	EventKey                 ChannelEventKeyView   `json:"event_key"`
	PublicationDigest        string                `json:"publication_digest"`
	Rejection                string                `json:"rejection"`
	ReplayAttempted          bool                  `json:"replay_attempted"`
	SchemaVersion            int                   `json:"schema_version"`
	SourceChannel            string                `json:"source_channel"`
	SourceChannelIDDigest    string                `json:"source_channel_id_digest"`
	Status                   string                `json:"status"`
	TargetAfter              ChannelMutationCounts `json:"target_after"`
	TargetBefore             ChannelMutationCounts `json:"target_before"`
	TargetChannel            string                `json:"target_channel"`
	TargetChannelIDDigest    string                `json:"target_channel_id_digest"`
	TargetMutationSuppressed bool                  `json:"target_mutation_suppressed"`
}

type ChannelStatusResponse struct {
	Channels      []ChannelView `json:"channels"`
	SchemaVersion int           `json:"schema_version"`
	Status        string        `json:"status"`
}

type ChannelService interface {
	ChannelCreate(context.Context, RequestMetadata, ChannelCreateRequest) (ChannelCreateResponse, *APIError)
	ChannelJoin(context.Context, RequestMetadata, ChannelJoinRequest) (ChannelJoinResponse, *APIError)
	ChannelInvite(context.Context, RequestMetadata, ChannelInviteRequest) (ChannelInviteResponse, *APIError)
	ChannelInviteClose(context.Context, RequestMetadata,
		ChannelInviteCloseRequest) (ChannelInviteCloseResponse, *APIError)
	ChannelRemove(context.Context, RequestMetadata, ChannelRemoveRequest) (ChannelRemoveResponse, *APIError)
	ChannelLeave(context.Context, RequestMetadata, ChannelLeaveRequest) (ChannelLeaveResponse, *APIError)
	ChannelAbandon(context.Context, RequestMetadata, ChannelAbandonRequest) (ChannelAbandonResponse, *APIError)
	ChannelReplayProbe(context.Context, RequestMetadata,
		ChannelReplayProbeRequest) (ChannelReplayProbeResponse, *APIError)
	ChannelStatus(context.Context, RequestMetadata) (ChannelStatusResponse, *APIError)
}
