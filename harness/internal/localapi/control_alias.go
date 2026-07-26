package localapi

import "github.com/mnemon-dev/mnemon/harness/internal/node"

const (
	SchemaVersion       = node.SchemaVersion
	MaxRequestBodyBytes = node.MaxRequestBodyBytes
	MaxDiagnosticBytes  = node.MaxDiagnosticBytes
)

type ErrorCode = node.ErrorCode

const (
	CodeInvalidArgument        = node.CodeInvalidArgument
	CodeContentRequired        = node.CodeContentRequired
	CodeContentTooLarge        = node.CodeContentTooLarge
	CodeArtifactInvalid        = node.CodeArtifactInvalid
	CodeArtifactTooLarge       = node.CodeArtifactTooLarge
	CodeAmbiguousChannel       = node.CodeAmbiguousChannel
	CodeAmbiguousParticipant   = node.CodeAmbiguousParticipant
	CodeUnknownAction          = node.CodeUnknownAction
	CodeAuthenticationFailed   = node.CodeAuthenticationFailed
	CodeContextRequired        = node.CodeContextRequired
	CodeContextInvalid         = node.CodeContextInvalid
	CodeContextStale           = node.CodeContextStale
	CodeAssetRevisionMismatch  = node.CodeAssetRevisionMismatch
	CodeActionNotAllowed       = node.CodeActionNotAllowed
	CodeCurrentTooLarge        = node.CodeCurrentTooLarge
	CodeOperationMismatch      = node.CodeOperationMismatch
	CodeWorkConflict           = node.CodeWorkConflict
	CodeWorkExpired            = node.CodeWorkExpired
	CodeProfileHostMismatch    = node.CodeProfileHostMismatch
	CodeHostActivationRequired = node.CodeHostActivationRequired
	CodeOperationPending       = node.CodeOperationPending
	CodePeerUnavailable        = node.CodePeerUnavailable
	CodeMnemondUnavailable     = node.CodeMnemondUnavailable
	CodeOwnerUnreachable       = node.CodeOwnerUnreachable
	CodeBusy                   = node.CodeBusy
	CodeInvalidToken           = node.CodeInvalidToken
	CodeWrongOwner             = node.CodeWrongOwner
	CodeTokenExpired           = node.CodeTokenExpired
	CodeTokenClosed            = node.CodeTokenClosed
	CodeTokenExhausted         = node.CodeTokenExhausted
	CodeChannelFull            = node.CodeChannelFull
	CodeNodeChannelLimit       = node.CodeNodeChannelLimit
	CodeBadProof               = node.CodeBadProof
	CodeIncompatibleProtocol   = node.CodeIncompatibleProtocol
	CodeNotMember              = node.CodeNotMember
	CodeMemberRevoked          = node.CodeMemberRevoked
	CodeChannelClosed          = node.CodeChannelClosed
	CodeBaselineConflict       = node.CodeBaselineConflict
	CodeOriginEpochMismatch    = node.CodeOriginEpochMismatch
	CodeRosterGap              = node.CodeRosterGap
	CodeRosterConflict         = node.CodeRosterConflict
	CodeInternal               = node.CodeInternal
)

type APIError = node.APIError
type Authenticator = node.Authenticator
type RequestMetadata = node.RequestMetadata
type Service = node.Service

const MaxAuthorityResponseBytes = node.MaxAuthorityResponseBytes

type AuthorityProvider = node.AuthorityProvider
type AuthorityProviderFunc = node.AuthorityProviderFunc
type AuthoritySnapshot = node.AuthoritySnapshot
type AuthorityResponse = node.AuthorityResponse

const MaxHealthResponseBytes = node.MaxHealthResponseBytes

type HealthProvider = node.HealthProvider
type HealthProviderFunc = node.HealthProviderFunc
type HealthSnapshot = node.HealthSnapshot
type HealthResponse = node.HealthResponse

const MaxStatusResponseBytes = node.MaxStatusResponseBytes

type StatusProvider = node.StatusProvider
type StatusProviderFunc = node.StatusProviderFunc
type StatusCheck = node.StatusCheck
type StatusResponse = node.StatusResponse
type StatusChannelSemantic = node.StatusChannelSemantic
type StatusChannel = node.StatusChannel
type RuntimeStatusSnapshot = node.RuntimeStatusSnapshot
type StatusSnapshot = node.StatusSnapshot
type StatusChannelSnapshot = node.StatusChannelSnapshot
type StatusChannelTopic = node.StatusChannelTopic
type StatusChannelCommit = node.StatusChannelCommit
type StatusChannelPublication = node.StatusChannelPublication
type StatusChannelCursor = node.StatusChannelCursor
type StatusChannelInbox = node.StatusChannelInbox
type StatusChannelArtifact = node.StatusChannelArtifact
type StatusChannelRuntime = node.StatusChannelRuntime
type StatusChannelLeave = node.StatusChannelLeave

const MaxChannelResponseBytes = node.MaxChannelResponseBytes

type ChannelCreateRequest = node.ChannelCreateRequest
type ChannelJoinRequest = node.ChannelJoinRequest
type ChannelInviteRequest = node.ChannelInviteRequest
type ChannelInviteCloseRequest = node.ChannelInviteCloseRequest
type ChannelRemoveRequest = node.ChannelRemoveRequest
type ChannelLeaveRequest = node.ChannelLeaveRequest
type ChannelAbandonRequest = node.ChannelAbandonRequest
type ChannelTopicView = node.ChannelTopicView
type ChannelOwnerView = node.ChannelOwnerView
type ChannelMemberView = node.ChannelMemberView
type ChannelInviteView = node.ChannelInviteView
type ChannelRosterHeadView = node.ChannelRosterHeadView
type ChannelView = node.ChannelView
type ChannelCreateResponse = node.ChannelCreateResponse
type ChannelJoinResponse = node.ChannelJoinResponse
type ChannelInviteResponse = node.ChannelInviteResponse
type ChannelInviteCloseResponse = node.ChannelInviteCloseResponse
type ChannelRemoveResponse = node.ChannelRemoveResponse
type ChannelLeaveResponse = node.ChannelLeaveResponse
type ChannelForensicCounts = node.ChannelForensicCounts
type ChannelAbandonResponse = node.ChannelAbandonResponse
type ChannelStatusResponse = node.ChannelStatusResponse
type ChannelService = node.ChannelService

const MaxShutdownResponseBytes = node.MaxShutdownResponseBytes

type LifecycleFunc = node.LifecycleFunc
type AdmissionReleaseFunc = node.AdmissionReleaseFunc
type MutationShutdownPreparer = node.MutationShutdownPreparer
type MutationShutdownPreparerFunc = node.MutationShutdownPreparerFunc
type ShutdownResponse = node.ShutdownResponse

type HookCheckRequest = node.HookCheckRequest
type HookCheckResponse = node.HookCheckResponse
type AgentCurrentRequest = node.AgentCurrentRequest
type AgentCurrentResponse = node.AgentCurrentResponse
type TeamworkActionRequest = node.TeamworkActionRequest
type AgentResolveRequest = node.AgentResolveRequest
type OperationResult = node.OperationResult
type WorkReceipt = node.WorkReceipt
type HandlingReceipt = node.HandlingReceipt
type OperationResponse = node.OperationResponse

type InitiationParticipant = node.InitiationParticipant
type InitiationChannel = node.InitiationChannel
type InitiationProjection = node.InitiationProjection

func NewAPIError(code ErrorCode, message string) *APIError {
	return node.NewAPIError(code, message)
}
