package node

import (
	"context"
	"errors"

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const controllerControlConnectionLimit uint32 = 32

// ErrControlTransportUndrained means transport cancellation could not prove
// that every admitted handler released its Node dependencies. The owner must
// retain Store authority rather than closing state underneath an unowned
// goroutine; the daemon process can then exit and let the OS reclaim it.
var ErrControlTransportUndrained = errors.New("local control transport did not drain admitted handlers")

// Wake attachment aliases expose the Agent-owned crash-ordering contract at
// Node's composition boundary without requiring transport adapters to depend
// directly on Agent implementation details.
type WakeAttachmentCandidate = agent.WakeAttachmentCandidate
type WakeAttachmentFilesystem = agent.WakeAttachmentFilesystem
type StagedRunAttachment = agent.StagedRunAttachment
type RunAttachment = agent.RunAttachment

type ControlMetadata = agent.ControlMetadata
type HookCheckResponse = agent.HookCheckResponse
type AgentCurrentResponse = agent.AgentCurrentResponse
type TeamworkActionRequest = agent.TeamworkActionRequest
type AgentResolveRequest = agent.AgentResolveRequest
type OperationResult = agent.OperationResult
type WorkReceipt = agent.WorkReceipt
type HandlingReceipt = agent.HandlingReceipt
type OperationResponse = agent.OperationResponse
type ControlError = agent.ControlError
type ControlErrorCode = agent.ControlErrorCode

// ControlErrorCodes forwards the Agent-owned immutable vocabulary so every
// transport adapter can prove exhaustive mapping without maintaining a second
// closed-set registry.
func ControlErrorCodes() []ControlErrorCode { return agent.ControlErrorCodes() }

const (
	ControlCodeInvalidArgument       = agent.CodeInvalidArgument
	ControlCodeContentRequired       = agent.CodeContentRequired
	ControlCodeContentTooLarge       = agent.CodeContentTooLarge
	ControlCodeArtifactInvalid       = agent.CodeArtifactInvalid
	ControlCodeArtifactTooLarge      = agent.CodeArtifactTooLarge
	ControlCodeAmbiguousChannel      = agent.CodeAmbiguousChannel
	ControlCodeAmbiguousParticipant  = agent.CodeAmbiguousParticipant
	ControlCodeUnknownAction         = agent.CodeUnknownAction
	ControlCodeAuthenticationFailed  = agent.CodeAuthenticationFailed
	ControlCodeContextRequired       = agent.CodeContextRequired
	ControlCodeContextInvalid        = agent.CodeContextInvalid
	ControlCodeContextStale          = agent.CodeContextStale
	ControlCodeAssetRevisionMismatch = agent.CodeAssetRevisionMismatch
	ControlCodeActionNotAllowed      = agent.CodeActionNotAllowed
	ControlCodeCurrentTooLarge       = agent.CodeCurrentTooLarge
	ControlCodeOperationMismatch     = agent.CodeOperationMismatch
	ControlCodeWorkConflict          = agent.CodeWorkConflict
	ControlCodeWorkExpired           = agent.CodeWorkExpired
	ControlCodeProfileHostMismatch   = agent.CodeProfileHostMismatch
	ControlCodeOperationPending      = agent.CodeOperationPending
	ControlCodePeerUnavailable       = agent.CodePeerUnavailable
	ControlCodeMnemondUnavailable    = agent.CodeMnemondUnavailable
	ControlCodeInternal              = agent.CodeInternal
)

type ManagedControlService = agent.ControlService

type ProfileAuthenticator interface {
	AuthenticateProfile(context.Context, model.Digest) (model.Profile, error)
}

type RuntimeStatus struct {
	Running    bool
	Ready      bool
	Healthy    bool
	Recovering bool
	Issue      string
}

type ControlStatus struct {
	AssetRevision   string
	ActivationReady bool
	ActivationIssue string
	Runtime         RuntimeStatus
}

type ControlObserver interface {
	ObserveControlHealth(context.Context, model.Profile) (DaemonHealth, *ControlError)
	ObserveControlStatus(context.Context, model.Profile) (ControlStatus, *ControlError)
	ObserveControlAuthority(context.Context, model.Profile) (Authority, *ControlError)
}

type MutationShutdownController interface {
	PrepareMutationShutdown(context.Context, model.Profile) (Authority, func(), *ControlError)
}

type ControlBindings struct {
	Authenticator ProfileAuthenticator
	Agent         ManagedControlService
	Observer      ControlObserver
	Mutation      MutationShutdownController
	Shutdown      func()
}

type ControlTransportOptions struct {
	NodeState      string
	AssetRevision  string
	MaxConnections uint32
}

// ControlTransportFactory prepares the owner-only local transport without
// starting Accept. Node releases its inherited launch permit only after this
// dormant bind succeeds.
type ControlTransportFactory interface {
	Prepare(context.Context, ControlTransportOptions, ControlBindings) (PreparedControlTransport, error)
}

type ControlTransportFactoryFunc func(context.Context, ControlTransportOptions,
	ControlBindings,
) (PreparedControlTransport, error)

func (factory ControlTransportFactoryFunc) Prepare(ctx context.Context,
	options ControlTransportOptions, bindings ControlBindings,
) (PreparedControlTransport, error) {
	if factory == nil {
		return nil, errors.New("local control transport factory is unavailable")
	}
	return factory(ctx, options, bindings)
}

// PreparedControlTransport owns one dormant bound listener. Run may begin
// accepting only after Prepare returns to Node. Shutdown drains admitted
// requests; Close is the idempotent final resource release for every path.
// Shutdown must return ErrControlTransportUndrained when bounded cancellation
// cannot prove handler exit.
type PreparedControlTransport interface {
	Run(context.Context) error
	Readiness(context.Context) error
	Shutdown(context.Context) error
	Close() error
}

var _ ManagedControlService = (*agent.Service)(nil)
