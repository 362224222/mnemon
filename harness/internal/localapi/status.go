package localapi

import (
	"context"
	"errors"
	"reflect"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// MaxStatusResponseBytes bounds eight compact Channel aggregates plus the
// closed activation and Runtime checks. Status contains no Event, Peer, queue
// lease, filesystem, payload, or transport identity.
const MaxStatusResponseBytes = 32 << 10

const (
	statusScopeManagedAgent = "managed_agent"

	statusReady    = "ready"
	statusDegraded = "degraded"

	activationReady  = "ready"
	activationFailed = "failed"

	runtimeStarting   = "starting"
	runtimeRecovering = "recovering"
	runtimeReady      = "ready"
	runtimeRetrying   = "retrying"
	runtimeFailed     = "failed"

	statusIssueNone               = "none"
	statusIssueAssetMismatch      = "asset_revision_mismatch"
	statusIssueAuthorityMismatch  = "durable_authority_mismatch"
	statusIssueInternalActivation = "internal_activation_issue"
	statusIssueInternalRuntime    = "internal_runtime_issue"
	statusIssueActivation         = "activation_or_installation_unavailable"
	statusIssueWakePrepare        = "wake_preparation_unavailable"
	statusIssueWakeAttachment     = "wake_attachment_publish_failed"
	statusIssueManagedRuntime     = "managed_runtime_failed"
	statusIssueRuntimeCallback    = "durable_runtime_callback_failed"
	statusIssueRecoveryLive       = "startup_runtime_still_live"
	statusIssueRecoveryInvalid    = "startup_runtime_evidence_invalid"
	statusIssueDurableRuntime     = "durable_runtime_transition_failed"
	statusIssueRuntimeUnproven    = "managed_runtime_exit_unproven"
	statusIssueRuntimeInvariant   = "managed_runtime_result_invalid"
)

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
// snapshot. Issue may contain an internal worker value; NewStatusResponse
// admits only the fixed public allowlist and replaces everything else.
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

type StatusCheck struct {
	Issue string `json:"issue"`
	State string `json:"state"`
}

type StatusResponse struct {
	Activation    StatusCheck     `json:"activation"`
	AssetRevision string          `json:"asset_revision"`
	Channels      []StatusChannel `json:"channels"`
	Runtime       StatusCheck     `json:"runtime"`
	SchemaVersion int             `json:"schema_version"`
	Scope         string          `json:"scope"`
	Status        string          `json:"status"`
}

// ExitStatus maps one already-validated observation to the existing CLI exit
// classes. A trusted degraded report remains printable even when its exit is
// non-zero.
func (response StatusResponse) ExitStatus() int {
	if validateStatusResponse(response) != nil {
		return CodeInternal.ExitStatus()
	}
	if response.Status == statusReady {
		return 0
	}
	if response.Activation.State == activationFailed {
		return CodeAssetRevisionMismatch.ExitStatus()
	}
	if response.Runtime.State == runtimeStarting || response.Runtime.State == runtimeRecovering ||
		response.Runtime.State == runtimeRetrying {
		return CodeMnemondUnavailable.ExitStatus()
	}
	if exit := statusChannelsExit(response.Channels); exit != 0 {
		return exit
	}
	return CodeInternal.ExitStatus()
}

// NewStatusResponse validates and closes a controller snapshot for the wire.
func NewStatusResponse(snapshot StatusSnapshot) (StatusResponse, error) {
	if _, err := model.ParseDigest(snapshot.AssetRevision); err != nil {
		return StatusResponse{}, errors.New("local API: status asset revision is invalid")
	}
	activation := StatusCheck{Issue: publicActivationIssue(snapshot.ActivationIssue), State: activationFailed}
	if snapshot.ActivationReady {
		if snapshot.ActivationIssue != "" {
			return StatusResponse{}, errors.New("local API: ready activation contains an issue")
		}
		activation = StatusCheck{Issue: statusIssueNone, State: activationReady}
	} else if snapshot.ActivationIssue == "" {
		activation.Issue = statusIssueInternalActivation
	}
	runtime, err := runtimeStatusCheck(snapshot.Runtime)
	if err != nil {
		return StatusResponse{}, err
	}
	channels, err := newStatusChannels(snapshot.Channels)
	if err != nil {
		return StatusResponse{}, err
	}
	state := statusDegraded
	if activation.State == activationReady && runtime.State == runtimeReady && statusChannelsExit(channels) == 0 {
		state = statusReady
	}
	response := StatusResponse{Activation: activation, AssetRevision: snapshot.AssetRevision,
		Channels: channels, Runtime: runtime, SchemaVersion: SchemaVersion,
		Scope: statusScopeManagedAgent, Status: state}
	if raw, err := model.CanonicalMarshal(response); err != nil || len(raw)+1 > MaxStatusResponseBytes {
		return StatusResponse{}, errors.New("local API: status response exceeds its closed bound")
	}
	return response, nil
}

func publicActivationIssue(issue string) string {
	switch issue {
	case "":
		return statusIssueNone
	case statusIssueAssetMismatch, statusIssueAuthorityMismatch:
		return issue
	default:
		return statusIssueInternalActivation
	}
}

func runtimeStatusCheck(snapshot RuntimeStatusSnapshot) (StatusCheck, error) {
	if snapshot.Ready && (!snapshot.Running || !snapshot.Healthy || snapshot.Recovering) ||
		snapshot.Recovering && (!snapshot.Running || !snapshot.Healthy || snapshot.Ready) {
		return StatusCheck{}, errors.New("local API: Runtime worker snapshot is inconsistent")
	}
	issue := publicRuntimeIssue(snapshot.Issue)
	switch {
	case snapshot.Ready:
		if snapshot.Issue != "" {
			return StatusCheck{}, errors.New("local API: ready Runtime worker contains an issue")
		}
		return StatusCheck{Issue: statusIssueNone, State: runtimeReady}, nil
	case !snapshot.Healthy:
		if snapshot.Issue == "" {
			issue = statusIssueInternalRuntime
		}
		return StatusCheck{Issue: issue, State: runtimeFailed}, nil
	case snapshot.Recovering:
		return StatusCheck{Issue: issue, State: runtimeRecovering}, nil
	case snapshot.Running:
		return StatusCheck{Issue: issue, State: runtimeRetrying}, nil
	default:
		return StatusCheck{Issue: issue, State: runtimeStarting}, nil
	}
}

func publicRuntimeIssue(issue string) string {
	switch issue {
	case "":
		return statusIssueNone
	case statusIssueActivation, statusIssueWakePrepare, statusIssueWakeAttachment,
		statusIssueManagedRuntime, statusIssueRuntimeCallback, statusIssueRecoveryLive,
		statusIssueRecoveryInvalid, statusIssueDurableRuntime, statusIssueRuntimeUnproven,
		statusIssueRuntimeInvariant:
		return issue
	default:
		return statusIssueInternalRuntime
	}
}

func validateStatusResponse(response StatusResponse) *APIError {
	if response.SchemaVersion != SchemaVersion || response.Scope != statusScopeManagedAgent ||
		(response.Status != statusReady && response.Status != statusDegraded) ||
		(response.Activation.State != activationReady && response.Activation.State != activationFailed) ||
		(response.Runtime.State != runtimeStarting && response.Runtime.State != runtimeRecovering &&
			response.Runtime.State != runtimeReady && response.Runtime.State != runtimeRetrying &&
			response.Runtime.State != runtimeFailed) {
		return invalidControlResponse("status response has an invalid state")
	}
	if _, err := model.ParseDigest(response.AssetRevision); err != nil {
		return invalidControlResponse("status response has an invalid asset revision")
	}
	want, err := NewStatusResponse(StatusSnapshot{AssetRevision: response.AssetRevision,
		ActivationReady: response.Activation.State == activationReady,
		ActivationIssue: activationIssueFromResponse(response.Activation),
		Runtime:         runtimeSnapshotFromResponse(response.Runtime), Channels: statusChannelSnapshots(response.Channels)})
	if err != nil || !reflect.DeepEqual(want, response) {
		return invalidControlResponse("status response is not a closed observation")
	}
	return nil
}

func activationIssueFromResponse(check StatusCheck) string {
	if check.Issue == statusIssueNone {
		return ""
	}
	return check.Issue
}

func runtimeSnapshotFromResponse(check StatusCheck) RuntimeStatusSnapshot {
	issue := check.Issue
	if issue == statusIssueNone {
		issue = ""
	}
	snapshot := RuntimeStatusSnapshot{Issue: issue}
	switch check.State {
	case runtimeStarting:
		snapshot.Healthy = true
	case runtimeRecovering:
		snapshot.Running, snapshot.Healthy, snapshot.Recovering = true, true, true
	case runtimeReady:
		snapshot.Running, snapshot.Ready, snapshot.Healthy = true, true, true
	case runtimeRetrying:
		snapshot.Running, snapshot.Healthy = true, true
	case runtimeFailed:
		snapshot.Healthy = false
	}
	return snapshot
}
