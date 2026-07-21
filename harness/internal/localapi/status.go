package localapi

import "github.com/mnemon-dev/mnemon/harness/internal/node"

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

func NewStatusResponse(snapshot StatusSnapshot) (StatusResponse, error) {
	return node.NewStatusResponse(snapshot)
}

func validateStatusResponse(response StatusResponse) *APIError {
	return node.ValidateStatusResponse(response)
}
