package cli

import (
	agentcontrol "github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
)

// localAPIValidationError translates only the closed set of errors emitted by
// the CLI's local Agent validation pass. Remote and execution errors must cross
// their own protocol boundaries instead of entering through this mapper.
func localAPIValidationError(controlErr *agentcontrol.ControlError) *localapi.APIError {
	if controlErr == nil {
		return nil
	}
	if controlErr.Retryable || controlErr.Replayed || controlErr.OperationID != nil {
		return localapi.NewAPIError(localapi.CodeInternal, "internal control error")
	}
	var code localapi.ErrorCode
	switch controlErr.Code {
	case agentcontrol.CodeInvalidArgument:
		code = localapi.CodeInvalidArgument
	case agentcontrol.CodeContentRequired:
		code = localapi.CodeContentRequired
	case agentcontrol.CodeContentTooLarge:
		code = localapi.CodeContentTooLarge
	case agentcontrol.CodeArtifactInvalid:
		code = localapi.CodeArtifactInvalid
	case agentcontrol.CodeArtifactTooLarge:
		code = localapi.CodeArtifactTooLarge
	case agentcontrol.CodeUnknownAction:
		code = localapi.CodeUnknownAction
	case agentcontrol.CodeContextRequired:
		code = localapi.CodeContextRequired
	case agentcontrol.CodeInternal:
		code = localapi.CodeInternal
	default:
		return localapi.NewAPIError(localapi.CodeInternal, "internal control error")
	}
	mapped := localapi.NewAPIError(code, controlErr.Message)
	if mapped.Code != code || mapped.Message != controlErr.Message {
		return localapi.NewAPIError(localapi.CodeInternal, "internal control error")
	}
	return mapped
}
