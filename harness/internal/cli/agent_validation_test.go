package cli

import (
	"testing"

	agentcontrol "github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
)

func TestLocalAPIValidationErrorMapsClosedValidationVocabulary(t *testing.T) {
	t.Parallel()
	tests := []struct {
		domain    agentcontrol.ControlErrorCode
		transport localapi.ErrorCode
	}{
		{agentcontrol.CodeInvalidArgument, localapi.CodeInvalidArgument},
		{agentcontrol.CodeContentRequired, localapi.CodeContentRequired},
		{agentcontrol.CodeContentTooLarge, localapi.CodeContentTooLarge},
		{agentcontrol.CodeArtifactInvalid, localapi.CodeArtifactInvalid},
		{agentcontrol.CodeArtifactTooLarge, localapi.CodeArtifactTooLarge},
		{agentcontrol.CodeUnknownAction, localapi.CodeUnknownAction},
		{agentcontrol.CodeContextRequired, localapi.CodeContextRequired},
		{agentcontrol.CodeInternal, localapi.CodeInternal},
	}
	for _, test := range tests {
		controlErr := agentcontrol.NewControlError(test.domain, "bounded validation error")
		mapped := localAPIValidationError(controlErr)
		if mapped == nil || mapped.Code != test.transport || mapped.Message != controlErr.Message ||
			mapped.Retryable || mapped.Replayed || mapped.OperationID != nil {
			t.Fatalf("localAPIValidationError(%q) = %#v", test.domain, mapped)
		}
	}
	if localAPIValidationError(nil) != nil {
		t.Fatal("nil validation error did not remain nil")
	}
}

func TestLocalAPIValidationErrorFailsClosedOnNonValidationAuthority(t *testing.T) {
	t.Parallel()
	operationID := "operation-must-not-cross"
	tests := []*agentcontrol.ControlError{
		agentcontrol.NewControlError(agentcontrol.CodeWorkConflict, "not a validation error"),
		{Code: agentcontrol.CodeInvalidArgument, Message: "bad retry", Retryable: true},
		{Code: agentcontrol.CodeInvalidArgument, Message: "bad replay", Replayed: true},
		{Code: agentcontrol.CodeInvalidArgument, Message: "bad operation", OperationID: &operationID},
		{Code: agentcontrol.CodeInvalidArgument},
	}
	for _, controlErr := range tests {
		mapped := localAPIValidationError(controlErr)
		if mapped == nil || mapped.Code != localapi.CodeInternal || mapped.Message != "internal control error" ||
			mapped.Retryable || mapped.Replayed || mapped.OperationID != nil {
			t.Fatalf("malformed validation error = %#v", mapped)
		}
	}
}
