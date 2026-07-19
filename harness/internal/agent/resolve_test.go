package agent

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestValidateResolveClosedDecisions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input    ResolveInput
		wantKind model.OperationKind
		wantCode ControlErrorCode
	}{
		{ResolveInput{Decision: "no-action", HasContext: true}, model.OperationResolveNoAction, ""},
		{ResolveInput{Decision: "retry", HasContext: true, Content: "try later"}, model.OperationResolveRetry, ""},
		{ResolveInput{Decision: "reject", HasContext: true, Content: "not actionable"}, model.OperationResolveReject, ""},
		{ResolveInput{Decision: "retry"}, "", CodeContextRequired},
		{ResolveInput{Decision: "reject", HasContext: true}, "", CodeContentRequired},
		{ResolveInput{Decision: "complete", HasContext: true}, "", CodeUnknownAction},
	}
	for _, test := range tests {
		result, apiErr := ValidateResolve(test.input)
		if test.wantCode != "" {
			if apiErr == nil || apiErr.Code != test.wantCode {
				t.Errorf("ValidateResolve(%#v) error = %#v", test.input, apiErr)
			}
			continue
		}
		if apiErr != nil || result.Kind != test.wantKind {
			t.Errorf("ValidateResolve(%#v) = %#v, %#v", test.input, result, apiErr)
		}
	}
}

func TestValidateResolveContentBounds(t *testing.T) {
	t.Parallel()
	large := make([]byte, 8193)
	for index := range large {
		large[index] = 'x'
	}
	for _, input := range []ResolveInput{
		{Decision: "retry", HasContext: true, Content: string([]byte{0xff})},
		{Decision: "retry", HasContext: true, Content: string(large)},
		{Decision: "retry", HasContext: true, Content: "bad\x00reason"},
	} {
		if _, apiErr := ValidateResolve(input); apiErr == nil {
			t.Errorf("invalid resolution content was accepted: %#v", input)
		}
	}
}
