package localapi

import "testing"

func TestControlAliasUsesNodeErrorSemantics(t *testing.T) {
	err := NewAPIError(CodeMnemondUnavailable, "down")
	if err.Code != CodeMnemondUnavailable || !err.Retryable || err.ExitStatus() != 5 {
		t.Fatalf("control alias error semantics drifted: %#v", err)
	}
}
