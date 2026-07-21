package node

import "testing"

func TestControlErrorCodesRemainClosedAndBounded(t *testing.T) {
	if !CodeMnemondUnavailable.Valid() || !CodeMnemondUnavailable.Retryable() ||
		CodeMnemondUnavailable.ExitStatus() != 5 {
		t.Fatalf("mnemond unavailable code lost retryable exit semantics")
	}
	err := NewAPIError(ErrorCode("unknown"), "")
	if err.Code != CodeInternal || err.Message != "internal control error" ||
		err.SchemaVersion != SchemaVersion || err.Status != "error" {
		t.Fatalf("malformed API error did not fail closed: %#v", err)
	}
}
