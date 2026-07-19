package agent

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestControlErrorCodesAreClosedUniqueAndDefensive(t *testing.T) {
	codes := ControlErrorCodes()
	seen := make(map[ControlErrorCode]struct{}, len(codes))
	for _, code := range codes {
		if !code.Valid() {
			t.Fatalf("registered Control error code %q is invalid", code)
		}
		if _, duplicate := seen[code]; duplicate {
			t.Fatalf("duplicate Control error code %q", code)
		}
		seen[code] = struct{}{}
	}
	if len(codes) != 23 || ControlErrorCode("memory.write").Valid() {
		t.Fatalf("Control error closed set = %#v", codes)
	}
	codes[0] = "changed"
	if ControlErrorCodes()[0] == "changed" {
		t.Fatal("ControlErrorCodes exposed mutable authority")
	}
}

func TestControlErrorIsBoundedAndDerivesRetryability(t *testing.T) {
	pending := NewControlError(CodeOperationPending, " operation is still active ")
	if pending.Code != CodeOperationPending || pending.Message != "operation is still active" ||
		!pending.Retryable || pending.Error() != "operation_pending: operation is still active" {
		t.Fatalf("pending Control error = %#v", pending)
	}
	invalid := NewControlError(ControlErrorCode("unknown"),
		strings.Repeat("x", MaxControlDiagnosticBytes+1))
	if invalid.Code != CodeInternal || invalid.Message != "internal control error" || invalid.Retryable {
		t.Fatalf("invalid Control error = %#v", invalid)
	}
}

func TestControlDTOJSONShapePreservesRequestAndResponseNullability(t *testing.T) {
	request, err := json.Marshal(TeamworkActionRequest{Action: "offer", Channel: "alpha",
		To: "reviewer", Deadline: "24h", Content: "review", Artifacts: []string{"a.md"}})
	if err != nil || string(request) !=
		`{"action":"offer","channel":"alpha","to":"reviewer","deadline":"24h","content":"review","artifacts":["a.md"]}` {
		t.Fatalf("Teamwork request JSON = %s, %v", request, err)
	}
	response, err := json.Marshal(OperationResponse{Results: []OperationResult{}})
	if err != nil || !strings.Contains(string(response), `"handling":null`) ||
		!strings.Contains(string(response), `"results":[]`) {
		t.Fatalf("Operation response JSON = %s, %v", response, err)
	}
	nilResponse, err := json.Marshal(OperationResponse{})
	if err != nil || !strings.Contains(string(nilResponse), `"results":null`) {
		t.Fatalf("nil Operation results JSON = %s, %v", nilResponse, err)
	}
}
