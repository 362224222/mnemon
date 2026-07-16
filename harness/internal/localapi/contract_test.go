package localapi

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestErrorCodeContract(t *testing.T) {
	t.Parallel()
	tests := []struct {
		code      ErrorCode
		exit      int
		retryable bool
	}{
		{CodeInvalidArgument, 2, false}, {CodeArtifactInvalid, 2, false},
		{CodeAuthenticationFailed, 3, false}, {CodeContextStale, 3, false},
		{CodeActionNotAllowed, 4, false}, {CodeWorkExpired, 4, false},
		{CodeOperationPending, 5, true}, {CodePeerUnavailable, 5, true},
		{CodeInternal, 1, false},
	}
	for _, test := range tests {
		if !test.code.Valid() || test.code.ExitStatus() != test.exit || test.code.Retryable() != test.retryable {
			t.Errorf("code %q = valid %t exit %d retryable %t", test.code, test.code.Valid(),
				test.code.ExitStatus(), test.code.Retryable())
		}
	}
	if ErrorCode("memory.write").Valid() {
		t.Fatal("unknown capability error code was accepted")
	}
}

func TestAPIErrorIsStableAndBounded(t *testing.T) {
	t.Parallel()
	value := NewAPIError(CodeOperationPending, "operation is still active")
	if value.ExitStatus() != 5 || !value.Retryable || value.Status != "error" || value.SchemaVersion != 1 {
		t.Fatalf("API error = %#v", value)
	}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "principal") || strings.Contains(string(raw), "token") {
		t.Fatalf("authority leaked in API error: %s", raw)
	}
	invalid := NewAPIError(ErrorCode("unknown"), strings.Repeat("x", MaxDiagnosticBytes+1))
	if invalid.Code != CodeInternal || invalid.Message != "internal control error" {
		t.Fatalf("invalid API error was not bounded: %#v", invalid)
	}
}

func TestAgentCurrentPrivateFieldsAreExplicit(t *testing.T) {
	t.Parallel()
	raw, err := json.Marshal(AgentCurrentResponse{SchemaVersion: 1, Status: "actionable",
		RunID: "run-one", ClaimSecret: "private", Projection: json.RawMessage(`{"status":"actionable"}`)})
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"run_id"`, `"claim_secret"`, `"projection"`} {
		if !strings.Contains(string(raw), field) {
			t.Fatalf("private current envelope %s lacks %s", raw, field)
		}
	}
}
