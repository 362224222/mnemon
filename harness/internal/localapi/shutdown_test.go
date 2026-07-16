package localapi

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestShutdownResponseIsFixedCanonicalAndBounded(t *testing.T) {
	t.Parallel()
	response := newShutdownResponse()
	raw, err := model.CanonicalMarshal(response)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"schema_version":1,"status":"stopping"}`
	if string(raw) != want || len(raw)+1 > MaxShutdownResponseBytes ||
		validateShutdownResponse(response) != nil {
		t.Fatalf("shutdown response = %q %#v", raw, response)
	}
}

func TestShutdownResponseRejectsOpenOrUnsupportedState(t *testing.T) {
	t.Parallel()
	tests := []ShutdownResponse{
		{},
		{SchemaVersion: SchemaVersion + 1, Status: "stopping"},
		{SchemaVersion: SchemaVersion, Status: "stopped"},
	}
	for _, response := range tests {
		if apiErr := validateShutdownResponse(response); apiErr == nil || apiErr.Code != CodeInternal {
			t.Fatalf("validateShutdownResponse(%#v) = %#v", response, apiErr)
		}
	}
}
