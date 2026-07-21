package node

import "testing"

func TestControlAgentResponseShapesAreVersioned(t *testing.T) {
	t.Parallel()
	current := AgentCurrentResponse{SchemaVersion: SchemaVersion, Status: "ready"}
	action := OperationResponse{SchemaVersion: SchemaVersion, Status: "committed", Action: "offer"}
	if current.SchemaVersion != 1 || action.SchemaVersion != 1 {
		t.Fatalf("control agent response schema version drifted")
	}
}
