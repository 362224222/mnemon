package node

import "testing"

func TestControlChannelResponsesAreClosedVersionedShapes(t *testing.T) {
	create := ChannelCreateResponse{SchemaVersion: SchemaVersion, Status: "created"}
	status := ChannelStatusResponse{SchemaVersion: SchemaVersion, Status: "ok", Channels: []ChannelView{}}
	if create.SchemaVersion != 1 || status.SchemaVersion != 1 || status.Channels == nil {
		t.Fatalf("Channel control response shape drifted")
	}
}
