package node

import "testing"

func TestControlChannelResponsesAreClosedVersionedShapes(t *testing.T) {
	t.Parallel()
	create := ChannelCreateResponse{SchemaVersion: SchemaVersion, Status: "created"}
	status := ChannelStatusResponse{SchemaVersion: SchemaVersion, Status: "ok", Channels: []ChannelView{}}
	probe := ChannelReplayProbeResponse{SchemaVersion: SchemaVersion, Status: "rejected"}
	if create.SchemaVersion != 1 || status.SchemaVersion != 1 || status.Channels == nil ||
		probe.SchemaVersion != 1 {
		t.Fatalf("Channel control response shape drifted")
	}
}
