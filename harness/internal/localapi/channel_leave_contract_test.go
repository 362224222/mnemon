package localapi

import "testing"

func TestChannelLeaveContractReportsPendingAndTerminalStateHonestly(t *testing.T) {
	t.Parallel()
	for _, lifecycle := range []struct {
		status     string
		membership string
	}{
		{status: "leaving", membership: "leaving"},
		{status: "left", membership: "left"},
		{status: "left", membership: "closed"},
	} {
		channel := validChannelContractView()
		channel.Membership = lifecycle.membership
		response := ChannelLeaveResponse{SchemaVersion: SchemaVersion,
			Status: lifecycle.status, Channel: channel}
		if apiErr := validateChannelLeaveResponse(response); apiErr != nil {
			t.Fatalf("valid leave lifecycle %#v rejected: %v", lifecycle, apiErr)
		}
	}
	channel := validChannelContractView()
	channel.Membership = "leaving"
	if validateChannelLeaveResponse(ChannelLeaveResponse{SchemaVersion: SchemaVersion,
		Status: "left", Channel: channel}) == nil {
		t.Fatal("pending owner acknowledgement was reported as terminal left")
	}
}
