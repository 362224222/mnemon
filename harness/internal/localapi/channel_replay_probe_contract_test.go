package localapi

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestChannelReplayProbeContractIsClosedAndCanReportFailedObservation(t *testing.T) {
	t.Parallel()
	const peerID = "12D3KooWCgPRroygp86pxPWqvQuXKSDf6CoJJHkmfEsNhm9rF46B"
	if !validChannelReplayProbeRequest(ChannelReplayProbeRequest{
		SourceChannel: "alpha", TargetChannel: "beta"}) {
		t.Fatal("valid replay probe request was rejected")
	}
	if validChannelReplayProbeRequest(ChannelReplayProbeRequest{
		SourceChannel: "alpha", TargetChannel: "alpha"}) {
		t.Fatal("same-Channel replay probe request was accepted")
	}
	valid := ChannelReplayProbeResponse{SchemaVersion: SchemaVersion, Status: "rejected",
		SourceChannel: "alpha", TargetChannel: "beta",
		SourceChannelIDDigest:    model.Sum([]byte("alpha")).String(),
		TargetChannelIDDigest:    model.Sum([]byte("beta")).String(),
		PublicationDigest:        model.Sum([]byte("publication")).String(),
		EventDigest:              model.Sum([]byte("event")).String(),
		EventKey:                 ChannelEventKeyView{OriginPeerID: peerID, OriginEpoch: "epoch-alpha", EventID: "event-alpha"},
		ReplayAttempted:          true,
		Rejection:                "wrong_topic",
		TargetBefore:             ChannelMutationCounts{Events: 3, Works: 2},
		TargetAfter:              ChannelMutationCounts{Events: 3, Works: 2},
		TargetMutationSuppressed: true}
	if apiErr := validateChannelReplayProbeResponse(valid); apiErr != nil {
		t.Fatalf("valid replay probe rejected: %v", apiErr)
	}
	failed := valid
	failed.Status = "accepted"
	failed.Rejection = ""
	failed.TargetAfter.Events++
	failed.TargetMutationSuppressed = false
	if apiErr := validateChannelReplayProbeResponse(failed); apiErr != nil {
		t.Fatalf("failed-observation replay probe rejected: %v", apiErr)
	}
	failed.TargetMutationSuppressed = true
	if validateChannelReplayProbeResponse(failed) == nil {
		t.Fatal("inconsistent replay mutation flag accepted")
	}
}
