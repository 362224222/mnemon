package localapi

import "github.com/mnemon-dev/mnemon/harness/internal/model"

func validChannelReplayProbeRequest(request ChannelReplayProbeRequest) bool {
	return validChannelAlias(request.SourceChannel) && validChannelAlias(request.TargetChannel) &&
		request.SourceChannel != request.TargetChannel
}

func validateChannelReplayProbeResponse(response ChannelReplayProbeResponse) *APIError {
	if response.SchemaVersion != SchemaVersion || !validChannelReplayProbeStatus(response) ||
		!response.ReplayAttempted ||
		!validChannelAlias(response.SourceChannel) ||
		!validChannelAlias(response.TargetChannel) ||
		response.SourceChannel == response.TargetChannel ||
		!validChannelEvidenceDigest(response.SourceChannelIDDigest) ||
		!validChannelEvidenceDigest(response.TargetChannelIDDigest) ||
		!validChannelEvidenceDigest(response.PublicationDigest) ||
		!validChannelEvidenceDigest(response.EventDigest) ||
		validateChannelEventKey(response.EventKey) != nil ||
		response.TargetMutationSuppressed != (response.TargetBefore == response.TargetAfter) {
		return invalidControlResponse("Channel replay probe response is invalid")
	}
	if raw, err := model.CanonicalMarshal(response); err != nil || len(raw)+1 > MaxChannelResponseBytes {
		return invalidControlResponse("Channel replay probe response exceeds its closed bound")
	}
	return nil
}

func validChannelReplayProbeStatus(response ChannelReplayProbeResponse) bool {
	switch response.Status {
	case "rejected":
		return response.Rejection == "wrong_topic"
	case "accepted", "ignored":
		return response.Rejection == ""
	default:
		return false
	}
}
