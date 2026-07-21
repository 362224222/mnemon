package localapi

func validateChannelLeaveResponse(response ChannelLeaveResponse) *APIError {
	statusMatches := response.Status == "leaving" && response.Channel.Membership == "leaving" ||
		response.Status == "left" && (response.Channel.Membership == "left" ||
			response.Channel.Membership == "closed")
	if response.SchemaVersion != SchemaVersion || !statusMatches || validateChannelView(response.Channel) != nil {
		return invalidControlResponse("Channel leave response is invalid")
	}
	return nil
}
