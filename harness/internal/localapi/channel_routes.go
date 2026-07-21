package localapi

import (
	"io"
	"net/http"
)

const (
	RouteChannelCreate       = "/v1/channel/create"
	RouteChannelJoin         = "/v1/channel/join"
	RouteChannelStatus       = "/v1/channel/status"
	RouteChannelInvites      = "/v1/channel/invites"
	RouteChannelInvitesClose = "/v1/channel/invites/close"
	RouteChannelRemove       = "/v1/channel/remove"
	RouteChannelLeave        = "/v1/channel/leave"
	RouteChannelAbandon      = "/v1/channel/abandon"
	RouteChannelReplayProbe  = "/v1/channel/replay-probe"
)

func IsChannelRoute(path string) bool {
	return path == RouteChannelCreate || path == RouteChannelJoin || path == RouteChannelStatus ||
		path == RouteChannelInvites || path == RouteChannelInvitesClose || path == RouteChannelRemove ||
		path == RouteChannelLeave || path == RouteChannelAbandon || path == RouteChannelReplayProbe
}

func (s *Server) registerChannelRoutes(mux *http.ServeMux) {
	mux.HandleFunc(RouteChannelCreate, s.handleChannelCreate)
	mux.HandleFunc(RouteChannelJoin, s.handleChannelJoin)
	mux.HandleFunc(RouteChannelStatus, s.handleChannelStatus)
	mux.HandleFunc(RouteChannelInvites, s.handleChannelInvite)
	mux.HandleFunc(RouteChannelInvitesClose, s.handleChannelInviteClose)
	mux.HandleFunc(RouteChannelRemove, s.handleChannelRemove)
	mux.HandleFunc(RouteChannelLeave, s.handleChannelLeave)
	mux.HandleFunc(RouteChannelAbandon, s.handleChannelAbandon)
	mux.HandleFunc(RouteChannelReplayProbe, s.handleChannelReplayProbe)
}

func (s *Server) handleChannelCreate(writer http.ResponseWriter, request *http.Request) {
	var input ChannelCreateRequest
	metadata, ok := s.prepareChannelPost(writer, request, &input)
	if !ok {
		return
	}
	if !validChannelCreateRequest(input) {
		writeError(writer, NewAPIError(CodeInvalidArgument, "Channel name is invalid"))
		return
	}
	response, apiErr := s.channels.ChannelCreate(request.Context(), metadata, input)
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	if apiErr := validateChannelCreateResponse(response); apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	writeResponse(writer, http.StatusOK, response)
}

func (s *Server) handleChannelJoin(writer http.ResponseWriter, request *http.Request) {
	var input ChannelJoinRequest
	metadata, ok := s.prepareChannelPost(writer, request, &input)
	if !ok {
		return
	}
	if !validChannelJoinRequest(input) {
		writeError(writer, NewAPIError(CodeInvalidToken, "Channel invite token is invalid"))
		return
	}
	response, apiErr := s.channels.ChannelJoin(request.Context(), metadata, input)
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	if apiErr := validateChannelJoinResponse(response); apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	writeResponse(writer, http.StatusOK, response)
}

func (s *Server) handleChannelInvite(writer http.ResponseWriter, request *http.Request) {
	var input ChannelInviteRequest
	metadata, ok := s.prepareChannelPost(writer, request, &input)
	if !ok {
		return
	}
	if !validChannelInviteRequest(input) {
		writeError(writer, NewAPIError(CodeInvalidArgument, "Channel invite options are invalid"))
		return
	}
	response, apiErr := s.channels.ChannelInvite(request.Context(), metadata, input)
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	if apiErr := validateChannelInviteResponse(response); apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	writeResponse(writer, http.StatusOK, response)
}

func (s *Server) handleChannelInviteClose(writer http.ResponseWriter, request *http.Request) {
	var input ChannelInviteCloseRequest
	metadata, ok := s.prepareChannelPost(writer, request, &input)
	if !ok {
		return
	}
	if !validChannelInviteCloseRequest(input) {
		writeError(writer, NewAPIError(CodeInvalidArgument, "Channel selector is invalid"))
		return
	}
	response, apiErr := s.channels.ChannelInviteClose(request.Context(), metadata, input)
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	if apiErr := validateChannelInviteCloseResponse(response); apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	writeResponse(writer, http.StatusOK, response)
}

func (s *Server) handleChannelStatus(writer http.ResponseWriter, request *http.Request) {
	metadata, ok := s.prepareChannelGet(writer, request)
	if !ok {
		return
	}
	response, apiErr := s.channels.ChannelStatus(request.Context(), metadata)
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	if apiErr := validateChannelStatusResponse(response); apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	writeResponse(writer, http.StatusOK, response)
}

func (s *Server) handleChannelRemove(writer http.ResponseWriter, request *http.Request) {
	var input ChannelRemoveRequest
	metadata, ok := s.prepareChannelPost(writer, request, &input)
	if !ok {
		return
	}
	if !validChannelRemoveRequest(input) {
		writeError(writer, NewAPIError(CodeInvalidArgument, "Channel member selector is invalid"))
		return
	}
	response, apiErr := s.channels.ChannelRemove(request.Context(), metadata, input)
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	if apiErr := validateChannelRemoveResponse(response); apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	writeResponse(writer, http.StatusOK, response)
}

func (s *Server) handleChannelLeave(writer http.ResponseWriter, request *http.Request) {
	var input ChannelLeaveRequest
	metadata, ok := s.prepareChannelPost(writer, request, &input)
	if !ok {
		return
	}
	if !validChannelLeaveRequest(input) {
		writeError(writer, NewAPIError(CodeInvalidArgument, "Channel selector is invalid"))
		return
	}
	response, apiErr := s.channels.ChannelLeave(request.Context(), metadata, input)
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	if apiErr := validateChannelLeaveResponse(response); apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	writeResponse(writer, http.StatusOK, response)
}

func (s *Server) handleChannelAbandon(writer http.ResponseWriter, request *http.Request) {
	var input ChannelAbandonRequest
	metadata, ok := s.prepareChannelPost(writer, request, &input)
	if !ok {
		return
	}
	if !validChannelAbandonRequest(input) {
		writeError(writer, NewAPIError(CodeInvalidArgument,
			"Channel abandon requires force and an exact Channel confirmation"))
		return
	}
	response, apiErr := s.channels.ChannelAbandon(request.Context(), metadata, input)
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	if apiErr := validateChannelAbandonResponse(response); apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	writeResponse(writer, http.StatusOK, response)
}

func (s *Server) handleChannelReplayProbe(writer http.ResponseWriter, request *http.Request) {
	var input ChannelReplayProbeRequest
	metadata, ok := s.prepareChannelPost(writer, request, &input)
	if !ok {
		return
	}
	if !validChannelReplayProbeRequest(input) {
		writeError(writer, NewAPIError(CodeInvalidArgument,
			"Channel replay probe requires distinct source and target Channels"))
		return
	}
	response, apiErr := s.channels.ChannelReplayProbe(request.Context(), metadata, input)
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	if apiErr := validateChannelReplayProbeResponse(response); apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	writeResponse(writer, http.StatusOK, response)
}

func (s *Server) prepareChannelPost(writer http.ResponseWriter, request *http.Request,
	target any,
) (RequestMetadata, bool) {
	metadata, ok := s.prepare(writer, request, headerPolicy{})
	if !ok {
		return RequestMetadata{}, false
	}
	if request.URL.RawQuery != "" {
		writeError(writer, NewAPIError(CodeInvalidArgument, "Channel request must not contain a query"))
		return RequestMetadata{}, false
	}
	if apiErr := decodeRequest(writer, request, target); apiErr != nil {
		writeError(writer, apiErr)
		return RequestMetadata{}, false
	}
	if s.channels == nil {
		writeError(writer, NewAPIError(CodeInternal, "Channel controller is unavailable"))
		return RequestMetadata{}, false
	}
	return metadata, true
}

func (s *Server) prepareChannelGet(writer http.ResponseWriter,
	request *http.Request,
) (RequestMetadata, bool) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		writeErrorStatus(writer, http.StatusMethodNotAllowed,
			NewAPIError(CodeInvalidArgument, "method is not allowed"))
		return RequestMetadata{}, false
	}
	if request.ContentLength != 0 || len(request.TransferEncoding) != 0 ||
		request.Header.Get("Content-Type") != "" || request.URL.RawQuery != "" {
		writeError(writer, NewAPIError(CodeInvalidArgument, "Channel status request must not contain content"))
		return RequestMetadata{}, false
	}
	if request.Body != nil {
		raw, err := io.ReadAll(io.LimitReader(request.Body, 1))
		if err != nil || len(raw) != 0 {
			writeError(writer, NewAPIError(CodeInvalidArgument, "Channel status request must not contain content"))
			return RequestMetadata{}, false
		}
	}
	metadata, apiErr := authenticateRequest(request.Context(), request, s.authenticator, headerPolicy{})
	if apiErr != nil {
		writeError(writer, apiErr)
		return RequestMetadata{}, false
	}
	if s.channels == nil {
		writeError(writer, NewAPIError(CodeInternal, "Channel controller is unavailable"))
		return RequestMetadata{}, false
	}
	return metadata, true
}
