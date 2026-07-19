package localapi

import "net/http"

func (s *Server) handleHookCheck(writer http.ResponseWriter, request *http.Request) {
	metadata, ok := s.prepare(writer, request, headerPolicy{attachmentAllowed: true})
	if !ok {
		return
	}
	var input HookCheckRequest
	if apiErr := decodeRequest(writer, request, &input); apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	response, apiErr := s.service.HookCheck(request.Context(), metadata, input)
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	response.SchemaVersion = SchemaVersion
	writeResponse(writer, http.StatusOK, response)
}

func (s *Server) handleAgentCurrent(writer http.ResponseWriter, request *http.Request) {
	metadata, ok := s.prepare(writer, request, headerPolicy{attachmentAllowed: true})
	if !ok {
		return
	}
	var input AgentCurrentRequest
	if apiErr := decodeRequest(writer, request, &input); apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	response, apiErr := s.service.AgentCurrent(request.Context(), metadata, input)
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	response.SchemaVersion = SchemaVersion
	writeResponse(writer, http.StatusOK, response)
}

func (s *Server) handleTeamworkAction(writer http.ResponseWriter, request *http.Request) {
	metadata, ok := s.prepare(writer, request, headerPolicy{
		operationRequired: true, operationAllowed: true, claimAllowed: true,
	})
	if !ok {
		return
	}
	var input TeamworkActionRequest
	if apiErr := decodeRequest(writer, request, &input); apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	if !validTeamworkAction(input.Action) {
		writeError(writer, NewAPIError(CodeUnknownAction, "unknown Teamwork action"))
		return
	}
	response, apiErr := s.service.TeamworkAction(request.Context(), metadata, input)
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	response.SchemaVersion = SchemaVersion
	writeResponse(writer, http.StatusOK, response)
}

func (s *Server) handleAgentResolve(writer http.ResponseWriter, request *http.Request) {
	metadata, ok := s.prepare(writer, request, headerPolicy{
		operationRequired: true, operationAllowed: true, claimRequired: true, claimAllowed: true,
	})
	if !ok {
		return
	}
	var input AgentResolveRequest
	if apiErr := decodeRequest(writer, request, &input); apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	if !validResolveDecision(input.Decision) {
		writeError(writer, NewAPIError(CodeUnknownAction, "unknown handling resolution"))
		return
	}
	response, apiErr := s.service.AgentResolve(request.Context(), metadata, input)
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	response.SchemaVersion = SchemaVersion
	writeResponse(writer, http.StatusOK, response)
}

func validResolveDecision(decision string) bool {
	return decision == "no-action" || decision == "retry" || decision == "reject"
}
