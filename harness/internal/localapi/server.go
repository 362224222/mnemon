package localapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	RouteHookCheck      = "/v1/hook/check"
	RouteAgentCurrent   = "/v1/agent/current"
	RouteTeamworkAction = "/v1/teamwork/action"
	RouteAgentResolve   = "/v1/agent/resolve"
)

type Service interface {
	HookCheck(context.Context, RequestMetadata, HookCheckRequest) (HookCheckResponse, *APIError)
	AgentCurrent(context.Context, RequestMetadata, AgentCurrentRequest) (AgentCurrentResponse, *APIError)
	TeamworkAction(context.Context, RequestMetadata, TeamworkActionRequest) (OperationResponse, *APIError)
	AgentResolve(context.Context, RequestMetadata, AgentResolveRequest) (OperationResponse, *APIError)
}

type Server struct {
	authenticator Authenticator
	service       Service
	handler       http.Handler
}

func NewServer(authenticator Authenticator, service Service) (*Server, error) {
	if authenticator == nil || service == nil {
		return nil, errors.New("local API: authenticator and service are required")
	}
	server := &Server{authenticator: authenticator, service: service}
	mux := http.NewServeMux()
	mux.HandleFunc(RouteHookCheck, server.handleHookCheck)
	mux.HandleFunc(RouteAgentCurrent, server.handleAgentCurrent)
	mux.HandleFunc(RouteTeamworkAction, server.handleTeamworkAction)
	mux.HandleFunc(RouteAgentResolve, server.handleAgentResolve)
	server.handler = http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !IsAgentRoute(request.URL.Path) {
			writeErrorStatus(writer, http.StatusNotFound,
				NewAPIError(CodeInvalidArgument, "local control route does not exist"))
			return
		}
		mux.ServeHTTP(writer, request)
	})
	return server, nil
}

func (s *Server) Handler() http.Handler { return s.handler }

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

func (s *Server) prepare(writer http.ResponseWriter, request *http.Request,
	policy headerPolicy,
) (RequestMetadata, bool) {
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", http.MethodPost)
		writeErrorStatus(writer, http.StatusMethodNotAllowed,
			NewAPIError(CodeInvalidArgument, "method is not allowed"))
		return RequestMetadata{}, false
	}
	if request.Header.Get("Content-Type") != "application/json" {
		writeError(writer, NewAPIError(CodeInvalidArgument, "Content-Type must be application/json"))
		return RequestMetadata{}, false
	}
	metadata, apiErr := authenticateRequest(request.Context(), request, s.authenticator, policy)
	if apiErr != nil {
		writeError(writer, apiErr)
		return RequestMetadata{}, false
	}
	return metadata, true
}

func decodeRequest(writer http.ResponseWriter, request *http.Request, target any) *APIError {
	if request.Body == nil {
		return NewAPIError(CodeInvalidArgument, "JSON object body is required")
	}
	if request.ContentLength > MaxRequestBodyBytes {
		return NewAPIError(CodeInvalidArgument, "request body exceeds the local control limit")
	}
	reader := io.LimitReader(request.Body, MaxRequestBodyBytes+1)
	raw, err := io.ReadAll(reader)
	if err != nil {
		return NewAPIError(CodeInvalidArgument, "request body cannot be read")
	}
	if len(raw) == 0 || len(raw) > MaxRequestBodyBytes {
		return NewAPIError(CodeInvalidArgument, "request body exceeds the local control limit")
	}
	canonical, err := model.NewJSON(raw)
	if err != nil || len(canonical.Bytes()) == 0 || canonical.Bytes()[0] != '{' {
		return NewAPIError(CodeInvalidArgument, "request body must be one valid JSON object")
	}
	decoder := json.NewDecoder(bytes.NewReader(canonical.Bytes()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return NewAPIError(CodeInvalidArgument, "request body does not match the closed route schema")
	}
	if token, err := decoder.Token(); err == nil || token != nil {
		return NewAPIError(CodeInvalidArgument, "request body contains a trailing value")
	}
	return nil
}

func writeError(writer http.ResponseWriter, apiErr *APIError) {
	status := httpStatusForError(apiErr)
	writeErrorStatus(writer, status, apiErr)
}

func writeErrorStatus(writer http.ResponseWriter, status int, apiErr *APIError) {
	if apiErr == nil {
		apiErr = NewAPIError(CodeInternal, "internal control error")
	}
	writeResponse(writer, status, apiErr)
}

func writeResponse(writer http.ResponseWriter, status int, value any) {
	raw, err := model.CanonicalMarshal(value)
	if err != nil {
		status = http.StatusInternalServerError
		raw = []byte(`{"code":"internal","message":"internal control error","operation_id":null,"replayed":false,"retryable":false,"schema_version":1,"status":"error"}`)
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.WriteHeader(status)
	_, _ = writer.Write(append(raw, '\n'))
}

func httpStatusForError(apiErr *APIError) int {
	if apiErr == nil {
		return http.StatusInternalServerError
	}
	switch apiErr.Code {
	case CodeAuthenticationFailed:
		return http.StatusUnauthorized
	case CodeOperationPending, CodePeerUnavailable, CodeMnemondUnavailable:
		return http.StatusServiceUnavailable
	case CodeContextStale, CodeActionNotAllowed, CodeOperationMismatch,
		CodeWorkConflict, CodeWorkExpired:
		return http.StatusConflict
	case CodeInternal:
		return http.StatusInternalServerError
	default:
		return http.StatusBadRequest
	}
}

func IsAgentRoute(path string) bool {
	return path == RouteHookCheck || path == RouteAgentCurrent ||
		path == RouteTeamworkAction || path == RouteAgentResolve
}

func validTeamworkAction(action string) bool {
	return action == "offer" || action == "accept" || action == "decline" ||
		action == "deliver" || action == "rework" || action == "close" || action == "cancel"
}

func validResolveDecision(decision string) bool {
	return decision == "no-action" || decision == "retry" || decision == "reject"
}
