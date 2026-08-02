package localapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	RouteHealth         = "/v1/health"
	RouteStatus         = "/v1/status"
	RouteAuthority      = "/v1/authority"
	RouteShutdown       = "/v1/shutdown"
	RouteHookCheck      = "/v1/hook/check"
	RouteAgentCurrent   = "/v1/agent/current"
	RouteTeamworkAction = "/v1/teamwork/action"
	RouteAgentResolve   = "/v1/agent/resolve"
)

type Server struct {
	authenticator Authenticator
	service       Service
	health        HealthProvider
	status        StatusProvider
	authority     AuthorityProvider
	lifecycle     LifecycleFunc
	mutation      MutationShutdownPreparer
	channels      ChannelService
	shutdownOnce  sync.Once
	handler       http.Handler
}

func NewServer(authenticator Authenticator, service Service,
	healthProviders ...HealthProvider,
) (*Server, error) {
	if len(healthProviders) > 1 || len(healthProviders) == 1 && healthProviders[0] == nil {
		return nil, errors.New("local API: at most one health provider is allowed")
	}
	var health HealthProvider
	if len(healthProviders) == 1 {
		health = healthProviders[0]
	}
	return newServer(authenticator, service, health, nil, nil, nil, nil)
}

// NewServerWithAuthority composes the controller-only observation route while
// retaining NewServer as the smaller test and service boundary.
func NewServerWithAuthority(authenticator Authenticator, service Service,
	health HealthProvider, authority AuthorityProvider,
) (*Server, error) {
	if health == nil || authority == nil {
		return nil, errors.New("local API: health and authority providers are required")
	}
	return newServer(authenticator, service, health, nil, authority, nil, nil)
}

// NewServerWithLifecycle composes all owner-only controller routes without
// changing the smaller NewServer and NewServerWithAuthority boundaries.
func NewServerWithLifecycle(authenticator Authenticator, service Service,
	health HealthProvider, authority AuthorityProvider, lifecycle LifecycleFunc,
	mutation ...MutationShutdownPreparer,
) (*Server, error) {
	if health == nil || authority == nil || lifecycle == nil || len(mutation) > 1 ||
		len(mutation) == 1 && mutation[0] == nil {
		return nil, errors.New("local API: health, authority and lifecycle providers are required")
	}
	var preparer MutationShutdownPreparer
	if len(mutation) == 1 {
		preparer = mutation[0]
	}
	return newServer(authenticator, service, health, nil, authority, lifecycle, preparer)
}

// NewServerWithStatusLifecycle composes the complete controller observation
// and lifecycle surface. The smaller constructors remain available to unit
// tests and domain services that do not claim operational status support.
func NewServerWithStatusLifecycle(authenticator Authenticator, service Service,
	health HealthProvider, status StatusProvider, authority AuthorityProvider,
	lifecycle LifecycleFunc, mutation ...MutationShutdownPreparer,
) (*Server, error) {
	if health == nil || status == nil || authority == nil || lifecycle == nil || len(mutation) > 1 ||
		len(mutation) == 1 && mutation[0] == nil {
		return nil, errors.New("local API: health, status, authority and lifecycle providers are required")
	}
	var preparer MutationShutdownPreparer
	if len(mutation) == 1 {
		preparer = mutation[0]
	}
	return newServer(authenticator, service, health, status, authority, lifecycle, preparer)
}

func newServer(authenticator Authenticator, service Service, health HealthProvider,
	status StatusProvider, authority AuthorityProvider, lifecycle LifecycleFunc,
	mutation MutationShutdownPreparer,
) (*Server, error) {
	if authenticator == nil || service == nil {
		return nil, errors.New("local API: authenticator and service are required")
	}
	if health == nil {
		if provided, ok := service.(HealthProvider); ok {
			health = provided
		}
	}
	server := &Server{authenticator: authenticator, service: service, health: health, status: status,
		authority: authority, lifecycle: lifecycle, mutation: mutation}
	if channels, ok := service.(ChannelService); ok {
		server.channels = channels
	}
	mux := http.NewServeMux()
	mux.HandleFunc(RouteHealth, server.handleHealth)
	mux.HandleFunc(RouteStatus, server.handleStatus)
	mux.HandleFunc(RouteAuthority, server.handleAuthority)
	mux.HandleFunc(RouteShutdown, server.handleShutdown)
	mux.HandleFunc(RouteHookCheck, server.handleHookCheck)
	mux.HandleFunc(RouteAgentCurrent, server.handleAgentCurrent)
	mux.HandleFunc(RouteTeamworkAction, server.handleTeamworkAction)
	mux.HandleFunc(RouteAgentResolve, server.handleAgentResolve)
	server.registerChannelRoutes(mux)
	agencyAvailable, err := registerAgencyRoutes(mux, service)
	if err != nil {
		return nil, err
	}
	server.handler = controlHandler(mux, agencyAvailable)
	return server, nil
}

func (s *Server) handleShutdown(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", http.MethodPost)
		writeErrorStatus(writer, http.StatusMethodNotAllowed,
			NewAPIError(CodeInvalidArgument, "method is not allowed"))
		return
	}
	if request.ContentLength != 0 || len(request.TransferEncoding) != 0 ||
		request.Header.Get("Content-Type") != "" {
		writeError(writer, NewAPIError(CodeInvalidArgument,
			"shutdown request must not contain content"))
		return
	}
	if request.URL.RawQuery != "" {
		writeError(writer, NewAPIError(CodeInvalidArgument,
			"shutdown request must not contain a query"))
		return
	}
	mutation, err := parseMutationShutdownHeader(request.Header)
	if err != nil {
		writeError(writer, NewAPIError(CodeInvalidArgument,
			"mutation shutdown header is invalid"))
		return
	}
	if request.Body != nil {
		raw, err := io.ReadAll(io.LimitReader(request.Body, 1))
		if err != nil || len(raw) != 0 {
			writeError(writer, NewAPIError(CodeInvalidArgument,
				"shutdown request must not contain content"))
			return
		}
	}
	metadata, apiErr := authenticateRequest(request.Context(), request, s.authenticator,
		headerPolicy{})
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	digestValue, err := singleHeader(request.Header, authorityDigestHeader)
	if err != nil {
		writeError(writer, NewAPIError(CodeInvalidArgument,
			"shutdown authority digest is required exactly once"))
		return
	}
	expectedDigest, err := parseAuthorityDigest(digestValue)
	if err != nil {
		writeError(writer, NewAPIError(CodeInvalidArgument,
			"shutdown authority digest is invalid"))
		return
	}
	if s.lifecycle == nil || s.authority == nil {
		writeError(writer, NewAPIError(CodeInternal,
			"lifecycle authority provider is unavailable"))
		return
	}
	if mutation {
		s.handleMutationShutdown(writer, request, metadata, expectedDigest)
		return
	}
	snapshot, apiErr := s.authority.Authority(request.Context(), metadata)
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	current, err := NewAuthorityResponse(snapshot)
	if err != nil {
		writeError(writer, NewAPIError(CodeInternal,
			"authority provider returned invalid state"))
		return
	}
	currentDigest, err := AuthorityDigest(current)
	if err != nil {
		writeError(writer, NewAPIError(CodeInternal,
			"authority provider returned invalid state"))
		return
	}
	if !sameAuthorityDigest(currentDigest, expectedDigest) {
		writeError(writer, NewAPIError(CodeOperationMismatch,
			"durable authority does not match the shutdown precondition"))
		return
	}
	writeResponse(writer, http.StatusOK, newShutdownResponse(currentDigest))
	s.shutdownOnce.Do(s.lifecycle)
}

func (s *Server) handleMutationShutdown(writer http.ResponseWriter, request *http.Request,
	metadata RequestMetadata, expectedDigest model.Digest,
) {
	if s.mutation == nil {
		writeError(writer, NewAPIError(CodeInternal,
			"mutation shutdown preparation is unavailable"))
		return
	}
	snapshot, release, apiErr := s.mutation.PrepareMutationShutdown(request.Context(), metadata)
	if apiErr != nil {
		if release != nil {
			release()
		}
		writeError(writer, apiErr)
		return
	}
	if release == nil {
		writeError(writer, NewAPIError(CodeInternal,
			"mutation shutdown admission release is unavailable"))
		return
	}
	retainSeal := false
	defer func() {
		if !retainSeal {
			release()
		}
	}()
	current, err := NewAuthorityResponse(snapshot)
	if err != nil {
		writeError(writer, NewAPIError(CodeInternal,
			"mutation shutdown authority is invalid"))
		return
	}
	currentDigest, err := AuthorityDigest(current)
	if err != nil {
		writeError(writer, NewAPIError(CodeInternal,
			"mutation shutdown authority is invalid"))
		return
	}
	if !sameAuthorityDigest(currentDigest, expectedDigest) {
		writeError(writer, NewAPIError(CodeOperationMismatch,
			"durable authority does not match the mutation shutdown precondition"))
		return
	}
	retainSeal = true
	writeResponse(writer, http.StatusOK, newShutdownResponse(currentDigest))
	s.shutdownOnce.Do(s.lifecycle)
}

func (s *Server) Handler() http.Handler { return s.handler }

func (s *Server) handleHealth(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		writeErrorStatus(writer, http.StatusMethodNotAllowed,
			NewAPIError(CodeInvalidArgument, "method is not allowed"))
		return
	}
	if request.ContentLength != 0 || len(request.TransferEncoding) != 0 {
		writeError(writer, NewAPIError(CodeInvalidArgument, "health request must not contain a body"))
		return
	}
	if request.URL.RawQuery != "" {
		writeError(writer, NewAPIError(CodeInvalidArgument, "health request must not contain a query"))
		return
	}
	if request.Body != nil {
		raw, err := io.ReadAll(io.LimitReader(request.Body, 1))
		if err != nil || len(raw) != 0 {
			writeError(writer, NewAPIError(CodeInvalidArgument, "health request must not contain a body"))
			return
		}
	}
	metadata, apiErr := authenticateRequest(request.Context(), request, s.authenticator, headerPolicy{})
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	if s.health == nil {
		writeError(writer, NewAPIError(CodeInternal, "health provider is unavailable"))
		return
	}
	snapshot, apiErr := s.health.Health(request.Context(), metadata)
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	response, err := healthResponse(snapshot)
	if err != nil {
		writeError(writer, NewAPIError(CodeInternal, "health provider returned invalid state"))
		return
	}
	writeResponse(writer, http.StatusOK, response)
}

func (s *Server) handleStatus(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		writeErrorStatus(writer, http.StatusMethodNotAllowed,
			NewAPIError(CodeInvalidArgument, "method is not allowed"))
		return
	}
	if request.ContentLength != 0 || len(request.TransferEncoding) != 0 ||
		request.Header.Get("Content-Type") != "" {
		writeError(writer, NewAPIError(CodeInvalidArgument,
			"status request must not contain content"))
		return
	}
	if request.URL.RawQuery != "" {
		writeError(writer, NewAPIError(CodeInvalidArgument,
			"status request must not contain a query"))
		return
	}
	if request.Body != nil {
		raw, err := io.ReadAll(io.LimitReader(request.Body, 1))
		if err != nil || len(raw) != 0 {
			writeError(writer, NewAPIError(CodeInvalidArgument,
				"status request must not contain content"))
			return
		}
	}
	metadata, apiErr := authenticateRequest(request.Context(), request, s.authenticator, headerPolicy{})
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	if s.status == nil {
		writeError(writer, NewAPIError(CodeInternal, "status provider is unavailable"))
		return
	}
	snapshot, apiErr := s.status.Status(request.Context(), metadata)
	if apiErr != nil {
		writeError(writer, publicStatusProviderError(apiErr))
		return
	}
	response, err := NewStatusResponse(snapshot)
	if err != nil {
		writeError(writer, NewAPIError(CodeInternal, "status provider returned invalid state"))
		return
	}
	writeResponse(writer, http.StatusOK, response)
}

func publicStatusProviderError(apiErr *APIError) *APIError {
	if apiErr != nil && apiErr.Code == CodeMnemondUnavailable {
		return NewAPIError(CodeMnemondUnavailable,
			"operational status is temporarily unavailable")
	}
	return NewAPIError(CodeInternal, "operational status observation failed")
}

func (s *Server) handleAuthority(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		writeErrorStatus(writer, http.StatusMethodNotAllowed,
			NewAPIError(CodeInvalidArgument, "method is not allowed"))
		return
	}
	if request.ContentLength != 0 || len(request.TransferEncoding) != 0 ||
		request.Header.Get("Content-Type") != "" {
		writeError(writer, NewAPIError(CodeInvalidArgument,
			"authority request must not contain content"))
		return
	}
	if request.URL.RawQuery != "" {
		writeError(writer, NewAPIError(CodeInvalidArgument,
			"authority request must not contain a query"))
		return
	}
	if request.Body != nil {
		raw, err := io.ReadAll(io.LimitReader(request.Body, 1))
		if err != nil || len(raw) != 0 {
			writeError(writer, NewAPIError(CodeInvalidArgument,
				"authority request must not contain content"))
			return
		}
	}
	metadata, apiErr := authenticateRequest(request.Context(), request, s.authenticator, headerPolicy{})
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	if s.authority == nil {
		writeError(writer, NewAPIError(CodeInternal, "authority provider is unavailable"))
		return
	}
	snapshot, apiErr := s.authority.Authority(request.Context(), metadata)
	if apiErr != nil {
		writeError(writer, apiErr)
		return
	}
	response, err := NewAuthorityResponse(snapshot)
	if err != nil {
		writeError(writer, NewAPIError(CodeInternal, "authority provider returned invalid state"))
		return
	}
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

func IsControlRoute(path string) bool {
	return path == RouteHealth || path == RouteStatus || path == RouteAuthority ||
		path == RouteShutdown || IsAgentRoute(path) || IsChannelRoute(path) || IsAgencyRoute(path)
}

func validTeamworkAction(action string) bool {
	return action == "offer" || action == "accept" || action == "decline" ||
		action == "deliver" || action == "rework" || action == "close" || action == "cancel"
}
