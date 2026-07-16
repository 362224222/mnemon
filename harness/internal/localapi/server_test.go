package localapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type fakeService struct {
	called   string
	metadata RequestMetadata
	action   TeamworkActionRequest
	resolve  AgentResolveRequest
	fail     *APIError
}

func (s *fakeService) HookCheck(_ context.Context, metadata RequestMetadata,
	_ HookCheckRequest,
) (HookCheckResponse, *APIError) {
	s.called, s.metadata = "hook", metadata
	return HookCheckResponse{Pending: true}, s.fail
}

func (s *fakeService) AgentCurrent(_ context.Context, metadata RequestMetadata,
	_ AgentCurrentRequest,
) (AgentCurrentResponse, *APIError) {
	s.called, s.metadata = "current", metadata
	return AgentCurrentResponse{Status: "none"}, s.fail
}

func (s *fakeService) TeamworkAction(_ context.Context, metadata RequestMetadata,
	request TeamworkActionRequest,
) (OperationResponse, *APIError) {
	s.called, s.metadata, s.action = "action", metadata, request
	return OperationResponse{Status: "accepted", Action: "teamwork." + request.Action,
		OperationID: "operation-one", Results: []OperationResult{}, Receipt: "receipt-one"}, s.fail
}

func (s *fakeService) AgentResolve(_ context.Context, metadata RequestMetadata,
	request AgentResolveRequest,
) (OperationResponse, *APIError) {
	s.called, s.metadata, s.resolve = "resolve", metadata, request
	return OperationResponse{Status: "resolved", Action: "agent.resolve." + request.Decision,
		OperationID: "operation-one", Results: []OperationResult{}, Receipt: "receipt-one"}, s.fail
}

func TestServerRoutesAuthenticatedClosedRequests(t *testing.T) {
	t.Parallel()
	credential := make([]byte, opaqueSecretBytes)
	operation := make([]byte, opaqueSecretBytes)
	claim := make([]byte, opaqueSecretBytes)
	auth := &fakeAuthenticator{want: modelDigest(credential)}
	service := &fakeService{}
	server, err := NewServer(auth, service)
	if err != nil {
		t.Fatal(err)
	}

	recorder := httptest.NewRecorder()
	request := authenticatedRequest(t, http.MethodPost, RouteTeamworkAction,
		`{"action":"deliver","artifacts":["findings.md"],"content":"done"}`, credential)
	request.Header.Set(operationKeyHeader, encodeSecret(operation))
	request.Header.Set(claimContextHeader, encodeSecret(claim))
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || service.called != "action" || service.action.Action != "deliver" ||
		service.action.Content != "done" || !service.metadata.HasOperationKey || !service.metadata.HasClaimContext {
		t.Fatalf("action response = %d %s, service=%#v", recorder.Code, recorder.Body.String(), service)
	}
	var envelope OperationResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil || envelope.SchemaVersion != 1 ||
		envelope.Status != "accepted" {
		t.Fatalf("operation envelope = %#v, %v", envelope, err)
	}
}

func TestServerRejectsAuthorityFieldsDuplicateKeysAndOversize(t *testing.T) {
	t.Parallel()
	credential := make([]byte, opaqueSecretBytes)
	operation := make([]byte, opaqueSecretBytes)
	auth := &fakeAuthenticator{want: modelDigest(credential)}
	server, _ := NewServer(auth, &fakeService{})
	tests := []struct {
		name string
		body string
	}{
		{name: "principal", body: `{"action":"offer","principal":"forged"}`},
		{name: "actor", body: `{"action":"offer","actor":"forged"}`},
		{name: "source", body: `{"action":"offer","source":"local"}`},
		{name: "unknown capability", body: `{"action":"memory.write"}`},
		{name: "duplicate", body: `{"action":"offer","action":"cancel"}`},
		{name: "array", body: `[{"action":"offer"}]`},
		{name: "oversize", body: `{"action":"offer","content":"` + strings.Repeat("x", MaxRequestBodyBytes) + `"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := authenticatedRequest(t, http.MethodPost, RouteTeamworkAction, test.body, credential)
			request.Header.Set(operationKeyHeader, encodeSecret(operation))
			server.Handler().ServeHTTP(recorder, request)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("response = %d %s", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestServerEnforcesMethodContentTypeAndRouteHeaders(t *testing.T) {
	t.Parallel()
	credential := make([]byte, opaqueSecretBytes)
	secret := encodeSecret(make([]byte, opaqueSecretBytes))
	auth := &fakeAuthenticator{want: modelDigest(credential)}
	server, _ := NewServer(auth, &fakeService{})
	tests := []struct {
		name   string
		method string
		path   string
		body   string
		mutate func(*http.Request)
		status int
	}{
		{name: "method", method: http.MethodGet, path: RouteHookCheck, body: `{}`, status: http.StatusMethodNotAllowed},
		{name: "content type", method: http.MethodPost, path: RouteHookCheck, body: `{}`,
			mutate: func(r *http.Request) { r.Header.Set("Content-Type", "application/json; charset=utf-8") }, status: http.StatusBadRequest},
		{name: "operation missing", method: http.MethodPost, path: RouteTeamworkAction, body: `{"action":"offer"}`, status: http.StatusBadRequest},
		{name: "operation on hook", method: http.MethodPost, path: RouteHookCheck, body: `{}`,
			mutate: func(r *http.Request) { r.Header.Set(operationKeyHeader, secret) }, status: http.StatusBadRequest},
		{name: "claim missing resolve", method: http.MethodPost, path: RouteAgentResolve, body: `{"decision":"retry"}`,
			mutate: func(r *http.Request) { r.Header.Set(operationKeyHeader, secret) }, status: http.StatusBadRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := authenticatedRequest(t, test.method, test.path, test.body, credential)
			if test.mutate != nil {
				test.mutate(request)
			}
			server.Handler().ServeHTTP(recorder, request)
			if recorder.Code != test.status {
				t.Fatalf("response = %d %s, want %d", recorder.Code, recorder.Body.String(), test.status)
			}
		})
	}
}

func TestServerReturnsStableServiceErrors(t *testing.T) {
	t.Parallel()
	credential := make([]byte, opaqueSecretBytes)
	service := &fakeService{fail: NewAPIError(CodeOperationPending, "operation remains active")}
	server, _ := NewServer(&fakeAuthenticator{want: modelDigest(credential)}, service)
	recorder := httptest.NewRecorder()
	request := authenticatedRequest(t, http.MethodPost, RouteTeamworkAction, `{"action":"offer"}`, credential)
	request.Header.Set(operationKeyHeader, encodeSecret(make([]byte, opaqueSecretBytes)))
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), `"retryable":true`) ||
		!strings.Contains(recorder.Body.String(), `"code":"operation_pending"`) {
		t.Fatalf("service error response = %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestServerHasNoGenericAgentEventOrMCPRoute(t *testing.T) {
	t.Parallel()
	server, _ := NewServer(&fakeAuthenticator{}, &fakeService{})
	for _, path := range []string{"/v1/agent/submit", "/v1/event", "/v1/capability/action", "/mcp", RouteHookCheck + "/"} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
		server.Handler().ServeHTTP(recorder, request)
		if recorder.Code != http.StatusNotFound || recorder.Header().Get("Content-Type") != "application/json" ||
			!strings.Contains(recorder.Body.String(), `"status":"error"`) {
			t.Errorf("route %q = %d %q", path, recorder.Code, recorder.Body.String())
		}
	}
}

func authenticatedRequest(t *testing.T, method, path, body string, credential []byte) *http.Request {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(authorizationHeader, profileScheme+encodeSecret(credential))
	return request
}

func encodeSecret(secret []byte) string { return base64.RawURLEncoding.EncodeToString(secret) }

func modelDigest(value []byte) model.Digest { return model.Sum(value) }
