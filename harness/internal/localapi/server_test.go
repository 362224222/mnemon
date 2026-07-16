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

func TestServerHealthIsAuthenticatedClosedAndIdentityFree(t *testing.T) {
	t.Parallel()
	credential := repeatedOpaqueBytes(0x31)
	revision := model.Sum([]byte("health-assets")).String()
	called := 0
	provider := HealthProviderFunc(func(_ context.Context,
		metadata RequestMetadata,
	) (HealthSnapshot, *APIError) {
		called++
		if metadata.Profile.ID() != model.TeamworkProfileID() || metadata.HasOperationKey ||
			metadata.HasClaimContext || metadata.HasRunAttachment {
			t.Fatalf("health metadata = %#v", metadata)
		}
		return HealthSnapshot{AssetRevision: revision, WorkersReady: true}, nil
	})
	server, err := NewServer(&fakeAuthenticator{want: modelDigest(credential)}, &fakeService{}, provider)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, RouteHealth, nil)
	request.Header.Set(authorizationHeader, profileScheme+encodeSecret(credential))
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	want := `{"asset_revision":"` + revision +
		`","schema_version":1,"status":"ready"}` + "\n"
	if recorder.Code != http.StatusOK || recorder.Body.String() != want || called != 1 ||
		recorder.Header().Get("Cache-Control") != "no-store" ||
		IsAgentRoute(RouteHealth) || !IsControlRoute(RouteHealth) {
		t.Fatalf("health response = %d %q headers=%v called=%d", recorder.Code,
			recorder.Body.String(), recorder.Header(), called)
	}
}

func TestServerHealthRejectsMethodsBodiesCapabilitiesAndBadAuth(t *testing.T) {
	t.Parallel()
	credential := repeatedOpaqueBytes(0x32)
	secret := encodeSecret(repeatedOpaqueBytes(0x33))
	called := 0
	provider := HealthProviderFunc(func(context.Context,
		RequestMetadata,
	) (HealthSnapshot, *APIError) {
		called++
		return HealthSnapshot{AssetRevision: model.Sum([]byte("health-assets")).String(),
			WorkersReady: true}, nil
	})
	server, _ := NewServer(&fakeAuthenticator{want: modelDigest(credential)}, &fakeService{}, provider)
	tests := []struct {
		name   string
		method string
		path   string
		body   string
		mutate func(*http.Request)
		status int
		allow  string
	}{
		{name: "method", method: http.MethodPost, path: RouteHealth, status: http.StatusMethodNotAllowed, allow: http.MethodGet},
		{name: "body", method: http.MethodGet, path: RouteHealth, body: `{}`, status: http.StatusBadRequest},
		{name: "query", method: http.MethodGet, path: RouteHealth + "?detail=1", status: http.StatusBadRequest},
		{name: "operation", method: http.MethodGet, mutate: func(request *http.Request) {
			request.Header.Set(operationKeyHeader, secret)
		}, status: http.StatusBadRequest},
		{name: "claim", method: http.MethodGet, mutate: func(request *http.Request) {
			request.Header.Set(claimContextHeader, secret)
		}, status: http.StatusBadRequest},
		{name: "attachment", method: http.MethodGet, mutate: func(request *http.Request) {
			request.Header.Set(runAttachmentHeader, secret)
		}, status: http.StatusBadRequest},
		{name: "authentication", method: http.MethodGet, mutate: func(request *http.Request) {
			request.Header.Set(authorizationHeader, profileScheme+encodeSecret(repeatedOpaqueBytes(0x34)))
		}, status: http.StatusUnauthorized},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := test.path
			if path == "" {
				path = RouteHealth
			}
			request := httptest.NewRequest(test.method, path, strings.NewReader(test.body))
			request.Header.Set(authorizationHeader, profileScheme+encodeSecret(credential))
			if test.mutate != nil {
				test.mutate(request)
			}
			recorder := httptest.NewRecorder()
			server.Handler().ServeHTTP(recorder, request)
			if recorder.Code != test.status || recorder.Header().Get("Allow") != test.allow {
				t.Fatalf("health rejection = %d %s Allow=%q", recorder.Code,
					recorder.Body.String(), recorder.Header().Get("Allow"))
			}
		})
	}
	if called != 0 {
		t.Fatalf("rejected requests called health provider %d times", called)
	}
}

func TestServerHealthProviderFailureIsClosed(t *testing.T) {
	t.Parallel()
	credential := repeatedOpaqueBytes(0x35)
	tests := []struct {
		name     string
		provider []HealthProvider
		status   int
		code     ErrorCode
	}{
		{name: "missing", status: http.StatusInternalServerError, code: CodeInternal},
		{name: "invalid snapshot", provider: []HealthProvider{HealthProviderFunc(func(context.Context,
			RequestMetadata,
		) (HealthSnapshot, *APIError) {
			return HealthSnapshot{AssetRevision: "peer-secret", WorkersReady: true}, nil
		})}, status: http.StatusInternalServerError, code: CodeInternal},
		{name: "provider error", provider: []HealthProvider{HealthProviderFunc(func(context.Context,
			RequestMetadata,
		) (HealthSnapshot, *APIError) {
			return HealthSnapshot{}, NewAPIError(CodeAssetRevisionMismatch, "managed asset revision differs")
		})}, status: http.StatusBadRequest, code: CodeAssetRevisionMismatch},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, err := NewServer(&fakeAuthenticator{want: modelDigest(credential)}, &fakeService{}, test.provider...)
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodGet, RouteHealth, nil)
			request.Header.Set(authorizationHeader, profileScheme+encodeSecret(credential))
			recorder := httptest.NewRecorder()
			server.Handler().ServeHTTP(recorder, request)
			var envelope APIError
			if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil ||
				recorder.Code != test.status || envelope.Code != test.code {
				t.Fatalf("health failure = %d %s (%v)", recorder.Code, recorder.Body.String(), err)
			}
		})
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
