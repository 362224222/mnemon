package localapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestChannelRoutesAreAuthenticatedClosedAndNeverAcceptTokenInURL(t *testing.T) {
	credential := repeatedOpaqueBytes(0x51)
	service := &channelRouteService{status: ChannelStatusResponse{SchemaVersion: SchemaVersion,
		Status: "ok", Channels: []ChannelView{validChannelContractView()}}}
	server, err := NewServer(fixedAuthenticator{want: modelDigest(credential)}, service)
	if err != nil {
		t.Fatal(err)
	}
	request := authenticatedRequest(t, http.MethodGet, RouteChannelStatus, "", credential)
	request.Header.Del("Content-Type")
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || service.statusCalls != 1 ||
		recorder.Body.Len() > MaxChannelResponseBytes ||
		strings.Contains(recorder.Body.String(), `"publications"`) {
		t.Fatalf("Channel status = %d %s", recorder.Code, recorder.Body.String())
	}
	for _, path := range []string{RouteChannelJoin + "?token=mnch1_secret", RouteChannelStatus + "?channel=alpha"} {
		request := authenticatedRequest(t, http.MethodGet, path, "", credential)
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		if recorder.Code == http.StatusOK || strings.Contains(recorder.Body.String(), "mnch1_secret") {
			t.Fatalf("query-bearing Channel route %q = %d %s", path, recorder.Code, recorder.Body.String())
		}
	}
}

func TestChannelAbandonRouteRequiresExplicitMatchingDestructiveConfirmation(t *testing.T) {
	credential := repeatedOpaqueBytes(0x52)
	service := &channelRouteService{abandon: ChannelAbandonResponse{SchemaVersion: SchemaVersion,
		Status: "abandoned", Channel: "alpha", TransitionedAt: "2026-07-21T10:00:00Z",
		Evidence: ChannelForensicCounts{MemberRecords: 3}}}
	server, err := NewServer(fixedAuthenticator{want: modelDigest(credential)}, service)
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{
		`{"channel":"alpha","confirm_channel":"alpha","force":false}`,
		`{"channel":"alpha","confirm_channel":"beta","force":true}`,
	} {
		request := authenticatedRequest(t, http.MethodPost, RouteChannelAbandon, body, credential)
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		if recorder.Code == http.StatusOK || service.abandonCalls != 0 {
			t.Fatalf("unsafe abandon %s = %d %s", body, recorder.Code, recorder.Body.String())
		}
	}
	request := authenticatedRequest(t, http.MethodPost, RouteChannelAbandon,
		`{"channel":"alpha","confirm_channel":"alpha","force":true}`, credential)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || service.abandonCalls != 1 ||
		service.abandonRequest.Channel != "alpha" {
		t.Fatalf("confirmed abandon = %d %s request=%#v calls=%d", recorder.Code,
			recorder.Body.String(), service.abandonRequest, service.abandonCalls)
	}
}

type channelMutationRouteFixture struct {
	credential []byte
	operation  []byte
	service    *channelRouteService
	handler    http.Handler
}

func newChannelMutationRouteFixture(t *testing.T) *channelMutationRouteFixture {
	t.Helper()
	credential := repeatedOpaqueBytes(0x53)
	service := &channelRouteService{}
	server, err := NewServer(fixedAuthenticator{want: modelDigest(credential)}, service)
	if err != nil {
		t.Fatal(err)
	}
	return &channelMutationRouteFixture{credential: credential,
		operation: repeatedOpaqueBytes(0x54), service: service, handler: server.Handler()}
}

func (fixture *channelMutationRouteFixture) post(t *testing.T, route, body string,
	withOperationKey bool,
) *httptest.ResponseRecorder {
	t.Helper()
	request := authenticatedRequest(t, http.MethodPost, route, body, fixture.credential)
	if withOperationKey {
		request.Header.Set(operationKeyHeader,
			base64.RawURLEncoding.EncodeToString(fixture.operation))
	}
	recorder := httptest.NewRecorder()
	fixture.handler.ServeHTTP(recorder, request)
	return recorder
}

func (fixture *channelMutationRouteFixture) calls(route string) int {
	switch route {
	case RouteChannelCreate:
		return fixture.service.createCalls
	case RouteChannelInvites:
		return fixture.service.inviteCalls
	case RouteChannelLeave:
		return fixture.service.leaveCalls
	default:
		return -1
	}
}

func TestChannelMutationRoutesRequireOperationKey(t *testing.T) {
	tests := []struct {
		name  string
		route string
		body  string
	}{
		{name: "create", route: RouteChannelCreate, body: `{"name":"alpha"}`},
		{name: "invite", route: RouteChannelInvites, body: `{}`},
		{name: "leave", route: RouteChannelLeave, body: `{"channel":"alpha"}`},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			fixture := newChannelMutationRouteFixture(t)
			recorder := fixture.post(t, testCase.route, testCase.body, false)
			if recorder.Code == http.StatusOK || fixture.calls(testCase.route) != 0 {
				t.Fatalf("keyless route = %d %s calls=%d", recorder.Code,
					recorder.Body.String(), fixture.calls(testCase.route))
			}
		})
	}
}

func TestChannelCreateRouteBindsOperationKeyAndCanonicalDigest(t *testing.T) {
	fixture := newChannelMutationRouteFixture(t)
	recorder := fixture.post(t, RouteChannelCreate, `{"name":"alpha"}`, true)
	wantDigest, apiErr := ChannelCreateRequestDigest(ChannelCreateRequest{Name: "alpha"})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	metadata := fixture.service.createMetadata
	if fixture.service.createCalls != 1 || !metadata.HasOperationKey ||
		metadata.OperationKeyHash != modelDigest(fixture.operation) ||
		!metadata.HasRequestDigest || metadata.RequestDigest != wantDigest ||
		!bytes.Equal(fixture.service.createSecret, fixture.operation) ||
		strings.Contains(recorder.Body.String(),
			base64.RawURLEncoding.EncodeToString(fixture.operation)) {
		t.Fatalf("keyed create metadata=%#v secret=%x response=%d %s",
			metadata, fixture.service.createSecret, recorder.Code, recorder.Body.String())
	}
}

func TestChannelInviteRouteBindsOperationKeyAndCanonicalDigest(t *testing.T) {
	fixture := newChannelMutationRouteFixture(t)
	recorder := fixture.post(t, RouteChannelInvites, `{}`, true)
	wantDigest, _ := ChannelInviteRequestDigest(ChannelInviteRequest{})
	if fixture.service.inviteCalls != 1 ||
		fixture.service.inviteMetadata.RequestDigest != wantDigest ||
		!bytes.Equal(fixture.service.inviteSecret, fixture.operation) {
		t.Fatalf("keyed invite metadata=%#v secret=%x response=%d %s",
			fixture.service.inviteMetadata, fixture.service.inviteSecret,
			recorder.Code, recorder.Body.String())
	}
}

func TestChannelLeaveRouteBindsCanonicalDigestWithoutRetainingSecret(t *testing.T) {
	fixture := newChannelMutationRouteFixture(t)
	recorder := fixture.post(t, RouteChannelLeave, `{"channel":"alpha"}`, true)
	wantDigest, _ := ChannelLeaveRequestDigest(ChannelLeaveRequest{Channel: "alpha"})
	if fixture.service.leaveCalls != 1 ||
		fixture.service.leaveMetadata.RequestDigest != wantDigest ||
		len(fixture.service.leaveMetadata.OperationKeySecret) != 0 {
		t.Fatalf("keyed leave metadata=%#v response=%d %s",
			fixture.service.leaveMetadata, recorder.Code, recorder.Body.String())
	}
}

func TestChannelMutationRequestDigestsAreRouteSeparated(t *testing.T) {
	create, _ := ChannelCreateRequestDigest(ChannelCreateRequest{})
	invite, _ := ChannelInviteRequestDigest(ChannelInviteRequest{})
	leave, _ := ChannelLeaveRequestDigest(ChannelLeaveRequest{})
	if create == invite || create == leave || invite == leave {
		t.Fatalf("empty Channel routes reused digest create=%s invite=%s leave=%s",
			create.String(), invite.String(), leave.String())
	}
}

type channelRouteService struct {
	status         ChannelStatusResponse
	statusCalls    int
	abandon        ChannelAbandonResponse
	abandonCalls   int
	abandonRequest ChannelAbandonRequest
	createCalls    int
	createMetadata RequestMetadata
	createSecret   []byte
	inviteCalls    int
	inviteMetadata RequestMetadata
	inviteSecret   []byte
	leaveCalls     int
	leaveMetadata  RequestMetadata
}

func (*channelRouteService) HookCheck(context.Context, RequestMetadata,
	HookCheckRequest,
) (HookCheckResponse, *APIError) {
	return HookCheckResponse{SchemaVersion: SchemaVersion}, nil
}
func (*channelRouteService) AgentCurrent(context.Context, RequestMetadata,
	AgentCurrentRequest,
) (AgentCurrentResponse, *APIError) {
	return AgentCurrentResponse{SchemaVersion: SchemaVersion, Status: "none"}, nil
}
func (*channelRouteService) TeamworkAction(context.Context, RequestMetadata,
	TeamworkActionRequest,
) (OperationResponse, *APIError) {
	return OperationResponse{}, NewAPIError(CodeActionNotAllowed, "not used")
}
func (*channelRouteService) AgentResolve(context.Context, RequestMetadata,
	AgentResolveRequest,
) (OperationResponse, *APIError) {
	return OperationResponse{}, NewAPIError(CodeActionNotAllowed, "not used")
}
func (service *channelRouteService) ChannelCreate(_ context.Context, metadata RequestMetadata,
	_ ChannelCreateRequest,
) (ChannelCreateResponse, *APIError) {
	service.createCalls++
	service.createMetadata = metadata
	service.createSecret = append([]byte(nil), metadata.OperationKeySecret...)
	return ChannelCreateResponse{}, NewAPIError(CodeActionNotAllowed, "not used")
}
func (*channelRouteService) ChannelJoin(context.Context, RequestMetadata,
	ChannelJoinRequest,
) (ChannelJoinResponse, *APIError) {
	return ChannelJoinResponse{}, NewAPIError(CodeActionNotAllowed, "not used")
}
func (service *channelRouteService) ChannelInvite(_ context.Context, metadata RequestMetadata,
	_ ChannelInviteRequest,
) (ChannelInviteResponse, *APIError) {
	service.inviteCalls++
	service.inviteMetadata = metadata
	service.inviteSecret = append([]byte(nil), metadata.OperationKeySecret...)
	return ChannelInviteResponse{}, NewAPIError(CodeActionNotAllowed, "not used")
}
func (*channelRouteService) ChannelInviteClose(context.Context, RequestMetadata,
	ChannelInviteCloseRequest,
) (ChannelInviteCloseResponse, *APIError) {
	return ChannelInviteCloseResponse{}, NewAPIError(CodeActionNotAllowed, "not used")
}
func (*channelRouteService) ChannelRemove(context.Context, RequestMetadata,
	ChannelRemoveRequest,
) (ChannelRemoveResponse, *APIError) {
	return ChannelRemoveResponse{}, NewAPIError(CodeActionNotAllowed, "not used")
}
func (service *channelRouteService) ChannelLeave(_ context.Context, metadata RequestMetadata,
	_ ChannelLeaveRequest,
) (ChannelLeaveResponse, *APIError) {
	service.leaveCalls++
	service.leaveMetadata = metadata
	return ChannelLeaveResponse{}, NewAPIError(CodeActionNotAllowed, "not used")
}
func (service *channelRouteService) ChannelAbandon(_ context.Context, _ RequestMetadata,
	request ChannelAbandonRequest,
) (ChannelAbandonResponse, *APIError) {
	service.abandonCalls++
	service.abandonRequest = request
	return service.abandon, nil
}
func (service *channelRouteService) ChannelStatus(context.Context,
	RequestMetadata,
) (ChannelStatusResponse, *APIError) {
	service.statusCalls++
	return service.status, nil
}
