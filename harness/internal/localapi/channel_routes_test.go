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

func TestChannelMutationRoutesRequireKeyAndBindIndependentCanonicalDigest(t *testing.T) {
	credential := repeatedOpaqueBytes(0x53)
	operation := repeatedOpaqueBytes(0x54)
	service := &channelRouteService{}
	server, err := NewServer(fixedAuthenticator{want: modelDigest(credential)}, service)
	if err != nil {
		t.Fatal(err)
	}
	body := `{"name":"alpha"}`
	request := authenticatedRequest(t, http.MethodPost, RouteChannelCreate, body, credential)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code == http.StatusOK || service.createCalls != 0 {
		t.Fatalf("keyless create = %d %s calls=%d",
			recorder.Code, recorder.Body.String(), service.createCalls)
	}

	request = authenticatedRequest(t, http.MethodPost, RouteChannelCreate, body, credential)
	request.Header.Set(operationKeyHeader,
		base64.RawURLEncoding.EncodeToString(operation))
	recorder = httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	wantDigest, apiErr := ChannelCreateRequestDigest(ChannelCreateRequest{Name: "alpha"})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	if service.createCalls != 1 || !service.createMetadata.HasOperationKey ||
		service.createMetadata.OperationKeyHash != modelDigest(operation) ||
		!service.createMetadata.HasRequestDigest ||
		service.createMetadata.RequestDigest != wantDigest ||
		!bytes.Equal(service.createSecret, operation) ||
		strings.Contains(recorder.Body.String(),
			base64.RawURLEncoding.EncodeToString(operation)) {
		t.Fatalf("keyed create metadata=%#v secret=%x response=%d %s",
			service.createMetadata, service.createSecret, recorder.Code, recorder.Body.String())
	}

	createEmpty, _ := ChannelCreateRequestDigest(ChannelCreateRequest{})
	inviteEmpty, _ := ChannelInviteRequestDigest(ChannelInviteRequest{})
	leaveEmpty, _ := ChannelLeaveRequestDigest(ChannelLeaveRequest{})
	if createEmpty == inviteEmpty || createEmpty == leaveEmpty || inviteEmpty == leaveEmpty {
		t.Fatalf("empty Channel routes reused digest create=%s invite=%s leave=%s",
			createEmpty.String(), inviteEmpty.String(), leaveEmpty.String())
	}

	request = authenticatedRequest(t, http.MethodPost, RouteChannelInvites, `{}`, credential)
	recorder = httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code == http.StatusOK || service.inviteCalls != 0 {
		t.Fatalf("keyless invite = %d %s calls=%d",
			recorder.Code, recorder.Body.String(), service.inviteCalls)
	}
	request = authenticatedRequest(t, http.MethodPost, RouteChannelInvites, `{}`, credential)
	request.Header.Set(operationKeyHeader,
		base64.RawURLEncoding.EncodeToString(operation))
	recorder = httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if service.inviteCalls != 1 || service.inviteMetadata.RequestDigest != inviteEmpty ||
		!bytes.Equal(service.inviteSecret, operation) {
		t.Fatalf("keyed invite metadata=%#v secret=%x response=%d %s",
			service.inviteMetadata, service.inviteSecret, recorder.Code, recorder.Body.String())
	}

	request = authenticatedRequest(t, http.MethodPost, RouteChannelLeave,
		`{"channel":"alpha"}`, credential)
	recorder = httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code == http.StatusOK || service.leaveCalls != 0 {
		t.Fatalf("keyless leave = %d %s calls=%d",
			recorder.Code, recorder.Body.String(), service.leaveCalls)
	}
	request = authenticatedRequest(t, http.MethodPost, RouteChannelLeave,
		`{"channel":"alpha"}`, credential)
	request.Header.Set(operationKeyHeader,
		base64.RawURLEncoding.EncodeToString(operation))
	recorder = httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	leaveDigest, _ := ChannelLeaveRequestDigest(ChannelLeaveRequest{Channel: "alpha"})
	if service.leaveCalls != 1 || service.leaveMetadata.RequestDigest != leaveDigest ||
		len(service.leaveMetadata.OperationKeySecret) != 0 {
		t.Fatalf("keyed leave metadata=%#v response=%d %s",
			service.leaveMetadata, recorder.Code, recorder.Body.String())
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
