package localapi

import (
	"context"
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

type channelRouteService struct {
	status         ChannelStatusResponse
	statusCalls    int
	abandon        ChannelAbandonResponse
	abandonCalls   int
	abandonRequest ChannelAbandonRequest
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
func (*channelRouteService) ChannelCreate(context.Context, RequestMetadata,
	ChannelCreateRequest,
) (ChannelCreateResponse, *APIError) {
	return ChannelCreateResponse{}, NewAPIError(CodeActionNotAllowed, "not used")
}
func (*channelRouteService) ChannelJoin(context.Context, RequestMetadata,
	ChannelJoinRequest,
) (ChannelJoinResponse, *APIError) {
	return ChannelJoinResponse{}, NewAPIError(CodeActionNotAllowed, "not used")
}
func (*channelRouteService) ChannelInvite(context.Context, RequestMetadata,
	ChannelInviteRequest,
) (ChannelInviteResponse, *APIError) {
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
func (*channelRouteService) ChannelLeave(context.Context, RequestMetadata,
	ChannelLeaveRequest,
) (ChannelLeaveResponse, *APIError) {
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
