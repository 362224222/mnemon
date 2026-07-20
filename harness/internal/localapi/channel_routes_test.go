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
	if recorder.Code != http.StatusOK || service.statusCalls != 1 {
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

type channelRouteService struct {
	status      ChannelStatusResponse
	statusCalls int
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
func (service *channelRouteService) ChannelStatus(context.Context,
	RequestMetadata,
) (ChannelStatusResponse, *APIError) {
	service.statusCalls++
	return service.status, nil
}
