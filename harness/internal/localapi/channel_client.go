package localapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"net/http"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func (c *Client) CreateChannel(ctx context.Context,
	request ChannelCreateRequest,
) (ChannelCreateResponse, *APIError) {
	if !validChannelCreateRequest(request) {
		return ChannelCreateResponse{}, NewAPIError(CodeInvalidArgument, "Channel name is invalid")
	}
	var response ChannelCreateResponse
	if apiErr := c.postChannel(ctx, RouteChannelCreate, request, &response); apiErr != nil {
		return ChannelCreateResponse{}, apiErr
	}
	if apiErr := validateChannelCreateResponse(response); apiErr != nil {
		return ChannelCreateResponse{}, apiErr
	}
	return response, nil
}

func (c *Client) JoinChannel(ctx context.Context,
	request ChannelJoinRequest,
) (ChannelJoinResponse, *APIError) {
	if !validChannelJoinRequest(request) {
		return ChannelJoinResponse{}, NewAPIError(CodeInvalidToken, "Channel invite token is invalid")
	}
	var response ChannelJoinResponse
	if apiErr := c.postChannel(ctx, RouteChannelJoin, request, &response); apiErr != nil {
		return ChannelJoinResponse{}, apiErr
	}
	if apiErr := validateChannelJoinResponse(response); apiErr != nil {
		return ChannelJoinResponse{}, apiErr
	}
	return response, nil
}

func (c *Client) CreateChannelInvite(ctx context.Context,
	request ChannelInviteRequest,
) (ChannelInviteResponse, *APIError) {
	if !validChannelInviteRequest(request) {
		return ChannelInviteResponse{}, NewAPIError(CodeInvalidArgument, "Channel invite options are invalid")
	}
	var response ChannelInviteResponse
	if apiErr := c.postChannel(ctx, RouteChannelInvites, request, &response); apiErr != nil {
		return ChannelInviteResponse{}, apiErr
	}
	if apiErr := validateChannelInviteResponse(response); apiErr != nil {
		return ChannelInviteResponse{}, apiErr
	}
	return response, nil
}

func (c *Client) CloseChannelInvite(ctx context.Context,
	request ChannelInviteCloseRequest,
) (ChannelInviteCloseResponse, *APIError) {
	if !validChannelInviteCloseRequest(request) {
		return ChannelInviteCloseResponse{}, NewAPIError(CodeInvalidArgument, "Channel selector is invalid")
	}
	var response ChannelInviteCloseResponse
	if apiErr := c.postChannel(ctx, RouteChannelInvitesClose, request, &response); apiErr != nil {
		return ChannelInviteCloseResponse{}, apiErr
	}
	if apiErr := validateChannelInviteCloseResponse(response); apiErr != nil {
		return ChannelInviteCloseResponse{}, apiErr
	}
	return response, nil
}

func (c *Client) ReadChannelStatus(ctx context.Context) (ChannelStatusResponse, *APIError) {
	var response ChannelStatusResponse
	if c == nil || c.http == nil || ctx == nil {
		return ChannelStatusResponse{}, invalidControlResponse("local control client is unavailable")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://mnemond"+RouteChannelStatus, nil)
	if err != nil {
		return ChannelStatusResponse{}, invalidControlResponse("local control request cannot be created")
	}
	request.Header.Set(authorizationHeader,
		profileScheme+base64.RawURLEncoding.EncodeToString(c.token[:]))
	if apiErr := c.send(request, &response, MaxChannelResponseBytes); apiErr != nil {
		return ChannelStatusResponse{}, apiErr
	}
	if apiErr := validateChannelStatusResponse(response); apiErr != nil {
		return ChannelStatusResponse{}, apiErr
	}
	return response, nil
}

func (c *Client) RemoveChannelMember(ctx context.Context,
	request ChannelRemoveRequest,
) (ChannelRemoveResponse, *APIError) {
	if !validChannelRemoveRequest(request) {
		return ChannelRemoveResponse{}, NewAPIError(CodeInvalidArgument, "Channel member selector is invalid")
	}
	var response ChannelRemoveResponse
	if apiErr := c.postChannel(ctx, RouteChannelRemove, request, &response); apiErr != nil {
		return ChannelRemoveResponse{}, apiErr
	}
	if apiErr := validateChannelRemoveResponse(response); apiErr != nil {
		return ChannelRemoveResponse{}, apiErr
	}
	return response, nil
}

func (c *Client) LeaveChannel(ctx context.Context,
	request ChannelLeaveRequest,
) (ChannelLeaveResponse, *APIError) {
	if !validChannelLeaveRequest(request) {
		return ChannelLeaveResponse{}, NewAPIError(CodeInvalidArgument, "Channel selector is invalid")
	}
	var response ChannelLeaveResponse
	if apiErr := c.postChannel(ctx, RouteChannelLeave, request, &response); apiErr != nil {
		return ChannelLeaveResponse{}, apiErr
	}
	if apiErr := validateChannelLeaveResponse(response); apiErr != nil {
		return ChannelLeaveResponse{}, apiErr
	}
	return response, nil
}

func (c *Client) AbandonChannel(ctx context.Context,
	request ChannelAbandonRequest,
) (ChannelAbandonResponse, *APIError) {
	if !validChannelAbandonRequest(request) {
		return ChannelAbandonResponse{}, NewAPIError(CodeInvalidArgument,
			"Channel abandon requires force and an exact Channel confirmation")
	}
	var response ChannelAbandonResponse
	if apiErr := c.postChannel(ctx, RouteChannelAbandon, request, &response); apiErr != nil {
		return ChannelAbandonResponse{}, apiErr
	}
	if apiErr := validateChannelAbandonResponse(response); apiErr != nil {
		return ChannelAbandonResponse{}, apiErr
	}
	return response, nil
}

func (c *Client) postChannel(ctx context.Context, route string, input, response any) *APIError {
	if c == nil || c.http == nil || ctx == nil || route == RouteChannelStatus || !IsChannelRoute(route) {
		return invalidControlResponse("local control client is unavailable")
	}
	body, err := model.CanonicalMarshal(input)
	if err != nil || len(body) == 0 || body[0] != '{' || len(body) > MaxRequestBodyBytes {
		return NewAPIError(CodeInvalidArgument, "request cannot be encoded canonically")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://mnemond"+route, bytes.NewReader(body))
	if err != nil {
		return invalidControlResponse("local control request cannot be created")
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(authorizationHeader,
		profileScheme+base64.RawURLEncoding.EncodeToString(c.token[:]))
	return c.send(request, response, MaxChannelResponseBytes)
}
