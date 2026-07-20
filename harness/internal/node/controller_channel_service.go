package node

import (
	"context"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
)

type controllerChannelService struct {
	localapi.Service
	gate     ManagedAdmission
	channels localapi.ChannelService
}

func (service controllerChannelService) ChannelCreate(ctx context.Context, metadata localapi.RequestMetadata,
	request localapi.ChannelCreateRequest,
) (localapi.ChannelCreateResponse, *localapi.APIError) {
	release, apiErr := enterControllerAdmission(ctx, service.gate)
	if apiErr != nil {
		return localapi.ChannelCreateResponse{}, apiErr
	}
	defer release()
	return service.channels.ChannelCreate(ctx, metadata, request)
}

func (service controllerChannelService) ChannelJoin(ctx context.Context, metadata localapi.RequestMetadata,
	request localapi.ChannelJoinRequest,
) (localapi.ChannelJoinResponse, *localapi.APIError) {
	release, apiErr := enterControllerAdmission(ctx, service.gate)
	if apiErr != nil {
		return localapi.ChannelJoinResponse{}, apiErr
	}
	defer release()
	return service.channels.ChannelJoin(ctx, metadata, request)
}

func (service controllerChannelService) ChannelInvite(ctx context.Context,
	metadata localapi.RequestMetadata, request localapi.ChannelInviteRequest,
) (localapi.ChannelInviteResponse, *localapi.APIError) {
	release, apiErr := enterControllerAdmission(ctx, service.gate)
	if apiErr != nil {
		return localapi.ChannelInviteResponse{}, apiErr
	}
	defer release()
	return service.channels.ChannelInvite(ctx, metadata, request)
}

func (service controllerChannelService) ChannelInviteClose(ctx context.Context,
	metadata localapi.RequestMetadata, request localapi.ChannelInviteCloseRequest,
) (localapi.ChannelInviteCloseResponse, *localapi.APIError) {
	release, apiErr := enterControllerAdmission(ctx, service.gate)
	if apiErr != nil {
		return localapi.ChannelInviteCloseResponse{}, apiErr
	}
	defer release()
	return service.channels.ChannelInviteClose(ctx, metadata, request)
}

func (service controllerChannelService) ChannelStatus(ctx context.Context,
	metadata localapi.RequestMetadata,
) (localapi.ChannelStatusResponse, *localapi.APIError) {
	release, apiErr := enterControllerAdmission(ctx, service.gate)
	if apiErr != nil {
		return localapi.ChannelStatusResponse{}, apiErr
	}
	defer release()
	return service.channels.ChannelStatus(ctx, metadata)
}

func (service controllerChannelService) ChannelRemove(ctx context.Context,
	metadata localapi.RequestMetadata, request localapi.ChannelRemoveRequest,
) (localapi.ChannelRemoveResponse, *localapi.APIError) {
	release, apiErr := enterControllerAdmission(ctx, service.gate)
	if apiErr != nil {
		return localapi.ChannelRemoveResponse{}, apiErr
	}
	defer release()
	return service.channels.ChannelRemove(ctx, metadata, request)
}

func (service controllerChannelService) ChannelLeave(ctx context.Context,
	metadata localapi.RequestMetadata, request localapi.ChannelLeaveRequest,
) (localapi.ChannelLeaveResponse, *localapi.APIError) {
	release, apiErr := enterControllerAdmission(ctx, service.gate)
	if apiErr != nil {
		return localapi.ChannelLeaveResponse{}, apiErr
	}
	defer release()
	return service.channels.ChannelLeave(ctx, metadata, request)
}

var _ localapi.ChannelService = controllerChannelService{}
