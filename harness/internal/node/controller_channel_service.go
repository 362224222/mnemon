package node

import (
	"context"
)

type controllerChannelService struct {
	Service
	gate     ManagedAdmission
	channels ChannelService
}

func (service controllerChannelService) ChannelCreate(ctx context.Context, metadata RequestMetadata,
	request ChannelCreateRequest,
) (ChannelCreateResponse, *APIError) {
	release, apiErr := enterControllerAdmission(ctx, service.gate)
	if apiErr != nil {
		return ChannelCreateResponse{}, apiErr
	}
	defer release()
	return service.channels.ChannelCreate(ctx, metadata, request)
}

func (service controllerChannelService) ChannelJoin(ctx context.Context, metadata RequestMetadata,
	request ChannelJoinRequest,
) (ChannelJoinResponse, *APIError) {
	release, apiErr := enterControllerAdmission(ctx, service.gate)
	if apiErr != nil {
		return ChannelJoinResponse{}, apiErr
	}
	defer release()
	return service.channels.ChannelJoin(ctx, metadata, request)
}

func (service controllerChannelService) ChannelInvite(ctx context.Context,
	metadata RequestMetadata, request ChannelInviteRequest,
) (ChannelInviteResponse, *APIError) {
	release, apiErr := enterControllerAdmission(ctx, service.gate)
	if apiErr != nil {
		return ChannelInviteResponse{}, apiErr
	}
	defer release()
	return service.channels.ChannelInvite(ctx, metadata, request)
}

func (service controllerChannelService) ChannelInviteClose(ctx context.Context,
	metadata RequestMetadata, request ChannelInviteCloseRequest,
) (ChannelInviteCloseResponse, *APIError) {
	release, apiErr := enterControllerAdmission(ctx, service.gate)
	if apiErr != nil {
		return ChannelInviteCloseResponse{}, apiErr
	}
	defer release()
	return service.channels.ChannelInviteClose(ctx, metadata, request)
}

func (service controllerChannelService) ChannelStatus(ctx context.Context,
	metadata RequestMetadata,
) (ChannelStatusResponse, *APIError) {
	release, apiErr := enterControllerAdmission(ctx, service.gate)
	if apiErr != nil {
		return ChannelStatusResponse{}, apiErr
	}
	defer release()
	return service.channels.ChannelStatus(ctx, metadata)
}

func (service controllerChannelService) ChannelRemove(ctx context.Context,
	metadata RequestMetadata, request ChannelRemoveRequest,
) (ChannelRemoveResponse, *APIError) {
	release, apiErr := enterControllerAdmission(ctx, service.gate)
	if apiErr != nil {
		return ChannelRemoveResponse{}, apiErr
	}
	defer release()
	return service.channels.ChannelRemove(ctx, metadata, request)
}

func (service controllerChannelService) ChannelLeave(ctx context.Context,
	metadata RequestMetadata, request ChannelLeaveRequest,
) (ChannelLeaveResponse, *APIError) {
	release, apiErr := enterControllerAdmission(ctx, service.gate)
	if apiErr != nil {
		return ChannelLeaveResponse{}, apiErr
	}
	defer release()
	return service.channels.ChannelLeave(ctx, metadata, request)
}

func (service controllerChannelService) ChannelAbandon(ctx context.Context,
	metadata RequestMetadata, request ChannelAbandonRequest,
) (ChannelAbandonResponse, *APIError) {
	release, apiErr := enterControllerAdmission(ctx, service.gate)
	if apiErr != nil {
		return ChannelAbandonResponse{}, apiErr
	}
	defer release()
	return service.channels.ChannelAbandon(ctx, metadata, request)
}

var _ ChannelService = controllerChannelService{}
