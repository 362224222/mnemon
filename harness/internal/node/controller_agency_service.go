package node

import (
	"context"
	"errors"
)

func validateOptionalAgencyService(service AgencyService) error {
	if service != nil && isNilNodeInterface(service) {
		return errors.New("mnemond controller Agency service is unavailable")
	}
	return nil
}

// controllerAgencyAdmissionService shares the controller's one mutation gate
// without adding R7 methods to the legacy Service interface.
type controllerAgencyAdmissionService struct {
	gate ManagedAdmission
	next AgencyService
}

func (service controllerAgencyAdmissionService) AgencyAttach(ctx context.Context) (AgencyAttachment, error) {
	release, err := enterAgencyAdmission(ctx, service.gate)
	if err != nil {
		return AgencyAttachment{}, err
	}
	defer release()
	return service.next.AgencyAttach(ctx)
}

func (service controllerAgencyAdmissionService) AgencyCurrent(ctx context.Context,
	authority AgencyAuthority,
) (AgencyView, error) {
	release, err := enterAgencyAdmission(ctx, service.gate)
	if err != nil {
		return AgencyView{}, err
	}
	defer release()
	return service.next.AgencyCurrent(ctx, authority)
}

func (service controllerAgencyAdmissionService) AgencySubmit(ctx context.Context,
	authority AgencyAuthority, submission AgencySubmission,
) (AgencyReceipt, error) {
	release, err := enterAgencyAdmission(ctx, service.gate)
	if err != nil {
		return AgencyReceipt{}, err
	}
	defer release()
	return service.next.AgencySubmit(ctx, authority, submission)
}

func (service controllerAgencyAdmissionService) AgencyCapture(ctx context.Context,
	content []byte,
) (AgencyArtifactCapture, error) {
	release, err := enterAgencyAdmission(ctx, service.gate)
	if err != nil {
		return AgencyArtifactCapture{}, err
	}
	defer release()
	return service.next.AgencyCapture(ctx, content)
}

func (service controllerAgencyAdmissionService) AgencyStatus(ctx context.Context) (
	AgencyStatusSnapshot, error,
) {
	release, err := enterAgencyAdmission(ctx, service.gate)
	if err != nil {
		return AgencyStatusSnapshot{}, err
	}
	defer release()
	return service.next.AgencyStatus(ctx)
}

func enterAgencyAdmission(ctx context.Context, gate ManagedAdmission) (func(), error) {
	if gate == nil {
		return nil, ErrManagedAdmission
	}
	release, err := gate.Enter(ctx)
	if err != nil || release == nil {
		return nil, ErrManagedAdmission
	}
	return release, nil
}

// controllerAgencyService and controllerChannelAgencyService preserve optional
// interfaces through composition. The local API discovers AgencyService by
// type assertion; the legacy Service remains unchanged.
type controllerAgencyService struct {
	Service
	AgencyService
}

type controllerChannelAgencyService struct {
	Service
	ChannelService
	AgencyService
}

var _ AgencyService = controllerAgencyAdmissionService{}
var _ AgencyService = controllerAgencyService{}
var _ AgencyService = controllerChannelAgencyService{}
var _ ChannelService = controllerChannelAgencyService{}
