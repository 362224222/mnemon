package cli

import (
	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/teamwork"
)

type managedTeamworkAction struct {
	validated agent.ValidatedAction
	receipt   teamwork.ActionReceiptPolicy
}

func validateManagedTeamworkAction(request localapi.TeamworkActionRequest,
	hasContext bool,
) (managedTeamworkAction, *localapi.APIError) {
	bundle, err := assets.Load()
	if err != nil {
		return managedTeamworkAction{}, localapi.NewAPIError(localapi.CodeInternal,
			"managed Teamwork Action assets are unavailable")
	}
	policy, err := agent.NewActionPolicy(bundle)
	if err != nil {
		return managedTeamworkAction{}, localapi.NewAPIError(localapi.CodeInternal,
			"managed Teamwork Action policy is unavailable")
	}
	handlers, err := agent.NewActionHandlers(policy)
	if err != nil {
		return managedTeamworkAction{}, localapi.NewAPIError(localapi.CodeInternal,
			"managed Teamwork Action handlers are unavailable")
	}
	handler, found := handlers.Action(request.Action)
	if !found {
		return managedTeamworkAction{}, localapi.NewAPIError(localapi.CodeUnknownAction,
			"unknown Teamwork action")
	}
	validated, validationErr := handlers.Validate(agent.ActionInput{Action: request.Action,
		HasContext: hasContext, ChannelAlias: request.Channel, Participant: request.To,
		Deadline: request.Deadline, Content: request.Content, ArtifactPaths: request.Artifacts})
	if validationErr != nil {
		return managedTeamworkAction{}, localAPIValidationError(validationErr)
	}
	return managedTeamworkAction{validated: validated,
		receipt: handler.Descriptor().Receipt()}, nil
}

func validateManagedTeamworkReceipt(action managedTeamworkAction,
	response localapi.OperationResponse,
) *localapi.APIError {
	receipt := action.receipt
	if response.Action != string(receipt.Action()) || response.Status != string(receipt.Status()) ||
		len(response.Results) == 0 || len(response.Results) > int(receipt.MaxResults()) ||
		receipt.MaxResults() == 1 && len(response.Results) != 1 {
		return invalidManagedTeamworkReceipt()
	}
	handling := ""
	if response.Handling != nil {
		handling = response.Handling.Status
	}
	wantHandling := "completed"
	if receipt.Handling() == teamwork.ReceiptHandlingContextDependent && !action.validated.HasContext {
		wantHandling = ""
	}
	if handling != wantHandling {
		return invalidManagedTeamworkReceipt()
	}
	return nil
}

func invalidManagedTeamworkReceipt() *localapi.APIError {
	return localapi.NewAPIError(localapi.CodeInternal,
		"mnemond returned a receipt outside the managed Action policy")
}
