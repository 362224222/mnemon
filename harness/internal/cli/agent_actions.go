package cli

import (
	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
)

func validateManagedTeamworkAction(request localapi.TeamworkActionRequest,
	hasContext bool,
) (agent.ValidatedAction, *localapi.APIError) {
	bundle, err := assets.Load()
	if err != nil {
		return agent.ValidatedAction{}, localapi.NewAPIError(localapi.CodeInternal,
			"managed Teamwork Action assets are unavailable")
	}
	policy, err := agent.NewActionPolicy(bundle)
	if err != nil {
		return agent.ValidatedAction{}, localapi.NewAPIError(localapi.CodeInternal,
			"managed Teamwork Action policy is unavailable")
	}
	handlers, err := agent.NewActionHandlers(policy)
	if err != nil {
		return agent.ValidatedAction{}, localapi.NewAPIError(localapi.CodeInternal,
			"managed Teamwork Action handlers are unavailable")
	}
	validated, validationErr := handlers.Validate(agent.ActionInput{Action: request.Action,
		HasContext: hasContext, ChannelAlias: request.Channel, Participant: request.To,
		Deadline: request.Deadline, Content: request.Content, ArtifactPaths: request.Artifacts})
	if validationErr != nil {
		return agent.ValidatedAction{}, localAPIValidationError(validationErr)
	}
	return validated, nil
}
