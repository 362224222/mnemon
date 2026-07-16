package agent

import (
	"errors"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
)

const MaxActionArtifacts = 16

type ActionInput struct {
	Action        string
	HasContext    bool
	ChannelAlias  string
	Participant   string
	Deadline      string
	Content       string
	ArtifactPaths []string
}

type ValidatedAction struct {
	Name          string
	HasContext    bool
	ChannelAlias  string
	Participant   string
	Deadline      time.Duration
	Content       string
	ArtifactPaths []string
	Candidate     event.AgentCandidate
}

func ValidateAction(input ActionInput) (ValidatedAction, *localapi.APIError) {
	if !validActionName(input.Action) {
		return ValidatedAction{}, localapi.NewAPIError(localapi.CodeUnknownAction, "unknown Teamwork action")
	}
	if input.Action != "offer" && !input.HasContext {
		return ValidatedAction{}, localapi.NewAPIError(localapi.CodeContextRequired, "this Teamwork action requires managed context")
	}
	if input.Action != "offer" && (input.ChannelAlias != "" || input.Participant != "" || input.Deadline != "") {
		return ValidatedAction{}, localapi.NewAPIError(localapi.CodeInvalidArgument, "selectors and deadline are only valid for offer")
	}
	if apiErr := validateSelector("channel", input.ChannelAlias); apiErr != nil {
		return ValidatedAction{}, apiErr
	}
	if apiErr := validateSelector("participant", input.Participant); apiErr != nil {
		return ValidatedAction{}, apiErr
	}
	paths, apiErr := validateArtifactPaths(input.ArtifactPaths)
	if apiErr != nil {
		return ValidatedAction{}, apiErr
	}
	artifactsAllowed := input.Action == "offer" || input.Action == "deliver" || input.Action == "rework"
	if !artifactsAllowed && len(paths) != 0 {
		return ValidatedAction{}, localapi.NewAPIError(localapi.CodeArtifactInvalid, "Artifacts are forbidden for this action")
	}
	if actionContentRequired(input.Action) && strings.TrimSpace(input.Content) == "" {
		return ValidatedAction{}, localapi.NewAPIError(localapi.CodeContentRequired, "this Teamwork action requires content")
	}

	deadline, apiErr := parseOfferDeadline(input.Action, input.Deadline)
	if apiErr != nil {
		return ValidatedAction{}, apiErr
	}
	candidate, err := actionCandidate(input.Action, input.Content, deadline)
	if err != nil {
		code := localapi.CodeInvalidArgument
		if strings.Contains(err.Error(), "must not be empty") {
			code = localapi.CodeContentRequired
		} else if errors.Is(err, event.ErrInvalidCandidate) && len(input.Content) > 8192 {
			code = localapi.CodeContentTooLarge
		}
		return ValidatedAction{}, localapi.NewAPIError(code, boundedCandidateMessage(err))
	}
	return ValidatedAction{Name: input.Action, HasContext: input.HasContext,
		ChannelAlias: input.ChannelAlias, Participant: input.Participant, Deadline: deadline,
		Content: input.Content, ArtifactPaths: paths, Candidate: candidate}, nil
}

func validActionName(action string) bool {
	return action == "offer" || action == "accept" || action == "decline" ||
		action == "deliver" || action == "rework" || action == "close" || action == "cancel"
}

func actionContentRequired(action string) bool {
	return action == "offer" || action == "decline" || action == "deliver" ||
		action == "rework" || action == "cancel"
}

func actionCandidate(action, content string, deadline time.Duration) (event.AgentCandidate, error) {
	switch action {
	case "offer":
		return event.NewOfferCandidate(content, deadline)
	case "accept":
		return event.NewAcceptCandidate(content)
	case "decline":
		return event.NewDeclineCandidate(content)
	case "deliver":
		return event.NewDeliverCandidate(content)
	case "rework":
		return event.NewReworkCandidate(content)
	case "close":
		return event.NewCloseCandidate(content)
	case "cancel":
		return event.NewCancelCandidate(content)
	default:
		return nil, errors.New("unknown Teamwork action")
	}
}

func parseOfferDeadline(action, value string) (time.Duration, *localapi.APIError) {
	if value == "" {
		return 0, nil
	}
	if action != "offer" {
		return 0, localapi.NewAPIError(localapi.CodeInvalidArgument, "deadline is only valid for offer")
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration < event.MinimumOfferDeadline || duration > event.MaximumOfferDeadline {
		return 0, localapi.NewAPIError(localapi.CodeInvalidArgument, "offer deadline must be a Go duration from 5m through 168h")
	}
	return duration, nil
}

func validateSelector(name, value string) *localapi.APIError {
	if value == "" {
		return nil
	}
	if !utf8.ValidString(value) || len(value) > 128 || strings.TrimSpace(value) != value {
		return localapi.NewAPIError(localapi.CodeInvalidArgument, name+" selector is invalid")
	}
	for _, character := range value {
		if character <= 0x20 || character == 0x7f {
			return localapi.NewAPIError(localapi.CodeInvalidArgument, name+" selector is invalid")
		}
	}
	return nil
}

func validateArtifactPaths(paths []string) ([]string, *localapi.APIError) {
	if len(paths) > MaxActionArtifacts {
		return nil, localapi.NewAPIError(localapi.CodeArtifactTooLarge, "an action accepts at most 16 Artifact paths")
	}
	result := append([]string(nil), paths...)
	for _, path := range result {
		if path == "" || !utf8.ValidString(path) || len(path) > 4096 || strings.IndexByte(path, 0) >= 0 {
			return nil, localapi.NewAPIError(localapi.CodeArtifactInvalid, "Artifact path is invalid")
		}
	}
	sort.Strings(result)
	for index := 1; index < len(result); index++ {
		if result[index] == result[index-1] {
			return nil, localapi.NewAPIError(localapi.CodeArtifactInvalid, "duplicate Artifact path")
		}
	}
	return result, nil
}

func boundedCandidateMessage(err error) string {
	if err == nil {
		return "invalid Teamwork action"
	}
	message := err.Error()
	if len(message) > localapi.MaxDiagnosticBytes {
		return "invalid Teamwork action input"
	}
	return message
}
