package agent

import (
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/teamwork"
)

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
	handler       ActionHandler
}

func (handlers ActionHandlers) Validate(input ActionInput) (ValidatedAction, *ControlError) {
	handler, ok := handlers.Action(input.Action)
	if !ok {
		return ValidatedAction{}, NewControlError(CodeUnknownAction, "unknown Teamwork action")
	}
	return handler.validate(input)
}

func (handler ActionHandler) validate(input ActionInput) (ValidatedAction, *ControlError) {
	if !handler.ready || input.Action != handler.Name() {
		return ValidatedAction{}, NewControlError(CodeUnknownAction, "unknown Teamwork action")
	}
	if apiErr := handler.validateContext(input.HasContext); apiErr != nil {
		return ValidatedAction{}, apiErr
	}
	if apiErr := handler.validateSelection(input); apiErr != nil {
		return ValidatedAction{}, apiErr
	}
	paths, apiErr := validateArtifactPaths(input.ArtifactPaths, handler.descriptor.Artifacts())
	if apiErr != nil {
		return ValidatedAction{}, apiErr
	}
	content := handler.descriptor.Content()
	if uint64(len(input.Content)) > uint64(content.MaxBytes()) {
		return ValidatedAction{}, NewControlError(CodeContentTooLarge,
			"Teamwork action content exceeds its managed policy")
	}
	if content.Required() && strings.TrimSpace(input.Content) == "" {
		return ValidatedAction{}, NewControlError(CodeContentRequired,
			"this Teamwork action requires content")
	}
	deadline, apiErr := handler.parseDeadline(input.Deadline)
	if apiErr != nil {
		return ValidatedAction{}, apiErr
	}
	candidate, err := handler.candidate(input.Content, deadline)
	if err != nil {
		code := CodeInvalidArgument
		if strings.Contains(err.Error(), "must not be empty") {
			code = CodeContentRequired
		}
		return ValidatedAction{}, NewControlError(code, boundedCandidateMessage(err))
	}
	return ValidatedAction{Name: handler.Name(), HasContext: input.HasContext,
		ChannelAlias: input.ChannelAlias, Participant: input.Participant, Deadline: deadline,
		Content: input.Content, ArtifactPaths: paths, Candidate: candidate, handler: handler}, nil
}

func (handler ActionHandler) validateContext(hasContext bool) *ControlError {
	if !hasContext && !handler.descriptor.AllowsContext(teamwork.ActionContextNone) {
		return NewControlError(CodeContextRequired, "this Teamwork action requires managed context")
	}
	if hasContext && !allowsManagedActionContext(handler.descriptor) {
		return NewControlError(CodeInvalidArgument, "this Teamwork action forbids managed context")
	}
	return nil
}

func allowsManagedActionContext(descriptor teamwork.ActionDescriptor) bool {
	for _, allowed := range descriptor.AllowedContexts() {
		if allowed != teamwork.ActionContextNone {
			return true
		}
	}
	return false
}

func (handler ActionHandler) validateSelection(input ActionInput) *ControlError {
	policy, hasSelectors := handler.descriptor.Selectors()
	if !hasSelectors && (input.ChannelAlias != "" || input.Participant != "") {
		return NewControlError(CodeInvalidArgument,
			"selectors are forbidden by this Teamwork action policy")
	}
	if apiErr := validateSelector("channel", input.ChannelAlias); apiErr != nil {
		return apiErr
	}
	if apiErr := validateSelector("participant", input.Participant); apiErr != nil {
		return apiErr
	}
	if hasSelectors && input.Participant != "" &&
		!allowsParticipantSelector(policy, input.Participant) {
		return NewControlError(CodeInvalidArgument,
			"participant selector is forbidden by this Teamwork action policy")
	}
	return nil
}

func allowsParticipantSelector(policy teamwork.ActionSelectorPolicy, value string) bool {
	want := teamwork.ParticipantEffectiveAlias
	if value == AgentParticipantAuto {
		want = teamwork.ParticipantAuto
	} else if value == AgentParticipantTeam {
		want = teamwork.ParticipantTeam
	}
	for _, allowed := range policy.Participants() {
		if allowed == want {
			return true
		}
	}
	return false
}

func (handler ActionHandler) parseDeadline(value string) (time.Duration, *ControlError) {
	policy, allowed := handler.descriptor.Deadline()
	if !allowed {
		if value != "" {
			return 0, NewControlError(CodeInvalidArgument,
				"deadline is forbidden by this Teamwork action policy")
		}
		return 0, nil
	}
	if value == "" {
		return policy.Default(), nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration < policy.Minimum() || duration > policy.Maximum() {
		return 0, NewControlError(CodeInvalidArgument,
			"Teamwork action deadline is outside its managed policy")
	}
	return duration, nil
}

func validateSelector(name, value string) *ControlError {
	if value == "" {
		return nil
	}
	if !utf8.ValidString(value) || len(value) > 128 || strings.TrimSpace(value) != value {
		return NewControlError(CodeInvalidArgument, name+" selector is invalid")
	}
	for _, character := range value {
		if character <= 0x20 || character == 0x7f {
			return NewControlError(CodeInvalidArgument, name+" selector is invalid")
		}
	}
	return nil
}

func validateArtifactPaths(paths []string,
	policy teamwork.ActionArtifactPolicy,
) ([]string, *ControlError) {
	if !policy.Allowed() && len(paths) != 0 {
		return nil, NewControlError(CodeArtifactInvalid, "Artifacts are forbidden for this action")
	}
	if uint64(len(paths)) > uint64(policy.MaxRoots()) {
		return nil, NewControlError(CodeArtifactTooLarge,
			"Teamwork action exceeds its managed Artifact root limit")
	}
	result := append([]string(nil), paths...)
	for _, path := range result {
		if path == "" || !utf8.ValidString(path) || uint64(len(path)) > uint64(policy.MaxPathBytes()) ||
			strings.IndexByte(path, 0) >= 0 {
			return nil, NewControlError(CodeArtifactInvalid, "Artifact path is invalid")
		}
	}
	sort.Strings(result)
	for index := 1; index < len(result); index++ {
		if result[index] == result[index-1] {
			return nil, NewControlError(CodeArtifactInvalid, "duplicate Artifact path")
		}
	}
	return result, nil
}

func (action ValidatedAction) matches(handlers ActionHandlers,
	kind model.OperationKind,
) bool {
	handler, ok := handlers.Action(action.Name)
	return ok && action.Candidate != nil && action.handler.ready &&
		action.handler.assetRevision == handlers.assetRevision &&
		action.handler.Name() == handler.Name() && action.handler.OperationKind() == kind &&
		action.handler.EventType() == handler.EventType()
}

func boundedCandidateMessage(err error) string {
	if err == nil {
		return "invalid Teamwork action"
	}
	message := err.Error()
	if len(message) > MaxControlDiagnosticBytes {
		return "invalid Teamwork action input"
	}
	return message
}
