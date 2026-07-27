package agent

import (
	"errors"
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

type actionRequestDigestWire struct {
	SchemaVersion  int                       `json:"schema_version"`
	Domain         string                    `json:"domain"`
	AssetRevision  model.Digest              `json:"asset_revision"`
	PolicyDigest   model.Digest              `json:"policy_digest"`
	Kind           model.OperationKind       `json:"kind"`
	Action         string                    `json:"action"`
	HasContext     bool                      `json:"has_context"`
	ContextHash    *model.Digest             `json:"context_hash"`
	ChannelAlias   string                    `json:"channel_alias"`
	Participant    string                    `json:"participant"`
	DeadlineNanos  int64                     `json:"deadline_nanos"`
	Content        string                    `json:"content"`
	ContentPolicy  actionContentDigestWire   `json:"content_policy"`
	ArtifactPolicy actionArtifactDigestWire  `json:"artifact_policy"`
	Artifacts      actionArtifactsDigestWire `json:"artifacts"`
}

type actionContentDigestWire struct {
	Required bool   `json:"required"`
	MaxBytes uint32 `json:"max_bytes"`
	Source   string `json:"source"`
}

type actionArtifactDigestWire struct {
	Allowed       bool   `json:"allowed"`
	MaxEntries    uint32 `json:"max_entries"`
	MaxPathBytes  uint32 `json:"max_path_bytes"`
	MaxRoots      uint8  `json:"max_roots"`
	MaxTotalBytes uint64 `json:"max_total_bytes"`
}

type actionArtifactsDigestWire struct {
	Kind  string   `json:"kind"`
	Paths []string `json:"paths"`
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
	if hasSelectors && input.Participant == "" {
		return NewControlError(CodeInvalidArgument,
			"this Teamwork action requires an explicit participant selector")
	}
	if hasSelectors && policy.Participant() != teamwork.ParticipantEffectiveAlias {
		return NewControlError(CodeInvalidArgument,
			"participant selector is forbidden by this Teamwork action policy")
	}
	return nil
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
		action.handler.assetRevision == handlers.AssetRevision() &&
		action.handler.Name() == handler.Name() && action.handler.OperationKind() == kind &&
		action.handler.EventType() == handler.EventType()
}

func (action ValidatedAction) requestDigest(contextHash model.Digest,
	hasContext bool,
) (model.Digest, error) {
	if !action.handler.ready || action.Name != action.handler.Name() || action.Candidate == nil ||
		action.HasContext != hasContext || hasContext == contextHash.IsZero() {
		return model.Digest{}, errors.New("Teamwork Action digest authority is incomplete")
	}
	descriptor := action.handler.Descriptor()
	contentPolicy := descriptor.Content()
	artifactPolicy := descriptor.Artifacts()
	paths := append([]string{}, action.ArtifactPaths...)
	sort.Strings(paths)
	var contextValue *model.Digest
	if hasContext {
		value := contextHash
		contextValue = &value
	}
	request, err := model.JSONFrom(actionRequestDigestWire{
		SchemaVersion: 1, Domain: "mnemon/r5/teamwork-action-request/v1",
		AssetRevision: action.handler.assetRevision,
		PolicyDigest:  model.Sum(descriptor.SourceBytes()),
		Kind:          action.handler.OperationKind(), Action: action.Name,
		HasContext: hasContext, ContextHash: contextValue,
		ChannelAlias: action.ChannelAlias, Participant: action.Participant,
		DeadlineNanos: action.Deadline.Nanoseconds(), Content: action.Content,
		ContentPolicy: actionContentDigestWire{Required: contentPolicy.Required(),
			MaxBytes: contentPolicy.MaxBytes(), Source: string(contentPolicy.Source())},
		ArtifactPolicy: actionArtifactDigestWire{Allowed: artifactPolicy.Allowed(),
			MaxEntries: artifactPolicy.MaxEntries(), MaxPathBytes: artifactPolicy.MaxPathBytes(),
			MaxRoots: artifactPolicy.MaxRoots(), MaxTotalBytes: artifactPolicy.MaxTotalBytes()},
		Artifacts: actionArtifactsDigestWire{Kind: "workspace_relative_path", Paths: paths},
	})
	if err != nil {
		return model.Digest{}, err
	}
	return model.Sum(request.Bytes()), nil
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
