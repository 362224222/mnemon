package agent

import (
	"strings"
	"unicode/utf8"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type ResolveInput struct {
	Decision   string
	HasContext bool
	Content    string
}

type ValidatedResolve struct {
	Decision string
	Content  string
	Kind     model.OperationKind
}

func ValidateResolve(input ResolveInput) (ValidatedResolve, *ControlError) {
	if !input.HasContext {
		return ValidatedResolve{}, NewControlError(CodeContextRequired, "handling resolution requires managed context")
	}
	kind := model.OperationKind("")
	switch input.Decision {
	case "no-action":
		kind = model.OperationResolveNoAction
	case "retry":
		kind = model.OperationResolveRetry
	case "reject":
		kind = model.OperationResolveReject
	default:
		return ValidatedResolve{}, NewControlError(CodeUnknownAction, "unknown handling resolution")
	}
	if input.Decision == "reject" && strings.TrimSpace(input.Content) == "" {
		return ValidatedResolve{}, NewControlError(CodeContentRequired, "reject resolution requires a reason")
	}
	if !utf8.ValidString(input.Content) {
		return ValidatedResolve{}, NewControlError(CodeInvalidArgument, "resolution content must be valid UTF-8")
	}
	if len(input.Content) > 8192 {
		return ValidatedResolve{}, NewControlError(CodeContentTooLarge, "resolution content exceeds 8192 bytes")
	}
	for _, character := range input.Content {
		if character == 0 || (character < 0x20 && character != '\n' && character != '\t') {
			return ValidatedResolve{}, NewControlError(CodeInvalidArgument, "resolution content has a forbidden control character")
		}
	}
	return ValidatedResolve{Decision: input.Decision, Content: strings.Clone(input.Content), Kind: kind}, nil
}

func validResolutionOperationKind(kind model.OperationKind) bool {
	return kind == model.OperationResolveNoAction || kind == model.OperationResolveRetry ||
		kind == model.OperationResolveReject
}
