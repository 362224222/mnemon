package agent

import (
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestActionHandlersRequireExplicitParticipantSelector(t *testing.T) {
	t.Parallel()
	handlers := testActionHandlers(t)
	_, apiErr := handlers.Validate(ActionInput{Action: "offer", Content: "review"})
	if apiErr == nil || apiErr.Code != CodeInvalidArgument {
		t.Fatalf("missing participant selector error = %#v", apiErr)
	}
	if _, apiErr := handlers.Validate(ActionInput{Action: "offer", Participant: "team",
		Content: "review"}); apiErr != nil {
		t.Fatalf("effective alias selector error = %#v", apiErr)
	}
}

func TestActionHandlersValidateAssetOwnedSchema(t *testing.T) {
	t.Parallel()
	handlers := testActionHandlers(t)
	tests := []struct {
		name      string
		input     ActionInput
		wantCode  ControlErrorCode
		wantPaths []string
	}{
		{name: "offer", input: ActionInput{Action: "offer", ChannelAlias: "beta", Participant: "team",
			Deadline: "24h", Content: "review this", ArtifactPaths: []string{"z.md", "a.md"}}, wantPaths: []string{"a.md", "z.md"}},
		{name: "accept", input: ActionInput{Action: "accept", HasContext: true}},
		{name: "decline", input: ActionInput{Action: "decline", HasContext: true, Content: "not suitable"}},
		{name: "deliver", input: ActionInput{Action: "deliver", HasContext: true, Content: "done", ArtifactPaths: []string{"result.md"}}},
		{name: "rework", input: ActionInput{Action: "rework", HasContext: true, Content: "fix race", ArtifactPaths: []string{"replacement.md"}}},
		{name: "close", input: ActionInput{Action: "close", HasContext: true}},
		{name: "cancel", input: ActionInput{Action: "cancel", HasContext: true, Content: "obsolete"}},
		{name: "unknown", input: ActionInput{Action: "memory.write"}, wantCode: CodeUnknownAction},
		{name: "context", input: ActionInput{Action: "deliver", Content: "done"}, wantCode: CodeContextRequired},
		{name: "required content", input: ActionInput{Action: "decline", HasContext: true}, wantCode: CodeContentRequired},
		{name: "whitespace content", input: ActionInput{Action: "offer", Participant: "reviewer", Content: " \n\t"}, wantCode: CodeContentRequired},
		{name: "forbidden selector", input: ActionInput{Action: "accept", HasContext: true, Participant: "peer"}, wantCode: CodeInvalidArgument},
		{name: "forbidden artifact", input: ActionInput{Action: "close", HasContext: true, ArtifactPaths: []string{"x"}}, wantCode: CodeArtifactInvalid},
		{name: "duplicate artifact", input: ActionInput{Action: "offer", Participant: "reviewer", Content: "x", ArtifactPaths: []string{"a", "a"}}, wantCode: CodeArtifactInvalid},
		{name: "short deadline", input: ActionInput{Action: "offer", Participant: "reviewer", Content: "x", Deadline: "4m59s"}, wantCode: CodeInvalidArgument},
		{name: "long deadline", input: ActionInput{Action: "offer", Participant: "reviewer", Content: "x", Deadline: "169h"}, wantCode: CodeInvalidArgument},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, apiErr := handlers.Validate(test.input)
			if test.wantCode != "" {
				if apiErr == nil || apiErr.Code != test.wantCode {
					t.Fatalf("Validate() error = %#v, want %s", apiErr, test.wantCode)
				}
				return
			}
			if apiErr != nil || result.Candidate == nil || result.Name != test.input.Action ||
				!result.matches(handlers, result.handler.OperationKind()) {
				t.Fatalf("Validate() = %#v, %#v", result, apiErr)
			}
			if result.Name == "offer" && result.Deadline != 24*time.Hour {
				t.Fatalf("deadline = %s", result.Deadline)
			}
			if test.wantPaths != nil && (len(result.ArtifactPaths) != 2 ||
				result.ArtifactPaths[0] != test.wantPaths[0] || result.ArtifactPaths[1] != test.wantPaths[1]) {
				t.Fatalf("Artifact paths = %#v", result.ArtifactPaths)
			}
		})
	}
}

func TestActionHandlersEnforceContentAndArtifactAssetBounds(t *testing.T) {
	t.Parallel()
	handlers := testActionHandlers(t)
	if _, apiErr := handlers.Validate(ActionInput{Action: "offer", Participant: "reviewer",
		Content: string([]byte{0xff})}); apiErr == nil || apiErr.Code != CodeInvalidArgument {
		t.Fatalf("invalid UTF-8 error = %#v", apiErr)
	}
	offer, _ := handlers.Action("offer")
	content := strings.Repeat("x", int(offer.Descriptor().Content().MaxBytes())+1)
	if _, apiErr := handlers.Validate(ActionInput{Action: "offer", Participant: "reviewer",
		Content: content}); apiErr == nil || apiErr.Code != CodeContentTooLarge {
		t.Fatalf("large content error = %#v", apiErr)
	}
	paths := make([]string, int(offer.Descriptor().Artifacts().MaxRoots())+1)
	for index := range paths {
		paths[index] = strings.Repeat("x", index+1)
	}
	if _, apiErr := handlers.Validate(ActionInput{Action: "offer", Participant: "reviewer",
		Content: "x", ArtifactPaths: paths}); apiErr == nil || apiErr.Code != CodeArtifactTooLarge {
		t.Fatalf("large Artifact set error = %#v", apiErr)
	}
	longPath := strings.Repeat("x", int(offer.Descriptor().Artifacts().MaxPathBytes())+1)
	if _, apiErr := handlers.Validate(ActionInput{Action: "offer", Participant: "reviewer",
		Content: "x", ArtifactPaths: []string{longPath}}); apiErr == nil || apiErr.Code != CodeArtifactInvalid {
		t.Fatalf("long Artifact path error = %#v", apiErr)
	}
}

func TestValidatedActionRequestDigestBindsNormalizedAuthority(t *testing.T) {
	t.Parallel()
	handlers := testActionHandlers(t)
	implicit, apiErr := handlers.Validate(ActionInput{Action: "offer", ChannelAlias: "alpha",
		Participant: "reviewer-a", Content: "review", ArtifactPaths: []string{"z.md", "a.md"}})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	explicit, apiErr := handlers.Validate(ActionInput{Action: "offer", ChannelAlias: "alpha",
		Participant: "reviewer-a", Deadline: "24h", Content: "review",
		ArtifactPaths: []string{"a.md", "z.md"}})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	want, err := implicit.requestDigest(model.Digest{}, false)
	if err != nil {
		t.Fatal(err)
	}
	got, err := explicit.requestDigest(model.Digest{}, false)
	if err != nil || got != want {
		t.Fatalf("equivalent normalized digests = (%s, %v), want %s", got, err, want)
	}

	contextHash := model.Sum([]byte("managed-context"))
	contextual, apiErr := handlers.Validate(ActionInput{Action: "offer", HasContext: true,
		ChannelAlias: "alpha", Participant: "reviewer-a",
		Content: "review", ArtifactPaths: []string{"z.md", "a.md"}})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	mutations := []struct {
		name       string
		action     ValidatedAction
		context    model.Digest
		hasContext bool
	}{
		{name: "context", action: contextual, context: contextHash, hasContext: true},
		{name: "channel", action: func() ValidatedAction { value := implicit; value.ChannelAlias = "beta"; return value }()},
		{name: "participant", action: func() ValidatedAction { value := implicit; value.Participant = "reviewer-b"; return value }()},
		{name: "deadline", action: func() ValidatedAction { value := implicit; value.Deadline = 23 * time.Hour; return value }()},
		{name: "content", action: func() ValidatedAction { value := implicit; value.Content = "another review"; return value }()},
		{name: "artifacts", action: func() ValidatedAction { value := implicit; value.ArtifactPaths = []string{"a.md"}; return value }()},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			digest, digestErr := mutation.action.requestDigest(mutation.context, mutation.hasContext)
			if digestErr == nil && digest == want {
				t.Fatalf("mutation retained request digest %s", digest)
			}
		})
	}
}
