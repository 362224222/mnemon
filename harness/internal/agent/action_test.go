package agent

import (
	"testing"
	"time"
)

func TestValidateActionClosedSchema(t *testing.T) {
	t.Parallel()
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
		{name: "whitespace content", input: ActionInput{Action: "offer", Content: " \n\t"}, wantCode: CodeContentRequired},
		{name: "forbidden selector", input: ActionInput{Action: "accept", HasContext: true, Participant: "peer"}, wantCode: CodeInvalidArgument},
		{name: "forbidden artifact", input: ActionInput{Action: "close", HasContext: true, ArtifactPaths: []string{"x"}}, wantCode: CodeArtifactInvalid},
		{name: "duplicate artifact", input: ActionInput{Action: "offer", Content: "x", ArtifactPaths: []string{"a", "a"}}, wantCode: CodeArtifactInvalid},
		{name: "short deadline", input: ActionInput{Action: "offer", Content: "x", Deadline: "4m59s"}, wantCode: CodeInvalidArgument},
		{name: "long deadline", input: ActionInput{Action: "offer", Content: "x", Deadline: "169h"}, wantCode: CodeInvalidArgument},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, apiErr := ValidateAction(test.input)
			if test.wantCode != "" {
				if apiErr == nil || apiErr.Code != test.wantCode {
					t.Fatalf("ValidateAction() error = %#v, want %s", apiErr, test.wantCode)
				}
				return
			}
			if apiErr != nil || result.Candidate == nil || result.Name != test.input.Action {
				t.Fatalf("ValidateAction() = %#v, %#v", result, apiErr)
			}
			if test.input.Deadline != "" && result.Deadline != 24*time.Hour {
				t.Fatalf("deadline = %s", result.Deadline)
			}
			if test.wantPaths != nil && (len(result.ArtifactPaths) != 2 ||
				result.ArtifactPaths[0] != test.wantPaths[0] || result.ArtifactPaths[1] != test.wantPaths[1]) {
				t.Fatalf("Artifact paths = %#v", result.ArtifactPaths)
			}
		})
	}
}

func TestValidateActionContentAndArtifactBounds(t *testing.T) {
	t.Parallel()
	if _, apiErr := ValidateAction(ActionInput{Action: "offer", Content: string([]byte{0xff})}); apiErr == nil || apiErr.Code != CodeInvalidArgument {
		t.Fatalf("invalid UTF-8 error = %#v", apiErr)
	}
	content := make([]byte, 8193)
	for index := range content {
		content[index] = 'x'
	}
	if _, apiErr := ValidateAction(ActionInput{Action: "offer", Content: string(content)}); apiErr == nil || apiErr.Code != CodeContentTooLarge {
		t.Fatalf("large content error = %#v", apiErr)
	}
	paths := make([]string, MaxActionArtifacts+1)
	for index := range paths {
		paths[index] = string(rune('a' + index))
	}
	if _, apiErr := ValidateAction(ActionInput{Action: "offer", Content: "x", ArtifactPaths: paths}); apiErr == nil || apiErr.Code != CodeArtifactTooLarge {
		t.Fatalf("large Artifact set error = %#v", apiErr)
	}
}
