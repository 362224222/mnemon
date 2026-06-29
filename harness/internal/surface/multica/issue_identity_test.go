package multica

import "testing"

func TestExtractIssueIdentityAcceptsMentionsLegacyFieldsAndTags(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "explicit assigned issue", input: "Your assigned issue ID is: iss-7\nPlease work on it.", want: "iss-7"},
		{name: "legacy issue id", input: "issue_id: iss-8", want: "iss-8"},
		{name: "multica mention", input: "Open [TEA-51](mention://issue/issue-51) for the current task.", want: "issue-51"},
		{name: "mention beats legacy text", input: "Your assigned issue ID is: stale\nOpen [TEA-52](mention://issue/issue-52).", want: "issue-52"},
		{name: "at identifier tag", input: "Please handle @TEA-49 next.", want: "TEA-49"},
		{name: "hash identifier tag", input: "Review #TEA-50.", want: "TEA-50"},
		{name: "non issue tag", input: "Please coordinate with @team.", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ExtractIssueIdentity(tc.input); got != tc.want {
				t.Fatalf("ExtractIssueIdentity = %q, want %q", got, tc.want)
			}
		})
	}
}
