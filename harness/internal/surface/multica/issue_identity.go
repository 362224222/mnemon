package multica

import (
	"regexp"
	"strings"
)

var (
	assignedIssuePattern       = regexp.MustCompile(`(?i)(?:assigned\s+issue\s+id\s+is|issue[_\s-]*id)\s*[:：]\s*([A-Za-z0-9][A-Za-z0-9._:-]*)`)
	multicaIssueMentionPattern = regexp.MustCompile(`(?i)mention://issue/([A-Za-z0-9][A-Za-z0-9._:-]*)`)
	multicaTaggedIssuePattern  = regexp.MustCompile(`(?i)(?:^|[\s([{"'])[@#]([A-Z][A-Z0-9]+-\d+)(?:$|[\s\])}.,;:'"])`)
)

// ExtractIssueIdentity returns the best issue lookup key from Multica runtime input.
//
// The returned value may be a stable mention target from mention://issue/<id>, a
// daemon-compatible legacy issue id field, or a visible issue identifier tag
// such as @TEA-123. Callers must resolve identifier tags through Multica before
// building rule-facing Mnemon event material.
func ExtractIssueIdentity(input string) string {
	match := multicaIssueMentionPattern.FindStringSubmatch(input)
	if len(match) >= 2 {
		return cleanIssueIdentity(match[1])
	}
	match = assignedIssuePattern.FindStringSubmatch(input)
	if len(match) >= 2 {
		return cleanIssueIdentity(match[1])
	}
	match = multicaTaggedIssuePattern.FindStringSubmatch(input)
	if len(match) >= 2 {
		return cleanIssueIdentity(match[1])
	}
	return ""
}

func cleanIssueIdentity(value string) string {
	return strings.Trim(value, " \t\r\n.,;)")
}
