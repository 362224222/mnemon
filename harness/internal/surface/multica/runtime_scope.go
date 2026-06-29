package multica

import "strings"

type RuntimeScopeMaterial struct {
	SessionID     string
	RootIssueID   string
	CorrelationID string
	TaskID        string
}

func RuntimeExplicitScopeMatches(sessionID, rootIssueID string, material RuntimeScopeMaterial) bool {
	if sessionID = strings.TrimSpace(sessionID); sessionID != "" && strings.TrimSpace(material.SessionID) != "" && sessionID != strings.TrimSpace(material.SessionID) {
		return false
	}
	if rootIssueID = strings.TrimSpace(rootIssueID); rootIssueID != "" && strings.TrimSpace(material.RootIssueID) != "" && rootIssueID != strings.TrimSpace(material.RootIssueID) {
		return false
	}
	return true
}

func RuntimeRefsMatchScope(refs []string, material RuntimeScopeMaterial, extraIssueIDs ...string) bool {
	scoped := false
	for _, ref := range refs {
		isScoped, matches := RuntimeScopeRefMatches(ref, material, extraIssueIDs...)
		if !isScoped {
			continue
		}
		scoped = true
		if matches {
			return true
		}
	}
	return !scoped
}

func RuntimeScopeRefMatches(ref string, material RuntimeScopeMaterial, extraIssueIDs ...string) (bool, bool) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return false, false
	}
	lower := strings.ToLower(ref)
	prefixes := []string{"multica:issue:", "multica:issue/", "mention://issue/", "multica:session:", "multica:session/", "multica:task:", "multica:task/"}
	scoped := false
	for _, prefix := range prefixes {
		if strings.HasPrefix(lower, prefix) {
			scoped = true
			break
		}
	}
	if !scoped {
		return false, false
	}
	candidates := []string{}
	if root := strings.TrimSpace(material.RootIssueID); root != "" {
		candidates = append(candidates,
			"multica:issue:"+root,
			"multica:issue/"+root,
			"mention://issue/"+root,
			"multica:session:"+root,
			"multica:session/"+root,
		)
	}
	if session := strings.TrimSpace(material.SessionID); session != "" {
		candidates = append(candidates, session)
	}
	if correlation := strings.TrimSpace(material.CorrelationID); correlation != "" {
		candidates = append(candidates, correlation)
	}
	if task := strings.TrimSpace(material.TaskID); task != "" {
		candidates = append(candidates,
			"multica:task:"+task,
			"multica:task/"+task,
		)
	}
	for _, issueID := range extraIssueIDs {
		issueID = strings.TrimSpace(issueID)
		if issueID == "" {
			continue
		}
		candidates = append(candidates,
			"multica:issue:"+issueID,
			"multica:issue/"+issueID,
			"mention://issue/"+issueID,
		)
	}
	for _, candidate := range candidates {
		if ref == candidate {
			return true, true
		}
	}
	return true, false
}
