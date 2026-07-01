package multica

import "strings"

const (
	RuntimeIssueSourceEnv        = "env:MULTICA_ISSUE_ID"
	RuntimeIssueSourceInput      = "input:issue"
	RuntimeIssueSourceInputText  = "input:text"
	RuntimeIssueSourceUnresolved = ""
)

type RuntimeContext struct {
	CWD                 string
	Text                string
	IssueIdentity       string
	IssueIdentitySource string
	TaskID              string
	AgentID             string
	AgentName           string
	WorkspaceID         string
	ServerURL           string
	ControlAddr         string
	ControlPrincipal    string
}

func RuntimeContextFromActivation(env []string, cwd string, input RuntimeInput) RuntimeContext {
	issueIdentity, issueSource := runtimeIssueIdentityFromActivation(env, input)
	return RuntimeContext{
		CWD:                 strings.TrimSpace(cwd),
		Text:                strings.TrimSpace(input.Text),
		IssueIdentity:       issueIdentity,
		IssueIdentitySource: issueSource,
		TaskID:              RuntimeEnvValue(env, "MULTICA_TASK_ID"),
		AgentID:             RuntimeEnvValue(env, "MULTICA_AGENT_ID"),
		AgentName:           RuntimeEnvValue(env, "MULTICA_AGENT_NAME"),
		WorkspaceID:         firstNonEmptyRuntimeString(RuntimeEnvValue(env, "MNEMON_MULTICA_WORKSPACE_ID"), RuntimeEnvValue(env, "MULTICA_WORKSPACE_ID")),
		ServerURL:           firstNonEmptyRuntimeString(RuntimeEnvValue(env, "MNEMON_MULTICA_SERVER_URL"), RuntimeEnvValue(env, "MULTICA_SERVER_URL")),
		ControlAddr:         RuntimeEnvValue(env, "MNEMON_CONTROL_ADDR"),
		ControlPrincipal:    RuntimeEnvValue(env, "MNEMON_CONTROL_PRINCIPAL"),
	}
}

func runtimeIssueIdentityFromActivation(env []string, input RuntimeInput) (string, string) {
	if issue := cleanIssueIdentity(RuntimeEnvValue(env, "MULTICA_ISSUE_ID")); issue != "" {
		return issue, RuntimeIssueSourceEnv
	}
	if issue := cleanIssueIdentity(input.IssueIdentity); issue != "" {
		source := strings.TrimSpace(input.IssueIdentitySource)
		if source == "" {
			source = RuntimeIssueSourceInput
		}
		return issue, source
	}
	if issue := ExtractIssueIdentity(input.Text); issue != "" {
		return issue, RuntimeIssueSourceInputText
	}
	return "", RuntimeIssueSourceUnresolved
}
