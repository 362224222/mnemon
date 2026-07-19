package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	agentcontrol "github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	WakeCue         = "[mnemon:wake] Managed work is pending. Use the Mnemon Harness skill to process one Event."
	maxContentBytes = model.MaxContentBytes
)

var errContentTooLarge = errors.New("content exceeds its closed byte bound")

type controlClient interface {
	daemonHealthClient
	HookCheck(context.Context) (localapi.HookCheckResponse, *localapi.APIError)
	AgentCurrent(context.Context) (localapi.AgentCurrentResponse, *localapi.APIError)
	TeamworkAction(context.Context, localapi.TeamworkActionRequest, *localapi.ContextFile,
		localapi.PendingJournal) (localapi.OperationResponse, *localapi.APIError)
	AgentResolve(context.Context, localapi.AgentResolveRequest, localapi.ContextFile,
		localapi.PendingJournal) (localapi.OperationResponse, *localapi.APIError)
}

type daemonHealthClient interface {
	ProbeHealth(context.Context) (localapi.HealthResponse, *localapi.APIError)
}

type journalStore interface {
	FindOrCreate(model.Digest, *model.Digest) (localapi.PendingJournal, bool, error)
	MarkTerminal(localapi.PendingJournal) (localapi.PendingJournal, error)
	MarkPresented(localapi.PendingJournal) (localapi.PendingJournal, error)
	RemoveTerminal(localapi.PendingJournal) error
}

type dependencies struct {
	workingDirectory func() (string, error)
	newClient        func(string) (controlClient, error)
	ensureDaemon     func(context.Context, string, string, daemonHealthClient) *localapi.APIError
	newJournals      func(string) (journalStore, error)
	readContext      func(string, string) (localapi.ContextFile, error)
	writeContext     func(string, model.RunID, string) (localapi.ContextFile, error)
	removeContext    func(string, localapi.ContextFile) error
}

type App struct {
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer
	deps   dependencies
}

func New(stdin io.Reader, stdout, stderr io.Writer) *App {
	return &App{stdin: stdin, stdout: stdout, stderr: stderr, deps: dependencies{
		workingDirectory: os.Getwd,
		newClient: func(nodeState string) (controlClient, error) {
			return localapi.NewClient(nodeState)
		},
		ensureDaemon: ensureAgentDaemon,
		newJournals: func(nodeState string) (journalStore, error) {
			return localapi.NewPendingJournalStore(nodeState)
		},
		readContext:   localapi.ReadContextFile,
		writeContext:  localapi.WriteContextFile,
		removeContext: localapi.RemoveContextFile,
	}}
}

// Run executes one closed Agent command and returns its frozen process exit
// status. It writes either one success value or one stable error envelope.
func (app *App) Run(ctx context.Context, args []string) int {
	if app == nil || app.stdin == nil || app.stdout == nil || app.stderr == nil || ctx == nil {
		return 1
	}
	command, apiErr := parseAgentCommand(args)
	if apiErr != nil {
		return app.writeError(command.jsonOutput, apiErr)
	}
	projectRoot, nodeState, err := app.resolveWorkspace()
	if err != nil {
		return app.writeError(command.jsonOutput,
			localapi.NewAPIError(localapi.CodeMnemondUnavailable, "Mnemon Harness is not set up in this workspace"))
	}
	client, err := app.deps.newClient(nodeState)
	if err != nil {
		code := localapi.CodeAuthenticationFailed
		message := "Mnemon Harness client state is unavailable"
		if !errors.Is(err, localapi.ErrUnsafeClientState) {
			code, message = localapi.CodeMnemondUnavailable, "mnemond local control is unavailable"
		}
		return app.writeError(command.jsonOutput, localapi.NewAPIError(code, message))
	}
	if app.deps.ensureDaemon == nil {
		return app.writeError(command.jsonOutput,
			localapi.NewAPIError(localapi.CodeInternal, "managed daemon ensure is unavailable"))
	}
	if apiErr := app.deps.ensureDaemon(ctx, projectRoot, nodeState, client); apiErr != nil {
		return app.writeError(command.jsonOutput, apiErr)
	}

	switch command.kind {
	case commandHook:
		return app.runHook(ctx, client)
	case commandCurrent:
		return app.runCurrent(ctx, client, nodeState)
	case commandTeamwork:
		return app.runTeamwork(ctx, client, projectRoot, nodeState, command)
	case commandResolve:
		return app.runResolve(ctx, client, projectRoot, nodeState, command)
	default:
		return app.writeError(command.jsonOutput,
			localapi.NewAPIError(localapi.CodeUnknownAction, "unknown Mnemon Harness Agent command"))
	}
}

type commandKind uint8

const (
	commandInvalid commandKind = iota
	commandHook
	commandCurrent
	commandTeamwork
	commandResolve
)

type agentCommand struct {
	kind        commandKind
	action      string
	channel     string
	participant string
	deadline    string
	contentFile string
	artifacts   []string
	contextPath string
	jsonOutput  bool
}

func parseAgentCommand(args []string) (agentCommand, *localapi.APIError) {
	command := agentCommand{}
	for _, argument := range args {
		if argument == "--json" {
			command.jsonOutput = true
		}
	}
	if len(args) < 2 {
		return command, localapi.NewAPIError(localapi.CodeInvalidArgument,
			"expected hook check, agent current|resolve, or teamwork action")
	}
	switch args[0] {
	case "hook":
		if len(args) != 2 || args[1] != "check" {
			return command, localapi.NewAPIError(localapi.CodeInvalidArgument, "hook accepts only check")
		}
		command.kind = commandHook
		return command, nil
	case "agent":
		switch args[1] {
		case "current":
			if len(args) != 3 || args[2] != "--json" {
				return command, localapi.NewAPIError(localapi.CodeInvalidArgument,
					"agent current requires --json")
			}
			command.kind, command.jsonOutput = commandCurrent, true
			return command, nil
		case "resolve":
			if len(args) < 3 {
				return command, localapi.NewAPIError(localapi.CodeInvalidArgument,
					"agent resolve requires a decision")
			}
			command.kind, command.action = commandResolve, args[2]
			if command.action != "no-action" && command.action != "retry" && command.action != "reject" {
				return command, localapi.NewAPIError(localapi.CodeUnknownAction,
					"unknown handling resolution")
			}
			if apiErr := parseAgentFlags(args[3:], &command, false); apiErr != nil {
				return command, apiErr
			}
			if command.contextPath == "" {
				return command, localapi.NewAPIError(localapi.CodeContextRequired,
					"agent resolve requires --context")
			}
			if len(command.artifacts) != 0 || command.channel != "" || command.participant != "" || command.deadline != "" {
				return command, localapi.NewAPIError(localapi.CodeInvalidArgument,
					"agent resolve forbids selectors and Artifacts")
			}
			return command, nil
		default:
			return command, localapi.NewAPIError(localapi.CodeUnknownAction, "unknown agent command")
		}
	case "teamwork":
		command.kind, command.action = commandTeamwork, args[1]
		if !validTeamworkAction(command.action) {
			return command, localapi.NewAPIError(localapi.CodeUnknownAction, "unknown Teamwork action")
		}
		if apiErr := parseAgentFlags(args[2:], &command, true); apiErr != nil {
			return command, apiErr
		}
		if command.action != "offer" && command.contextPath == "" {
			return command, localapi.NewAPIError(localapi.CodeContextRequired,
				"this Teamwork action requires --context")
		}
		if command.action != "offer" && (command.channel != "" || command.participant != "" || command.deadline != "") {
			return command, localapi.NewAPIError(localapi.CodeInvalidArgument,
				"selectors and deadline are only valid for offer")
		}
		return command, nil
	default:
		return command, localapi.NewAPIError(localapi.CodeUnknownAction, "unknown Mnemon Harness Agent command")
	}
}

func parseAgentFlags(args []string, command *agentCommand, allowArtifacts bool) *localapi.APIError {
	seen := make(map[string]bool)
	for index := 0; index < len(args); index++ {
		flag := args[index]
		if flag == "--json" {
			if seen[flag] {
				return invalidFlag("duplicate --json")
			}
			seen[flag], command.jsonOutput = true, true
			continue
		}
		if flag == "--artifact" {
			if !allowArtifacts || index+1 >= len(args) || strings.HasPrefix(args[index+1], "--") {
				return invalidFlag("--artifact requires one workspace-relative path")
			}
			index++
			command.artifacts = append(command.artifacts, args[index])
			continue
		}
		if flag != "--channel" && flag != "--to" && flag != "--deadline" &&
			flag != "--content-file" && flag != "--context" {
			return invalidFlag("unknown or unsupported Agent flag")
		}
		if seen[flag] || index+1 >= len(args) {
			return invalidFlag(flag + " requires exactly one value")
		}
		seen[flag] = true
		index++
		value := args[index]
		if value == "" || strings.HasPrefix(value, "--") {
			return invalidFlag(flag + " requires a nonempty value")
		}
		switch flag {
		case "--channel":
			command.channel = value
		case "--to":
			command.participant = value
		case "--deadline":
			command.deadline = value
		case "--content-file":
			command.contentFile = value
		case "--context":
			command.contextPath = value
		}
	}
	return nil
}

func invalidFlag(message string) *localapi.APIError {
	return localapi.NewAPIError(localapi.CodeInvalidArgument, message)
}

func validTeamworkAction(action string) bool {
	return action == "offer" || action == "accept" || action == "decline" || action == "deliver" ||
		action == "rework" || action == "close" || action == "cancel"
}

func (app *App) runHook(ctx context.Context, client controlClient) int {
	response, apiErr := client.HookCheck(ctx)
	if apiErr != nil {
		return app.writeError(false, apiErr)
	}
	if response.SchemaVersion != localapi.SchemaVersion {
		return app.writeError(false, localapi.NewAPIError(localapi.CodeInternal,
			"mnemond returned an unsupported Hook response"))
	}
	if response.Pending {
		if _, err := fmt.Fprintln(app.stdout, WakeCue); err != nil {
			return 1
		}
	}
	return 0
}

func (app *App) runCurrent(ctx context.Context, client controlClient, nodeState string) int {
	response, apiErr := client.AgentCurrent(ctx)
	if apiErr != nil {
		return app.writeError(true, apiErr)
	}
	if response.Status != "actionable" {
		if response.Status == "none" && len(response.Projection) != 0 {
			projection, apiErr := noneCurrentOutput(response.Projection)
			if apiErr != nil {
				return app.writeError(true, apiErr)
			}
			if _, err := app.stdout.Write(append(projection, '\n')); err != nil {
				return 1
			}
			return 0
		}
		return app.writeJSON(struct {
			SchemaVersion int    `json:"schema_version"`
			Status        string `json:"status"`
		}{localapi.SchemaVersion, response.Status})
	}
	runID, err := model.ParseRunID(response.RunID)
	if err != nil {
		return app.writeError(true, localapi.NewAPIError(localapi.CodeInternal,
			"mnemond returned an invalid managed Run"))
	}
	contextFile, err := app.deps.writeContext(nodeState, runID, response.ClaimSecret)
	if err != nil {
		return app.writeError(true, localapi.NewAPIError(localapi.CodeContextInvalid,
			"managed context could not be stored safely"))
	}
	projection, apiErr := currentOutput(response.Projection, contextFile.Path())
	if apiErr != nil {
		return app.writeError(true, apiErr)
	}
	if _, err := app.stdout.Write(append(projection, '\n')); err != nil {
		return 1
	}
	return 0
}

func noneCurrentOutput(raw json.RawMessage) ([]byte, *localapi.APIError) {
	projection, err := localapi.ParseInitiationProjection(raw)
	if err != nil {
		return nil, localapi.NewAPIError(localapi.CodeInternal,
			"mnemond initiation projection cannot be presented safely")
	}
	encoded, err := model.CanonicalMarshal(struct {
		InitiationContext struct {
			Channels []localapi.InitiationChannel `json:"channels"`
		} `json:"initiation_context"`
		SchemaVersion int    `json:"schema_version"`
		Status        string `json:"status"`
	}{InitiationContext: projection.InitiationContext,
		SchemaVersion: localapi.SchemaVersion, Status: "none"})
	if err != nil {
		return nil, localapi.NewAPIError(localapi.CodeInternal,
			"mnemond initiation projection cannot be encoded safely")
	}
	return encoded, nil
}

func currentOutput(projection json.RawMessage, contextPath string) ([]byte, *localapi.APIError) {
	parsed, err := model.ParseCurrentProjection(projection)
	if err != nil {
		return nil, localapi.NewAPIError(localapi.CodeInternal,
			"mnemond current projection cannot be presented safely")
	}
	var fields map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(parsed.CanonicalJSON().Bytes()))
	if err := decoder.Decode(&fields); err != nil || fields == nil || fields["status"] != nil ||
		fields["context_file"] != nil {
		return nil, localapi.NewAPIError(localapi.CodeInternal,
			"mnemond current projection cannot be presented safely")
	}
	status, _ := json.Marshal("actionable")
	path, _ := json.Marshal(contextPath)
	fields["status"], fields["context_file"] = status, path
	raw, err := model.CanonicalMarshal(fields)
	if err != nil || len(raw) > 256<<10 {
		return nil, localapi.NewAPIError(localapi.CodeCurrentTooLarge,
			"current projection exceeds 256 KiB")
	}
	return raw, nil
}

func (app *App) runTeamwork(ctx context.Context, client controlClient, projectRoot, nodeState string,
	command agentCommand,
) int {
	content, apiErr := app.readContent(projectRoot, command.contentFile)
	if apiErr != nil {
		return app.writeError(command.jsonOutput, apiErr)
	}
	artifacts, apiErr := normalizeArtifactArguments(command.artifacts)
	if apiErr != nil {
		return app.writeError(command.jsonOutput, apiErr)
	}
	request := localapi.TeamworkActionRequest{Action: command.action, Channel: command.channel,
		To: command.participant, Deadline: command.deadline, Content: content, Artifacts: artifacts}
	var contextFile *localapi.ContextFile
	if command.contextPath != "" {
		verified, err := app.deps.readContext(nodeState, command.contextPath)
		if err != nil {
			return app.writeError(command.jsonOutput, localapi.NewAPIError(localapi.CodeContextInvalid,
				"managed context identity is invalid"))
		}
		contextFile = &verified
	}
	validated, apiErr := validateManagedTeamworkAction(request, contextFile != nil)
	if apiErr != nil {
		return app.writeError(command.jsonOutput, apiErr)
	}
	request.Artifacts = validated.ArtifactPaths
	return app.submitTeamwork(ctx, client, nodeState, request, contextFile, command.jsonOutput)
}

func (app *App) submitTeamwork(ctx context.Context, client controlClient, nodeState string,
	request localapi.TeamworkActionRequest, contextFile *localapi.ContextFile, jsonOutput bool,
) int {
	requestDigest, apiErr := canonicalRequestDigest(request)
	if apiErr != nil {
		return app.writeError(jsonOutput, apiErr)
	}
	journalStore, err := app.deps.newJournals(nodeState)
	if err != nil {
		return app.writeError(jsonOutput, localapi.NewAPIError(localapi.CodeInternal,
			"pending operation journal is unavailable"))
	}
	var contextDigest *model.Digest
	if contextFile != nil {
		value := contextFile.Digest()
		contextDigest = &value
	}
	journal, _, err := journalStore.FindOrCreate(requestDigest, contextDigest)
	if err != nil {
		return app.writeError(jsonOutput, localapi.NewAPIError(localapi.CodeOperationMismatch,
			"pending operation journal is invalid"))
	}
	response, remoteErr := client.TeamworkAction(ctx, request, contextFile, journal)
	if remoteErr != nil {
		if !isTerminalRemoteError(remoteErr) {
			return app.writeError(jsonOutput, remoteErr)
		}
		return app.presentTerminalError(journalStore, journal, remoteErr, jsonOutput)
	}
	return app.presentTerminalOperation(nodeState, journalStore, journal, contextFile, response, jsonOutput)
}

func (app *App) runResolve(ctx context.Context, client controlClient, projectRoot, nodeState string,
	command agentCommand,
) int {
	content, apiErr := app.readContent(projectRoot, command.contentFile)
	if apiErr != nil {
		return app.writeError(command.jsonOutput, apiErr)
	}
	contextFile, err := app.deps.readContext(nodeState, command.contextPath)
	if err != nil {
		return app.writeError(command.jsonOutput, localapi.NewAPIError(localapi.CodeContextInvalid,
			"managed context identity is invalid"))
	}
	request := localapi.AgentResolveRequest{Decision: command.action, Content: content}
	if _, validationErr := agentcontrol.ValidateResolve(agentcontrol.ResolveInput{Decision: request.Decision,
		HasContext: true, Content: request.Content}); validationErr != nil {
		return app.writeError(command.jsonOutput, localAPIValidationError(validationErr))
	}
	requestDigest, apiErr := canonicalRequestDigest(request)
	if apiErr != nil {
		return app.writeError(command.jsonOutput, apiErr)
	}
	journalStore, err := app.deps.newJournals(nodeState)
	if err != nil {
		return app.writeError(command.jsonOutput, localapi.NewAPIError(localapi.CodeInternal,
			"pending operation journal is unavailable"))
	}
	contextDigest := contextFile.Digest()
	journal, _, err := journalStore.FindOrCreate(requestDigest, &contextDigest)
	if err != nil {
		return app.writeError(command.jsonOutput, localapi.NewAPIError(localapi.CodeOperationMismatch,
			"pending operation journal is invalid"))
	}
	response, remoteErr := client.AgentResolve(ctx, request, contextFile, journal)
	if remoteErr != nil {
		if !isTerminalRemoteError(remoteErr) {
			return app.writeError(command.jsonOutput, remoteErr)
		}
		return app.presentTerminalError(journalStore, journal, remoteErr, command.jsonOutput)
	}
	return app.presentTerminalOperation(nodeState, journalStore, journal, &contextFile,
		response, command.jsonOutput)
}

func isTerminalRemoteError(apiErr *localapi.APIError) bool {
	if apiErr == nil || apiErr.Retryable {
		return false
	}
	if apiErr.OperationID != nil {
		return true
	}
	// Closed input errors are known to precede a domain effect. Internal,
	// authentication, context and activation failures retain the replay handle
	// because a response may have become ambiguous below the CLI boundary.
	return apiErr.ExitStatus() == 2
}

func (app *App) presentTerminalOperation(nodeState string, journals journalStore,
	pending localapi.PendingJournal, contextFile *localapi.ContextFile,
	response localapi.OperationResponse, jsonOutput bool,
) int {
	terminal, err := journals.MarkTerminal(pending)
	if err != nil {
		return app.writeError(jsonOutput, localapi.NewAPIError(localapi.CodeInternal,
			"terminal operation journal could not be persisted"))
	}
	if exit := app.writeOperation(response, jsonOutput); exit != 0 {
		return exit
	}
	_, err = journals.MarkPresented(terminal)
	if err != nil {
		// The validated success envelope is already visible. Preserve its frozen
		// exit contract and leave the terminal handle for replay/doctor repair;
		// a second envelope or a contradictory exit would be less truthful.
		return 0
	}
	app.removePresentedContext(nodeState, contextFile)
	return 0
}

func (app *App) presentTerminalError(journals journalStore, pending localapi.PendingJournal,
	remoteErr *localapi.APIError, jsonOutput bool,
) int {
	terminal, err := journals.MarkTerminal(pending)
	if err != nil {
		return app.writeError(jsonOutput, localapi.NewAPIError(localapi.CodeInternal,
			"terminal operation journal could not be persisted"))
	}
	exit, written := app.presentError(jsonOutput, remoteErr)
	if !written {
		return exit
	}
	_, err = journals.MarkPresented(terminal)
	if err != nil {
		// The validated rejection envelope is already visible, so its domain exit
		// remains authoritative even if the local presentation marker needs repair.
		return exit
	}
	// A rejected action leaves its claim context available for a corrected
	// action. The presented tombstone remains so an already in-flight caller
	// can still prove the old operation identity while a new call gets a new key.
	return exit
}

func canonicalRequestDigest(request any) (model.Digest, *localapi.APIError) {
	raw, err := model.CanonicalMarshal(request)
	if err != nil || len(raw) > localapi.MaxRequestBodyBytes {
		return model.Digest{}, localapi.NewAPIError(localapi.CodeInvalidArgument,
			"request cannot be encoded canonically")
	}
	return model.Sum(raw), nil
}

func (app *App) removePresentedContext(nodeState string, contextFile *localapi.ContextFile) {
	if contextFile != nil {
		if err := app.deps.removeContext(nodeState, *contextFile); err != nil {
			// The validated domain receipt remains the authority. A tampered or
			// busy ephemeral handle is left for bounded mnemond GC/doctor repair.
			return
		}
	}
}

func (app *App) readContent(projectRoot, source string) (string, *localapi.APIError) {
	if source == "" {
		return "", nil
	}
	var raw []byte
	var err error
	if source == "-" {
		raw, err = io.ReadAll(io.LimitReader(app.stdin, int64(maxContentBytes)+1))
	} else {
		raw, err = readBoundedWorkspaceFile(projectRoot, source, maxContentBytes)
	}
	if err != nil {
		if errors.Is(err, errContentTooLarge) {
			return "", localapi.NewAPIError(localapi.CodeContentTooLarge,
				"content exceeds 8192 bytes")
		}
		return "", localapi.NewAPIError(localapi.CodeInvalidArgument,
			"content file cannot be read safely")
	}
	if len(raw) > maxContentBytes {
		return "", localapi.NewAPIError(localapi.CodeContentTooLarge,
			"content exceeds 8192 bytes")
	}
	if !utf8.Valid(raw) || bytes.IndexByte(raw, 0) >= 0 {
		return "", localapi.NewAPIError(localapi.CodeInvalidArgument,
			"content must be UTF-8 text without NUL bytes")
	}
	return string(raw), nil
}

func normalizeArtifactArguments(values []string) ([]string, *localapi.APIError) {
	if len(values) > 16 {
		return nil, localapi.NewAPIError(localapi.CodeArtifactTooLarge,
			"an action accepts at most 16 Artifact paths")
	}
	result := make([]string, len(values))
	seen := make(map[string]struct{}, len(values))
	for index, value := range values {
		logical, err := normalizeWorkspaceRelative(value)
		if err != nil || isHarnessInternal(logical) && !isManagedReadonlyViewPath(logical) {
			return nil, localapi.NewAPIError(localapi.CodeArtifactInvalid,
				"Artifact path must be workspace-relative, produced, or an exact managed readonly view")
		}
		if _, exists := seen[logical]; exists {
			return nil, localapi.NewAPIError(localapi.CodeArtifactInvalid,
				"duplicate Artifact path")
		}
		seen[logical], result[index] = struct{}{}, logical
	}
	return result, nil
}

func normalizeWorkspaceRelative(value string) (string, error) {
	if value == "" || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 || filepath.IsAbs(value) {
		return "", errors.New("invalid workspace-relative path")
	}
	components := strings.Split(filepath.ToSlash(value), "/")
	for len(components) > 0 && components[0] == "." {
		components = components[1:]
	}
	if len(components) == 0 {
		return "", errors.New("empty workspace-relative path")
	}
	for _, component := range components {
		if component == "" || component == "." || component == ".." {
			return "", errors.New("path contains traversal or empty component")
		}
	}
	logical := strings.Join(components, "/")
	if len(logical) > 4096 {
		return "", errors.New("path is too long")
	}
	return logical, nil
}

func isHarnessInternal(logical string) bool {
	components := strings.Split(filepath.ToSlash(logical), "/")
	return len(components) >= 2 && strings.EqualFold(components[0], ".mnemon") &&
		strings.EqualFold(components[1], "harness")
}

func isManagedReadonlyViewPath(logical string) bool {
	_, err := model.NewCurrentArtifactView(model.Sum([]byte("mnemon/cli/view-path-validation/v1")), logical)
	return err == nil
}

func readBoundedWorkspaceFile(projectRoot, requested string, maximum int) ([]byte, error) {
	logical, err := normalizeWorkspaceRelative(requested)
	if err != nil || isHarnessInternal(logical) || maximum < 0 {
		return nil, errors.New("invalid content path")
	}
	rootInfo, err := os.Lstat(projectRoot)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("workspace root is unsafe")
	}
	root, err := os.OpenRoot(projectRoot)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	openedRoot, openErr := root.Stat(".")
	rootPathAfterOpen, pathErr := os.Lstat(projectRoot)
	if openErr != nil || pathErr != nil || !sameCLIFileSnapshot(rootInfo, openedRoot) ||
		!sameCLIFileSnapshot(rootInfo, rootPathAfterOpen) {
		return nil, errors.New("workspace root changed while opening")
	}
	components := strings.Split(logical, "/")
	parent := root
	opened := make([]*os.Root, 0, len(components)-1)
	defer func() {
		for index := len(opened) - 1; index >= 0; index-- {
			_ = opened[index].Close()
		}
	}()
	for _, component := range components[:len(components)-1] {
		before, err := parent.Lstat(component)
		if err != nil || !before.IsDir() || before.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("content path component is unsafe")
		}
		next, err := parent.OpenRoot(component)
		if err != nil {
			return nil, err
		}
		opened = append(opened, next)
		openedInfo, statErr := next.Stat(".")
		after, pathErr := parent.Lstat(component)
		if statErr != nil || pathErr != nil || !sameCLIFileSnapshot(before, openedInfo) ||
			!sameCLIFileSnapshot(before, after) {
			return nil, errors.New("content path component changed")
		}
		parent = next
	}
	name := components[len(components)-1]
	before, err := parent.Lstat(name)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 ||
		before.Size() < 0 {
		return nil, errors.New("content file is unsafe or oversized")
	}
	if before.Size() > int64(maximum) {
		return nil, errContentTooLarge
	}
	file, err := parent.Open(name)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil || !sameCLIFileSnapshot(before, openedInfo) {
		return nil, errors.New("content file changed while opening")
	}
	raw, err := io.ReadAll(io.LimitReader(file, int64(maximum)+1))
	if err != nil {
		return nil, errors.New("content file read failed")
	}
	if len(raw) > maximum {
		return nil, errContentTooLarge
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, errors.New("content file cannot be reverified")
	}
	verified, err := io.ReadAll(io.LimitReader(file, int64(maximum)+1))
	if err != nil || !bytes.Equal(raw, verified) {
		return nil, errors.New("content file changed while reading")
	}
	afterFD, fdErr := file.Stat()
	afterPath, pathErr := parent.Lstat(name)
	rootAfter, rootErr := os.Lstat(projectRoot)
	openedRootAfter, openedRootErr := root.Stat(".")
	if fdErr != nil || pathErr != nil || rootErr != nil || openedRootErr != nil ||
		!sameCLIFileSnapshot(before, afterFD) || !sameCLIFileSnapshot(before, afterPath) ||
		!sameCLIFileSnapshot(rootInfo, rootAfter) || !sameCLIFileSnapshot(rootInfo, openedRootAfter) {
		return nil, errors.New("content file changed while reading")
	}
	return raw, nil
}

func sameCLIFileSnapshot(left, right os.FileInfo) bool {
	return left != nil && right != nil && os.SameFile(left, right) && left.Mode() == right.Mode() &&
		left.Size() == right.Size() && left.ModTime().Equal(right.ModTime())
}

func (app *App) resolveWorkspace() (string, string, error) {
	if app.deps.workingDirectory == nil || app.deps.newClient == nil || app.deps.newJournals == nil ||
		app.deps.readContext == nil || app.deps.writeContext == nil || app.deps.removeContext == nil {
		return "", "", errors.New("Agent CLI dependencies are incomplete")
	}
	return resolveManagedWorkspace(app.deps.workingDirectory)
}

func (app *App) writeOperation(response localapi.OperationResponse, jsonOutput bool) int {
	if jsonOutput {
		return app.writeJSON(response)
	}
	if _, err := fmt.Fprintf(app.stdout, "%s %s (%d result(s))\n",
		response.Status, response.Action, len(response.Results)); err != nil {
		return 1
	}
	return 0
}

func (app *App) writeError(jsonOutput bool, apiErr *localapi.APIError) int {
	exit, _ := app.presentError(jsonOutput, apiErr)
	return exit
}

func (app *App) presentError(jsonOutput bool, apiErr *localapi.APIError) (int, bool) {
	if apiErr == nil {
		apiErr = localapi.NewAPIError(localapi.CodeInternal, "internal Agent command error")
	}
	if jsonOutput {
		if app.writeJSON(apiErr) != 0 {
			return 1, false
		}
	} else if _, err := fmt.Fprintf(app.stderr, "%s: %s\n", apiErr.Code, apiErr.Message); err != nil {
		return 1, false
	}
	return apiErr.ExitStatus(), true
}

func (app *App) writeJSON(value any) int {
	raw, err := model.CanonicalMarshal(value)
	if err != nil {
		return 1
	}
	if _, err := app.stdout.Write(append(raw, '\n')); err != nil {
		return 1
	}
	return 0
}
