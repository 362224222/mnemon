package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
)

const (
	hostActivationTimeout   = 5 * time.Second
	hostActivationOutputMax = 1 << 20
	hostActivationMessages  = 32
	hostActivationHashMax   = 256
)

var ErrHostActivationRequired = errors.New("managed Host activation is required")

type codexRPCEnvelope struct {
	Error  json.RawMessage `json:"error,omitempty"`
	ID     json.RawMessage `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
}

type codexHooksListResponse struct {
	Data []codexHooksListEntry `json:"data"`
}

type codexSkillsListResponse struct {
	Data []codexSkillsListEntry `json:"data"`
}

type codexSkillsListEntry struct {
	CWD    string               `json:"cwd"`
	Errors []codexSkillError    `json:"errors"`
	Skills []codexSkillMetadata `json:"skills"`
}

type codexSkillError struct {
	Message string `json:"message"`
	Path    string `json:"path"`
}

type codexSkillMetadata struct {
	Dependencies     json.RawMessage `json:"dependencies"`
	Description      string          `json:"description"`
	Enabled          bool            `json:"enabled"`
	Interface        json.RawMessage `json:"interface"`
	Name             string          `json:"name"`
	Path             string          `json:"path"`
	Scope            string          `json:"scope"`
	ShortDescription *string         `json:"shortDescription"`
}

type codexHooksListEntry struct {
	CWD      string              `json:"cwd"`
	Errors   []codexHookError    `json:"errors"`
	Hooks    []codexHookMetadata `json:"hooks"`
	Warnings []string            `json:"warnings"`
}

type codexHookError struct {
	Message string `json:"message"`
	Path    string `json:"path"`
}

type codexHookMetadata struct {
	Command       *string `json:"command"`
	CurrentHash   string  `json:"currentHash"`
	DisplayOrder  int64   `json:"displayOrder"`
	Enabled       bool    `json:"enabled"`
	EventName     string  `json:"eventName"`
	HandlerType   string  `json:"handlerType"`
	IsManaged     bool    `json:"isManaged"`
	Key           string  `json:"key"`
	Matcher       *string `json:"matcher"`
	PluginID      *string `json:"pluginId"`
	Source        string  `json:"source"`
	SourcePath    string  `json:"sourcePath"`
	StatusMessage *string `json:"statusMessage"`
	TimeoutSec    uint64  `json:"timeoutSec"`
	TrustStatus   string  `json:"trustStatus"`
}

type codexProtocolReader struct {
	messages int
	scanner  *bufio.Scanner
	total    int
}

// VerifyHostActivation proves that the selected Host discovered the exact
// managed Skill and loaded, enabled and trusted the exact Hook registration.
// It is read-only with respect to Host state: setup must never manufacture
// Codex trust or mutate a private Host database. The projection is verified
// both before and after observation.
func VerifyHostActivation(ctx context.Context, workspace, nodeState string,
	observation HostObservation, bundle assets.Bundle,
) error {
	if ctx == nil {
		return hostActivationUnavailable("validate context", nil)
	}
	if err := VerifyHostProjection(workspace, nodeState, observation.Host, bundle); err != nil {
		return err
	}
	if observation.Host != assets.HostCodex {
		return hostActivationUnavailable("Host has no bounded activation observation surface", nil)
	}
	executable, err := verifyHostExecutable(observation.Executable)
	if err != nil || executable != observation.Executable {
		return hostActivationUnavailable("revalidate Host executable", err)
	}
	version, err := canonicalHostVersion([]byte(observation.Version))
	if err != nil || version != observation.Version {
		return hostActivationUnavailable("revalidate Host version", err)
	}
	registration, ok := bundle.Registration(assets.HostCodex)
	if !ok {
		return hostActivationUnavailable("load Codex registration", nil)
	}

	probeCtx, cancel := context.WithTimeout(ctx, hostActivationTimeout)
	defer cancel()
	command := exec.CommandContext(probeCtx, executable, "app-server", "--stdio")
	command.Dir = workspace
	command.Env = hostActivationEnvironment(os.Environ())
	command.WaitDelay = 250 * time.Millisecond
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	stdin, err := command.StdinPipe()
	if err != nil {
		return hostActivationUnavailable("open Host protocol input", err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return hostActivationUnavailable("open Host protocol output", err)
	}
	var stderr boundedProbeBuffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		_ = stdin.Close()
		return hostActivationUnavailable("start Host activation observation", err)
	}
	waited := false
	watcher := startHostActivationWatcher(probeCtx, command.Process.Pid)
	defer func() {
		watcher.stopAndWait()
		if waited {
			return
		}
		_ = stdin.Close()
		terminateHostActivationProcessGroup(command.Process.Pid)
		_ = command.Wait()
	}()

	reader := newCodexProtocolReader(stdout)
	encoder := json.NewEncoder(stdin)
	encoder.SetEscapeHTML(false)
	initialize := struct {
		ID     int    `json:"id"`
		Method string `json:"method"`
		Params struct {
			Capabilities struct {
				ExperimentalAPI bool `json:"experimentalApi"`
			} `json:"capabilities"`
			ClientInfo struct {
				Name    string `json:"name"`
				Version string `json:"version"`
			} `json:"clientInfo"`
		} `json:"params"`
	}{ID: 1, Method: "initialize"}
	initialize.Params.Capabilities.ExperimentalAPI = true
	initialize.Params.ClientInfo.Name = "mnemon-harness"
	initialize.Params.ClientInfo.Version = "r5"
	if err := encoder.Encode(initialize); err != nil {
		return hostActivationUnavailable("write Host initialize", err)
	}
	if _, err := reader.result(1); err != nil {
		return hostActivationUnavailable("read Host initialize", err)
	}
	if err := encoder.Encode(struct {
		Method string `json:"method"`
	}{Method: "initialized"}); err != nil {
		return hostActivationUnavailable("write Host initialized", err)
	}
	listRequest := struct {
		ID     int    `json:"id"`
		Method string `json:"method"`
		Params struct {
			CWDs []string `json:"cwds"`
		} `json:"params"`
	}{ID: 2, Method: "hooks/list"}
	listRequest.Params.CWDs = []string{workspace}
	if err := encoder.Encode(listRequest); err != nil {
		return hostActivationUnavailable("write Host hooks/list", err)
	}
	raw, err := reader.result(2)
	if err != nil {
		return hostActivationUnavailable("read Host hooks/list", err)
	}
	skillsRequest := struct {
		ID     int    `json:"id"`
		Method string `json:"method"`
		Params struct {
			CWDs        []string `json:"cwds"`
			ForceReload bool     `json:"forceReload"`
		} `json:"params"`
	}{ID: 3, Method: "skills/list"}
	skillsRequest.Params.CWDs = []string{workspace}
	skillsRequest.Params.ForceReload = true
	if err := encoder.Encode(skillsRequest); err != nil {
		return hostActivationUnavailable("write Host skills/list", err)
	}
	skillsRaw, err := reader.result(3)
	if err != nil {
		return hostActivationUnavailable("read Host skills/list", err)
	}
	if err := stdin.Close(); err != nil {
		return hostActivationUnavailable("close Host protocol input", err)
	}
	if err := reader.drainNotifications(); err != nil {
		return hostActivationUnavailable("drain Host protocol output", err)
	}
	waitErr := command.Wait()
	waited = true
	watcher.stopAndWait()
	terminateHostActivationProcessGroup(command.Process.Pid)
	if probeCtx.Err() != nil {
		return hostActivationUnavailable("complete Host activation observation", probeCtx.Err())
	}
	if waitErr != nil || stderr.full {
		return hostActivationUnavailable("complete Host activation observation", waitErr)
	}

	var response codexHooksListResponse
	if err := decodeClosedActivationJSON(raw, &response); err != nil {
		return hostActivationUnavailable("decode Host hooks/list", err)
	}
	var skills codexSkillsListResponse
	if err := decodeClosedActivationJSON(skillsRaw, &skills); err != nil {
		return hostActivationUnavailable("decode Host skills/list", err)
	}
	if err := validateCodexActivation(workspace, registration, response, skills); err != nil {
		return err
	}
	if err := VerifyHostProjection(workspace, nodeState, observation.Host, bundle); err != nil {
		return err
	}
	return nil
}

func validateCodexActivation(workspace string, registration assets.Registration,
	response codexHooksListResponse, skills codexSkillsListResponse,
) error {
	if err := validateCodexSkillActivation(workspace, registration, skills); err != nil {
		return err
	}
	if len(response.Data) != 1 || response.Data[0].CWD != workspace ||
		len(response.Data[0].Errors) != 0 {
		return fmt.Errorf("%w: Codex did not load the project Hook set", ErrHostActivationRequired)
	}
	configPath := filepath.Join(workspace, ".codex", registration.Target)
	hookPath := filepath.Join(workspace, ".codex", "hooks", "mnemon-harness", "hook.sh")
	status := registration.Value.Hook.StatusMessage
	candidates := make([]codexHookMetadata, 0, 1)
	for _, hook := range response.Data[0].Hooks {
		if hook.SourcePath != configPath {
			continue
		}
		if (hook.Command != nil && *hook.Command == hookPath) ||
			(hook.StatusMessage != nil && *hook.StatusMessage == status) {
			candidates = append(candidates, hook)
		}
	}
	if len(candidates) != 1 {
		return fmt.Errorf("%w: Codex did not expose one exact Mnemon Hook", ErrHostActivationRequired)
	}
	hook := candidates[0]
	if hook.Command == nil || *hook.Command != hookPath || hook.StatusMessage == nil ||
		*hook.StatusMessage != status || hook.Source != "project" ||
		hook.EventName != "userPromptSubmit" || hook.HandlerType != "command" ||
		hook.TimeoutSec != uint64(registration.Value.Hook.Timeout) || hook.IsManaged ||
		!hook.Enabled || hook.TrustStatus != "trusted" || hook.CurrentHash == "" ||
		len(hook.CurrentHash) > hostActivationHashMax || !utf8.ValidString(hook.CurrentHash) {
		return fmt.Errorf("%w: Codex project Hook is absent, disabled, modified, or untrusted",
			ErrHostActivationRequired)
	}
	return nil
}

func validateCodexSkillActivation(workspace string, registration assets.Registration,
	skills codexSkillsListResponse,
) error {
	if len(skills.Data) != 1 || skills.Data[0].CWD != workspace ||
		len(skills.Data[0].Errors) != 0 {
		return fmt.Errorf("%w: Codex did not load the project Skill set", ErrHostActivationRequired)
	}
	skillPath := filepath.Join(workspace, filepath.FromSlash(registration.SkillTarget), "SKILL.md")
	matches := 0
	for _, skill := range skills.Data[0].Skills {
		if skill.Path != skillPath {
			continue
		}
		matches++
		if skill.Name != "mnemon-harness" || skill.Scope != "repo" || !skill.Enabled ||
			strings.TrimSpace(skill.Description) == "" {
			return fmt.Errorf("%w: Codex project Skill is disabled or invalid",
				ErrHostActivationRequired)
		}
	}
	if matches != 1 {
		return fmt.Errorf("%w: Codex did not expose one exact Mnemon Skill",
			ErrHostActivationRequired)
	}
	return nil
}

func newCodexProtocolReader(source io.Reader) *codexProtocolReader {
	scanner := bufio.NewScanner(source)
	scanner.Buffer(make([]byte, 4096), hostActivationOutputMax+1)
	return &codexProtocolReader{scanner: scanner}
}

func (reader *codexProtocolReader) next() ([]byte, error) {
	if reader == nil || reader.scanner == nil || reader.messages >= hostActivationMessages {
		return nil, errors.New("Host protocol exceeded its message bound")
	}
	if !reader.scanner.Scan() {
		if err := reader.scanner.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	line := append([]byte(nil), reader.scanner.Bytes()...)
	reader.messages++
	reader.total += len(line) + 1
	if len(line) == 0 || reader.total > hostActivationOutputMax || !utf8.Valid(line) {
		return nil, errors.New("Host protocol output is empty, oversized, or non-UTF-8")
	}
	return line, nil
}

func (reader *codexProtocolReader) result(wantID int) ([]byte, error) {
	for {
		line, err := reader.next()
		if err != nil {
			return nil, err
		}
		var envelope codexRPCEnvelope
		if err := decodeClosedActivationJSON(line, &envelope); err != nil {
			return nil, err
		}
		if len(envelope.ID) == 0 {
			if envelope.Method == "" || len(envelope.Result) != 0 || len(envelope.Error) != 0 {
				return nil, errors.New("Host protocol notification is malformed")
			}
			continue
		}
		if string(envelope.ID) != strconv.Itoa(wantID) {
			return nil, errors.New("Host protocol response identity differs")
		}
		if len(envelope.Error) != 0 && !bytes.Equal(envelope.Error, []byte("null")) {
			return nil, errors.New("Host protocol returned an error")
		}
		if len(envelope.Result) == 0 || bytes.Equal(envelope.Result, []byte("null")) ||
			envelope.Method != "" || len(envelope.Params) != 0 {
			return nil, errors.New("Host protocol response is malformed")
		}
		return append([]byte(nil), envelope.Result...), nil
	}
}

func (reader *codexProtocolReader) drainNotifications() error {
	for {
		line, err := reader.next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		var envelope codexRPCEnvelope
		if err := decodeClosedActivationJSON(line, &envelope); err != nil {
			return err
		}
		if len(envelope.ID) != 0 || envelope.Method == "" || len(envelope.Result) != 0 ||
			len(envelope.Error) != 0 {
			return errors.New("Host protocol emitted a non-notification after hooks/list")
		}
	}
}

func decodeClosedActivationJSON(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("Host protocol JSON contains a trailing value")
	}
	return nil
}

func hostActivationEnvironment(environment []string) []string {
	allowed := map[string]bool{
		"CODEX_HOME": true, "HOME": true, "LANG": true, "LOGNAME": true, "PATH": true,
		"TEMP": true, "TMP": true, "TMPDIR": true, "USER": true,
		"XDG_CONFIG_HOME": true, "XDG_DATA_HOME": true,
	}
	result := make([]string, 0, len(allowed)+4)
	for _, entry := range environment {
		name, _, ok := strings.Cut(entry, "=")
		if ok && (allowed[name] || strings.HasPrefix(name, "LC_")) {
			result = append(result, entry)
		}
	}
	return result
}

func hostActivationUnavailable(stage string, cause error) error {
	if cause != nil {
		return fmt.Errorf("%w: %s: %v", ErrHostUnavailable, stage, cause)
	}
	return fmt.Errorf("%w: %s", ErrHostUnavailable, stage)
}

func terminateHostActivationProcessGroup(pid int) {
	if pid <= 0 {
		return
	}
	_ = syscall.Kill(-pid, syscall.SIGKILL)
}
