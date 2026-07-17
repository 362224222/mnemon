package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	runtimeProcessSchemaVersion = 1
	runtimeProcessJSONMax       = 1024
	runtimeProcessTokenMax      = 256
	runtimeProcessSupportWait   = 10 * time.Second
)

// checkSystemRuntimeProcessSupport proves both the observation primitives and
// the complete owned-child group termination path before the adapter can make
// a worker ready. The projected Hook already requires /bin/sh; /bin/sleep is
// therefore part of the same minimal Unix host contract, not an extra Runtime
// dependency.
func checkSystemRuntimeProcessSupport() (supportErr error) {
	if err := checkSystemRuntimeProcessPrimitives(); err != nil {
		return runtimeProcessError("support", err)
	}
	command := exec.Command("/bin/sleep", "30")
	command.Env = []string{"PATH=/usr/bin:/bin"}
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := command.Start(); err != nil {
		return runtimeProcessError("support helper", err)
	}
	waited := false
	defer func() {
		if waited {
			return
		}
		_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		if err := command.Wait(); err != nil {
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) {
				supportErr = errors.Join(supportErr, runtimeProcessError("support cleanup", err))
			}
		}
	}()
	ids, _, err := captureRuntimeProcessIDs(command.Process.Pid)
	if err != nil {
		return runtimeProcessError("support capture", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), runtimeProcessSupportWait)
	proof, err := terminateOwnedSystemRuntimeProcess(ctx, ids)
	cancel()
	if err != nil {
		return runtimeProcessError("support terminate", err)
	}
	if err := validateRuntimeProcessProof(proof); err != nil || len(proof.signals) == 0 {
		if err == nil {
			err = errors.New("support helper exited without exercising group termination")
		}
		return runtimeProcessError("support proof", err)
	}
	waitErr := command.Wait()
	waited = true
	if waitErr != nil {
		var exitErr *exec.ExitError
		if !errors.As(waitErr, &exitErr) {
			return runtimeProcessError("support wait", waitErr)
		}
	}
	return nil
}

var (
	ErrRuntimeProcess          = errors.New("managed Runtime process")
	ErrRuntimeProcessLive      = errors.New("managed Runtime process is still live")
	ErrRuntimeProcessUncertain = errors.New("managed Runtime process identity is uncertain")
)

// runtimeProcessIDs is the complete restart authority for one isolated
// Runtime process group. PID alone is deliberately never sufficient: a boot
// identity and kernel process-start token must bind it to one process
// incarnation, and Setsid requires the leader to own both its group and
// session.
type runtimeProcessIDs struct {
	SchemaVersion int    `json:"schema_version"`
	OS            string `json:"os"`
	PID           int    `json:"pid"`
	PGID          int    `json:"pgid"`
	SID           int    `json:"sid"`
	UID           uint32 `json:"uid"`
	StartToken    string `json:"start_token"`
}

type runtimeProcessState string

const (
	runtimeProcessGone        runtimeProcessState = "gone"
	runtimeProcessExactExited runtimeProcessState = "exact_exited"
	runtimeProcessReused      runtimeProcessState = "reused"
)

type runtimeProcessPlatformProof struct {
	state   runtimeProcessState
	method  string
	signals []string
}

type runtimeProcessRecovery struct {
	IDs     runtimeProcessIDs
	State   runtimeProcessState
	Receipt model.JSON
	At      time.Time
}

// captureRuntimeProcessIDs observes the initialized child and serializes the
// exact platform identity that RecordAgentRuntimeLaunch persists. The adapter
// must call it only after starting the child with Setsid.
func captureRuntimeProcessIDs(pid int) (runtimeProcessIDs, model.JSON, error) {
	ids, err := captureSystemRuntimeProcess(pid)
	if err != nil {
		return runtimeProcessIDs{}, model.JSON{}, runtimeProcessError("capture", err)
	}
	if err := validateRuntimeProcessIDs(ids, runtime.GOOS); err != nil {
		return runtimeProcessIDs{}, model.JSON{}, runtimeProcessError("capture", err)
	}
	encoded, err := model.JSONFrom(ids)
	if err != nil {
		return runtimeProcessIDs{}, model.JSON{}, runtimeProcessError("encode", err)
	}
	if len(encoded.Bytes()) > runtimeProcessJSONMax {
		return runtimeProcessIDs{}, model.JSON{}, runtimeProcessError("encode",
			errors.New("Runtime IDs exceed their bound"))
	}
	return ids, encoded, nil
}

// parseRuntimeProcessIDs accepts only the closed canonical schema. Unknown
// fields are rejected so later code cannot accidentally treat an extension as
// process authority it never verified.
func parseRuntimeProcessIDs(value model.JSON) (runtimeProcessIDs, error) {
	if value.IsZero() || len(value.Bytes()) == 0 || len(value.Bytes()) > runtimeProcessJSONMax {
		return runtimeProcessIDs{}, runtimeProcessError("parse",
			errors.New("Runtime IDs are empty or exceed their bound"))
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(value.Bytes(), &members); err != nil {
		return runtimeProcessIDs{}, runtimeProcessError("parse", err)
	}
	required := [...]string{"schema_version", "os", "pid", "pgid", "sid", "uid", "start_token"}
	if len(members) != len(required) {
		return runtimeProcessIDs{}, runtimeProcessError("parse",
			errors.New("Runtime IDs do not contain the exact closed field set"))
	}
	for _, field := range required {
		if _, exists := members[field]; !exists {
			return runtimeProcessIDs{}, runtimeProcessError("parse",
				fmt.Errorf("Runtime IDs lack required field %q", field))
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(value.Bytes()))
	decoder.DisallowUnknownFields()
	var ids runtimeProcessIDs
	if err := decoder.Decode(&ids); err != nil {
		return runtimeProcessIDs{}, runtimeProcessError("parse", err)
	}
	if err := requireRuntimeProcessJSONEOF(decoder); err != nil {
		return runtimeProcessIDs{}, runtimeProcessError("parse", err)
	}
	if err := validateRuntimeProcessIDs(ids, runtime.GOOS); err != nil {
		return runtimeProcessIDs{}, runtimeProcessError("parse", err)
	}
	canonical, err := model.JSONFrom(ids)
	if err != nil || canonical.String() != value.String() {
		return runtimeProcessIDs{}, runtimeProcessError("parse",
			errors.New("Runtime IDs differ from the closed canonical schema"))
	}
	return ids, nil
}

// recoverRuntimeProcess performs only OS process work. The caller supplies its
// trusted clock, which is sampled after exit proof exists; Store admission and
// settlement must happen later in a separate short transaction.
func recoverRuntimeProcess(ctx context.Context, value model.JSON,
	now func() time.Time,
) (runtimeProcessRecovery, error) {
	if ctx == nil || now == nil {
		return runtimeProcessRecovery{}, runtimeProcessError("recover",
			errors.New("context and trusted clock are required"))
	}
	if err := ctx.Err(); err != nil {
		return runtimeProcessRecovery{}, runtimeProcessError("recover", err)
	}
	ids, err := parseRuntimeProcessIDs(value)
	if err != nil {
		return runtimeProcessRecovery{}, err
	}
	proof, err := recoverSystemRuntimeProcess(ctx, ids)
	if err != nil {
		return runtimeProcessRecovery{}, runtimeProcessError("recover", err)
	}
	if err := validateRuntimeProcessProof(proof); err != nil {
		return runtimeProcessRecovery{}, runtimeProcessError("recover", err)
	}
	if err := ctx.Err(); err != nil {
		return runtimeProcessRecovery{}, runtimeProcessError("recover", err)
	}
	recovery, err := newRuntimeProcessRecovery(value, ids, proof, now())
	if err != nil {
		return runtimeProcessRecovery{}, runtimeProcessError("receipt", err)
	}
	return recovery, nil
}

// newRuntimeProcessRecovery is the single production constructor for a
// durable recovery result. Keeping receipt construction here prevents tests or
// callers from drifting from the digest and signal representation that Store
// settlement audits later.
func newRuntimeProcessRecovery(value model.JSON, ids runtimeProcessIDs,
	proof runtimeProcessPlatformProof, at time.Time,
) (runtimeProcessRecovery, error) {
	encodedIDs, err := model.JSONFrom(ids)
	if err != nil || encodedIDs.String() != value.String() {
		return runtimeProcessRecovery{}, errors.New("Runtime IDs and recovery authority differ")
	}
	if err := validateRuntimeProcessProof(proof); err != nil {
		return runtimeProcessRecovery{}, err
	}
	canonical, err := canonicalRuntimeProcessTime(at)
	if err != nil {
		return runtimeProcessRecovery{}, err
	}
	signals := append([]string(nil), proof.signals...)
	if signals == nil {
		signals = make([]string, 0)
	}
	receipt, err := model.JSONFrom(struct {
		Kind             string              `json:"kind"`
		Method           string              `json:"method"`
		ProcessExit      string              `json:"process_exit"`
		RuntimeIDsDigest string              `json:"runtime_ids_digest"`
		SchemaVersion    int                 `json:"schema_version"`
		Signals          []string            `json:"signals"`
		State            runtimeProcessState `json:"state"`
	}{Kind: "startup_orphan", Method: proof.method, ProcessExit: "confirmed",
		RuntimeIDsDigest: model.Sum(value.Bytes()).String(), SchemaVersion: runtimeProcessSchemaVersion,
		Signals: signals, State: proof.state})
	if err != nil {
		return runtimeProcessRecovery{}, err
	}
	return runtimeProcessRecovery{IDs: ids, State: proof.state, Receipt: receipt, At: canonical}, nil
}

func validateRuntimeProcessIDs(ids runtimeProcessIDs, expectedOS string) error {
	if ids.SchemaVersion != runtimeProcessSchemaVersion ||
		(expectedOS != "linux" && expectedOS != "darwin") || ids.OS != expectedOS {
		return errors.New("Runtime IDs have an unsupported schema or operating system")
	}
	if ids.PID <= 1 || ids.PID > 1<<31-1 || ids.PGID != ids.PID || ids.SID != ids.PID {
		return errors.New("Runtime leader is not an isolated positive process group and session")
	}
	return validateRuntimeProcessStartToken(ids.OS, ids.StartToken)
}

func validateRuntimeProcessStartToken(operatingSystem, token string) error {
	if token == "" || len(token) > runtimeProcessTokenMax ||
		!utf8.ValidString(token) || strings.TrimSpace(token) != token {
		return errors.New("Runtime process start token is invalid")
	}
	parts := strings.Split(token, ":")
	switch operatingSystem {
	case "linux":
		if len(parts) != 3 || parts[0] != "linux" || !canonicalRuntimeProcessUUID(parts[1]) {
			return errors.New("Linux process start token is invalid")
		}
		start, err := strconv.ParseUint(parts[2], 10, 64)
		if err != nil || start == 0 || strconv.FormatUint(start, 10) != parts[2] {
			return errors.New("Linux process start token is invalid")
		}
	case "darwin":
		if len(parts) != 4 || parts[0] != "darwin" || !canonicalRuntimeProcessUUID(parts[1]) {
			return errors.New("Darwin process start token is invalid")
		}
		seconds, secondsErr := strconv.ParseUint(parts[2], 10, 64)
		micros, microsErr := strconv.ParseUint(parts[3], 10, 32)
		if secondsErr != nil || microsErr != nil || seconds == 0 || micros >= 1_000_000 ||
			strconv.FormatUint(seconds, 10) != parts[2] || strconv.FormatUint(micros, 10) != parts[3] {
			return errors.New("Darwin process start token is invalid")
		}
	default:
		return errors.New("Runtime process start token has an unsupported operating system")
	}
	return nil
}

func runtimeProcessTokenBootID(ids runtimeProcessIDs) string {
	parts := strings.Split(ids.StartToken, ":")
	if len(parts) < 2 {
		return ""
	}
	return parts[1]
}

func canonicalRuntimeProcessUUID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed.String() == value
}

func validateRuntimeProcessProof(proof runtimeProcessPlatformProof) error {
	if proof.state != runtimeProcessGone && proof.state != runtimeProcessExactExited &&
		proof.state != runtimeProcessReused {
		return errors.New("platform did not prove Runtime process exit")
	}
	methodStates := map[string]runtimeProcessState{
		"darwin_owned_group_kill":     runtimeProcessExactExited,
		"boot_session_changed":        runtimeProcessGone,
		"exact_process_exited":        runtimeProcessExactExited,
		"linux_owned_group_stop_kill": runtimeProcessExactExited,
		"pid_reused_group_absent":     runtimeProcessReused,
		"process_and_group_absent":    runtimeProcessGone,
	}
	expectedState, known := methodStates[proof.method]
	if !known {
		return errors.New("platform returned an unknown exit-proof method")
	}
	if proof.state != expectedState {
		return errors.New("platform exit-proof method and state differ")
	}
	switch proof.method {
	case "linux_owned_group_stop_kill":
		if proof.state != runtimeProcessExactExited || len(proof.signals) != 2 ||
			proof.signals[0] != "SIGSTOP" || proof.signals[1] != "SIGKILL" {
			return errors.New("Linux owned-group proof lacks its stop/kill barrier")
		}
	case "darwin_owned_group_kill":
		if len(proof.signals) != 2 || proof.signals[0] != "SIGSTOP" ||
			proof.signals[1] != "SIGKILL" {
			return errors.New("Darwin kill proof lacks its stop/kill barrier")
		}
	default:
		if len(proof.signals) != 0 {
			return errors.New("observation-only exit proof contains process signals")
		}
	}
	return nil
}

func canonicalRuntimeProcessTime(value time.Time) (time.Time, error) {
	canonical := value.Round(0).UTC()
	if canonical.IsZero() || canonical.UnixNano() <= 0 ||
		!time.Unix(0, canonical.UnixNano()).UTC().Equal(canonical) {
		return time.Time{}, errors.New("trusted clock returned a non-canonical instant")
	}
	return canonical, nil
}

func requireRuntimeProcessJSONEOF(decoder *json.Decoder) error {
	var trailing struct{}
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("Runtime IDs contain a trailing JSON value")
		}
		return err
	}
	return nil
}

func runtimeProcessError(stage string, cause error) error {
	if cause == nil {
		cause = errors.New("unknown failure")
	}
	return fmt.Errorf("%w: %s: %w", ErrRuntimeProcess, stage, cause)
}
