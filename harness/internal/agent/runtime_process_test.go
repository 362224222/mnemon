package agent

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestRuntimeProcessIDsAreClosedCanonicalAuthority(t *testing.T) {
	ids := runtimeProcessTestIDs()
	value, err := model.JSONFrom(ids)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parseRuntimeProcessIDs(value)
	if err != nil || parsed != ids {
		t.Fatalf("parseRuntimeProcessIDs() = (%#v, %v)", parsed, err)
	}

	tests := []struct {
		name  string
		value string
	}{
		{"unknown field", strings.TrimSuffix(value.String(), "}") + `,"thread_id":"wrong"}`},
		{"missing field", `{"os":"` + runtime.GOOS + `","pgid":42,"pid":42,"schema_version":1,"sid":42,"start_token":"` + ids.StartToken + `"}`},
		{"wrong schema", strings.Replace(value.String(), `"schema_version":1`, `"schema_version":2`, 1)},
		{"wrong OS", strings.Replace(value.String(), `"os":"`+runtime.GOOS+`"`, `"os":"other"`, 1)},
		{"leader PID one", strings.Replace(value.String(), `"pgid":42,"pid":42`, `"pgid":1,"pid":1`, 1)},
		{"group differs", strings.Replace(value.String(), `"pgid":42`, `"pgid":43`, 1)},
		{"session differs", strings.Replace(value.String(), `"sid":42`, `"sid":43`, 1)},
		{"non-canonical token number", strings.Replace(value.String(), ids.StartToken,
			strings.TrimSuffix(ids.StartToken, "1")+"01", 1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate, err := model.NewJSON([]byte(test.value))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := parseRuntimeProcessIDs(candidate); !errors.Is(err, ErrRuntimeProcess) {
				t.Fatalf("parse error = %v", err)
			}
		})
	}

	oversized, err := model.NewJSON([]byte(`{"padding":"` +
		strings.Repeat("x", runtimeProcessJSONMax) + `"}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseRuntimeProcessIDs(oversized); !errors.Is(err, ErrRuntimeProcess) {
		t.Fatalf("oversized parse error = %v", err)
	}
}

func TestRuntimeProcessProofAndReceiptAreClosed(t *testing.T) {
	ids := runtimeProcessTestIDs()
	value, err := model.JSONFrom(ids)
	if err != nil {
		t.Fatal(err)
	}
	proofs := []runtimeProcessPlatformProof{
		{state: runtimeProcessGone, method: "boot_session_changed"},
		{state: runtimeProcessGone, method: "process_and_group_absent"},
		{state: runtimeProcessReused, method: "pid_reused_group_absent"},
		{state: runtimeProcessExactExited, method: "exact_process_exited"},
		{state: runtimeProcessExactExited, method: "linux_owned_group_stop_kill",
			signals: []string{"SIGSTOP", "SIGKILL"}},
		{state: runtimeProcessExactExited, method: "darwin_owned_group_kill",
			signals: []string{"SIGSTOP", "SIGKILL"}},
	}
	for _, proof := range proofs {
		if err := validateRuntimeProcessProof(proof); err != nil {
			t.Fatalf("valid proof %#v: %v", proof, err)
		}
	}
	invalid := []runtimeProcessPlatformProof{
		{},
		{state: runtimeProcessGone, method: "unknown"},
		{state: runtimeProcessReused, method: "boot_session_changed"},
		{state: runtimeProcessGone, method: "exact_process_exited"},
		{state: runtimeProcessGone, method: "process_and_group_absent", signals: []string{"SIGKILL"}},
		{state: runtimeProcessExactExited, method: "linux_owned_group_stop_kill", signals: []string{"SIGKILL"}},
	}
	for _, proof := range invalid {
		if err := validateRuntimeProcessProof(proof); err == nil {
			t.Fatalf("invalid proof accepted: %#v", proof)
		}
	}

	// Receipt construction is exercised independently of OS recovery so its
	// digest, signal list and trusted-time rules stay deterministic.
	proof := runtimeProcessPlatformProof{state: runtimeProcessGone, method: "process_and_group_absent"}
	if err := validateRuntimeProcessProof(proof); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 7, 17, 12, 0, 0, 123, time.UTC)
	receipt, err := newRuntimeProcessRecovery(value, ids, proof, at)
	if err != nil {
		t.Fatal(err)
	}
	wantDigest := model.Sum(value.Bytes()).String()
	if !strings.Contains(receipt.Receipt.String(), `"runtime_ids_digest":"`+wantDigest+`"`) ||
		receipt.State != runtimeProcessGone || !receipt.At.Equal(at) ||
		!strings.Contains(receipt.Receipt.String(), `"signals":[]`) {
		t.Fatalf("recovery receipt = %#v", receipt)
	}
	different := ids
	different.PID, different.PGID, different.SID = ids.PID+1, ids.PID+1, ids.PID+1
	if _, err := newRuntimeProcessRecovery(value, different, proof, at); err == nil {
		t.Fatal("recovery constructor accepted mismatched digest authority")
	}
}

func TestRuntimeProcessRecoveryRejectsInputBeforePlatformWork(t *testing.T) {
	if _, err := recoverRuntimeProcess(nil, model.JSON{}, time.Now); !errors.Is(err, ErrRuntimeProcess) {
		t.Fatalf("nil recovery error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	value, err := model.JSONFrom(runtimeProcessTestIDs())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recoverRuntimeProcess(ctx, value, time.Now); !errors.Is(err, ErrRuntimeProcess) {
		t.Fatalf("canceled recovery error = %v", err)
	}
	if _, err := canonicalRuntimeProcessTime(time.Time{}); err == nil {
		t.Fatal("zero trusted time was accepted")
	}
}

func runtimeProcessTestIDs() runtimeProcessIDs {
	const boot = "123e4567-e89b-12d3-a456-426614174000"
	token := "linux:" + boot + ":1"
	if runtime.GOOS == "darwin" {
		token = "darwin:" + boot + ":1:0"
	}
	return runtimeProcessIDs{SchemaVersion: runtimeProcessSchemaVersion, OS: runtime.GOOS,
		PID: 42, PGID: 42, SID: 42, UID: 501, StartToken: token}
}
