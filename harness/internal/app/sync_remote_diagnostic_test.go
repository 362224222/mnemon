package app

import (
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

type staticPullRemoteWorkspace struct {
	resp contract.SyncPullResponse
}

func (s staticPullRemoteWorkspace) SyncPush(contract.SyncPushRequest) (contract.SyncPushResponse, error) {
	return contract.SyncPushResponse{}, nil
}

func (s staticPullRemoteWorkspace) SyncPull(contract.SyncPullRequest) (contract.SyncPullResponse, error) {
	return s.resp, nil
}

func (s staticPullRemoteWorkspace) SyncStatus() (contract.SyncStatusResponse, error) {
	return contract.SyncStatusResponse{}, nil
}

func countRemoteDiagnostics(t *testing.T, rt *runtime.Runtime, remoteID string, want string) int {
	t.Helper()
	events, err := rt.PendingEvents(0)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	n := 0
	for _, ev := range events {
		if ev.Type == "sync.remote_diagnostic.observed" &&
			eventmodel.PayloadRule(ev.Payload)["remote_id"] == remoteID &&
			strings.Contains(stringPayload(eventmodel.PayloadNarrative(ev.Payload), "diagnostic"), want) {
			n++
		}
		if ev.Type == "sync.diagnostic" &&
			strings.Contains(stringPayload(ev.Payload, "reason"), want) {
			n++
		}
	}
	return n
}

func stringPayload(payload map[string]any, key string) string {
	value, _ := payload[key].(string)
	return value
}

func TestWorkerPullRemoteDiagnosticLandsDurablyOnce(t *testing.T) {
	root := t.TempDir()
	rt := openServingRuntime(t, root)
	progressRef := contract.ResourceRef{Kind: "progress_digest", ID: "project"}
	progress := foreignProgressMaterial("dec-remote-diagnostic-progress", "remote-diagnostic-progress", "valid progress imports beside diagnostic")
	progressEnv, err := contract.SyncedEventEnvelopeFromMaterial(progress)
	if err != nil {
		t.Fatalf("materialize progress: %v", err)
	}
	remote := staticPullRemoteWorkspace{resp: contract.SyncPullResponse{
		Events: []eventmodel.EventEnvelope{progressEnv},
		Diagnostics: []contract.EventExchangeResult{{
			OriginMnemond: "agent-b",
			EventID:       "bad-publication-entry",
			Subject:       eventmodel.Subject("progress_digest", "project"),
			Status:        "invalid",
			Diagnostic:    "publication digest mismatch",
		}},
		NextCursor: "1",
	}}

	if err := syncWorkerPull(rt, remote, "github-sub", nil); err != nil {
		t.Fatalf("worker pull with diagnostic: %v", err)
	}
	_, fields, err := rt.Resource(progressRef)
	if err != nil {
		t.Fatalf("read progress: %v", err)
	}
	if content, _ := fields["content"].(string); !strings.Contains(content, "valid progress imports beside diagnostic") {
		t.Fatalf("valid pull event must still import:\n%s", content)
	}
	if got := countRemoteDiagnostics(t, rt, "github-sub", "publication digest mismatch"); got != 2 {
		t.Fatalf("remote diagnostic must land as observation + durable diagnostic, got %d matching events", got)
	}

	if err := syncWorkerPull(rt, remote, "github-sub", nil); err != nil {
		t.Fatalf("repeat worker pull with diagnostic: %v", err)
	}
	if got := countRemoteDiagnostics(t, rt, "github-sub", "publication digest mismatch"); got != 2 {
		t.Fatalf("repeat pull must dedupe remote diagnostic, got %d matching events", got)
	}
}
