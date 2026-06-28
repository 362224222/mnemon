package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/driver"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation"
)

func TestAgentRunDryRunPrintsSentinelOnly(t *testing.T) {
	var out, errw bytes.Buffer
	err := run(context.Background(), []string{"agent", "run", "--principal", "codex-a@project", "--dry-run"}, &out, &errw)
	if err != nil {
		t.Fatalf("agent run dry-run: %v\nstderr=%s", err, errw.String())
	}
	if got := strings.TrimSpace(out.String()); got != driver.ManagedWakeQuery {
		t.Fatalf("dry-run output = %q, want %q", got, driver.ManagedWakeQuery)
	}
}

func TestAgentRunRequiresPrincipal(t *testing.T) {
	var out, errw bytes.Buffer
	err := run(context.Background(), []string{"agent", "run", "--dry-run"}, &out, &errw)
	if err == nil || !strings.Contains(err.Error(), "--principal") {
		t.Fatalf("missing principal should fail clearly, got err=%v out=%q stderr=%q", err, out.String(), errw.String())
	}
}

func TestAgentRunNoopRecordsSentinelQuery(t *testing.T) {
	var out, errw bytes.Buffer
	err := run(context.Background(), []string{"agent", "run", "--principal", "codex-a@project", "--runtime", "noop", "--workspace", t.TempDir()}, &out, &errw)
	if err != nil {
		t.Fatalf("agent run noop: %v\nstderr=%s", err, errw.String())
	}
	if !strings.Contains(out.String(), `"query": "`+driver.ManagedWakeQuery+`"`) {
		t.Fatalf("noop run should report sentinel query only:\n%s", out.String())
	}
}

func TestAgentRunNoopUsesRenderedWakeCandidate(t *testing.T) {
	rendered := presentation.Response{
		SchemaVersion: 1,
		Status:        presentation.StatusOK,
		AuditID:       "render-1",
		BodyDigest:    "sha256:render",
		Events: []eventmodel.EventEnvelope{eventmodel.DerivedEnvelope(eventmodel.Event{
			SchemaVersion: eventmodel.SchemaVersion,
			ID:            "derived:assignment.work_available:assignment/asg1:codex-a@project",
			Type:          "assignment.work_available",
			Subject:       "assignment/asg1",
			Actor:         "mnemond@local",
			Audience:      "codex-a@project",
			Payload:       eventmodel.BuildPayload(nil, map[string]any{"body": "Assignment asg1 is yours."}, nil),
		}, "2026-06-24T10:00:00Z", "2026-06-24T10:05:00Z", "work", nil)},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/render" {
			t.Fatalf("path = %s, want /render", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer local-token" {
			t.Fatalf("Authorization = %q, want bearer token", got)
		}
		_ = json.NewEncoder(w).Encode(rendered)
	}))
	defer server.Close()
	root := t.TempDir()
	tokenFile := filepath.Join(root, "token")
	if err := os.WriteFile(tokenFile, []byte("local-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out, errw bytes.Buffer
	err := run(context.Background(), []string{
		"agent", "run",
		"--principal", "codex-a@project",
		"--runtime", "noop",
		"--addr", server.URL,
		"--token-file", tokenFile,
		"--workspace", root,
	}, &out, &errw)
	if err != nil {
		t.Fatalf("agent run noop render: %v\nstderr=%s", err, errw.String())
	}
	if !strings.Contains(out.String(), `"query": "`+driver.ManagedWakeQuery+`"`) {
		t.Fatalf("render candidate run should report sentinel query only:\n%s", out.String())
	}
	if !strings.Contains(out.String(), `"derived_event_id": "derived:assignment.work_available:assignment/asg1:codex-a@project"`) {
		t.Fatalf("render candidate did not become wake record:\n%s", out.String())
	}
	if !strings.Contains(out.String(), `"render_audit_id": "render-1"`) {
		t.Fatalf("wake record should carry render audit id:\n%s", out.String())
	}
}
