package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

func TestCloudflareEnvFileMergesWithoutOverridingProcessEnv(t *testing.T) {
	t.Setenv("CLOUDFLARE_API_TOKEN", "process-token")
	path := filepath.Join(t.TempDir(), "cloudflare.env")
	if err := os.WriteFile(path, []byte("CLOUDFLARE_API_TOKEN=file-token\nCLOUDFLARE_ACCOUNT_ID=account-1\nMNEMON_CLOUDFLARE_WORKER_NAME=mnemon-r3\nMNEMON_CLOUDFLARE_SUBDOMAIN=mnemon-test\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	env, used, err := loadCloudflareBootstrapEnv(path)
	if err != nil {
		t.Fatal(err)
	}
	if used != path {
		t.Fatalf("env file = %q, want %q", used, path)
	}
	if env["CLOUDFLARE_API_TOKEN"] != "process-token" {
		t.Fatalf("process env must win, got %q", env["CLOUDFLARE_API_TOKEN"])
	}
	if env["CLOUDFLARE_ACCOUNT_ID"] != "account-1" || env["MNEMON_CLOUDFLARE_WORKER_NAME"] != "mnemon-r3" || env["MNEMON_CLOUDFLARE_SUBDOMAIN"] != "mnemon-test" {
		t.Fatalf("file values not loaded: %+v", env)
	}
}

func TestCloudflareEnvFileRejectsLoosePermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cloudflare.env")
	if err := os.WriteFile(path, []byte("CLOUDFLARE_API_TOKEN=file-token\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, _, err := readCloudflareEnvFile(path)
	if err == nil || !strings.Contains(err.Error(), "0600") {
		t.Fatalf("expected permission error, got %v", err)
	}
}

func TestCloudflareEndpointFromWranglerOutput(t *testing.T) {
	out := "Uploaded mnemon\nPublished mnemon-r3 (1.2 sec)\n  https://mnemon-r3.example.workers.dev\n"
	if got := cloudflareEndpointFromWranglerOutput(out); got != "https://mnemon-r3.example.workers.dev" {
		t.Fatalf("endpoint = %q", got)
	}
}

func TestEnsureCloudflareWorkersSubdomainCreatesWhenMissing(t *testing.T) {
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer cf-token" {
			t.Fatalf("authorization header = %q", got)
		}
		requests = append(requests, r.Method+" "+r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			_, _ = w.Write([]byte(`{"success":true,"result":{"subdomain":""},"errors":[]}`))
		case http.MethodPut:
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body["subdomain"] != "mnemon-0ff22127" {
				t.Fatalf("subdomain body = %+v", body)
			}
			_, _ = w.Write([]byte(`{"success":true,"result":{"subdomain":"mnemon-0ff22127"},"errors":[]}`))
		default:
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	got, err := ensureCloudflareWorkersSubdomain(context.Background(), cloudflareBootstrapPlan{
		APIToken:   "cf-token",
		AccountID:  "0ff22127f3f11976dfea078f13f4c056",
		HTTPClient: server.Client(),
		APIBaseURL: server.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != "mnemon-0ff22127" {
		t.Fatalf("subdomain = %q", got)
	}
	want := []string{
		"GET /accounts/0ff22127f3f11976dfea078f13f4c056/workers/subdomain",
		"PUT /accounts/0ff22127f3f11976dfea078f13f4c056/workers/subdomain",
	}
	if strings.Join(requests, "\n") != strings.Join(want, "\n") {
		t.Fatalf("requests = %#v, want %#v", requests, want)
	}
}

func TestEnsureCloudflareWorkersSubdomainReusesExisting(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("existing subdomain should not be overwritten, got %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true,"result":{"subdomain":"existing-subdomain"},"errors":[]}`))
	}))
	defer server.Close()

	got, err := ensureCloudflareWorkersSubdomain(context.Background(), cloudflareBootstrapPlan{
		APIToken:   "cf-token",
		AccountID:  "acct-1",
		Subdomain:  "ignored-when-existing",
		HTTPClient: server.Client(),
		APIBaseURL: server.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != "existing-subdomain" {
		t.Fatalf("subdomain = %q", got)
	}
}

func TestCloudflareGrantAndTokenJSONDoNotExposeCloudflareToken(t *testing.T) {
	plan := cloudflareBootstrapPlan{
		APIToken:     "cfat_secret",
		ReplicaToken: "replica-secret",
		Principal:    "planner@team",
		Scopes:       mustParseScopesForTest(t, []string{"memory/project"}),
	}
	tokens := cloudflareTokensJSON(plan)
	if strings.Contains(tokens, plan.APIToken) {
		t.Fatalf("tokens JSON leaked Cloudflare token: %s", tokens)
	}
	grants := cloudflareGrantsJSON(plan)
	if strings.Contains(grants, plan.APIToken) || strings.Contains(grants, plan.ReplicaToken) {
		t.Fatalf("grants JSON leaked a secret: %s", grants)
	}
	var decoded map[string]map[string]any
	if err := json.Unmarshal([]byte(grants), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["planner@team"]["principal"] != "planner@team" {
		t.Fatalf("grant principal mismatch: %+v", decoded)
	}
	scopes, ok := decoded["planner@team"]["scopes"].([]any)
	if !ok || len(scopes) != 1 {
		t.Fatalf("grant scopes missing: %+v", decoded["planner@team"]["scopes"])
	}
	scope, ok := scopes[0].(map[string]any)
	if !ok || scope["kind"] != "memory" || scope["id"] != "project" {
		t.Fatalf("grant scopes must use lower-case ABI keys, got %+v", scopes[0])
	}
	if _, hasUpperKind := scope["Kind"]; hasUpperKind {
		t.Fatalf("grant scopes leaked Go field name Kind: %+v", scope)
	}
}

func TestBootstrapCloudflareNoDeployWritesLocalConfigWithoutRunningCommands(t *testing.T) {
	root := t.TempDir()
	plan := cloudflareBootstrapPlan{
		Root:         root,
		WorkerName:   "mnemon-r3",
		Principal:    "planner@team",
		ReplicaID:    "local-a",
		ReplicaToken: "replica-secret",
		RemoteID:     "cloudflare",
		Scopes:       mustParseScopesForTest(t, []string{"memory/project"}),
		Endpoint:     "https://mnemon-r3.example.workers.dev",
		NoDeploy:     true,
		CommandRunner: func(context.Context, commandInvocation) (commandResult, error) {
			t.Fatal("no-deploy must not run external commands")
			return commandResult{}, nil
		},
	}
	result, err := bootstrapCloudflareHub(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if result.Endpoint != plan.Endpoint || result.ConfigPath == "" || result.TokenRef == "" {
		t.Fatalf("result mismatch: %+v", result)
	}
	tokenPath := filepath.Join(root, filepath.FromSlash(result.TokenRef))
	if raw, err := os.ReadFile(tokenPath); err != nil || strings.TrimSpace(string(raw)) != "replica-secret" {
		t.Fatalf("token file mismatch raw=%q err=%v", string(raw), err)
	}
	cfg, err := os.ReadFile(filepath.Join(root, ".mnemon", "harness", "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(cfg), "replica-secret") || strings.Contains(string(cfg), "cfat_") {
		t.Fatalf("product config must not contain secrets:\n%s", string(cfg))
	}
}

func mustParseScopesForTest(t *testing.T, values []string) []contract.ResourceRef {
	t.Helper()
	scopes, err := parseScopeRefs(values)
	if err != nil {
		t.Fatal(err)
	}
	return scopes
}
