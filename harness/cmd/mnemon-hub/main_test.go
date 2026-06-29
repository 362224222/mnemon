package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
)

func writeReplicas(t *testing.T, dir, content string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(dir, "replicas.json")
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeToken(t *testing.T, dir, name, token string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestHelpDescribesRemoteExchangeBackend(t *testing.T) {
	var errw bytes.Buffer
	err := run(context.Background(), []string{"--help"}, io.Discard, &errw)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("help error = %v, want flag.ErrHelp", err)
	}
	got := errw.String()
	for _, want := range []string{"remote event exchange backend", "replica push", "pull", "status", "cursors", "tenant boundaries"} {
		if !strings.Contains(got, want) {
			t.Fatalf("mnemon-hub help missing %q:\n%s", want, got)
		}
	}
	for _, blocked := range []string{"managed runtime", "Multica projection", "local drive source"} {
		if strings.Contains(got, blocked) {
			t.Fatalf("mnemon-hub help leaked non-exchange wording %q:\n%s", blocked, got)
		}
	}
}

const twoReplicaDoc = `{
  "schema_version": 1,
  "replicas": [
    {"principal": "replica-a@team", "credential_ref": "a.token",
     "scopes": [{"kind": "memory", "id": "project"}, {"kind": "skill", "id": "project"}]},
    {"principal": "replica-b@team", "credential_ref": "b.token",
     "scopes": [{"kind": "memory", "id": "project"}]}
  ]
}`

// replicas.json is fail-closed at every gate: strict decoding (unknown fields), schema version,
// world-readable refusal, empty scopes, missing credential, duplicate principal/token.
func TestLoadReplicasFailClosed(t *testing.T) {
	dir := t.TempDir()
	writeToken(t, dir, "a.token", "tok-a")
	writeToken(t, dir, "b.token", "tok-b")

	path := writeReplicas(t, dir, twoReplicaDoc, 0o600)
	grants, tokens, err := loadReplicas(path)
	if err != nil {
		t.Fatalf("valid replicas.json: %v", err)
	}
	if len(grants) != 2 || len(tokens) != 2 || tokens["tok-a"] != "replica-a@team" {
		t.Fatalf("grants/tokens not assembled: %+v / %+v", grants, tokens)
	}
	if g, ok := grants.Grant("replica-b@team", contract.SyncVerbPull); !ok || len(g.Scopes) != 1 || g.Scopes[0].Kind != "memory" {
		t.Fatalf("replica-b grant scopes wrong: %+v ok=%v", g, ok)
	}

	cases := []struct {
		name string
		doc  string
		mode os.FileMode
		want string
	}{
		{"world readable", twoReplicaDoc, 0o644, "world-readable"},
		{"unknown field", `{"schema_version":1,"replicas":[{"principal":"p","credential_ref":"a.token","scopes":[{"kind":"memory","id":"project"}],"extra":true}]}`, 0o600, "unknown field"},
		{"bad schema", `{"schema_version":2,"replicas":[]}`, 0o600, "schema_version"},
		{"no replicas", `{"schema_version":1,"replicas":[]}`, 0o600, "no replicas"},
		{"empty scopes", `{"schema_version":1,"replicas":[{"principal":"p","credential_ref":"a.token","scopes":[]}]}`, 0o600, "scopes must be non-empty"},
		{"missing credential", `{"schema_version":1,"replicas":[{"principal":"p","scopes":[{"kind":"memory","id":"project"}]}]}`, 0o600, "credential_ref is required"},
		{"duplicate principal", `{"schema_version":1,"replicas":[{"principal":"p","credential_ref":"a.token","scopes":[{"kind":"memory","id":"project"}]},{"principal":"p","credential_ref":"b.token","scopes":[{"kind":"memory","id":"project"}]}]}`, 0o600, "duplicate principal"},
		{"duplicate token", `{"schema_version":1,"replicas":[{"principal":"p1","credential_ref":"a.token","scopes":[{"kind":"memory","id":"project"}]},{"principal":"p2","credential_ref":"a.token","scopes":[{"kind":"memory","id":"project"}]}]}`, 0o600, "also bound"},
	}
	for _, tc := range cases {
		caseDir := t.TempDir()
		writeToken(t, caseDir, "a.token", "tok-a")
		writeToken(t, caseDir, "b.token", "tok-b")
		p := writeReplicas(t, caseDir, tc.doc, tc.mode)
		if _, _, err := loadReplicas(p); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: want error containing %q, got %v", tc.name, tc.want, err)
		}
	}

	// MED-2: the credential token file holds the actual secret — a world-readable (0644) token file
	// is refused even when replicas.json itself is correctly 0600.
	credDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(credDir, "a.token"), []byte("tok-a\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(credDir, "a.token"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(credDir, "b.token"), []byte("tok-b\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	credPath := writeReplicas(t, credDir, twoReplicaDoc, 0o600)
	if _, _, err := loadReplicas(credPath); err == nil || !strings.Contains(err.Error(), "world-readable") {
		t.Fatalf("world-readable token file must be refused: %v", err)
	}
}

func TestDevSelfsignedGeneratesUsablePair(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "certs")
	certPath, keyPath, err := generateSelfSigned(dir)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if _, err := tls.LoadX509KeyPair(certPath, keyPath); err != nil {
		t.Fatalf("generated pair must load as a TLS key pair: %v", err)
	}
	if info, err := os.Stat(keyPath); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("key must be 0600: %v %v", info, err)
	}
	var out bytes.Buffer
	if err := run(context.Background(), []string{"--dev-selfsigned", dir}, &out, &out); err != nil {
		t.Fatalf("run --dev-selfsigned: %v", err)
	}
	if !strings.Contains(out.String(), certPath) || !strings.Contains(out.String(), keyPath) {
		t.Fatalf("--dev-selfsigned must print the pair paths, got:\n%s", out.String())
	}
}

func TestRunFlagValidation(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), nil, &out, &out); err == nil || !strings.Contains(err.Error(), "--store and --replicas") {
		t.Fatalf("missing flags must fail: %v", err)
	}
	if err := run(context.Background(), []string{"--store", "x.db", "--replicas", "r.json", "--tls-cert", "c.pem"}, &out, &out); err == nil || !strings.Contains(err.Error(), "set together") {
		t.Fatalf("lone --tls-cert must fail: %v", err)
	}
}

// Full hub integration over native TLS: mnemon-hub serves push/pull/status with the dev self-signed
// pair; the SAME channel sync client used against the co-hosted hub talks to it via ca_file
// (dual-form proof); scopes differ per principal; audit lines land on stdout.
func TestMnemonHubServesSyncOverTLS(t *testing.T) {
	work := t.TempDir()
	certPath, keyPath, err := generateSelfSigned(filepath.Join(work, "certs"))
	if err != nil {
		t.Fatal(err)
	}
	repDir := filepath.Join(work, "rep")
	if err := os.MkdirAll(repDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeToken(t, repDir, "a.token", "tok-a")
	writeToken(t, repDir, "b.token", "tok-b")
	replicasPath := writeReplicas(t, repDir, twoReplicaDoc, 0o600)

	var out lockedBuffer
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{
			"--addr", "127.0.0.1:0",
			"--store", filepath.Join(work, "hub", "hub.db"),
			"--replicas", replicasPath,
			"--tls-cert", certPath,
			"--tls-key", keyPath,
		}, &out, &out)
	}()
	endpoint := waitForListen(t, &out)
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("mnemon-hub exited with error: %v", err)
		}
	}()

	clientA, err := access.NewSyncClient(endpoint, access.SyncClientConfig{Token: "tok-a", CAFile: certPath})
	if err != nil {
		t.Fatal(err)
	}
	clientB, err := access.NewSyncClient(endpoint, access.SyncClientConfig{Token: "tok-b", CAFile: certPath})
	if err != nil {
		t.Fatal(err)
	}

	mem := contract.ResourceRef{Kind: "memory", ID: "project"}
	fields := map[string]any{"content": "pushed through mnemon-hub"}
	material := contract.SyncedEventMaterial{
		OriginReplicaID: "local-a", LocalDecisionID: "dec-1", LocalIngestSeq: 1, Actor: "codex@a",
		ResourceRef: mem, ResourceVersion: 1, FieldsDigest: digestFor(fields), Fields: fields,
		DecidedAt: "2026-06-12T00:00:00Z", Status: "pending",
	}
	pushResp, err := clientA.SyncPush(contract.SyncPushRequest{ReplicaID: "local-a", BatchID: "b1", Events: hubTestSyncEvents(t, material)})
	if err != nil || len(pushResp.Accepted) != 1 {
		t.Fatalf("push over TLS: %+v err=%v", pushResp, err)
	}
	pullResp, err := clientB.SyncPull(contract.SyncPullRequest{ReplicaID: "local-b"})
	if err != nil || len(pullResp.Events) != 1 || contract.DecisionIDFromEventID(pullResp.Events[0].Event.ID) != "dec-1" {
		t.Fatalf("pull over TLS: %+v err=%v", pullResp, err)
	}
	status, err := clientA.SyncStatus()
	if err != nil || status.HubEventsReceived != 1 || status.HubEventsServed != 1 {
		t.Fatalf("status over TLS: %+v err=%v", status, err)
	}

	// B's grant is memory-only: pushing a skill event is rejected by the clamp (scope probe).
	skillFields := map[string]any{"name": "project"}
	skillMaterial := contract.SyncedEventMaterial{
		OriginReplicaID: "local-b", LocalDecisionID: "dec-skill", LocalIngestSeq: 2, Actor: "codex@b",
		ResourceRef: contract.ResourceRef{Kind: "skill", ID: "project"}, ResourceVersion: 1,
		FieldsDigest: digestFor(skillFields), Fields: skillFields, DecidedAt: "2026-06-12T00:00:00Z", Status: "pending",
	}
	scopeResp, err := clientB.SyncPush(contract.SyncPushRequest{ReplicaID: "local-b", BatchID: "b2", Events: hubTestSyncEvents(t, skillMaterial)})
	if err != nil || len(scopeResp.Rejected) != 1 {
		t.Fatalf("out-of-scope push must reject per-event: %+v err=%v", scopeResp, err)
	}

	// An unknown token is 401 (the wire security floor under TLS).
	badClient, err := access.NewSyncClient(endpoint, access.SyncClientConfig{Token: "wrong", CAFile: certPath})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := badClient.SyncStatus(); err == nil {
		t.Fatal("unknown token must be unauthorized")
	}

	for _, want := range []string{
		"principal=replica-a@team verb=sync.push result=ok",
		"principal=replica-b@team verb=sync.pull result=ok",
		"principal=replica-a@team verb=sync.status result=ok",
		"principal=- verb=sync.status result=unauthorized",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("audit line %q missing in:\n%s", want, out.String())
		}
	}
}

var listenLine = regexp.MustCompile(`mnemon-hub: listening on (https?://[^\s]+) `)

func waitForListen(t *testing.T, out *lockedBuffer) string {
	t.Helper()
	for i := 0; i < 100; i++ {
		if m := listenLine.FindStringSubmatch(out.String()); m != nil {
			return m[1]
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("mnemon-hub did not report a listen address:\n%s", out.String())
	return ""
}

// lockedBuffer keeps the run goroutine's writes race-free with the test's polling reads.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func digestFor(fields map[string]any) string {
	b, _ := json.Marshal(fields)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func hubTestSyncEvents(t *testing.T, materials ...contract.SyncedEventMaterial) []eventmodel.EventEnvelope {
	t.Helper()
	events := make([]eventmodel.EventEnvelope, 0, len(materials))
	for _, material := range materials {
		env, err := contract.SyncedEventEnvelopeFromMaterial(material)
		if err != nil {
			t.Fatalf("synced event fixture: %v", err)
		}
		events = append(events, env)
	}
	return events
}
