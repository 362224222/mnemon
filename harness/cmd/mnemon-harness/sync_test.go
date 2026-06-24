package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/app"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/policy"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

func TestSyncPushOnceAcksPendingLocalEvents(t *testing.T) {
	restoreSyncFlags(t)
	root := t.TempDir()
	storePath := filepath.Join(root, runtime.DefaultStorePath)
	ref := contract.ResourceRef{Kind: "memory", ID: "project"}

	localBinding := access.ChannelBinding{
		Principal:            "codex@project",
		ActorKind:            contract.KindHostAgent,
		Transport:            access.TransportHTTP,
		Endpoint:             "http://127.0.0.1:8787",
		AllowedVerbs:         []access.Verb{access.VerbObserve, access.VerbPull, access.VerbStatus},
		AllowedObservedTypes: []string{policy.MemoryWriteCandidateObserved},
		SubscriptionScope:    []contract.ResourceRef{ref},
		IdempotencyNamespace: "host:codex@project",
	}
	local, err := app.OpenLocalRuntime(storePath, access.LoadedBindings{Bindings: []access.ChannelBinding{localBinding}}, nil, nil)
	if err != nil {
		t.Fatalf("open local runtime: %v", err)
	}
	localSrv := httptest.NewServer(runtime.NewRuntimeHandler(local, access.HeaderAuthenticator{}))
	client := access.NewClient(localSrv.URL, "codex@project")
	if _, err := client.IngestObserve("codex@project", contract.ObservationEnvelope{
		ExternalID: "sync-push-memory",
		Event: contract.Event{Type: policy.MemoryWriteCandidateObserved, Payload: map[string]any{
			"content":    "sync push should ack this local memory",
			"source":     "test",
			"confidence": "high",
		}},
	}); err != nil {
		t.Fatalf("local observe: %v", err)
	}
	localSrv.Close()
	if err := local.Close(); err != nil {
		t.Fatalf("close local runtime: %v", err)
	}

	syncRoot = root
	syncStorePath = storePath
	syncRemoteID = "workspace"
	syncRemoteURL = "http://127.0.0.1:1"
	syncRemoteToken = "remote-token"
	var down bytes.Buffer
	cmd := mustTestCommand(t)
	cmd.SetOut(&down)
	if err := runSyncPush(cmd, nil); err == nil || !strings.Contains(err.Error(), "sync push failed") {
		t.Fatalf("remote-down push must report transport failure, got %v", err)
	}
	st, err := syncStatusForTest(storePath)
	if err != nil {
		t.Fatalf("status after remote down: %v", err)
	}
	if st.SyncPending != 1 || st.SyncSynced != 0 {
		t.Fatalf("remote-down push must leave local event pending, got %+v", st)
	}

	remoteBinding := access.ReplicaAgentBinding("replica@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	remote, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "remote.db"), runtime.RuntimeConfig{
		Bindings: []access.ChannelBinding{remoteBinding},
		Subs:     access.SubsFromBindings([]access.ChannelBinding{remoteBinding}),
	})
	if err != nil {
		t.Fatalf("open remote runtime: %v", err)
	}
	defer remote.Close()
	remoteSrv := httptest.NewServer(runtime.NewRuntimeHandler(remote, access.TokenAuthenticator{Tokens: map[string]contract.ActorID{"remote-token": "replica@project"}}))
	defer remoteSrv.Close()

	syncRemoteURL = remoteSrv.URL
	var out bytes.Buffer
	cmd = mustTestCommand(t)
	cmd.SetOut(&out)
	if err := runSyncPush(cmd, nil); err != nil {
		t.Fatalf("sync push once: %v", err)
	}
	if !strings.Contains(out.String(), "Sync push: 1 accepted, 0 rejected, 0 conflicts") {
		t.Fatalf("unexpected sync output: %s", out.String())
	}
	st, err = syncStatusForTest(storePath)
	if err != nil {
		t.Fatalf("status after push: %v", err)
	}
	if st.SyncPending != 0 || st.SyncSynced != 1 || st.SyncConflicts != 0 {
		t.Fatalf("successful push must mark the local event synced, got %+v", st)
	}
}

func TestSyncPullOnceImportsRemoteMemoryThroughLocalMnemon(t *testing.T) {
	restoreSyncFlags(t)
	root := t.TempDir()
	storePath := filepath.Join(root, runtime.DefaultStorePath)
	ref := contract.ResourceRef{Kind: "memory", ID: "project"}
	localReplica := access.ReplicaAgentBinding("replica@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	otherReplica := access.ReplicaAgentBinding("replica@other", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	remote, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "remote.db"), runtime.RuntimeConfig{
		Bindings: []access.ChannelBinding{localReplica, otherReplica},
		Subs:     access.SubsFromBindings([]access.ChannelBinding{localReplica, otherReplica}),
	})
	if err != nil {
		t.Fatalf("open remote runtime: %v", err)
	}
	defer remote.Close()
	remoteSrv := httptest.NewServer(runtime.NewRuntimeHandler(remote, access.TokenAuthenticator{Tokens: map[string]contract.ActorID{
		"local-token": "replica@project",
		"other-token": "replica@other",
	}}))
	defer remoteSrv.Close()

	fields := remoteMemoryFields("remote-entry-1", "Remote synced memory appears locally")
	remoteMaterial := contract.SyncedEventMaterial{
		OriginReplicaID: "other-replica",
		LocalDecisionID: "dec-remote-1",
		LocalIngestSeq:  7,
		Actor:           "codex@other",
		ResourceRef:     ref,
		ResourceVersion: 1,
		FieldsDigest:    syncTestDigest(fields),
		Fields:          fields,
		DecidedAt:       "2026-06-06T00:00:00Z",
		Status:          "pending",
	}
	if resp, err := access.NewClientWithToken(remoteSrv.URL, "other-token").SyncPush(contract.SyncPushRequest{
		ReplicaID: "other-replica",
		BatchID:   "remote-batch",
		Events:    syncTestEvents(t, remoteMaterial),
	}); err != nil || len(resp.Accepted) != 1 {
		t.Fatalf("seed remote event: resp=%+v err=%v", resp, err)
	}

	syncRoot = root
	syncStorePath = storePath
	syncRemoteID = "workspace"
	syncRemoteURL = remoteSrv.URL
	syncRemoteToken = "local-token"
	var out bytes.Buffer
	cmd := mustTestCommand(t)
	cmd.SetOut(&out)
	if err := runSyncPull(cmd, nil); err != nil {
		t.Fatalf("sync pull once: %v", err)
	}
	if !strings.Contains(out.String(), "Sync pull: 1 events") {
		t.Fatalf("unexpected pull output: %s", out.String())
	}
	content := localMemoryContentForTest(t, storePath, ref)
	if !strings.Contains(content, "Remote synced memory appears locally") {
		t.Fatalf("pulled memory not visible through local presentation view:\n%s", content)
	}
	st, err := syncStatusForTest(storePath)
	if err != nil {
		t.Fatalf("status after pull: %v", err)
	}
	if st.SyncPending != 0 {
		t.Fatalf("remote import must not create outbound pending echo, got %+v", st)
	}

	out.Reset()
	cmd = mustTestCommand(t)
	cmd.SetOut(&out)
	if err := runSyncPull(cmd, nil); err != nil {
		t.Fatalf("second sync pull: %v", err)
	}
	if !strings.Contains(out.String(), "Sync pull: 0 events") {
		t.Fatalf("second pull must be cursor-idempotent, got %s", out.String())
	}
	content = localMemoryContentForTest(t, storePath, ref)
	if strings.Count(content, "Remote synced memory appears locally") != 1 {
		t.Fatalf("duplicate pull must not duplicate memory:\n%s", content)
	}
}

func TestSyncPullOnceImportsRemoteSkillThroughLocalMnemon(t *testing.T) {
	restoreSyncFlags(t)
	root := t.TempDir()
	storePath := filepath.Join(root, runtime.DefaultStorePath)
	ref := contract.ResourceRef{Kind: "skill", ID: "project"}
	localReplica := access.ReplicaAgentBinding("replica@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	otherReplica := access.ReplicaAgentBinding("replica@other", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	remote, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "remote.db"), runtime.RuntimeConfig{
		Bindings: []access.ChannelBinding{localReplica, otherReplica},
		Subs:     access.SubsFromBindings([]access.ChannelBinding{localReplica, otherReplica}),
	})
	if err != nil {
		t.Fatalf("open remote runtime: %v", err)
	}
	defer remote.Close()
	remoteSrv := httptest.NewServer(runtime.NewRuntimeHandler(remote, access.TokenAuthenticator{Tokens: map[string]contract.ActorID{
		"local-token": "replica@project",
		"other-token": "replica@other",
	}}))
	defer remoteSrv.Close()

	fields := remoteSkillFields("release-checklist", "active")
	remoteMaterial := contract.SyncedEventMaterial{
		OriginReplicaID: "other-replica",
		LocalDecisionID: "dec-remote-skill-1",
		LocalIngestSeq:  17,
		Actor:           "codex@other",
		ResourceRef:     ref,
		ResourceVersion: 1,
		FieldsDigest:    syncTestDigest(fields),
		Fields:          fields,
		DecidedAt:       "2026-06-06T00:00:00Z",
		Status:          "pending",
	}
	if resp, err := access.NewClientWithToken(remoteSrv.URL, "other-token").SyncPush(contract.SyncPushRequest{
		ReplicaID: "other-replica",
		BatchID:   "remote-skill-batch",
		Events:    syncTestEvents(t, remoteMaterial),
	}); err != nil || len(resp.Accepted) != 1 {
		t.Fatalf("seed remote skill event: resp=%+v err=%v", resp, err)
	}

	syncRoot = root
	syncStorePath = storePath
	syncRemoteID = "workspace"
	syncRemoteURL = remoteSrv.URL
	syncRemoteToken = "local-token"
	var out bytes.Buffer
	cmd := mustTestCommand(t)
	cmd.SetOut(&out)
	if err := runSyncPull(cmd, nil); err != nil {
		t.Fatalf("sync pull skill once: %v", err)
	}
	if !strings.Contains(out.String(), "Sync pull: 1 events") {
		t.Fatalf("unexpected pull output: %s", out.String())
	}
	decls := localSkillDeclarationsForTest(t, storePath, ref)
	if len(decls) != 1 || decls[0]["skill_id"] != "release-checklist" || decls[0]["status"] != "active" {
		t.Fatalf("pulled skill declaration not visible through local presentation view: %+v", decls)
	}
	st, err := syncStatusForTest(storePath)
	if err != nil {
		t.Fatalf("status after skill pull: %v", err)
	}
	if st.SyncPending != 0 {
		t.Fatalf("remote skill import must not create outbound pending echo, got %+v", st)
	}

	out.Reset()
	cmd = mustTestCommand(t)
	cmd.SetOut(&out)
	if err := runSyncPull(cmd, nil); err != nil {
		t.Fatalf("second sync pull skill: %v", err)
	}
	if !strings.Contains(out.String(), "Sync pull: 0 events") {
		t.Fatalf("second pull must be cursor-idempotent, got %s", out.String())
	}
	decls = localSkillDeclarationsForTest(t, storePath, ref)
	if len(decls) != 1 {
		t.Fatalf("duplicate skill pull must not duplicate declarations: %+v", decls)
	}
}

func TestSyncConnectWritesRemoteConfigWithoutLeakingToken(t *testing.T) {
	restoreSyncFlags(t)
	root := t.TempDir()
	syncRoot = root
	syncRemoteURL = "https://remote.example.test"
	syncRemoteToken = "secret-workspace-token"
	var out bytes.Buffer
	cmd := mustTestCommand(t)
	cmd.SetOut(&out)
	if err := runSyncConnect(cmd, []string{"team"}); err != nil {
		t.Fatalf("sync connect: %v", err)
	}
	if strings.Contains(out.String(), "secret-workspace-token") {
		t.Fatalf("sync connect output must not expose token:\n%s", out.String())
	}
	for _, want := range []string{"Remote Workspace: connected team", "Sync: ready"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("sync connect output missing %q:\n%s", want, out.String())
		}
	}
	config := string(mustReadCmd(t, filepath.Join(root, ".mnemon", "harness", "sync", "remotes.json")))
	for _, want := range []string{`"current": "team"`, `"id": "team"`, `"credential_ref": ".mnemon/harness/sync/credentials/team.token"`} {
		if !strings.Contains(config, want) {
			t.Fatalf("sync connect config missing %q:\n%s", want, config)
		}
	}
	if token := strings.TrimSpace(string(mustReadCmd(t, filepath.Join(root, ".mnemon", "harness", "sync", "credentials", "team.token")))); token != "secret-workspace-token" {
		t.Fatalf("sync connect token file not written correctly: %q", token)
	}
	syncRemoteID = "default"
	syncRemoteURL = ""
	syncRemoteToken = ""
	remote, err := resolveSyncRemote()
	if err != nil {
		t.Fatalf("resolve current remote: %v", err)
	}
	if remote.ID != "team" || remote.Endpoint != "https://remote.example.test" || remote.Token != "secret-workspace-token" {
		t.Fatalf("current remote not resolved: %+v", remote)
	}
}

func TestSyncRemoteConfigLoadsCredentialRef(t *testing.T) {
	restoreSyncFlags(t)
	root := t.TempDir()
	credRel := filepath.Join(".mnemon", "harness", "sync", "credentials", "workspace.token")
	credPath := filepath.Join(root, credRel)
	if err := os.MkdirAll(filepath.Dir(credPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(credPath, []byte("tok-workspace\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	remotesPath := filepath.Join(root, ".mnemon", "harness", "sync", "remotes.json")
	if err := os.MkdirAll(filepath.Dir(remotesPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(remotesPath, []byte(`{
	  "schema_version": 1,
	  "remotes": [{
	    "id": "workspace",
	    "endpoint": "http://127.0.0.1:8787",
	    "credential_ref": ".mnemon/harness/sync/credentials/workspace.token"
	  }]
	}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	syncRoot = root
	syncRemoteID = "workspace"
	remote, err := resolveSyncRemote()
	if err != nil {
		t.Fatalf("resolve remote config: %v", err)
	}
	if remote.ID != "workspace" || remote.Endpoint != "http://127.0.0.1:8787" || remote.Token != "tok-workspace" {
		t.Fatalf("remote config not loaded: %+v", remote)
	}
}

func restoreSyncFlags(t *testing.T) {
	t.Helper()
	oldRoot := syncRoot
	oldStorePath := syncStorePath
	oldRemotesPath := syncRemotesPath
	oldRemoteID := syncRemoteID
	oldRemoteURL := syncRemoteURL
	oldRemoteToken := syncRemoteToken
	oldRemoteTokenFile := syncRemoteTokenFile
	oldCAFile := syncCAFile
	oldAllowInsecure := syncAllowInsecure
	t.Cleanup(func() {
		syncRoot = oldRoot
		syncStorePath = oldStorePath
		syncRemotesPath = oldRemotesPath
		syncRemoteID = oldRemoteID
		syncRemoteURL = oldRemoteURL
		syncRemoteToken = oldRemoteToken
		syncRemoteTokenFile = oldRemoteTokenFile
		syncCAFile = oldCAFile
		syncAllowInsecure = oldAllowInsecure
	})
	syncRoot = "."
	syncStorePath = ""
	syncRemotesPath = ""
	syncRemoteID = "default"
	syncRemoteURL = ""
	syncRemoteToken = ""
	syncRemoteTokenFile = ""
	syncCAFile = ""
	syncAllowInsecure = false
}

func syncStatusForTest(storePath string) (contract.ChannelStatus, error) {
	rt, err := runtime.OpenRuntime(storePath, runtime.RuntimeConfig{})
	if err != nil {
		return contract.ChannelStatus{}, err
	}
	defer rt.Close()
	return rt.Status("status@test")
}

func localMemoryContentForTest(t *testing.T, storePath string, ref contract.ResourceRef) string {
	t.Helper()
	binding := access.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	rt, err := app.OpenLocalRuntime(storePath, access.LoadedBindings{Bindings: []access.ChannelBinding{binding}}, nil, nil)
	if err != nil {
		t.Fatalf("open local runtime for projection: %v", err)
	}
	defer rt.Close()
	proj, err := rt.API().PullPresentationView("codex@project", contract.Subscription{Actor: "codex@project"})
	if err != nil {
		t.Fatalf("pull local presentation view: %v", err)
	}
	for _, item := range proj.Content {
		if item.Ref == ref {
			if content, ok := item.Fields["content"].(string); ok {
				return content
			}
		}
	}
	return ""
}

func localSkillDeclarationsForTest(t *testing.T, storePath string, ref contract.ResourceRef) []map[string]any {
	t.Helper()
	binding := access.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	rt, err := app.OpenLocalRuntime(storePath, access.LoadedBindings{Bindings: []access.ChannelBinding{binding}}, nil, nil)
	if err != nil {
		t.Fatalf("open local runtime for skill projection: %v", err)
	}
	defer rt.Close()
	proj, err := rt.API().PullPresentationView("codex@project", contract.Subscription{Actor: "codex@project"})
	if err != nil {
		t.Fatalf("pull local skill projection: %v", err)
	}
	for _, item := range proj.Content {
		if item.Ref == ref {
			raw, _ := item.Fields["declarations"].([]any)
			out := make([]map[string]any, 0, len(raw))
			for _, decl := range raw {
				if m, ok := decl.(map[string]any); ok {
					out = append(out, m)
				}
			}
			return out
		}
	}
	return nil
}

func remoteMemoryFields(entryID, content string) map[string]any {
	entries := []any{map[string]any{
		"id":         entryID,
		"content":    content,
		"source":     "remote",
		"confidence": "high",
		"actor":      "codex@other",
		"ingest_seq": float64(7),
	}}
	return map[string]any{
		"content": "# Local Memory\n- " + content,
		"entries": entries,
	}
}

func remoteSkillFields(skillID, status string) map[string]any {
	return map[string]any{
		"name": "project",
		"declarations": []any{map[string]any{
			"id":         "remote/" + skillID + "/" + status,
			"skill_id":   skillID,
			"name":       skillID,
			"status":     status,
			"content":    "Remote declaration for " + skillID,
			"source":     "remote",
			"confidence": "high",
			"actor":      "codex@other",
			"ingest_seq": float64(17),
		}},
		"updated_by": "codex@other",
	}
}

func syncTestDigest(fields map[string]any) string {
	data, _ := json.Marshal(fields)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func syncTestEvents(t *testing.T, materials ...contract.SyncedEventMaterial) []eventmodel.EventEnvelope {
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
