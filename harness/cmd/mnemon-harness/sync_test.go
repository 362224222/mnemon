package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/app"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemonhub/exchange"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

func TestSyncPushOnceAcksPendingLocalEvents(t *testing.T) {
	restoreSyncFlags(t)
	root := t.TempDir()
	storePath := filepath.Join(root, runtime.DefaultStorePath)
	ref := contract.ResourceRef{Kind: "progress_digest", ID: "project"}

	localBinding := access.ChannelBinding{
		Principal:            "codex@project",
		ActorKind:            contract.KindHostAgent,
		Transport:            access.TransportHTTP,
		Endpoint:             "http://127.0.0.1:8787",
		AllowedVerbs:         []access.Verb{access.VerbObserve, access.VerbPull, access.VerbStatus},
		AllowedObservedTypes: []string{"progress_digest.write_candidate.observed"},
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
		ExternalID: "sync-push-progress",
		Event: contract.Event{Type: "progress_digest.write_candidate.observed", Payload: map[string]any{
			"summary": "sync push should ack this local event",
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

func TestSyncPullOnceImportsRemoteProgressThroughLocalMnemon(t *testing.T) {
	restoreSyncFlags(t)
	root := t.TempDir()
	storePath := filepath.Join(root, runtime.DefaultStorePath)
	ref := contract.ResourceRef{Kind: "progress_digest", ID: "project"}
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

	fields := remoteProgressFields("remote-entry-1", "Remote synced progress appears locally")
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
	content := localResourceContentForTest(t, storePath, ref)
	if !strings.Contains(content, "Remote synced progress appears locally") {
		t.Fatalf("pulled progress not visible through local presentation view:\n%s", content)
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
	content = localResourceContentForTest(t, storePath, ref)
	if strings.Count(content, "Remote synced progress appears locally") != 1 {
		t.Fatalf("duplicate pull must not duplicate progress:\n%s", content)
	}
}

func TestSyncPullOnceImportsRemoteAssignmentThroughLocalMnemon(t *testing.T) {
	restoreSyncFlags(t)
	root := t.TempDir()
	storePath := filepath.Join(root, runtime.DefaultStorePath)
	ref := contract.ResourceRef{Kind: "assignment", ID: "project"}
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

	fields := remoteAssignmentFields("release-checklist", "2h")
	remoteMaterial := contract.SyncedEventMaterial{
		OriginReplicaID: "other-replica",
		LocalDecisionID: "dec-remote-assignment-1",
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
		BatchID:   "remote-assignment-batch",
		Events:    syncTestEvents(t, remoteMaterial),
	}); err != nil || len(resp.Accepted) != 1 {
		t.Fatalf("seed remote assignment event: resp=%+v err=%v", resp, err)
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
		t.Fatalf("sync pull assignment once: %v", err)
	}
	if !strings.Contains(out.String(), "Sync pull: 1 events") {
		t.Fatalf("unexpected pull output: %s", out.String())
	}
	items := localResourceItemsForTest(t, storePath, ref)
	if len(items) != 1 || items[0]["scope"] != "release-checklist" || items[0]["ttl"] != "2h" {
		t.Fatalf("pulled assignment item not visible through local presentation view: %+v", items)
	}
	st, err := syncStatusForTest(storePath)
	if err != nil {
		t.Fatalf("status after assignment pull: %v", err)
	}
	if st.SyncPending != 0 {
		t.Fatalf("remote assignment import must not create outbound pending echo, got %+v", st)
	}

	out.Reset()
	cmd = mustTestCommand(t)
	cmd.SetOut(&out)
	if err := runSyncPull(cmd, nil); err != nil {
		t.Fatalf("second sync pull assignment: %v", err)
	}
	if !strings.Contains(out.String(), "Sync pull: 0 events") {
		t.Fatalf("second pull must be cursor-idempotent, got %s", out.String())
	}
	items = localResourceItemsForTest(t, storePath, ref)
	if len(items) != 1 {
		t.Fatalf("duplicate assignment pull must not duplicate items: %+v", items)
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
	for _, want := range []string{`"current": "team"`, `"backend": "http"`, `"id": "team"`, `"credential_ref": ".mnemon/harness/sync/credentials/team.token"`} {
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
	if remote.ID != "team" || remote.Backend != exchange.RemoteBackendHTTP || remote.Endpoint != "https://remote.example.test" || remote.Token != "secret-workspace-token" {
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
	if remote.ID != "workspace" || remote.Backend != exchange.RemoteBackendHTTP || remote.Endpoint != "http://127.0.0.1:8787" || remote.Token != "tok-workspace" {
		t.Fatalf("remote config not loaded: %+v", remote)
	}
}

func TestSyncRemotePlanLoadsDirectionalCredentials(t *testing.T) {
	restoreSyncFlags(t)
	root := t.TempDir()
	writeCredential := func(id, token string) string {
		t.Helper()
		credRel := filepath.Join(".mnemon", "harness", "sync", "credentials", id+".token")
		credPath := filepath.Join(root, credRel)
		if err := os.MkdirAll(filepath.Dir(credPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(credPath, []byte(token+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return filepath.ToSlash(credRel)
	}
	pubCred := writeCredential("pub", "tok-pub")
	subCred := writeCredential("sub", "tok-sub")
	remotesPath := filepath.Join(root, ".mnemon", "harness", "sync", "remotes.json")
	if err := os.WriteFile(remotesPath, []byte(fmt.Sprintf(`{
	  "schema_version": 1,
	  "remotes": [{
	    "id": "pub",
	    "direction": "publish",
	    "endpoint": "http://127.0.0.1:8787",
	    "credential_ref": %q
	  }, {
	    "id": "sub",
	    "direction": "subscribe",
	    "endpoint": "http://127.0.0.1:8788",
	    "credential_ref": %q
	  }]
	}`+"\n", pubCred, subCred)), 0o644); err != nil {
		t.Fatal(err)
	}
	syncRoot = root

	plan, err := resolveSyncRemotePlan()
	if err != nil {
		t.Fatalf("resolve directional remote plan: %v", err)
	}
	if len(plan.PushTargets) != 1 || plan.PushTargets[0].ID != "pub" || plan.PushTargets[0].Token != "tok-pub" {
		t.Fatalf("push target not resolved with its credential: %+v", plan.PushTargets)
	}
	if len(plan.PullSources) != 1 || plan.PullSources[0].ID != "sub" || plan.PullSources[0].Token != "tok-sub" {
		t.Fatalf("pull source not resolved with its credential: %+v", plan.PullSources)
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

func localResourceContentForTest(t *testing.T, storePath string, ref contract.ResourceRef) string {
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

func localResourceItemsForTest(t *testing.T, storePath string, ref contract.ResourceRef) []map[string]any {
	t.Helper()
	binding := access.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	rt, err := app.OpenLocalRuntime(storePath, access.LoadedBindings{Bindings: []access.ChannelBinding{binding}}, nil, nil)
	if err != nil {
		t.Fatalf("open local runtime for item projection: %v", err)
	}
	defer rt.Close()
	proj, err := rt.API().PullPresentationView("codex@project", contract.Subscription{Actor: "codex@project"})
	if err != nil {
		t.Fatalf("pull local item projection: %v", err)
	}
	for _, item := range proj.Content {
		if item.Ref == ref {
			raw, _ := item.Fields["items"].([]any)
			out := make([]map[string]any, 0, len(raw))
			for _, entry := range raw {
				if m, ok := entry.(map[string]any); ok {
					out = append(out, m)
				}
			}
			return out
		}
	}
	return nil
}

func remoteProgressFields(entryID, summary string) map[string]any {
	items := []any{map[string]any{
		"id":         entryID,
		"summary":    summary,
		"actor":      "codex@other",
		"ingest_seq": float64(7),
	}}
	return map[string]any{
		"content": "# Progress\n- " + summary,
		"items":   items,
	}
}

func remoteAssignmentFields(scope, ttl string) map[string]any {
	return map[string]any{
		"content": "# Assignments\n- " + scope,
		"items": []any{map[string]any{
			"id":                "remote/" + scope + "/" + ttl,
			"scope":             scope,
			"ttl":               ttl,
			"assignee":          "codex@impl",
			"expected_work":     "complete " + scope,
			"expected_feedback": "summary",
			"evidence":          "remote import fixture",
			"actor":             "codex@other",
			"ingest_seq":        float64(17),
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
