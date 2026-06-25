package access

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

func TestLoadBindingFile(t *testing.T) {
	root := t.TempDir()
	channelDir := filepath.Join(root, ".mnemon", "harness", "channel")
	if err := os.MkdirAll(filepath.Join(channelDir, "tokens"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(channelDir, "tokens", "codex.token"), []byte("tok-codex\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(channelDir, "tokens", "replica.token"), []byte("tok-replica\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	bindingsJSON := `{
	  "schema_version": 1,
	  "bindings": [{
	    "principal": "codex@project",
	    "actor_kind": "host-agent",
	    "transport": "http",
	    "endpoint": "http://127.0.0.1:8787",
	    "allowed_verbs": ["observe","pull","render","status"],
	    "allowed_observed_types": ["session.observed","progress_digest.write_candidate.observed"],
	    "subscription_scope": [{"kind":"progress_digest","id":"project"}],
	    "idempotency_namespace": "host:codex@project",
	    "credential_ref": ".mnemon/harness/channel/tokens/codex.token"
	  },{
	    "principal": "replica@project",
	    "actor_kind": "replica-agent",
	    "transport": "http",
	    "endpoint": "http://127.0.0.1:8787",
	    "allowed_verbs": ["sync.push","sync.pull","sync.status"],
	    "subscription_scope": [{"kind":"progress_digest","id":"project"}],
	    "idempotency_namespace": "replica:replica@project",
	    "credential_ref": ".mnemon/harness/channel/tokens/replica.token"
	  }]
	}`
	bindingPath := filepath.Join(channelDir, "bindings.json")
	if err := os.WriteFile(bindingPath, []byte(bindingsJSON), 0o644); err != nil {
		t.Fatal(err)
	}

	loaded, err := LoadBindingFile(root, bindingPath)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded.Bindings) != 2 {
		t.Fatalf("want 2 bindings; got %d", len(loaded.Bindings))
	}
	b := loaded.Bindings[0]
	if b.Principal != "codex@project" || b.ActorKind != contract.KindHostAgent || b.Transport != TransportHTTP {
		t.Fatalf("mapped binding wrong: %+v", b)
	}
	if !b.Allows(VerbObserve) || !b.Allows(VerbPull) || !b.Allows(VerbRender) || !b.Allows(VerbStatus) {
		t.Fatalf("verbs not mapped: %+v", b.AllowedVerbs)
	}
	if !b.AllowsObservedType("session.observed") || b.AllowsObservedType("record.observed") {
		t.Fatalf("observed types not mapped: %+v", b.AllowedObservedTypes)
	}
	if len(b.SubscriptionScope) != 1 || b.SubscriptionScope[0] != (contract.ResourceRef{Kind: "progress_digest", ID: "project"}) {
		t.Fatalf("scope wrong: %+v", b.SubscriptionScope)
	}
	if loaded.Tokens["tok-codex"] != "codex@project" {
		t.Fatalf("token map wrong: %+v", loaded.Tokens)
	}
	replica := loaded.Bindings[1]
	if replica.Principal != "replica@project" || replica.ActorKind != contract.KindReplicaAgent {
		t.Fatalf("replica binding wrong: %+v", replica)
	}
	if !replica.Allows(VerbSyncPush) || !replica.Allows(VerbSyncPull) || !replica.Allows(VerbSyncStatus) || replica.Allows(VerbObserve) {
		t.Fatalf("replica verbs not mapped as sync-only: %+v", replica.AllowedVerbs)
	}
	if loaded.Tokens["tok-replica"] != "replica@project" {
		t.Fatalf("replica token map wrong: %+v", loaded.Tokens)
	}
}

// P4c: the binding declares the endpoint's context-budget tier (P4 "接入点声明订阅"). A declared tier
// round-trips into ChannelBinding.Budget and SubsFromBindings; an unknown tier is rejected at load
// (the closed-set guard at the binding boundary); an omitted tier stays empty (= hot, full delivery).
func TestBindingFileBudgetTier(t *testing.T) {
	root := t.TempDir()
	channelDir := filepath.Join(root, ".mnemon", "harness", "channel")
	if err := os.MkdirAll(filepath.Join(channelDir, "tokens"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(channelDir, "tokens", "codex.token"), []byte("tok\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	write := func(budget string) (LoadedBindings, error) {
		j := `{"schema_version":1,"bindings":[{
		  "principal":"codex@project","actor_kind":"host-agent","transport":"http",
		  "endpoint":"http://127.0.0.1:8787","allowed_verbs":["observe","pull","status"],
		  "subscription_scope":[{"kind":"memory","id":"project"}],
		  "idempotency_namespace":"host:codex@project",
		  "credential_ref":".mnemon/harness/channel/tokens/codex.token"` + budget + `}]}`
		p := filepath.Join(channelDir, "bindings.json")
		if err := os.WriteFile(p, []byte(j), 0o644); err != nil {
			t.Fatal(err)
		}
		return LoadBindingFile(root, p)
	}

	loaded, err := write(`,"budget":"digest-only"`)
	if err != nil {
		t.Fatalf("load with digest-only budget: %v", err)
	}
	if loaded.Bindings[0].Budget != contract.BudgetDigestOnly {
		t.Fatalf("budget not mapped: %q", loaded.Bindings[0].Budget)
	}
	if sub := SubsFromBindings(loaded.Bindings)["codex@project"]; sub.Budget != contract.BudgetDigestOnly {
		t.Fatalf("SubsFromBindings must carry the budget tier, got %q", sub.Budget)
	}

	// omitted budget => empty (resolves to hot/full downstream — no silent downgrade)
	if loaded, err := write(``); err != nil || loaded.Bindings[0].Budget != "" {
		t.Fatalf("omitted budget must stay empty, got %q err=%v", loaded.Bindings[0].Budget, err)
	}

	// an unknown tier is rejected at the binding boundary (fail-loud closed set)
	if _, err := write(`,"budget":"cold"`); err == nil {
		t.Fatal("an unknown budget tier must be rejected at load")
	}
	// the loaded set must validate as a BindingSet (principal + namespace uniqueness).
	if _, err := NewBindingSet(loaded.Bindings...); err != nil {
		t.Fatalf("loaded bindings must validate: %v", err)
	}
}

func TestLoadBindingFileRejectsMalformed(t *testing.T) {
	root := t.TempDir()
	bad := []string{
		`{"schema_version":2,"bindings":[]}`, // unsupported schema version
		`{"schema_version":1,"bindings":[{"principal":"p","actor_kind":"root","transport":"http","endpoint":"x","allowed_verbs":["observe"]}]}`,         // unknown actor kind
		`{"schema_version":1,"bindings":[{"principal":"p","actor_kind":"host-agent","transport":"http","endpoint":"x","allowed_verbs":["frob"]}]}`,      // unknown verb
		`{"schema_version":1,"bindings":[{"principal":"p","actor_kind":"host-agent","transport":"pigeon","endpoint":"x","allowed_verbs":["observe"]}]}`, // unknown transport
		`{"schema_version":1,"bindings":[{"principal":"p","actor_kind":"host-agent","transport":"http","endpoint":"","allowed_verbs":["observe"]}]}`,    // http with no endpoint
		`{"schema_version":1,"bindings":[{"principal":"","actor_kind":"host-agent","transport":"http","endpoint":"x","allowed_verbs":["observe"]}]}`,    // no principal
		`{"schema_version":1,"bindings":[` +
			`{"principal":"a","actor_kind":"host-agent","transport":"http","endpoint":"x","allowed_verbs":["observe"],"idempotency_namespace":"ns"},` +
			`{"principal":"b","actor_kind":"host-agent","transport":"http","endpoint":"x","allowed_verbs":["observe"],"idempotency_namespace":"ns"}]}`, // duplicate namespace
	}
	for i, js := range bad {
		p := filepath.Join(root, "bad-"+strconv.Itoa(i)+".json")
		if err := os.WriteFile(p, []byte(js), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadBindingFile(root, p); err == nil {
			t.Fatalf("malformed binding file %d must be rejected", i)
		}
	}
}
