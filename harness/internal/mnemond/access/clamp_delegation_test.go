package access

import (
	"reflect"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// ChannelBinding.ClampRefs must DELEGATE to contract.ClampRefs — the ONE clamp implementation the
// standalone hub (mnemonhub) shares. Pin the delegation by equivalence over the full semantic
// matrix (results AND error texts), so a re-grown hand-rolled copy in channel cannot silently
// diverge from the hub's clamp again.
func TestChannelClampRefsDelegatesToContractClamp(t *testing.T) {
	mem := contract.ResourceRef{Kind: "memory", ID: "project"}
	skill := contract.ResourceRef{Kind: "skill", ID: "project"}
	note := contract.ResourceRef{Kind: "note", ID: "project"}

	cases := []struct {
		name      string
		scope     []contract.ResourceRef
		requested []contract.ResourceRef
	}{
		{"empty requested defaults to scope", []contract.ResourceRef{mem, skill}, nil},
		{"narrowing", []contract.ResourceRef{mem, skill}, []contract.ResourceRef{skill}},
		{"out of scope", []contract.ResourceRef{mem}, []contract.ResourceRef{note}},
		{"empty scope denies explicit", nil, []contract.ResourceRef{mem}},
		{"empty scope empty requested", nil, nil},
	}
	for _, tc := range cases {
		b := ReplicaAgentBinding("replica@peer", "http://127.0.0.1:1", tc.scope)
		gotRefs, gotErr := b.ClampRefs(tc.requested)
		wantRefs, wantErr := contract.ClampRefs(b.Principal, tc.scope, tc.requested)
		if !reflect.DeepEqual(gotRefs, wantRefs) {
			t.Fatalf("%s: refs diverged: binding=%v contract=%v", tc.name, gotRefs, wantRefs)
		}
		if (gotErr == nil) != (wantErr == nil) || (gotErr != nil && gotErr.Error() != wantErr.Error()) {
			t.Fatalf("%s: errors diverged: binding=%v contract=%v", tc.name, gotErr, wantErr)
		}
	}
}

// The sync verb strings are contract-owned ABI surface; the channel aliases must stay identical.
func TestSyncVerbAliasesMatchContract(t *testing.T) {
	if string(VerbSyncPush) != contract.SyncVerbPush ||
		string(VerbSyncPull) != contract.SyncVerbPull ||
		string(VerbSyncStatus) != contract.SyncVerbStatus {
		t.Fatalf("channel sync verbs diverged from contract: %s/%s/%s", VerbSyncPush, VerbSyncPull, VerbSyncStatus)
	}
}
