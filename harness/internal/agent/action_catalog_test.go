package agent

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/teamwork"
)

type actionPolicyProviderStub struct {
	revisions    []string
	revisionCall int
	paths        []string
	raw          map[string][]byte
	readErrors   map[string]error
	reads        map[string]int
}

func (provider *actionPolicyProviderStub) Revision() string {
	index := provider.revisionCall
	provider.revisionCall++
	if len(provider.revisions) == 0 {
		return ""
	}
	if index >= len(provider.revisions) {
		index = len(provider.revisions) - 1
	}
	return provider.revisions[index]
}

func (provider *actionPolicyProviderStub) TeamworkActionPaths() []string {
	return provider.paths
}

func (provider *actionPolicyProviderStub) ReadTeamworkAction(path string) ([]byte, error) {
	provider.reads[path]++
	if err := provider.readErrors[path]; err != nil {
		return nil, err
	}
	return provider.raw[path], nil
}

func TestActionPolicyLoadsOneImmutableRealBundleSnapshot(t *testing.T) {
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	provider := newActionPolicyProviderStub(t, bundle)
	policy, err := NewActionPolicy(provider)
	if err != nil {
		t.Fatal(err)
	}
	revision, _ := model.ParseDigest(bundle.Manifest().AssetRevision)
	wantPaths := append([]string(nil), provider.paths...)
	if policy.AssetRevision() != revision || provider.revisionCall != 2 {
		t.Fatalf("Action policy revision = (%s, calls %d), want (%s, 2)",
			policy.AssetRevision(), provider.revisionCall, revision)
	}
	actions := policy.Actions()
	if len(actions) != len(wantPaths) {
		t.Fatalf("Actions() count = %d, want %d", len(actions), len(wantPaths))
	}
	for _, path := range wantPaths {
		if provider.reads[path] != 1 {
			t.Fatalf("ReadTeamworkAction(%q) calls = %d, want one", path, provider.reads[path])
		}
	}
	for index, action := range actions {
		if action.Ordinal() != uint8(index) {
			t.Fatalf("Action %q ordinal = %d, want semantic position %d", action.Name(), action.Ordinal(), index)
		}
		fromName, nameOK := policy.Action(action.Name())
		fromOperation, operationOK := policy.Operation(action.OperationKind())
		if !nameOK || !operationOK || fromName.SourcePath() != action.SourcePath() ||
			fromOperation.SourcePath() != action.SourcePath() {
			t.Fatalf("Action %q lookup parity failed", action.Name())
		}
	}

	firstPath := actions[0].SourcePath()
	wantRaw := actions[0].SourceBytes()
	provider.paths[0] = "actions/teamwork/tampered.json"
	provider.raw[firstPath][0] ^= 0xff
	actions[0] = teamwork.ActionDescriptor{}
	returnedRaw := policy.Actions()[0].SourceBytes()
	returnedRaw[0] ^= 0xff
	if policy.Actions()[0].SourcePath() != firstPath ||
		!bytes.Equal(policy.Actions()[0].SourceBytes(), wantRaw) {
		t.Fatal("Action policy retained caller-owned mutable state")
	}

	var zero ActionPolicy
	if !zero.AssetRevision().IsZero() || zero.Actions() != nil {
		t.Fatalf("zero ActionPolicy = %#v", zero)
	}
	if _, ok := zero.Action("anything"); ok {
		t.Fatal("zero ActionPolicy resolved an Action")
	}
	if _, ok := zero.Operation(model.OperationKind("teamwork.anything")); ok {
		t.Fatal("zero ActionPolicy resolved an operation")
	}
}

func TestActionPolicyAcceptsBundleThroughConsumerOwnedInterface(t *testing.T) {
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	policy, err := NewActionPolicy(bundle)
	if err != nil {
		t.Fatal(err)
	}
	manifest := bundle.Manifest()
	if policy.AssetRevision().String() != manifest.AssetRevision ||
		len(policy.Actions()) != teamwork.TeamworkActionCount {
		t.Fatalf("real bundle policy = (%s, %d Actions)", policy.AssetRevision(), len(policy.Actions()))
	}
	for _, action := range policy.Actions() {
		raw, readErr := bundle.Read(action.SourcePath())
		if readErr != nil || !bytes.Equal(raw, action.SourceBytes()) {
			t.Fatalf("Action %s differs from real bundle: %v", action.Name(), readErr)
		}
	}
}

func TestActionPolicyRejectsInvalidProviderSnapshots(t *testing.T) {
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	stable := newActionPolicyProviderStub(t, bundle)
	otherRevision := model.Sum([]byte("other managed manifest")).String()
	zeroRevision := "sha256:" + strings.Repeat("0", 64)
	readFailure := errors.New("read Action")
	tests := []struct {
		name   string
		mutate func(*actionPolicyProviderStub)
	}{
		{name: "invalid revision", mutate: func(provider *actionPolicyProviderStub) {
			provider.revisions = []string{"asset-r5"}
		}},
		{name: "zero revision", mutate: func(provider *actionPolicyProviderStub) {
			provider.revisions = []string{zeroRevision}
		}},
		{name: "missing path", mutate: func(provider *actionPolicyProviderStub) {
			provider.paths = provider.paths[:len(provider.paths)-1]
		}},
		{name: "extra path", mutate: func(provider *actionPolicyProviderStub) {
			provider.paths = append(provider.paths, "actions/teamwork/z-extra.json")
		}},
		{name: "duplicate path", mutate: func(provider *actionPolicyProviderStub) {
			provider.paths[1] = provider.paths[0]
		}},
		{name: "out of order", mutate: func(provider *actionPolicyProviderStub) {
			provider.paths[0], provider.paths[1] = provider.paths[1], provider.paths[0]
		}},
		{name: "parent path", mutate: func(provider *actionPolicyProviderStub) {
			provider.paths[0] = "actions/teamwork/../action.json"
		}},
		{name: "nested path", mutate: func(provider *actionPolicyProviderStub) {
			provider.paths[0] = "actions/teamwork/nested/action.json"
		}},
		{name: "wrong extension", mutate: func(provider *actionPolicyProviderStub) {
			provider.paths[0] = strings.TrimSuffix(provider.paths[0], ".json") + ".yaml"
		}},
		{name: "overlong path segment", mutate: func(provider *actionPolicyProviderStub) {
			provider.paths[0] = teamworkActionAssetPrefix + strings.Repeat("x", model.MaxIdentifierBytes+1) + ".json"
		}},
		{name: "unsupported closed set", mutate: func(provider *actionPolicyProviderStub) {
			oldPath := provider.paths[0]
			oldName := strings.TrimSuffix(strings.TrimPrefix(oldPath, teamworkActionAssetPrefix), ".json")
			newName := "aaa-unsupported"
			newPath := teamworkActionAssetPrefix + newName + ".json"
			provider.paths[0] = newPath
			provider.raw[newPath] = bytes.ReplaceAll(provider.raw[oldPath], []byte(oldName), []byte(newName))
			delete(provider.raw, oldPath)
		}},
		{name: "read error", mutate: func(provider *actionPolicyProviderStub) {
			provider.readErrors[provider.paths[3]] = readFailure
		}},
		{name: "invalid Action source", mutate: func(provider *actionPolicyProviderStub) {
			provider.raw[provider.paths[3]] = []byte("{}\n")
		}},
		{name: "revision drift", mutate: func(provider *actionPolicyProviderStub) {
			provider.revisions = []string{provider.revisions[0], otherRevision}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider := cloneActionPolicyProvider(stable)
			test.mutate(provider)
			policy, policyErr := NewActionPolicy(provider)
			if policyErr == nil || !policy.AssetRevision().IsZero() || policy.Actions() != nil {
				t.Fatalf("NewActionPolicy() = (%#v, %v), want inert error", policy, policyErr)
			}
		})
	}
	if policy, err := NewActionPolicy(nil); err == nil || !policy.AssetRevision().IsZero() {
		t.Fatalf("NewActionPolicy(nil) = (%#v, %v)", policy, err)
	}
}

func TestActionPolicyReadsEachCanonicalPathAtMostOnce(t *testing.T) {
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	provider := newActionPolicyProviderStub(t, bundle)
	failingPath := provider.paths[4]
	readFailure := errors.New("read Action")
	provider.readErrors[failingPath] = readFailure
	if _, err := NewActionPolicy(provider); !errors.Is(err, readFailure) {
		t.Fatalf("NewActionPolicy() error = %v", err)
	}
	for _, path := range provider.paths {
		want := 0
		if path <= failingPath {
			want = 1
		}
		if provider.reads[path] != want {
			t.Fatalf("ReadTeamworkAction(%q) calls = %d, want %d", path, provider.reads[path], want)
		}
	}
}

func newActionPolicyProviderStub(t testing.TB, bundle assets.Bundle) *actionPolicyProviderStub {
	t.Helper()
	manifest := bundle.Manifest()
	provider := &actionPolicyProviderStub{revisions: []string{manifest.AssetRevision},
		raw: make(map[string][]byte), readErrors: make(map[string]error), reads: make(map[string]int)}
	for _, record := range manifest.Files {
		if !strings.HasPrefix(record.Path, teamworkActionAssetPrefix) {
			continue
		}
		raw, err := bundle.Read(record.Path)
		if err != nil {
			t.Fatal(err)
		}
		provider.paths = append(provider.paths, record.Path)
		provider.raw[record.Path] = raw
	}
	return provider
}

func cloneActionPolicyProvider(source *actionPolicyProviderStub) *actionPolicyProviderStub {
	clone := &actionPolicyProviderStub{revisions: append([]string(nil), source.revisions...),
		paths: append([]string(nil), source.paths...), raw: make(map[string][]byte),
		readErrors: make(map[string]error), reads: make(map[string]int)}
	for path, raw := range source.raw {
		clone.raw[path] = append([]byte(nil), raw...)
	}
	return clone
}
