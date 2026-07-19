package store

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestManagedResolutionRequestDigestBindsEveryAuthorityDimension(t *testing.T) {
	t.Parallel()
	revision := model.Sum([]byte("resolution-revision"))
	contextHash := model.Sum([]byte("resolution-context"))
	want, err := ManagedResolutionRequestDigest(revision, contextHash,
		model.OperationResolveRetry, "retry later")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name     string
		revision model.Digest
		context  model.Digest
		kind     model.OperationKind
		content  string
	}{
		{name: "revision", revision: model.Sum([]byte("another-revision")), context: contextHash,
			kind: model.OperationResolveRetry, content: "retry later"},
		{name: "context", revision: revision, context: model.Sum([]byte("another-context")),
			kind: model.OperationResolveRetry, content: "retry later"},
		{name: "kind", revision: revision, context: contextHash,
			kind: model.OperationResolveNoAction, content: "retry later"},
		{name: "content", revision: revision, context: contextHash,
			kind: model.OperationResolveRetry, content: "retry after repair"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, digestErr := ManagedResolutionRequestDigest(test.revision, test.context,
				test.kind, test.content)
			if digestErr != nil || got == want {
				t.Fatalf("mutated digest = (%s, %v), original %s", got, digestErr, want)
			}
		})
	}
}

func mustManagedResolutionRequestDigest(t *testing.T, profile model.Profile,
	contextHash model.Digest, kind model.OperationKind, content string,
) model.Digest {
	t.Helper()
	digest, err := ManagedResolutionRequestDigest(managedAssetRevisionDigest(profile.ActiveAssetRevision()),
		contextHash, kind, content)
	if err != nil {
		t.Fatal(err)
	}
	return digest
}
