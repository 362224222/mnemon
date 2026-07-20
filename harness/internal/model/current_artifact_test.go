package model

import (
	"errors"
	"strings"
	"testing"
)

func TestCurrentArtifactViewsBindExactManagedPathsOnlyAtProjectionTopLevel(t *testing.T) {
	projection := currentTestProjection(t, []OperationKind{OperationTeamworkAccept})
	root := projection.ArtifactRefs()[0].RootDigest()
	viewPath := ".mnemon/harness/node/views/run-current-view/0/review.txt"
	view, err := NewCurrentArtifactView(root, viewPath)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := view.ViewPath(); !ok || got != viewPath {
		t.Fatalf("ViewPath() = (%q, %t)", got, ok)
	}
	materialized, err := NewCurrentProjection(CurrentProjectionSpec{
		SourceEvent: projection.SourceEvent(), ActionWork: projection.ActionWork(),
		AllowedActions: projection.AllowedActions(), ArtifactViews: []CurrentArtifactRef{view},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !materialized.HasMaterializedArtifactViews() ||
		strings.Count(materialized.CanonicalJSON().String(), viewPath) != 1 {
		t.Fatalf("materialized projection = %s", materialized.CanonicalJSON().String())
	}
	parsed, err := ParseCurrentProjection(materialized.CanonicalJSON().Bytes())
	if err != nil || !equalArtifactRefs(parsed.ArtifactRefs(), materialized.ArtifactRefs()) {
		t.Fatalf("ParseCurrentProjection(materialized) = (%#v, %v)", parsed, err)
	}
	if _, err := NewCurrentEvent(CurrentEventSpec{Key: projection.SourceEvent().Key(),
		Digest: projection.SourceEvent().Digest(), Type: projection.SourceEvent().Type(),
		WorkRef: projection.SourceEvent().WorkRef(), Summary: projection.SourceEvent().Summary(),
		Payload: projection.SourceEvent().Payload(), ArtifactRefs: []CurrentArtifactRef{view},
		AcceptedAt: projection.SourceEvent().AcceptedAt()}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("semantic Event view error = %v", err)
	}

	for _, path := range []string{
		"views/run-current-view/0/review.txt",
		".mnemon/harness/node/objects/sha256/aa",
		".mnemon/harness/node/views/run-current-view/00/review.txt",
		".mnemon/harness/node/views/run-current-view/0/../secret",
		".MNEMON/harness/node/views/run-current-view/0/review.txt",
	} {
		if _, err := NewCurrentArtifactView(root, path); err == nil {
			t.Fatalf("NewCurrentArtifactView accepted %q", path)
		}
	}
	other, _ := NewCurrentArtifactView(Sum([]byte("other-current-root")),
		".mnemon/harness/node/views/run-current-view/1/other.txt")
	if _, err := NewCurrentProjection(CurrentProjectionSpec{SourceEvent: projection.SourceEvent(),
		ActionWork: projection.ActionWork(), AllowedActions: projection.AllowedActions(),
		ArtifactViews: []CurrentArtifactRef{other}}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("mismatched materialized root error = %v", err)
	}
}
