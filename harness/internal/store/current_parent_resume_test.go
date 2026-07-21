package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestFinalizeAgentCurrentReadProjectsParentResumeChildResults(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newDerivationDispositionFixture(t, true)
	if err := fixture.store.ReconcileWorkDerivationDisposition(ctx, fixture.children[0]); err != nil {
		t.Fatal(err)
	}
	handling := fixture.handling(t)
	claimAt := fixture.now.Add(7 * time.Second)
	claim := claimCurrent(t, fixture.acceptanceFixture, "owner-parent-resume",
		"token-parent-resume", claimAt)
	if claim.Status != AgentClaimActionable || claim.Handling.ID() != handling.ID() {
		t.Fatalf("parent-resume claim = %#v, want Handling %s", claim, handling.ID())
	}
	spec := currentReadSpec(fixture.acceptanceFixture, claim.Run.ID(), "token-parent-resume",
		claimAt.Add(time.Second))
	spec = plannedCurrentReadSpec(t, fixture.store, spec)
	result, err := fixture.store.FinalizeAgentCurrentRead(ctx, spec)
	if err != nil || result.Replayed {
		t.Fatalf("FinalizeAgentCurrentRead(parent-resume) = (%#v,%v)", result, err)
	}
	assertParentResumeCurrentProjection(t, result.Projection, fixture, handling)
	assertParentResumeCurrentReceipt(t, result.Receipt, fixture)
	replaySpec := currentReadSpec(fixture.acceptanceFixture, claim.Run.ID(), "token-parent-resume",
		claimAt.Add(2*time.Second))
	replaySpec.ArtifactViews = result.Receipt.ArtifactRefs()
	replay, err := fixture.store.FinalizeAgentCurrentRead(ctx, replaySpec)
	assertParentResumeCurrentReplay(t, replay, result, err)
}

func assertParentResumeCurrentProjection(t *testing.T, projection model.CurrentProjection,
	fixture *derivationDispositionFixture, handling model.Handling,
) {
	t.Helper()
	if projection.ActionWork().Ref() != fixture.parent ||
		projection.SourceEvent().Key().EventID() != handling.EventID() ||
		len(projection.ChildResults()) != len(fixture.children) ||
		!sameOperationKinds(projection.AllowedActions(),
			[]model.OperationKind{model.OperationTeamworkDeliver, model.OperationResolveRetry}) {
		t.Fatalf("parent-resume projection = %s", projection.CanonicalJSON().String())
	}
	assertParentResumeCurrentChildResults(t, projection.ChildResults(), fixture)
}

func assertParentResumeCurrentChildResults(t *testing.T,
	children []model.CurrentChildResult, fixture *derivationDispositionFixture,
) {
	t.Helper()
	for index, child := range children {
		if child.Ordinal() != uint8(index) || child.WorkRef() != fixture.children[index] ||
			child.State() != model.WorkClosed || child.Version() != 4 {
			t.Fatalf("child result %d = %#v", index, child)
		}
	}
}

func assertParentResumeCurrentReceipt(t *testing.T, receipt model.CurrentReadReceipt,
	fixture *derivationDispositionFixture,
) {
	t.Helper()
	if len(receipt.ArtifactRefs()) != 1 ||
		receipt.ArtifactRefs()[0].RootDigest() != fixture.artifacts[0][0].RootDigest() ||
		!strings.Contains(receipt.CanonicalJSON().String(), `"child_results"`) {
		t.Fatalf("parent-resume receipt artifacts/json = %s", receipt.CanonicalJSON().String())
	}
}

func assertParentResumeCurrentReplay(t *testing.T, replay, result AgentCurrentReadResult,
	err error,
) {
	t.Helper()
	if err != nil || !replay.Replayed ||
		replay.Receipt.CanonicalJSON().String() != result.Receipt.CanonicalJSON().String() {
		t.Fatalf("parent-resume replay = (%#v,%v)", replay, err)
	}
}
