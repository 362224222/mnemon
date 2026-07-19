package agent

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type currentViewMaterializerStub struct {
	results  []artifact.MaterializedView
	errAt    int
	calls    []artifact.ViewSpec
	cleanups []model.RunID
}

func (stub *currentViewMaterializerStub) Materialize(_ context.Context,
	spec artifact.ViewSpec,
) (artifact.MaterializedView, error) {
	stub.calls = append(stub.calls, spec)
	index := len(stub.calls) - 1
	if stub.errAt > 0 && len(stub.calls) == stub.errAt {
		return artifact.MaterializedView{}, errors.New("injected materialization failure")
	}
	return stub.results[index], nil
}

func (stub *currentViewMaterializerStub) CleanupRun(_ context.Context, run model.RunID) error {
	stub.cleanups = append(stub.cleanups, run)
	return nil
}

func TestCurrentViewCoordinatorMaterializesCanonicalPlanAndExactReplay(t *testing.T) {
	run, _ := model.ParseRunID("run-current-view-coordinator")
	firstRoot, secondRoot := model.Sum([]byte("current-view-first")), model.Sum([]byte("current-view-second"))
	firstManifest, secondManifest := model.Sum([]byte("manifest-first")), model.Sum([]byte("manifest-second"))
	if secondRoot.String() < firstRoot.String() {
		firstRoot, secondRoot = secondRoot, firstRoot
		firstManifest, secondManifest = secondManifest, firstManifest
	}
	node := t.TempDir()
	stub := &currentViewMaterializerStub{results: []artifact.MaterializedView{
		{Directory: filepath.Join(node, "views", run.String(), "0"),
			Path:         filepath.Join(node, "views", run.String(), "0", "input.txt"),
			RelativePath: "views/" + run.String() + "/0/input.txt",
			RootDigest:   firstRoot, ManifestDigest: firstManifest},
		{Directory: filepath.Join(node, "views", run.String(), "1"),
			Path:         filepath.Join(node, "views", run.String(), "1", "bundle"),
			RelativePath: "views/" + run.String() + "/1/bundle",
			RootDigest:   secondRoot, ManifestDigest: secondManifest},
	}}
	coordinator, err := NewCurrentViewCoordinator(stub)
	if err != nil {
		t.Fatal(err)
	}
	plan := store.AgentCurrentReadPlan{RunID: run, Artifacts: []store.AgentCurrentArtifactMaterialization{
		{Ordinal: 0, RootDigest: firstRoot, ManifestDigest: firstManifest},
		{Ordinal: 1, RootDigest: secondRoot, ManifestDigest: secondManifest},
	}}
	views, err := coordinator.Materialize(context.Background(), plan)
	if err != nil || len(views) != 2 || len(stub.calls) != 2 || len(stub.cleanups) != 0 {
		t.Fatalf("Materialize() = (%#v, %v), calls=%#v cleanups=%v", views, err, stub.calls, stub.cleanups)
	}
	for index, want := range []string{
		".mnemon/harness/node/views/" + run.String() + "/0/input.txt",
		".mnemon/harness/node/views/" + run.String() + "/1/bundle",
	} {
		path, ok := views[index].ViewPath()
		if !ok || path != want || views[index].RootDigest() != plan.Artifacts[index].RootDigest {
			t.Fatalf("view[%d] = %#v path=%q", index, views[index], path)
		}
	}
	plan.Replay = &store.AgentCurrentReadResult{Receipt: currentViewReplayReceipt(t, run, views)}
	stub.calls = nil
	replayed, err := coordinator.Materialize(context.Background(), plan)
	if err != nil || !sameCoordinatedCurrentViews(replayed, views) || len(stub.calls) != 2 {
		t.Fatalf("replay Materialize() = (%#v, %v), calls=%d", replayed, err, len(stub.calls))
	}
}

func TestCurrentViewCoordinatorCleansPartialRunAndRejectsReplayDrift(t *testing.T) {
	run, _ := model.ParseRunID("run-current-view-cleanup")
	rootA, rootB := model.Sum([]byte("cleanup-a")), model.Sum([]byte("cleanup-b"))
	manifestA, manifestB := model.Sum([]byte("manifest-cleanup-a")), model.Sum([]byte("manifest-cleanup-b"))
	if rootB.String() < rootA.String() {
		rootA, rootB = rootB, rootA
		manifestA, manifestB = manifestB, manifestA
	}
	node := t.TempDir()
	plan := store.AgentCurrentReadPlan{RunID: run, Artifacts: []store.AgentCurrentArtifactMaterialization{
		{Ordinal: 0, RootDigest: rootA, ManifestDigest: manifestA},
		{Ordinal: 1, RootDigest: rootB, ManifestDigest: manifestB},
	}}
	stub := &currentViewMaterializerStub{errAt: 2, results: []artifact.MaterializedView{{
		Directory:    filepath.Join(node, "views", run.String(), "0"),
		Path:         filepath.Join(node, "views", run.String(), "0", "a"),
		RelativePath: "views/" + run.String() + "/0/a", RootDigest: rootA, ManifestDigest: manifestA,
	}}}
	coordinator, _ := NewCurrentViewCoordinator(stub)
	if _, err := coordinator.Materialize(context.Background(), plan); !errors.Is(err, ErrCurrentViewCoordination) || len(stub.cleanups) != 1 || stub.cleanups[0] != run {
		t.Fatalf("partial failure = %v cleanups=%v", err, stub.cleanups)
	}

	stub.errAt, stub.calls, stub.cleanups = 0, nil, nil
	stub.results = []artifact.MaterializedView{{Directory: filepath.Join(node, "views", run.String(), "0"),
		Path:         filepath.Join(node, "views", run.String(), "0", "a"),
		RelativePath: "views/" + run.String() + "/0/a", RootDigest: rootA, ManifestDigest: manifestA},
		{Directory: filepath.Join(node, "views", run.String(), "1"),
			Path:         filepath.Join(node, "views", run.String(), "1", "b"),
			RelativePath: "views/" + run.String() + "/1/b", RootDigest: rootB, ManifestDigest: manifestB}}
	wrong, _ := model.NewCurrentArtifactView(rootA,
		".mnemon/harness/node/views/"+run.String()+"/0/different")
	second, _ := model.NewCurrentArtifactView(rootB,
		".mnemon/harness/node/views/"+run.String()+"/1/b")
	plan.Replay = &store.AgentCurrentReadResult{Receipt: currentViewReplayReceipt(t, run,
		[]model.CurrentArtifactRef{wrong, second})}
	if _, err := coordinator.Materialize(context.Background(), plan); !errors.Is(err, ErrCurrentViewCoordination) || len(stub.cleanups) != 1 {
		t.Fatalf("replay drift = %v cleanups=%v", err, stub.cleanups)
	}
}

func currentViewReplayReceipt(t *testing.T, run model.RunID,
	views []model.CurrentArtifactRef,
) model.CurrentReadReceipt {
	t.Helper()
	operation := artifactResolverOperation(t, "coordinator-replay", model.OperationTeamworkDeliver, nil)
	base, _ := artifactResolverCurrent(t, operation)
	projection := base.Projection()
	semantic := make([]model.CurrentArtifactRef, len(views))
	for index := range views {
		semantic[index], _ = model.NewCurrentArtifactRef(views[index].RootDigest())
	}
	source := projection.SourceEvent()
	event, err := model.NewCurrentEvent(model.CurrentEventSpec{Key: source.Key(), Digest: source.Digest(),
		Type: source.Type(), WorkRef: source.WorkRef(), Summary: source.Summary(), Payload: source.Payload(),
		ArtifactRefs: semantic, AcceptedAt: source.AcceptedAt()})
	if err != nil {
		t.Fatal(err)
	}
	actionWork := projection.ActionWork()
	baseBrief, _ := actionWork.Brief()
	brief, err := model.NewCurrentBrief(model.CurrentBriefSpec{Content: baseBrief.Content(),
		DeadlineUnixNano: baseBrief.DeadlineUnixNano(), ArtifactRefs: semantic})
	if err != nil {
		t.Fatal(err)
	}
	work, err := model.NewCurrentWork(model.CurrentWorkSpec{Ref: actionWork.Ref(), Version: actionWork.Version(),
		Iteration: actionWork.Iteration(), DeadlineUnixNano: actionWork.DeadlineUnixNano(),
		State: actionWork.State(), StateData: actionWork.StateData(), LocalRole: actionWork.LocalRole(), Brief: brief})
	if err != nil {
		t.Fatal(err)
	}
	bound, err := model.NewCurrentProjection(model.CurrentProjectionSpec{SourceEvent: event,
		ActionWork: work, AllowedActions: projection.AllowedActions(), ArtifactViews: views})
	if err != nil {
		t.Fatal(err)
	}
	handling, _ := model.ParseHandlingID("handling-current-view-coordinator")
	receipt, err := model.NewCurrentReadReceipt(model.CurrentReadReceiptSpec{RunID: run,
		ProfileID: model.TeamworkProfileID(), HandlingID: handling, HandlingAttempt: 1,
		Projection: bound, ActionWorkUpdatedBy: source.Key().EventID(), ActionWorkUpdatedAt: source.AcceptedAt(), ReadAt: base.ReadAt()})
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

var _ CurrentViewMaterializer = (*currentViewMaterializerStub)(nil)
