package agent

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

var ErrCurrentViewCoordination = errors.New("managed current Artifact view coordination failed")

type CurrentViewMaterializer interface {
	Materialize(context.Context, artifact.ViewSpec) (artifact.MaterializedView, error)
	CleanupRun(context.Context, model.RunID) error
}

type AgentCurrentViews interface {
	Materialize(context.Context, store.AgentCurrentReadPlan) ([]model.CurrentArtifactRef, error)
	CleanupRun(context.Context, model.RunID) error
}

type CurrentViewCoordinator struct {
	materializer CurrentViewMaterializer
}

func NewCurrentViewCoordinator(materializer CurrentViewMaterializer) (*CurrentViewCoordinator, error) {
	if materializer == nil {
		return nil, fmt.Errorf("%w: materializer is required", ErrCurrentViewCoordination)
	}
	return &CurrentViewCoordinator{materializer: materializer}, nil
}

func (coordinator *CurrentViewCoordinator) Materialize(ctx context.Context,
	plan store.AgentCurrentReadPlan,
) ([]model.CurrentArtifactRef, error) {
	if coordinator == nil || coordinator.materializer == nil || ctx == nil || plan.RunID.IsZero() ||
		plan.Artifacts == nil || len(plan.Artifacts) > model.MaxCurrentArtifactRefs {
		return nil, fmt.Errorf("%w: current materialization plan is incomplete", ErrCurrentViewCoordination)
	}
	if plan.Replay != nil && (plan.Replay.Receipt.RunID() != plan.RunID ||
		len(plan.Replay.Receipt.ArtifactRefs()) != len(plan.Artifacts)) {
		return nil, fmt.Errorf("%w: replay evidence differs from its plan", ErrCurrentViewCoordination)
	}
	views := make([]model.CurrentArtifactRef, len(plan.Artifacts))
	materialized := false
	for index, item := range plan.Artifacts {
		if item.Ordinal != uint32(index) || item.RootDigest.IsZero() || item.ManifestDigest.IsZero() {
			coordinator.cleanupAfterFailure(plan.RunID, materialized)
			return nil, fmt.Errorf("%w: plan contains a noncanonical Artifact item", ErrCurrentViewCoordination)
		}
		result, err := coordinator.materializer.Materialize(ctx, artifact.ViewSpec{RunID: plan.RunID,
			Ordinal: item.Ordinal, RootDigest: item.RootDigest, ManifestDigest: item.ManifestDigest})
		if err != nil {
			coordinator.cleanupAfterFailure(plan.RunID, true)
			return nil, fmt.Errorf("%w: materialize ordinal %d: %v",
				ErrCurrentViewCoordination, item.Ordinal, err)
		}
		materialized = true
		if err := validateMaterializedCurrentView(plan.RunID, item, result); err != nil {
			coordinator.cleanupAfterFailure(plan.RunID, true)
			return nil, err
		}
		publicPath := ".mnemon/harness/node/" + result.RelativePath
		views[index], err = model.NewCurrentArtifactView(item.RootDigest, publicPath)
		if err != nil {
			coordinator.cleanupAfterFailure(plan.RunID, true)
			return nil, fmt.Errorf("%w: public view path: %v", ErrCurrentViewCoordination, err)
		}
	}
	if plan.Replay != nil && !sameCoordinatedCurrentViews(views, plan.Replay.Receipt.ArtifactRefs()) {
		coordinator.cleanupAfterFailure(plan.RunID, materialized)
		return nil, fmt.Errorf("%w: replay materialized a different path/root mapping",
			ErrCurrentViewCoordination)
	}
	return views, nil
}

func (coordinator *CurrentViewCoordinator) CleanupRun(ctx context.Context, runID model.RunID) error {
	if coordinator == nil || coordinator.materializer == nil || ctx == nil || runID.IsZero() {
		return fmt.Errorf("%w: cleanup input is incomplete", ErrCurrentViewCoordination)
	}
	if err := coordinator.materializer.CleanupRun(ctx, runID); err != nil {
		return fmt.Errorf("%w: cleanup Run: %v", ErrCurrentViewCoordination, err)
	}
	return nil
}

func (coordinator *CurrentViewCoordinator) cleanupAfterFailure(runID model.RunID, needed bool) {
	if !needed || runID.IsZero() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = coordinator.materializer.CleanupRun(ctx, runID)
}

func validateMaterializedCurrentView(runID model.RunID,
	item store.AgentCurrentArtifactMaterialization, result artifact.MaterializedView,
) error {
	if result.RootDigest != item.RootDigest || result.ManifestDigest != item.ManifestDigest ||
		result.Directory == "" || !filepath.IsAbs(result.Directory) || result.Path == "" ||
		!filepath.IsAbs(result.Path) || result.RelativePath == "" ||
		strings.Contains(result.RelativePath, `\`) || filepath.IsAbs(result.RelativePath) {
		return fmt.Errorf("%w: materializer result differs from its plan", ErrCurrentViewCoordination)
	}
	prefix := "views/" + runID.String() + "/" + strconv.FormatUint(uint64(item.Ordinal), 10)
	if result.RelativePath != prefix && !strings.HasPrefix(result.RelativePath, prefix+"/") {
		return fmt.Errorf("%w: materializer result escaped its Run ordinal", ErrCurrentViewCoordination)
	}
	if filepath.Clean(result.Path) != result.Path || filepath.Clean(result.Directory) != result.Directory ||
		(result.Path != result.Directory && !strings.HasPrefix(result.Path, result.Directory+string(filepath.Separator))) {
		return fmt.Errorf("%w: materializer returned a noncanonical local path", ErrCurrentViewCoordination)
	}
	return nil
}

func sameCoordinatedCurrentViews(left, right []model.CurrentArtifactRef) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		leftPath, leftOK := left[index].ViewPath()
		rightPath, rightOK := right[index].ViewPath()
		if left[index].RootDigest() != right[index].RootDigest() || leftOK != rightOK ||
			leftPath != rightPath {
			return false
		}
	}
	return true
}

var _ CurrentViewMaterializer = (*artifact.ViewMaterializer)(nil)
var _ AgentCurrentViews = (*CurrentViewCoordinator)(nil)
