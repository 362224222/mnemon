package agent

import (
	"context"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type fakeArtifactCoordinator struct {
	result       ArtifactCoordinationResult
	apiErr       *ControlError
	last         ArtifactCoordinationSpec
	calls        int
	publishErr   *ControlError
	publishID    model.OperationID
	publishCalls int
}

func (coordinator *fakeArtifactCoordinator) Coordinate(_ context.Context,
	spec ArtifactCoordinationSpec,
) (ArtifactCoordinationResult, *ControlError) {
	coordinator.calls++
	coordinator.last = spec
	return coordinator.result, coordinator.apiErr
}

func (coordinator *fakeArtifactCoordinator) PublishAccepted(_ context.Context,
	operation model.OperationID,
) *ControlError {
	coordinator.publishCalls++
	coordinator.publishID = operation
	return coordinator.publishErr
}

func assertNoArtifactPublication(t *testing.T, coordinator *fakeArtifactCoordinator) {
	t.Helper()
	if coordinator.publishCalls != 0 {
		t.Fatalf("unexpected Artifact publication calls = %d", coordinator.publishCalls)
	}
}
