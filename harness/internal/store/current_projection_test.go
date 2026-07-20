package store

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestDeriveCurrentActionsFailsClosedWithoutExactUpdate(t *testing.T) {
	want := []model.OperationKind{
		model.OperationResolveNoAction,
		model.OperationResolveRetry,
		model.OperationResolveReject,
	}
	got := deriveCurrentActions(model.CurrentReviewer, model.Event{}, model.ReviewWork{}, false)
	if !sameOperationKinds(got, want) {
		t.Fatalf("stale authority actions = %v, want %v", got, want)
	}
}
