package store

import (
	"context"
	"errors"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestExactDeadlineCauseRejectsMissingWorkUpdateEvent(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	if _, err := exactDeadlineCause(context.Background(), st.db, model.ReviewWork{}); !errors.Is(err, ErrDeadlineResolution) {
		t.Fatalf("exactDeadlineCause() error = %v", err)
	}
}
