package store

import (
	"context"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func exerciseStoreChannelLeaveRetry(t *testing.T, st *Store,
	request model.SignedChannelLeaveRequest,
) time.Time {
	t.Helper()
	targets, err := st.ReadDueChannelLeaveTargets(context.Background(),
		request.Record().RequestedAt())
	if err != nil || len(targets) != 1 || targets[0].Request().RequestID() != request.RequestID() ||
		targets[0].Attempts() != 0 {
		t.Fatalf("ReadDueChannelLeaveTargets() = (%#v,%v)", targets, err)
	}
	attemptedAt := request.Record().RequestedAt().Add(time.Second)
	retryAt := attemptedAt.Add(time.Minute)
	if err := st.StartChannelLeaveAttempt(context.Background(), StartChannelLeaveAttemptSpec{
		RequestID: request.RequestID(), ExpectedNextAttemptAt: request.Record().RequestedAt(),
		AttemptedAt: attemptedAt, RetryAt: retryAt}); err != nil {
		t.Fatal(err)
	}
	if targets, err := st.ReadDueChannelLeaveTargets(context.Background(),
		retryAt.Add(-time.Nanosecond)); err != nil || len(targets) != 0 {
		t.Fatalf("early retry targets = (%#v,%v)", targets, err)
	}
	targets, err = st.ReadDueChannelLeaveTargets(context.Background(), retryAt)
	if err != nil || len(targets) != 1 || targets[0].Attempts() != 1 ||
		!targets[0].NextAttemptAt().Equal(retryAt) {
		t.Fatalf("due retry targets = (%#v,%v)", targets, err)
	}
	return attemptedAt
}
