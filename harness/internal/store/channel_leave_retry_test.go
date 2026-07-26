package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestChannelLeaveFailureIsFencedObservableAndExplicitlyRecoverableAcrossRestart(t *testing.T) {
	t.Parallel()
	fixture := newInstalledJoinedChannelFixture(t, "leave-terminal-retry",
		"leave-terminal-team", 0x71, 0x72)
	request := signedStoreLeaveRequest(t, fixture, fixture.at.Add(time.Second))
	initialOperation := testChannelLeaveOperation("leave-terminal-initial")
	if _, err := fixture.store.BeginChannelLeave(context.Background(), BeginChannelLeaveSpec{
		ChannelID: request.Record().ChannelID(), Request: request,
		Operation: initialOperation, At: request.Record().RequestedAt()}); err != nil {
		t.Fatal(err)
	}
	attemptedAt := request.Record().RequestedAt().Add(time.Second)
	retryAt := attemptedAt.Add(time.Minute)
	if err := fixture.store.StartChannelLeaveAttempt(context.Background(),
		StartChannelLeaveAttemptSpec{RequestID: request.RequestID(),
			ExpectedNextAttemptAt: request.Record().RequestedAt(), AttemptedAt: attemptedAt,
			RetryAt: retryAt}); err != nil {
		t.Fatal(err)
	}
	failedAt := attemptedAt.Add(time.Second)
	if err := fixture.store.FailChannelLeaveAttempt(context.Background(),
		FailChannelLeaveAttemptSpec{RequestID: request.RequestID(), ExpectedAttempts: 1,
			ExpectedNextAttemptAt: retryAt, Failure: ChannelLeaveFailurePermanent,
			FailedAt: failedAt}); err != nil {
		t.Fatal(err)
	}
	assertChannelLeaveStatusProgress(t, fixture.store, request.Record().ChannelID(),
		"failed", 1, ChannelLeaveFailurePermanent)
	path := fixture.store.Path()
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenExisting(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	if due, err := restarted.ReadDueChannelLeaveTargets(context.Background(),
		retryAt.Add(24*time.Hour)); err != nil || len(due) != 0 {
		t.Fatalf("terminal leave retried after restart = (%#v,%v)", due, err)
	}
	assertChannelLeaveStatusProgress(t, restarted, request.Record().ChannelID(),
		"failed", 1, ChannelLeaveFailurePermanent)

	recoveredAt := failedAt.Add(time.Second)
	recoveryOperation := testChannelLeaveOperation("leave-terminal-recovery")
	recovered, err := restarted.BeginChannelLeave(context.Background(), BeginChannelLeaveSpec{
		ChannelID: request.Record().ChannelID(), Operation: recoveryOperation, At: recoveredAt})
	if err != nil || !recovered.Replay ||
		recovered.Request.RequestID() != request.RequestID() {
		t.Fatalf("explicit leave recovery = (%#v,%v)", recovered, err)
	}
	assertChannelLeaveStatusProgress(t, restarted, request.Record().ChannelID(),
		"queued", 0, "")
	due, err := restarted.ReadDueChannelLeaveTargets(context.Background(), recoveredAt)
	if err != nil || len(due) != 1 || due[0].RetryGeneration() != 1 {
		t.Fatalf("recovered leave generation = (%#v,%v)", due, err)
	}
	nextRetryAt := retryAt
	if err := restarted.StartChannelLeaveAttempt(context.Background(),
		StartChannelLeaveAttemptSpec{RequestID: request.RequestID(),
			ExpectedGeneration: 1, ExpectedNextAttemptAt: recoveredAt, AttemptedAt: recoveredAt,
			RetryAt: nextRetryAt}); err != nil {
		t.Fatal(err)
	}
	stale := restarted.FailChannelLeaveAttempt(context.Background(),
		FailChannelLeaveAttemptSpec{RequestID: request.RequestID(), ExpectedGeneration: 0,
			ExpectedAttempts:      1,
			ExpectedNextAttemptAt: retryAt, Failure: ChannelLeaveFailureAttemptsExhausted,
			FailedAt: recoveredAt.Add(time.Second)})
	if !errors.Is(stale, ErrChannelLeaveConflict) {
		t.Fatalf("old attempt terminalization error = %v", stale)
	}
	assertChannelLeaveStatusProgress(t, restarted, request.Record().ChannelID(), "sent", 1, "")
	secondFailedAt := recoveredAt.Add(2 * time.Second)
	if err := restarted.FailChannelLeaveAttempt(context.Background(),
		FailChannelLeaveAttemptSpec{RequestID: request.RequestID(), ExpectedGeneration: 1,
			ExpectedAttempts:      1,
			ExpectedNextAttemptAt: nextRetryAt, Failure: ChannelLeaveFailurePermanent,
			FailedAt: secondFailedAt}); err != nil {
		t.Fatal(err)
	}
	responseLossReplay, err := restarted.BeginChannelLeave(context.Background(),
		BeginChannelLeaveSpec{ChannelID: request.Record().ChannelID(),
			Operation: recoveryOperation, At: secondFailedAt.Add(time.Second)})
	if err != nil || !responseLossReplay.Replay {
		t.Fatalf("response-loss recovery replay = (%#v,%v)", responseLossReplay, err)
	}
	assertChannelLeaveStatusProgress(t, restarted, request.Record().ChannelID(),
		"failed", 1, ChannelLeaveFailurePermanent)
	if due, err := restarted.ReadDueChannelLeaveTargets(context.Background(),
		secondFailedAt.Add(24*time.Hour)); err != nil || len(due) != 0 {
		t.Fatalf("replayed recovery advanced terminal generation = (%#v,%v)", due, err)
	}
	for name, mutation := range map[string]string{
		"generation change": `UPDATE channel_leave_requests SET retry_generation=2
			WHERE request_id=?`,
		"attempt overflow": `UPDATE channel_leave_requests SET attempts=6
			WHERE request_id=?`,
		"dead status": `UPDATE channel_leave_requests SET status='rejected'
			WHERE request_id=?`,
	} {
		if _, err := restarted.db.Exec(mutation, request.RequestID().String()); err == nil {
			t.Fatalf("schema admitted Channel leave %s", name)
		}
	}
}

func TestChannelLeaveCorruptRetryStateCombinationsFailClosed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		seed     string
		mutation string
	}{
		{name: "queued fifth attempt", seed: "queued-fifth",
			mutation: `UPDATE channel_leave_requests SET attempts=5 WHERE request_id=?`},
		{name: "sent zero attempts", seed: "sent-zero",
			mutation: `UPDATE channel_leave_requests SET status='sent' WHERE request_id=?`},
		{name: "failed zero attempts", seed: "failed-zero", mutation: `UPDATE channel_leave_requests
			SET status='failed',failure_code='permanent_failure' WHERE request_id=?`},
		{name: "premature exhaustion", seed: "premature-exhaustion",
			mutation: `UPDATE channel_leave_requests
			SET status='failed',attempts=1,failure_code='attempts_exhausted' WHERE request_id=?`},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newInstalledJoinedChannelFixture(t, "leave-corrupt-"+test.seed,
				"leave-corrupt-team-"+test.seed, 0x73, 0x74)
			request := signedStoreLeaveRequest(t, fixture, fixture.at.Add(time.Second))
			if _, err := fixture.store.BeginChannelLeave(context.Background(),
				BeginChannelLeaveSpec{ChannelID: request.Record().ChannelID(), Request: request,
					Operation: testChannelLeaveOperation("leave-corrupt-" + test.seed),
					At:        request.Record().RequestedAt()}); err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.store.db.Exec("PRAGMA ignore_check_constraints=ON"); err != nil {
				t.Fatal(err)
			}
			_, mutationErr := fixture.store.db.Exec(test.mutation, request.RequestID().String())
			if _, err := fixture.store.db.Exec("PRAGMA ignore_check_constraints=OFF"); err != nil {
				t.Fatal(err)
			}
			if mutationErr != nil {
				t.Fatal(mutationErr)
			}
			if _, err := fixture.store.ReadChannelObservation(context.Background()); err == nil {
				t.Fatal("corrupt Channel leave retry state remained observable as authority")
			}
		})
	}
}

func assertChannelLeaveStatusProgress(t *testing.T, st *Store, channelID model.ChannelID,
	wantStatus string, wantAttempts uint64, wantDiagnostic ChannelLeaveFailureCode,
) {
	t.Helper()
	observation, err := st.ReadChannelObservation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	progress := requireChannelObservationChannel(t, observation, channelID).Progress().Leave()
	if progress.Status != wantStatus || progress.Attempts != wantAttempts ||
		progress.Diagnostic != wantDiagnostic {
		t.Fatalf("leave status progress = %#v, want (%q,%d,%q)", progress,
			wantStatus, wantAttempts, wantDiagnostic)
	}
}

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
