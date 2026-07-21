package store

import (
	"context"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestReadChannelStatusProgressKeepsIdleChannelHealthyAndCountsAcceptedPublication(t *testing.T) {
	t.Parallel()
	fixture, _ := acceptedGossipFixtureWithPublication(t, "channel-status-progress")
	authority, err := fixture.store.ReadChannelStatusAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	progress := requireChannelStatusChannel(t, authority, fixture.channel).Progress()
	publication := progress.Publication()
	if progress.Commit().Accepted != 1 || publication.Queued != 1 || publication.Published != 0 ||
		publication.Blocked != 0 || progress.Inbox().Durable != 0 ||
		progress.Runtime() != (ChannelStatusRuntimeProgress{}) {
		t.Fatalf("Channel progress = %#v", progress)
	}
}

func TestReadChannelStatusProgressCountsDurableInboxAndCursorWithoutPayload(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "channel-status-progress-inbox", 0)
	publication := fixture.publication(t, 1, 1, "progress", true)
	fixture.put(t, publication, fixture.at)
	authority, err := fixture.store.ReadChannelStatusAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	progress := requireChannelStatusChannel(t, authority, fixture.channel.Channel().ID()).Progress()
	if progress.Inbox().Durable != 1 || progress.Inbox().Pending != 1 ||
		progress.Cursor().InboundOrigins != 1 || progress.Cursor().InboundGapped != 0 {
		t.Fatalf("imported Channel progress = %#v", progress)
	}
}

func TestReadChannelStatusProgressCountsOnlyCurrentHandlingAttempt(t *testing.T) {
	t.Parallel()
	fixture, first, firstToken, claimAt := newWakeRuntimeFixture(t,
		"channel-status-progress-current-attempt", 0)
	diagnostic := runtimeTestJSON(t, `{"adapter":"codex-app-server","phase":"launch"}`)
	firstRuntimeIDs := runtimeTestJSON(t, `{"process":"status-progress-first"}`)
	firstLaunchAt := claimAt.Add(250 * time.Millisecond)
	failed := failStatusProgressRuntimeAttempt(t, fixture, first, firstToken, diagnostic,
		firstRuntimeIDs, firstLaunchAt)

	secondToken := model.Sum([]byte("runtime-token-channel-status-progress-current-attempt-second"))
	second := preclaimWake(t, fixture, secondToken, failed.Handling.AvailableAt())
	if second.Handling.Attempts() != 2 || second.Run.HandlingAttempt() != 2 {
		t.Fatalf("second attempt claim = Handling %#v Run %#v", second.Handling, second.Run)
	}
	completeStatusProgressRuntimeAttempt(t, fixture, second, secondToken, diagnostic)
	requireStatusProgressHistoricalFailures(t, fixture, second)

	authority, err := fixture.store.ReadChannelStatusAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	runtime := requireChannelStatusChannel(t, authority, fixture.channel).Progress().Runtime()
	assertStatusProgressCurrentAttemptRuntime(t, runtime)
}

func failStatusProgressRuntimeAttempt(t *testing.T, fixture *acceptanceFixture,
	claim AgentClaimResult, token model.Digest, diagnostic, runtimeIDs model.JSON,
	launchAt time.Time,
) AgentRuntimeTransitionResult {
	t.Helper()
	if result, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(),
		runtimeLaunchSpec(fixture, claim, token, diagnostic, runtimeIDs,
			launchAt)); err != nil || result.Status != AgentRuntimeApplied {
		t.Fatalf("first Runtime launch = (%#v, %v)", result, err)
	}
	failed, err := fixture.store.FailAgentRuntime(context.Background(),
		runtimeFailureSpec(fixture, claim, token, diagnostic, runtimeIDs,
			runtimeTestJSON(t, `{"kind":"runtime_completion","result":"failed"}`),
			"transient Runtime failure", launchAt.Add(750*time.Millisecond)))
	if err != nil || failed.Status != AgentRuntimeApplied ||
		failed.Run.Status() != model.AgentRunFailed ||
		failed.Handling.Status() != model.HandlingPending {
		t.Fatalf("first Runtime failure = (%#v, %v)", failed, err)
	}
	return failed
}

func completeStatusProgressRuntimeAttempt(t *testing.T, fixture *acceptanceFixture,
	claim AgentClaimResult, token model.Digest, diagnostic model.JSON,
) {
	t.Helper()
	runtimeIDs := runtimeTestJSON(t, `{"process":"status-progress-second"}`)
	launchAt := claim.Run.StartedAt().Add(250 * time.Millisecond)
	if result, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(),
		runtimeLaunchSpec(fixture, claim, token, diagnostic, runtimeIDs,
			launchAt)); err != nil || result.Status != AgentRuntimeApplied {
		t.Fatalf("second Runtime launch = (%#v, %v)", result, err)
	}
	if result, err := fixture.store.RecordAgentWakeDelivery(context.Background(),
		wakeDeliverySpec(fixture, claim, token,
			runtimeTestJSON(t, `{"hook_id":"status-progress-second"}`),
			launchAt.Add(250*time.Millisecond))); err != nil ||
		result.Status != AgentRuntimeApplied {
		t.Fatalf("second wake delivery = (%#v, %v)", result, err)
	}
	finishedAt := launchAt.Add(time.Second)
	result, err := fixture.store.db.Exec(`UPDATE agent_runs SET status='outcome_accepted',
		finished_at=?,completion_at=?,completion_receipt_json=?,outcome_receipt_json=?
		WHERE run_id=? AND status='running'`, storeTime(finishedAt),
		storeTime(finishedAt.Add(time.Millisecond)),
		runtimeTestJSON(t, `{"kind":"runtime_completion","result":"accepted"}`).Bytes(),
		runtimeTestJSON(t, `{"kind":"agent_outcome","result":"accepted"}`).Bytes(),
		claim.Run.ID().String())
	if err != nil {
		t.Fatal(err)
	}
	if err := requireExactlyOneRow(result, "complete second test AgentRun"); err != nil {
		t.Fatal(err)
	}
	result, err = fixture.store.db.Exec(`UPDATE agent_handlings SET status='completed',
		claim_owner=NULL,claim_token_hash=NULL,lease_until=NULL,last_disposition='teamwork_action',
		last_error=NULL,updated_at=? WHERE handling_id=? AND status='claimed'`,
		storeTime(finishedAt), claim.Handling.ID().String())
	if err != nil {
		t.Fatal(err)
	}
	if err := requireExactlyOneRow(result, "complete second test Handling"); err != nil {
		t.Fatal(err)
	}
}

func requireStatusProgressHistoricalFailures(t *testing.T, fixture *acceptanceFixture,
	claim AgentClaimResult,
) {
	t.Helper()
	var historicalFailures int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM agent_runs
		WHERE handling_id=? AND status='failed'`, claim.Handling.ID().String()).
		Scan(&historicalFailures); err != nil || historicalFailures != 1 {
		t.Fatalf("historical failed Run count = %d, %v", historicalFailures, err)
	}
}

func assertStatusProgressCurrentAttemptRuntime(t *testing.T,
	runtime ChannelStatusRuntimeProgress,
) {
	t.Helper()
	if runtime.HandlingCompleted != 1 || runtime.RunCompleted != 1 ||
		runtime.RunFailed != 0 || runtime.RunActive != 0 || runtime.RunRetry != 0 {
		t.Fatalf("Runtime progress counted historical attempt = %#v", runtime)
	}
}
