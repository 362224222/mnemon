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
	if result, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(),
		runtimeLaunchSpec(fixture, first, firstToken, diagnostic, firstRuntimeIDs,
			firstLaunchAt)); err != nil || result.Status != AgentRuntimeApplied {
		t.Fatalf("first Runtime launch = (%#v, %v)", result, err)
	}
	failureAt := claimAt.Add(time.Second)
	failed, err := fixture.store.FailAgentRuntime(context.Background(),
		runtimeFailureSpec(fixture, first, firstToken, diagnostic, firstRuntimeIDs,
			runtimeTestJSON(t, `{"kind":"runtime_completion","result":"failed"}`),
			"transient Runtime failure", failureAt))
	if err != nil || failed.Status != AgentRuntimeApplied ||
		failed.Run.Status() != model.AgentRunFailed ||
		failed.Handling.Status() != model.HandlingPending {
		t.Fatalf("first Runtime failure = (%#v, %v)", failed, err)
	}

	secondToken := model.Sum([]byte("runtime-token-channel-status-progress-current-attempt-second"))
	second := preclaimWake(t, fixture, secondToken, failed.Handling.AvailableAt())
	if second.Handling.Attempts() != 2 || second.Run.HandlingAttempt() != 2 {
		t.Fatalf("second attempt claim = Handling %#v Run %#v", second.Handling, second.Run)
	}
	secondRuntimeIDs := runtimeTestJSON(t, `{"process":"status-progress-second"}`)
	secondLaunchAt := second.Run.StartedAt().Add(250 * time.Millisecond)
	if result, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(),
		runtimeLaunchSpec(fixture, second, secondToken, diagnostic, secondRuntimeIDs,
			secondLaunchAt)); err != nil || result.Status != AgentRuntimeApplied {
		t.Fatalf("second Runtime launch = (%#v, %v)", result, err)
	}
	if result, err := fixture.store.RecordAgentWakeDelivery(context.Background(),
		wakeDeliverySpec(fixture, second, secondToken,
			runtimeTestJSON(t, `{"hook_id":"status-progress-second"}`),
			secondLaunchAt.Add(250*time.Millisecond))); err != nil ||
		result.Status != AgentRuntimeApplied {
		t.Fatalf("second wake delivery = (%#v, %v)", result, err)
	}

	finishedAt := secondLaunchAt.Add(time.Second)
	completionAt := finishedAt.Add(time.Millisecond)
	result, err := fixture.store.db.Exec(`UPDATE agent_runs SET status='outcome_accepted',
		finished_at=?,completion_at=?,completion_receipt_json=?,outcome_receipt_json=?
		WHERE run_id=? AND status='running'`, storeTime(finishedAt), storeTime(completionAt),
		runtimeTestJSON(t, `{"kind":"runtime_completion","result":"accepted"}`).Bytes(),
		runtimeTestJSON(t, `{"kind":"agent_outcome","result":"accepted"}`).Bytes(),
		second.Run.ID().String())
	if err != nil {
		t.Fatal(err)
	}
	if err := requireExactlyOneRow(result, "complete second test AgentRun"); err != nil {
		t.Fatal(err)
	}
	result, err = fixture.store.db.Exec(`UPDATE agent_handlings SET status='completed',
		claim_owner=NULL,claim_token_hash=NULL,lease_until=NULL,last_disposition='teamwork_action',
		last_error=NULL,updated_at=? WHERE handling_id=? AND status='claimed'`,
		storeTime(finishedAt), second.Handling.ID().String())
	if err != nil {
		t.Fatal(err)
	}
	if err := requireExactlyOneRow(result, "complete second test Handling"); err != nil {
		t.Fatal(err)
	}
	var historicalFailures int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM agent_runs
		WHERE handling_id=? AND status='failed'`, second.Handling.ID().String()).
		Scan(&historicalFailures); err != nil || historicalFailures != 1 {
		t.Fatalf("historical failed Run count = %d, %v", historicalFailures, err)
	}

	authority, err := fixture.store.ReadChannelStatusAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	runtime := requireChannelStatusChannel(t, authority, fixture.channel).Progress().Runtime()
	if runtime.HandlingCompleted != 1 || runtime.RunCompleted != 1 ||
		runtime.RunFailed != 0 || runtime.RunActive != 0 || runtime.RunRetry != 0 {
		t.Fatalf("Runtime progress counted historical attempt = %#v", runtime)
	}
}
