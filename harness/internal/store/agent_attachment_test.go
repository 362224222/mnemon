package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestAgentWakePreclaimPeekConsumeAndExpiry(t *testing.T) {
	fixture, events := newAgentClaimFixture(t, 1, "attachment")
	at := fixture.now.Add(time.Minute)
	handling := insertClaimHandling(t, fixture.store, "handling-attachment", events[0], 1, at, at, 0)
	token := model.Sum([]byte("attachment-capability"))
	claim := preclaimWake(t, fixture, token, at)
	if claim.Status != AgentClaimActionable || claim.Handling.ID() != handling.ID() ||
		claim.Run.Status() != model.AgentRunStarting || claim.Run.Launcher() != "mnemond-wake" {
		t.Fatalf("PreclaimAgentWake() = %#v", claim)
	}
	attachmentHash, hasAttachment := claim.Run.AttachmentTokenHash()
	expiresAt, hasExpiry := claim.Run.AttachmentExpiresAt()
	if !hasAttachment || !hasExpiry || attachmentHash != token ||
		!expiresAt.Equal(at.Add(5*time.Minute)) {
		t.Fatalf("preclaim attachment authority = %#v", claim.Run)
	}
	spec := AgentAttachmentSpec{ProfileID: fixture.profile.ID(),
		ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(), AttachmentTokenHash: token, At: at}
	if err := fixture.store.PeekAgentRunAttachment(context.Background(), spec); err != nil {
		t.Fatalf("PeekAgentRunAttachment() error = %v", err)
	}
	peeked, err := fixture.store.GetAgentRun(context.Background(), claim.Run.ID())
	if err != nil {
		t.Fatal(err)
	}
	if _, consumed := peeked.AttachedAt(); consumed || peeked.Status() != model.AgentRunStarting {
		t.Fatalf("peek consumed attachment: %#v", peeked)
	}
	prelaunch := spec
	prelaunch.At = at.Add(250 * time.Millisecond)
	if _, err := fixture.store.ConsumeAgentRunAttachment(context.Background(), prelaunch); !errors.Is(err,
		ErrAgentAttachmentStale) {
		t.Fatalf("pre-launch attachment consume error = %v", err)
	}
	diagnostic := runtimeTestJSON(t, `{"adapter":"codex-app-server","phase":"initialized"}`)
	runtimeIDs := runtimeTestJSON(t, `{"process":"attachment-running"}`)
	launched, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(), AgentRuntimeLaunchSpec{
		ProfileID: fixture.profile.ID(), ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(),
		RunID: claim.Run.ID(), ClaimFenceHash: token, HandlingRecovery: claim.Run.HandlingRecovery(),
		LauncherDiagnostic: diagnostic, RuntimeIDs: runtimeIDs, At: at.Add(500 * time.Millisecond)})
	if err != nil || launched.Status != AgentRuntimeApplied || launched.Run.Status() != model.AgentRunRunning {
		t.Fatalf("launch before attachment consume = (%#v, %v)", launched, err)
	}
	if _, err := fixture.store.ConsumeAgentRunAttachment(context.Background(), prelaunch); !errors.Is(err,
		ErrAgentAttachmentStale) {
		t.Fatalf("attachment consume before durable Runtime start error = %v", err)
	}
	if _, err := fixture.store.db.Exec(`UPDATE agent_runs SET attached_at=? WHERE run_id=?`,
		storeTime(prelaunch.At), claim.Run.ID().String()); err == nil {
		t.Fatal("schema allowed attachment evidence before Runtime start")
	}
	consumedAt := at.Add(time.Second)
	spec.At = consumedAt
	consumed, err := fixture.store.ConsumeAgentRunAttachment(context.Background(), spec)
	if err != nil || consumed.Run.ID() != claim.Run.ID() || consumed.Status != AgentClaimActionable {
		t.Fatalf("ConsumeAgentRunAttachment() = (%#v, %v)", consumed, err)
	}
	if attachedAt, ok := consumed.Run.AttachedAt(); !ok || !attachedAt.Equal(consumedAt) ||
		consumed.Run.Status() != model.AgentRunRunning {
		t.Fatalf("consumed Run = %#v", consumed.Run)
	}
	if err := fixture.store.PeekAgentRunAttachment(context.Background(), spec); !errors.Is(err, ErrAgentAttachmentStale) {
		t.Fatalf("replayed peek error = %v", err)
	}
	if _, err := fixture.store.ConsumeAgentRunAttachment(context.Background(), spec); !errors.Is(err, ErrAgentAttachmentStale) {
		t.Fatalf("replayed consume error = %v", err)
	}
}

func TestAgentWakePreclaimIsAtomicAndConcurrent(t *testing.T) {
	fixture, events := newAgentClaimFixture(t, 1, "attachment-concurrent")
	at := fixture.now.Add(time.Minute)
	insertClaimHandling(t, fixture.store, "handling-attachment-concurrent", events[0], 1, at, at, 0)
	start := make(chan struct{})
	results := make(chan AgentClaimResult, 2)
	errs := make(chan error, 2)
	var wait sync.WaitGroup
	for index := 0; index < 2; index++ {
		index := index
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			token := model.Sum([]byte("attachment-concurrent-" + string(rune('a'+index))))
			result, err := fixture.store.PreclaimAgentWake(context.Background(), AgentWakePreclaimSpec{
				ProfileID: fixture.profile.ID(), ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(),
				ClaimOwner: "wake-owner-" + string(rune('a'+index)), AttachmentTokenHash: token,
				At: at, LeaseUntil: at.Add(5 * time.Minute)})
			results <- result
			errs <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	counts := map[AgentClaimStatus]int{}
	for result := range results {
		counts[result.Status]++
	}
	if counts[AgentClaimActionable] != 1 || counts[AgentClaimBusy] != 1 {
		t.Fatalf("preclaim concurrency = %#v", counts)
	}
	var runs int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM agent_runs
		WHERE handling_id='handling-attachment-concurrent'`).Scan(&runs); err != nil || runs != 1 {
		t.Fatalf("preclaim Run count = %d, %v", runs, err)
	}
}

func TestAgentAttachmentNeverFallsBackAndExpiryRequeues(t *testing.T) {
	fixture, events := newAgentClaimFixture(t, 2, "attachment-stale")
	at := fixture.now.Add(time.Minute)
	first := insertClaimHandling(t, fixture.store, "handling-attachment-stale-a", events[0], 2, at, at, 0)
	insertClaimHandling(t, fixture.store, "handling-attachment-stale-b", events[1], 1, at, at, 0)
	token := model.Sum([]byte("attachment-stale"))
	claim := preclaimWake(t, fixture, token, at)
	wrong := AgentAttachmentSpec{ProfileID: fixture.profile.ID(),
		ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(),
		AttachmentTokenHash:   model.Sum([]byte("wrong-attachment")), At: at}
	if err := fixture.store.PeekAgentRunAttachment(context.Background(), wrong); !errors.Is(err, ErrAgentAttachmentStale) {
		t.Fatalf("wrong attachment peek error = %v", err)
	}
	stored, err := fixture.store.GetAgentHandling(context.Background(), first.ID())
	if err != nil || stored.Status() != model.HandlingClaimed {
		t.Fatalf("wrong attachment changed claim = (%#v, %v)", stored, err)
	}
	lease, _ := claim.Run.LeaseUntil()
	expired := wrong
	expired.AttachmentTokenHash, expired.At = token, lease
	if err := fixture.store.PeekAgentRunAttachment(context.Background(), expired); !errors.Is(err, ErrAgentAttachmentStale) {
		t.Fatalf("expired attachment error = %v", err)
	}
	requeued, err := fixture.store.GetAgentHandling(context.Background(), first.ID())
	if err != nil || requeued.Status() != model.HandlingPending || requeued.Attempts() != 1 ||
		!requeued.AvailableAt().Equal(lease.Add(5*time.Second)) {
		t.Fatalf("expired attachment Handling = (%#v, %v)", requeued, err)
	}
	run, err := fixture.store.GetAgentRun(context.Background(), claim.Run.ID())
	if err != nil || run.Status() != model.AgentRunRequeued {
		t.Fatalf("expired attachment Run = (%#v, %v)", run, err)
	}
	reapable, err := fixture.store.ListReapableAgentRunAttachments(context.Background(),
		AgentAttachmentCleanupSpec{ProfileID: fixture.profile.ID(),
			ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(), At: lease,
			Candidates: []AgentRunAttachmentCandidate{{RunID: claim.Run.ID(), TokenHash: token}}})
	if err != nil || len(reapable) != 1 || reapable[0].RunID != claim.Run.ID() ||
		reapable[0].TokenHash != token {
		t.Fatalf("reapable expired attachment = (%#v, %v)", reapable, err)
	}
}

func TestAgentAttachmentCleanupExcludesActiveConsumedRun(t *testing.T) {
	fixture, events := newAgentClaimFixture(t, 1, "attachment-cleanup-active")
	at := fixture.now.Add(time.Minute)
	insertClaimHandling(t, fixture.store, "handling-attachment-cleanup-active", events[0], 1, at, at, 0)
	token := model.Sum([]byte("attachment-cleanup-active"))
	claim := preclaimWake(t, fixture, token, at)
	diagnostic := runtimeTestJSON(t, `{"adapter":"codex-app-server","phase":"initialized"}`)
	runtimeIDs := runtimeTestJSON(t, `{"process":"attachment-cleanup-active"}`)
	if result, err := fixture.store.RecordAgentRuntimeLaunch(context.Background(), AgentRuntimeLaunchSpec{
		ProfileID: fixture.profile.ID(), ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(),
		RunID: claim.Run.ID(), ClaimFenceHash: token, HandlingRecovery: claim.Run.HandlingRecovery(),
		LauncherDiagnostic: diagnostic, RuntimeIDs: runtimeIDs, At: at.Add(500 * time.Millisecond),
	}); err != nil || result.Status != AgentRuntimeApplied {
		t.Fatalf("launch cleanup fixture = (%#v, %v)", result, err)
	}
	if _, err := fixture.store.ConsumeAgentRunAttachment(context.Background(), AgentAttachmentSpec{
		ProfileID: fixture.profile.ID(), ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(),
		AttachmentTokenHash: token, At: at.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	reapable, err := fixture.store.ListReapableAgentRunAttachments(context.Background(),
		AgentAttachmentCleanupSpec{ProfileID: fixture.profile.ID(),
			ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(), At: at.Add(2 * time.Second),
			Candidates: []AgentRunAttachmentCandidate{{RunID: claim.Run.ID(), TokenHash: token}}})
	if err != nil || len(reapable) != 0 {
		t.Fatalf("active consumed cleanup = (%#v, %v)", reapable, err)
	}
	lease, _ := claim.Run.LeaseUntil()
	reapable, err = fixture.store.ListReapableAgentRunAttachments(context.Background(),
		AgentAttachmentCleanupSpec{ProfileID: fixture.profile.ID(),
			ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(), At: lease,
			Candidates: []AgentRunAttachmentCandidate{{RunID: claim.Run.ID(), TokenHash: token}}})
	if err != nil || len(reapable) != 1 || reapable[0].RunID != claim.Run.ID() {
		t.Fatalf("expired consumed cleanup = (%#v, %v)", reapable, err)
	}
}

func TestAgentAttachmentCleanupUsesRealFilesNotHistoricalRows(t *testing.T) {
	fixture, events := newAgentClaimFixture(t, 2, "attachment-history")
	at := fixture.now.Add(2 * time.Hour)
	historyCreated := at.Add(-2 * time.Hour)
	historyHandling := "handling-attachment-history"
	if _, err := fixture.store.db.Exec(`INSERT INTO agent_handlings(
		handling_id,profile_id,event_id,status,priority,available_at,attempts,last_disposition,created_at,updated_at)
		VALUES(?,?,?,'completed',0,?,70,'no_action',?,?)`, historyHandling,
		fixture.profile.ID().String(), events[0].String(), storeTime(historyCreated),
		storeTime(historyCreated), storeTime(historyCreated)); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 70; index++ {
		started := historyCreated.Add(time.Duration(index) * time.Second)
		lease := started.Add(5 * time.Minute)
		finished := started.Add(time.Second)
		hash := model.Sum([]byte(fmt.Sprintf("historical-attachment-%03d", index)))
		if _, err := fixture.store.db.Exec(`INSERT INTO agent_runs(
			run_id,profile_id,handling_id,cause_json,handling_attempt,claim_fence_hash,lease_until,
			attachment_token_hash,attachment_expires_at,launcher,runtime_kind,launcher_diagnostic_json,
			runtime_ids_json,status,started_at,finished_at,error)
			VALUES(?,?,?,'{}',?,?,?,?,?,'mnemond-wake',?,'{}','{}','failed',?,?,'historical')`,
			fmt.Sprintf("run-attachment-history-%03d", index), fixture.profile.ID().String(),
			historyHandling, index+1, hash.Bytes(), storeTime(lease), hash.Bytes(), storeTime(lease),
			string(fixture.profile.Runtime()), storeTime(started), storeTime(finished)); err != nil {
			t.Fatal(err)
		}
	}
	target := insertClaimHandling(t, fixture.store, "handling-attachment-history-target",
		events[1], 1, at, at, 0)
	nodeState := filepath.Dir(fixture.store.Path())
	token := bytes.Repeat([]byte{0xf1}, 32)
	stageID := bytes.Repeat([]byte{0xf2}, 16)
	staged, err := localapi.StageRunAttachment(nodeState,
		bytes.NewReader(append(token, stageID...)))
	if err != nil {
		t.Fatal(err)
	}
	claim := preclaimWake(t, fixture, staged.TokenHash(), at)
	if claim.Status != AgentClaimActionable || claim.Handling.ID() != target.ID() || claim.Run.ID().IsZero() {
		t.Fatalf("target preclaim = %#v", claim)
	}
	attachment, err := staged.Publish(claim.Run.ID())
	if err != nil {
		t.Fatal(err)
	}
	page, err := localapi.ListRunAttachmentCandidates(nodeState)
	if err != nil || len(page.Candidates()) != 1 || page.More() {
		t.Fatalf("filesystem candidate page = (%#v, %v)", page, err)
	}
	filesystem := page.Candidates()[0]
	if filesystem.RunID() != claim.Run.ID() || filesystem.TokenHash() != staged.TokenHash() {
		t.Fatalf("filesystem candidate = %#v", filesystem)
	}
	lease, _ := claim.Run.LeaseUntil()
	reapable, err := fixture.store.ListReapableAgentRunAttachments(context.Background(),
		AgentAttachmentCleanupSpec{ProfileID: fixture.profile.ID(),
			ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(), At: lease,
			Candidates: []AgentRunAttachmentCandidate{{RunID: filesystem.RunID(),
				TokenHash: filesystem.TokenHash()}}})
	if err != nil || len(reapable) != 1 || reapable[0].RunID != claim.Run.ID() {
		t.Fatalf("reapable real attachment = (%#v, %v)", reapable, err)
	}
	if removed, err := localapi.RemoveReapableRunAttachment(nodeState, reapable[0].RunID,
		reapable[0].TokenHash); err != nil || !removed {
		t.Fatalf("remove real attachment = (%t, %v)", removed, err)
	}
	if _, err := os.Lstat(attachment.Path()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("real attachment remains: %v", err)
	}
	var historical int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM agent_runs
		WHERE run_id LIKE 'run-attachment-history-%'`).Scan(&historical); err != nil || historical != 70 {
		t.Fatalf("historical Run count = %d, %v", historical, err)
	}
}

func TestAgentAttachmentRejectsAuthorityAndDurableDrift(t *testing.T) {
	fixture, events := newAgentClaimFixture(t, 1, "attachment-drift")
	at := fixture.now.Add(time.Minute)
	insertClaimHandling(t, fixture.store, "handling-attachment-drift", events[0], 1, at, at, 0)
	token := model.Sum([]byte("attachment-drift"))
	claim := preclaimWake(t, fixture, token, at)
	spec := AgentAttachmentSpec{ProfileID: fixture.profile.ID(),
		ExpectedAssetRevision: "wrong-asset", AttachmentTokenHash: token, At: at}
	if err := fixture.store.PeekAgentRunAttachment(context.Background(), spec); !errors.Is(err, ErrAgentClaimAsset) {
		t.Fatalf("asset drift error = %v", err)
	}
	mustExec(t, fixture.store, `DROP TRIGGER agent_runs_creation_identity_immutable`)
	if _, err := fixture.store.db.Exec(`UPDATE agent_runs SET launcher='external' WHERE run_id=?`,
		claim.Run.ID().String()); err != nil {
		t.Fatal(err)
	}
	spec.ExpectedAssetRevision = fixture.profile.ActiveAssetRevision()
	if err := fixture.store.PeekAgentRunAttachment(context.Background(), spec); !errors.Is(err, ErrAgentAttachmentStale) {
		t.Fatalf("launcher drift error = %v", err)
	}
}

func preclaimWake(t *testing.T, fixture *acceptanceFixture, token model.Digest,
	at time.Time,
) AgentClaimResult {
	t.Helper()
	result, err := fixture.store.PreclaimAgentWake(context.Background(), AgentWakePreclaimSpec{
		ProfileID: fixture.profile.ID(), ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(),
		ClaimOwner: "wake-owner", AttachmentTokenHash: token, At: at,
		LeaseUntil: at.Add(5 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	return result
}
