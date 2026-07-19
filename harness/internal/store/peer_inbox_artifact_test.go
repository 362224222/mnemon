package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestPeerInboxArtifactClaimIsStableExclusiveAndRestartSafe(t *testing.T) {
	t.Parallel()

	t.Run("concurrent workers have one winner", func(t *testing.T) {
		fixture := newPeerInboxFixture(t, "artifact-claim-concurrent", 0)
		fixture.put(t, fixture.publication(t, 1, 1, "artifact-claim-concurrent", true), fixture.at)
		claimAt := fixture.at.Add(time.Second)
		type outcome struct {
			result PeerInboxArtifactClaimResult
			err    error
		}
		start := make(chan struct{})
		outcomes := make(chan outcome, 2)
		for _, owner := range []string{"artifact-concurrent-a", "artifact-concurrent-b"} {
			owner := owner
			go func() {
				<-start
				result, err := fixture.store.ClaimPeerInboxArtifact(context.Background(),
					ClaimPeerInboxArtifactSpec{LeaseOwner: owner, At: claimAt})
				outcomes <- outcome{result: result, err: err}
			}()
		}
		close(start)
		found := 0
		for range 2 {
			outcome := <-outcomes
			if outcome.err != nil {
				t.Fatalf("concurrent claim error = %v", outcome.err)
			}
			if outcome.result.Found() {
				found++
			}
		}
		if found != 1 {
			t.Fatalf("concurrent claim winners = %d, want 1", found)
		}
	})

	t.Run("one winner and expired generation recovery", func(t *testing.T) {
		fixture := newPeerInboxFixture(t, "artifact-claim-exclusive", 0)
		publication := fixture.publication(t, 1, 1, "artifact-claim-exclusive", true)
		put := fixture.put(t, publication, fixture.at)
		claimAt := fixture.at.Add(time.Second)
		first := mustClaimPeerInboxArtifact(t, fixture.store, "artifact-worker-a", claimAt)
		if first.InboxID() != put.InboxID || first.ChannelID() != publication.Event().Scope().ChannelID() ||
			first.OriginPeerID() != publication.Event().Scope().OriginPeerID() ||
			first.OriginEpoch() != publication.Event().Scope().OriginEpoch() || first.Fence().Attempt() != 1 ||
			!first.Fence().LeaseUntil().Equal(claimAt.Add(peerInboxArtifactLease)) {
			t.Fatalf("first claim = %#v", first)
		}
		second, err := fixture.store.ClaimPeerInboxArtifact(context.Background(),
			ClaimPeerInboxArtifactSpec{LeaseOwner: "artifact-worker-b", At: claimAt})
		if err != nil || second.Found() {
			t.Fatalf("concurrent second claim = (found %t,%v)", second.Found(), err)
		}

		path := fixture.store.Path()
		if err := fixture.store.Close(); err != nil {
			t.Fatal(err)
		}
		reopened, err := OpenExisting(context.Background(), path)
		if err != nil {
			t.Fatalf("restart with Artifact lease: %v", err)
		}
		t.Cleanup(func() { _ = reopened.Close() })
		fixture.store = reopened
		recovered := mustClaimPeerInboxArtifact(t, reopened, "artifact-worker-c",
			first.Fence().LeaseUntil())
		if recovered.InboxID() != first.InboxID() || recovered.Fence().Attempt() != 2 ||
			recovered.Fence().LeaseOwner() != "artifact-worker-c" {
			t.Fatalf("recovered claim = %#v", recovered)
		}
		_, err = reopened.RetryPeerInboxArtifact(context.Background(), RetryPeerInboxArtifactSpec{
			Fence: first.Fence(), Diagnostic: PeerInboxArtifactRetryBusy,
			RetryAfter: time.Second, At: first.Fence().LeaseUntil().Add(-time.Nanosecond)})
		if !errors.Is(err, ErrPeerInboxArtifactStale) {
			t.Fatalf("old generation retry error = %v", err)
		}
	})

	t.Run("stable earliest and owner bounds", func(t *testing.T) {
		fixture := newPeerInboxFixture(t, "artifact-claim-order", 0)
		firstPublication := fixture.publication(t, 1, 1, "artifact-order-first", true)
		secondPublication := fixture.publication(t, 2, 2, "artifact-order-second", true)
		firstPut := fixture.put(t, firstPublication, fixture.at)
		fixture.put(t, secondPublication, fixture.at.Add(time.Second))
		claim := mustClaimPeerInboxArtifact(t, fixture.store, "artifact-order-worker",
			fixture.at.Add(2*time.Second))
		if claim.InboxID() != firstPut.InboxID {
			t.Fatalf("claimed Inbox %s, want oldest %s", claim.InboxID(), firstPut.InboxID)
		}
		for _, owner := range []string{"", "has space", strings.Repeat("x", model.MaxIdentifierBytes+1)} {
			if _, err := fixture.store.ClaimPeerInboxArtifact(context.Background(),
				ClaimPeerInboxArtifactSpec{LeaseOwner: owner, At: fixture.at}); !errors.Is(err, ErrPeerInboxArtifactInput) {
				t.Fatalf("owner %q error = %v", owner, err)
			}
		}
	})

	t.Run("attempt overflow fails closed", func(t *testing.T) {
		fixture := newPeerInboxFixture(t, "artifact-claim-overflow", 0)
		fixture.put(t, fixture.publication(t, 1, 1, "artifact-overflow", true), fixture.at)
		mustExec(t, fixture.store, `UPDATE peer_inbox SET attempts=?`, uint64(math.MaxUint32))
		result, err := fixture.store.ClaimPeerInboxArtifact(context.Background(),
			ClaimPeerInboxArtifactSpec{LeaseOwner: "artifact-overflow-worker", At: fixture.at.Add(time.Second)})
		if !errors.Is(err, ErrPeerInboxArtifactInvariant) || result.Found() {
			t.Fatalf("overflow claim = (found %t,%v)", result.Found(), err)
		}
		assertPeerInboxArtifactState(t, fixture.store, "stored", math.MaxUint32, "", false)
	})
}

func TestPeerInboxArtifactClaimSkipsUnavailableChannelAuthority(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, peerInboxFixture, time.Time)
	}{
		{name: "topic not joined", mutate: func(t *testing.T, fixture peerInboxFixture, _ time.Time) {
			mustExec(t, fixture.store, `UPDATE channels SET topic_state='not_joined'
				WHERE channel_id=?`, fixture.channel.Channel().ID().String())
		}},
		{name: "authority newer than trusted time", mutate: func(t *testing.T, fixture peerInboxFixture, at time.Time) {
			mustExec(t, fixture.store, `UPDATE channels SET updated_at=? WHERE channel_id=?`,
				storeTime(at.Add(time.Hour)), fixture.channel.Channel().ID().String())
		}},
		{name: "origin revoked", mutate: func(t *testing.T, fixture peerInboxFixture, at time.Time) {
			terminal := fixture.channel.AppendTerminal(t, fixture.remote.Identity().PeerID(), model.MemberRevoked)
			result, err := fixture.store.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
				ChannelID:                    fixture.channel.Channel().ID(),
				AuthenticatedTransportPeerID: fixture.remote.Identity().PeerID(),
				Records:                      []model.Member{terminal.Member()}, At: at})
			if err != nil || result.Status != ChannelRosterApplied {
				t.Fatalf("revoke origin = (%#v,%v)", result, err)
			}
		}},
		{name: "origin quarantine", mutate: func(t *testing.T, fixture peerInboxFixture, at time.Time) {
			challenger := fixture.publication(t, 1, 1, "artifact-skip-quarantine-challenger", true)
			result := fixture.put(t, challenger, at)
			if result.Disposition != PeerInboxConflicted {
				t.Fatalf("origin challenger disposition = %s", result.Disposition)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			blocked := newPeerInboxFixture(t, "artifact-skip-"+strings.ReplaceAll(test.name, " ", "-"), 0)
			blocked.put(t, blocked.publication(t, 1, 1, "artifact-skip-old", true), blocked.at)
			healthy := addPeerInboxArtifactChannel(t, blocked, "artifact-skip-healthy-"+test.name)
			healthyPut := healthy.put(t, healthy.publication(t, 1, 1, "artifact-skip-healthy", true),
				healthy.at)
			mutationAt := healthy.at.Add(time.Second)
			test.mutate(t, blocked, mutationAt)
			claim := mustClaimPeerInboxArtifact(t, blocked.store, "artifact-skip-worker",
				mutationAt.Add(time.Second))
			if claim.InboxID() != healthyPut.InboxID || claim.ChannelID() != healthy.channel.Channel().ID() {
				t.Fatalf("claim selected blocked Channel: Inbox=%s Channel=%s", claim.InboxID(), claim.ChannelID())
			}
		})
	}
}

func TestProbePeerInboxArtifactAuthorityIsFenceBoundAndChannelScoped(t *testing.T) {
	t.Parallel()

	t.Run("active authority is read only", func(t *testing.T) {
		fixture := newPeerInboxFixture(t, "artifact-authority-probe-active", 0)
		fixture.put(t, fixture.publication(t, 1, 1, "artifact-authority-probe-active", true), fixture.at)
		claimAt := fixture.at.Add(time.Second)
		claim := mustClaimPeerInboxArtifact(t, fixture.store, "artifact-authority-probe-active-worker", claimAt)
		var beforeLease, beforeUpdated string
		if err := fixture.store.db.QueryRow(`SELECT lease_until,updated_at FROM peer_inbox
			WHERE inbox_id=?`, claim.InboxID().String()).Scan(&beforeLease, &beforeUpdated); err != nil {
			t.Fatal(err)
		}
		if err := fixture.store.ProbePeerInboxArtifactAuthority(context.Background(),
			ProbePeerInboxArtifactAuthoritySpec{Fence: claim.Fence(), At: claimAt.Add(time.Second)}); err != nil {
			t.Fatalf("active authority probe error = %v", err)
		}
		var afterLease, afterUpdated string
		if err := fixture.store.db.QueryRow(`SELECT lease_until,updated_at FROM peer_inbox
			WHERE inbox_id=?`, claim.InboxID().String()).Scan(&afterLease, &afterUpdated); err != nil {
			t.Fatal(err)
		}
		if afterLease != beforeLease || afterUpdated != beforeUpdated {
			t.Fatalf("authority probe mutated lease: before=(%q,%q) after=(%q,%q)",
				beforeLease, beforeUpdated, afterLease, afterUpdated)
		}
	})

	for _, test := range []struct {
		name   string
		mutate func(*testing.T, peerInboxFixture, time.Time)
	}{
		{name: "Channel topic lost", mutate: func(t *testing.T, fixture peerInboxFixture, _ time.Time) {
			mustExec(t, fixture.store, `UPDATE channels SET topic_state='not_joined'
				WHERE channel_id=?`, fixture.channel.Channel().ID().String())
		}},
		{name: "origin binding revoked", mutate: func(t *testing.T, fixture peerInboxFixture, at time.Time) {
			terminal := fixture.channel.AppendTerminal(t, fixture.remote.Identity().PeerID(), model.MemberRevoked)
			result, err := fixture.store.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
				ChannelID:                    fixture.channel.Channel().ID(),
				AuthenticatedTransportPeerID: fixture.remote.Identity().PeerID(),
				Records:                      []model.Member{terminal.Member()}, At: at})
			if err != nil || result.Status != ChannelRosterApplied {
				t.Fatalf("revoke origin = (%#v,%v)", result, err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newPeerInboxFixture(t, "artifact-authority-probe-"+strings.ReplaceAll(test.name, " ", "-"), 0)
			fixture.put(t, fixture.publication(t, 1, 1, "artifact-authority-probe-loss", true), fixture.at)
			claimAt := fixture.at.Add(time.Second)
			claim := mustClaimPeerInboxArtifact(t, fixture.store, "artifact-authority-probe-loss-worker", claimAt)
			mutationAt := claimAt.Add(time.Second)
			test.mutate(t, fixture, mutationAt)
			if err := fixture.store.ProbePeerInboxArtifactAuthority(context.Background(),
				ProbePeerInboxArtifactAuthoritySpec{Fence: claim.Fence(), At: mutationAt}); !errors.Is(err, ErrPeerInboxArtifactAuthority) {
				t.Fatalf("authority loss probe error = %v", err)
			}
		})
	}

	t.Run("expired and changed fences are stale", func(t *testing.T) {
		fixture := newPeerInboxFixture(t, "artifact-authority-probe-stale", 0)
		fixture.put(t, fixture.publication(t, 1, 1, "artifact-authority-probe-stale", true), fixture.at)
		claimAt := fixture.at.Add(time.Second)
		claim := mustClaimPeerInboxArtifact(t, fixture.store, "artifact-authority-probe-stale-worker", claimAt)
		renewAt := claimAt.Add(time.Minute)
		renewed, err := fixture.store.RenewPeerInboxArtifactLease(context.Background(),
			RenewPeerInboxArtifactSpec{Fence: claim.Fence(), At: renewAt})
		if err != nil || !renewed.Changed() {
			t.Fatalf("renew before changed-fence probe = (%#v,%v)", renewed, err)
		}
		if err := fixture.store.ProbePeerInboxArtifactAuthority(context.Background(),
			ProbePeerInboxArtifactAuthoritySpec{Fence: claim.Fence(), At: renewAt.Add(time.Second)}); !errors.Is(err, ErrPeerInboxArtifactStale) {
			t.Fatalf("changed fence probe error = %v", err)
		}
		if err := fixture.store.ProbePeerInboxArtifactAuthority(context.Background(),
			ProbePeerInboxArtifactAuthoritySpec{Fence: renewed.Fence(), At: renewAt.Add(time.Second)}); err != nil {
			t.Fatalf("renewed fence probe error = %v", err)
		}
		if err := fixture.store.ProbePeerInboxArtifactAuthority(context.Background(),
			ProbePeerInboxArtifactAuthoritySpec{Fence: renewed.Fence(), At: renewed.Fence().LeaseUntil()}); !errors.Is(err, ErrPeerInboxArtifactStale) {
			t.Fatalf("expired fence probe error = %v", err)
		}
	})

	t.Run("overlapping Channel authority loss does not bleed", func(t *testing.T) {
		fixture := newPeerInboxFixture(t, "artifact-authority-probe-overlap", 0)
		fixture.put(t, fixture.publication(t, 1, 1, "artifact-authority-probe-overlap", true), fixture.at)
		claimAt := fixture.at.Add(time.Second)
		claim := mustClaimPeerInboxArtifact(t, fixture.store, "artifact-authority-probe-overlap-worker", claimAt)

		overlap := testkit.NewSignedChannelForOwnerAt(t, "artifact-authority-probe-other-channel",
			fixture.channel.Owner(), fixture.at)
		overlapRemote := overlap.AppendActiveIdentity(t, fixture.remote.Identity())
		insertSignedChannelFixture(t, fixture.store.db, overlap, model.TopicJoined)
		insertSignedPeerBinding(t, fixture.store.db, overlap.Channel().ID(), overlapRemote,
			"artifact-authority-probe-shared-peer", model.BindingPending,
			model.ReachabilityUnknown, overlapRemote.Member().CreatedAt())
		mustExec(t, fixture.store, `UPDATE channels SET topic_state='not_joined'
			WHERE channel_id=?`, overlap.Channel().ID().String())
		if err := fixture.store.ProbePeerInboxArtifactAuthority(context.Background(),
			ProbePeerInboxArtifactAuthoritySpec{Fence: claim.Fence(), At: claimAt.Add(time.Second)}); err != nil {
			t.Fatalf("probe after overlapping Channel loss error = %v", err)
		}
	})
}

func TestRenewPeerInboxArtifactLeaseRevalidatesAuthority(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "artifact-renew-authority", 0)
	fixture.put(t, fixture.publication(t, 1, 1, "artifact-renew-authority", true), fixture.at)
	claimAt := fixture.at.Add(time.Second)
	claim := mustClaimPeerInboxArtifact(t, fixture.store, "artifact-renew-authority-worker", claimAt)
	revokeAt := claimAt.Add(time.Second)
	terminal := fixture.channel.AppendTerminal(t, fixture.remote.Identity().PeerID(), model.MemberRevoked)
	merged, err := fixture.store.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
		ChannelID:                    fixture.channel.Channel().ID(),
		AuthenticatedTransportPeerID: fixture.remote.Identity().PeerID(),
		Records:                      []model.Member{terminal.Member()}, At: revokeAt})
	if err != nil || merged.Status != ChannelRosterApplied {
		t.Fatalf("revoke before renewal = (%#v,%v)", merged, err)
	}
	result, err := fixture.store.RenewPeerInboxArtifactLease(context.Background(),
		RenewPeerInboxArtifactSpec{Fence: claim.Fence(), At: revokeAt.Add(time.Second)})
	if !errors.Is(err, ErrPeerInboxArtifactAuthority) || result.Changed() || result.Replayed() {
		t.Fatalf("renew after authority loss = (%#v,%v)", result, err)
	}
	assertPeerInboxArtifactState(t, fixture.store, "waiting_artifact", 1,
		"artifact-renew-authority-worker", true)
	var leaseUntil string
	if err := fixture.store.db.QueryRow(`SELECT lease_until FROM peer_inbox
		WHERE inbox_id=?`, claim.InboxID().String()).Scan(&leaseUntil); err != nil {
		t.Fatal(err)
	}
	if leaseUntil != storeTime(claim.Fence().LeaseUntil()) {
		t.Fatalf("failed authority renewal changed lease_until = %q, want %q",
			leaseUntil, storeTime(claim.Fence().LeaseUntil()))
	}
}

func TestPeerInboxArtifactRenewRetryAndFenceReplay(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "artifact-renew-retry", 0)
	fixture.put(t, fixture.publication(t, 1, 1, "artifact-renew-retry", true), fixture.at)
	claimAt := fixture.at.Add(time.Second)
	claim := mustClaimPeerInboxArtifact(t, fixture.store, "artifact-renew-worker", claimAt)
	renewAt := claimAt.Add(time.Minute)
	renewed, err := fixture.store.RenewPeerInboxArtifactLease(context.Background(),
		RenewPeerInboxArtifactSpec{Fence: claim.Fence(), At: renewAt})
	if err != nil || !renewed.Changed() || renewed.Replayed() ||
		!renewed.Fence().LeaseUntil().Equal(renewAt.Add(peerInboxArtifactLease)) {
		t.Fatalf("renew = (%#v,%v)", renewed, err)
	}
	replayRenew, err := fixture.store.RenewPeerInboxArtifactLease(context.Background(),
		RenewPeerInboxArtifactSpec{Fence: claim.Fence(), At: renewAt})
	if err != nil || replayRenew.Changed() || !replayRenew.Replayed() ||
		replayRenew.Fence().LeaseUntil() != renewed.Fence().LeaseUntil() {
		t.Fatalf("renew response-loss replay = (%#v,%v)", replayRenew, err)
	}
	if _, err := fixture.store.RetryPeerInboxArtifact(context.Background(), RetryPeerInboxArtifactSpec{
		Fence: claim.Fence(), Diagnostic: PeerInboxArtifactRetryBusy,
		RetryAfter: time.Second, At: renewAt.Add(time.Second)}); !errors.Is(err, ErrPeerInboxArtifactStale) {
		t.Fatalf("pre-renew fence error = %v", err)
	}

	retryAt := renewAt.Add(time.Second)
	retrySpec := RetryPeerInboxArtifactSpec{Fence: renewed.Fence(),
		Diagnostic: PeerInboxArtifactRetryTransportUnavailable,
		RetryAfter: maxArtifactRetry, At: retryAt}
	retried, err := fixture.store.RetryPeerInboxArtifact(context.Background(), retrySpec)
	if err != nil || !retried.Changed() || retried.Replayed() || retried.Status() != model.InboxRetry ||
		!retried.NextAttemptAt().Equal(retryAt.Add(maxArtifactRetry)) {
		t.Fatalf("retry = (%#v,%v)", retried, err)
	}
	replayed, err := fixture.store.RetryPeerInboxArtifact(context.Background(), retrySpec)
	if err != nil || replayed.Changed() || !replayed.Replayed() {
		t.Fatalf("retry response-loss replay = (%#v,%v)", replayed, err)
	}
	before, err := fixture.store.ClaimPeerInboxArtifact(context.Background(),
		ClaimPeerInboxArtifactSpec{LeaseOwner: "artifact-before-backoff", At: retried.NextAttemptAt().Add(-time.Nanosecond)})
	if err != nil || before.Found() {
		t.Fatalf("claim before retry deadline = (found %t,%v)", before.Found(), err)
	}
	reclaimed := mustClaimPeerInboxArtifact(t, fixture.store, "artifact-after-backoff", retried.NextAttemptAt())
	if reclaimed.Fence().Attempt() != claim.Fence().Attempt()+1 {
		t.Fatalf("retry reclaim attempt = %d", reclaimed.Fence().Attempt())
	}
	for _, invalid := range []RetryPeerInboxArtifactSpec{
		{Fence: reclaimed.Fence(), Diagnostic: "artifact_future", RetryAfter: time.Second, At: retried.NextAttemptAt()},
		{Fence: reclaimed.Fence(), Diagnostic: PeerInboxArtifactRetryBusy, RetryAfter: time.Second - 1, At: retried.NextAttemptAt()},
		{Fence: reclaimed.Fence(), Diagnostic: PeerInboxArtifactRetryBusy, RetryAfter: maxArtifactRetry + 1, At: retried.NextAttemptAt()},
	} {
		if _, err := fixture.store.RetryPeerInboxArtifact(context.Background(), invalid); !errors.Is(err, ErrPeerInboxArtifactInput) {
			t.Fatalf("invalid retry %#v error = %v", invalid, err)
		}
	}
}

func TestRenewPeerInboxArtifactLeaseExtendsDurableStageOwnership(t *testing.T) {
	t.Parallel()
	fixture, claim, _, closure := newPeerInboxArtifactClosureClaim(t,
		"artifact-renew-stage-ownership", false)
	stageAt := fixture.at.Add(2 * time.Second)
	if _, err := fixture.store.StagePeerInboxArtifactClosure(context.Background(),
		StagePeerInboxArtifactClosureSpec{Fence: claim.Fence(), Closure: closure,
			At: stageAt}); err != nil {
		t.Fatal(err)
	}
	initialExpiry := claim.Fence().LeaseUntil().Add(peerInboxArtifactStageTTL)
	assertPeerInboxArtifactStagePins(t, fixture.store, claim.InboxID(),
		claim.RequiredArtifactRoots(), initialExpiry)

	fence := claim.Fence()
	replayFence := fence
	renewAt := stageAt
	for !renewAt.After(initialExpiry) {
		renewAt = fence.LeaseUntil().Add(-time.Second)
		replayFence = fence
		renewed, err := fixture.store.RenewPeerInboxArtifactLease(context.Background(),
			RenewPeerInboxArtifactSpec{Fence: fence, At: renewAt})
		if err != nil || !renewed.Changed() || renewed.Replayed() {
			t.Fatalf("renew at %s = (%#v,%v)", renewAt, renewed, err)
		}
		fence = renewed.Fence()
	}
	wantExpiry := fence.LeaseUntil().Add(peerInboxArtifactStageTTL)
	assertPeerInboxArtifactStagePins(t, fixture.store, claim.InboxID(),
		claim.RequiredArtifactRoots(), wantExpiry)

	replayed, err := fixture.store.RenewPeerInboxArtifactLease(context.Background(),
		RenewPeerInboxArtifactSpec{Fence: replayFence, At: renewAt})
	if err != nil || replayed.Changed() || !replayed.Replayed() || replayed.Fence() != fence {
		t.Fatalf("final renewal response-loss replay = (%#v,%v)", replayed, err)
	}
	assertPeerInboxArtifactStagePins(t, fixture.store, claim.InboxID(),
		claim.RequiredArtifactRoots(), wantExpiry)

	readyAt := renewAt.Add(time.Second)
	ready, err := fixture.store.MarkPeerInboxArtifactReady(context.Background(),
		MarkPeerInboxArtifactReadySpec{Fence: fence, At: readyAt})
	if err != nil || !ready.Changed() || ready.Status() != model.InboxReady {
		t.Fatalf("ready after initial stage TTL = (%#v,%v)", ready, err)
	}
}

func TestRenewPeerInboxArtifactReplayConvergesAfterLaterStage(t *testing.T) {
	t.Parallel()
	fixture, claim, _, closure := newPeerInboxArtifactClosureClaim(t,
		"artifact-renew-before-stage", false)
	renewAt := claim.Fence().LeaseUntil().Add(-time.Minute)
	renewed, err := fixture.store.RenewPeerInboxArtifactLease(context.Background(),
		RenewPeerInboxArtifactSpec{Fence: claim.Fence(), At: renewAt})
	if err != nil || !renewed.Changed() {
		t.Fatalf("renew before stage = (%#v,%v)", renewed, err)
	}
	stageAt := renewAt.Add(time.Second)
	if _, err := fixture.store.StagePeerInboxArtifactClosure(context.Background(),
		StagePeerInboxArtifactClosureSpec{Fence: renewed.Fence(), Closure: closure,
			At: stageAt}); err != nil {
		t.Fatal(err)
	}
	wantExpiry := renewed.Fence().LeaseUntil().Add(peerInboxArtifactStageTTL)
	assertPeerInboxArtifactStagePins(t, fixture.store, claim.InboxID(),
		claim.RequiredArtifactRoots(), wantExpiry)
	assertPeerInboxArtifactRenewReceipt(t, fixture.store, claim.InboxID(),
		claim.Fence(), renewed.Fence(), renewAt)

	replayed, err := fixture.store.RenewPeerInboxArtifactLease(context.Background(),
		RenewPeerInboxArtifactSpec{Fence: claim.Fence(), At: renewAt})
	if err != nil || replayed.Changed() || !replayed.Replayed() ||
		replayed.Fence() != renewed.Fence() {
		t.Fatalf("renew replay after later stage = (%#v,%v)", replayed, err)
	}
}

func TestPeerInboxArtifactRenewReceiptOnlyLatestFenceReplaysAcrossRestart(t *testing.T) {
	fixture := newPeerInboxFixture(t, "artifact-renew-receipt-restart", 0)
	fixture.put(t, fixture.publication(t, 1, 1, "artifact-renew-receipt-restart", true),
		fixture.at)
	claimAt := fixture.at.Add(time.Second)
	claim := mustClaimPeerInboxArtifact(t, fixture.store, "artifact-renew-receipt-worker", claimAt)
	firstAt := claimAt.Add(time.Second)
	firstSpec := RenewPeerInboxArtifactSpec{Fence: claim.Fence(), At: firstAt}
	first, err := fixture.store.RenewPeerInboxArtifactLease(context.Background(), firstSpec)
	if err != nil || !first.Changed() {
		t.Fatalf("F0 -> F1 renew = (%#v,%v)", first, err)
	}
	secondAt := firstAt.Add(time.Second)
	secondSpec := RenewPeerInboxArtifactSpec{Fence: first.Fence(), At: secondAt}
	second, err := fixture.store.RenewPeerInboxArtifactLease(context.Background(), secondSpec)
	if err != nil || !second.Changed() {
		t.Fatalf("F1 -> F2 renew = (%#v,%v)", second, err)
	}
	if _, err := fixture.store.RenewPeerInboxArtifactLease(context.Background(), firstSpec); !errors.Is(err, ErrPeerInboxArtifactStale) {
		t.Fatalf("superseded F0 request error = %v", err)
	}
	if _, err := fixture.store.RenewPeerInboxArtifactLease(context.Background(),
		RenewPeerInboxArtifactSpec{Fence: claim.Fence(), At: secondAt}); !errors.Is(err, ErrPeerInboxArtifactStale) {
		t.Fatalf("F0 masquerading as F1 -> F2 error = %v", err)
	}
	assertPeerInboxArtifactRenewReceipt(t, fixture.store, claim.InboxID(),
		first.Fence(), second.Fence(), secondAt)

	path := fixture.store.Path()
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenExisting(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	fixture.store = restarted
	replayed, err := restarted.RenewPeerInboxArtifactLease(context.Background(), secondSpec)
	if err != nil || replayed.Changed() || !replayed.Replayed() || replayed.Fence() != second.Fence() {
		t.Fatalf("latest renew restart replay = (%#v,%v)", replayed, err)
	}
}

func TestPeerInboxArtifactRenewReceiptIsClearedBySupersedingTransitions(t *testing.T) {
	t.Parallel()
	t.Run("claim", func(t *testing.T) {
		fixture, claim, renewed, renewAt := newRenewedPeerInboxArtifact(t, "artifact-renew-clear-claim")
		reclaimed := mustClaimPeerInboxArtifact(t, fixture.store, "artifact-renew-clear-reclaimed",
			renewed.Fence().LeaseUntil())
		if reclaimed.Fence().Attempt() != claim.Fence().Attempt()+1 {
			t.Fatalf("reclaimed attempt = %d", reclaimed.Fence().Attempt())
		}
		assertPeerInboxArtifactRenewReceiptCount(t, fixture.store, claim.InboxID(), 0)
		_ = renewAt
	})
	t.Run("retry", func(t *testing.T) {
		fixture, claim, renewed, renewAt := newRenewedPeerInboxArtifact(t, "artifact-renew-clear-retry")
		result, err := fixture.store.RetryPeerInboxArtifact(context.Background(), RetryPeerInboxArtifactSpec{
			Fence: renewed.Fence(), Diagnostic: PeerInboxArtifactRetryBusy,
			RetryAfter: time.Second, At: renewAt.Add(time.Second)})
		if err != nil || !result.Changed() {
			t.Fatalf("retry after renew = (%#v,%v)", result, err)
		}
		assertPeerInboxArtifactRenewReceiptCount(t, fixture.store, claim.InboxID(), 0)
	})
	t.Run("quarantine", func(t *testing.T) {
		fixture, claim, renewed, renewAt := newRenewedPeerInboxArtifact(t, "artifact-renew-clear-quarantine")
		result, err := fixture.store.QuarantinePeerInboxArtifact(context.Background(),
			QuarantinePeerInboxArtifactSpec{Fence: renewed.Fence(),
				Diagnostic: PeerInboxArtifactDigestMismatch, At: renewAt.Add(time.Second)})
		if err != nil || !result.Changed() {
			t.Fatalf("quarantine after renew = (%#v,%v)", result, err)
		}
		assertPeerInboxArtifactRenewReceiptCount(t, fixture.store, claim.InboxID(), 0)
	})
	t.Run("ready", func(t *testing.T) {
		fixture, claim, renewed, renewAt := newRenewedPeerInboxArtifact(t, "artifact-renew-clear-ready")
		result, err := fixture.store.MarkPeerInboxArtifactReady(context.Background(),
			MarkPeerInboxArtifactReadySpec{Fence: renewed.Fence(), At: renewAt.Add(time.Second)})
		if err != nil || !result.Changed() {
			t.Fatalf("ready after renew = (%#v,%v)", result, err)
		}
		assertPeerInboxArtifactRenewReceiptCount(t, fixture.store, claim.InboxID(), 0)
	})
}

func TestPeerInboxArtifactRenewReceiptTamperFailsClosedWithoutLaundering(t *testing.T) {
	t.Parallel()
	t.Run("current output mismatch blocks renew and claim", func(t *testing.T) {
		fixture, claim, renewed, renewAt := newRenewedPeerInboxArtifact(t,
			"artifact-renew-output-tamper")
		tamperedNext := renewAt.Add(17 * time.Second)
		mustExec(t, fixture.store, `UPDATE peer_inbox SET next_attempt_at=? WHERE inbox_id=?`,
			storeTime(tamperedNext), claim.InboxID().String())
		_, err := fixture.store.RenewPeerInboxArtifactLease(context.Background(),
			RenewPeerInboxArtifactSpec{Fence: renewed.Fence(), At: renewAt.Add(time.Second)})
		if !errors.Is(err, ErrPeerInboxArtifactInvariant) {
			t.Fatalf("nonmatching renew laundered output mismatch: %v", err)
		}
		assertPeerInboxArtifactRenewReceipt(t, fixture.store, claim.InboxID(),
			claim.Fence(), renewed.Fence(), renewAt)
		result, err := fixture.store.ClaimPeerInboxArtifact(context.Background(),
			ClaimPeerInboxArtifactSpec{LeaseOwner: "artifact-renew-tamper-reclaim",
				At: renewed.Fence().LeaseUntil()})
		if !errors.Is(err, ErrPeerInboxArtifactInvariant) || result.Found() {
			t.Fatalf("claim laundered output mismatch = (found %t,%v)", result.Found(), err)
		}
		assertPeerInboxArtifactRenewReceiptCount(t, fixture.store, claim.InboxID(), 1)
		assertPeerInboxArtifactState(t, fixture.store, "waiting_artifact", 1,
			renewed.Fence().LeaseOwner(), true)
	})
	t.Run("request digest blocks read boundaries", func(t *testing.T) {
		fixture, claim, renewed, renewAt := newRenewedPeerInboxArtifact(t,
			"artifact-renew-digest-tamper")
		mustExec(t, fixture.store, `UPDATE peer_inbox_artifact_renew_receipts
			SET request_digest=? WHERE inbox_id=?`, model.Sum([]byte("tampered Artifact renew")).Bytes(),
			claim.InboxID().String())
		err := fixture.store.ProbePeerInboxArtifactAuthority(context.Background(),
			ProbePeerInboxArtifactAuthoritySpec{Fence: renewed.Fence(), At: renewAt.Add(time.Second)})
		if !errors.Is(err, ErrPeerInboxArtifactInvariant) {
			t.Fatalf("tampered receipt probe error = %v", err)
		}
	})
}

func TestPeerInboxArtifactRenewReceiptWriteRollbackPreservesOldFence(t *testing.T) {
	t.Parallel()
	t.Run("insert", func(t *testing.T) {
		fixture := newPeerInboxFixture(t, "artifact-renew-receipt-insert-rollback", 0)
		fixture.put(t, fixture.publication(t, 1, 1,
			"artifact-renew-receipt-insert-rollback", true), fixture.at)
		claimAt := fixture.at.Add(time.Second)
		claim := mustClaimPeerInboxArtifact(t, fixture.store,
			"artifact-renew-insert-rollback-worker", claimAt)
		mustExec(t, fixture.store, `CREATE TEMP TRIGGER reject_artifact_renew_receipt_insert
			BEFORE INSERT ON peer_inbox_artifact_renew_receipts
			BEGIN SELECT RAISE(ABORT,'injected Artifact renew receipt insert failure'); END`)
		renewAt := claimAt.Add(time.Second)
		_, err := fixture.store.RenewPeerInboxArtifactLease(context.Background(),
			RenewPeerInboxArtifactSpec{Fence: claim.Fence(), At: renewAt})
		if !errors.Is(err, ErrPeerInboxArtifactInvariant) {
			t.Fatalf("renew receipt insert failure error = %v", err)
		}
		if err := fixture.store.ProbePeerInboxArtifactAuthority(context.Background(),
			ProbePeerInboxArtifactAuthoritySpec{Fence: claim.Fence(), At: renewAt}); err != nil {
			t.Fatalf("old fence after receipt insert rollback = %v", err)
		}
		assertPeerInboxArtifactRenewReceiptCount(t, fixture.store, claim.InboxID(), 0)
	})

	t.Run("update", func(t *testing.T) {
		fixture, claim, _, closure := newPeerInboxArtifactClosureClaim(t,
			"artifact-renew-receipt-update-rollback", false)
		stageAt := fixture.at.Add(2 * time.Second)
		if _, err := fixture.store.StagePeerInboxArtifactClosure(context.Background(),
			StagePeerInboxArtifactClosureSpec{Fence: claim.Fence(), Closure: closure,
				At: stageAt}); err != nil {
			t.Fatal(err)
		}
		firstAt := stageAt.Add(time.Second)
		first, err := fixture.store.RenewPeerInboxArtifactLease(context.Background(),
			RenewPeerInboxArtifactSpec{Fence: claim.Fence(), At: firstAt})
		if err != nil || !first.Changed() {
			t.Fatalf("first renew before update injection = (%#v,%v)", first, err)
		}
		firstExpiry := first.Fence().LeaseUntil().Add(peerInboxArtifactStageTTL)
		assertPeerInboxArtifactRenewReceipt(t, fixture.store, claim.InboxID(),
			claim.Fence(), first.Fence(), firstAt)
		assertPeerInboxArtifactStagePins(t, fixture.store, claim.InboxID(),
			claim.RequiredArtifactRoots(), firstExpiry)

		mustExec(t, fixture.store, `CREATE TEMP TRIGGER reject_artifact_renew_receipt_update
			BEFORE UPDATE ON peer_inbox_artifact_renew_receipts
			BEGIN SELECT RAISE(ABORT,'injected Artifact renew receipt update failure'); END`)
		secondAt := firstAt.Add(time.Second)
		_, err = fixture.store.RenewPeerInboxArtifactLease(context.Background(),
			RenewPeerInboxArtifactSpec{Fence: first.Fence(), At: secondAt})
		if !errors.Is(err, ErrPeerInboxArtifactInvariant) {
			t.Fatalf("renew receipt update failure error = %v", err)
		}
		if err := fixture.store.ProbePeerInboxArtifactAuthority(context.Background(),
			ProbePeerInboxArtifactAuthoritySpec{Fence: first.Fence(), At: secondAt}); err != nil {
			t.Fatalf("F1 after receipt update rollback = %v", err)
		}
		assertPeerInboxArtifactRenewReceipt(t, fixture.store, claim.InboxID(),
			claim.Fence(), first.Fence(), firstAt)
		assertPeerInboxArtifactStagePins(t, fixture.store, claim.InboxID(),
			claim.RequiredArtifactRoots(), firstExpiry)
		var leaseUntil, updatedAt string
		if err := fixture.store.db.QueryRow(`SELECT lease_until,updated_at FROM peer_inbox
			WHERE inbox_id=?`, claim.InboxID().String()).Scan(&leaseUntil, &updatedAt); err != nil {
			t.Fatal(err)
		}
		if leaseUntil != storeTime(first.Fence().LeaseUntil()) || updatedAt != storeTime(firstAt) {
			t.Fatalf("Inbox advanced after receipt update rollback = (%q,%q)", leaseUntil, updatedAt)
		}
	})
}

func TestPeerInboxArtifactRenewAndRetryHaveOneConcurrentWinner(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "artifact-renew-retry-race", 0)
	fixture.put(t, fixture.publication(t, 1, 1, "artifact-renew-retry-race", true), fixture.at)
	claimAt := fixture.at.Add(time.Second)
	claim := mustClaimPeerInboxArtifact(t, fixture.store, "artifact-renew-retry-race-worker", claimAt)
	at := claimAt.Add(time.Second)
	start := make(chan struct{})
	type outcome struct {
		kind    string
		changed bool
		err     error
	}
	outcomes := make(chan outcome, 2)
	go func() {
		<-start
		result, err := fixture.store.RenewPeerInboxArtifactLease(context.Background(),
			RenewPeerInboxArtifactSpec{Fence: claim.Fence(), At: at})
		outcomes <- outcome{kind: "renew", changed: result.Changed(), err: err}
	}()
	go func() {
		<-start
		result, err := fixture.store.RetryPeerInboxArtifact(context.Background(),
			RetryPeerInboxArtifactSpec{Fence: claim.Fence(), Diagnostic: PeerInboxArtifactRetryBusy,
				RetryAfter: time.Second, At: at})
		outcomes <- outcome{kind: "retry", changed: result.Changed(), err: err}
	}()
	close(start)
	winner := ""
	for range 2 {
		result := <-outcomes
		if result.err == nil && result.changed {
			if winner != "" {
				t.Fatalf("two transition winners: %s and %s", winner, result.kind)
			}
			winner = result.kind
		} else if !errors.Is(result.err, ErrPeerInboxArtifactStale) {
			t.Fatalf("%s loser = (changed %t,%v)", result.kind, result.changed, result.err)
		}
	}
	if winner == "" {
		t.Fatal("renew/retry race has no winner")
	}
	wantReceipts := 0
	if winner == "renew" {
		wantReceipts = 1
	}
	assertPeerInboxArtifactRenewReceiptCount(t, fixture.store, claim.InboxID(), wantReceipts)
}

func TestPeerInboxArtifactTerminalReplayRejectsNextAttemptDrift(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		run  func(*testing.T, peerInboxFixture, PeerInboxArtifactClaim, time.Time)
	}{
		{name: "quarantine", run: func(t *testing.T, fixture peerInboxFixture,
			claim PeerInboxArtifactClaim, at time.Time) {
			spec := QuarantinePeerInboxArtifactSpec{Fence: claim.Fence(),
				Diagnostic: PeerInboxArtifactDigestMismatch, At: at}
			if _, err := fixture.store.QuarantinePeerInboxArtifact(context.Background(), spec); err != nil {
				t.Fatal(err)
			}
			mustExec(t, fixture.store, `UPDATE peer_inbox SET next_attempt_at=? WHERE inbox_id=?`,
				storeTime(at.Add(time.Second)), claim.InboxID().String())
			if _, err := fixture.store.QuarantinePeerInboxArtifact(context.Background(), spec); !errors.Is(err, ErrPeerInboxArtifactStale) {
				t.Fatalf("quarantine replay after next-at drift error = %v", err)
			}
		}},
		{name: "ready", run: func(t *testing.T, fixture peerInboxFixture,
			claim PeerInboxArtifactClaim, at time.Time) {
			spec := MarkPeerInboxArtifactReadySpec{Fence: claim.Fence(), At: at}
			if _, err := fixture.store.MarkPeerInboxArtifactReady(context.Background(), spec); err != nil {
				t.Fatal(err)
			}
			mustExec(t, fixture.store, `UPDATE peer_inbox SET next_attempt_at=? WHERE inbox_id=?`,
				storeTime(at.Add(time.Second)), claim.InboxID().String())
			if _, err := fixture.store.MarkPeerInboxArtifactReady(context.Background(), spec); !errors.Is(err, ErrPeerInboxArtifactStale) {
				t.Fatalf("ready replay after next-at drift error = %v", err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newPeerInboxFixture(t, "artifact-terminal-next-drift-"+test.name, 0)
			fixture.put(t, fixture.publication(t, 1, 1,
				"artifact-terminal-next-drift-"+test.name, true), fixture.at)
			claimAt := fixture.at.Add(time.Second)
			claim := mustClaimPeerInboxArtifact(t, fixture.store,
				"artifact-terminal-next-drift-worker-"+test.name, claimAt)
			test.run(t, fixture, claim, claimAt.Add(time.Second))
		})
	}
}

func TestStagePeerInboxArtifactClosureIsExactDurableAndRestartSafe(t *testing.T) {
	t.Parallel()

	t.Run("response loss restart and staged visibility", func(t *testing.T) {
		fixture, claim, root, closure := newPeerInboxArtifactClosureClaim(t,
			"artifact-stage-restart", false)
		stageAt := fixture.at.Add(2 * time.Second)
		spec := StagePeerInboxArtifactClosureSpec{Fence: claim.Fence(), Closure: closure, At: stageAt}
		staged, err := fixture.store.StagePeerInboxArtifactClosure(context.Background(), spec)
		if err != nil || !staged.Changed() || staged.Replayed() {
			t.Fatalf("first stage = (%#v,%v)", staged, err)
		}
		assertPeerInboxArtifactRootState(t, fixture.store, root.RootDigest, "staged")
		assertPeerInboxArtifactStagePins(t, fixture.store, claim.InboxID(),
			claim.RequiredArtifactRoots(), claim.Fence().LeaseUntil().Add(peerInboxArtifactStageTTL))
		if _, err := fixture.store.GetVerifiedArtifactRoot(context.Background(), root.RootDigest); !errors.Is(err, ErrArtifactUnverified) {
			t.Fatalf("staged root visible as verified: %v", err)
		}
		replay, err := fixture.store.StagePeerInboxArtifactClosure(context.Background(), spec)
		if err != nil || replay.Changed() || !replay.Replayed() {
			t.Fatalf("stage response-loss replay = (%#v,%v)", replay, err)
		}

		path := fixture.store.Path()
		if err := fixture.store.Close(); err != nil {
			t.Fatal(err)
		}
		restarted, err := OpenExisting(context.Background(), path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = restarted.Close() })
		fixture.store = restarted
		restartReplay, err := restarted.StagePeerInboxArtifactClosure(context.Background(), spec)
		if err != nil || restartReplay.Changed() || !restartReplay.Replayed() {
			t.Fatalf("restart stage replay = (%#v,%v)", restartReplay, err)
		}
		checkpoint, found, err := restarted.ReadPeerInboxArtifactRoot(context.Background(),
			ReadPeerInboxArtifactRootSpec{Fence: claim.Fence(), RootDigest: root.RootDigest, At: stageAt})
		if err != nil || !found || checkpoint.State() != PeerInboxArtifactRootStaged ||
			checkpoint.RootDigest() != root.RootDigest {
			t.Fatalf("restart staged checkpoint = (%#v,found %t,%v)", checkpoint, found, err)
		}
	})

	t.Run("later shared timestamps make an older observation stale", func(t *testing.T) {
		fixture, claim, root, closure := newPeerInboxArtifactClosureClaim(t,
			"artifact-stage-concurrent-time", false)
		olderAt := fixture.at.Add(2 * time.Second)
		laterAt := olderAt.Add(2 * time.Second)
		later := VerifiedArtifactClosure{
			Roots:      append([]VerifiedArtifactRoot(nil), closure.Roots...),
			Blocks:     append([]VerifiedArtifactBlock(nil), closure.Blocks...),
			RootBlocks: append([]VerifiedArtifactRootBlock(nil), closure.RootBlocks...),
		}
		for index := range later.Roots {
			later.Roots[index].CreatedAt = laterAt
			later.Roots[index].VerifiedAt = laterAt
		}
		for index := range later.Blocks {
			later.Blocks[index].CreatedAt = laterAt
		}
		if _, err := fixture.store.StagePeerInboxArtifactClosure(context.Background(),
			StagePeerInboxArtifactClosureSpec{Fence: claim.Fence(), Closure: later,
				At: laterAt}); err != nil {
			t.Fatal(err)
		}
		if _, _, err := fixture.store.ReadPeerInboxArtifactRoot(context.Background(),
			ReadPeerInboxArtifactRootSpec{Fence: claim.Fence(), RootDigest: root.RootDigest,
				At: olderAt}); !errors.Is(err, ErrPeerInboxArtifactStale) {
			t.Fatalf("older cached-root observation error = %v", err)
		}
		if _, err := fixture.store.StagePeerInboxArtifactClosure(context.Background(),
			StagePeerInboxArtifactClosureSpec{Fence: claim.Fence(), Closure: closure,
				At: olderAt}); !errors.Is(err, ErrPeerInboxArtifactStale) {
			t.Fatalf("older shared stage error = %v", err)
		}
		if _, err := fixture.store.MarkPeerInboxArtifactReady(context.Background(),
			MarkPeerInboxArtifactReadySpec{Fence: claim.Fence(), At: olderAt}); !errors.Is(err, ErrPeerInboxArtifactStale) {
			t.Fatalf("older ready observation error = %v", err)
		}
		assertPeerInboxArtifactRootState(t, fixture.store, root.RootDigest, "staged")
		assertPeerInboxArtifactState(t, fixture.store, "waiting_artifact", 1,
			"artifact-stage-concurrent-time-worker", true)
		if _, err := fixture.store.MarkPeerInboxArtifactReady(context.Background(),
			MarkPeerInboxArtifactReadySpec{Fence: claim.Fence(), At: laterAt.Add(time.Second)}); err != nil {
			t.Fatalf("fresh ready after stale observation: %v", err)
		}
	})

	t.Run("fence authority exact roots and rollback", func(t *testing.T) {
		fixture, claim, _, closure := newPeerInboxArtifactClosureClaim(t,
			"artifact-stage-fail-closed", false)
		stageAt := fixture.at.Add(2 * time.Second)
		wrong := claim.Fence()
		wrong.attempt++
		if _, err := fixture.store.StagePeerInboxArtifactClosure(context.Background(),
			StagePeerInboxArtifactClosureSpec{Fence: wrong, Closure: closure, At: stageAt}); !errors.Is(err, ErrPeerInboxArtifactStale) {
			t.Fatalf("wrong stage fence error = %v", err)
		}
		other, _, _ := newArtifactSourceClosure(t, "artifact-stage-other-root",
			[]byte("other"), fixture.at)
		if _, err := fixture.store.StagePeerInboxArtifactClosure(context.Background(),
			StagePeerInboxArtifactClosureSpec{Fence: claim.Fence(), Closure: other, At: stageAt}); !errors.Is(err, ErrPeerInboxArtifactInput) {
			t.Fatalf("wrong exact root error = %v", err)
		}
		mustExec(t, fixture.store, `CREATE TRIGGER test_artifact_stage_pin_abort
			BEFORE INSERT ON artifact_pins WHEN NEW.owner_kind='inbox'
			BEGIN SELECT RAISE(ABORT, 'forced stage rollback'); END`)
		if _, err := fixture.store.StagePeerInboxArtifactClosure(context.Background(),
			StagePeerInboxArtifactClosureSpec{Fence: claim.Fence(), Closure: closure, At: stageAt}); !errors.Is(err, ErrPeerInboxArtifactInvariant) {
			t.Fatalf("forced stage rollback error = %v", err)
		}
		for _, table := range []string{"artifact_roots", "artifact_blocks", "artifact_root_blocks", "artifact_pins"} {
			var count int
			if err := fixture.store.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 0 {
				t.Fatalf("%s after stage rollback = (%d,%v)", table, count, err)
			}
		}
	})

	t.Run("authority loss does not install metadata", func(t *testing.T) {
		fixture, claim, _, closure := newPeerInboxArtifactClosureClaim(t,
			"artifact-stage-authority", false)
		stageAt := fixture.at.Add(2 * time.Second)
		mustExec(t, fixture.store, `UPDATE channels SET topic_state='not_joined'
			WHERE channel_id=?`, fixture.channel.Channel().ID().String())
		if _, err := fixture.store.StagePeerInboxArtifactClosure(context.Background(),
			StagePeerInboxArtifactClosureSpec{Fence: claim.Fence(), Closure: closure, At: stageAt}); !errors.Is(err, ErrPeerInboxArtifactAuthority) {
			t.Fatalf("stage after authority loss error = %v", err)
		}
		var count int
		if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM artifact_roots`).Scan(&count); err != nil || count != 0 {
			t.Fatalf("roots after authority loss = (%d,%v)", count, err)
		}
	})

	t.Run("shared roots and blocks retain independent Inbox owners", func(t *testing.T) {
		fixture := newPeerInboxFixture(t, "artifact-stage-shared", 0)
		closureA, rootA, _ := newArtifactSourceClosure(t, "artifact-stage-shared-a",
			[]byte("shared-block"), fixture.at)
		closureB, rootB, _ := newArtifactSourceClosure(t, "artifact-stage-shared-b",
			[]byte("shared-block"), fixture.at)
		closure := combinePeerInboxArtifactClosures(t, closureA, closureB)
		if len(closure.Blocks) != 1 {
			t.Fatalf("combined shared closure blocks = %d, want 1", len(closure.Blocks))
		}
		firstPut := fixture.put(t, peerInboxArtifactPublication(t, fixture, 1, 1,
			"artifact-stage-shared-first", []model.Digest{rootA.RootDigest, rootB.RootDigest}), fixture.at)
		secondPut := fixture.put(t, peerInboxArtifactPublication(t, fixture, 2, 2,
			"artifact-stage-shared-second", []model.Digest{rootB.RootDigest, rootA.RootDigest}),
			fixture.at.Add(time.Second))
		firstAt := fixture.at.Add(2 * time.Second)
		first := mustClaimPeerInboxArtifact(t, fixture.store, "artifact-stage-shared-first", firstAt)
		if _, err := fixture.store.StagePeerInboxArtifactClosure(context.Background(),
			StagePeerInboxArtifactClosureSpec{Fence: first.Fence(), Closure: closure, At: firstAt}); err != nil {
			t.Fatal(err)
		}
		secondAt := firstAt.Add(time.Second)
		second := mustClaimPeerInboxArtifact(t, fixture.store, "artifact-stage-shared-second", secondAt)
		if second.InboxID() != secondPut.InboxID || first.InboxID() != firstPut.InboxID {
			t.Fatalf("shared Inbox claim order = (%s,%s)", first.InboxID(), second.InboxID())
		}
		if _, found, err := fixture.store.ReadPeerInboxArtifactRoot(context.Background(),
			ReadPeerInboxArtifactRootSpec{Fence: second.Fence(), RootDigest: rootA.RootDigest,
				At: secondAt}); err != nil || found {
			t.Fatalf("other Inbox unowned stage = (found %t,%v)", found, err)
		}
		secondStage, err := fixture.store.StagePeerInboxArtifactClosure(context.Background(),
			StagePeerInboxArtifactClosureSpec{Fence: second.Fence(), Closure: closure, At: secondAt})
		if err != nil || !secondStage.Changed() || secondStage.Replayed() {
			t.Fatalf("second shared stage = (%#v,%v)", secondStage, err)
		}
		for table, want := range map[string]int{"artifact_roots": 2, "artifact_blocks": 1,
			"artifact_root_blocks": 2, "artifact_pins": 4} {
			var count int
			if err := fixture.store.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != want {
				t.Fatalf("shared %s count = (%d,%v), want %d", table, count, err, want)
			}
		}
		if _, err := fixture.store.MarkPeerInboxArtifactReady(context.Background(),
			MarkPeerInboxArtifactReadySpec{Fence: first.Fence(), At: secondAt.Add(time.Second)}); err != nil {
			t.Fatal(err)
		}
		assertPeerInboxArtifactStagePins(t, fixture.store, second.InboxID(),
			second.RequiredArtifactRoots(), second.Fence().LeaseUntil().Add(peerInboxArtifactStageTTL))
		if _, err := fixture.store.MarkPeerInboxArtifactReady(context.Background(),
			MarkPeerInboxArtifactReadySpec{Fence: second.Fence(), At: secondAt.Add(2 * time.Second)}); err != nil {
			t.Fatal(err)
		}
		assertPeerInboxArtifactPins(t, fixture.store, first.InboxID(), first.RequiredArtifactRoots())
		assertPeerInboxArtifactPins(t, fixture.store, second.InboxID(), second.RequiredArtifactRoots())
		assertPeerInboxArtifactRootState(t, fixture.store, rootA.RootDigest, "verified")
		assertPeerInboxArtifactRootState(t, fixture.store, rootB.RootDigest, "verified")
	})
}

func TestPeerInboxArtifactStagePinsSurviveRetryAndBoundQuarantine(t *testing.T) {
	t.Parallel()

	t.Run("retry extends exact stage ownership", func(t *testing.T) {
		fixture, claim, _, closure := newPeerInboxArtifactClosureClaim(t,
			"artifact-stage-retry", false)
		stageAt := fixture.at.Add(2 * time.Second)
		if _, err := fixture.store.StagePeerInboxArtifactClosure(context.Background(),
			StagePeerInboxArtifactClosureSpec{Fence: claim.Fence(), Closure: closure, At: stageAt}); err != nil {
			t.Fatal(err)
		}
		retryAt := claim.Fence().LeaseUntil().Add(-time.Second)
		spec := RetryPeerInboxArtifactSpec{Fence: claim.Fence(),
			Diagnostic: PeerInboxArtifactRetryTimeout, RetryAfter: 17 * time.Second, At: retryAt}
		result, err := fixture.store.RetryPeerInboxArtifact(context.Background(), spec)
		if err != nil || !result.Changed() || result.Replayed() {
			t.Fatalf("retry staged closure = (%#v,%v)", result, err)
		}
		wantExpiry := result.NextAttemptAt().Add(peerInboxArtifactStageTTL)
		assertPeerInboxArtifactStagePins(t, fixture.store, claim.InboxID(),
			claim.RequiredArtifactRoots(), wantExpiry)
		replay, err := fixture.store.RetryPeerInboxArtifact(context.Background(), spec)
		if err != nil || replay.Changed() || !replay.Replayed() {
			t.Fatalf("staged retry replay = (%#v,%v)", replay, err)
		}
		reclaimed := mustClaimPeerInboxArtifact(t, fixture.store,
			"artifact-stage-retry-reclaimed", result.NextAttemptAt())
		refreshAt := result.NextAttemptAt().Add(time.Second)
		refreshed, err := fixture.store.StagePeerInboxArtifactClosure(context.Background(),
			StagePeerInboxArtifactClosureSpec{Fence: reclaimed.Fence(), Closure: closure, At: refreshAt})
		if err != nil || !refreshed.Changed() || refreshed.Replayed() {
			t.Fatalf("reclaimed stage refresh = (%#v,%v)", refreshed, err)
		}
		assertPeerInboxArtifactStagePins(t, fixture.store, claim.InboxID(),
			claim.RequiredArtifactRoots(), reclaimed.Fence().LeaseUntil().Add(peerInboxArtifactStageTTL))
	})

	t.Run("quarantine retains one bounded cleanup window", func(t *testing.T) {
		fixture, claim, _, closure := newPeerInboxArtifactClosureClaim(t,
			"artifact-stage-quarantine", false)
		stageAt := fixture.at.Add(2 * time.Second)
		if _, err := fixture.store.StagePeerInboxArtifactClosure(context.Background(),
			StagePeerInboxArtifactClosureSpec{Fence: claim.Fence(), Closure: closure, At: stageAt}); err != nil {
			t.Fatal(err)
		}
		quarantineAt := stageAt.Add(time.Second)
		spec := QuarantinePeerInboxArtifactSpec{Fence: claim.Fence(),
			Diagnostic: PeerInboxArtifactDigestMismatch, At: quarantineAt}
		result, err := fixture.store.QuarantinePeerInboxArtifact(context.Background(), spec)
		if err != nil || !result.Changed() || result.Replayed() {
			t.Fatalf("quarantine staged closure = (%#v,%v)", result, err)
		}
		assertPeerInboxArtifactStagePins(t, fixture.store, claim.InboxID(),
			claim.RequiredArtifactRoots(), claim.Fence().LeaseUntil().Add(peerInboxArtifactStageTTL))
		replay, err := fixture.store.QuarantinePeerInboxArtifact(context.Background(), spec)
		if err != nil || replay.Changed() || !replay.Replayed() {
			t.Fatalf("staged quarantine replay = (%#v,%v)", replay, err)
		}
	})
}

func TestPeerInboxArtifactReadyRequiresSealedClosureAndOwnsPins(t *testing.T) {
	t.Parallel()

	t.Run("cached multi-root ready and response-loss replay", func(t *testing.T) {
		fixture := newPeerInboxFixture(t, "artifact-ready-multi", 0)
		closureA, rootA, _ := newArtifactSourceClosure(t, "artifact-ready-a", []byte("ready-a"),
			fixture.at.Add(-2*time.Second))
		closureB, rootB, _ := newArtifactSourceClosure(t, "artifact-ready-b", []byte("ready-b"),
			fixture.at.Add(-2*time.Second))
		for _, closure := range []VerifiedArtifactClosure{closureA, closureB} {
			if _, err := fixture.store.CheckpointVerifiedArtifactClosure(context.Background(), closure); err != nil {
				t.Fatal(err)
			}
		}
		closure := combinePeerInboxArtifactClosures(t, closureA, closureB)
		publication := peerInboxArtifactPublication(t, fixture, 1, 1, "artifact-ready-multi",
			[]model.Digest{rootB.RootDigest, rootA.RootDigest})
		put := fixture.put(t, publication, fixture.at)
		claimAt := fixture.at.Add(time.Second)
		claim := mustClaimPeerInboxArtifact(t, fixture.store, "artifact-ready-worker", claimAt)
		roots := claim.RequiredArtifactRoots()
		if len(roots) != 2 || roots[0].String() >= roots[1].String() {
			t.Fatalf("claim roots = %#v", roots)
		}
		roots[0] = model.Sum([]byte("mutated-copy"))
		if claim.RequiredArtifactRoots()[0] == roots[0] {
			t.Fatal("claim root accessor leaked mutable backing storage")
		}
		readyAt := claimAt.Add(time.Second)
		staged, err := fixture.store.StagePeerInboxArtifactClosure(context.Background(),
			StagePeerInboxArtifactClosureSpec{Fence: claim.Fence(), Closure: closure, At: readyAt})
		if err != nil || !staged.Changed() || staged.Replayed() {
			t.Fatalf("stage cached closure = (%#v,%v)", staged, err)
		}
		spec := MarkPeerInboxArtifactReadySpec{Fence: claim.Fence(), At: readyAt}
		ready, err := fixture.store.MarkPeerInboxArtifactReady(context.Background(), spec)
		if err != nil || !ready.Changed() || ready.Replayed() || ready.Status() != model.InboxReady {
			t.Fatalf("ready = (%#v,%v)", ready, err)
		}
		assertPeerInboxArtifactState(t, fixture.store, "ready", 1, "", false)
		assertPeerInboxArtifactPins(t, fixture.store, put.InboxID, claim.RequiredArtifactRoots())

		later := fixture.channel.AppendActive(t, "artifact-ready-later-member")
		merged, err := fixture.store.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
			ChannelID:                    fixture.channel.Channel().ID(),
			AuthenticatedTransportPeerID: fixture.remote.Identity().PeerID(),
			Records:                      []model.Member{later.Member()}, At: readyAt.Add(time.Hour)})
		if err != nil || merged.Status != ChannelRosterApplied {
			t.Fatalf("later roster merge = (%#v,%v)", merged, err)
		}
		replay, err := fixture.store.MarkPeerInboxArtifactReady(context.Background(), spec)
		if err != nil || replay.Changed() || !replay.Replayed() {
			t.Fatalf("ready replay after authority advance = (%#v,%v)", replay, err)
		}
		assertPeerInboxArtifactPins(t, fixture.store, put.InboxID, claim.RequiredArtifactRoots())
		mustExec(t, fixture.store, `UPDATE peer_inbox SET diagnostic='artifact_busy' WHERE inbox_id=?`,
			put.InboxID.String())
		if _, err := fixture.store.MarkPeerInboxArtifactReady(context.Background(), spec); !errors.Is(err, ErrPeerInboxArtifactStale) {
			t.Fatalf("ready replay with diagnostic error = %v", err)
		}
	})

	t.Run("no-root transition is still fenced", func(t *testing.T) {
		fixture := newPeerInboxFixture(t, "artifact-ready-empty", 0)
		put := fixture.put(t, fixture.publication(t, 1, 1, "artifact-ready-empty", true), fixture.at)
		claimAt := fixture.at.Add(time.Second)
		claim := mustClaimPeerInboxArtifact(t, fixture.store, "artifact-ready-empty-worker", claimAt)
		wrong := claim.Fence()
		wrong.attempt++
		if _, err := fixture.store.MarkPeerInboxArtifactReady(context.Background(),
			MarkPeerInboxArtifactReadySpec{Fence: wrong, At: claimAt.Add(time.Second)}); !errors.Is(err, ErrPeerInboxArtifactStale) {
			t.Fatalf("wrong empty-root fence error = %v", err)
		}
		if _, err := fixture.store.MarkPeerInboxArtifactReady(context.Background(),
			MarkPeerInboxArtifactReadySpec{Fence: claim.Fence(), At: claimAt.Add(time.Second)}); err != nil {
			t.Fatal(err)
		}
		assertPeerInboxArtifactPins(t, fixture.store, put.InboxID, nil)
	})

	t.Run("missing staged incomplete and preforged pins fail closed", func(t *testing.T) {
		t.Run("missing", func(t *testing.T) {
			fixture, claim, _, _ := newPeerInboxArtifactClosureClaim(t, "artifact-ready-missing", false)
			_, err := fixture.store.MarkPeerInboxArtifactReady(context.Background(),
				MarkPeerInboxArtifactReadySpec{Fence: claim.Fence(), At: fixture.at.Add(2 * time.Second)})
			if !errors.Is(err, ErrPeerInboxArtifactNotReady) {
				t.Fatalf("missing root error = %v", err)
			}
			assertPeerInboxArtifactState(t, fixture.store, "waiting_artifact", 1, "artifact-ready-missing-worker", true)
		})

		t.Run("incomplete sealed map", func(t *testing.T) {
			fixture, claim, _, _ := newPeerInboxArtifactClosureClaim(t, "artifact-ready-incomplete", true)
			mustExec(t, fixture.store, `DROP TRIGGER artifact_root_blocks_verified_delete`)
			mustExec(t, fixture.store, `DROP TRIGGER artifact_root_blocks_gc_delete`)
			mustExec(t, fixture.store, `DELETE FROM artifact_root_blocks`)
			_, err := fixture.store.MarkPeerInboxArtifactReady(context.Background(),
				MarkPeerInboxArtifactReadySpec{Fence: claim.Fence(), At: fixture.at.Add(2 * time.Second)})
			if !errors.Is(err, ErrPeerInboxArtifactInvariant) {
				t.Fatalf("incomplete closure error = %v", err)
			}
			assertPeerInboxArtifactState(t, fixture.store, "waiting_artifact", 1, "artifact-ready-incomplete-worker", true)
		})

		t.Run("preforged exact required pin", func(t *testing.T) {
			fixture, claim, root, _ := newPeerInboxArtifactClosureClaim(t, "artifact-ready-preforged", true)
			mustExec(t, fixture.store, `INSERT INTO artifact_pins(root_digest,owner_kind,owner_id,
				expires_at,created_at) VALUES(?,'inbox',?,NULL,?)`, root.RootDigest.String(),
				claim.InboxID().String(), storeTime(fixture.at))
			_, err := fixture.store.MarkPeerInboxArtifactReady(context.Background(),
				MarkPeerInboxArtifactReadySpec{Fence: claim.Fence(), At: fixture.at.Add(2 * time.Second)})
			if !errors.Is(err, ErrPeerInboxArtifactInvariant) {
				t.Fatalf("preforged pin error = %v", err)
			}
			assertPeerInboxArtifactState(t, fixture.store, "waiting_artifact", 1, "artifact-ready-preforged-worker", true)
		})

		t.Run("ready update failure rolls pins back", func(t *testing.T) {
			fixture, claim, _, closure := newPeerInboxArtifactClosureClaim(t, "artifact-ready-rollback", false)
			readyAt := fixture.at.Add(2 * time.Second)
			if _, err := fixture.store.StagePeerInboxArtifactClosure(context.Background(),
				StagePeerInboxArtifactClosureSpec{Fence: claim.Fence(), Closure: closure, At: readyAt}); err != nil {
				t.Fatal(err)
			}
			mustExec(t, fixture.store, `CREATE TRIGGER test_peer_inbox_ready_abort
				BEFORE UPDATE OF status ON peer_inbox WHEN NEW.status='ready'
				BEGIN SELECT RAISE(ABORT, 'forced ready rollback'); END`)
			_, err := fixture.store.MarkPeerInboxArtifactReady(context.Background(),
				MarkPeerInboxArtifactReadySpec{Fence: claim.Fence(), At: readyAt})
			if !errors.Is(err, ErrPeerInboxArtifactInvariant) {
				t.Fatalf("forced ready rollback error = %v", err)
			}
			assertPeerInboxArtifactStagePins(t, fixture.store, claim.InboxID(),
				claim.RequiredArtifactRoots(), claim.Fence().LeaseUntil().Add(peerInboxArtifactStageTTL))
			assertPeerInboxArtifactRootState(t, fixture.store, closure.Roots[0].RootDigest, "staged")
			assertPeerInboxArtifactState(t, fixture.store, "waiting_artifact", 1,
				"artifact-ready-rollback-worker", true)
		})
	})

	t.Run("aggregate closure limit is a quarantinable remote failure", func(t *testing.T) {
		fixture := newPeerInboxFixture(t, "artifact-ready-aggregate-limit", 0)
		closureA, rootA := peerInboxArtifactEmptyTreeClosure(t, "artifact-limit-a",
			maxVerifiedClosureEntries/2, fixture.at.Add(-2*time.Second))
		closureB, rootB := peerInboxArtifactEmptyTreeClosure(t, "artifact-limit-b",
			maxVerifiedClosureEntries/2, fixture.at.Add(-2*time.Second))
		for _, closure := range []VerifiedArtifactClosure{closureA, closureB} {
			if _, err := fixture.store.CheckpointVerifiedArtifactClosure(context.Background(), closure); err != nil {
				t.Fatal(err)
			}
		}
		publication := peerInboxArtifactPublication(t, fixture, 1, 1,
			"artifact-ready-aggregate-limit", []model.Digest{rootA.RootDigest, rootB.RootDigest})
		put := fixture.put(t, publication, fixture.at)
		claimAt := fixture.at.Add(time.Second)
		claim := mustClaimPeerInboxArtifact(t, fixture.store, "artifact-limit-worker", claimAt)
		settleAt := claimAt.Add(time.Second)
		if _, err := fixture.store.MarkPeerInboxArtifactReady(context.Background(),
			MarkPeerInboxArtifactReadySpec{Fence: claim.Fence(), At: settleAt}); !errors.Is(err, ErrPeerInboxArtifactLimit) {
			t.Fatalf("aggregate closure limit error = %v", err)
		}
		assertPeerInboxArtifactPins(t, fixture.store, put.InboxID, nil)
		settled, err := fixture.store.QuarantinePeerInboxArtifact(context.Background(),
			QuarantinePeerInboxArtifactSpec{Fence: claim.Fence(),
				Diagnostic: PeerInboxArtifactLimitExceeded, At: settleAt})
		if err != nil || settled.Status() != model.InboxQuarantined || !settled.Changed() {
			t.Fatalf("quarantine aggregate limit = (%#v,%v)", settled, err)
		}
	})
}

func TestReadPeerInboxArtifactRootClosesCacheProbe(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "artifact-root-probe", 0)
	closureA, rootA, _ := newArtifactSourceClosure(t, "artifact-probe-cached", []byte("cached"),
		fixture.at.Add(-2*time.Second))
	closureB, rootB, _ := newArtifactSourceClosure(t, "artifact-probe-missing", []byte("missing"),
		fixture.at.Add(-2*time.Second))
	if _, err := fixture.store.CheckpointVerifiedArtifactClosure(context.Background(), closureA); err != nil {
		t.Fatal(err)
	}
	publication := peerInboxArtifactPublication(t, fixture, 1, 1, "artifact-probe",
		[]model.Digest{rootA.RootDigest, rootB.RootDigest})
	fixture.put(t, publication, fixture.at)
	claimAt := fixture.at.Add(time.Second)
	claim := mustClaimPeerInboxArtifact(t, fixture.store, "artifact-probe-worker", claimAt)
	probeAt := claimAt.Add(time.Second)
	cached, found, err := fixture.store.ReadPeerInboxArtifactRoot(context.Background(),
		ReadPeerInboxArtifactRootSpec{Fence: claim.Fence(), RootDigest: rootA.RootDigest, At: probeAt})
	verifiedAt, isVerified := cached.VerifiedAt()
	if err != nil || !found || cached.RootDigest() != rootA.RootDigest ||
		cached.ManifestDigest() != rootA.ManifestDigest ||
		cached.State() != PeerInboxArtifactRootVerified || !isVerified || verifiedAt.IsZero() {
		t.Fatalf("cached root = (%#v,%t,%v)", cached, found, err)
	}
	if _, found, err := fixture.store.ReadPeerInboxArtifactRoot(context.Background(),
		ReadPeerInboxArtifactRootSpec{Fence: claim.Fence(), RootDigest: rootB.RootDigest, At: probeAt}); err != nil || found {
		t.Fatalf("missing root = (found %t,%v)", found, err)
	}
	closure := combinePeerInboxArtifactClosures(t, closureA, closureB)
	if _, err := fixture.store.StagePeerInboxArtifactClosure(context.Background(),
		StagePeerInboxArtifactClosureSpec{Fence: claim.Fence(), Closure: closure, At: probeAt}); err != nil {
		t.Fatal(err)
	}
	staged, found, err := fixture.store.ReadPeerInboxArtifactRoot(context.Background(),
		ReadPeerInboxArtifactRootSpec{Fence: claim.Fence(), RootDigest: rootB.RootDigest, At: probeAt})
	if verifiedAt, verified := staged.VerifiedAt(); err != nil || !found ||
		staged.RootDigest() != rootB.RootDigest || staged.Manifest() != rootB.Manifest ||
		staged.ManifestDigest() != rootB.ManifestDigest || staged.TotalBytes() != rootB.TotalBytes ||
		staged.State() != PeerInboxArtifactRootStaged || verified || !verifiedAt.IsZero() {
		t.Fatalf("staged root = (%#v,found %t,%v)", staged, found, err)
	}
	if _, err := fixture.store.GetVerifiedArtifactRoot(context.Background(), rootB.RootDigest); !errors.Is(err, ErrArtifactUnverified) {
		t.Fatalf("generic verified API exposed staged root: %v", err)
	}
	path := fixture.store.Path()
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenExisting(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	fixture.store = restarted
	resumed, found, err := restarted.ReadPeerInboxArtifactRoot(context.Background(),
		ReadPeerInboxArtifactRootSpec{Fence: claim.Fence(), RootDigest: rootB.RootDigest, At: probeAt})
	if err != nil || !found || resumed.State() != PeerInboxArtifactRootStaged ||
		resumed.Manifest().String() != rootB.Manifest.String() {
		t.Fatalf("restarted staged root = (%#v,found %t,%v)", resumed, found, err)
	}
	if _, _, err := fixture.store.ReadPeerInboxArtifactRoot(context.Background(),
		ReadPeerInboxArtifactRootSpec{Fence: claim.Fence(), RootDigest: model.Sum([]byte("unrelated")),
			At: probeAt}); !errors.Is(err, ErrPeerInboxArtifactInput) {
		t.Fatalf("non-required root error = %v", err)
	}
	if _, _, err := fixture.store.ReadPeerInboxArtifactRoot(context.Background(),
		ReadPeerInboxArtifactRootSpec{Fence: claim.Fence(), RootDigest: rootA.RootDigest,
			At: claim.Fence().LeaseUntil()}); !errors.Is(err, ErrPeerInboxArtifactStale) {
		t.Fatalf("expired probe error = %v", err)
	}

	mustExec(t, fixture.store, `DROP TRIGGER artifact_root_blocks_gc_delete`)
	mustExec(t, fixture.store, `DELETE FROM artifact_root_blocks WHERE root_digest=?`, rootB.RootDigest.String())
	if _, _, err := fixture.store.ReadPeerInboxArtifactRoot(context.Background(),
		ReadPeerInboxArtifactRootSpec{Fence: claim.Fence(), RootDigest: rootB.RootDigest,
			At: probeAt}); !errors.Is(err, ErrPeerInboxArtifactInvariant) {
		t.Fatalf("corrupt staged closure error = %v", err)
	}
}

func TestPeerInboxArtifactAuthorityRaceQuarantineAndPressure(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "artifact-authority-race", 0)
	publication := fixture.publication(t, 1, 1, "artifact-authority-race", true)
	put := fixture.put(t, publication, fixture.at)
	claimAt := fixture.at.Add(time.Second)
	claim := mustClaimPeerInboxArtifact(t, fixture.store, "artifact-authority-worker", claimAt)
	pendingBytes := peerInboxArtifactPressure(t, fixture.store)
	if pendingBytes == 0 {
		t.Fatal("pending Inbox did not reserve pressure")
	}
	challenger := fixture.publication(t, 1, 1, "artifact-authority-challenger", true)
	conflict := fixture.put(t, challenger, claimAt.Add(time.Second))
	if conflict.Disposition != PeerInboxConflicted {
		t.Fatalf("challenger disposition = %s", conflict.Disposition)
	}
	if _, err := fixture.store.MarkPeerInboxArtifactReady(context.Background(),
		MarkPeerInboxArtifactReadySpec{Fence: claim.Fence(), At: claimAt.Add(2 * time.Second)}); !errors.Is(err, ErrPeerInboxArtifactAuthority) {
		t.Fatalf("ready after origin quarantine error = %v", err)
	}
	assertPeerInboxArtifactPins(t, fixture.store, put.InboxID, nil)

	quarantineAt := claimAt.Add(2 * time.Second)
	spec := QuarantinePeerInboxArtifactSpec{Fence: claim.Fence(),
		Diagnostic: PeerInboxArtifactDigestMismatch, At: quarantineAt}
	settled, err := fixture.store.QuarantinePeerInboxArtifact(context.Background(), spec)
	if err != nil || !settled.Changed() || settled.Replayed() || settled.Status() != model.InboxQuarantined {
		t.Fatalf("quarantine = (%#v,%v)", settled, err)
	}
	if got := peerInboxArtifactPressure(t, fixture.store); got != 0 {
		t.Fatalf("pressure after quarantine = %d", got)
	}
	replayed, err := fixture.store.QuarantinePeerInboxArtifact(context.Background(), spec)
	if err != nil || replayed.Changed() || !replayed.Replayed() || peerInboxArtifactPressure(t, fixture.store) != 0 {
		t.Fatalf("quarantine replay = (%#v,%v), pressure %d", replayed, err,
			peerInboxArtifactPressure(t, fixture.store))
	}
	assertPeerInboxArtifactPins(t, fixture.store, put.InboxID, nil)
	for _, table := range []string{"events", "works", "agent_handlings"} {
		var count int
		if err := fixture.store.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("%s after Artifact quarantine = (%d,%v)", table, count, err)
		}
	}
	result, err := fixture.store.ClaimPeerInboxArtifact(context.Background(),
		ClaimPeerInboxArtifactSpec{LeaseOwner: "artifact-terminal-worker", At: quarantineAt.Add(time.Hour)})
	if err != nil || result.Found() {
		t.Fatalf("terminal reclaim = (found %t,%v)", result.Found(), err)
	}
	if _, err := fixture.store.QuarantinePeerInboxArtifact(context.Background(),
		QuarantinePeerInboxArtifactSpec{Fence: claim.Fence(), Diagnostic: "artifact_future",
			At: quarantineAt}); !errors.Is(err, ErrPeerInboxArtifactInput) {
		t.Fatalf("unknown permanent diagnostic error = %v", err)
	}
}

func TestPeerInboxArtifactTamperedPublicationIsNodeInvariant(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "artifact-tampered-publication", 0)
	fixture.put(t, fixture.publication(t, 1, 1, "artifact-tampered-publication", true), fixture.at)
	mustExec(t, fixture.store, `DROP TRIGGER peer_inbox_identity_immutable`)
	mustExec(t, fixture.store, `UPDATE peer_inbox SET origin_signature=zeroblob(64)`)
	result, err := fixture.store.ClaimPeerInboxArtifact(context.Background(),
		ClaimPeerInboxArtifactSpec{LeaseOwner: "artifact-tamper-worker", At: fixture.at.Add(time.Second)})
	if !errors.Is(err, ErrPeerInboxArtifactInvariant) || errors.Is(err, ErrPeerInboxArtifactAuthority) || result.Found() {
		t.Fatalf("tampered signed tuple claim = (found %t,%v)", result.Found(), err)
	}
}

func peerInboxArtifactPublication(t *testing.T, fixture peerInboxFixture,
	originSequence, channelSequence uint64, suffix string, roots []model.Digest,
) model.SignedPublication {
	t.Helper()
	base := fixture.publication(t, originSequence, channelSequence, suffix, true)
	event := base.Event()
	refs := make([]model.ArtifactRef, len(roots))
	for index, root := range roots {
		var err error
		refs[index], err = model.NewArtifactRef(root, model.ArtifactProduced)
		if err != nil {
			t.Fatal(err)
		}
	}
	return fixture.signEvent(t, model.EventSpec{ID: event.ID(), Scope: event.Scope(),
		Source: model.EventSourceLocal, ActorPrincipal: event.ActorPrincipal(), Type: event.Type(),
		Audience: event.Audience(), Summary: event.Summary(), Payload: event.Payload(),
		Artifacts: refs, CausedBy: event.CausedBy(), CreatedAt: event.CreatedAt(),
		AcceptedAt: event.AcceptedAt()})
}

func newPeerInboxArtifactClosureClaim(t *testing.T, seed string,
	checkpoint bool,
) (peerInboxFixture, PeerInboxArtifactClaim, VerifiedArtifactRoot, VerifiedArtifactClosure) {
	t.Helper()
	fixture := newPeerInboxFixture(t, seed, 0)
	closure, root, _ := newArtifactSourceClosure(t, seed, []byte("closure-"+seed),
		fixture.at.Add(-2*time.Second))
	if checkpoint {
		if _, err := fixture.store.CheckpointVerifiedArtifactClosure(context.Background(), closure); err != nil {
			t.Fatal(err)
		}
	}
	publication := peerInboxArtifactPublication(t, fixture, 1, 1, seed,
		[]model.Digest{root.RootDigest})
	fixture.put(t, publication, fixture.at)
	claim := mustClaimPeerInboxArtifact(t, fixture.store, seed+"-worker", fixture.at.Add(time.Second))
	return fixture, claim, root, closure
}

func peerInboxArtifactEmptyTreeClosure(t *testing.T, rootPath string, emptyFiles int,
	verifiedAt time.Time,
) (VerifiedArtifactClosure, VerifiedArtifactRoot) {
	t.Helper()
	entries := make([]artifactdomain.ManifestEntry, 0, emptyFiles+1)
	entries = append(entries, artifactdomain.ManifestEntry{Kind: artifactdomain.EntryDirectory,
		LogicalPath: rootPath, Mode: 0o700, Blocks: []artifactdomain.ManifestBlock{}})
	for index := 0; index < emptyFiles; index++ {
		entries = append(entries, artifactdomain.ManifestEntry{Kind: artifactdomain.EntryFile,
			LogicalPath: fmt.Sprintf("%s/file-%04d", rootPath, index), Mode: 0o600,
			Blocks: []artifactdomain.ManifestBlock{}})
	}
	manifest, err := artifactdomain.NewManifest(artifactdomain.ManifestSpec{
		RootKind: artifactdomain.EntryDirectory, RootPath: rootPath, Entries: entries})
	if err != nil {
		t.Fatal(err)
	}
	root := VerifiedArtifactRoot{RootDigest: manifest.RootDigest(), Manifest: manifest.CanonicalJSON(),
		ManifestDigest: manifest.ManifestDigest(), TotalBytes: manifest.TotalBytes(),
		CreatedAt: verifiedAt, VerifiedAt: verifiedAt}
	return VerifiedArtifactClosure{Roots: []VerifiedArtifactRoot{root}}, root
}

func addPeerInboxArtifactChannel(t *testing.T, base peerInboxFixture, seed string) peerInboxFixture {
	t.Helper()
	seed = strings.ReplaceAll(seed, " ", "-")
	channel := testkit.NewSignedChannelForOwnerAt(t, seed, base.channel.Owner(), base.at.Add(time.Second))
	remote := channel.AppendActive(t, seed+"-remote")
	insertSignedChannelFixture(t, base.store.db, channel, model.TopicJoined)
	insertSignedPeerBinding(t, base.store.db, channel.Channel().ID(), remote, seed+"-remote",
		model.BindingPending, model.ReachabilityUnknown, remote.Member().CreatedAt())
	mustExec(t, base.store, `INSERT INTO publication_epochs(channel_id,origin_peer_id,origin_epoch,
		source_floor_channel_seq,source_head_channel_seq,updated_at) VALUES(?,?,?,1,0,?)`,
		channel.Channel().ID().String(), channel.Owner().PeerID().String(),
		channel.Owner().OriginEpoch().String(), storeTime(channel.Channel().UpdatedAt()))
	fixture := peerInboxFixture{channelBaselineFixture: channelBaselineFixture{store: base.store,
		channel: channel, remote: remote, at: channel.Channel().UpdatedAt().Add(time.Second)}}
	if _, err := base.store.InstallInboundChannelBaseline(context.Background(),
		InstallInboundChannelBaselineSpec{AuthenticatedPeerID: remote.Identity().PeerID(),
			Baseline: fixture.remoteBaseline(0), At: fixture.at}); err != nil {
		t.Fatal(err)
	}
	fixture.at = fixture.at.Add(time.Second)
	return fixture
}

func mustClaimPeerInboxArtifact(t *testing.T, store *Store, owner string,
	at time.Time,
) PeerInboxArtifactClaim {
	t.Helper()
	result, err := store.ClaimPeerInboxArtifact(context.Background(),
		ClaimPeerInboxArtifactSpec{LeaseOwner: owner, At: at})
	if err != nil || !result.Found() {
		t.Fatalf("ClaimPeerInboxArtifact(%q) = (found %t,%v)", owner, result.Found(), err)
	}
	return result.Claim()
}

func newRenewedPeerInboxArtifact(t *testing.T, seed string,
) (peerInboxFixture, PeerInboxArtifactClaim, PeerInboxArtifactRenewal, time.Time) {
	t.Helper()
	fixture := newPeerInboxFixture(t, seed, 0)
	fixture.put(t, fixture.publication(t, 1, 1, seed, true), fixture.at)
	claimAt := fixture.at.Add(time.Second)
	claim := mustClaimPeerInboxArtifact(t, fixture.store, seed+"-worker", claimAt)
	renewAt := claimAt.Add(time.Second)
	renewed, err := fixture.store.RenewPeerInboxArtifactLease(context.Background(),
		RenewPeerInboxArtifactSpec{Fence: claim.Fence(), At: renewAt})
	if err != nil || !renewed.Changed() || renewed.Replayed() {
		t.Fatalf("renew fixture = (%#v,%v)", renewed, err)
	}
	return fixture, claim, renewed, renewAt
}

func assertPeerInboxArtifactRenewReceipt(t *testing.T, store *Store,
	inboxID model.InboxID, oldFence, newFence PeerInboxArtifactFence, requestedAt time.Time,
) {
	t.Helper()
	var oldOwner, oldLease, requested, outputStatus, outputNext string
	var outputOwner, outputLease, outputUpdated string
	var oldAttempt, outputAttempt, nonceLength, digestLength int64
	var diagnosticIsNull int
	err := store.db.QueryRow(`SELECT old_lease_owner,old_lease_until,old_attempt,
		length(semantic_nonce),requested_at,length(request_digest),output_status,
		output_attempt,output_next_attempt_at,output_lease_owner,output_lease_until,
		output_diagnostic IS NULL,output_updated_at
		FROM peer_inbox_artifact_renew_receipts WHERE inbox_id=?`, inboxID.String()).Scan(&oldOwner, &oldLease,
		&oldAttempt, &nonceLength, &requested, &digestLength, &outputStatus,
		&outputAttempt, &outputNext, &outputOwner, &outputLease,
		&diagnosticIsNull, &outputUpdated)
	if err != nil {
		t.Fatal(err)
	}
	if oldOwner != oldFence.LeaseOwner() || oldLease != storeTime(oldFence.LeaseUntil()) ||
		oldAttempt != int64(oldFence.Attempt()) || nonceLength != 32 || digestLength != 32 ||
		requested != storeTime(requestedAt) || outputStatus != string(model.InboxWaitingArtifact) ||
		outputAttempt != int64(newFence.Attempt()) ||
		outputOwner != newFence.LeaseOwner() || outputLease != storeTime(newFence.LeaseUntil()) ||
		diagnosticIsNull != 1 || outputUpdated != storeTime(requestedAt) {
		t.Fatalf("Artifact renew receipt differs: old=(%q,%q,%d) request=(%q,%d,%d) "+
			"output=(%q,%d,%q,%q,%q,%d,%q)", oldOwner, oldLease, oldAttempt,
			requested, nonceLength, digestLength, outputStatus, outputAttempt, outputNext,
			outputOwner, outputLease, diagnosticIsNull, outputUpdated)
	}
}

func assertPeerInboxArtifactRenewReceiptCount(t *testing.T, store *Store,
	inboxID model.InboxID, want int,
) {
	t.Helper()
	var got int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM peer_inbox_artifact_renew_receipts
		WHERE inbox_id=?`, inboxID.String()).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("Artifact renew receipt count = %d, want %d", got, want)
	}
}

func assertPeerInboxArtifactState(t *testing.T, store *Store, status string,
	attempt uint32, owner string, hasLease bool,
) {
	t.Helper()
	var gotStatus string
	var gotAttempt uint32
	var gotOwner, gotLease sqlNullString
	if err := store.db.QueryRow(`SELECT status,attempts,lease_owner,lease_until FROM peer_inbox
		ORDER BY received_at LIMIT 1`).Scan(&gotStatus, &gotAttempt, &gotOwner, &gotLease); err != nil {
		t.Fatal(err)
	}
	if gotStatus != status || gotAttempt != attempt || gotOwner.String != owner ||
		gotOwner.Valid != hasLease || gotLease.Valid != hasLease {
		t.Fatalf("Inbox state = status %q attempt %d owner %#v lease %#v", gotStatus, gotAttempt,
			gotOwner, gotLease)
	}
}

// Keep the test helper independent from database/sql so accidental SQL
// absence never becomes part of the production receiver contract.
type sqlNullString struct {
	String string
	Valid  bool
}

func (value *sqlNullString) Scan(source any) error {
	if source == nil {
		value.String, value.Valid = "", false
		return nil
	}
	text, ok := source.(string)
	if !ok {
		return errors.New("non-string nullable value")
	}
	value.String, value.Valid = text, true
	return nil
}

func assertPeerInboxArtifactPins(t *testing.T, store *Store, inboxID model.InboxID,
	want []model.Digest,
) {
	t.Helper()
	rows, err := store.db.Query(`SELECT root_digest FROM artifact_pins
		WHERE owner_kind='inbox' AND owner_id=? ORDER BY root_digest`, inboxID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []model.Digest
	for rows.Next() {
		var text string
		if err := rows.Scan(&text); err != nil {
			t.Fatal(err)
		}
		digest, err := model.ParseDigest(text)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, digest)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("Inbox pins = %#v, want %#v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("Inbox pins = %#v, want %#v", got, want)
		}
	}
}

func assertPeerInboxArtifactStagePins(t *testing.T, store *Store, inboxID model.InboxID,
	want []model.Digest, expiresAt time.Time,
) {
	t.Helper()
	rows, err := store.db.Query(`SELECT root_digest,expires_at FROM artifact_pins
		WHERE owner_kind='inbox' AND owner_id=? ORDER BY root_digest`, inboxID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []model.Digest
	for rows.Next() {
		var rootText, expiresText string
		if err := rows.Scan(&rootText, &expiresText); err != nil {
			t.Fatal(err)
		}
		root, err := model.ParseDigest(rootText)
		if err != nil || expiresText != storeTime(expiresAt) {
			t.Fatalf("stage pin = (%q,%q), want expiry %q: %v",
				rootText, expiresText, storeTime(expiresAt), err)
		}
		got = append(got, root)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("stage pins = %#v, want %#v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("stage pins = %#v, want %#v", got, want)
		}
	}
}

func assertPeerInboxArtifactRootState(t *testing.T, store *Store, root model.Digest,
	want string,
) {
	t.Helper()
	var state string
	var verified sqlNullString
	if err := store.db.QueryRow(`SELECT state,verified_at FROM artifact_roots WHERE root_digest=?`,
		root.String()).Scan(&state, &verified); err != nil {
		t.Fatal(err)
	}
	if state != want || verified.Valid != (want == "verified") {
		t.Fatalf("Artifact root state = (%q,%#v), want %q", state, verified, want)
	}
}

func combinePeerInboxArtifactClosures(t *testing.T,
	closures ...VerifiedArtifactClosure,
) VerifiedArtifactClosure {
	t.Helper()
	var combined VerifiedArtifactClosure
	blocks := make(map[model.Digest]VerifiedArtifactBlock)
	for _, closure := range closures {
		combined.Roots = append(combined.Roots, closure.Roots...)
		combined.RootBlocks = append(combined.RootBlocks, closure.RootBlocks...)
		for _, block := range closure.Blocks {
			if prior, found := blocks[block.Digest]; found && prior.SizeBytes != block.SizeBytes {
				t.Fatalf("shared block %s has conflicting sizes", block.Digest)
			}
			blocks[block.Digest] = block
		}
	}
	for _, block := range blocks {
		combined.Blocks = append(combined.Blocks, block)
	}
	sort.Slice(combined.Roots, func(left, right int) bool {
		return combined.Roots[left].RootDigest.String() < combined.Roots[right].RootDigest.String()
	})
	sort.Slice(combined.Blocks, func(left, right int) bool {
		return combined.Blocks[left].Digest.String() < combined.Blocks[right].Digest.String()
	})
	sort.Slice(combined.RootBlocks, func(left, right int) bool {
		if combined.RootBlocks[left].RootDigest != combined.RootBlocks[right].RootDigest {
			return combined.RootBlocks[left].RootDigest.String() < combined.RootBlocks[right].RootDigest.String()
		}
		return combined.RootBlocks[left].Ordinal < combined.RootBlocks[right].Ordinal
	})
	return combined
}

func peerInboxArtifactPressure(t *testing.T, store *Store) int64 {
	t.Helper()
	var pending int64
	if err := store.db.QueryRow(`SELECT pending_bytes FROM peer_inbox_node_pressure
		WHERE singleton_id=1`).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	return pending
}
