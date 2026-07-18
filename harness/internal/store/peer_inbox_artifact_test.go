package store

import (
	"context"
	"errors"
	"fmt"
	"math"
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
			fixture, claim, _, _ := newPeerInboxArtifactClosureClaim(t, "artifact-ready-rollback", true)
			mustExec(t, fixture.store, `CREATE TRIGGER test_peer_inbox_ready_abort
				BEFORE UPDATE OF status ON peer_inbox WHEN NEW.status='ready'
				BEGIN SELECT RAISE(ABORT, 'forced ready rollback'); END`)
			_, err := fixture.store.MarkPeerInboxArtifactReady(context.Background(),
				MarkPeerInboxArtifactReadySpec{Fence: claim.Fence(), At: fixture.at.Add(2 * time.Second)})
			if !errors.Is(err, ErrPeerInboxArtifactInvariant) {
				t.Fatalf("forced ready rollback error = %v", err)
			}
			assertPeerInboxArtifactPins(t, fixture.store, claim.InboxID(), nil)
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
	if err != nil || !found || cached.RootDigest != rootA.RootDigest ||
		cached.ManifestDigest != rootA.ManifestDigest {
		t.Fatalf("cached root = (%#v,%t,%v)", cached, found, err)
	}
	if _, found, err := fixture.store.ReadPeerInboxArtifactRoot(context.Background(),
		ReadPeerInboxArtifactRootSpec{Fence: claim.Fence(), RootDigest: rootB.RootDigest, At: probeAt}); err != nil || found {
		t.Fatalf("missing root = (found %t,%v)", found, err)
	}
	staged := closureB.Roots[0]
	mustExec(t, fixture.store, `INSERT INTO artifact_roots(root_digest,manifest_json,
		manifest_digest,total_bytes,state,created_at) VALUES(?,?,?,?,'staged',?)`,
		staged.RootDigest.String(), staged.Manifest.Bytes(), staged.ManifestDigest.Bytes(),
		staged.TotalBytes, storeTime(staged.CreatedAt))
	if _, found, err := fixture.store.ReadPeerInboxArtifactRoot(context.Background(),
		ReadPeerInboxArtifactRootSpec{Fence: claim.Fence(), RootDigest: rootB.RootDigest, At: probeAt}); err != nil || found {
		t.Fatalf("staged root = (found %t,%v)", found, err)
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

	mustExec(t, fixture.store, `DROP TRIGGER artifact_root_blocks_verified_delete`)
	mustExec(t, fixture.store, `DELETE FROM artifact_root_blocks WHERE root_digest=?`, rootA.RootDigest.String())
	if _, _, err := fixture.store.ReadPeerInboxArtifactRoot(context.Background(),
		ReadPeerInboxArtifactRootSpec{Fence: claim.Fence(), RootDigest: rootA.RootDigest,
			At: probeAt}); !errors.Is(err, ErrPeerInboxArtifactInvariant) {
		t.Fatalf("corrupt cached closure error = %v", err)
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

func peerInboxArtifactPressure(t *testing.T, store *Store) int64 {
	t.Helper()
	var pending int64
	if err := store.db.QueryRow(`SELECT pending_bytes FROM peer_inbox_node_pressure
		WHERE singleton_id=1`).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	return pending
}
