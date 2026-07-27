package store

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestPeerInboxSemanticClaimReturnsExactAuthoritySnapshot(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "semantic-snapshot", 0)
	publication, work, cause := peerInboxSemanticCurrentWorkPublication(t, fixture,
		"semantic-snapshot", 1, 1)
	put := fixture.put(t, publication, fixture.at)
	readyAt := fixture.at.Add(time.Second)
	markPeerInboxSemanticReady(t, fixture.store, put.InboxID, readyAt)

	claimAt := readyAt.Add(time.Second)
	result, err := fixture.store.ClaimPeerInboxSemantic(context.Background(),
		ClaimPeerInboxSemanticSpec{LeaseOwner: "semantic-snapshot-worker", At: claimAt})
	if err != nil || !result.Found() {
		t.Fatalf("ClaimPeerInboxSemantic() = (found %t,%v)", result.Found(), err)
	}
	claim := result.Claim()
	if claim.InboxID() != put.InboxID || claim.Fence().Attempt() != 1 ||
		claim.Fence().LeaseOwner() != "semantic-snapshot-worker" ||
		!claim.Fence().LeaseUntil().Equal(claimAt.Add(peerInboxSemanticLease)) ||
		claim.SnapshotDigest().IsZero() || claim.DecisionSeed().IsZero() {
		t.Fatalf("claim authority = %#v", claim)
	}
	if claim.ImportedEvent().Source() != model.EventSourceImported ||
		claim.ImportedEvent().Key() != publication.Event().Key() ||
		claim.ImportedEvent().Digest() != publication.Event().Digest() ||
		!bytes.Equal(claim.ImportedEvent().CanonicalJSON().Bytes(),
			publication.Event().CanonicalJSON().Bytes()) ||
		!bytes.Equal(claim.Publication().WireJSON().Bytes(), publication.WireJSON().Bytes()) {
		t.Fatal("claim changed exact signed publication or imported Event identity")
	}
	current, ok := claim.CurrentWork()
	if !ok || current.Ref() != work.Ref() || current.Version() != work.Version() ||
		current.State() != work.State() || current.UpdatedBy() != work.UpdatedBy() {
		t.Fatalf("current Work = (%#v,%t), want %#v", current, ok, work)
	}
	causes := claim.CausalEvents()
	if len(causes) != 1 || causes[0].Key() != cause.Key() ||
		causes[0].Source() != model.EventSourceLocal || causes[0].Digest() != cause.Digest() {
		t.Fatalf("causal Events = %#v, want exact %s", causes, cause.ID())
	}
	causes[0] = model.Event{}
	if replayed := claim.CausalEvents(); len(replayed) != 1 || replayed[0].ID().IsZero() {
		t.Fatal("claim causal Event slice is not defensive")
	}
	if roots := claim.RequiredArtifactRoots(); len(roots) != 0 {
		t.Fatalf("required roots = %#v, want empty", roots)
	}
	if err := fixture.store.ProbePeerInboxSemanticAuthority(context.Background(),
		ProbePeerInboxSemanticAuthoritySpec{Fence: claim.Fence(), At: claimAt.Add(time.Second)}); err != nil {
		t.Fatalf("ProbePeerInboxSemanticAuthority() error = %v", err)
	}
	assertPeerInboxSemanticNoDomainMutation(t, fixture.store, publication.Event().ID())
}

func TestPeerInboxSemanticClaimsGloballyOldestAndSeparatesRetryDomains(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "semantic-oldest", 0)
	base := fixture.at
	first := fixture.put(t, fixture.publication(t, 1, 1, "semantic-oldest-first", true), base)
	artifactRetry := fixture.put(t,
		fixture.publication(t, 2, 2, "semantic-oldest-artifact", true), base.Add(time.Second))
	second := fixture.put(t,
		fixture.publication(t, 3, 3, "semantic-oldest-second", true), base.Add(2*time.Second))
	markPeerInboxSemanticReady(t, fixture.store, first.InboxID, base.Add(3*time.Second))
	markPeerInboxSemanticReady(t, fixture.store, second.InboxID, base.Add(4*time.Second))
	mustExec(t, fixture.store, `UPDATE peer_inbox SET status='retry',next_attempt_at=?,
		diagnostic=?,updated_at=? WHERE inbox_id=?`, storeTime(base),
		string(PeerInboxArtifactRetryTimeout), storeTime(base.Add(3*time.Second)),
		artifactRetry.InboxID.String())

	claimAt := base.Add(5 * time.Second)
	one := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-oldest-one", claimAt)
	two := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-oldest-two", claimAt)
	if one.InboxID() != first.InboxID || two.InboxID() != second.InboxID {
		t.Fatalf("claim order = (%s,%s), want (%s,%s)", one.InboxID(), two.InboxID(),
			first.InboxID, second.InboxID)
	}
	empty, err := fixture.store.ClaimPeerInboxSemantic(context.Background(),
		ClaimPeerInboxSemanticSpec{LeaseOwner: "semantic-oldest-empty", At: claimAt})
	if err != nil || empty.Found() {
		t.Fatalf("claim with only Artifact retry = (found %t,%v)", empty.Found(), err)
	}
	var status, diagnostic string
	var attempts uint32
	if err := fixture.store.db.QueryRow(`SELECT status,attempts,diagnostic FROM peer_inbox
		WHERE inbox_id=?`, artifactRetry.InboxID.String()).Scan(&status, &attempts, &diagnostic); err != nil ||
		status != "retry" || attempts != 0 || diagnostic != string(PeerInboxArtifactRetryTimeout) {
		t.Fatalf("Artifact retry changed = (%q,%d,%q,%v)", status, attempts, diagnostic, err)
	}

	observer := newPeerInboxObserverFixture(t, "semantic-nonaudience")
	ignored := observer.put(t, observer.publication(t, 1, 1, "semantic-nonaudience", false), observer.at)
	if ignored.Disposition != PeerInboxIgnored {
		t.Fatalf("observer disposition = %s", ignored.Disposition)
	}
	observerResult, err := observer.store.ClaimPeerInboxSemantic(context.Background(),
		ClaimPeerInboxSemanticSpec{LeaseOwner: "semantic-observer-worker", At: observer.at.Add(time.Second)})
	if err != nil || observerResult.Found() {
		t.Fatalf("nonaudience claim = (found %t,%v)", observerResult.Found(), err)
	}
}

func TestPeerInboxSemanticSkipsInvalidOldestWithoutLosingFutureEligibility(t *testing.T) {
	t.Parallel()
	oldest := newPeerInboxFixture(t, "semantic-authority-oldest", 0)
	oldestPut := oldest.put(t,
		oldest.publication(t, 1, 1, "semantic-authority-oldest", true), oldest.at)
	oldestReadyAt := oldest.at.Add(time.Second)
	markPeerInboxSemanticReady(t, oldest.store, oldestPut.InboxID, oldestReadyAt)

	later := addPeerInboxArtifactChannel(t, oldest, "semantic-authority-later")
	laterPut := later.put(t,
		later.publication(t, 1, 1, "semantic-authority-later", true), later.at)
	laterReadyAt := later.at.Add(time.Second)
	markPeerInboxSemanticReady(t, later.store, laterPut.InboxID, laterReadyAt)

	driftAt := laterReadyAt.Add(time.Second)
	mustExec(t, oldest.store, `UPDATE channels SET topic_state='blocked',updated_at=?
		WHERE channel_id=?`, storeTime(driftAt), oldest.channel.Channel().ID().String())
	claimAt := driftAt.Add(time.Second)
	claim := mustClaimPeerInboxSemantic(t, oldest.store, "semantic-authority-later-worker", claimAt)
	if claim.InboxID() != laterPut.InboxID {
		t.Fatalf("claim with invalid oldest = %s, want later %s", claim.InboxID(), laterPut.InboxID)
	}
	assertPeerInboxSemanticState(t, oldest.store, oldestPut.InboxID, "ready", 0, "", false)

	restoredAt := claimAt.Add(time.Second)
	mustExec(t, oldest.store, `UPDATE channels SET topic_state='joined',updated_at=?
		WHERE channel_id=?`, storeTime(restoredAt), oldest.channel.Channel().ID().String())
	restored := mustClaimPeerInboxSemantic(t, oldest.store, "semantic-authority-restored-worker",
		restoredAt.Add(time.Second))
	if restored.InboxID() != oldestPut.InboxID || restored.Fence().Attempt() != 1 {
		t.Fatalf("restored authority claim = %s attempt %d, want %s attempt 1",
			restored.InboxID(), restored.Fence().Attempt(), oldestPut.InboxID)
	}
}

func TestPeerInboxSemanticReclaimAdvancesAttemptAndFencesOldOwner(t *testing.T) {
	t.Parallel()
	fixture, _, readyAt := newReadyPeerInboxSemantic(t, "semantic-reclaim")
	first := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-reclaim-first", readyAt.Add(time.Second))
	reclaimAt := first.Fence().LeaseUntil()
	second := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-reclaim-second", reclaimAt)
	if first.Fence().Attempt() != 1 || second.Fence().Attempt() != 2 ||
		second.DecisionSeed() != first.DecisionSeed() ||
		second.SnapshotDigest() != first.SnapshotDigest() {
		t.Fatalf("reclaim generations = first %#v second %#v", first.Fence(), second.Fence())
	}
	if err := fixture.store.ProbePeerInboxSemanticAuthority(context.Background(),
		ProbePeerInboxSemanticAuthoritySpec{Fence: first.Fence(), At: reclaimAt}); !errors.Is(err, ErrPeerInboxSemanticStale) {
		t.Fatalf("old probe error = %v", err)
	}
	if _, err := fixture.store.RenewPeerInboxSemantic(context.Background(),
		RenewPeerInboxSemanticSpec{Fence: first.Fence(), At: reclaimAt}); !errors.Is(err, ErrPeerInboxSemanticStale) {
		t.Fatalf("old renew error = %v", err)
	}
	if _, err := fixture.store.RetryPeerInboxSemantic(context.Background(),
		RetryPeerInboxSemanticSpec{Fence: first.Fence(), Diagnostic: PeerInboxSemanticRetryBusy,
			RetryAfter: time.Second, At: reclaimAt}); !errors.Is(err, ErrPeerInboxSemanticStale) {
		t.Fatalf("old retry error = %v", err)
	}
	if err := fixture.store.ProbePeerInboxSemanticAuthority(context.Background(),
		ProbePeerInboxSemanticAuthoritySpec{Fence: second.Fence(), At: reclaimAt.Add(time.Second)}); err != nil {
		t.Fatalf("new probe error = %v", err)
	}
}

func TestPeerInboxSemanticRenewAndRetryReplayAcrossRestart(t *testing.T) {
	t.Parallel()
	fixture, _, readyAt := newReadyPeerInboxSemantic(t, "semantic-restart")
	path := fixture.store.Path()
	claimAt := readyAt.Add(time.Second)
	claim := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-restart-worker", claimAt)
	renewAt := claimAt.Add(time.Second)
	renewed, err := fixture.store.RenewPeerInboxSemantic(context.Background(),
		RenewPeerInboxSemanticSpec{Fence: claim.Fence(), At: renewAt})
	if err != nil || !renewed.Changed() || renewed.Replayed() {
		t.Fatalf("renew = (%#v,%v)", renewed, err)
	}
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.store = nil
	restarted, err := OpenExisting(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	replay, err := restarted.RenewPeerInboxSemantic(context.Background(),
		RenewPeerInboxSemanticSpec{Fence: claim.Fence(), At: renewAt})
	if err != nil || !replay.Replayed() || replay.Changed() ||
		replay.Fence().LeaseUntil() != renewed.Fence().LeaseUntil() {
		t.Fatalf("restart renewal replay = (%#v,%v)", replay, err)
	}
	retryAt := renewAt.Add(time.Second)
	retrySpec := RetryPeerInboxSemanticSpec{Fence: renewed.Fence(),
		Diagnostic: PeerInboxSemanticRetryDependencyUnavailable,
		RetryAfter: 5 * time.Second, At: retryAt}
	retry, err := restarted.RetryPeerInboxSemantic(context.Background(), retrySpec)
	if err != nil || !retry.Changed() || retry.Replayed() ||
		!retry.NextAttemptAt().Equal(retryAt.Add(5*time.Second)) {
		t.Fatalf("retry = (%#v,%v)", retry, err)
	}
	retryReplay, err := restarted.RetryPeerInboxSemantic(context.Background(), retrySpec)
	if err != nil || !retryReplay.Replayed() || retryReplay.Changed() ||
		retryReplay.NextAttemptAt() != retry.NextAttemptAt() {
		t.Fatalf("retry replay = (%#v,%v)", retryReplay, err)
	}
	before, err := restarted.ClaimPeerInboxSemantic(context.Background(),
		ClaimPeerInboxSemanticSpec{LeaseOwner: "semantic-restart-before", At: retry.NextAttemptAt().Add(-time.Nanosecond)})
	if err != nil || before.Found() {
		t.Fatalf("pre-schedule claim = (found %t,%v)", before.Found(), err)
	}
	reclaimed := mustClaimPeerInboxSemantic(t, restarted, "semantic-restart-reclaim", retry.NextAttemptAt())
	if reclaimed.Fence().Attempt() != claim.Fence().Attempt()+1 ||
		reclaimed.DecisionSeed() != claim.DecisionSeed() {
		t.Fatalf("scheduled reclaim = %#v, first %#v", reclaimed.Fence(), claim.Fence())
	}
	if _, err := restarted.RetryPeerInboxSemantic(context.Background(),
		RetryPeerInboxSemanticSpec{Fence: reclaimed.Fence(), Diagnostic: "semantic_future",
			RetryAfter: time.Second, At: retry.NextAttemptAt().Add(time.Second)}); !errors.Is(err, ErrPeerInboxSemanticInput) {
		t.Fatalf("unknown diagnostic error = %v", err)
	}
	assertPeerInboxSemanticNoDomainMutation(t, restarted, claim.ImportedEvent().ID())
}

func TestPeerInboxSemanticOnlyLatestRenewReceiptCanReplay(t *testing.T) {
	t.Parallel()
	fixture, inboxID, readyAt := newReadyPeerInboxSemantic(t, "semantic-multi-renew")
	claim := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-multi-renew-worker",
		readyAt.Add(time.Second))
	firstAt := readyAt.Add(2 * time.Second)
	firstSpec := RenewPeerInboxSemanticSpec{Fence: claim.Fence(), At: firstAt}
	first, err := fixture.store.RenewPeerInboxSemantic(context.Background(), firstSpec)
	if err != nil || !first.Changed() {
		t.Fatalf("first renew = (%#v,%v)", first, err)
	}
	secondAt := readyAt.Add(3 * time.Second)
	secondSpec := RenewPeerInboxSemanticSpec{Fence: first.Fence(), At: secondAt}
	second, err := fixture.store.RenewPeerInboxSemantic(context.Background(), secondSpec)
	if err != nil || !second.Changed() {
		t.Fatalf("second renew = (%#v,%v)", second, err)
	}
	if _, err := fixture.store.RenewPeerInboxSemantic(context.Background(), firstSpec); !errors.Is(err, ErrPeerInboxSemanticStale) {
		t.Fatalf("superseded F0 renew replay error = %v", err)
	}
	replayed, err := fixture.store.RenewPeerInboxSemantic(context.Background(), secondSpec)
	if err != nil || !replayed.Replayed() || replayed.Changed() ||
		replayed.Fence().LeaseUntil() != second.Fence().LeaseUntil() {
		t.Fatalf("latest F1 renew replay = (%#v,%v)", replayed, err)
	}
	assertPeerInboxSemanticTransitionReceipt(t, fixture.store, inboxID, "renew", 1,
		first.Fence().LeaseUntil(), second.Fence().LeaseUntil())
}

func TestPeerInboxSemanticRetryReceiptReplaysWithoutReauthorizing(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxFixture(t, "semantic-retry-authority-drift", 0)
	publication, _, _ := peerInboxSemanticCurrentWorkPublication(t, fixture,
		"semantic-retry-authority-drift", 1, 1)
	put := fixture.put(t, publication, fixture.at)
	readyAt := fixture.at.Add(time.Second)
	markPeerInboxSemanticReady(t, fixture.store, put.InboxID, readyAt)
	claim := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-retry-authority-worker",
		readyAt.Add(time.Second))
	retryAt := readyAt.Add(2 * time.Second)
	spec := RetryPeerInboxSemanticSpec{Fence: claim.Fence(),
		Diagnostic: PeerInboxSemanticRetryTimeout, RetryAfter: 5 * time.Second, At: retryAt}
	first, err := fixture.store.RetryPeerInboxSemantic(context.Background(), spec)
	if err != nil || !first.Changed() {
		t.Fatalf("retry = (%#v,%v)", first, err)
	}
	driftAt := retryAt.Add(time.Second)
	mustExec(t, fixture.store, `UPDATE channels SET topic_state='blocked',updated_at=?
		WHERE channel_id=?`, storeTime(driftAt), fixture.channel.Channel().ID().String())
	mustExec(t, fixture.store, `UPDATE works SET state_json=? WHERE home_peer_id=? AND work_id=?`,
		[]byte(`{"snapshot":"changed-after-retry"}`),
		publication.Event().Scope().WorkRef().HomePeerID().String(),
		publication.Event().Scope().WorkRef().WorkID().String())
	replayed, err := fixture.store.RetryPeerInboxSemantic(context.Background(), spec)
	if err != nil || !replayed.Replayed() || replayed.Changed() ||
		replayed.NextAttemptAt() != first.NextAttemptAt() {
		t.Fatalf("retry replay after authority/snapshot drift = (%#v,%v)", replayed, err)
	}
	_, err = fixture.store.RetryPeerInboxSemantic(context.Background(), RetryPeerInboxSemanticSpec{
		Fence: claim.Fence(), Diagnostic: PeerInboxSemanticRetryBusy,
		RetryAfter: 5 * time.Second, At: retryAt})
	if !errors.Is(err, ErrPeerInboxSemanticAuthority) {
		t.Fatalf("nonmatching retry after authority drift error = %v", err)
	}
}

func TestPeerInboxSemanticClaimSupersedesTransitionReceipt(t *testing.T) {
	t.Parallel()
	fixture, inboxID, readyAt := newReadyPeerInboxSemantic(t, "semantic-receipt-claim")
	claim := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-receipt-claim-worker",
		readyAt.Add(time.Second))
	retryAt := readyAt.Add(2 * time.Second)
	retry, err := fixture.store.RetryPeerInboxSemantic(context.Background(),
		RetryPeerInboxSemanticSpec{Fence: claim.Fence(), Diagnostic: PeerInboxSemanticRetryBusy,
			RetryAfter: time.Second, At: retryAt})
	if err != nil || !retry.Changed() {
		t.Fatalf("retry = (%#v,%v)", retry, err)
	}
	assertPeerInboxSemanticTransitionReceipt(t, fixture.store, inboxID, "retry", 1,
		claim.Fence().LeaseUntil(), time.Time{})
	reclaimed := mustClaimPeerInboxSemantic(t, fixture.store,
		"semantic-receipt-reclaim-worker", retry.NextAttemptAt())
	if reclaimed.Fence().Attempt() != 2 {
		t.Fatalf("reclaim attempt = %d, want 2", reclaimed.Fence().Attempt())
	}
	var receipts int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM peer_inbox_semantic_transition_receipts
		WHERE inbox_id=?`, inboxID.String()).Scan(&receipts); err != nil || receipts != 0 {
		t.Fatalf("receipt after reclaim = (%d,%v), want zero", receipts, err)
	}
	if _, err := fixture.store.RetryPeerInboxSemantic(context.Background(),
		RetryPeerInboxSemanticSpec{Fence: claim.Fence(), Diagnostic: PeerInboxSemanticRetryBusy,
			RetryAfter: time.Second, At: retryAt}); !errors.Is(err, ErrPeerInboxSemanticStale) {
		t.Fatalf("old retry after receipt deletion error = %v", err)
	}
}

func TestPeerInboxSemanticConcurrentRenewAndRetryHaveOneWinner(t *testing.T) {
	t.Parallel()
	fixture, _, readyAt := newReadyPeerInboxSemantic(t, "semantic-transition-race")
	claim := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-transition-race-worker",
		readyAt.Add(time.Second))
	at := readyAt.Add(2 * time.Second)
	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(2)
	var renewal PeerInboxSemanticRenewal
	var retry PeerInboxSemanticRetry
	var renewErr, retryErr error
	go func() {
		defer wait.Done()
		<-start
		renewal, renewErr = fixture.store.RenewPeerInboxSemantic(context.Background(),
			RenewPeerInboxSemanticSpec{Fence: claim.Fence(), At: at})
	}()
	go func() {
		defer wait.Done()
		<-start
		retry, retryErr = fixture.store.RetryPeerInboxSemantic(context.Background(),
			RetryPeerInboxSemanticSpec{Fence: claim.Fence(), Diagnostic: PeerInboxSemanticRetryBusy,
				RetryAfter: time.Second, At: at})
	}()
	close(start)
	wait.Wait()
	winners := 0
	if renewal.Changed() && renewErr == nil {
		winners++
	} else if !errors.Is(renewErr, ErrPeerInboxSemanticStale) {
		t.Fatalf("renew loser = (%#v,%v)", renewal, renewErr)
	}
	if retry.Changed() && retryErr == nil {
		winners++
	} else if !errors.Is(retryErr, ErrPeerInboxSemanticStale) {
		t.Fatalf("retry loser = (%#v,%v)", retry, retryErr)
	}
	if winners != 1 {
		t.Fatalf("renew/retry winners = %d, renew (%#v,%v), retry (%#v,%v)",
			winners, renewal, renewErr, retry, retryErr)
	}
	var receipts int
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM peer_inbox_semantic_transition_receipts`).
		Scan(&receipts); err != nil || receipts != 1 {
		t.Fatalf("transition receipt count = (%d,%v), want one", receipts, err)
	}
}

func TestPeerInboxSemanticTransitionReceiptTamperAndRollbackFailClosed(t *testing.T) {
	t.Parallel()
	t.Run("request digest tamper", func(t *testing.T) {
		fixture, inboxID, readyAt := newReadyPeerInboxSemantic(t, "semantic-receipt-digest-tamper")
		claim := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-receipt-digest-worker",
			readyAt.Add(time.Second))
		renewAt := readyAt.Add(2 * time.Second)
		spec := RenewPeerInboxSemanticSpec{Fence: claim.Fence(), At: renewAt}
		if _, err := fixture.store.RenewPeerInboxSemantic(context.Background(), spec); err != nil {
			t.Fatal(err)
		}
		mustExec(t, fixture.store, `UPDATE peer_inbox_semantic_transition_receipts
			SET request_digest=? WHERE inbox_id=?`, model.Sum([]byte("tampered request")).Bytes(),
			inboxID.String())
		if _, err := fixture.store.RenewPeerInboxSemantic(context.Background(), spec); !errors.Is(err, ErrPeerInboxSemanticInvariant) {
			t.Fatalf("tampered receipt error = %v", err)
		}
	})
	t.Run("result projection tamper", func(t *testing.T) {
		fixture, inboxID, readyAt := newReadyPeerInboxSemantic(t, "semantic-receipt-output-tamper")
		claim := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-receipt-output-worker",
			readyAt.Add(time.Second))
		renewAt := readyAt.Add(2 * time.Second)
		spec := RenewPeerInboxSemanticSpec{Fence: claim.Fence(), At: renewAt}
		renewed, err := fixture.store.RenewPeerInboxSemantic(context.Background(), spec)
		if err != nil {
			t.Fatal(err)
		}
		mustExec(t, fixture.store, `UPDATE peer_inbox SET next_attempt_at=? WHERE inbox_id=?`,
			storeTime(renewAt.Add(time.Second)), inboxID.String())
		if _, err := fixture.store.RenewPeerInboxSemantic(context.Background(), spec); !errors.Is(err, ErrPeerInboxSemanticInvariant) {
			t.Fatalf("receipt/output mismatch error = %v", err)
		}
		_, err = fixture.store.RenewPeerInboxSemantic(context.Background(),
			RenewPeerInboxSemanticSpec{Fence: renewed.Fence(), At: renewAt.Add(time.Second)})
		if !errors.Is(err, ErrPeerInboxSemanticInvariant) {
			t.Fatalf("nonmatching renew laundered receipt/output mismatch: %v", err)
		}
		assertPeerInboxSemanticTransitionReceipt(t, fixture.store, inboxID, "renew", 1,
			claim.Fence().LeaseUntil(), renewed.Fence().LeaseUntil())
	})
	t.Run("due claim cannot launder result projection tamper", func(t *testing.T) {
		fixture, inboxID, readyAt := newReadyPeerInboxSemantic(t, "semantic-receipt-claim-tamper")
		claim := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-receipt-claim-tamper-worker",
			readyAt.Add(time.Second))
		retryAt := readyAt.Add(2 * time.Second)
		retry, err := fixture.store.RetryPeerInboxSemantic(context.Background(),
			RetryPeerInboxSemanticSpec{Fence: claim.Fence(), Diagnostic: PeerInboxSemanticRetryBusy,
				RetryAfter: time.Second, At: retryAt})
		if err != nil {
			t.Fatal(err)
		}
		tamperedNext := retry.NextAttemptAt().Add(time.Nanosecond)
		mustExec(t, fixture.store, `UPDATE peer_inbox SET next_attempt_at=? WHERE inbox_id=?`,
			storeTime(tamperedNext), inboxID.String())
		result, err := fixture.store.ClaimPeerInboxSemantic(context.Background(),
			ClaimPeerInboxSemanticSpec{LeaseOwner: "semantic-receipt-claim-after-tamper", At: tamperedNext})
		if !errors.Is(err, ErrPeerInboxSemanticInvariant) || result.Found() {
			t.Fatalf("due claim laundered receipt/output mismatch = (found %t,%v)", result.Found(), err)
		}
		assertPeerInboxSemanticState(t, fixture.store, inboxID, "retry", 1,
			string(PeerInboxSemanticRetryBusy), false)
		assertPeerInboxSemanticTransitionReceipt(t, fixture.store, inboxID, "retry", 1,
			claim.Fence().LeaseUntil(), time.Time{})
	})
	t.Run("receipt write rollback", func(t *testing.T) {
		fixture, inboxID, readyAt := newReadyPeerInboxSemantic(t, "semantic-receipt-rollback")
		claim := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-receipt-rollback-worker",
			readyAt.Add(time.Second))
		mustExec(t, fixture.store, `CREATE TEMP TRIGGER reject_semantic_transition_receipt
			BEFORE INSERT ON peer_inbox_semantic_transition_receipts
			BEGIN SELECT RAISE(ABORT,'injected transition receipt failure'); END`)
		_, err := fixture.store.RenewPeerInboxSemantic(context.Background(),
			RenewPeerInboxSemanticSpec{Fence: claim.Fence(), At: readyAt.Add(2 * time.Second)})
		if !errors.Is(err, ErrPeerInboxSemanticInvariant) {
			t.Fatalf("receipt write failure error = %v", err)
		}
		assertPeerInboxSemanticState(t, fixture.store, inboxID, "processing", 1, "", true)
		if err := fixture.store.ProbePeerInboxSemanticAuthority(context.Background(),
			ProbePeerInboxSemanticAuthoritySpec{Fence: claim.Fence(), At: readyAt.Add(2 * time.Second)}); err != nil {
			t.Fatalf("original fence after receipt rollback = %v", err)
		}
		var receipts int
		if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM peer_inbox_semantic_transition_receipts`).
			Scan(&receipts); err != nil || receipts != 0 {
			t.Fatalf("receipt count after rollback = (%d,%v)", receipts, err)
		}
	})
}

func TestPeerInboxSemanticFailsClosedOnAuthorityArtifactAndSnapshotDrift(t *testing.T) {
	t.Parallel()
	t.Run("Channel authority", func(t *testing.T) {
		fixture, _, readyAt := newReadyPeerInboxSemantic(t, "semantic-authority")
		claim := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-authority-worker",
			readyAt.Add(time.Second))
		driftAt := readyAt.Add(2 * time.Second)
		mustExec(t, fixture.store, `UPDATE channels SET topic_state='blocked',updated_at=?
			WHERE channel_id=?`, storeTime(driftAt), fixture.channel.Channel().ID().String())
		if err := fixture.store.ProbePeerInboxSemanticAuthority(context.Background(),
			ProbePeerInboxSemanticAuthoritySpec{Fence: claim.Fence(), At: driftAt}); !errors.Is(err, ErrPeerInboxSemanticAuthority) {
			t.Fatalf("authority drift probe error = %v", err)
		}
	})
	t.Run("permanent Inbox pin", func(t *testing.T) {
		fixture, inboxID, root, readyAt := newReadyPeerInboxSemanticArtifact(t,
			"semantic-pin-drift")
		mustExec(t, fixture.store, `DELETE FROM artifact_pins WHERE root_digest=?
			AND owner_kind='inbox' AND owner_id=?`, root.String(), inboxID.String())
		result, err := fixture.store.ClaimPeerInboxSemantic(context.Background(),
			ClaimPeerInboxSemanticSpec{LeaseOwner: "semantic-pin-worker", At: readyAt.Add(time.Second)})
		if !errors.Is(err, ErrPeerInboxSemanticInvariant) || result.Found() {
			t.Fatalf("missing pin claim = (found %t,%v)", result.Found(), err)
		}
		assertPeerInboxSemanticState(t, fixture.store, inboxID, "ready", 1, "", false)
	})
	t.Run("verified closure", func(t *testing.T) {
		fixture, inboxID, root, readyAt := newReadyPeerInboxSemanticArtifact(t,
			"semantic-closure-drift")
		mustExec(t, fixture.store, `DROP TRIGGER artifact_roots_no_unverify`)
		mustExec(t, fixture.store, `DROP TRIGGER artifact_roots_verified_at_immutable`)
		mustExec(t, fixture.store, `UPDATE artifact_roots SET state='staged',verified_at=NULL
			WHERE root_digest=?`, root.String())
		result, err := fixture.store.ClaimPeerInboxSemantic(context.Background(),
			ClaimPeerInboxSemanticSpec{LeaseOwner: "semantic-closure-worker", At: readyAt.Add(time.Second)})
		if !errors.Is(err, ErrPeerInboxSemanticInvariant) || result.Found() {
			t.Fatalf("unverified closure claim = (found %t,%v)", result.Found(), err)
		}
		assertPeerInboxSemanticState(t, fixture.store, inboxID, "ready", 1, "", false)
	})
	t.Run("signed publication tamper", func(t *testing.T) {
		fixture, inboxID, readyAt := newReadyPeerInboxSemantic(t, "semantic-publication-tamper")
		mustExec(t, fixture.store, `DROP TRIGGER peer_inbox_identity_immutable`)
		mustExec(t, fixture.store, `UPDATE peer_inbox SET origin_signature=zeroblob(64)
			WHERE inbox_id=?`, inboxID.String())
		result, err := fixture.store.ClaimPeerInboxSemantic(context.Background(),
			ClaimPeerInboxSemanticSpec{LeaseOwner: "semantic-tamper-worker", At: readyAt.Add(time.Second)})
		if !errors.Is(err, ErrPeerInboxSemanticInvariant) || result.Found() {
			t.Fatalf("tampered publication claim = (found %t,%v)", result.Found(), err)
		}
		assertPeerInboxSemanticState(t, fixture.store, inboxID, "ready", 0, "", false)
	})
	t.Run("current Work snapshot", func(t *testing.T) {
		fixture := newPeerInboxFixture(t, "semantic-work-drift", 0)
		publication, _, _ := peerInboxSemanticCurrentWorkPublication(t, fixture,
			"semantic-work-drift", 1, 1)
		put := fixture.put(t, publication, fixture.at)
		readyAt := fixture.at.Add(time.Second)
		markPeerInboxSemanticReady(t, fixture.store, put.InboxID, readyAt)
		claim := mustClaimPeerInboxSemantic(t, fixture.store, "semantic-work-worker",
			readyAt.Add(time.Second))
		mustExec(t, fixture.store, `UPDATE works SET state_json=? WHERE home_peer_id=? AND work_id=?`,
			[]byte(`{"snapshot":"changed"}`), publication.Event().Scope().WorkRef().HomePeerID().String(),
			publication.Event().Scope().WorkRef().WorkID().String())
		if err := fixture.store.ProbePeerInboxSemanticAuthority(context.Background(),
			ProbePeerInboxSemanticAuthoritySpec{Fence: claim.Fence(), At: readyAt.Add(2 * time.Second)}); !errors.Is(err, ErrPeerInboxSemanticInvariant) {
			t.Fatalf("Work snapshot drift probe error = %v", err)
		}
	})
}

func TestPeerInboxSemanticClaimRollsBackAndHasOneConcurrentWinner(t *testing.T) {
	t.Parallel()
	t.Run("rollback", func(t *testing.T) {
		fixture, inboxID, readyAt := newReadyPeerInboxSemantic(t, "semantic-rollback")
		mustExec(t, fixture.store, `CREATE TEMP TRIGGER reject_semantic_claim
			BEFORE UPDATE OF status ON peer_inbox WHEN NEW.status='processing'
			BEGIN SELECT RAISE(ABORT,'injected semantic claim failure'); END`)
		result, err := fixture.store.ClaimPeerInboxSemantic(context.Background(),
			ClaimPeerInboxSemanticSpec{LeaseOwner: "semantic-rollback-worker", At: readyAt.Add(time.Second)})
		if err == nil || result.Found() {
			t.Fatalf("injected claim = (found %t,%v)", result.Found(), err)
		}
		assertPeerInboxSemanticState(t, fixture.store, inboxID, "ready", 0, "", false)
	})
	t.Run("concurrent winner", func(t *testing.T) {
		fixture, inboxID, readyAt := newReadyPeerInboxSemantic(t, "semantic-concurrent")
		claimAt := readyAt.Add(time.Second)
		start := make(chan struct{})
		var wait sync.WaitGroup
		wait.Add(2)
		results := make([]PeerInboxSemanticClaimResult, 2)
		errorsFound := make([]error, 2)
		for index := range results {
			go func(index int) {
				defer wait.Done()
				<-start
				results[index], errorsFound[index] = fixture.store.ClaimPeerInboxSemantic(
					context.Background(), ClaimPeerInboxSemanticSpec{
						LeaseOwner: "semantic-concurrent-worker-" + string(rune('a'+index)), At: claimAt})
			}(index)
		}
		close(start)
		wait.Wait()
		winners := 0
		for index := range results {
			if errorsFound[index] != nil {
				t.Fatalf("concurrent claim %d error = %v", index, errorsFound[index])
			}
			if results[index].Found() {
				winners++
			}
		}
		if winners != 1 {
			t.Fatalf("concurrent winners = %d, want 1", winners)
		}
		assertPeerInboxSemanticState(t, fixture.store, inboxID, "processing", 1, "", true)
	})
}

func mustClaimPeerInboxSemantic(t *testing.T, store *Store, owner string,
	at time.Time,
) PeerInboxSemanticClaim {
	t.Helper()
	result, err := store.ClaimPeerInboxSemantic(context.Background(),
		ClaimPeerInboxSemanticSpec{LeaseOwner: owner, At: at})
	if err != nil || !result.Found() {
		t.Fatalf("ClaimPeerInboxSemantic(%q) = (found %t,%v)", owner, result.Found(), err)
	}
	return result.Claim()
}

func newReadyPeerInboxSemantic(t *testing.T,
	seed string,
) (peerInboxFixture, model.InboxID, time.Time) {
	t.Helper()
	fixture := newPeerInboxFixture(t, seed, 0)
	publication := fixture.publication(t, 1, 1, seed, true)
	put := fixture.put(t, publication, fixture.at)
	readyAt := fixture.at.Add(time.Second)
	markPeerInboxSemanticReady(t, fixture.store, put.InboxID, readyAt)
	return fixture, put.InboxID, readyAt
}

func newReadyPeerInboxSemanticArtifact(t *testing.T,
	seed string,
) (peerInboxFixture, model.InboxID, model.Digest, time.Time) {
	t.Helper()
	fixture, artifactClaim, root, closure := newPeerInboxArtifactClosureClaim(t, seed, true)
	stageAt := fixture.at.Add(time.Second)
	_, owner := mustPreparePeerInboxArtifactPublish(t, fixture.store,
		artifactClaim.Fence(), closure, stageAt)
	mustAcceptPeerInboxArtifactPublish(t, fixture.store,
		artifactClaim.Fence(), owner, stageAt)
	readyAt := stageAt.Add(time.Second)
	mustMarkPeerInboxArtifactReady(t, fixture.store,
		artifactClaim.Fence(), owner, readyAt)
	return fixture, artifactClaim.InboxID(), root.RootDigest, readyAt
}

func markPeerInboxSemanticReady(t *testing.T, store *Store, inboxID model.InboxID,
	at time.Time,
) {
	t.Helper()
	result, err := store.db.Exec(`UPDATE peer_inbox SET status='ready',next_attempt_at=?,
		lease_owner=NULL,lease_until=NULL,diagnostic=NULL,updated_at=?
		WHERE inbox_id=? AND status='stored'`, storeTime(at), storeTime(at), inboxID.String())
	if err != nil {
		t.Fatal(err)
	}
	if err := requireExactlyOneRow(result, "test mark semantic ready"); err != nil {
		t.Fatal(err)
	}
}

func peerInboxSemanticCurrentWorkPublication(t *testing.T, fixture peerInboxFixture,
	seed string, originSequence, channelSequence uint64,
) (model.SignedPublication, model.ReviewWork, model.Event) {
	t.Helper()
	local := fixture.channel.Owner()
	remote := fixture.remote.Identity()
	workID, err := model.ParseWorkID("work-inbox-" + seed)
	if err != nil {
		t.Fatal(err)
	}
	workRef, err := model.NewWorkRef(local.PeerID(), workID)
	if err != nil {
		t.Fatal(err)
	}
	localMember, ok := fixture.channel.Roster().CurrentMember(local.PeerID())
	if !ok {
		t.Fatal("local member missing from fixture roster")
	}
	offerScope, err := model.NewEventScope(fixture.channel.Channel().ID(), local.PeerID(),
		local.OriginEpoch(), 100, 100, localMember.Head(), fixture.channel.Roster().Head(), workRef)
	if err != nil {
		t.Fatal(err)
	}
	audience, err := model.NewAudience([]model.PeerID{remote.PeerID()})
	if err != nil {
		t.Fatal(err)
	}
	offerID, _ := model.ParseEventID("event-inbox-" + seed + "-offered")
	offerAt := fixture.at.Add(-10 * time.Second)
	deadline := fixture.at.Add(time.Hour)
	offerPayload, _ := model.JSONFrom(struct {
		Content     string `json:"content"`
		Deadline    string `json:"deadline"`
		Iteration   uint8  `json:"iteration"`
		WorkVersion uint64 `json:"work_version"`
	}{"semantic snapshot offer", deadline.UTC().Format(time.RFC3339Nano), 1, 1})
	offerPublication := fixture.signEventAs(t, model.EventSpec{ID: offerID, Scope: offerScope,
		Source: model.EventSourceLocal, ActorPrincipal: "principal-semantic-local",
		Type: model.EventReviewOffered, Audience: audience, Summary: "semantic snapshot offer",
		Payload: offerPayload, CreatedAt: offerAt, AcceptedAt: offerAt}, local)
	participants, err := model.NewParticipantSnapshot(fixture.channel.Channel().ID(),
		fixture.channel.Roster().Head().Revision(), local.PeerID(), remote.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	work, err := model.NewReviewWork(model.ReviewWorkSpec{Ref: workRef,
		ChannelID: fixture.channel.Channel().ID(), Participants: participants, Version: 1,
		Iteration: 1, DeadlineUnixNano: deadline.UnixNano(),
		State: model.WorkOffered, StateData: offerPayload, UpdatedBy: offerID, UpdatedAt: offerAt})
	if err != nil {
		t.Fatal(err)
	}
	mutation, err := NewWorkCreation(work)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := insertAcceptedEvent(context.Background(), tx, offerPublication); err != nil {
		t.Fatal(err)
	}
	if err := applyWorkMutation(context.Background(), tx, mutation, offerPublication.Event()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	remoteScope, err := model.NewEventScope(fixture.channel.Channel().ID(), remote.PeerID(),
		remote.OriginEpoch(), originSequence, channelSequence, fixture.remote.Member().Head(),
		fixture.channel.Roster().Head(), workRef)
	if err != nil {
		t.Fatal(err)
	}
	localAudience, _ := model.NewAudience([]model.PeerID{local.PeerID()})
	requestID, _ := model.ParseEventID("event-inbox-" + seed + "-request")
	requestPayload, _ := model.NewJSON([]byte(`{"iteration":1,"work_version":1}`))
	request := fixture.signEvent(t, model.EventSpec{ID: requestID, Scope: remoteScope,
		Source: model.EventSourceLocal, ActorPrincipal: "principal-semantic-remote",
		Type: model.EventReviewAcceptRequested, Audience: localAudience,
		Summary: "semantic snapshot request", Payload: requestPayload,
		CausedBy:  []model.EventKey{offerPublication.Event().Key()},
		CreatedAt: fixture.at.Add(-time.Second), AcceptedAt: fixture.at.Add(-time.Second)})
	return request, work, offerPublication.Event()
}

func assertPeerInboxSemanticTransitionReceipt(t *testing.T, store *Store,
	inboxID model.InboxID, wantKind string, wantAttempt uint32, wantOldLease,
	wantOutputLease time.Time,
) {
	t.Helper()
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM peer_inbox_semantic_transition_receipts`).
		Scan(&count); err != nil || count != 1 {
		t.Fatalf("transition receipt count = (%d,%v), want one", count, err)
	}
	var kind, oldLeaseText string
	var attempt uint32
	var outputLease sqlNullString
	if err := store.db.QueryRow(`SELECT transition_kind,old_attempt,old_lease_until,
		output_lease_until FROM peer_inbox_semantic_transition_receipts WHERE inbox_id=?`,
		inboxID.String()).Scan(&kind, &attempt, &oldLeaseText, &outputLease); err != nil {
		t.Fatal(err)
	}
	if kind != wantKind || attempt != wantAttempt || oldLeaseText != storeTime(wantOldLease) ||
		outputLease.Valid != !wantOutputLease.IsZero() ||
		outputLease.Valid && outputLease.String != storeTime(wantOutputLease) {
		t.Fatalf("transition receipt = (%q,%d,%q,%#v), want (%q,%d,%q,%q)", kind,
			attempt, oldLeaseText, outputLease, wantKind, wantAttempt, storeTime(wantOldLease),
			storeTime(wantOutputLease))
	}
}

func assertPeerInboxSemanticState(t *testing.T, store *Store, inboxID model.InboxID,
	wantStatus string, wantAttempt uint32, wantDiagnostic string, wantLease bool,
) {
	t.Helper()
	var status string
	var attempt uint32
	var owner, lease, diagnostic sqlNullString
	if err := store.db.QueryRow(`SELECT status,attempts,lease_owner,lease_until,diagnostic
		FROM peer_inbox WHERE inbox_id=?`, inboxID.String()).Scan(&status, &attempt,
		&owner, &lease, &diagnostic); err != nil {
		t.Fatal(err)
	}
	if status != wantStatus || attempt != wantAttempt || owner.Valid != wantLease ||
		lease.Valid != wantLease || diagnostic.String != wantDiagnostic ||
		diagnostic.Valid != (wantDiagnostic != "") {
		t.Fatalf("semantic Inbox state = (%q,%d,%#v,%#v,%#v), want (%q,%d,%q,lease %t)",
			status, attempt, owner, lease, diagnostic, wantStatus, wantAttempt, wantDiagnostic, wantLease)
	}
}

func assertPeerInboxSemanticNoDomainMutation(t *testing.T, store *Store,
	incoming model.EventID,
) {
	t.Helper()
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_id=?`,
		incoming.String()).Scan(&count); err != nil || count != 0 {
		t.Fatalf("imported domain Event count = (%d,%v), want zero", count, err)
	}
}
