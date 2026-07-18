package store

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestGossipPublicationLeaseRetryPublishAndExactReplay(t *testing.T) {
	fixture := newAcceptanceFixture(t, 1)
	_, authority := fixture.reserveOffer(t, "gossip-lifecycle", nil)
	acceptance := fixture.offer(t, authority, "gossip-lifecycle", fixture.reviewers, nil, nil)
	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), acceptance,
		fixture.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	claimAt := fixture.now.Add(2 * time.Second)
	first := claimPublication(t, fixture.store, fixture.channel, "gossip-worker-first",
		claimAt, claimAt.Add(time.Minute))
	if first.Lease.Record.Status() != model.PublicationLeased ||
		first.Lease.Record.Attempts() != 1 ||
		!bytes.Equal(first.Lease.Record.Publication().WireJSON().Bytes(),
			acceptance.Items[0].Publication.WireJSON().Bytes()) {
		t.Fatalf("first publication lease = %#v", first.Lease)
	}

	retryAt := claimAt.Add(time.Second)
	nextAttempt := retryAt.Add(5 * time.Second)
	retrySpec := RetryGossipPublicationSpec{Fence: first.Lease.Fence, At: retryAt,
		NextAttemptAt: nextAttempt, Diagnostic: "temporary Gossip router pressure"}
	retried, err := fixture.store.RetryGossipPublication(context.Background(), retrySpec)
	if err != nil || !retried.Changed || retried.Replayed ||
		retried.Record.Status() != model.PublicationQueued || retried.Record.Attempts() != 1 ||
		!retried.Record.NextAttemptAt().Equal(nextAttempt) ||
		retried.Record.LastError() != retrySpec.Diagnostic {
		t.Fatalf("RetryGossipPublication() = (%#v,%v)", retried, err)
	}
	replayedRetry, err := fixture.store.RetryGossipPublication(context.Background(), retrySpec)
	if err != nil || replayedRetry.Changed || !replayedRetry.Replayed ||
		!replayedRetry.Record.UpdatedAt().Equal(retryAt) {
		t.Fatalf("retry replay = (%#v,%v)", replayedRetry, err)
	}
	assertForgedGossipPublicationReplaysFail(t, retrySpec.Fence, func(fence GossipPublicationFence) error {
		forged := retrySpec
		forged.Fence = fence
		_, err := fixture.store.RetryGossipPublication(context.Background(), forged)
		return err
	})
	if _, err := fixture.store.MarkGossipPublicationPublished(context.Background(),
		MarkGossipPublicationPublishedSpec{Fence: first.Lease.Fence, At: retryAt}); !errors.Is(err, ErrGossipPublicationStale) {
		t.Fatalf("settled attempt completed by stale worker: %v", err)
	}

	second := claimPublication(t, fixture.store, fixture.channel, "gossip-worker-second",
		nextAttempt, nextAttempt.Add(time.Minute))
	if second.Lease.Record.Attempts() != 2 || second.Lease.Fence.Attempt != 2 ||
		!bytes.Equal(second.Lease.Record.Publication().WireJSON().Bytes(),
			acceptance.Items[0].Publication.WireJSON().Bytes()) {
		t.Fatalf("second publication lease = %#v", second.Lease)
	}
	publishedAt := nextAttempt.Add(time.Second)
	markSpec := MarkGossipPublicationPublishedSpec{Fence: second.Lease.Fence, At: publishedAt}
	published, err := fixture.store.MarkGossipPublicationPublished(context.Background(), markSpec)
	if err != nil || !published.Changed || published.Replayed ||
		published.Record.Status() != model.PublicationPublished {
		t.Fatalf("MarkGossipPublicationPublished() = (%#v,%v)", published, err)
	}
	if at, ok := published.Record.PublishedAt(); !ok || !at.Equal(publishedAt) {
		t.Fatalf("published_at = (%v,%t), want %v", at, ok, publishedAt)
	}
	replayedMark, err := fixture.store.MarkGossipPublicationPublished(context.Background(), markSpec)
	if err != nil || replayedMark.Changed || !replayedMark.Replayed ||
		!replayedMark.Record.UpdatedAt().Equal(publishedAt) {
		t.Fatalf("published replay = (%#v,%v)", replayedMark, err)
	}
	assertForgedGossipPublicationReplaysFail(t, markSpec.Fence, func(fence GossipPublicationFence) error {
		forged := markSpec
		forged.Fence = fence
		_, err := fixture.store.MarkGossipPublicationPublished(context.Background(), forged)
		return err
	})
}

func TestGossipPublicationConcurrentClaimHasOneOwnerGeneration(t *testing.T) {
	fixture := acceptedGossipFixture(t, "gossip-concurrent")
	at := fixture.now.Add(2 * time.Second)
	const workers = 12
	start := make(chan struct{})
	results := make(chan GossipPublicationClaimResult, workers)
	errs := make(chan error, workers)
	var wait sync.WaitGroup
	for index := 0; index < workers; index++ {
		index := index
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			result, err := fixture.store.ClaimGossipPublication(context.Background(),
				GossipPublicationClaimSpec{ChannelID: fixture.channel,
					LeaseOwner: fmt.Sprintf("gossip-concurrent-%02d", index), At: at,
					LeaseUntil: at.Add(time.Minute)})
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
			t.Fatalf("concurrent claim error = %v", err)
		}
	}
	claimed := 0
	var winner GossipPublicationLease
	for result := range results {
		if result.Claimed {
			claimed++
			winner = result.Lease
		}
	}
	if claimed != 1 || winner.Fence.Attempt != 1 {
		t.Fatalf("concurrent claimed count/fence = %d/%#v", claimed, winner.Fence)
	}
	var status, owner string
	var attempts int
	if err := fixture.store.db.QueryRow(`SELECT status,attempts,lease_owner FROM gossip_publications`).Scan(
		&status, &attempts, &owner); err != nil || status != "leased" || attempts != 1 ||
		owner != winner.Fence.LeaseOwner {
		t.Fatalf("durable concurrent winner = (%q,%d,%q,%v)", status, attempts, owner, err)
	}
}

func TestGossipPublicationLeaseRecoversAfterRestartAndPublishCrash(t *testing.T) {
	fixture, acceptance := acceptedGossipFixtureWithPublication(t, "gossip-restart")
	claimAt := fixture.now.Add(2 * time.Second)
	first := claimPublication(t, fixture.store, fixture.channel, "gossip-before-crash",
		claimAt, claimAt.Add(time.Minute))
	original := append([]byte(nil), first.Lease.Record.Publication().WireJSON().Bytes()...)

	// This models both crash windows: the worker may have stopped before
	// Topic.Publish or after Topic.Publish but before its durable mark.
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.store = nil
	restarted, err := Open(context.Background(), fixture.path)
	if err != nil {
		t.Fatal(err)
	}
	fixture.store = restarted
	beforeExpiry, err := restarted.ClaimGossipPublication(context.Background(),
		GossipPublicationClaimSpec{ChannelID: fixture.channel, LeaseOwner: "gossip-too-early",
			At: claimAt.Add(30 * time.Second), LeaseUntil: claimAt.Add(60 * time.Second)})
	if err != nil || beforeExpiry.Claimed {
		t.Fatalf("claim before lease expiry = (%#v,%v)", beforeExpiry, err)
	}

	recovered := claimPublication(t, restarted, fixture.channel, "gossip-after-restart",
		first.Lease.Fence.LeaseUntil, first.Lease.Fence.LeaseUntil.Add(time.Minute))
	if recovered.Lease.Fence.Attempt != 2 || recovered.Lease.Record.Attempts() != 2 ||
		!bytes.Equal(recovered.Lease.Record.Publication().WireJSON().Bytes(), original) ||
		!bytes.Equal(recovered.Lease.Record.Publication().WireJSON().Bytes(),
			acceptance.WireJSON().Bytes()) {
		t.Fatalf("recovered lease changed immutable publication: %#v", recovered.Lease)
	}
	if _, err := restarted.MarkGossipPublicationPublished(context.Background(),
		MarkGossipPublicationPublishedSpec{Fence: first.Lease.Fence,
			At: first.Lease.Fence.LeaseUntil.Add(-time.Nanosecond)}); !errors.Is(err, ErrGossipPublicationStale) {
		t.Fatalf("crashed worker survived new generation: %v", err)
	}
}

func TestGossipPublicationChannelIsolationAndAuthorityGenerationFence(t *testing.T) {
	fixture, primaryPublication := acceptedGossipFixtureWithPublication(t, "gossip-isolation")
	secondaryChannel, secondaryPublication := installSecondaryGossipPublication(t, fixture,
		"gossip-isolation-secondary", 101)
	at := fixture.now.Add(2 * time.Second)
	primary := claimPublication(t, fixture.store, fixture.channel, "gossip-primary", at,
		at.Add(time.Minute))
	var secondaryStatus string
	if err := fixture.store.db.QueryRow(`SELECT status FROM gossip_publications WHERE channel_id=?`,
		secondaryChannel.String()).Scan(&secondaryStatus); err != nil || secondaryStatus != "queued" {
		t.Fatalf("primary claim changed secondary Channel = (%q,%v)", secondaryStatus, err)
	}
	secondary := claimPublication(t, fixture.store, secondaryChannel, "gossip-secondary", at,
		at.Add(time.Minute))
	if !bytes.Equal(primary.Lease.Record.Publication().WireJSON().Bytes(), primaryPublication.WireJSON().Bytes()) ||
		!bytes.Equal(secondary.Lease.Record.Publication().WireJSON().Bytes(), secondaryPublication.WireJSON().Bytes()) {
		t.Fatal("Channel-scoped claims crossed immutable publications")
	}

	signed := acceptanceSignedChannel(t, fixture)
	newHead := signed.AppendActiveUpdate(t, signed.Owner().PeerID())
	mergeAt := fixture.now.Add(3 * time.Second)
	merged, err := fixture.store.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
		ChannelID: fixture.channel, AuthenticatedTransportPeerID: fixture.reviewers[0],
		Records: []model.Member{newHead.Member()}, At: mergeAt})
	if err != nil || merged.Status != ChannelRosterApplied || merged.Roster.Head().Revision() != 3 {
		t.Fatalf("authority rotation = (%#v,%v)", merged, err)
	}
	if _, err := fixture.store.MarkGossipPublicationPublished(context.Background(),
		MarkGossipPublicationPublishedSpec{Fence: primary.Lease.Fence, At: mergeAt}); !errors.Is(err, ErrGossipPublicationStale) {
		t.Fatalf("old authority generation completed publication: %v", err)
	}
	if _, err := fixture.store.RetryGossipPublication(context.Background(), RetryGossipPublicationSpec{
		Fence: primary.Lease.Fence, At: mergeAt, NextAttemptAt: mergeAt.Add(time.Second),
		Diagnostic: "must not settle after authority rotation"}); !errors.Is(err, ErrGossipPublicationStale) {
		t.Fatalf("old authority generation requeued publication: %v", err)
	}
	mustExec(t, fixture.store, `UPDATE channels SET topic_state='joined',updated_at=? WHERE channel_id=?`,
		storeTime(mergeAt.Add(time.Second)), fixture.channel.String())
	refreshed := claimPublication(t, fixture.store, fixture.channel, "gossip-new-authority",
		primary.Lease.Fence.LeaseUntil, primary.Lease.Fence.LeaseUntil.Add(time.Minute))
	if refreshed.Lease.Fence.RosterHead != merged.Roster.Head() || refreshed.Lease.Fence.Attempt != 2 ||
		!bytes.Equal(refreshed.Lease.Record.Publication().WireJSON().Bytes(),
			primaryPublication.WireJSON().Bytes()) {
		t.Fatalf("fresh authority lease = %#v", refreshed.Lease)
	}
}

func TestGossipPublicationRejectsUnavailableAuthorityAndOversizeDiagnostic(t *testing.T) {
	t.Run("terminal Channel", func(t *testing.T) {
		fixture := acceptedGossipFixture(t, "gossip-terminal")
		signed := acceptanceSignedChannel(t, fixture)
		left := signed.AppendTerminal(t, signed.Owner().PeerID(), model.MemberLeft)
		if _, err := fixture.store.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
			ChannelID: fixture.channel, AuthenticatedTransportPeerID: fixture.reviewers[0],
			Records: []model.Member{left.Member()}, At: fixture.now.Add(2 * time.Second)}); err != nil {
			t.Fatal(err)
		}
		_, err := fixture.store.ClaimGossipPublication(context.Background(), GossipPublicationClaimSpec{
			ChannelID: fixture.channel, LeaseOwner: "gossip-terminal-worker", At: fixture.now.Add(3 * time.Second),
			LeaseUntil: fixture.now.Add(time.Minute)})
		if !errors.Is(err, ErrGossipPublicationAuthority) {
			t.Fatalf("terminal Channel claim error = %v", err)
		}
	})

	t.Run("conflicted Channel", func(t *testing.T) {
		fixture := acceptedGossipFixture(t, "gossip-conflicted")
		signed := acceptanceSignedChannel(t, fixture)
		challenger := ownerConflictChallenger(t, signed, fixture.now.Add(time.Second))
		if _, err := fixture.store.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
			ChannelID: fixture.channel, AuthenticatedTransportPeerID: fixture.reviewers[0],
			Records: []model.Member{challenger}, At: fixture.now.Add(2 * time.Second)}); err != nil {
			t.Fatal(err)
		}
		_, err := fixture.store.ClaimGossipPublication(context.Background(), GossipPublicationClaimSpec{
			ChannelID: fixture.channel, LeaseOwner: "gossip-conflict-worker", At: fixture.now.Add(3 * time.Second),
			LeaseUntil: fixture.now.Add(time.Minute)})
		if !errors.Is(err, ErrGossipPublicationAuthority) {
			t.Fatalf("conflicted Channel claim error = %v", err)
		}
	})

	t.Run("revoked local member", func(t *testing.T) {
		fixture := acceptedGossipFixture(t, "gossip-revoked")
		channel, publication, signed := installJoinedMemberGossipPublication(t, fixture,
			"gossip-local-revoked", 202)
		if publication.Event().Scope().OriginMember().Revision() != 2 {
			t.Fatal("joined-member publication did not use local signed membership")
		}
		revoked := signed.AppendTerminal(t, fixture.node.PeerID(), model.MemberRevoked)
		if _, err := fixture.store.MergeChannelRoster(context.Background(), MergeChannelRosterSpec{
			ChannelID: channel, AuthenticatedTransportPeerID: signed.Owner().PeerID(),
			Records: []model.Member{revoked.Member()}, At: fixture.now.Add(2 * time.Second)}); err != nil {
			t.Fatal(err)
		}
		_, err := fixture.store.ClaimGossipPublication(context.Background(), GossipPublicationClaimSpec{
			ChannelID: channel, LeaseOwner: "gossip-revoked-worker", At: fixture.now.Add(3 * time.Second),
			LeaseUntil: fixture.now.Add(time.Minute)})
		if !errors.Is(err, ErrGossipPublicationAuthority) {
			t.Fatalf("revoked local membership claim error = %v", err)
		}
	})

	t.Run("oversize diagnostic", func(t *testing.T) {
		fixture := acceptedGossipFixture(t, "gossip-diagnostic")
		at := fixture.now.Add(2 * time.Second)
		claim := claimPublication(t, fixture.store, fixture.channel, "gossip-diagnostic-worker",
			at, at.Add(time.Minute))
		_, err := fixture.store.RetryGossipPublication(context.Background(), RetryGossipPublicationSpec{
			Fence: claim.Lease.Fence, At: at.Add(time.Second), NextAttemptAt: at.Add(2 * time.Second),
			Diagnostic: strings.Repeat("x", model.MaxContentBytes+1)})
		if !errors.Is(err, ErrGossipPublicationInput) {
			t.Fatalf("oversize diagnostic error = %v", err)
		}
		var status, owner string
		var attempts int
		if err := fixture.store.db.QueryRow(`SELECT status,attempts,lease_owner FROM gossip_publications`).Scan(
			&status, &attempts, &owner); err != nil || status != "leased" || attempts != 1 ||
			owner != claim.Lease.Fence.LeaseOwner {
			t.Fatalf("oversize diagnostic changed lease = (%q,%d,%q,%v)", status, attempts, owner, err)
		}
	})

	t.Run("SQL error is not stale", func(t *testing.T) {
		fixture := acceptedGossipFixture(t, "gossip-sql-error")
		if _, err := fixture.store.db.Exec(`CREATE TEMP TRIGGER gossip_publication_injected_failure
			BEFORE UPDATE ON gossip_publications
			BEGIN SELECT RAISE(ABORT, 'injected publication storage failure'); END`); err != nil {
			t.Fatal(err)
		}
		at := fixture.now.Add(2 * time.Second)
		_, err := fixture.store.ClaimGossipPublication(context.Background(), GossipPublicationClaimSpec{
			ChannelID: fixture.channel, LeaseOwner: "gossip-storage-failure", At: at,
			LeaseUntil: at.Add(time.Minute)})
		if err == nil || errors.Is(err, ErrGossipPublicationStale) ||
			!strings.Contains(err.Error(), "injected publication storage failure") {
			t.Fatalf("injected SQL claim error = %v", err)
		}
	})
}

func assertForgedGossipPublicationReplaysFail(t *testing.T,
	fence GossipPublicationFence, settle func(GossipPublicationFence) error,
) {
	t.Helper()
	wrongHead, err := model.NewRecordHead(fence.RosterHead.Revision(),
		model.Sum([]byte("forged Gossip publication roster head")))
	if err != nil {
		t.Fatal(err)
	}
	for name, forged := range map[string]GossipPublicationFence{
		"owner": func() GossipPublicationFence {
			changed := fence
			changed.LeaseOwner += "-forged"
			return changed
		}(),
		"until": func() GossipPublicationFence {
			changed := fence
			changed.LeaseUntil = changed.LeaseUntil.Add(time.Second)
			return changed
		}(),
		"head": func() GossipPublicationFence {
			changed := fence
			changed.RosterHead = wrongHead
			return changed
		}(),
	} {
		t.Run("forged replay "+name, func(t *testing.T) {
			if err := settle(forged); !errors.Is(err, ErrGossipPublicationStale) {
				t.Fatalf("forged %s replay error = %v", name, err)
			}
		})
	}
}

func acceptedGossipFixture(t *testing.T, suffix string) *acceptanceFixture {
	t.Helper()
	fixture, _ := acceptedGossipFixtureWithPublication(t, suffix)
	return fixture
}

func acceptedGossipFixtureWithPublication(t *testing.T,
	suffix string,
) (*acceptanceFixture, model.SignedPublication) {
	t.Helper()
	fixture := newAcceptanceFixture(t, 1)
	_, authority := fixture.reserveOffer(t, suffix, nil)
	acceptance := fixture.offer(t, authority, suffix, fixture.reviewers, nil, nil)
	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), acceptance,
		fixture.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	return fixture, acceptance.Items[0].Publication
}

func claimPublication(t *testing.T, st *Store, channelID model.ChannelID, owner string,
	at, leaseUntil time.Time,
) GossipPublicationClaimResult {
	t.Helper()
	result, err := st.ClaimGossipPublication(context.Background(), GossipPublicationClaimSpec{
		ChannelID: channelID, LeaseOwner: owner, At: at, LeaseUntil: leaseUntil})
	if err != nil || !result.Claimed {
		t.Fatalf("ClaimGossipPublication(%s) = (%#v,%v)", channelID.String(), result, err)
	}
	return result
}

func installSecondaryGossipPublication(t *testing.T, fixture *acceptanceFixture,
	seed string, originSequence uint64,
) (model.ChannelID, model.SignedPublication) {
	t.Helper()
	local := testkit.NewIdentity(t, "owner:accept-"+t.Name())
	signed := testkit.NewSignedChannelForOwnerAt(t, seed, local, fixture.now.Add(-30*time.Minute))
	remote := testkit.NewIdentity(t, seed+"-remote")
	remoteMember := signed.AppendActiveIdentity(t, remote)
	insertSignedChannelFixture(t, fixture.store.db, signed, model.TopicJoined)
	installActiveGossipBinding(t, fixture, signed.Channel().ID(), remoteMember, "secondary-remote")
	publication := insertTestGossipPublication(t, fixture, signed, remote.PeerID(),
		signed.OwnerMember().Member().Head(), originSequence, "secondary")
	return signed.Channel().ID(), publication
}

func installJoinedMemberGossipPublication(t *testing.T, fixture *acceptanceFixture,
	seed string, originSequence uint64,
) (model.ChannelID, model.SignedPublication, *testkit.SignedChannel) {
	t.Helper()
	owner := testkit.NewIdentity(t, seed+"-owner")
	local := testkit.NewIdentity(t, "owner:accept-"+t.Name())
	signed := testkit.NewSignedChannelForOwnerAt(t, seed, owner, fixture.now.Add(-30*time.Minute))
	localMember := signed.AppendActiveIdentity(t, local)
	insertSignedChannelFixture(t, fixture.store.db, signed, model.TopicJoined)
	installActiveGossipBinding(t, fixture, signed.Channel().ID(), signed.OwnerMember(), "joined-owner")
	publication := insertTestGossipPublication(t, fixture, signed, owner.PeerID(),
		localMember.Member().Head(), originSequence, "joined")
	return signed.Channel().ID(), publication, signed
}

func installActiveGossipBinding(t *testing.T, fixture *acceptanceFixture, channelID model.ChannelID,
	member testkit.MemberFixture, alias string,
) {
	t.Helper()
	st := fixture.store
	insertSignedPeerBinding(t, st.db, channelID, member, alias, model.BindingPending,
		model.ReachabilityUnknown, member.Member().CreatedAt())
	mustExec(t, st, `INSERT INTO peer_cursors(channel_id,origin_peer_id,origin_epoch,
		baseline_channel_seq,contiguous_channel_seq,observed_channel_seq,updated_at)
		VALUES(?,?,?,0,0,0,?)`, channelID.String(), member.Identity().PeerID().String(),
		member.Identity().OriginEpoch().String(), storeTime(member.Member().CreatedAt()))
	mustExec(t, st, `UPDATE peer_bindings SET state='active' WHERE channel_id=? AND peer_id=?`,
		channelID.String(), member.Identity().PeerID().String())
	mustExec(t, st, `INSERT INTO publication_epochs(channel_id,origin_peer_id,origin_epoch,
		source_floor_channel_seq,source_head_channel_seq,updated_at) VALUES(?,?,?,1,0,?)`,
		channelID.String(), fixture.node.PeerID().String(), fixture.node.OriginEpoch().String(),
		storeTime(member.Member().CreatedAt()))
	mustExec(t, st, `INSERT INTO peer_pull_acks(channel_id,target_peer_id,origin_peer_id,
		origin_epoch,baseline_channel_seq,acknowledged_channel_seq,baseline_confirmed_at,updated_at)
		VALUES(?,?,?,?,0,0,NULL,?)`, channelID.String(), member.Identity().PeerID().String(),
		fixture.node.PeerID().String(), fixture.node.OriginEpoch().String(), storeTime(member.Member().CreatedAt()))
	mustExec(t, st, `UPDATE peer_pull_acks SET baseline_confirmed_at=?
		WHERE channel_id=? AND target_peer_id=?`, storeTime(member.Member().CreatedAt()),
		channelID.String(), member.Identity().PeerID().String())
}

func insertTestGossipPublication(t *testing.T, fixture *acceptanceFixture,
	signed *testkit.SignedChannel, target model.PeerID, originMember model.RecordHead,
	originSequence uint64, suffix string,
) model.SignedPublication {
	t.Helper()
	channelID := signed.Channel().ID()
	workID, _ := model.ParseWorkID(fmt.Sprintf("work-gossip-%s-%d", suffix, originSequence))
	work, _ := model.NewWorkRef(fixture.node.PeerID(), workID)
	scope, err := model.NewEventScope(channelID, fixture.node.PeerID(), fixture.node.OriginEpoch(),
		originSequence, 1, originMember, signed.Roster().Head(), work)
	if err != nil {
		t.Fatal(err)
	}
	audience, _ := model.NewAudience([]model.PeerID{target})
	eventID, _ := model.ParseEventID(fmt.Sprintf("event-gossip-%s-%d", suffix, originSequence))
	payload, _ := model.NewJSON([]byte(`{"content":"gossip worker fixture","deadline":"2026-07-20T13:00:00Z","iteration":1,"work_version":1}`))
	event, err := model.NewEvent(model.EventSpec{ID: eventID, Scope: scope, Source: model.EventSourceLocal,
		ActorPrincipal: fixture.profile.Principal(), Type: model.EventReviewOffered, Audience: audience,
		Summary: "gossip publication fixture", Payload: payload, CreatedAt: fixture.now,
		AcceptedAt: fixture.now})
	if err != nil {
		t.Fatal(err)
	}
	body, err := model.NewPublicationBody(event)
	if err != nil {
		t.Fatal(err)
	}
	message, _ := model.PublicationSigningMessage(channelID, body.Digest())
	publication, err := model.AttachSignature(body, ed25519.Sign(fixture.privateKey, message))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE publication_epochs SET source_head_channel_seq=1,updated_at=?
		WHERE channel_id=? AND origin_peer_id=? AND origin_epoch=? AND source_head_channel_seq=0`,
		storeTime(fixture.now), channelID.String(), fixture.node.PeerID().String(),
		fixture.node.OriginEpoch().String()); err != nil {
		t.Fatal(err)
	}
	if err := insertAcceptedEvent(context.Background(), tx, publication); err != nil {
		t.Fatal(err)
	}
	if err := insertPublicationEvidence(context.Background(), tx, event); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return publication
}
