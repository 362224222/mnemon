package store

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestFinalizeAgentCurrentReadDerivesAndReplaysDurableProjection(t *testing.T) {
	fixture, events := newAgentClaimFixture(t, 1, "current-read")
	claimAt := fixture.now.Add(2 * time.Second)
	handling := insertClaimHandling(t, fixture.store, "handling-current-read", events[0], 10,
		claimAt, claimAt, 0)
	claim := claimCurrent(t, fixture, "owner-current-read", "token-current-read", claimAt)
	readAt := claimAt.Add(time.Second)
	spec := currentReadSpec(fixture, claim.Run.ID(), "token-current-read", readAt)

	result, err := fixture.store.FinalizeAgentCurrentRead(context.Background(), spec)
	if err != nil || result.Replayed {
		t.Fatalf("FinalizeAgentCurrentRead() = (%#v, %v)", result, err)
	}
	if result.Receipt.RunID() != claim.Run.ID() || result.Receipt.HandlingID() != handling.ID() ||
		result.Receipt.HandlingAttempt() != 1 || result.Receipt.SourceEvent().EventID() != events[0] ||
		result.Projection.ActionWork().Version() != 1 ||
		result.Projection.ActionWork().LocalRole() != model.CurrentInitiator {
		t.Fatalf("current binding = %#v", result.Receipt)
	}
	wantActions := []model.OperationKind{model.OperationTeamworkCancel, model.OperationResolveRetry}
	if !sameOperationKinds(result.Projection.AllowedActions(), wantActions) {
		t.Fatalf("allowed actions = %v, want %v", result.Projection.AllowedActions(), wantActions)
	}
	if len(result.Receipt.ArtifactRefs()) != 0 {
		t.Fatalf("empty source Event invented Artifact refs: %v", result.Receipt.ArtifactRefs())
	}
	if strings.Contains(result.Receipt.CanonicalJSON().String(), "token-current-read") ||
		strings.Contains(result.Receipt.CanonicalJSON().String(), spec.ClaimTokenHash.String()) {
		t.Fatalf("current receipt leaked claim authority: %s", result.Receipt.CanonicalJSON().String())
	}
	var stored []byte
	var disposition string
	if err := fixture.store.db.QueryRow(`SELECT r.current_read_receipt_json,h.last_disposition
		FROM agent_runs r JOIN agent_handlings h ON h.handling_id=r.handling_id WHERE r.run_id=?`,
		claim.Run.ID().String()).Scan(&stored, &disposition); err != nil ||
		string(stored) != result.Receipt.CanonicalJSON().String() || disposition != "read" {
		t.Fatalf("durable current evidence = (%s, %q, %v)", stored, disposition, err)
	}

	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.store = nil
	restarted, err := Open(context.Background(), fixture.path)
	if err != nil {
		t.Fatal(err)
	}
	fixture.store = restarted
	spec.At = readAt.Add(time.Second)
	replay, err := restarted.FinalizeAgentCurrentRead(context.Background(), spec)
	if err != nil || !replay.Replayed ||
		replay.Receipt.CanonicalJSON().String() != result.Receipt.CanonicalJSON().String() ||
		replay.Projection.CanonicalJSON().String() != result.Projection.CanonicalJSON().String() {
		t.Fatalf("restart replay = (%#v, %v)", replay, err)
	}
	if _, err := restarted.db.Exec(`UPDATE agent_runs SET current_read_receipt_json='{}' WHERE run_id=?`,
		claim.Run.ID().String()); err == nil {
		t.Fatal("schema allowed current-read evidence rewrite")
	}
}

func TestFinalizeAgentCurrentReadIsFencedAndConcurrentReplaySafe(t *testing.T) {
	t.Run("wrong token and lease boundary", func(t *testing.T) {
		fixture, events := newAgentClaimFixture(t, 1, "current-fence")
		claimAt := fixture.now.Add(2 * time.Second)
		insertClaimHandling(t, fixture.store, "handling-current-fence", events[0], 1, claimAt, claimAt, 0)
		claim := claimCurrent(t, fixture, "owner-current-fence", "token-current-fence", claimAt)
		wrong := currentReadSpec(fixture, claim.Run.ID(), "wrong-current-token", claimAt.Add(time.Second))
		if _, err := fixture.store.FinalizeAgentCurrentRead(context.Background(), wrong); !errors.Is(err, ErrCurrentReadStale) {
			t.Fatalf("wrong token error = %v", err)
		}
		var receipts int
		if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM agent_runs
			WHERE run_id=? AND current_read_receipt_json IS NOT NULL`, claim.Run.ID().String()).Scan(&receipts); err != nil || receipts != 0 {
			t.Fatalf("wrong token persisted receipts = %d, err=%v", receipts, err)
		}
		lease, _ := claim.Run.LeaseUntil()
		expired := currentReadSpec(fixture, claim.Run.ID(), "token-current-fence", lease)
		if _, err := fixture.store.FinalizeAgentCurrentRead(context.Background(), expired); !errors.Is(err, ErrCurrentReadStale) {
			t.Fatalf("lease boundary error = %v", err)
		}
	})

	t.Run("Profile projection budget", func(t *testing.T) {
		fixture, events := newAgentClaimFixture(t, 1, "current-budget")
		budgetSpec := model.DefaultHandlingBudget().Spec()
		budgetSpec.MaxCurrentJSONBytes = 1024
		budget, err := model.NewHandlingBudget(budgetSpec)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.db.Exec(`UPDATE profiles SET handling_budget_json=? WHERE profile_id=?`,
			budget.JSON().Bytes(), fixture.profile.ID().String()); err != nil {
			t.Fatal(err)
		}
		longState, _ := model.JSONFrom(struct {
			Content string `json:"content"`
		}{strings.Repeat("x", 2048)})
		if _, err := fixture.store.db.Exec(`UPDATE works SET state_json=? WHERE updated_by_event=?`,
			longState.Bytes(), events[0].String()); err != nil {
			t.Fatal(err)
		}
		claimAt := fixture.now.Add(2 * time.Second)
		insertClaimHandling(t, fixture.store, "handling-current-budget", events[0], 1, claimAt, claimAt, 0)
		claim := claimCurrent(t, fixture, "owner-current-budget", "token-current-budget", claimAt)
		_, err = fixture.store.FinalizeAgentCurrentRead(context.Background(),
			currentReadSpec(fixture, claim.Run.ID(), "token-current-budget", claimAt.Add(time.Second)))
		if !errors.Is(err, ErrCurrentReadTooLarge) {
			t.Fatalf("Profile budget error = %v", err)
		}
		var receipts int
		if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM agent_runs
			WHERE run_id=? AND current_read_receipt_json IS NOT NULL`, claim.Run.ID().String()).Scan(&receipts); err != nil || receipts != 0 {
			t.Fatalf("oversize current persisted receipts = %d, err=%v", receipts, err)
		}
	})

	t.Run("concurrent same claim", func(t *testing.T) {
		fixture, events := newAgentClaimFixture(t, 1, "current-concurrent")
		claimAt := fixture.now.Add(2 * time.Second)
		insertClaimHandling(t, fixture.store, "handling-current-concurrent", events[0], 1, claimAt, claimAt, 0)
		claim := claimCurrent(t, fixture, "owner-current-concurrent", "token-current-concurrent", claimAt)
		spec := currentReadSpec(fixture, claim.Run.ID(), "token-current-concurrent", claimAt.Add(time.Second))

		start := make(chan struct{})
		results := make(chan AgentCurrentReadResult, 2)
		errs := make(chan error, 2)
		var wait sync.WaitGroup
		for index := 0; index < 2; index++ {
			wait.Add(1)
			go func() {
				defer wait.Done()
				<-start
				result, err := fixture.store.FinalizeAgentCurrentRead(context.Background(), spec)
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
				t.Fatalf("concurrent finalize error = %v", err)
			}
		}
		var canonical string
		replayed := map[bool]int{}
		for result := range results {
			replayed[result.Replayed]++
			if canonical == "" {
				canonical = result.Receipt.CanonicalJSON().String()
			} else if result.Receipt.CanonicalJSON().String() != canonical {
				t.Fatal("concurrent finalizers returned different receipts")
			}
		}
		if replayed[false] != 1 || replayed[true] != 1 {
			t.Fatalf("concurrent replay states = %#v", replayed)
		}
	})
}

func TestFinalizeAgentCurrentReadIncludesOnlyVerifiedEventPinnedArtifacts(t *testing.T) {
	t.Run("verified and pinned", func(t *testing.T) {
		fixture, eventID, root, claim, claimAt := currentArtifactReadFixture(t, "valid")
		result, err := fixture.store.FinalizeAgentCurrentRead(context.Background(),
			currentReadSpec(fixture, claim.Run.ID(), "token-current-artifact-valid", claimAt.Add(time.Second)))
		if err != nil {
			t.Fatal(err)
		}
		refs := result.Receipt.ArtifactRefs()
		if len(refs) != 1 || refs[0].RootDigest() != root.RootDigest ||
			result.Receipt.SourceEvent().EventID() != eventID {
			t.Fatalf("current Artifact refs = %v", refs)
		}
	})

	t.Run("missing Event pin", func(t *testing.T) {
		fixture, eventID, root, claim, claimAt := currentArtifactReadFixture(t, "unpinned")
		if _, err := fixture.store.db.Exec(`DELETE FROM artifact_pins
			WHERE root_digest=? AND owner_kind='event' AND owner_id=?`, root.RootDigest.String(), eventID.String()); err != nil {
			t.Fatal(err)
		}
		_, err := fixture.store.FinalizeAgentCurrentRead(context.Background(),
			currentReadSpec(fixture, claim.Run.ID(), "token-current-artifact-unpinned", claimAt.Add(time.Second)))
		if !errors.Is(err, ErrCurrentReadInvariant) {
			t.Fatalf("missing Event pin error = %v", err)
		}
	})
}

func TestDeriveCurrentActionsUsesExactParticipantStateAndSourceEvent(t *testing.T) {
	now := time.Date(2026, 7, 16, 16, 0, 0, 0, time.UTC)
	home, _ := model.ParsePeerID("peer-current-policy-home")
	reviewer, _ := model.ParsePeerID("peer-current-policy-reviewer")
	channel, _ := model.ParseChannelID("channel-current-policy")
	workID, _ := model.ParseWorkID("work-current-policy")
	ref, _ := model.NewWorkRef(home, workID)
	participants, _ := model.NewParticipantSnapshot(channel, 2, home, reviewer)

	offered := currentPolicyEvent(t, now, channel, ref, home, reviewer,
		model.EventReviewOffered, "event-current-policy-offered",
		`{"content":"review","deadline":"2026-07-17T16:00:00Z","iteration":1,"work_version":1}`)
	work := currentPolicyWork(t, ref, channel, participants, offered, model.WorkOffered, 1, 1, now.Add(24*time.Hour).UnixNano())
	offeredFacts, _ := decodeClosedEventPayload(offered)
	if exact, err := currentWorkIsExactSource(offered, work, offeredFacts); err != nil || !exact {
		t.Fatalf("offered current binding = (%t, %v)", exact, err)
	}
	if got := deriveCurrentActions(model.CurrentReviewer, offered, work, true); !sameOperationKinds(got,
		[]model.OperationKind{model.OperationTeamworkAccept, model.OperationTeamworkDecline, model.OperationResolveRetry}) {
		t.Fatalf("reviewer OFFERED actions = %v", got)
	}
	if got := deriveCurrentActions(model.CurrentReviewer, offered, work, false); !sameOperationKinds(got,
		[]model.OperationKind{model.OperationResolveNoAction, model.OperationResolveRetry, model.OperationResolveReject}) {
		t.Fatalf("stale actions = %v", got)
	}
	accepted := currentPolicyEvent(t, now.Add(time.Second), channel, ref, home, reviewer,
		model.EventReviewAccepted, "event-current-policy-accepted", `{"iteration":1,"work_version":1}`)
	activeWork := currentPolicyWork(t, ref, channel, participants, accepted, model.WorkActive, 2, 1,
		now.Add(24*time.Hour).UnixNano())
	acceptedFacts, _ := decodeClosedEventPayload(accepted)
	if exact, err := currentWorkIsExactSource(accepted, activeWork, acceptedFacts); err != nil || !exact {
		t.Fatalf("accepted current binding = (%t, %v)", exact, err)
	}
	if got := deriveCurrentActions(model.CurrentReviewer, accepted, activeWork, true); !sameOperationKinds(got,
		[]model.OperationKind{model.OperationTeamworkOffer, model.OperationTeamworkDeliver, model.OperationResolveRetry}) {
		t.Fatalf("reviewer ACTIVE actions = %v", got)
	}

	delivered := currentPolicyEvent(t, now.Add(2*time.Second), channel, ref, home, reviewer,
		model.EventReviewDelivered, "event-current-policy-delivered", `{"iteration":1,"work_version":3}`)
	deliveredWork := currentPolicyWork(t, ref, channel, participants, delivered, model.WorkDelivered, 4, 1,
		now.Add(24*time.Hour).UnixNano())
	deliveredFacts, _ := decodeClosedEventPayload(delivered)
	if exact, err := currentWorkIsExactSource(delivered, deliveredWork, deliveredFacts); err != nil || !exact {
		t.Fatalf("delivered current binding = (%t, %v)", exact, err)
	}
	if got := deriveCurrentActions(model.CurrentInitiator, delivered, deliveredWork, true); !sameOperationKinds(got,
		[]model.OperationKind{model.OperationTeamworkRework, model.OperationTeamworkClose,
			model.OperationTeamworkCancel, model.OperationResolveRetry}) {
		t.Fatalf("initiator DELIVERED actions = %v", got)
	}
	rework := currentPolicyEvent(t, now.Add(3*time.Second), channel, ref, home, reviewer,
		model.EventReviewReworkRequested, "event-current-policy-rework",
		`{"content":"correct this","iteration":1,"work_version":4}`)
	reworkWork := currentPolicyWork(t, ref, channel, participants, rework, model.WorkRework, 5, 2,
		now.Add(24*time.Hour).UnixNano())
	reworkFacts, _ := decodeClosedEventPayload(rework)
	if exact, err := currentWorkIsExactSource(rework, reworkWork, reworkFacts); err != nil || !exact {
		t.Fatalf("rework current binding = (%t, %v)", exact, err)
	}
}

func TestCurrentDistinguishesOrdinaryDerivedWorkHandlingFromParentResume(t *testing.T) {
	fixture := newDerivationDispositionFixture(t, false)
	event := currentPolicyEvent(t, fixture.now, fixture.channel, fixture.children[0],
		fixture.node.PeerID(), fixture.reviewers[1], model.EventReviewAccepted,
		"event-derived-work-current", `{"iteration":1,"work_version":1}`)
	eventID := event.ID()
	regularID, _ := model.ParseHandlingID("handling-derived-work-ordinary")
	regular, err := model.NewHandling(model.HandlingSpec{ID: regularID,
		ProfileID: model.TeamworkProfileID(), EventID: eventID, Status: model.HandlingPending,
		AvailableAt: event.AcceptedAt(), CreatedAt: event.AcceptedAt(), UpdatedAt: event.AcceptedAt()})
	if err != nil {
		t.Fatal(err)
	}
	if parentResume, err := handlingIsParentResume(context.Background(), fixture.store.db,
		regular, event); err != nil || parentResume {
		t.Fatalf("ordinary derived Work handling classified as parent resume: %t, %v", parentResume, err)
	}

	resumeID, err := deterministicDerivationHandlingID(fixture.operation)
	if err != nil {
		t.Fatal(err)
	}
	resume, err := model.NewHandling(model.HandlingSpec{ID: resumeID,
		ProfileID: model.TeamworkProfileID(), EventID: eventID, Status: model.HandlingPending,
		AvailableAt: event.AcceptedAt(), CreatedAt: event.AcceptedAt(), UpdatedAt: event.AcceptedAt()})
	if err != nil {
		t.Fatal(err)
	}
	if parentResume, err := handlingIsParentResume(context.Background(), fixture.store.db,
		resume, event); err != nil || !parentResume {
		t.Fatalf("derivation resume was not recognized: %t, %v", parentResume, err)
	}
}

func currentReadSpec(fixture *acceptanceFixture, run model.RunID, token string,
	at time.Time,
) AgentCurrentReadSpec {
	return AgentCurrentReadSpec{ProfileID: fixture.profile.ID(),
		ExpectedAssetRevision: fixture.profile.ActiveAssetRevision(), RunID: run,
		ClaimTokenHash: model.Sum([]byte(token)), At: at}
}

func currentArtifactReadFixture(t *testing.T, suffix string) (*acceptanceFixture,
	model.EventID, VerifiedArtifactRoot, AgentClaimResult, time.Time,
) {
	t.Helper()
	fixture := newAcceptanceFixture(t, 1)
	operation, authority := fixture.reserveOffer(t, "current-artifact-"+suffix, nil)
	root := verifiedRoot(t, "current-artifact-root-"+suffix,
		`{"entries":[],"kind":"current","total_bytes":0}`, 0)
	if _, err := fixture.store.CheckpointVerifiedArtifactRoot(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	checkpointOperationRoot(t, fixture, operation, authority.LeaseOwner, root)
	artifact, _ := model.NewArtifactRef(root.RootDigest, model.ArtifactProduced)
	spec := fixture.offer(t, authority, "current-artifact-"+suffix, fixture.reviewers,
		[]model.ArtifactRef{artifact}, nil)
	if _, err := fixture.store.CommitLocalAcceptance(context.Background(), spec, fixture.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	eventID, _ := model.ParseEventID("event-current-artifact-" + suffix + "-0")
	claimAt := fixture.now.Add(2 * time.Second)
	insertClaimHandling(t, fixture.store, "handling-current-artifact-"+suffix, eventID, 1, claimAt, claimAt, 0)
	token := "token-current-artifact-" + suffix
	claim := claimCurrent(t, fixture, "owner-current-artifact-"+suffix, token, claimAt)
	return fixture, eventID, root, claim, claimAt
}

func sameOperationKinds(left, right []model.OperationKind) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func currentPolicyEvent(t *testing.T, at time.Time, channel model.ChannelID, ref model.WorkRef,
	home, reviewer model.PeerID, eventType model.EventType, eventText, payloadText string,
) model.Event {
	t.Helper()
	epoch, _ := model.ParseOriginEpoch("epoch-current-policy-home")
	head, _ := model.NewRecordHead(1, model.Sum([]byte("current-policy-head")))
	scope, _ := model.NewEventScope(channel, home, epoch, 1, 1, head, head, ref)
	audience, _ := model.NewAudience([]model.PeerID{reviewer})
	payload, _ := model.NewJSON([]byte(payloadText))
	eventID, _ := model.ParseEventID(eventText)
	event, err := model.NewEvent(model.EventSpec{ID: eventID, Scope: scope, Source: model.EventSourceImported,
		ActorPrincipal: "principal-current-policy", Type: eventType, Audience: audience,
		Summary: "current policy", Payload: payload, CreatedAt: at, AcceptedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func currentPolicyWork(t *testing.T, ref model.WorkRef, channel model.ChannelID,
	participants model.ParticipantSnapshot, event model.Event, state model.WorkState,
	version uint64, iteration uint8, deadline int64,
) model.ReviewWork {
	t.Helper()
	work, err := model.NewReviewWork(model.ReviewWorkSpec{Ref: ref, ChannelID: channel,
		Participants: participants, Version: version, Iteration: iteration, DeadlineUnixNano: deadline,
		State: state, StateData: event.Payload(), UpdatedBy: event.ID(), UpdatedAt: event.AcceptedAt()})
	if err != nil {
		t.Fatal(err)
	}
	return work
}
