package agent

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

// TestTeamworkActionExecutorOffersExplicitAutoAndCanonicalTeam remains the
// stable ND-21 evidence symbol while its identity-specific helpers live beside
// executor_identity.go.
func TestTeamworkActionExecutorOffersExplicitAutoAndCanonicalTeam(t *testing.T) {
	runTeamworkActionExecutorOffersExplicitAutoAndCanonicalTeam(t)
}

func TestTeamworkActionExecutorRejectsMissingOrMismatchedActionHandlers(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, 1)
	options := TeamworkActionExecutorOptions{Profile: fixture.profile,
		Signer:    executorSigner(t, "executor-construction-"+t.Name()),
		Artifacts: fixture.artifacts, Clock: fixture.clock}
	if _, err := newTeamworkActionExecutor(fixture.backend, fixture.selector, options); err == nil {
		t.Fatal("executor accepted missing Action handlers")
	}

	options.Actions = testActionHandlers(t)
	options.Profile = executorProfile(t, fixture.at,
		model.Sum([]byte("mismatched-action-revision")).String())
	if _, err := newTeamworkActionExecutor(fixture.backend, fixture.selector, options); err == nil {
		t.Fatal("executor accepted Action handlers from a different asset revision")
	}
}

func TestTeamworkActionExecutorNestedOfferUsesCurrentCausality(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, 2)
	parent := fixture.work(t, model.WorkActive, 2, 1, true)
	fixture.backend.work = parent
	action := executorAction(t, "offer", true, "delegate review", "1h", AgentParticipantTeam, nil)
	reservation := executorReservation(t, fixture, action, parent, true)
	response, apiErr := fixture.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
		Request: TeamworkActionRequest{Action: "offer", To: AgentParticipantTeam,
			Deadline: "1h", Content: "delegate review"},
		Action: action, Reservation: reservation, At: fixture.at,
	})
	if apiErr != nil || response.Handling == nil || len(response.Results) != 2 {
		t.Fatalf("nested offer = (%#v, %v)", response, apiErr)
	}
	source := executorCurrent(t, reservation.Run).SourceEvent()
	for _, item := range fixture.backend.committed.items {
		causes := item.Publication.Event().CausedBy()
		if len(causes) != 1 || causes[0] != source ||
			item.Work.Work.Ref().HomePeerID() != fixture.scope.node.PeerID() {
			t.Fatalf("nested offer causality/home = %#v", item)
		}
	}
}

func TestTeamworkActionExecutorTerminalReplayAndStableRejection(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, 1)
	action := executorAction(t, "offer", false, "goal", "30m", AgentParticipantAuto, nil)
	base := executorReservation(t, fixture, action, model.ReviewWork{}, false)
	first, apiErr := fixture.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
		Request: TeamworkActionRequest{Action: "offer", Content: "goal"},
		Action:  action, Reservation: base, At: fixture.at,
	})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	if first.Receipt != model.Sum(fixture.backend.lastReceipt.Bytes()).String() ||
		strings.Contains(first.Receipt, fixture.backend.lastReceipt.String()) {
		t.Fatalf("accepted receipt is not the bounded durable evidence digest: %q", first.Receipt)
	}
	committed := executorTerminalOperation(t, base.Operation, model.OperationCommitted,
		fixture.backend.lastReceipt, fixture.at)
	fixture.selector.err = errors.New("selector must not run")
	fixture.artifacts.apiErr = NewControlError(CodeInternal, "capture must not run")
	fixture.executor.backend, fixture.executor.selector, fixture.executor.signer = nil, nil, nil
	fixture.executor.artifacts, fixture.executor.clock = nil, nil
	replay, apiErr := fixture.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
		Action: action, Reservation: store.ManagedOperationReservation{Operation: committed, Replayed: true},
		At: time.Time{},
	})
	if apiErr != nil || !replay.Replayed || replay.OperationID != first.OperationID ||
		replay.Receipt != first.Receipt ||
		fixture.selector.calls != 1 || fixture.artifacts.calls != 1 {
		t.Fatalf("committed replay = (%#v, %v), calls selector=%d artifacts=%d",
			replay, apiErr, fixture.selector.calls, fixture.artifacts.calls)
	}
	rejectedFixture := newExecutorFixture(t, 1)
	rejectedBase := executorReservation(t, rejectedFixture, action, model.ReviewWork{}, false)
	longCandidates := make([]string, model.MaxChildWorks)
	for index := range longCandidates {
		longCandidates[index] = fmt.Sprintf("candidate-%d-%s", index, strings.Repeat("x", 100))
	}
	rejectedFixture.selector.err = &AgentSelectionCandidatesError{kind: ErrAgentSelectionParticipantAmbiguous,
		candidates: longCandidates}
	_, firstErr := rejectedFixture.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
		Action: action, Reservation: rejectedBase, At: rejectedFixture.at,
	})
	if firstErr == nil || firstErr.Code != CodeAmbiguousParticipant || firstErr.Replayed ||
		firstErr.OperationID == nil || rejectedFixture.backend.rejected.Status() != model.OperationRejected ||
		rejectedFixture.backend.rejectAt != rejectedFixture.clock.now || rejectedFixture.artifacts.calls != 0 ||
		len(firstErr.Message) > MaxControlDiagnosticBytes ||
		!strings.Contains(rejectedFixture.backend.rejection.String(), `"status":"error"`) ||
		!strings.Contains(rejectedFixture.backend.rejection.String(), `"replayed":false`) ||
		strings.Contains(rejectedFixture.backend.rejection.String(), `"action"`) {
		t.Fatalf("initial rejection = %#v", firstErr)
	}
	rejectedFixture.executor.backend, rejectedFixture.executor.selector, rejectedFixture.executor.signer = nil, nil, nil
	rejectedFixture.executor.artifacts, rejectedFixture.executor.clock = nil, nil
	_, replayErr := rejectedFixture.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
		Action: action, Reservation: store.ManagedOperationReservation{Operation: rejectedFixture.backend.rejected,
			Replayed: true}, At: time.Time{},
	})
	if replayErr == nil || replayErr.Code != firstErr.Code || replayErr.Message != firstErr.Message ||
		!replayErr.Replayed || rejectedFixture.selector.calls != 1 {
		t.Fatalf("rejected replay = %#v", replayErr)
	}
}

func TestTeamworkActionExecutorRejectsTerminalRequestDigestMismatch(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, 1)
	action := executorAction(t, "offer", false, "goal", "30m", AgentParticipantAuto, nil)
	reservation := executorReservation(t, fixture, action, model.ReviewWork{}, false)
	receipt, err := fakeAcceptanceReceipt(executionAcceptanceSpec{}, model.ReviewWork{})
	if err != nil {
		t.Fatal(err)
	}
	committed := executorTerminalOperation(t, reservation.Operation, model.OperationCommitted,
		receipt, fixture.at)
	changed := action
	changed.Content = "changed behind the same operation"
	_, apiErr := fixture.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
		Action: changed, Reservation: store.ManagedOperationReservation{Operation: committed, Replayed: true},
	})
	if apiErr == nil || apiErr.Code != CodeOperationMismatch || !apiErr.Replayed {
		t.Fatalf("terminal request digest mismatch = %#v", apiErr)
	}
}

func TestTeamworkActionExecutorRejectsCommitFailureAndHonorsFence(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, 1)
	work := fixture.work(t, model.WorkDelivered, 3, 1, false)
	fixture.backend.work, fixture.backend.commitErr = work, store.ErrWorkCASConflict
	action := executorAction(t, "close", true, "", "", "", nil)
	reservation := executorReservation(t, fixture, action, work, true)
	_, apiErr := fixture.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
		Action: action, Reservation: reservation, At: fixture.at,
	})
	if apiErr == nil || apiErr.Code != CodeWorkConflict ||
		fixture.backend.rejected.Status() != model.OperationRejected ||
		fixture.backend.commitAt != fixture.clock.now || fixture.backend.rejectAt != fixture.clock.now {
		t.Fatalf("commit rejection = %#v, terminal=%s", apiErr, fixture.backend.rejected.Status())
	}

	fenced := newExecutorFixture(t, 1)
	fencedWork := fenced.work(t, model.WorkDelivered, 3, 1, false)
	fenced.backend.work, fenced.backend.commitErr, fenced.backend.rejectErr =
		fencedWork, store.ErrWorkCASConflict, store.ErrOperationFence
	fencedReservation := executorReservation(t, fenced, action, fencedWork, true)
	_, apiErr = fenced.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
		Action: action, Reservation: fencedReservation, At: fenced.at,
	})
	if apiErr == nil || apiErr.Code != CodeOperationPending || fenced.backend.rejected.Status().Valid() {
		t.Fatalf("fenced rejection = %#v, terminal=%s", apiErr, fenced.backend.rejected.Status())
	}
}

func TestTeamworkActionExecutorAtomicallyLetsDeadlineWin(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, 1)
	work := fixture.work(t, model.WorkActive, 2, 1, false)
	workSpec := work.Spec()
	workSpec.DeadlineUnixNano = fixture.at.Add(500 * time.Millisecond).UnixNano()
	var err error
	work, err = model.NewReviewWork(workSpec)
	if err != nil {
		t.Fatal(err)
	}
	fixture.backend.work = work
	action := executorAction(t, "cancel", true, "deadline race", "", "", nil)
	reservation := executorReservation(t, fixture, action, work, true)

	response, apiErr := fixture.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
		Action: action, Reservation: reservation, At: fixture.at,
	})
	if apiErr == nil || apiErr.Code != CodeWorkExpired || apiErr.Replayed ||
		apiErr.OperationID == nil || response.OperationID != "" || fixture.backend.deadlines != 1 ||
		fixture.backend.commits != 0 || fixture.backend.rejects != 0 ||
		fixture.backend.deadlineAt != fixture.clock.now {
		t.Fatalf("deadline winner = (%#v, %#v), backend=%#v", response, apiErr, fixture.backend)
	}
	item := fixture.backend.deadline.expiry
	if item.Work == nil || item.Work.Work.State() != model.WorkExpired ||
		item.Work.Work.Version() != work.Version()+1 || item.Work.Work.Iteration() != work.Iteration() {
		t.Fatalf("deadline Work mutation = %#v", item.Work)
	}
	expiry := item.Publication.Event()
	wantEventID, _ := derivedDeadlineEventID(reservation.Operation.ID())
	contextHash, _ := reservation.Operation.ContextHash()
	if expiry.ID() != wantEventID || expiry.Type() != model.EventReviewExpired ||
		!expiry.AcceptedAt().Equal(fixture.clock.now) || len(expiry.CausedBy()) != 1 ||
		expiry.CausedBy()[0] != executorCurrent(t, reservation.Run).SourceEvent() ||
		fixture.backend.deadline.contextHash != contextHash ||
		fixture.backend.deadline.operation != localExecutionAuthority(reservation.Operation) {
		t.Fatalf("deadline Event/authority = %#v / %#v", expiry, fixture.backend.deadline)
	}

	_, replayErr := fixture.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
		Action: action, Reservation: store.ManagedOperationReservation{Operation: fixture.backend.rejected,
			Replayed: true}, At: fixture.at.Add(2 * time.Second),
	})
	if replayErr == nil || replayErr.Code != apiErr.Code || replayErr.Message != apiErr.Message ||
		!replayErr.Replayed || fixture.backend.deadlines != 1 || fixture.backend.rejects != 0 {
		t.Fatalf("deadline replay = %#v", replayErr)
	}
}

func TestTeamworkActionExecutorKeepsRetryableOperationOpen(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, 1)
	fixture.selector.err = ErrAgentSelectionParticipantUnavailable
	action := executorAction(t, "offer", false, "retry selection", "30m", AgentParticipantAuto, nil)
	reservation := executorReservation(t, fixture, action, model.ReviewWork{}, false)

	_, apiErr := fixture.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
		Action: action, Reservation: reservation, At: fixture.at,
	})
	if apiErr == nil || apiErr.Code != CodePeerUnavailable || !apiErr.Retryable ||
		apiErr.OperationID == nil || fixture.backend.rejects != 0 || fixture.artifacts.calls != 0 {
		t.Fatalf("retryable selection = %#v, rejects=%d artifacts=%d", apiErr,
			fixture.backend.rejects, fixture.artifacts.calls)
	}
	fixture.selector.err = nil
	response, apiErr := fixture.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
		Action: action, Reservation: reservation, At: fixture.at,
	})
	if apiErr != nil || response.Status != "accepted" || fixture.backend.commits != 1 ||
		fixture.backend.rejects != 0 {
		t.Fatalf("retryable continuation = (%#v, %#v)", response, apiErr)
	}
}

type executorFixture struct {
	at        time.Time
	clock     *executorTestClock
	profile   model.Profile
	scope     executionScope
	reviewers []AgentOfferReviewer
	backend   *fakeExecutionBackend
	selector  *fakeOfferResolver
	artifacts *fakeArtifactCoordinator
	executor  *TeamworkActionExecutor
}

func newExecutorFixture(t *testing.T, reviewerCount int) *executorFixture {
	t.Helper()
	at := time.Date(2026, 7, 16, 20, 0, 0, 0, time.UTC)
	actions := testActionHandlers(t)
	profile := executorProfile(t, at.Add(-time.Hour), actions.AssetRevision().String())
	local := agentSelectorPeer(t, "executor-local-"+t.Name())
	node, err := model.NewNode(model.NodeSpec{PeerID: local,
		OriginEpoch: executorEpoch(t, "executor-local-"+t.Name()), NextOriginSequence: 10,
		ActiveAssetRevision: profile.ActiveAssetRevision(), CreatedAt: at.Add(-time.Hour),
		UpdatedAt: at.Add(-time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	channel, _ := model.ParseChannelID("channel-executor")
	head := executorHead(t, "executor-roster", uint64(reviewerCount+1))
	scope := executionScope{node: node, profile: profile, channelID: channel,
		originMember: executorHead(t, "executor-local-member", 1), publicationRoster: head,
		firstOriginSequence: 10, firstChannelSequence: 20, count: uint8(reviewerCount)}
	reviewers := make([]AgentOfferReviewer, reviewerCount)
	for index := range reviewers {
		reviewers[index] = AgentOfferReviewer{peerID: agentSelectorPeer(t, fmt.Sprintf("executor-reviewer-%s-%d", t.Name(), index)),
			effectiveAlias: fmt.Sprintf("reviewer-%d", index), reachability: model.ReachabilityReachable}
	}
	sort.Slice(reviewers, func(left, right int) bool {
		leftKey, _ := canonicalAgentPeerBytes(reviewers[left].PeerID())
		rightKey, _ := canonicalAgentPeerBytes(reviewers[right].PeerID())
		return string(leftKey) < string(rightKey)
	})
	selection := AgentOfferSelection{channelID: channel, channelAlias: "alpha", rosterHead: head,
		reviewers: reviewers}
	backend := &fakeExecutionBackend{scope: scope}
	selector := &fakeOfferResolver{selection: selection}
	artifacts := &fakeArtifactCoordinator{}
	signer := executorSigner(t, "executor-signer-"+t.Name())
	clock := &executorTestClock{now: at.Add(time.Second)}
	executor, err := newTeamworkActionExecutor(backend, selector, TeamworkActionExecutorOptions{
		Profile: profile, Actions: actions, Signer: signer, Artifacts: artifacts, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	return &executorFixture{at: at, clock: clock, profile: profile, scope: scope, reviewers: reviewers,
		backend: backend, selector: selector, artifacts: artifacts, executor: executor}
}

type executorTestClock struct{ now time.Time }

func (clock *executorTestClock) Now() time.Time { return clock.now }

func (f *executorFixture) work(t *testing.T, state model.WorkState, version uint64,
	iteration uint8, localReviewer bool,
) model.ReviewWork {
	t.Helper()
	home, reviewer := f.scope.node.PeerID(), f.reviewers[0].PeerID()
	if localReviewer {
		home, reviewer = f.reviewers[0].PeerID(), f.scope.node.PeerID()
	}
	workID, _ := model.ParseWorkID("work-executor-" + strings.ToLower(string(state)))
	ref, _ := model.NewWorkRef(home, workID)
	participants, _ := model.NewParticipantSnapshot(f.scope.channelID,
		f.scope.publicationRoster.Revision(), home, reviewer)
	eventID, _ := model.ParseEventID("event-current-" + strings.ToLower(string(state)))
	stateData, _ := model.NewJSON([]byte(`{"current":true}`))
	work, err := model.NewReviewWork(model.ReviewWorkSpec{Ref: ref, ChannelID: f.scope.channelID,
		Participants: participants, Version: version, Iteration: iteration,
		DeadlineUnixNano: f.at.Add(time.Hour).UnixNano(), State: state, StateData: stateData,
		UpdatedBy: eventID, UpdatedAt: f.at.Add(-2 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	return work
}

type fakeOfferResolver struct {
	selection AgentOfferSelection
	err       error
	last      AgentOfferSelectionSpec
	calls     int
}

func (r *fakeOfferResolver) Resolve(_ context.Context,
	spec AgentOfferSelectionSpec,
) (AgentOfferSelection, error) {
	r.calls++
	r.last = spec
	return r.selection, r.err
}

type fakeArtifactCoordinator struct {
	result ArtifactCoordinationResult
	apiErr *ControlError
	last   ArtifactCoordinationSpec
	calls  int
}

func (c *fakeArtifactCoordinator) Coordinate(_ context.Context,
	spec ArtifactCoordinationSpec,
) (ArtifactCoordinationResult, *ControlError) {
	c.calls++
	c.last = spec
	return c.result, c.apiErr
}

type fakeExecutionBackend struct {
	scope               executionScope
	work                model.ReviewWork
	operation           model.Operation
	probeTerminal       model.Operation
	committed           executionAcceptanceSpec
	lastReceipt         model.JSON
	rejected            model.Operation
	rejection           model.JSON
	commitAt            time.Time
	rejectAt            time.Time
	deadline            executionDeadlineSpec
	deadlineAt          time.Time
	deadlineErr         error
	probeErr            error
	deadlines           int
	probeMisses         int
	probes              int
	commits             int
	rejects             int
	commitErr           error
	rejectErr           error
	rejectReplay        bool
	rejectFreshTerminal bool
}

func (b *fakeExecutionBackend) Prepare(_ context.Context, channel model.ChannelID,
	_ model.Audience, count uint8,
) (executionScope, error) {
	scope := b.scope
	scope.channelID, scope.count = channel, count
	return scope, nil
}

func (b *fakeExecutionBackend) GetReviewWork(_ context.Context,
	_ model.WorkRef,
) (model.ReviewWork, error) {
	if b.work.Ref().IsZero() {
		return model.ReviewWork{}, errors.New("Work unavailable")
	}
	return b.work, nil
}

func (b *fakeExecutionBackend) Probe(_ context.Context,
	_ store.ManagedOperationProbeSpec,
) (store.ManagedOperationProbe, error) {
	b.probes++
	if b.probeErr != nil {
		return store.ManagedOperationProbe{}, b.probeErr
	}
	if b.probeMisses > 0 {
		b.probeMisses--
		return store.ManagedOperationProbe{}, nil
	}
	if b.probeTerminal.Status().Terminal() {
		return store.ManagedOperationProbe{Operation: b.probeTerminal, Found: true}, nil
	}
	return store.ManagedOperationProbe{}, nil
}

func (b *fakeExecutionBackend) Commit(_ context.Context, spec executionAcceptanceSpec,
	at time.Time,
) (store.LocalAcceptanceResult, error) {
	b.committed = spec
	b.commitAt = at
	b.commits++
	if b.commitErr != nil {
		return store.LocalAcceptanceResult{}, b.commitErr
	}
	receipt, err := fakeAcceptanceReceipt(spec, b.work)
	if err != nil {
		return store.LocalAcceptanceResult{}, err
	}
	b.lastReceipt = receipt
	return store.LocalAcceptanceResult{Receipt: receipt}, nil
}

func (b *fakeExecutionBackend) ResolveDeadline(_ context.Context, spec executionDeadlineSpec,
	at time.Time,
) (store.DeadlineResolutionResult, error) {
	b.deadline, b.deadlineAt = spec, at
	b.deadlines++
	if b.deadlineErr != nil {
		return store.DeadlineResolutionResult{}, b.deadlineErr
	}
	rejection, err := model.NewOperationRejectionReceipt(model.OperationRejectionSpec{
		OperationID: spec.operation.ID, Code: string(CodeWorkExpired),
		Message: "Work deadline reached before action commit",
	})
	if err != nil {
		return store.DeadlineResolutionResult{}, err
	}
	receipt := rejection.JSON()
	b.rejected = executorTerminalOperationValue(b.operation, model.OperationRejected, receipt, at)
	return store.DeadlineResolutionResult{Receipt: receipt}, nil
}

func (b *fakeExecutionBackend) Reject(_ context.Context, _ model.OperationID, _ string,
	at time.Time, result model.JSON,
) (store.OperationRejectionResult, error) {
	b.rejection = result
	b.rejectAt = at
	b.rejects++
	if b.rejectErr != nil {
		return store.OperationRejectionResult{}, b.rejectErr
	}
	if b.rejectReplay {
		return store.OperationRejectionResult{Operation: b.rejected, Replayed: true}, nil
	}
	if b.rejectFreshTerminal {
		return store.OperationRejectionResult{Operation: b.rejected}, nil
	}
	b.rejected = executorTerminalOperationValue(b.operation, model.OperationRejected, result, at)
	return store.OperationRejectionResult{Operation: b.rejected}, nil
}

func fakeAcceptanceReceipt(spec executionAcceptanceSpec, current model.ReviewWork) (model.JSON, error) {
	type captureWire struct {
		ManifestDigest model.Digest `json:"manifest_digest"`
		RootDigest     model.Digest `json:"root_digest"`
	}
	type workWire struct {
		Ref     model.WorkRef   `json:"ref"`
		State   model.WorkState `json:"state"`
		Version uint64          `json:"version"`
	}
	type eventWire struct {
		ArtifactRoots []model.ArtifactRef `json:"artifact_roots"`
		EventDigest   model.Digest        `json:"event_digest"`
		EventID       model.EventID       `json:"event_id"`
		EventType     model.EventType     `json:"event_type"`
		Work          workWire            `json:"work"`
	}
	events := make([]eventWire, len(spec.items))
	produced := make(map[model.Digest]struct{})
	for index, item := range spec.items {
		work := current
		if item.Work != nil {
			work = item.Work.Work
		}
		eventValue := item.Publication.Event()
		for _, ref := range eventValue.Artifacts() {
			if ref.Role() == model.ArtifactProduced {
				produced[ref.RootDigest()] = struct{}{}
			}
		}
		events[index] = eventWire{ArtifactRoots: eventValue.Artifacts(), EventDigest: eventValue.Digest(),
			EventID: eventValue.ID(), EventType: eventValue.Type(),
			Work: workWire{Ref: work.Ref(), State: work.State(), Version: work.Version()}}
	}
	captures := make([]captureWire, 0, len(produced))
	for root := range produced {
		captures = append(captures, captureWire{ManifestDigest: model.Sum([]byte("manifest-" + root.String())),
			RootDigest: root})
	}
	sort.Slice(captures, func(left, right int) bool {
		return captures[left].RootDigest.String() < captures[right].RootDigest.String()
	})
	return model.JSONFrom(struct {
		CaptureRoots []captureWire `json:"capture_roots"`
		Events       []eventWire   `json:"events"`
		OperationID  string        `json:"operation_id"`
		Status       string        `json:"status"`
	}{CaptureRoots: captures, Events: events,
		OperationID: spec.operation.ID.String(), Status: "committed"})
}

func executorReservation(t *testing.T, fixture *executorFixture, action ValidatedAction,
	work model.ReviewWork, contextBound bool,
) store.ManagedOperationReservation {
	t.Helper()
	kind := action.handler.OperationKind()
	operationID, _ := model.ParseOperationID("operation-executor-" + strings.TrimPrefix(string(kind), "teamwork."))
	runID, _ := model.ParseRunID("run-executor-" + strings.TrimPrefix(string(kind), "teamwork."))
	started := fixture.at.Add(-time.Minute)
	lease := fixture.at.Add(time.Hour)
	var contextHash *model.Digest
	var currentJSON *model.JSON
	var handlingID *model.HandlingID
	var fence *model.Digest
	var attempt uint32
	if contextBound {
		value := model.Sum([]byte("context-" + operationID.String()))
		contextHash, fence = &value, &value
		handling, _ := model.ParseHandlingID("handling-" + operationID.String())
		handlingID, attempt = &handling, 1
		current := executorCurrentReceipt(t, fixture, runID, handling, work, kind)
		valueJSON := current.CanonicalJSON()
		currentJSON = &valueJSON
	}
	empty, _ := model.NewJSON([]byte(`{}`))
	run, err := model.NewAgentRun(model.AgentRunSpec{ID: runID, ProfileID: fixture.profile.ID(),
		HandlingID: handlingID, Cause: empty, HandlingAttempt: attempt, ClaimFenceHash: fence,
		LeaseUntil: func() *time.Time {
			if contextBound {
				return &lease
			}
			return nil
		}(),
		Launcher: "executor-test", Runtime: fixture.profile.Runtime(), LauncherDiagnostic: empty,
		RuntimeIDs: empty, Status: model.AgentRunRunning, StartedAt: started,
		CurrentReadReceipt: currentJSON})
	if err != nil {
		t.Fatal(err)
	}
	digestContext := model.Digest{}
	if contextHash != nil {
		digestContext = *contextHash
	}
	requestDigest, err := action.requestDigest(digestContext, contextBound)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := model.NewOperation(model.OperationSpec{ID: operationID, ProfileID: fixture.profile.ID(),
		AgentRunID: runID, ClientKeyHash: model.Sum([]byte("key-" + operationID.String())),
		ContextHash: contextHash, Kind: kind, RequestDigest: requestDigest,
		Status: model.OperationStarted, LeaseOwner: "owner-" + operationID.String(), LeaseUntil: &lease,
		CreatedAt: started})
	if err != nil {
		t.Fatal(err)
	}
	fixture.backend.operation = operation
	return store.ManagedOperationReservation{Operation: operation, Run: run,
		HasHandling: contextBound, Acquired: true}
}

func executorCurrentReceipt(t *testing.T, fixture *executorFixture, run model.RunID,
	handling model.HandlingID, work model.ReviewWork, kind model.OperationKind,
) model.CurrentReadReceipt {
	t.Helper()
	epoch := executorEpoch(t, "current-source")
	key, _ := model.NewEventKey(work.Ref().HomePeerID(), epoch, work.UpdatedBy())
	eventType := map[model.WorkState]model.EventType{model.WorkOffered: model.EventReviewOffered,
		model.WorkActive: model.EventReviewAccepted, model.WorkDelivered: model.EventReviewDelivered,
		model.WorkRework: model.EventReviewReworkRequested}[work.State()]
	source, err := model.NewCurrentEvent(model.CurrentEventSpec{Key: key, Digest: model.Sum([]byte("source")),
		Type: eventType, WorkRef: work.Ref(), Summary: "current", Payload: work.StateData(),
		AcceptedAt: work.UpdatedAt()})
	if err != nil {
		t.Fatal(err)
	}
	role := model.CurrentInitiator
	if work.Participants().ReviewerPeerID() == fixture.scope.node.PeerID() {
		role = model.CurrentReviewer
	}
	brief, _ := model.NewCurrentBrief(model.CurrentBriefSpec{Content: "persistent goal",
		DeadlineUnixNano: work.DeadlineUnixNano()})
	currentWork, err := model.NewCurrentWork(model.CurrentWorkSpec{Ref: work.Ref(), Version: work.Version(),
		Iteration: work.Iteration(), DeadlineUnixNano: work.DeadlineUnixNano(), State: work.State(),
		StateData: work.StateData(), LocalRole: role, Brief: brief})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := model.NewCurrentProjection(model.CurrentProjectionSpec{SourceEvent: source,
		ActionWork: currentWork, AllowedActions: []model.OperationKind{kind, model.OperationResolveRetry}})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := model.NewCurrentReadReceipt(model.CurrentReadReceiptSpec{RunID: run,
		ProfileID: fixture.profile.ID(), HandlingID: handling, HandlingAttempt: 1,
		Projection: projection, ReadAt: fixture.at.Add(-time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

func executorCurrent(t *testing.T, run model.AgentRun) model.CurrentReadReceipt {
	t.Helper()
	raw, ok := run.CurrentReadReceipt()
	if !ok {
		t.Fatal("Run has no current receipt")
	}
	current, err := model.ParseCurrentReadReceipt(raw.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	return current
}

func executorAction(t *testing.T, name string, hasContext bool, content, deadline, participant string,
	paths []string,
) ValidatedAction {
	t.Helper()
	action, apiErr := testActionHandlers(t).Validate(ActionInput{Action: name, HasContext: hasContext,
		ChannelAlias: func() string {
			if name == "offer" {
				return "alpha"
			}
			return ""
		}(),
		Participant: participant, Deadline: deadline, Content: content, ArtifactPaths: paths})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	return action
}

func executorTerminalOperation(t *testing.T, base model.Operation, status model.OperationStatus,
	result model.JSON, at time.Time,
) model.Operation {
	t.Helper()
	operation := executorTerminalOperationValue(base, status, result, at)
	if operation.ID().IsZero() {
		t.Fatal("terminal operation projection failed")
	}
	return operation
}

func executorTerminalOperationValue(base model.Operation, status model.OperationStatus,
	result model.JSON, at time.Time,
) model.Operation {
	contextHash, hasContext := base.ContextHash()
	var contextValue *model.Digest
	if hasContext {
		contextValue = &contextHash
	}
	operation, _ := model.NewOperation(model.OperationSpec{ID: base.ID(), ProfileID: base.ProfileID(),
		AgentRunID: base.AgentRunID(), ClientKeyHash: base.ClientKeyHash(), ContextHash: contextValue,
		Kind: base.Kind(), RequestDigest: base.RequestDigest(), Status: status,
		Result: &result, CreatedAt: base.CreatedAt(), FinishedAt: &at})
	return operation
}

func executorOperationWithID(t *testing.T, base model.Operation,
	id model.OperationID,
) model.Operation {
	t.Helper()
	contextHash, hasContext := base.ContextHash()
	var contextValue *model.Digest
	if hasContext {
		contextValue = &contextHash
	}
	lease, _ := base.LeaseUntil()
	operation, err := model.NewOperation(model.OperationSpec{ID: id, ProfileID: base.ProfileID(),
		AgentRunID: base.AgentRunID(), ClientKeyHash: base.ClientKeyHash(), ContextHash: contextValue,
		Kind: base.Kind(), RequestDigest: base.RequestDigest(), Status: model.OperationStarted,
		LeaseOwner: base.LeaseOwner(), LeaseUntil: &lease, CreatedAt: base.CreatedAt()})
	if err != nil {
		t.Fatal(err)
	}
	return operation
}

func mustExecutorRejectionReceipt(t *testing.T, operationID model.OperationID,
	code ControlErrorCode, message string,
) model.JSON {
	t.Helper()
	receipt, err := model.NewOperationRejectionReceipt(model.OperationRejectionSpec{
		OperationID: operationID, Code: string(code), Message: message,
	})
	if err != nil {
		t.Fatal(err)
	}
	return receipt.JSON()
}

func executorProfile(t *testing.T, at time.Time, assetRevision string) model.Profile {
	t.Helper()
	profile, err := model.NewProfile(model.ProfileSpec{ID: model.TeamworkProfileID(),
		Principal: "principal-executor", WorkspaceRoot: "/workspace/executor", Host: model.HostCodex,
		Runtime: model.RuntimeCodexAppServer, CredentialHash: model.Sum([]byte("executor-credential")),
		ActiveAssetRevision: assetRevision, HandlingBudget: model.DefaultHandlingBudget().JSON(),
		Enabled: true, CreatedAt: at, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

func executorSigner(t *testing.T, label string) event.PublicationSigner {
	t.Helper()
	seed := sha256.Sum256([]byte(label))
	signer, err := event.NewEd25519Signer(ed25519.NewKeyFromSeed(seed[:]))
	if err != nil {
		t.Fatal(err)
	}
	return signer
}

func executorEpoch(t *testing.T, value string) model.OriginEpoch {
	t.Helper()
	epoch, err := model.ParseOriginEpoch("epoch-" + fmt.Sprintf("%x", sha256.Sum256([]byte(value))))
	if err != nil {
		t.Fatal(err)
	}
	return epoch
}

func executorHead(t *testing.T, value string, revision uint64) model.RecordHead {
	t.Helper()
	head, err := model.NewRecordHead(revision, model.Sum([]byte(value)))
	if err != nil {
		t.Fatal(err)
	}
	return head
}
