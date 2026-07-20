package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// Terminal replay never scans an unbounded durable causal graph. T0 has at
// most a handful of Work transitions, so 64 leaves ample room for legitimate
// fan-in while keeping corrupt or adversarial state bounded.
const peerInboxSemanticWorkCausalityLimit = 64

// CommitPeerInboxSemanticSpec carries one exact semantic fence, an immutable
// Store plan built from its claim outside SQLite, and any local response
// publications assembled from that plan.
type CommitPeerInboxSemanticSpec struct {
	Fence     PeerInboxSemanticFence
	Plan      PeerInboxSemanticPlan
	Scope     LocalAdmissionScope
	Responses []model.SignedPublication
}

// PeerInboxSemanticCommitResult reports the exact durable projection of a
// fresh semantic commit or its validated terminal replay.
type PeerInboxSemanticCommitResult struct {
	status         model.InboxStatus
	diagnostic     string
	importedEvent  model.EventID
	responseEvents []model.EventID
	receiptEvent   model.EventID
	hasReceipt     bool
	decision       model.JSON
	changed        bool
	replayed       bool
}

// Status returns the terminal Inbox status.
func (result PeerInboxSemanticCommitResult) Status() model.InboxStatus { return result.status }

// Diagnostic returns the closed terminal diagnostic, if any.
func (result PeerInboxSemanticCommitResult) Diagnostic() string { return result.diagnostic }

// ImportedEventID returns the locally durable identity of the imported Event.
func (result PeerInboxSemanticCommitResult) ImportedEventID() model.EventID {
	return result.importedEvent
}

// ResponseEventIDs returns a defensive copy of the ordered local response IDs.
func (result PeerInboxSemanticCommitResult) ResponseEventIDs() []model.EventID {
	return append([]model.EventID(nil), result.responseEvents...)
}

// ReceiptEventID returns the terminal local receipt Event, when the decision
// emitted a response.
func (result PeerInboxSemanticCommitResult) ReceiptEventID() (model.EventID, bool) {
	return result.receiptEvent, result.hasReceipt
}

// Decision returns the canonical durable decision projection.
func (result PeerInboxSemanticCommitResult) Decision() model.JSON { return result.decision }

// Changed reports whether this call committed the fresh semantic decision.
func (result PeerInboxSemanticCommitResult) Changed() bool { return result.changed }

// Replayed reports whether this call recovered a validated terminal decision.
func (result PeerInboxSemanticCommitResult) Replayed() bool { return result.replayed }

type peerInboxSemanticTerminalRow struct {
	inboxID       model.InboxID
	publication   model.SignedPublication
	requiredRoots []model.Digest
	semanticNonce [32]byte
	status        model.InboxStatus
	attempt       uint32
	diagnostic    string
	updatedAt     time.Time
	decisionRaw   []byte
	receiptEvent  model.EventID
	hasReceipt    bool
}

// PeerInboxSemanticResponseEventID derives the only Event identity accepted
// for one response ordinal. It is stable across admission retries while the
// opaque semantic decision seed prevents a caller from choosing another
// Inbox generation's identity.
func PeerInboxSemanticResponseEventID(seed model.Digest, ordinal uint8) (model.EventID, error) {
	if seed.IsZero() || ordinal > 1 {
		return model.EventID{}, ErrPeerInboxSemanticInput
	}
	input := make([]byte, 0, len(peerInboxSemanticResponseIDDomain)+2+32)
	input = append(input, peerInboxSemanticResponseIDDomain...)
	input = append(input, 0)
	input = append(input, seed.Bytes()...)
	input = append(input, ordinal)
	digest := sha256.Sum256(input)
	id, err := model.ParseEventID("event-semantic-" + hex.EncodeToString(digest[:]))
	if err != nil {
		return model.EventID{}, fmt.Errorf("%w: derive semantic response Event ID: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	return id, nil
}

// CommitPeerInboxSemantic is the atomic materialization boundary for one
// verified audience Inbox row. A terminal replay validates the original
// snapshot, frozen materialization plan and every immutable Event projection
// before returning; it never reacquires expired Channel or lease authority.
func (s *Store) CommitPeerInboxSemantic(ctx context.Context,
	spec CommitPeerInboxSemanticSpec, trustedNow time.Time,
) (PeerInboxSemanticCommitResult, error) {
	decisionAt, committedAt, err := validatePeerInboxSemanticCommitInput(s, ctx, spec, trustedNow)
	if err != nil {
		return PeerInboxSemanticCommitResult{}, err
	}
	requestDigest, err := peerInboxSemanticCommitRequestDigest(spec.Fence, spec.Plan,
		spec.Scope, spec.Responses)
	if err != nil {
		return PeerInboxSemanticCommitResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PeerInboxSemanticCommitResult{}, fmt.Errorf("commit Peer Inbox semantic: begin: %w", err)
	}
	defer tx.Rollback()

	terminal, found, err := readPeerInboxSemanticTerminalRow(ctx, tx, spec.Fence.inboxID)
	if err != nil {
		return PeerInboxSemanticCommitResult{}, err
	}
	if found {
		result, err := validatePeerInboxSemanticTerminalReplay(ctx, tx, terminal, spec,
			requestDigest, decisionAt, committedAt)
		if err != nil {
			return PeerInboxSemanticCommitResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return PeerInboxSemanticCommitResult{}, fmt.Errorf("commit Peer Inbox semantic: replay read: %w", err)
		}
		result.replayed = true
		return result, nil
	}

	row, err := readPeerInboxSemanticRow(ctx, tx, spec.Fence.inboxID)
	if err != nil {
		return PeerInboxSemanticCommitResult{}, err
	}
	_, transitionReceipt, err := readValidatedPeerInboxSemanticTransitionReceipt(ctx, tx, row.inboxID)
	if err != nil {
		return PeerInboxSemanticCommitResult{}, err
	}
	snapshot, err := derivePeerInboxSemanticSnapshot(ctx, tx, row, committedAt)
	if err != nil {
		return PeerInboxSemanticCommitResult{}, err
	}
	if snapshot.digest != spec.Fence.snapshotDigest {
		return PeerInboxSemanticCommitResult{}, ErrPeerInboxSemanticStale
	}
	if err := requireLivePeerInboxSemanticFence(row, spec.Fence, committedAt); err != nil {
		return PeerInboxSemanticCommitResult{}, err
	}
	if decisionAt.Before(row.updatedAt) {
		return PeerInboxSemanticCommitResult{}, ErrPeerInboxSemanticStale
	}
	localAudience := snapshot.importedEvent.Audience().Peers()
	if len(localAudience) != 1 {
		return PeerInboxSemanticCommitResult{}, fmt.Errorf("%w: imported Event local audience",
			ErrPeerInboxSemanticInvariant)
	}
	plan := spec.Plan
	if err := validatePeerInboxSemanticPlan(plan); err != nil {
		return PeerInboxSemanticCommitResult{}, err
	}
	if err := validatePeerInboxSemanticWorkPredecessor(snapshot, plan); err != nil {
		return PeerInboxSemanticCommitResult{}, err
	}
	if err := validatePeerInboxSemanticResponses(snapshot, plan, spec.Scope, spec.Responses,
		decisionAt); err != nil {
		return PeerInboxSemanticCommitResult{}, err
	}

	if err := insertAcceptedEvent(ctx, tx, snapshot.publication); err != nil {
		return PeerInboxSemanticCommitResult{}, fmt.Errorf("%w: import Event: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	if err := materializePeerInboxSemanticArtifacts(ctx, tx, snapshot.importedEvent); err != nil {
		return PeerInboxSemanticCommitResult{}, err
	}
	responses := peerInboxSemanticResponseEvents(spec.Responses)
	if workIntent, ok := plan.Work(); ok &&
		workIntent.Source() == PeerInboxSemanticFromImportedEvent {
		mutation, err := peerInboxSemanticWorkMutation(workIntent, snapshot.importedEvent, responses)
		if err != nil {
			return PeerInboxSemanticCommitResult{}, err
		}
		if err := applyWorkMutation(ctx, tx, mutation, snapshot.importedEvent); err != nil {
			return PeerInboxSemanticCommitResult{}, fmt.Errorf("%w: imported Work mutation: %v",
				ErrPeerInboxSemanticInvariant, err)
		}
		if mutation.Work.State().Terminal() {
			if err := reconcileWorkDerivationDisposition(ctx, tx, mutation.Work.Ref()); err != nil {
				return PeerInboxSemanticCommitResult{}, err
			}
		}
	}
	if settlement, ok := plan.Settlement(); ok {
		if err := settlePeerInboxSemanticHandling(ctx, tx, settlement, committedAt); err != nil {
			return PeerInboxSemanticCommitResult{}, err
		}
	}
	if len(spec.Responses) != 0 {
		items, references, err := peerInboxSemanticLocalItems(plan, spec.Responses,
			snapshot.importedEvent)
		if err != nil {
			return PeerInboxSemanticCommitResult{}, err
		}
		_, err = applyLocalAcceptanceTx(ctx, tx, LocalAcceptanceSpec{Scope: spec.Scope,
			Items: items, Controller: true, AuthorizedReferences: references,
			semanticControllerBatch: len(items) == 2}, committedAt, false)
		if err != nil {
			return PeerInboxSemanticCommitResult{}, err
		}
	}
	if handling, ok := plan.Handling(); ok {
		if err := materializePeerInboxSemanticHandling(ctx, tx, handling,
			snapshot.importedEvent, responses); err != nil {
			return PeerInboxSemanticCommitResult{}, err
		}
	}

	decision, err := newPeerInboxSemanticDecision(ctx, tx, row, snapshot, plan, spec.Responses,
		requestDigest, decisionAt, committedAt)
	if err != nil {
		return PeerInboxSemanticCommitResult{}, err
	}
	decisionJSON, err := encodePeerInboxSemanticDecision(decision)
	if err != nil {
		return PeerInboxSemanticCommitResult{}, err
	}
	if err := deletePeerInboxSemanticTransitionReceipt(ctx, tx, row.inboxID,
		transitionReceipt); err != nil {
		return PeerInboxSemanticCommitResult{}, err
	}
	var receipt any
	if len(responses) != 0 {
		receipt = responses[len(responses)-1].ID().String()
	}
	diagnostic := any(nil)
	if plan.Diagnostic() != "" {
		diagnostic = plan.Diagnostic()
	}
	result, err := tx.ExecContext(ctx, `UPDATE peer_inbox SET status=?,next_attempt_at=?,
		lease_owner=NULL,lease_until=NULL,local_event_id=?,decision_json=?,receipt_event_id=?,
		diagnostic=?,updated_at=? WHERE inbox_id=? AND status='processing' AND attempts=?
		AND lease_owner=? AND lease_until=? AND semantic_nonce=? AND local_event_id IS NULL
		AND decision_json IS NULL AND receipt_event_id IS NULL AND diagnostic IS NULL`,
		string(plan.InboxStatus()), storeTime(committedAt), snapshot.importedEvent.ID().String(),
		decisionJSON.Bytes(), receipt, diagnostic, storeTime(committedAt), row.inboxID.String(),
		spec.Fence.attempt, spec.Fence.leaseOwner, storeTime(spec.Fence.leaseUntil),
		spec.Fence.semanticNonce[:])
	if err != nil {
		return PeerInboxSemanticCommitResult{}, fmt.Errorf("%w: terminal Inbox CAS: %v",
			ErrPeerInboxSemanticStale, err)
	}
	if err := exactlyOne(result); err != nil {
		return PeerInboxSemanticCommitResult{}, fmt.Errorf("%w: terminal Inbox CAS: %v",
			ErrPeerInboxSemanticStale, err)
	}
	terminal, found, err = readPeerInboxSemanticTerminalRow(ctx, tx, row.inboxID)
	if err != nil || !found {
		return PeerInboxSemanticCommitResult{}, fmt.Errorf("%w: terminal Inbox projection: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	validated, err := validatePeerInboxSemanticTerminalReplay(ctx, tx, terminal, spec,
		requestDigest, decisionAt, committedAt)
	if err != nil {
		return PeerInboxSemanticCommitResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return PeerInboxSemanticCommitResult{}, fmt.Errorf("commit Peer Inbox semantic: commit: %w", err)
	}
	validated.changed = true
	return validated, nil
}

func validatePeerInboxSemanticCommitInput(s *Store, ctx context.Context,
	spec CommitPeerInboxSemanticSpec, trustedNow time.Time,
) (time.Time, time.Time, error) {
	decisionAt, err := canonicalStoreTime(spec.Plan.DecisionAt())
	if err != nil || decisionAt.IsZero() {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: decision time: %v",
			ErrPeerInboxSemanticInput, err)
	}
	committedAt, err := validatePeerInboxSemanticFenceCall(s, ctx, spec.Fence, trustedNow)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	if committedAt.Before(decisionAt) {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: trusted commit time precedes decision time",
			ErrPeerInboxSemanticInput)
	}
	if spec.Plan.inboxID != spec.Fence.inboxID || spec.Plan.attempt != spec.Fence.attempt ||
		spec.Plan.snapshotDigest != spec.Fence.snapshotDigest ||
		!spec.Plan.disposition.Valid() {
		return time.Time{}, time.Time{}, ErrPeerInboxSemanticInput
	}
	if len(spec.Responses) > 2 {
		return time.Time{}, time.Time{}, ErrPeerInboxSemanticInput
	}
	if len(spec.Responses) == 0 {
		if !peerInboxSemanticAdmissionScopeZero(spec.Scope) {
			return time.Time{}, time.Time{}, ErrPeerInboxSemanticInput
		}
	} else if spec.Scope.Count() != uint8(len(spec.Responses)) ||
		spec.Scope.ChannelID().IsZero() || spec.Scope.Node().PeerID().IsZero() ||
		spec.Scope.Profile().ID() != model.TeamworkProfileID() {
		return time.Time{}, time.Time{}, ErrPeerInboxSemanticInput
	}
	return decisionAt, committedAt, nil
}

func peerInboxSemanticAdmissionScopeZero(scope LocalAdmissionScope) bool {
	return scope.Count() == 0 && scope.ChannelID().IsZero() && scope.Node().PeerID().IsZero() &&
		scope.Node().OriginEpoch().IsZero() && scope.Profile().ID().IsZero() &&
		scope.OriginMember().IsZero() && scope.PublicationRoster().IsZero() &&
		scope.FirstOriginSequence() == 0 && scope.FirstChannelSequence() == 0
}

func validatePeerInboxSemanticPlan(plan PeerInboxSemanticPlan) error {
	if !plan.Disposition().Valid() ||
		(plan.InboxStatus() != model.InboxAccepted && plan.InboxStatus() != model.InboxRejected &&
			plan.InboxStatus() != model.InboxConflicted) || len(plan.Responses()) > 2 {
		return fmt.Errorf("%w: planner returned an invalid terminal shape",
			ErrPeerInboxSemanticInvariant)
	}
	wantStatus := map[PeerInboxSemanticDisposition]model.InboxStatus{
		PeerInboxSemanticApply:       model.InboxAccepted,
		PeerInboxSemanticReceiptOnly: model.InboxAccepted,
		PeerInboxSemanticReject:      model.InboxRejected,
		PeerInboxSemanticConflict:    model.InboxConflicted,
	}[plan.Disposition()]
	if plan.InboxStatus() != wantStatus {
		return fmt.Errorf("%w: planner disposition/status mismatch",
			ErrPeerInboxSemanticInvariant)
	}
	if (plan.InboxStatus() == model.InboxAccepted) != (plan.Diagnostic() == "") ||
		(plan.InboxStatus() == model.InboxRejected || plan.InboxStatus() == model.InboxConflicted) &&
			!validPublicationDiagnostic(plan.Diagnostic()) {
		return fmt.Errorf("%w: planner diagnostic/status mismatch",
			ErrPeerInboxSemanticInvariant)
	}
	return nil
}

func validatePeerInboxSemanticWorkPredecessor(snapshot peerInboxSemanticSnapshot,
	plan PeerInboxSemanticPlan,
) error {
	intent, ok := plan.Work()
	if !ok {
		return nil
	}
	if intent.IsCreation() {
		if snapshot.hasCurrentWork || intent.ExpectedVersion() != 0 ||
			intent.ExpectedIteration() != 0 || intent.ExpectedState() != "" {
			return fmt.Errorf("%w: Work creation predecessor differs",
				ErrPeerInboxSemanticInvariant)
		}
		return nil
	}
	if !snapshot.hasCurrentWork || intent.ExpectedVersion() != snapshot.currentWork.Version() ||
		intent.ExpectedIteration() != snapshot.currentWork.Iteration() ||
		intent.ExpectedState() != snapshot.currentWork.State() ||
		intent.WorkRef() != snapshot.currentWork.Ref() ||
		intent.ChannelID() != snapshot.currentWork.ChannelID() {
		return fmt.Errorf("%w: Work transition predecessor differs",
			ErrPeerInboxSemanticInvariant)
	}
	return nil
}

func validatePeerInboxSemanticResponses(snapshot peerInboxSemanticSnapshot,
	plan PeerInboxSemanticPlan, scope LocalAdmissionScope, publications []model.SignedPublication,
	at time.Time,
) error {
	intents := plan.Responses()
	if len(intents) != len(publications) {
		return fmt.Errorf("%w: response count differs from deterministic plan",
			ErrPeerInboxSemanticInput)
	}
	for index, publication := range publications {
		event := publication.Event()
		wantID, err := PeerInboxSemanticResponseEventID(snapshot.decisionSeed, uint8(index))
		expectedScope, scopeErr := scope.EventScope(uint8(index), snapshot.importedEvent.Scope().WorkRef())
		intent := intents[index]
		causes := event.CausedBy()
		if err != nil || scopeErr != nil || event.ID() != wantID || event.Source() != model.EventSourceLocal ||
			event.Scope() != expectedScope || event.Type() != intent.EventType() ||
			event.ActorPrincipal() != scope.Profile().Principal() ||
			event.Payload().String() != intent.Payload().String() || len(causes) != 1 ||
			causes[0] != intent.Cause() || !event.AcceptedAt().Equal(at) ||
			!event.CreatedAt().Equal(at) || event.Audience().Len() != 1 ||
			!event.Audience().Contains(snapshot.importedEvent.Scope().OriginPeerID()) ||
			event.Summary() != peerInboxSemanticResponseSummary(event.Type()) {
			return fmt.Errorf("%w: response %d differs from deterministic intent",
				ErrPeerInboxSemanticInput, index)
		}
		if !peerInboxSemanticResponseArtifactsMatch(intent, snapshot.importedEvent,
			event.Artifacts()) {
			return fmt.Errorf("%w: response %d Artifact closure differs from imported authority",
				ErrPeerInboxSemanticInput, index)
		}
	}
	return nil
}

func peerInboxSemanticResponseArtifactsMatch(intent PeerInboxSemanticResponseIntent,
	imported model.Event, actual []model.ArtifactRef,
) bool {
	if intent.EventType() != model.EventReviewDelivered {
		return len(actual) == 0
	}
	if imported.Type() != model.EventReviewDeliveryReady || intent.Cause() != imported.Key() {
		return false
	}
	source := imported.Artifacts()
	if len(source) != len(actual) {
		return false
	}
	for index := range source {
		if (source[index].Role() != model.ArtifactProduced &&
			source[index].Role() != model.ArtifactReferenced) ||
			actual[index].Role() != model.ArtifactReferenced ||
			actual[index].RootDigest() != source[index].RootDigest() {
			return false
		}
	}
	return true
}

func peerInboxSemanticResponseSummary(eventType model.EventType) string {
	return map[model.EventType]string{
		model.EventReviewAccepted:       "Review accepted",
		model.EventReviewAcceptRejected: "Review acceptance rejected",
		model.EventReviewDelivered:      "Review delivered",
		model.EventReviewDeclined:       "Review declined",
		model.EventReviewExpired:        "Review expired",
		model.EventReviewOutcome:        "Review outcome",
	}[eventType]
}

// PeerInboxSemanticResponseSummary is the canonical controller-owned summary
// used by the semantic worker before Store verifies the signed response.
func PeerInboxSemanticResponseSummary(eventType model.EventType) string {
	return peerInboxSemanticResponseSummary(eventType)
}

func materializePeerInboxSemanticArtifacts(ctx context.Context, tx *sql.Tx,
	event model.Event,
) error {
	for _, ref := range event.Artifacts() {
		switch ref.Role() {
		case model.ArtifactProduced:
			provenance, err := model.NewArtifactProvenance(model.ArtifactProvenanceSpec{
				RootDigest: ref.RootDigest(), ProducerEvent: event.Key(),
				ProducerOriginPeer: event.Scope().OriginPeerID(), Relation: model.ProvenanceReplica,
				CreatedAt: event.AcceptedAt(),
			})
			if err != nil {
				return fmt.Errorf("%w: replica provenance: %v", ErrPeerInboxSemanticInvariant, err)
			}
			if _, err := insertArtifactProvenance(ctx, tx, provenance); err != nil {
				return fmt.Errorf("%w: replica provenance insert: %v", ErrPeerInboxSemanticInvariant, err)
			}
		case model.ArtifactReferenced:
			if err := requireReusableArtifactRoot(ctx, tx, ref.RootDigest()); err != nil {
				return fmt.Errorf("%w: imported referenced Artifact: %v",
					ErrPeerInboxSemanticInvariant, err)
			}
		default:
			return fmt.Errorf("%w: unknown imported Artifact role", ErrPeerInboxSemanticInvariant)
		}
		if _, err := insertEventArtifactPin(ctx, tx, ref.RootDigest(), event.ID(),
			event.AcceptedAt()); err != nil {
			return fmt.Errorf("%w: imported Event Artifact pin: %v",
				ErrPeerInboxSemanticInvariant, err)
		}
	}
	return nil
}

func peerInboxSemanticResponseEvents(publications []model.SignedPublication) []model.Event {
	events := make([]model.Event, len(publications))
	for index := range publications {
		events[index] = publications[index].Event()
	}
	return events
}

func peerInboxSemanticWorkMutation(intent PeerInboxSemanticWorkIntent, imported model.Event,
	responses []model.Event,
) (WorkMutation, error) {
	source, err := peerInboxSemanticIntentSourceEvent(intent.Source(), intent.ResponseOrdinal(),
		imported, responses)
	if err != nil || intent.WorkRef() != source.Scope().WorkRef() ||
		intent.ChannelID() != source.Scope().ChannelID() ||
		intent.ObservedAtUnixNano() != source.AcceptedAt().UnixNano() {
		return WorkMutation{}, fmt.Errorf("%w: Work intent source differs",
			ErrPeerInboxSemanticInvariant)
	}
	work, err := model.NewReviewWork(model.ReviewWorkSpec{Ref: intent.WorkRef(),
		ChannelID: intent.ChannelID(), Participants: intent.Participants(), Version: intent.NextVersion(),
		Iteration: intent.NextIteration(), DeadlineUnixNano: intent.DeadlineUnixNano(),
		State: intent.NextState(), StateData: intent.StateData(), UpdatedBy: source.ID(),
		UpdatedAt: source.AcceptedAt()})
	if err != nil {
		return WorkMutation{}, fmt.Errorf("%w: construct planned Work: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	if intent.IsCreation() {
		mutation, err := NewWorkCreation(work)
		if err != nil {
			return WorkMutation{}, fmt.Errorf("%w: planned Work creation: %v",
				ErrPeerInboxSemanticInvariant, err)
		}
		return mutation, nil
	}
	mutation, err := NewWorkTransition(work, intent.ExpectedVersion(), intent.ExpectedState())
	if err != nil || intent.ExpectedIteration() == 0 {
		return WorkMutation{}, fmt.Errorf("%w: planned Work transition: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	return mutation, nil
}

func peerInboxSemanticIntentSourceEvent(source PeerInboxSemanticEffectSource, ordinal uint8,
	imported model.Event, responses []model.Event,
) (model.Event, error) {
	switch source {
	case PeerInboxSemanticFromImportedEvent:
		if ordinal != 0 {
			return model.Event{}, ErrPeerInboxSemanticInvariant
		}
		return imported, nil
	case PeerInboxSemanticFromLocalResponse:
		if int(ordinal) >= len(responses) {
			return model.Event{}, ErrPeerInboxSemanticInvariant
		}
		return responses[ordinal], nil
	default:
		return model.Event{}, ErrPeerInboxSemanticInvariant
	}
}

func peerInboxSemanticLocalItems(plan PeerInboxSemanticPlan,
	publications []model.SignedPublication, imported model.Event,
) ([]LocalAcceptanceItem, []model.Digest, error) {
	items := make([]LocalAcceptanceItem, len(publications))
	responses := peerInboxSemanticResponseEvents(publications)
	for index := range publications {
		items[index].Publication = publications[index]
	}
	if intent, ok := plan.Work(); ok && intent.Source() == PeerInboxSemanticFromLocalResponse {
		mutation, err := peerInboxSemanticWorkMutation(intent, imported, responses)
		if err != nil {
			return nil, nil, err
		}
		if int(intent.ResponseOrdinal()) >= len(items) {
			return nil, nil, ErrPeerInboxSemanticInvariant
		}
		items[intent.ResponseOrdinal()].Work = &mutation
	}
	referenceSet := make(map[model.Digest]struct{})
	for _, intent := range plan.Responses() {
		if intent.EventType() != model.EventReviewDelivered || intent.Cause() != imported.Key() {
			continue
		}
		for _, ref := range imported.Artifacts() {
			if ref.Role() != model.ArtifactProduced && ref.Role() != model.ArtifactReferenced {
				return nil, nil, fmt.Errorf("%w: delivered source has an invalid root role",
					ErrPeerInboxSemanticInvariant)
			}
			referenceSet[ref.RootDigest()] = struct{}{}
		}
	}
	references := make([]model.Digest, 0, len(referenceSet))
	for root := range referenceSet {
		references = append(references, root)
	}
	sort.Slice(references, func(i, j int) bool { return references[i].String() < references[j].String() })
	return items, references, nil
}

func materializePeerInboxSemanticHandling(ctx context.Context, tx *sql.Tx,
	intent PeerInboxSemanticHandlingIntent, imported model.Event, responses []model.Event,
) error {
	var source model.Event
	switch intent.Source() {
	case PeerInboxSemanticFromImportedEvent:
		if intent.ResponseOrdinal() != 0 {
			return ErrPeerInboxSemanticInvariant
		}
		source = imported
	case PeerInboxSemanticFromLocalResponse:
		if int(intent.ResponseOrdinal()) >= len(responses) {
			return ErrPeerInboxSemanticInvariant
		}
		source = responses[intent.ResponseOrdinal()]
	default:
		return ErrPeerInboxSemanticInvariant
	}
	if source.Type() != intent.SourceEventType() || source.Scope().WorkRef() != intent.WorkRef() {
		return fmt.Errorf("%w: Handling source differs from plan", ErrPeerInboxSemanticInvariant)
	}
	work, err := readReviewWork(ctx, tx, intent.WorkRef())
	if err != nil || work.ChannelID() != source.Scope().ChannelID() {
		return fmt.Errorf("%w: Handling Work is unavailable", ErrPeerInboxSemanticInvariant)
	}
	localPeer, err := readNode(ctx, tx)
	if err != nil {
		return fmt.Errorf("%w: Handling local Node: %v", ErrPeerInboxSemanticInvariant, err)
	}
	rolePeer := work.Participants().InitiatorPeerID()
	if intent.LocalRole() == model.WorkRoleReviewer {
		rolePeer = work.Participants().ReviewerPeerID()
	} else if intent.LocalRole() != model.WorkRoleInitiator {
		return fmt.Errorf("%w: Handling role is invalid", ErrPeerInboxSemanticInvariant)
	}
	if rolePeer != localPeer.PeerID() {
		return fmt.Errorf("%w: Handling role is not local", ErrPeerInboxSemanticInvariant)
	}
	handlingID, err := peerInboxSemanticHandlingID(source.ID())
	if err != nil {
		return err
	}
	handling, err := model.NewHandling(model.HandlingSpec{ID: handlingID,
		ProfileID: model.TeamworkProfileID(), EventID: source.ID(), Status: model.HandlingPending,
		AvailableAt: source.AcceptedAt(), CreatedAt: source.AcceptedAt(), UpdatedAt: source.AcceptedAt()})
	if err != nil {
		return fmt.Errorf("%w: construct Handling: %v", ErrPeerInboxSemanticInvariant, err)
	}
	if _, err := insertAgentHandling(ctx, tx, handling); err != nil {
		return fmt.Errorf("%w: insert Handling: %v", ErrPeerInboxSemanticInvariant, err)
	}
	return requirePeerInboxSemanticHandlingPins(ctx, tx, handling, source, true)
}

func peerInboxSemanticHandlingID(event model.EventID) (model.HandlingID, error) {
	if event.IsZero() {
		return model.HandlingID{}, ErrPeerInboxSemanticInvariant
	}
	digest := sha256.Sum256([]byte(peerInboxSemanticHandlingIDDomain + "\x00" + event.String()))
	id, err := model.ParseHandlingID("handling-semantic-" + hex.EncodeToString(digest[:]))
	if err != nil {
		return model.HandlingID{}, fmt.Errorf("%w: derive Handling ID: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	return id, nil
}

func requirePeerInboxSemanticHandlingPins(ctx context.Context, tx *sql.Tx,
	handling model.Handling, source model.Event, insertMissing bool,
) error {
	expected := make(map[model.Digest]struct{}, len(source.Artifacts()))
	for _, ref := range source.Artifacts() {
		expected[ref.RootDigest()] = struct{}{}
	}
	rows, err := tx.QueryContext(ctx, `SELECT root_digest,created_at FROM artifact_pins
		WHERE owner_kind='handling' AND owner_id=? ORDER BY root_digest`, handling.ID().String())
	if err != nil {
		return fmt.Errorf("%w: read Handling pins: %v", ErrPeerInboxSemanticInvariant, err)
	}
	present := make(map[model.Digest]struct{}, len(expected))
	for rows.Next() {
		var rootText, createdText string
		if err := rows.Scan(&rootText, &createdText); err != nil {
			rows.Close()
			return fmt.Errorf("%w: scan Handling pin: %v", ErrPeerInboxSemanticInvariant, err)
		}
		root, rootErr := model.ParseDigest(rootText)
		created, createdErr := parseCanonicalStoreTime(createdText)
		if rootErr != nil || createdErr != nil || !created.Equal(handling.CreatedAt()) {
			rows.Close()
			return fmt.Errorf("%w: malformed Handling pin", ErrPeerInboxSemanticInvariant)
		}
		if _, ok := expected[root]; !ok {
			rows.Close()
			return fmt.Errorf("%w: unexpected Handling pin", ErrPeerInboxSemanticInvariant)
		}
		present[root] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("%w: iterate Handling pins: %v", ErrPeerInboxSemanticInvariant, err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("%w: close Handling pins: %v", ErrPeerInboxSemanticInvariant, err)
	}
	for root := range expected {
		if _, ok := present[root]; ok {
			continue
		}
		if !insertMissing {
			return fmt.Errorf("%w: missing Handling pin", ErrPeerInboxSemanticInvariant)
		}
		if err := insertArtifactOwnerPin(ctx, tx, root, "handling", handling.ID().String(),
			handling.CreatedAt()); err != nil {
			return fmt.Errorf("%w: insert Handling pin: %v", ErrPeerInboxSemanticInvariant, err)
		}
	}
	return nil
}

func newPeerInboxSemanticDecision(ctx context.Context, tx *sql.Tx, row peerInboxSemanticRow,
	snapshot peerInboxSemanticSnapshot,
	plan PeerInboxSemanticPlan, responses []model.SignedPublication, requestDigest model.Digest,
	decisionAt, committedAt time.Time,
) (peerInboxSemanticDecision, error) {
	causal := make([]peerInboxSemanticEventDecision, len(snapshot.causalEvents))
	for index, event := range snapshot.causalEvents {
		publicationDigest, err := readPeerInboxSemanticPublicationDigest(ctx, tx, event.ID())
		if err != nil {
			return peerInboxSemanticDecision{}, fmt.Errorf("%w: causal publication digest: %v",
				ErrPeerInboxSemanticInvariant, err)
		}
		causal[index] = peerInboxSemanticEventProjection(event, publicationDigest)
	}
	responseEvidence := make([]peerInboxSemanticEventDecision, len(responses))
	responseIDs := make([]model.EventID, len(responses))
	for index, publication := range responses {
		responseEvidence[index] = peerInboxSemanticEventProjection(publication.Event(),
			publication.Digest())
		responseIDs[index] = publication.Event().ID()
	}
	priorWork := (*peerInboxSemanticWorkDecision)(nil)
	if snapshot.hasCurrentWork {
		priorWork = peerInboxSemanticWorkProjection(snapshot.currentWork)
	}
	receipt := ""
	if len(responseIDs) != 0 {
		receipt = responseIDs[len(responseIDs)-1].String()
	}
	return peerInboxSemanticDecision{Attempt: row.attempts, CausalEvents: causal,
		CommittedAt: storeTime(committedAt), DecidedAt: storeTime(decisionAt),
		DecisionSeed: snapshot.decisionSeed.String(),
		Domain:       peerInboxSemanticDecisionDomain,
		ImportedEvent: peerInboxSemanticEventProjection(snapshot.importedEvent,
			snapshot.publication.Digest()), InboxID: row.inboxID.String(),
		Plan: peerInboxSemanticPlanProjection(plan), PriorWork: priorWork,
		ReceiptEventID: receipt, RequestDigest: requestDigest.String(),
		Responses: responseEvidence, SchemaVersion: 1,
		SnapshotDigest: snapshot.digest.String(), Status: string(plan.InboxStatus())}, nil
}

func deletePeerInboxSemanticTransitionReceipt(ctx context.Context, tx *sql.Tx,
	inboxID model.InboxID, found bool,
) error {
	if !found {
		return nil
	}
	result, err := tx.ExecContext(ctx,
		"DELETE FROM peer_inbox_semantic_transition_receipts WHERE inbox_id=?", inboxID.String())
	if err != nil || exactlyOne(result) != nil {
		return fmt.Errorf("%w: delete semantic transition receipt: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	return nil
}

func readPeerInboxSemanticPublicationDigest(ctx context.Context, q rowQuerier,
	event model.EventID,
) (model.Digest, error) {
	if ctx == nil || q == nil || event.IsZero() {
		return model.Digest{}, ErrPeerInboxSemanticInvariant
	}
	var raw []byte
	if err := q.QueryRowContext(ctx, "SELECT publication_digest FROM events WHERE event_id=?",
		event.String()).Scan(&raw); err != nil {
		return model.Digest{}, err
	}
	return model.DigestFromBytes(raw)
}

func readPeerInboxSemanticTerminalRow(ctx context.Context, tx *sql.Tx,
	inboxID model.InboxID,
) (peerInboxSemanticTerminalRow, bool, error) {
	var inboxText, statusText, nextText, updatedText string
	var channelText, originText, epochText, eventText string
	var originSequence, channelSequence uint64
	var eventDigestRaw, publicationDigestRaw, signature, publicationRaw []byte
	var rootsRaw, nonceRaw, decisionRaw []byte
	var attempt int64
	var isAudience int
	var leaseOwner, leaseUntil, diagnostic, localEvent, receiptEvent sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT inbox_id,status,attempts,next_attempt_at,lease_owner,
		lease_until,diagnostic,updated_at,local_event_id,receipt_event_id,decision_json,
		is_audience,semantic_nonce,required_artifact_roots_json,channel_id,origin_peer_id,
		origin_epoch,origin_seq,channel_seq,event_id,event_digest,publication_digest,
		origin_signature,publication_json FROM peer_inbox WHERE inbox_id=?`, inboxID.String()).
		Scan(&inboxText, &statusText, &attempt, &nextText, &leaseOwner, &leaseUntil,
			&diagnostic, &updatedText, &localEvent, &receiptEvent, &decisionRaw,
			&isAudience, &nonceRaw, &rootsRaw, &channelText, &originText, &epochText,
			&originSequence, &channelSequence, &eventText, &eventDigestRaw,
			&publicationDigestRaw, &signature, &publicationRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return peerInboxSemanticTerminalRow{}, false, ErrPeerInboxSemanticStale
	}
	if err != nil {
		return peerInboxSemanticTerminalRow{}, false, fmt.Errorf("%w: read terminal Inbox: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	status := model.InboxStatus(statusText)
	if status != model.InboxAccepted && status != model.InboxRejected &&
		status != model.InboxConflicted {
		return peerInboxSemanticTerminalRow{}, false, nil
	}
	parsedInbox, inboxErr := model.ParseInboxID(inboxText)
	nextAttempt, nextErr := parseCanonicalStoreTime(nextText)
	updatedAt, updatedErr := parseCanonicalStoreTime(updatedText)
	if inboxErr != nil || parsedInbox != inboxID || attempt < 1 || uint64(attempt) > math.MaxUint32 ||
		nextErr != nil || updatedErr != nil || !nextAttempt.Equal(updatedAt) ||
		leaseOwner.Valid || leaseUntil.Valid || isAudience != 1 || len(nonceRaw) != 32 ||
		!localEvent.Valid || localEvent.String != eventText || len(decisionRaw) < 2 {
		return peerInboxSemanticTerminalRow{}, false, fmt.Errorf("%w: malformed terminal Inbox shape",
			ErrPeerInboxSemanticInvariant)
	}
	if status == model.InboxAccepted {
		if diagnostic.Valid {
			return peerInboxSemanticTerminalRow{}, false, fmt.Errorf("%w: accepted Inbox has diagnostic",
				ErrPeerInboxSemanticInvariant)
		}
	} else if !diagnostic.Valid || !validPublicationDiagnostic(diagnostic.String) {
		return peerInboxSemanticTerminalRow{}, false, fmt.Errorf("%w: terminal Inbox diagnostic",
			ErrPeerInboxSemanticInvariant)
	}
	incumbent := peerInboxIncumbent{inboxID: inboxText, channelID: channelText,
		originPeerID: originText, originEpoch: epochText, originSequence: originSequence,
		channelSequence: channelSequence, eventID: eventText, eventDigest: eventDigestRaw,
		publicationDigest: publicationDigestRaw, signature: signature, wire: publicationRaw}
	if err := validatePeerInboxIncumbent(incumbent); err != nil {
		return peerInboxSemanticTerminalRow{}, false, fmt.Errorf("%w: terminal publication tuple: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	parsedPublication, err := model.ParseSignedPublication(publicationRaw)
	if err != nil {
		return peerInboxSemanticTerminalRow{}, false, fmt.Errorf("%w: terminal signed publication: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	publication, err := model.ProjectImportedPublication(&parsedPublication)
	if err != nil || publication.Event().ID().String() != eventText {
		return peerInboxSemanticTerminalRow{}, false, fmt.Errorf("%w: terminal imported publication: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	expectedRoots := peerInboxArtifactRoots(publication.Event())
	expectedRootsJSON, rootsErr := model.JSONFrom(expectedRoots)
	canonicalRoots, canonicalErr := model.NewJSON(rootsRaw)
	if rootsErr != nil || canonicalErr != nil || !bytes.Equal(canonicalRoots.Bytes(), rootsRaw) ||
		!bytes.Equal(expectedRootsJSON.Bytes(), rootsRaw) {
		return peerInboxSemanticTerminalRow{}, false, fmt.Errorf("%w: terminal Artifact roots",
			ErrPeerInboxSemanticInvariant)
	}
	var nonce [32]byte
	copy(nonce[:], nonceRaw)
	var parsedReceipt model.EventID
	if receiptEvent.Valid {
		parsedReceipt, err = model.ParseEventID(receiptEvent.String)
		if err != nil {
			return peerInboxSemanticTerminalRow{}, false, fmt.Errorf("%w: terminal receipt ID",
				ErrPeerInboxSemanticInvariant)
		}
	}
	return peerInboxSemanticTerminalRow{inboxID: parsedInbox, publication: publication,
		requiredRoots: expectedRoots, semanticNonce: nonce, status: status,
		attempt: uint32(attempt), diagnostic: diagnostic.String, updatedAt: updatedAt,
		decisionRaw: append([]byte(nil), decisionRaw...), receiptEvent: parsedReceipt,
		hasReceipt: receiptEvent.Valid}, true, nil
}

func validatePeerInboxSemanticTerminalReplay(ctx context.Context, tx *sql.Tx,
	row peerInboxSemanticTerminalRow, spec CommitPeerInboxSemanticSpec,
	requestDigest model.Digest, decisionAt, trustedNow time.Time,
) (PeerInboxSemanticCommitResult, error) {
	decision, decisionJSON, err := decodePeerInboxSemanticDecision(row.decisionRaw)
	if err != nil {
		return PeerInboxSemanticCommitResult{}, err
	}
	decidedAt, decidedErr := parseCanonicalStoreTime(decision.DecidedAt)
	committedAt, committedErr := parseCanonicalStoreTime(decision.CommittedAt)
	decisionSeed, seedErr := model.ParseDigest(decision.DecisionSeed)
	snapshotDigest, snapshotErr := model.ParseDigest(decision.SnapshotDigest)
	storedRequest, requestErr := model.ParseDigest(decision.RequestDigest)
	expectedSeed := peerInboxSemanticDecisionSeed(row.semanticNonce)
	if decidedErr != nil || committedErr != nil || seedErr != nil || snapshotErr != nil ||
		requestErr != nil ||
		decision.InboxID != row.inboxID.String() || decision.InboxID != spec.Fence.inboxID.String() ||
		decision.Attempt != row.attempt || decision.Attempt != spec.Fence.attempt ||
		decision.Status != string(row.status) || decision.Plan.InboxStatus != decision.Status ||
		decision.Plan.Diagnostic != row.diagnostic || !committedAt.Equal(row.updatedAt) ||
		!decidedAt.Equal(decisionAt) || committedAt.Before(decidedAt) || trustedNow.Before(committedAt) ||
		decisionSeed != expectedSeed ||
		snapshotDigest != spec.Fence.snapshotDigest || storedRequest != requestDigest ||
		spec.Fence.semanticNonce != row.semanticNonce {
		return PeerInboxSemanticCommitResult{}, fmt.Errorf("%w: terminal decision authority differs",
			ErrPeerInboxSemanticInvariant)
	}
	if err := requireExactPeerInboxArtifactPins(ctx, tx, row.inboxID, row.requiredRoots,
		committedAt); err != nil {
		return PeerInboxSemanticCommitResult{}, fmt.Errorf("%w: terminal permanent Inbox pins: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	imported, importedPublication, err := readPeerInboxSemanticStoredPublication(ctx, tx,
		row.publication.Event().ID())
	if err != nil || imported.Source() != model.EventSourceImported ||
		!bytes.Equal(importedPublication.WireJSON().Bytes(), row.publication.WireJSON().Bytes()) ||
		!peerInboxSemanticEventDecisionMatches(decision.ImportedEvent, imported,
			importedPublication.Digest()) {
		return PeerInboxSemanticCommitResult{}, fmt.Errorf("%w: terminal imported Event evidence: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	priorWork, hasPriorWork, err := peerInboxSemanticWorkFromDecision(decision.PriorWork)
	if err != nil {
		return PeerInboxSemanticCommitResult{}, err
	}
	if hasPriorWork {
		update, updateErr := readCurrentSourceEvent(ctx, tx, priorWork.UpdatedBy())
		if updateErr != nil || priorWork.ValidateUpdateEvent(update) != nil ||
			!priorWork.UpdatedAt().Equal(update.AcceptedAt()) {
			return PeerInboxSemanticCommitResult{}, fmt.Errorf("%w: prior Work update evidence",
				ErrPeerInboxSemanticInvariant)
		}
	}
	causalEvents := make([]model.Event, len(decision.CausalEvents))
	for index, evidence := range decision.CausalEvents {
		eventID, parseErr := model.ParseEventID(evidence.EventID)
		if parseErr != nil {
			return PeerInboxSemanticCommitResult{}, fmt.Errorf("%w: causal Event ID",
				ErrPeerInboxSemanticInvariant)
		}
		event, publication, readErr := readPeerInboxSemanticStoredPublication(ctx, tx, eventID)
		if readErr != nil || !peerInboxSemanticEventDecisionMatches(evidence, event,
			publication.Digest()) {
			return PeerInboxSemanticCommitResult{}, fmt.Errorf("%w: causal Event evidence %d: %v",
				ErrPeerInboxSemanticInvariant, index, readErr)
		}
		causalEvents[index] = event
	}
	snapshotRow := peerInboxSemanticRow{peerInboxArtifactRow: peerInboxArtifactRow{
		inboxID: row.inboxID, publication: row.publication,
		requiredRoots: append([]model.Digest(nil), row.requiredRoots...)}, semanticNonce: row.semanticNonce}
	recomputedSnapshot, err := digestPeerInboxSemanticSnapshot(snapshotRow, row.publication,
		priorWork, hasPriorWork, causalEvents, decisionSeed)
	if err != nil || recomputedSnapshot != snapshotDigest {
		return PeerInboxSemanticCommitResult{}, fmt.Errorf("%w: terminal snapshot digest: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	snapshot := peerInboxSemanticSnapshot{publication: row.publication, importedEvent: imported,
		currentWork: priorWork, hasCurrentWork: hasPriorWork, causalEvents: causalEvents,
		decisionSeed: decisionSeed, digest: snapshotDigest}
	localPeers := imported.Audience().Peers()
	if len(localPeers) != 1 {
		return PeerInboxSemanticCommitResult{}, fmt.Errorf("%w: terminal imported audience",
			ErrPeerInboxSemanticInvariant)
	}
	plan := spec.Plan
	if err := validatePeerInboxSemanticPlan(plan); err != nil ||
		!equalPeerInboxSemanticPlan(peerInboxSemanticPlanProjection(plan), decision.Plan) {
		return PeerInboxSemanticCommitResult{}, fmt.Errorf("%w: terminal plan projection differs: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	if err := validatePeerInboxSemanticWorkPredecessor(snapshot, plan); err != nil {
		return PeerInboxSemanticCommitResult{}, err
	}
	if err := validatePeerInboxSemanticResponses(snapshot, plan, spec.Scope, spec.Responses,
		decidedAt); err != nil {
		return PeerInboxSemanticCommitResult{}, err
	}
	if len(decision.Responses) != len(spec.Responses) {
		return PeerInboxSemanticCommitResult{}, fmt.Errorf("%w: terminal response evidence count",
			ErrPeerInboxSemanticInvariant)
	}
	responseEvents := make([]model.Event, len(spec.Responses))
	responseIDs := make([]model.EventID, len(spec.Responses))
	for index, supplied := range spec.Responses {
		stored, publication, readErr := readPeerInboxSemanticStoredPublication(ctx, tx,
			supplied.Event().ID())
		if readErr != nil || stored.Source() != model.EventSourceLocal ||
			!bytes.Equal(publication.WireJSON().Bytes(), supplied.WireJSON().Bytes()) ||
			!peerInboxSemanticEventDecisionMatches(decision.Responses[index], stored,
				publication.Digest()) {
			return PeerInboxSemanticCommitResult{}, fmt.Errorf("%w: response Event evidence %d: %v",
				ErrPeerInboxSemanticInvariant, index, readErr)
		}
		if err := validatePeerInboxSemanticResponseDissemination(ctx, tx, stored); err != nil {
			return PeerInboxSemanticCommitResult{}, err
		}
		if err := validatePeerInboxSemanticResponseArtifacts(ctx, tx, stored); err != nil {
			return PeerInboxSemanticCommitResult{}, err
		}
		responseEvents[index], responseIDs[index] = stored, stored.ID()
	}
	wantReceipt := ""
	if len(responseIDs) != 0 {
		wantReceipt = responseIDs[len(responseIDs)-1].String()
	}
	if decision.ReceiptEventID != wantReceipt || row.hasReceipt != (wantReceipt != "") ||
		(row.hasReceipt && row.receiptEvent.String() != wantReceipt) {
		return PeerInboxSemanticCommitResult{}, fmt.Errorf("%w: terminal semantic receipt differs",
			ErrPeerInboxSemanticInvariant)
	}
	if err := validatePeerInboxSemanticImportedArtifacts(ctx, tx, imported); err != nil {
		return PeerInboxSemanticCommitResult{}, err
	}
	if err := validatePeerInboxSemanticWorkEffect(ctx, tx, plan, imported, responseEvents); err != nil {
		return PeerInboxSemanticCommitResult{}, err
	}
	if handling, ok := plan.Handling(); ok {
		if err := validatePeerInboxSemanticHandling(ctx, tx, handling, imported,
			responseEvents); err != nil {
			return PeerInboxSemanticCommitResult{}, err
		}
	}
	if settlement, ok := plan.Settlement(); ok {
		if err := validatePeerInboxSemanticHandlingSettlement(ctx, tx, settlement); err != nil {
			return PeerInboxSemanticCommitResult{}, err
		}
	}
	var transitionReceipts int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM peer_inbox_semantic_transition_receipts
		WHERE inbox_id=?`, row.inboxID.String()).Scan(&transitionReceipts); err != nil ||
		transitionReceipts != 0 {
		return PeerInboxSemanticCommitResult{}, fmt.Errorf("%w: terminal Inbox retained transition receipt",
			ErrPeerInboxSemanticInvariant)
	}
	result := PeerInboxSemanticCommitResult{status: row.status, diagnostic: row.diagnostic,
		importedEvent: imported.ID(), responseEvents: responseIDs, decision: decisionJSON}
	if row.hasReceipt {
		result.receiptEvent, result.hasReceipt = row.receiptEvent, true
	}
	return result, nil
}

func readPeerInboxSemanticStoredPublication(ctx context.Context, q rowQuerier,
	eventID model.EventID,
) (model.Event, model.SignedPublication, error) {
	event, err := readCurrentSourceEvent(ctx, q, eventID)
	if err != nil {
		return model.Event{}, model.SignedPublication{}, err
	}
	var bodyRaw, digestRaw, signature []byte
	if err := q.QueryRowContext(ctx, `SELECT canonical_publication_json,publication_digest,
		origin_signature FROM events WHERE event_id=?`, eventID.String()).
		Scan(&bodyRaw, &digestRaw, &signature); err != nil {
		return model.Event{}, model.SignedPublication{}, err
	}
	bodyJSON, bodyErr := model.NewJSON(bodyRaw)
	digest, digestErr := model.DigestFromBytes(digestRaw)
	body, rebuildErr := model.NewPublicationBody(event)
	if bodyErr != nil || digestErr != nil || rebuildErr != nil ||
		!bytes.Equal(bodyJSON.Bytes(), bodyRaw) || body.Digest() != digest ||
		!bytes.Equal(body.CanonicalJSON().Bytes(), bodyRaw) {
		return model.Event{}, model.SignedPublication{}, fmt.Errorf("%w: durable publication body differs",
			ErrPeerInboxSemanticInvariant)
	}
	publication, err := model.AttachSignature(body, signature)
	if err != nil {
		return model.Event{}, model.SignedPublication{}, err
	}
	return event, publication, nil
}

func peerInboxSemanticEventDecisionMatches(evidence peerInboxSemanticEventDecision,
	event model.Event, publicationDigest model.Digest,
) bool {
	key := event.Key()
	return evidence.EventDigest == event.Digest().String() && evidence.EventID == event.ID().String() &&
		evidence.OriginEpoch == key.OriginEpoch().String() &&
		evidence.OriginPeerID == key.OriginPeerID().String() &&
		evidence.PublicationDigest == publicationDigest.String() && evidence.Source == string(event.Source())
}

func validatePeerInboxSemanticImportedArtifacts(ctx context.Context, tx *sql.Tx,
	event model.Event,
) error {
	roots := make([]model.Digest, 0, len(event.Artifacts()))
	produced := make(map[model.Digest]struct{}, len(event.Artifacts()))
	for _, ref := range event.Artifacts() {
		if _, err := requireVerifiedArtifactRoot(ctx, tx, ref.RootDigest()); err != nil {
			return fmt.Errorf("%w: terminal imported Artifact root: %v",
				ErrPeerInboxSemanticInvariant, err)
		}
		roots = append(roots, ref.RootDigest())
		if ref.Role() == model.ArtifactReferenced {
			if err := requireReusableArtifactRoot(ctx, tx, ref.RootDigest()); err != nil {
				return fmt.Errorf("%w: terminal referenced Artifact: %v",
					ErrPeerInboxSemanticInvariant, err)
			}
			continue
		}
		if ref.Role() != model.ArtifactProduced {
			return ErrPeerInboxSemanticInvariant
		}
		produced[ref.RootDigest()] = struct{}{}
	}
	sort.Slice(roots, func(left, right int) bool {
		return roots[left].String() < roots[right].String()
	})
	if err := requireExactPeerInboxSemanticOwnerPins(ctx, tx, "event", event.ID().String(),
		roots, event.AcceptedAt()); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT root_digest,producer_origin_peer_id,
		local_agent_run_id,operation_id,relation,created_at FROM artifact_provenance
		WHERE producer_event_id=? ORDER BY root_digest`, event.ID().String())
	if err != nil {
		return fmt.Errorf("%w: read terminal replica provenance: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	defer rows.Close()
	actual := make(map[model.Digest]struct{}, len(produced))
	for rows.Next() {
		var rootText, origin, relation, created string
		var run, operation sql.NullString
		if err := rows.Scan(&rootText, &origin, &run, &operation, &relation, &created); err != nil {
			return fmt.Errorf("%w: scan terminal replica provenance: %v",
				ErrPeerInboxSemanticInvariant, err)
		}
		root, parseErr := model.ParseDigest(rootText)
		if parseErr != nil || origin != event.Scope().OriginPeerID().String() || run.Valid ||
			operation.Valid || relation != string(model.ProvenanceReplica) ||
			created != storeTime(event.AcceptedAt()) {
			return fmt.Errorf("%w: terminal replica provenance differs",
				ErrPeerInboxSemanticInvariant)
		}
		if _, ok := produced[root]; !ok {
			return fmt.Errorf("%w: terminal imported Event has extra producer provenance",
				ErrPeerInboxSemanticInvariant)
		}
		actual[root] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%w: iterate terminal replica provenance: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	if len(actual) != len(produced) {
		return fmt.Errorf("%w: terminal replica provenance closure differs",
			ErrPeerInboxSemanticInvariant)
	}
	return nil
}

func validatePeerInboxSemanticResponseDissemination(ctx context.Context, tx *sql.Tx,
	event model.Event,
) error {
	scope := event.Scope()
	var channel, origin, epoch, status string
	var channelSequence uint64
	if err := tx.QueryRowContext(ctx, `SELECT channel_id,origin_peer_id,origin_epoch,channel_seq,status
		FROM gossip_publications WHERE event_id=?`, event.ID().String()).
		Scan(&channel, &origin, &epoch, &channelSequence, &status); err != nil ||
		channel != scope.ChannelID().String() || origin != scope.OriginPeerID().String() ||
		epoch != scope.OriginEpoch().String() || channelSequence != scope.ChannelSequence() ||
		(status != "queued" && status != "leased" && status != "published" &&
			status != "blocked" && status != "abandoned") {
		return fmt.Errorf("%w: response Gossip evidence differs", ErrPeerInboxSemanticInvariant)
	}
	targets := event.Audience().Peers()
	if len(targets) != 1 {
		return ErrPeerInboxSemanticInvariant
	}
	deliveryID := deterministicDeliveryID(event.ID(), targets[0])
	rows, err := tx.QueryContext(ctx, `SELECT delivery_id,channel_id,target_peer_id,status
		FROM peer_deliveries WHERE event_id=? ORDER BY delivery_id`, event.ID().String())
	if err != nil {
		return fmt.Errorf("%w: response delivery evidence differs", ErrPeerInboxSemanticInvariant)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var actualID, deliveryChannel, deliveryTarget, deliveryStatus string
		if err := rows.Scan(&actualID, &deliveryChannel, &deliveryTarget, &deliveryStatus); err != nil ||
			actualID != deliveryID || deliveryChannel != scope.ChannelID().String() ||
			deliveryTarget != targets[0].String() ||
			(deliveryStatus != "pending" && deliveryStatus != "scanned" &&
				deliveryStatus != "blocked" && deliveryStatus != "abandoned") {
			return fmt.Errorf("%w: response delivery evidence differs",
				ErrPeerInboxSemanticInvariant)
		}
		count++
	}
	if err := rows.Err(); err != nil || count != 1 {
		return fmt.Errorf("%w: response delivery closure differs",
			ErrPeerInboxSemanticInvariant)
	}
	return nil
}

func validatePeerInboxSemanticResponseArtifacts(ctx context.Context, tx *sql.Tx,
	event model.Event,
) error {
	roots := make([]model.Digest, len(event.Artifacts()))
	for index, ref := range event.Artifacts() {
		if ref.Role() != model.ArtifactReferenced {
			return fmt.Errorf("%w: response Artifact role differs",
				ErrPeerInboxSemanticInvariant)
		}
		if _, err := requireVerifiedArtifactRoot(ctx, tx, ref.RootDigest()); err != nil {
			return fmt.Errorf("%w: response Artifact root: %v",
				ErrPeerInboxSemanticInvariant, err)
		}
		if err := requireReusableArtifactRoot(ctx, tx, ref.RootDigest()); err != nil {
			return fmt.Errorf("%w: response referenced Artifact: %v",
				ErrPeerInboxSemanticInvariant, err)
		}
		roots[index] = ref.RootDigest()
	}
	sort.Slice(roots, func(left, right int) bool {
		return roots[left].String() < roots[right].String()
	})
	if err := requireExactPeerInboxSemanticOwnerPins(ctx, tx, "event", event.ID().String(),
		roots, event.AcceptedAt()); err != nil {
		return err
	}
	if err := requireExactPeerInboxSemanticOwnerPins(ctx, tx, "publication",
		event.ID().String(), roots, event.AcceptedAt()); err != nil {
		return err
	}
	targets := event.Audience().Peers()
	if len(targets) != 1 {
		return ErrPeerInboxSemanticInvariant
	}
	if err := requireExactPeerInboxSemanticOwnerPins(ctx, tx, "delivery",
		deterministicDeliveryID(event.ID(), targets[0]), roots, event.AcceptedAt()); err != nil {
		return err
	}
	var provenance int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM artifact_provenance
		WHERE producer_event_id=?`, event.ID().String()).Scan(&provenance); err != nil || provenance != 0 {
		return fmt.Errorf("%w: response Event acquired producer provenance",
			ErrPeerInboxSemanticInvariant)
	}
	return nil
}

func requireExactPeerInboxSemanticOwnerPins(ctx context.Context, tx *sql.Tx,
	kind, owner string, roots []model.Digest, createdAt time.Time,
) error {
	rows, err := tx.QueryContext(ctx, `SELECT root_digest,expires_at,created_at FROM artifact_pins
		WHERE owner_kind=? AND owner_id=? ORDER BY root_digest`, kind, owner)
	if err != nil {
		return fmt.Errorf("%w: read response %s pins: %v",
			ErrPeerInboxSemanticInvariant, kind, err)
	}
	defer rows.Close()
	actual := make([]model.Digest, 0, len(roots))
	for rows.Next() {
		var rootText, createdText string
		var expires sql.NullString
		if err := rows.Scan(&rootText, &expires, &createdText); err != nil {
			return fmt.Errorf("%w: scan response %s pin: %v",
				ErrPeerInboxSemanticInvariant, kind, err)
		}
		root, rootErr := model.ParseDigest(rootText)
		storedAt, timeErr := parseCanonicalStoreTime(createdText)
		if rootErr != nil || timeErr != nil || expires.Valid || !storedAt.Equal(createdAt) {
			return fmt.Errorf("%w: malformed response %s pin",
				ErrPeerInboxSemanticInvariant, kind)
		}
		actual = append(actual, root)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%w: iterate response %s pins: %v",
			ErrPeerInboxSemanticInvariant, kind, err)
	}
	if len(actual) != len(roots) {
		return fmt.Errorf("%w: response %s pin closure differs",
			ErrPeerInboxSemanticInvariant, kind)
	}
	for index := range roots {
		if actual[index] != roots[index] {
			return fmt.Errorf("%w: response %s pin closure differs",
				ErrPeerInboxSemanticInvariant, kind)
		}
	}
	return nil
}

func validatePeerInboxSemanticWorkEffect(ctx context.Context, tx *sql.Tx,
	plan PeerInboxSemanticPlan, imported model.Event, responses []model.Event,
) error {
	intent, ok := plan.Work()
	if !ok {
		return nil
	}
	mutation, err := peerInboxSemanticWorkMutation(intent, imported, responses)
	if err != nil {
		return err
	}
	durable, found, err := readPeerInboxSemanticCurrentWork(ctx, tx, mutation.Work.Ref())
	if err != nil || !found || durable.Version() < mutation.Work.Version() ||
		durable.ChannelID() != mutation.Work.ChannelID() ||
		durable.Participants() != mutation.Work.Participants() ||
		durable.DeadlineUnixNano() != mutation.Work.DeadlineUnixNano() {
		return fmt.Errorf("%w: durable Work lost semantic effect", ErrPeerInboxSemanticInvariant)
	}
	update, updateErr := readCurrentSourceEvent(ctx, tx, durable.UpdatedBy())
	if updateErr != nil || !durable.UpdatedAt().Equal(update.AcceptedAt()) {
		return fmt.Errorf("%w: durable Work update evidence differs",
			ErrPeerInboxSemanticInvariant)
	}
	if durable.Version() == mutation.Work.Version() && !samePeerInboxSemanticWork(durable, mutation.Work) {
		return fmt.Errorf("%w: durable Work semantic effect differs", ErrPeerInboxSemanticInvariant)
	}
	if durable.Version() > mutation.Work.Version() {
		if err := requirePeerInboxSemanticWorkDescent(ctx, tx, durable, mutation.Work); err != nil {
			return err
		}
	}
	return nil
}

type peerInboxSemanticWorkCausalFrame struct {
	event model.Event
	next  int
}

func requirePeerInboxSemanticWorkDescent(ctx context.Context, tx *sql.Tx,
	durable, semantic model.ReviewWork,
) error {
	target, err := readCurrentSourceEvent(ctx, tx, semantic.UpdatedBy())
	facts, factsErr := decodeClosedEventPayload(target)
	exact, exactErr := currentWorkIsExactSource(target, semantic, facts)
	if err != nil || factsErr != nil || exactErr != nil || !exact {
		return fmt.Errorf("%w: semantic Work update evidence differs: %v",
			ErrPeerInboxSemanticInvariant, errors.Join(err, factsErr, exactErr))
	}
	current, err := readCurrentSourceEvent(ctx, tx, durable.UpdatedBy())
	if err != nil || current.Scope().WorkRef() != semantic.Ref() {
		return fmt.Errorf("%w: durable Work causal head differs: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	if current.Key() == target.Key() {
		return nil
	}

	stack := []peerInboxSemanticWorkCausalFrame{{event: current}}
	active := map[model.EventKey]struct{}{current.Key(): {}}
	visited := map[model.EventKey]struct{}{current.Key(): {}}
	for len(stack) != 0 {
		frame := &stack[len(stack)-1]
		causes := frame.event.CausedBy()
		if frame.next == len(causes) {
			delete(active, frame.event.Key())
			stack = stack[:len(stack)-1]
			continue
		}
		causeKey := causes[frame.next]
		frame.next++
		if _, cyclic := active[causeKey]; cyclic {
			return fmt.Errorf("%w: durable Work causal descent contains a cycle",
				ErrPeerInboxSemanticInvariant)
		}
		if _, seen := visited[causeKey]; seen {
			continue
		}
		if len(visited) >= peerInboxSemanticWorkCausalityLimit {
			return fmt.Errorf("%w: durable Work causal descent exceeds %d Events",
				ErrPeerInboxSemanticInvariant, peerInboxSemanticWorkCausalityLimit)
		}
		cause, err := readCurrentSourceEvent(ctx, tx, causeKey.EventID())
		if err != nil || cause.Key() != causeKey {
			return fmt.Errorf("%w: durable Work causal EventKey differs: %v",
				ErrPeerInboxSemanticInvariant, err)
		}
		visited[causeKey] = struct{}{}
		if cause.Scope().WorkRef() != semantic.Ref() {
			continue
		}
		if causeKey == target.Key() {
			return nil
		}
		active[causeKey] = struct{}{}
		stack = append(stack, peerInboxSemanticWorkCausalFrame{event: cause})
	}
	return fmt.Errorf("%w: durable Work does not descend from semantic effect",
		ErrPeerInboxSemanticInvariant)
}

func samePeerInboxSemanticWork(left, right model.ReviewWork) bool {
	return left.Ref() == right.Ref() && left.ChannelID() == right.ChannelID() &&
		left.Participants() == right.Participants() && left.Version() == right.Version() &&
		left.Iteration() == right.Iteration() && left.DeadlineUnixNano() == right.DeadlineUnixNano() &&
		left.State() == right.State() && left.StateData().String() == right.StateData().String() &&
		left.UpdatedBy() == right.UpdatedBy() && left.UpdatedAt().Equal(right.UpdatedAt())
}

func validatePeerInboxSemanticHandling(ctx context.Context, tx *sql.Tx,
	intent PeerInboxSemanticHandlingIntent, imported model.Event, responses []model.Event,
) error {
	var source model.Event
	if intent.Source() == PeerInboxSemanticFromImportedEvent {
		source = imported
	} else if intent.Source() == PeerInboxSemanticFromLocalResponse &&
		int(intent.ResponseOrdinal()) < len(responses) {
		source = responses[intent.ResponseOrdinal()]
	} else {
		return ErrPeerInboxSemanticInvariant
	}
	id, err := peerInboxSemanticHandlingID(source.ID())
	if err != nil {
		return err
	}
	handling, err := readAgentHandling(ctx, tx, id)
	if err != nil || handling.ProfileID() != model.TeamworkProfileID() ||
		handling.EventID() != source.ID() || handling.Priority() != 0 ||
		!handling.CreatedAt().Equal(source.AcceptedAt()) {
		return fmt.Errorf("%w: terminal Handling creation differs", ErrPeerInboxSemanticInvariant)
	}
	return requirePeerInboxSemanticHandlingPins(ctx, tx, handling, source, false)
}
