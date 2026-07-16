package store

import (
	"bytes"
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrCurrentReadInput       = errors.New("invalid Agent current-read input")
	ErrCurrentReadStale       = errors.New("Agent current claim is stale")
	ErrCurrentReadInvariant   = errors.New("Agent current durable invariant violated")
	ErrCurrentReadUnsupported = errors.New("Agent current projection is not implemented for this handling")
	ErrCurrentReadTooLarge    = errors.New("Agent current projection exceeds its bounded contract")
)

// AgentCurrentReadSpec contains only authenticated/fenced transport facts and
// trusted server time. Event, Work, role, actions and Artifact refs are always
// derived inside FinalizeAgentCurrentRead from durable authority.
type AgentCurrentReadSpec struct {
	ProfileID             model.ProfileID
	ExpectedAssetRevision string
	RunID                 model.RunID
	ClaimTokenHash        model.Digest
	At                    time.Time
}

type AgentCurrentReadResult struct {
	Projection model.CurrentProjection
	Receipt    model.CurrentReadReceipt
	Replayed   bool
}

// FinalizeAgentCurrentRead creates the exact bounded projection and persists
// it as write-once AgentRun evidence in one transaction. Replay returns that
// stored projection rather than rendering a newer Work version.
func (s *Store) FinalizeAgentCurrentRead(ctx context.Context,
	spec AgentCurrentReadSpec,
) (AgentCurrentReadResult, error) {
	if s == nil || s.db == nil || ctx == nil || spec.ProfileID != model.TeamworkProfileID() ||
		spec.ExpectedAssetRevision == "" || spec.RunID.IsZero() || spec.ClaimTokenHash.IsZero() {
		return AgentCurrentReadResult{}, ErrCurrentReadInput
	}
	at, err := canonicalClaimTime(spec.At)
	if err != nil {
		return AgentCurrentReadResult{}, fmt.Errorf("%w: %v", ErrCurrentReadInput, err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AgentCurrentReadResult{}, fmt.Errorf("finalize Agent current read: begin: %w", err)
	}
	defer tx.Rollback()

	profile, budget, err := requireAgentClaimAuthority(ctx, tx, spec.ProfileID, spec.ExpectedAssetRevision)
	if err != nil {
		return AgentCurrentReadResult{}, err
	}
	run, err := readAgentRun(ctx, tx, spec.RunID)
	if errors.Is(err, sql.ErrNoRows) {
		return AgentCurrentReadResult{}, ErrCurrentReadStale
	}
	if err != nil {
		return AgentCurrentReadResult{}, fmt.Errorf("%w: read AgentRun: %v", ErrCurrentReadInvariant, err)
	}
	handlingID, hasHandling := run.HandlingID()
	if !hasHandling || run.ProfileID() != profile.ID() || run.Runtime() != profile.Runtime() {
		return AgentCurrentReadResult{}, fmt.Errorf("%w: Run is not bound to the authenticated current Profile", ErrCurrentReadStale)
	}
	handling, err := readAgentHandling(ctx, tx, handlingID)
	if errors.Is(err, sql.ErrNoRows) {
		return AgentCurrentReadResult{}, ErrCurrentReadStale
	}
	if err != nil {
		return AgentCurrentReadResult{}, fmt.Errorf("%w: read Handling: %v", ErrCurrentReadInvariant, err)
	}
	if err := requireExactCurrentClaim(run, handling, spec.ClaimTokenHash, at); err != nil {
		return AgentCurrentReadResult{}, err
	}

	if stored, ok := run.CurrentReadReceipt(); ok {
		receipt, err := model.ParseCurrentReadReceipt(stored.Bytes())
		if err != nil {
			return AgentCurrentReadResult{}, fmt.Errorf("%w: parse stored receipt: %v", ErrCurrentReadInvariant, err)
		}
		if err := requireCurrentReceiptBinding(receipt, run, handling); err != nil {
			return AgentCurrentReadResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return AgentCurrentReadResult{}, fmt.Errorf("finalize Agent current read: commit replay: %w", err)
		}
		return AgentCurrentReadResult{Projection: receipt.Projection(), Receipt: receipt, Replayed: true}, nil
	}

	sourceEvent, err := readCurrentSourceEvent(ctx, tx, handling.EventID())
	if err != nil {
		return AgentCurrentReadResult{}, fmt.Errorf("%w: source Event: %v", ErrCurrentReadInvariant, err)
	}
	parentResume, err := handlingIsParentResume(ctx, tx, handling, sourceEvent)
	if err != nil {
		return AgentCurrentReadResult{}, fmt.Errorf("%w: inspect derivation: %v", ErrCurrentReadInvariant, err)
	}
	if parentResume {
		return AgentCurrentReadResult{}, fmt.Errorf("%w: parent-resume requires bounded child_results", ErrCurrentReadUnsupported)
	}
	work, err := readReviewWork(ctx, tx, sourceEvent.Scope().WorkRef())
	if err != nil {
		return AgentCurrentReadResult{}, fmt.Errorf("%w: action Work: %v", ErrCurrentReadInvariant, err)
	}
	if work.ChannelID() != sourceEvent.Scope().ChannelID() {
		return AgentCurrentReadResult{}, fmt.Errorf("%w: source Event and action Work Channels differ", ErrCurrentReadInvariant)
	}
	if at.Before(sourceEvent.AcceptedAt()) || at.Before(work.UpdatedAt()) {
		return AgentCurrentReadResult{}, fmt.Errorf("%w: trusted time precedes projected Event or Work evidence", ErrCurrentReadInvariant)
	}
	node, err := readNode(ctx, tx)
	if err != nil {
		return AgentCurrentReadResult{}, fmt.Errorf("%w: local Node: %v", ErrCurrentReadInvariant, err)
	}
	role, err := localCurrentRole(node.PeerID(), work)
	if err != nil {
		return AgentCurrentReadResult{}, err
	}
	if sourceEvent.Source() == model.EventSourceImported && !sourceEvent.Audience().Contains(node.PeerID()) {
		return AgentCurrentReadResult{}, fmt.Errorf("%w: imported source Event does not address the local Node", ErrCurrentReadInvariant)
	}
	if len(sourceEvent.Artifacts()) > budget.Spec().MaxCurrentArtifactRefs {
		return AgentCurrentReadResult{}, fmt.Errorf("%w: source Event has %d Artifact refs, Profile budget is %d",
			ErrCurrentReadTooLarge, len(sourceEvent.Artifacts()), budget.Spec().MaxCurrentArtifactRefs)
	}
	if err := requireCurrentArtifacts(ctx, tx, sourceEvent); err != nil {
		return AgentCurrentReadResult{}, err
	}
	facts, err := decodeClosedEventPayload(sourceEvent)
	if err != nil {
		return AgentCurrentReadResult{}, fmt.Errorf("%w: %v", ErrCurrentReadInvariant, err)
	}
	exactUpdate, err := currentWorkIsExactSource(sourceEvent, work, facts)
	if err != nil {
		return AgentCurrentReadResult{}, err
	}
	actions := deriveCurrentActions(role, sourceEvent, work, exactUpdate)
	currentArtifacts := make([]model.CurrentArtifactRef, len(sourceEvent.Artifacts()))
	for index, ref := range sourceEvent.Artifacts() {
		currentArtifacts[index], err = model.NewCurrentArtifactRef(ref.RootDigest())
		if err != nil {
			return AgentCurrentReadResult{}, fmt.Errorf("%w: Artifact projection: %v", ErrCurrentReadInvariant, err)
		}
	}
	currentEvent, err := model.NewCurrentEvent(model.CurrentEventSpec{
		Key: sourceEvent.Key(), Digest: sourceEvent.Digest(), Type: sourceEvent.Type(),
		WorkRef: sourceEvent.Scope().WorkRef(), Summary: sourceEvent.Summary(), Payload: sourceEvent.Payload(),
		ArtifactRefs: currentArtifacts, AcceptedAt: sourceEvent.AcceptedAt(),
	})
	if err != nil {
		return AgentCurrentReadResult{}, fmt.Errorf("%w: Event projection: %v", ErrCurrentReadInvariant, err)
	}
	currentWork, err := model.NewCurrentWork(model.CurrentWorkSpec{
		Ref: work.Ref(), Version: work.Version(), Iteration: work.Iteration(),
		DeadlineUnixNano: work.DeadlineUnixNano(), State: work.State(), StateData: work.StateData(), LocalRole: role,
	})
	if err != nil {
		return AgentCurrentReadResult{}, fmt.Errorf("%w: Work projection: %v", ErrCurrentReadInvariant, err)
	}
	projection, err := model.NewCurrentProjection(model.CurrentProjectionSpec{
		SourceEvent: currentEvent, ActionWork: currentWork, AllowedActions: actions,
	})
	if errors.Is(err, model.ErrLimit) {
		return AgentCurrentReadResult{}, fmt.Errorf("%w: %v", ErrCurrentReadTooLarge, err)
	}
	if err != nil {
		return AgentCurrentReadResult{}, fmt.Errorf("%w: projection: %v", ErrCurrentReadInvariant, err)
	}
	if len(projection.CanonicalJSON().Bytes()) > budget.Spec().MaxCurrentJSONBytes {
		return AgentCurrentReadResult{}, fmt.Errorf("%w: projection has %d bytes, Profile budget is %d",
			ErrCurrentReadTooLarge, len(projection.CanonicalJSON().Bytes()), budget.Spec().MaxCurrentJSONBytes)
	}
	receipt, err := model.NewCurrentReadReceipt(model.CurrentReadReceiptSpec{
		RunID: run.ID(), ProfileID: run.ProfileID(), HandlingID: handling.ID(),
		HandlingAttempt: run.HandlingAttempt(), Projection: projection, ReadAt: at,
	})
	if err != nil {
		return AgentCurrentReadResult{}, fmt.Errorf("%w: receipt: %v", ErrCurrentReadInvariant, err)
	}
	if err := writeCurrentReadEvidence(ctx, tx, run, handling, spec.ClaimTokenHash, receipt); err != nil {
		return AgentCurrentReadResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return AgentCurrentReadResult{}, fmt.Errorf("finalize Agent current read: commit: %w", err)
	}
	return AgentCurrentReadResult{Projection: projection, Receipt: receipt}, nil
}

func currentWorkIsExactSource(event model.Event, work model.ReviewWork,
	facts closedPayloadFacts,
) (bool, error) {
	if work.UpdatedBy() != event.ID() {
		return false, nil
	}
	if err := work.ValidateUpdateEvent(event); err != nil {
		return false, fmt.Errorf("%w: current Work update: %v", ErrCurrentReadInvariant, err)
	}
	versionMatches := false
	iterationMatches := false
	if event.Type() == model.EventReviewOffered {
		versionMatches = facts.WorkVersion == work.Version()
		iterationMatches = facts.Iteration == work.Iteration()
	} else {
		versionMatches = facts.WorkVersion < model.MaxSQLiteInteger && facts.WorkVersion+1 == work.Version()
		if event.Type() == model.EventReviewReworkRequested {
			iterationMatches = facts.Iteration == 1 && work.Iteration() == 2
		} else {
			iterationMatches = facts.Iteration == work.Iteration()
		}
	}
	if !versionMatches || !iterationMatches {
		return false, fmt.Errorf("%w: source Event payload does not bind the resulting Work version/iteration",
			ErrCurrentReadInvariant)
	}
	if facts.DeadlineUnixNano != 0 && facts.DeadlineUnixNano != work.DeadlineUnixNano() {
		return false, fmt.Errorf("%w: source Event and Work deadlines differ", ErrCurrentReadInvariant)
	}
	return true, nil
}

func requireExactCurrentClaim(run model.AgentRun, handling model.Handling, token model.Digest,
	at time.Time,
) error {
	handlingID, hasHandling := run.HandlingID()
	runFence, hasRunFence := run.ClaimFenceHash()
	runLease, hasRunLease := run.LeaseUntil()
	handlingFence, hasHandlingFence := handling.ClaimTokenHash()
	handlingLease, hasHandlingLease := handling.LeaseUntil()
	if !hasHandling || !hasRunFence || !hasRunLease || !hasHandlingFence || !hasHandlingLease ||
		(run.Status() != model.AgentRunStarting && run.Status() != model.AgentRunRunning) ||
		handling.Status() != model.HandlingClaimed ||
		run.ProfileID() != handling.ProfileID() || handlingID != handling.ID() ||
		run.HandlingAttempt() != handling.Attempts() || !sameCurrentDigest(runFence, token) ||
		!sameCurrentDigest(handlingFence, token) ||
		!runLease.Equal(handlingLease) || !runLease.After(at) {
		return ErrCurrentReadStale
	}
	if at.Before(run.StartedAt()) || at.Before(handling.UpdatedAt()) {
		return fmt.Errorf("%w: trusted time precedes claim evidence", ErrCurrentReadInvariant)
	}
	return nil
}

func sameCurrentDigest(left, right model.Digest) bool {
	return subtle.ConstantTimeCompare(left.Bytes(), right.Bytes()) == 1
}

func requireCurrentReceiptBinding(receipt model.CurrentReadReceipt, run model.AgentRun,
	handling model.Handling,
) error {
	handlingID, _ := run.HandlingID()
	lease, _ := run.LeaseUntil()
	if receipt.RunID() != run.ID() || receipt.ProfileID() != run.ProfileID() ||
		receipt.HandlingID() != handlingID || receipt.HandlingID() != handling.ID() ||
		receipt.HandlingAttempt() != run.HandlingAttempt() || receipt.HandlingAttempt() != handling.Attempts() ||
		receipt.SourceEvent().EventID() != handling.EventID() || receipt.ReadAt().Before(run.StartedAt()) ||
		!receipt.ReadAt().Before(lease) || handling.LastDisposition() != "read" ||
		!handling.UpdatedAt().Equal(receipt.ReadAt()) {
		return fmt.Errorf("%w: stored receipt differs from Run/Handling snapshot", ErrCurrentReadInvariant)
	}
	return nil
}

func localCurrentRole(local model.PeerID, work model.ReviewWork) (model.CurrentRole, error) {
	switch local {
	case work.Participants().InitiatorPeerID():
		return model.CurrentInitiator, nil
	case work.Participants().ReviewerPeerID():
		return model.CurrentReviewer, nil
	default:
		return "", fmt.Errorf("%w: local Node is not an action Work participant", ErrCurrentReadInvariant)
	}
}

func deriveCurrentActions(role model.CurrentRole, event model.Event, work model.ReviewWork,
	exactUpdate bool,
) []model.OperationKind {
	var domain []model.OperationKind
	if exactUpdate {
		switch role {
		case model.CurrentReviewer:
			switch {
			case work.State() == model.WorkOffered && event.Type() == model.EventReviewOffered:
				domain = append(domain, model.OperationTeamworkAccept, model.OperationTeamworkDecline)
			case work.State() == model.WorkActive && event.Type() == model.EventReviewAccepted:
				domain = append(domain, model.OperationTeamworkOffer, model.OperationTeamworkDeliver)
			case work.State() == model.WorkRework && event.Type() == model.EventReviewReworkRequested:
				domain = append(domain, model.OperationTeamworkOffer, model.OperationTeamworkDeliver)
			}
		case model.CurrentInitiator:
			if work.State() == model.WorkDelivered && event.Type() == model.EventReviewDelivered {
				if work.Iteration() == 1 {
					domain = append(domain, model.OperationTeamworkRework)
				}
				domain = append(domain, model.OperationTeamworkClose)
			}
			if !work.State().Terminal() {
				domain = append(domain, model.OperationTeamworkCancel)
			}
		}
	}
	if len(domain) == 0 {
		return []model.OperationKind{model.OperationResolveNoAction, model.OperationResolveRetry, model.OperationResolveReject}
	}
	return append(domain, model.OperationResolveRetry)
}

func requireCurrentArtifacts(ctx context.Context, q rowQuerier, event model.Event) error {
	for _, ref := range event.Artifacts() {
		if _, err := requireVerifiedArtifactRoot(ctx, q, ref.RootDigest()); err != nil {
			return fmt.Errorf("%w: source Event Artifact %s is not verified: %v",
				ErrCurrentReadInvariant, ref.RootDigest().String(), err)
		}
		var pinned int
		if err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM artifact_pins
			WHERE root_digest=? AND owner_kind='event' AND owner_id=?)`,
			ref.RootDigest().String(), event.ID().String()).Scan(&pinned); err != nil {
			return fmt.Errorf("%w: inspect source Event Artifact pin: %v", ErrCurrentReadInvariant, err)
		}
		if pinned != 1 {
			return fmt.Errorf("%w: source Event Artifact %s is not pinned by the Event",
				ErrCurrentReadInvariant, ref.RootDigest().String())
		}
	}
	return nil
}

func handlingIsParentResume(ctx context.Context, q rowQuerier, handling model.Handling,
	event model.Event,
) (bool, error) {
	var operationText string
	err := q.QueryRowContext(ctx, `SELECT operation_id FROM work_derivations
		WHERE child_home_peer_id=? AND child_work_id=?`,
		event.Scope().WorkRef().HomePeerID().String(), event.Scope().WorkRef().WorkID().String()).
		Scan(&operationText)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	operation, err := model.ParseOperationID(operationText)
	if err != nil {
		return false, err
	}
	resumeID, err := deterministicDerivationHandlingID(operation)
	if err != nil {
		return false, err
	}
	return handling.ID() == resumeID, nil
}

func writeCurrentReadEvidence(ctx context.Context, tx *sql.Tx, run model.AgentRun,
	handling model.Handling, token model.Digest, receipt model.CurrentReadReceipt,
) error {
	fence, _ := run.ClaimFenceHash()
	lease, _ := run.LeaseUntil()
	result, err := tx.ExecContext(ctx, `UPDATE agent_handlings SET last_disposition='read',updated_at=?
		WHERE handling_id=? AND profile_id=? AND event_id=? AND status='claimed'
		AND claim_token_hash=? AND lease_until=? AND attempts=? AND updated_at<=?`,
		storeTime(receipt.ReadAt()), handling.ID().String(), handling.ProfileID().String(),
		handling.EventID().String(), token.Bytes(), storeTime(lease), handling.Attempts(), storeTime(receipt.ReadAt()))
	if err != nil {
		return fmt.Errorf("finalize Agent current read: mark Handling read: %w", err)
	}
	if err := requireExactlyOneRow(result, "finalize Agent current read: Handling fence"); err != nil {
		return fmt.Errorf("%w: %v", ErrCurrentReadStale, err)
	}
	result, err = tx.ExecContext(ctx, `UPDATE agent_runs SET current_read_receipt_json=?
		WHERE run_id=? AND profile_id=? AND handling_id=? AND handling_attempt=?
		AND claim_fence_hash=? AND lease_until=? AND status IN ('starting','running')
		AND current_read_receipt_json IS NULL`, receipt.CanonicalJSON().Bytes(), run.ID().String(),
		run.ProfileID().String(), handling.ID().String(), run.HandlingAttempt(), fence.Bytes(), storeTime(lease))
	if err != nil {
		return fmt.Errorf("finalize Agent current read: persist receipt: %w", err)
	}
	if err := requireExactlyOneRow(result, "finalize Agent current read: AgentRun fence"); err != nil {
		return fmt.Errorf("%w: %v", ErrCurrentReadStale, err)
	}
	return nil
}

type storedCurrentHeadWire struct {
	Digest   string `json:"digest"`
	Revision uint64 `json:"revision"`
}

type storedCurrentRefWire struct {
	HomePeerID string `json:"home_peer_id"`
	WorkID     string `json:"work_id"`
}

type storedCurrentEventKeyWire struct {
	EventID      string `json:"event_id"`
	OriginEpoch  string `json:"origin_epoch"`
	OriginPeerID string `json:"origin_peer_id"`
}

type storedCurrentArtifactWire struct {
	Role       model.ArtifactRole `json:"role"`
	RootDigest string             `json:"root_digest"`
}

type storedCurrentEventWire struct {
	AcceptedAt    string                      `json:"accepted_at"`
	Actor         string                      `json:"actor_principal"`
	Artifacts     []storedCurrentArtifactWire `json:"artifact_roots"`
	Audience      []string                    `json:"audience"`
	CausedBy      []storedCurrentEventKeyWire `json:"caused_by"`
	ChannelID     string                      `json:"channel_id"`
	ChannelSeq    uint64                      `json:"channel_seq"`
	CreatedAt     string                      `json:"created_at"`
	EventID       string                      `json:"event_id"`
	OriginEpoch   string                      `json:"origin_epoch"`
	OriginMember  storedCurrentHeadWire       `json:"origin_member"`
	OriginPeerID  string                      `json:"origin_peer_id"`
	OriginSeq     uint64                      `json:"origin_seq"`
	Payload       json.RawMessage             `json:"payload"`
	Resource      storedCurrentRefWire        `json:"resource"`
	RosterHead    storedCurrentHeadWire       `json:"publication_roster"`
	SchemaVersion int                         `json:"schema_version"`
	Summary       string                      `json:"summary"`
	Type          model.EventType             `json:"event_type"`
}

func readCurrentSourceEvent(ctx context.Context, q rowQuerier, id model.EventID) (model.Event, error) {
	var sourceText string
	var raw, storedDigestBytes []byte
	if err := q.QueryRowContext(ctx, `SELECT source,canonical_event_json,event_digest
		FROM events WHERE event_id=?`, id.String()).Scan(&sourceText, &raw, &storedDigestBytes); err != nil {
		return model.Event{}, err
	}
	canonical, err := model.NewJSON(raw)
	if err != nil || !bytes.Equal(canonical.Bytes(), raw) {
		return model.Event{}, errors.New("source Event bytes are not exact canonical JSON")
	}
	var wire storedCurrentEventWire
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return model.Event{}, fmt.Errorf("decode source Event: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return model.Event{}, errors.New("source Event has a trailing JSON value")
	}
	if wire.SchemaVersion != model.SchemaVersion {
		return model.Event{}, errors.New("source Event has unsupported schema version")
	}
	eventID, err := model.ParseEventID(wire.EventID)
	if err != nil || eventID != id {
		return model.Event{}, errors.New("source Event identity differs from Handling")
	}
	channel, err := model.ParseChannelID(wire.ChannelID)
	if err != nil {
		return model.Event{}, err
	}
	origin, err := model.ParsePeerID(wire.OriginPeerID)
	if err != nil {
		return model.Event{}, err
	}
	epoch, err := model.ParseOriginEpoch(wire.OriginEpoch)
	if err != nil {
		return model.Event{}, err
	}
	originHead, err := storedCurrentHead(wire.OriginMember)
	if err != nil {
		return model.Event{}, err
	}
	rosterHead, err := storedCurrentHead(wire.RosterHead)
	if err != nil {
		return model.Event{}, err
	}
	workRef, err := storedCurrentWorkRef(wire.Resource)
	if err != nil {
		return model.Event{}, err
	}
	scope, err := model.NewEventScope(channel, origin, epoch, wire.OriginSeq, wire.ChannelSeq,
		originHead, rosterHead, workRef)
	if err != nil {
		return model.Event{}, err
	}
	audiencePeers := make([]model.PeerID, len(wire.Audience))
	for index, value := range wire.Audience {
		audiencePeers[index], err = model.ParsePeerID(value)
		if err != nil {
			return model.Event{}, err
		}
	}
	audience, err := model.NewAudience(audiencePeers)
	if err != nil {
		return model.Event{}, err
	}
	payload, err := model.NewJSON(wire.Payload)
	if err != nil {
		return model.Event{}, err
	}
	artifacts := make([]model.ArtifactRef, len(wire.Artifacts))
	for index, value := range wire.Artifacts {
		root, err := model.ParseDigest(value.RootDigest)
		if err != nil {
			return model.Event{}, err
		}
		artifacts[index], err = model.NewArtifactRef(root, value.Role)
		if err != nil {
			return model.Event{}, err
		}
	}
	causedBy := make([]model.EventKey, len(wire.CausedBy))
	for index, value := range wire.CausedBy {
		causedBy[index], err = storedCurrentEventKey(value)
		if err != nil {
			return model.Event{}, err
		}
	}
	createdAt, err := time.Parse(time.RFC3339Nano, wire.CreatedAt)
	if err != nil {
		return model.Event{}, err
	}
	acceptedAt, err := time.Parse(time.RFC3339Nano, wire.AcceptedAt)
	if err != nil {
		return model.Event{}, err
	}
	event, err := model.NewEvent(model.EventSpec{ID: eventID, Scope: scope,
		Source: model.EventSource(sourceText), ActorPrincipal: wire.Actor, Type: wire.Type,
		Audience: audience, Summary: wire.Summary, Payload: payload, Artifacts: artifacts,
		CausedBy: causedBy, CreatedAt: createdAt, AcceptedAt: acceptedAt})
	if err != nil {
		return model.Event{}, err
	}
	storedDigest, err := model.DigestFromBytes(storedDigestBytes)
	if err != nil || event.Digest() != storedDigest || !bytes.Equal(event.CanonicalJSON().Bytes(), raw) {
		return model.Event{}, errors.New("source Event canonical evidence is inconsistent")
	}
	return event, nil
}

func storedCurrentHead(wire storedCurrentHeadWire) (model.RecordHead, error) {
	digest, err := model.ParseDigest(wire.Digest)
	if err != nil {
		return model.RecordHead{}, err
	}
	return model.NewRecordHead(wire.Revision, digest)
}

func storedCurrentWorkRef(wire storedCurrentRefWire) (model.WorkRef, error) {
	home, err := model.ParsePeerID(wire.HomePeerID)
	if err != nil {
		return model.WorkRef{}, err
	}
	workID, err := model.ParseWorkID(wire.WorkID)
	if err != nil {
		return model.WorkRef{}, err
	}
	return model.NewWorkRef(home, workID)
}

func storedCurrentEventKey(wire storedCurrentEventKeyWire) (model.EventKey, error) {
	peer, err := model.ParsePeerID(wire.OriginPeerID)
	if err != nil {
		return model.EventKey{}, err
	}
	epoch, err := model.ParseOriginEpoch(wire.OriginEpoch)
	if err != nil {
		return model.EventKey{}, err
	}
	eventID, err := model.ParseEventID(wire.EventID)
	if err != nil {
		return model.EventKey{}, err
	}
	return model.NewEventKey(peer, epoch, eventID)
}
