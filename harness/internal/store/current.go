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
	"strconv"
	"strings"
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
	// ArtifactViews is populated only by the trusted server after it has
	// materialized every item returned by PlanAgentCurrentRead. It is never
	// accepted from the Agent or the local transport request.
	ArtifactViews []model.CurrentArtifactRef
}

type AgentCurrentReadResult struct {
	Projection model.CurrentProjection
	Receipt    model.CurrentReadReceipt
	Replayed   bool
}

// AgentCurrentArtifactMaterialization is the immutable CAS authority for one
// Run-local readonly view. Ordinals are dense and assigned from the canonical
// semantic root union, never from caller order.
type AgentCurrentArtifactMaterialization struct {
	Ordinal        uint32
	RootDigest     model.Digest
	ManifestDigest model.Digest
}

// AgentCurrentReadPlan is a read-only checkpoint between Store authority and
// filesystem materialization. Replay is evidence only: callers must still
// re-materialize/reverify every Artifact and call FinalizeAgentCurrentRead.
type AgentCurrentReadPlan struct {
	RunID     model.RunID
	Artifacts []AgentCurrentArtifactMaterialization
	Replay    *AgentCurrentReadResult
}

type currentReadAuthority struct {
	budget     model.HandlingBudget
	run        model.AgentRun
	handling   model.Handling
	projection model.CurrentProjection
	receipt    model.CurrentReadReceipt
	hasReceipt bool
	artifacts  []AgentCurrentArtifactMaterialization
}

// PlanAgentCurrentRead derives the exact semantic Artifact union and returns
// only verified manifest identities. It never writes current-read evidence.
// The final transaction repeats every authority check after filesystem work.
func (s *Store) PlanAgentCurrentRead(ctx context.Context,
	spec AgentCurrentReadSpec,
) (AgentCurrentReadPlan, error) {
	if len(spec.ArtifactViews) != 0 {
		return AgentCurrentReadPlan{}, fmt.Errorf("%w: materialized views are not plan input", ErrCurrentReadInput)
	}
	at, err := validateAgentCurrentReadInput(s, ctx, spec)
	if err != nil {
		return AgentCurrentReadPlan{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AgentCurrentReadPlan{}, fmt.Errorf("plan Agent current read: begin: %w", err)
	}
	defer tx.Rollback()
	authority, err := deriveCurrentReadAuthority(ctx, tx, spec, at)
	if err != nil {
		return AgentCurrentReadPlan{}, err
	}
	if err := tx.Commit(); err != nil {
		return AgentCurrentReadPlan{}, fmt.Errorf("plan Agent current read: commit: %w", err)
	}
	plan := AgentCurrentReadPlan{RunID: authority.run.ID(),
		Artifacts: append([]AgentCurrentArtifactMaterialization{}, authority.artifacts...)}
	if authority.hasReceipt {
		replay := AgentCurrentReadResult{Projection: authority.receipt.Projection(),
			Receipt: authority.receipt, Replayed: true}
		plan.Replay = &replay
	}
	return plan, nil
}

// FinalizeAgentCurrentRead repeats the complete Store derivation after the
// server materializer returns, validates its exact Run/ordinal/root paths and
// only then persists write-once current-read evidence. Receipt replay also
// requires the exact already-stored mapping to be re-materialized.
func (s *Store) FinalizeAgentCurrentRead(ctx context.Context,
	spec AgentCurrentReadSpec,
) (AgentCurrentReadResult, error) {
	at, err := validateAgentCurrentReadInput(s, ctx, spec)
	if err != nil {
		return AgentCurrentReadResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AgentCurrentReadResult{}, fmt.Errorf("finalize Agent current read: begin: %w", err)
	}
	defer tx.Rollback()
	authority, err := deriveCurrentReadAuthority(ctx, tx, spec, at)
	if err != nil {
		return AgentCurrentReadResult{}, err
	}
	views, err := requireExactCurrentArtifactViews(authority.run.ID(), authority.artifacts,
		spec.ArtifactViews, authority.budget.Spec().MaxCurrentPathBytes)
	if err != nil {
		return AgentCurrentReadResult{}, err
	}
	if authority.hasReceipt {
		if !sameCurrentArtifactViews(views, authority.receipt.ArtifactRefs()) {
			return AgentCurrentReadResult{}, fmt.Errorf("%w: replayed Artifact view mapping differs from stored receipt",
				ErrCurrentReadInvariant)
		}
		if err := tx.Commit(); err != nil {
			return AgentCurrentReadResult{}, fmt.Errorf("finalize Agent current read: commit replay: %w", err)
		}
		return AgentCurrentReadResult{Projection: authority.receipt.Projection(),
			Receipt: authority.receipt, Replayed: true}, nil
	}

	projection, err := model.NewCurrentProjection(model.CurrentProjectionSpec{
		SourceEvent: authority.projection.SourceEvent(), ActionWork: authority.projection.ActionWork(),
		AllowedActions: authority.projection.AllowedActions(), ArtifactViews: views,
	})
	if errors.Is(err, model.ErrLimit) {
		return AgentCurrentReadResult{}, fmt.Errorf("%w: %v", ErrCurrentReadTooLarge, err)
	}
	if err != nil {
		return AgentCurrentReadResult{}, fmt.Errorf("%w: bind Artifact views: %v", ErrCurrentReadInvariant, err)
	}
	if err := requireCurrentProjectionBudget(projection, authority.budget); err != nil {
		return AgentCurrentReadResult{}, err
	}
	receipt, err := model.NewCurrentReadReceipt(model.CurrentReadReceiptSpec{
		RunID: authority.run.ID(), ProfileID: authority.run.ProfileID(), HandlingID: authority.handling.ID(),
		HandlingAttempt: authority.run.HandlingAttempt(), Projection: projection, ReadAt: at,
	})
	if err != nil {
		return AgentCurrentReadResult{}, fmt.Errorf("%w: receipt: %v", ErrCurrentReadInvariant, err)
	}
	if err := writeCurrentReadEvidence(ctx, tx, authority.run, authority.handling,
		spec.ClaimTokenHash, receipt); err != nil {
		return AgentCurrentReadResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return AgentCurrentReadResult{}, fmt.Errorf("finalize Agent current read: commit: %w", err)
	}
	return AgentCurrentReadResult{Projection: projection, Receipt: receipt}, nil
}

func validateAgentCurrentReadInput(s *Store, ctx context.Context,
	spec AgentCurrentReadSpec,
) (time.Time, error) {
	if s == nil || s.db == nil || ctx == nil || spec.ProfileID != model.TeamworkProfileID() ||
		spec.ExpectedAssetRevision == "" || spec.RunID.IsZero() || spec.ClaimTokenHash.IsZero() {
		return time.Time{}, ErrCurrentReadInput
	}
	at, err := canonicalClaimTime(spec.At)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: %v", ErrCurrentReadInput, err)
	}
	return at, nil
}

func deriveCurrentReadAuthority(ctx context.Context, tx *sql.Tx, spec AgentCurrentReadSpec,
	at time.Time,
) (currentReadAuthority, error) {
	profile, budget, err := requireAgentClaimAuthority(ctx, tx, spec.ProfileID, spec.ExpectedAssetRevision)
	if err != nil {
		return currentReadAuthority{}, err
	}
	run, err := readAgentRun(ctx, tx, spec.RunID)
	if errors.Is(err, sql.ErrNoRows) {
		return currentReadAuthority{}, ErrCurrentReadStale
	}
	if err != nil {
		return currentReadAuthority{}, fmt.Errorf("%w: read AgentRun: %v", ErrCurrentReadInvariant, err)
	}
	handlingID, hasHandling := run.HandlingID()
	if !hasHandling || run.ProfileID() != profile.ID() || run.Runtime() != profile.Runtime() {
		return currentReadAuthority{}, fmt.Errorf("%w: Run is not bound to the authenticated current Profile", ErrCurrentReadStale)
	}
	handling, err := readAgentHandling(ctx, tx, handlingID)
	if errors.Is(err, sql.ErrNoRows) {
		return currentReadAuthority{}, ErrCurrentReadStale
	}
	if err != nil {
		return currentReadAuthority{}, fmt.Errorf("%w: read Handling: %v", ErrCurrentReadInvariant, err)
	}
	if err := requireExactCurrentClaim(run, handling, spec.ClaimTokenHash, at); err != nil {
		return currentReadAuthority{}, err
	}

	if stored, ok := run.CurrentReadReceipt(); ok {
		receipt, err := model.ParseCurrentReadReceipt(stored.Bytes())
		if err != nil {
			return currentReadAuthority{}, fmt.Errorf("%w: parse stored receipt: %v", ErrCurrentReadInvariant, err)
		}
		if err := requireCurrentReceiptBinding(receipt, run, handling); err != nil {
			return currentReadAuthority{}, err
		}
		artifacts, err := validateStoredCurrentAuthority(ctx, tx, receipt, run, handling, budget)
		if err != nil {
			return currentReadAuthority{}, err
		}
		return currentReadAuthority{budget: budget, run: run, handling: handling,
			projection: receipt.Projection(), receipt: receipt, hasReceipt: true,
			artifacts: artifacts}, nil
	}
	projection, artifacts, err := deriveFreshCurrentProjection(ctx, tx, handling, budget, at)
	if err != nil {
		return currentReadAuthority{}, err
	}
	return currentReadAuthority{budget: budget, run: run, handling: handling,
		projection: projection, artifacts: artifacts}, nil
}

func deriveFreshCurrentProjection(ctx context.Context, tx *sql.Tx, handling model.Handling,
	budget model.HandlingBudget, at time.Time,
) (model.CurrentProjection, []AgentCurrentArtifactMaterialization, error) {
	sourceEvent, err := readCurrentSourceEvent(ctx, tx, handling.EventID())
	if err != nil {
		return model.CurrentProjection{}, nil, fmt.Errorf("%w: source Event: %v", ErrCurrentReadInvariant, err)
	}
	parentResume, err := handlingIsParentResume(ctx, tx, handling, sourceEvent)
	if err != nil {
		return model.CurrentProjection{}, nil, fmt.Errorf("%w: inspect derivation: %v", ErrCurrentReadInvariant, err)
	}
	if parentResume {
		return model.CurrentProjection{}, nil, fmt.Errorf("%w: parent-resume requires bounded child_results", ErrCurrentReadUnsupported)
	}
	work, err := readReviewWork(ctx, tx, sourceEvent.Scope().WorkRef())
	if err != nil {
		return model.CurrentProjection{}, nil, fmt.Errorf("%w: action Work: %v", ErrCurrentReadInvariant, err)
	}
	if work.ChannelID() != sourceEvent.Scope().ChannelID() {
		return model.CurrentProjection{}, nil, fmt.Errorf("%w: source Event and action Work Channels differ", ErrCurrentReadInvariant)
	}
	if at.Before(sourceEvent.AcceptedAt()) || at.Before(work.UpdatedAt()) {
		return model.CurrentProjection{}, nil, fmt.Errorf("%w: trusted time precedes projected Event or Work evidence", ErrCurrentReadInvariant)
	}
	node, err := readNode(ctx, tx)
	if err != nil {
		return model.CurrentProjection{}, nil, fmt.Errorf("%w: local Node: %v", ErrCurrentReadInvariant, err)
	}
	role, err := localCurrentRole(node.PeerID(), work)
	if err != nil {
		return model.CurrentProjection{}, nil, err
	}
	brief, err := readCurrentWorkBrief(ctx, tx, work)
	if err != nil {
		return model.CurrentProjection{}, nil, err
	}
	if sourceEvent.Source() == model.EventSourceImported && !sourceEvent.Audience().Contains(node.PeerID()) {
		return model.CurrentProjection{}, nil, fmt.Errorf("%w: imported source Event does not address the local Node", ErrCurrentReadInvariant)
	}
	if len(sourceEvent.Artifacts()) > budget.Spec().MaxCurrentArtifactRefs {
		return model.CurrentProjection{}, nil, fmt.Errorf("%w: source Event has %d Artifact refs, Profile budget is %d",
			ErrCurrentReadTooLarge, len(sourceEvent.Artifacts()), budget.Spec().MaxCurrentArtifactRefs)
	}
	if err := requireCurrentArtifacts(ctx, tx, sourceEvent); err != nil {
		return model.CurrentProjection{}, nil, err
	}
	facts, err := decodeClosedEventPayload(sourceEvent)
	if err != nil {
		return model.CurrentProjection{}, nil, fmt.Errorf("%w: %v", ErrCurrentReadInvariant, err)
	}
	exactUpdate, err := currentWorkIsExactSource(sourceEvent, work, facts)
	if err != nil {
		return model.CurrentProjection{}, nil, err
	}
	actions := deriveCurrentActions(role, sourceEvent, work, exactUpdate)
	currentArtifacts := make([]model.CurrentArtifactRef, len(sourceEvent.Artifacts()))
	for index, ref := range sourceEvent.Artifacts() {
		currentArtifacts[index], err = model.NewCurrentArtifactRef(ref.RootDigest())
		if err != nil {
			return model.CurrentProjection{}, nil, fmt.Errorf("%w: Artifact projection: %v", ErrCurrentReadInvariant, err)
		}
	}
	currentEvent, err := model.NewCurrentEvent(model.CurrentEventSpec{
		Key: sourceEvent.Key(), Digest: sourceEvent.Digest(), Type: sourceEvent.Type(),
		WorkRef: sourceEvent.Scope().WorkRef(), Summary: sourceEvent.Summary(), Payload: sourceEvent.Payload(),
		ArtifactRefs: currentArtifacts, AcceptedAt: sourceEvent.AcceptedAt(),
	})
	if err != nil {
		return model.CurrentProjection{}, nil, fmt.Errorf("%w: Event projection: %v", ErrCurrentReadInvariant, err)
	}
	currentWork, err := model.NewCurrentWork(model.CurrentWorkSpec{
		Ref: work.Ref(), Version: work.Version(), Iteration: work.Iteration(),
		DeadlineUnixNano: work.DeadlineUnixNano(), State: work.State(), StateData: work.StateData(),
		LocalRole: role, Brief: brief,
	})
	if err != nil {
		return model.CurrentProjection{}, nil, fmt.Errorf("%w: Work projection: %v", ErrCurrentReadInvariant, err)
	}
	projection, err := model.NewCurrentProjection(model.CurrentProjectionSpec{
		SourceEvent: currentEvent, ActionWork: currentWork, AllowedActions: actions,
	})
	if errors.Is(err, model.ErrLimit) {
		return model.CurrentProjection{}, nil, fmt.Errorf("%w: %v", ErrCurrentReadTooLarge, err)
	}
	if err != nil {
		return model.CurrentProjection{}, nil, fmt.Errorf("%w: projection: %v", ErrCurrentReadInvariant, err)
	}
	if err := requireCurrentProjectionBudget(projection, budget); err != nil {
		return model.CurrentProjection{}, nil, err
	}
	artifacts, err := currentArtifactMaterializationPlan(ctx, tx, projection.ArtifactRefs())
	if err != nil {
		return model.CurrentProjection{}, nil, err
	}
	return projection, artifacts, nil
}

func validateStoredCurrentAuthority(ctx context.Context, tx *sql.Tx,
	receipt model.CurrentReadReceipt, run model.AgentRun, handling model.Handling,
	budget model.HandlingBudget,
) ([]AgentCurrentArtifactMaterialization, error) {
	sourceEvent, err := readCurrentSourceEvent(ctx, tx, handling.EventID())
	if err != nil {
		return nil, fmt.Errorf("%w: replay source Event: %v", ErrCurrentReadInvariant, err)
	}
	if parentResume, err := handlingIsParentResume(ctx, tx, handling, sourceEvent); err != nil {
		return nil, fmt.Errorf("%w: inspect replay derivation: %v", ErrCurrentReadInvariant, err)
	} else if parentResume {
		return nil, fmt.Errorf("%w: parent-resume receipt used the base projection", ErrCurrentReadInvariant)
	}
	projection := receipt.Projection()
	projectedEvent := projection.SourceEvent()
	if projectedEvent.Key() != sourceEvent.Key() || projectedEvent.Digest() != sourceEvent.Digest() ||
		projectedEvent.Type() != sourceEvent.Type() || projectedEvent.WorkRef() != sourceEvent.Scope().WorkRef() ||
		projectedEvent.Summary() != sourceEvent.Summary() ||
		!bytes.Equal(projectedEvent.Payload().Bytes(), sourceEvent.Payload().Bytes()) ||
		!projectedEvent.AcceptedAt().Equal(sourceEvent.AcceptedAt()) ||
		!sameCurrentEventArtifactRoots(projectedEvent.ArtifactRefs(), sourceEvent.Artifacts()) {
		return nil, fmt.Errorf("%w: stored projection source Event differs from durable Event",
			ErrCurrentReadInvariant)
	}
	if err := requireCurrentArtifacts(ctx, tx, sourceEvent); err != nil {
		return nil, err
	}
	work, err := readReviewWork(ctx, tx, receipt.ActionWork())
	if err != nil {
		return nil, fmt.Errorf("%w: replay action Work: %v", ErrCurrentReadInvariant, err)
	}
	if work.ChannelID() != sourceEvent.Scope().ChannelID() || work.Ref() != sourceEvent.Scope().WorkRef() {
		return nil, fmt.Errorf("%w: replay source Event and Work authority differ", ErrCurrentReadInvariant)
	}
	node, err := readNode(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("%w: replay local Node: %v", ErrCurrentReadInvariant, err)
	}
	if sourceEvent.Source() == model.EventSourceImported && !sourceEvent.Audience().Contains(node.PeerID()) {
		return nil, fmt.Errorf("%w: replay imported Event does not address the local Node",
			ErrCurrentReadInvariant)
	}
	role, err := localCurrentRole(node.PeerID(), work)
	if err != nil {
		return nil, err
	}
	durableBrief, err := readCurrentWorkBrief(ctx, tx, work)
	if err != nil {
		return nil, err
	}
	projectedWork := projection.ActionWork()
	projectedBrief, ok := projectedWork.Brief()
	if !ok || projectedWork.Ref() != work.Ref() ||
		projectedWork.DeadlineUnixNano() != work.DeadlineUnixNano() || projectedWork.LocalRole() != role ||
		projectedBrief.Content() != durableBrief.Content() ||
		projectedBrief.DeadlineUnixNano() != durableBrief.DeadlineUnixNano() ||
		!sameCurrentArtifactRoots(projectedBrief.ArtifactRefs(), durableBrief.ArtifactRefs()) {
		return nil, fmt.Errorf("%w: stored projection Work brief differs from durable authority",
			ErrCurrentReadInvariant)
	}
	if err := requireCurrentProjectionBudget(projection, budget); err != nil {
		return nil, err
	}
	artifacts, err := currentArtifactMaterializationPlan(ctx, tx, projection.ArtifactRefs())
	if err != nil {
		return nil, err
	}
	storedViews, err := requireExactCurrentArtifactViews(run.ID(), artifacts, receipt.ArtifactRefs(),
		budget.Spec().MaxCurrentPathBytes)
	if err != nil {
		return nil, fmt.Errorf("%w: stored receipt Artifact mapping: %v", ErrCurrentReadInvariant, err)
	}
	if !sameCurrentArtifactViews(storedViews, receipt.ArtifactRefs()) {
		return nil, fmt.Errorf("%w: stored receipt Artifact mapping is not exact", ErrCurrentReadInvariant)
	}
	return artifacts, nil
}

func sameCurrentEventArtifactRoots(current []model.CurrentArtifactRef,
	durable []model.ArtifactRef,
) bool {
	if len(current) != len(durable) {
		return false
	}
	for index := range current {
		if current[index].RootDigest() != durable[index].RootDigest() {
			return false
		}
		if _, hasPath := current[index].ViewPath(); hasPath {
			return false
		}
	}
	return true
}

func sameCurrentArtifactRoots(left, right []model.CurrentArtifactRef) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].RootDigest() != right[index].RootDigest() {
			return false
		}
	}
	return true
}

func currentArtifactMaterializationPlan(ctx context.Context, q rowQuerier,
	refs []model.CurrentArtifactRef,
) ([]AgentCurrentArtifactMaterialization, error) {
	result := make([]AgentCurrentArtifactMaterialization, len(refs))
	for index, ref := range refs {
		verified, err := requireVerifiedArtifactRoot(ctx, q, ref.RootDigest())
		if err != nil {
			return nil, fmt.Errorf("%w: materialization root %s is not verified: %v",
				ErrCurrentReadInvariant, ref.RootDigest().String(), err)
		}
		result[index] = AgentCurrentArtifactMaterialization{Ordinal: uint32(index),
			RootDigest: ref.RootDigest(), ManifestDigest: verified.ManifestDigest}
	}
	return result, nil
}

func requireExactCurrentArtifactViews(run model.RunID,
	plan []AgentCurrentArtifactMaterialization, supplied []model.CurrentArtifactRef, pathLimit int,
) ([]model.CurrentArtifactRef, error) {
	if pathLimit < 1 || pathLimit > model.DefaultCurrentPathBytes {
		return nil, fmt.Errorf("%w: invalid Profile Artifact path budget", ErrCurrentReadInvariant)
	}
	if len(supplied) != len(plan) {
		return nil, fmt.Errorf("%w: materializer returned %d views for %d authorized roots",
			ErrCurrentReadInvariant, len(supplied), len(plan))
	}
	result := append([]model.CurrentArtifactRef{}, supplied...)
	for index := range plan {
		item := plan[index]
		if item.Ordinal != uint32(index) || item.RootDigest.IsZero() || item.ManifestDigest.IsZero() ||
			result[index].RootDigest() != item.RootDigest {
			return nil, fmt.Errorf("%w: materialized Artifact root/ordinal differs from Store plan",
				ErrCurrentReadInvariant)
		}
		path, ok := result[index].ViewPath()
		if !ok {
			return nil, fmt.Errorf("%w: every authorized Artifact requires a readonly view",
				ErrCurrentReadInvariant)
		}
		if len(path) > pathLimit {
			return nil, fmt.Errorf("%w: Artifact view path has %d bytes, limit is %d",
				ErrCurrentReadTooLarge, len(path), pathLimit)
		}
		components := strings.Split(path, "/")
		if len(components) < 6 || components[0] != ".mnemon" || components[1] != "harness" ||
			components[2] != "node" || components[3] != "views" || components[4] != run.String() ||
			components[5] != strconv.FormatUint(uint64(item.Ordinal), 10) {
			return nil, fmt.Errorf("%w: Artifact view is outside the exact Run/ordinal namespace",
				ErrCurrentReadInvariant)
		}
	}
	return result, nil
}

func sameCurrentArtifactViews(left, right []model.CurrentArtifactRef) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		leftPath, leftOK := left[index].ViewPath()
		rightPath, rightOK := right[index].ViewPath()
		if left[index].RootDigest() != right[index].RootDigest() || leftOK != rightOK ||
			leftPath != rightPath {
			return false
		}
	}
	return true
}

func requireCurrentProjectionBudget(projection model.CurrentProjection,
	budget model.HandlingBudget,
) error {
	if len(projection.CanonicalJSON().Bytes()) > budget.Spec().MaxCurrentJSONBytes {
		return fmt.Errorf("%w: projection has %d bytes, Profile budget is %d",
			ErrCurrentReadTooLarge, len(projection.CanonicalJSON().Bytes()), budget.Spec().MaxCurrentJSONBytes)
	}
	if len(projection.ArtifactRefs()) > budget.Spec().MaxCurrentArtifactRefs {
		return fmt.Errorf("%w: projection has %d Artifact refs, Profile budget is %d",
			ErrCurrentReadTooLarge, len(projection.ArtifactRefs()), budget.Spec().MaxCurrentArtifactRefs)
	}
	return nil
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

func readCurrentWorkBrief(ctx context.Context, q rowQuerier,
	work model.ReviewWork,
) (model.CurrentBrief, error) {
	var count int
	var eventText sql.NullString
	err := q.QueryRowContext(ctx, `SELECT COUNT(*),MIN(event_id) FROM events
		WHERE work_home_peer_id=? AND work_id=? AND origin_peer_id=? AND event_type='review.offered'`,
		work.Ref().HomePeerID().String(), work.Ref().WorkID().String(), work.Ref().HomePeerID().String()).
		Scan(&count, &eventText)
	if err != nil {
		return model.CurrentBrief{}, fmt.Errorf("%w: locate offered Work brief: %v", ErrCurrentReadInvariant, err)
	}
	if count != 1 || !eventText.Valid {
		return model.CurrentBrief{}, fmt.Errorf("%w: Work has %d immutable review.offered Events, want exactly 1",
			ErrCurrentReadInvariant, count)
	}
	eventID, err := model.ParseEventID(eventText.String)
	if err != nil {
		return model.CurrentBrief{}, fmt.Errorf("%w: offered Event identity: %v", ErrCurrentReadInvariant, err)
	}
	offered, err := readCurrentSourceEvent(ctx, q, eventID)
	if err != nil {
		return model.CurrentBrief{}, fmt.Errorf("%w: offered Event evidence: %v", ErrCurrentReadInvariant, err)
	}
	participants := work.Participants()
	if offered.Type() != model.EventReviewOffered || offered.Scope().WorkRef() != work.Ref() ||
		offered.Scope().ChannelID() != work.ChannelID() ||
		offered.Scope().PublicationRoster().Revision() != participants.RosterRevision() ||
		participants.InitiatorPeerID() != work.Ref().HomePeerID() || offered.Audience().Len() != 1 ||
		!offered.Audience().Contains(participants.ReviewerPeerID()) || offered.AcceptedAt().After(work.UpdatedAt()) {
		return model.CurrentBrief{}, fmt.Errorf("%w: offered Event does not bind Work participant/history",
			ErrCurrentReadInvariant)
	}
	facts, err := decodeClosedEventPayload(offered)
	if err != nil || facts.WorkVersion != 1 || facts.Iteration != 1 ||
		facts.DeadlineUnixNano != work.DeadlineUnixNano() {
		return model.CurrentBrief{}, fmt.Errorf("%w: offered Event does not bind Work deadline/version/iteration",
			ErrCurrentReadInvariant)
	}
	var payload struct {
		Content  string `json:"content"`
		Deadline string `json:"deadline"`
		versionPayload
	}
	if err := decodeExactPayload(offered, &payload); err != nil || payload.Content == "" {
		return model.CurrentBrief{}, fmt.Errorf("%w: offered Event content is unavailable", ErrCurrentReadInvariant)
	}
	if err := requireCurrentArtifacts(ctx, q, offered); err != nil {
		return model.CurrentBrief{}, err
	}
	artifacts := make([]model.CurrentArtifactRef, len(offered.Artifacts()))
	for index, ref := range offered.Artifacts() {
		artifacts[index], err = model.NewCurrentArtifactRef(ref.RootDigest())
		if err != nil {
			return model.CurrentBrief{}, fmt.Errorf("%w: offered Artifact ref: %v", ErrCurrentReadInvariant, err)
		}
	}
	brief, err := model.NewCurrentBrief(model.CurrentBriefSpec{Content: payload.Content,
		DeadlineUnixNano: work.DeadlineUnixNano(), ArtifactRefs: artifacts})
	if err != nil {
		return model.CurrentBrief{}, fmt.Errorf("%w: offered Work brief: %v", ErrCurrentReadInvariant, err)
	}
	return brief, nil
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
		if err := requireCurrentArtifactProvenance(ctx, q, event, ref); err != nil {
			return err
		}
	}
	return nil
}

func requireCurrentArtifactProvenance(ctx context.Context, q rowQuerier, event model.Event,
	ref model.ArtifactRef,
) error {
	if ref.Role() == model.ArtifactReferenced {
		if err := requireReusableArtifactRoot(ctx, q, ref.RootDigest()); err != nil {
			return fmt.Errorf("%w: referenced Artifact %s has no producer provenance: %v",
				ErrCurrentReadInvariant, ref.RootDigest().String(), err)
		}
		return nil
	}
	if ref.Role() != model.ArtifactProduced {
		return fmt.Errorf("%w: source Event Artifact has invalid role", ErrCurrentReadInvariant)
	}
	var origin, relation string
	var localRun, operation sql.NullString
	err := q.QueryRowContext(ctx, `SELECT producer_origin_peer_id,relation,local_agent_run_id,operation_id
		FROM artifact_provenance WHERE root_digest=? AND producer_event_id=?`,
		ref.RootDigest().String(), event.ID().String()).Scan(&origin, &relation, &localRun, &operation)
	if err != nil {
		return fmt.Errorf("%w: produced Artifact %s provenance is unavailable: %v",
			ErrCurrentReadInvariant, ref.RootDigest().String(), err)
	}
	expectedRelation := string(model.ProvenanceReplica)
	validAuthority := !localRun.Valid && !operation.Valid
	if event.Source() == model.EventSourceLocal {
		expectedRelation = string(model.ProvenanceLocalCapture)
		validAuthority = localRun.Valid && operation.Valid
	}
	if origin != event.Scope().OriginPeerID().String() || relation != expectedRelation || !validAuthority {
		return fmt.Errorf("%w: produced Artifact %s provenance differs from source Event",
			ErrCurrentReadInvariant, ref.RootDigest().String())
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
