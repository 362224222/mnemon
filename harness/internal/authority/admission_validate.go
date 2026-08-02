package authority

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

const MaxOpenHandlingsPerPrincipal = 64

type admissionRejection struct {
	code       agency.SemanticLabel
	diagnostic string
}

func validateMutableAuthorityTx(ctx context.Context, tx *sql.Tx, attachment agency.Attachment,
	request agency.BoundIntent, now time.Time,
) (*admissionRejection, error) {
	if rejection, err := validateIssuedViewTx(ctx, tx, attachment, request); err != nil || rejection != nil {
		return rejection, err
	}
	if rejection := validateLocalTargets(attachment, request.Targets()); rejection != nil {
		return rejection, nil
	}
	claim, claimUntil, err := currentClaimForAdmissionTx(ctx, tx, attachment)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if errors.Is(err, sql.ErrNoRows) {
		claim = nil
	}
	if rejection := validateBoundSubject(request, claim, claimUntil, now); rejection != nil {
		return rejection, nil
	}
	if claim == nil {
		rejection, err := rejectIfClaimableTx(ctx, tx, attachment.Principal())
		if err != nil || rejection != nil {
			return rejection, err
		}
	}
	if rejection, err := validateReferenceExpectationTx(ctx, tx, request); err != nil || rejection != nil {
		return rejection, err
	}
	if rejection, err := validateHandlingBoundTx(ctx, tx, attachment.Principal(), request); err != nil || rejection != nil {
		return rejection, err
	}
	if request.Intent().Consequence() == agency.ConsequenceResolveCompleted && len(request.Artifacts()) == 0 {
		return reject(rejectionArtifactUnavailable, "completed requires a verified Artifact"), nil
	}
	return nil, nil
}

// validateIssuedViewTx proves that this request was bound from one exact View
// frozen by Current. BoundIntent construction already resolves every selected
// offer from that View; live subject and Reference heads are checked below.
// Unselected world changes therefore do not invalidate a fresh operation.
func validateIssuedViewTx(ctx context.Context, tx *sql.Tx, attachment agency.Attachment,
	request agency.BoundIntent,
) (*admissionRejection, error) {
	var digestValue string
	var canonical []byte
	err := tx.QueryRowContext(ctx, `SELECT authority_digest, authority_json
		FROM current_operations WHERE attachment_id = ? AND authority_digest = ? LIMIT 1`,
		attachment.ID().String(), request.ViewDigest().String()).Scan(&digestValue, &canonical)
	if errors.Is(err, sql.ErrNoRows) {
		return reject(rejectionStaleView, "View was not issued by this authority"), nil
	}
	if err != nil {
		return nil, fmt.Errorf("admit Intent: inspect issued View: %w", err)
	}
	digest, err := agency.ParseDigest(digestValue)
	view, parseErr := agency.ParseViewAuthorityCanonicalJSON(canonical, attachment)
	if err != nil || parseErr != nil || digest != request.ViewDigest() || view.Digest() != digest {
		return nil, errors.New("admit Intent: corrupt issued View authority")
	}
	return nil, nil
}

func validateHandlingBoundTx(ctx context.Context, tx *sql.Tx, principal agency.AgentPrincipalID,
	request agency.BoundIntent,
) (*admissionRejection, error) {
	delta := int64(len(request.Targets()))
	switch request.Intent().Consequence() {
	case agency.ConsequenceResolveCompleted, agency.ConsequenceResolveDeclined,
		agency.ConsequenceResolveUnresolved:
		delta--
	case agency.ConsequencePublishReference, agency.ConsequenceSupersedeReference,
		agency.ConsequenceRetractReference:
		return nil, nil
	}
	if delta <= 0 {
		return nil, nil
	}
	var openCount int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM handlings
		WHERE target_principal_id = ? AND state = 'open'`, principal.String()).Scan(&openCount); err != nil {
		return nil, fmt.Errorf("admit Intent: count open Handlings: %w", err)
	}
	if openCount+delta > MaxOpenHandlingsPerPrincipal {
		return reject(rejectionResourceBound, "open Handling bound reached"), nil
	}
	return nil, nil
}

func validateLocalTargets(attachment agency.Attachment,
	targets []agency.ResolvedTarget,
) *admissionRejection {
	for _, target := range targets {
		if target.Destination() != agency.TargetDestinationLocal {
			return reject(rejectionRemoteUnsupported, "remote targets are unavailable in M1A")
		}
		if target.LocalPrincipal() != attachment.Principal() {
			return reject(rejectionStaleView, "target is outside the local Principal")
		}
	}
	return nil
}

func validateBoundSubject(request agency.BoundIntent, claim *projectedClaim,
	claimUntil, now time.Time,
) *admissionRejection {
	subject, present := request.Subject()
	if !present {
		return nil
	}
	if claim == nil || subject.HandlingID() != claim.handlingID || subject.Head() != claim.head ||
		subject.Fence() != claim.fence || !now.Before(claimUntil) {
		return reject(rejectionStaleSubject, "subject claim, head, or fence is stale")
	}
	return nil
}

func rejectIfClaimableTx(ctx context.Context, tx *sql.Tx,
	principal agency.AgentPrincipalID,
) (*admissionRejection, error) {
	var claimable int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM handlings
		WHERE target_principal_id = ? AND state = 'open' AND claim_attachment_id IS NULL)`,
		principal.String()).Scan(&claimable); err != nil {
		return nil, fmt.Errorf("admit Intent: inspect claimable Handling: %w", err)
	}
	if claimable == 1 {
		return reject(rejectionStaleView, "a pending Handling now requires a current View"), nil
	}
	return nil, nil
}

func currentClaimForAdmissionTx(ctx context.Context, tx *sql.Tx,
	attachment agency.Attachment,
) (*projectedClaim, time.Time, error) {
	var handlingValue, eventValue, digestValue, claimUntilValue string
	var fence uint64
	var canonical []byte
	err := tx.QueryRowContext(ctx, `SELECT h.handling_id, h.claim_fence, h.head_event_id,
		e.event_digest, e.canonical_json, h.claim_until
		FROM handlings h JOIN events e ON e.event_id = h.head_event_id
		WHERE h.claim_attachment_id = ? AND h.target_principal_id = ? AND h.state = 'open'`,
		attachment.ID().String(), attachment.Principal().String()).
		Scan(&handlingValue, &fence, &eventValue, &digestValue, &canonical, &claimUntilValue)
	if err != nil {
		return nil, time.Time{}, err
	}
	handlingID, err := agency.NewHandlingID(handlingValue)
	if err != nil {
		return nil, time.Time{}, errors.New("admit Intent: corrupt claimed Handling ID")
	}
	eventRef, kind, payload, err := inspectStoredEvent(eventValue, digestValue, canonical)
	if err != nil {
		return nil, time.Time{}, err
	}
	artifacts, err := loadEventArtifactsTx(ctx, tx, eventRef.ID())
	if err != nil {
		return nil, time.Time{}, err
	}
	claimUntil, err := parseTime(claimUntilValue)
	if err != nil {
		return nil, time.Time{}, err
	}
	return &projectedClaim{handlingID: handlingID, head: eventRef, fence: fence,
		kind: kind, payload: payload, artifacts: artifacts}, claimUntil, nil
}

func validateReferenceExpectationTx(ctx context.Context, tx *sql.Tx,
	request agency.BoundIntent,
) (*admissionRejection, error) {
	expected, present := request.ExpectedReference()
	if !present {
		return nil, nil
	}
	var headValue, state string
	err := tx.QueryRowContext(ctx, `SELECT head_event_id, state FROM active_references
		WHERE reference_key = ?`, expected.Key().String()).Scan(&headValue, &state)
	if expected.IsAbsent() {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("admit Intent: inspect absent Reference: %w", err)
		}
		return reject(rejectionStaleReference, "Reference key is no longer absent"), nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return reject(rejectionStaleReference, "Reference head no longer exists"), nil
	}
	if err != nil {
		return nil, fmt.Errorf("admit Intent: inspect Reference head: %w", err)
	}
	if headValue != expected.Head().ID().String() {
		return reject(rejectionStaleReference, "Reference head changed"), nil
	}
	if request.Intent().Consequence() == agency.ConsequenceRetractReference && state != "active" {
		return reject(rejectionStaleReference, "Reference head is already retracted"), nil
	}
	return nil, nil
}

func reject(code agency.SemanticLabel, diagnostic string) *admissionRejection {
	return &admissionRejection{code: code, diagnostic: diagnostic}
}
