package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func readWorkExpiryDeliveryFence(ctx context.Context, tx *sql.Tx, scope LocalAdmissionScope,
	target model.PeerID,
) (workExpiryDeliveryFence, error) {
	binding, confirmed, err := readAudienceBindingTx(ctx, tx, scope, target)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return workExpiryDeliveryFence{}, fmt.Errorf("%w: frozen reviewer has no binding",
				ErrWorkDeadlineInvariant)
		}
		return workExpiryDeliveryFence{}, fmt.Errorf("read Work expiry delivery authority: %w", err)
	}
	if !binding.Valid() {
		return workExpiryDeliveryFence{}, fmt.Errorf("%w: invalid reviewer binding state",
			ErrWorkDeadlineInvariant)
	}
	if confirmed.Valid {
		if _, err := parseCanonicalStoreTime(confirmed.String); err != nil {
			return workExpiryDeliveryFence{}, fmt.Errorf("%w: invalid reviewer baseline time",
				ErrWorkDeadlineInvariant)
		}
	}
	return workExpiryDeliveryFence{binding: binding, baselineAt: confirmed.String,
		baselineConfirmed: confirmed.Valid}, nil
}

func (f workExpiryDeliveryFence) status() (string, string) {
	if f.binding == model.BindingActive && f.baselineConfirmed {
		return "pending", ""
	}
	return "blocked", workExpiryBlockedReason
}

func readWorkExpiryOperationFence(ctx context.Context, tx *sql.Tx, work model.ReviewWork,
	at time.Time,
) (workExpiryOperationFence, error) {
	ids, err := readCompetingExpiryOperationIDs(ctx, tx, work.Ref())
	if err != nil {
		return workExpiryOperationFence{}, err
	}
	if len(ids) == 0 {
		return workExpiryOperationFence{}, nil
	}
	if len(ids) != 1 {
		return workExpiryOperationFence{}, fmt.Errorf("%w: multiple competing Operations",
			ErrWorkDeadlineInvariant)
	}
	return reconstructWorkExpiryOperationFence(ctx, tx, ids[0], work, at)
}

func readCompetingExpiryOperationIDs(ctx context.Context, tx *sql.Tx,
	work model.WorkRef,
) ([]model.OperationID, error) {
	rows, err := tx.QueryContext(ctx, `SELECT o.operation_id FROM operations o
		JOIN agent_runs r ON r.run_id=o.agent_run_id
		WHERE o.profile_id=? AND o.status='started' AND o.context_hash IS NOT NULL
		AND r.current_read_receipt_json IS NOT NULL AND CASE
			WHEN json_valid(CAST(r.current_read_receipt_json AS TEXT))=0 THEN 1
			ELSE (
			json_extract(CAST(r.current_read_receipt_json AS TEXT),'$.action_work.home_peer_id')=?
			AND json_extract(CAST(r.current_read_receipt_json AS TEXT),'$.action_work.work_id')=?) END
		ORDER BY o.operation_id LIMIT 2`, model.TeamworkProfileID().String(),
		work.HomePeerID().String(), work.WorkID().String())
	if err != nil {
		return nil, fmt.Errorf("inspect competing expiry Operation: %w", err)
	}
	ids := make([]model.OperationID, 0, 2)
	for rows.Next() {
		var text string
		if err := rows.Scan(&text); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan competing expiry Operation: %w", err)
		}
		id, err := model.ParseOperationID(text)
		if err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("%w: invalid competing Operation ID",
				ErrWorkDeadlineInvariant)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("read competing expiry Operations: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close competing expiry Operations: %w", err)
	}
	return ids, nil
}

func reconstructWorkExpiryOperationFence(ctx context.Context, tx *sql.Tx,
	id model.OperationID, work model.ReviewWork, at time.Time,
) (workExpiryOperationFence, error) {
	operation, err := readOperationByID(ctx, tx, id)
	if err != nil {
		return workExpiryOperationFence{}, fmt.Errorf("reconstruct competing Operation: %w", err)
	}
	run, err := readAgentRun(ctx, tx, operation.AgentRunID())
	if err != nil {
		return workExpiryOperationFence{}, fmt.Errorf("%w: invalid competing AgentRun: %v",
			ErrWorkDeadlineInvariant, err)
	}
	raw, ok := run.CurrentReadReceipt()
	if !ok {
		return workExpiryOperationFence{}, fmt.Errorf("%w: competing AgentRun has no current receipt",
			ErrWorkDeadlineInvariant)
	}
	current, err := model.ParseCurrentReadReceipt(raw.Bytes())
	if err != nil {
		return workExpiryOperationFence{}, fmt.Errorf("%w: invalid competing current receipt: %v",
			ErrWorkDeadlineInvariant, err)
	}
	if operation.CreatedAt().After(at) {
		return workExpiryOperationFence{}, fmt.Errorf("%w: competing Operation began after expiry preparation",
			ErrWorkDeadlineStale)
	}
	contextHash, hasContext := operation.ContextHash()
	if !hasContext || !validWorkExpiryCurrent(run, current, work, operation.Kind(), at) {
		return workExpiryOperationFence{}, fmt.Errorf("%w: competing Operation does not bind exact Work",
			ErrWorkDeadlineInvariant)
	}
	if err := validateStartedManagedOperation(operation, run, contextHash, at); err != nil {
		return workExpiryOperationFence{}, fmt.Errorf("%w: %v", ErrWorkDeadlineInvariant, err)
	}
	return workExpiryOperationFence{operation: operation, run: run, current: raw, found: true}, nil
}

func validWorkExpiryCurrent(run model.AgentRun, current model.CurrentReadReceipt,
	work model.ReviewWork, kind model.OperationKind, at time.Time,
) bool {
	handlingID, hasHandling := run.HandlingID()
	return hasHandling && current.RunID() == run.ID() && current.ProfileID() == run.ProfileID() &&
		current.HandlingID() == handlingID && current.HandlingAttempt() == run.HandlingAttempt() &&
		run.Status().OperationAuthority() && current.ActionWork() == work.Ref() &&
		current.ActionWorkVersion() == work.Version() && current.ActionWorkUpdatedBy() == work.UpdatedBy() &&
		current.ActionWorkUpdatedAt().Equal(work.UpdatedAt()) && !current.ReadAt().After(at) &&
		deadlineCompetingHomeAction(kind) && managedCurrentAllows(current, kind)
}

func sameWorkExpiryOperationFence(left, right workExpiryOperationFence) bool {
	if left.found != right.found || !left.found {
		return left.found == right.found
	}
	return sameDeadlineOperation(left.operation, right.operation) &&
		sameDeadlineAgentRun(left.run, right.run) && left.current.String() == right.current.String()
}

func sameDeadlineOperation(left, right model.Operation) bool {
	leftContext, leftHasContext := left.ContextHash()
	rightContext, rightHasContext := right.ContextHash()
	leftLease, leftHasLease := left.LeaseUntil()
	rightLease, rightHasLease := right.LeaseUntil()
	leftCapture, leftHasCapture := left.Capture()
	rightCapture, rightHasCapture := right.Capture()
	return left.ID() == right.ID() && left.ProfileID() == right.ProfileID() &&
		left.AgentRunID() == right.AgentRunID() && left.ClientKeyHash() == right.ClientKeyHash() &&
		leftHasContext == rightHasContext && (!leftHasContext || leftContext == rightContext) &&
		left.Kind() == right.Kind() && left.RequestDigest() == right.RequestDigest() &&
		left.Status() == right.Status() && left.LeaseOwner() == right.LeaseOwner() &&
		leftHasLease == rightHasLease && (!leftHasLease || leftLease.Equal(rightLease)) &&
		leftHasCapture == rightHasCapture && (!leftHasCapture || leftCapture.String() == rightCapture.String()) &&
		left.CreatedAt().Equal(right.CreatedAt())
}

func sameDeadlineAgentRun(left, right model.AgentRun) bool {
	leftHandling, leftHasHandling := left.HandlingID()
	rightHandling, rightHasHandling := right.HandlingID()
	leftFence, leftHasFence := left.ClaimFenceHash()
	rightFence, rightHasFence := right.ClaimFenceHash()
	leftLease, leftHasLease := left.LeaseUntil()
	rightLease, rightHasLease := right.LeaseUntil()
	return left.ID() == right.ID() && left.ProfileID() == right.ProfileID() &&
		left.Status() == right.Status() && leftHasHandling == rightHasHandling &&
		(!leftHasHandling || leftHandling == rightHandling) &&
		left.HandlingAttempt() == right.HandlingAttempt() &&
		left.HandlingRecovery() == right.HandlingRecovery() && leftHasFence == rightHasFence &&
		(!leftHasFence || leftFence == rightFence) && leftHasLease == rightHasLease &&
		(!leftHasLease || leftLease.Equal(rightLease))
}
