package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type managedOperationRejectionSpec struct {
	RunID       model.RunID
	ProfileID   model.ProfileID
	ContextHash model.Digest
	Code        string
	Message     string
	At          time.Time
}

func rejectStartedManagedOperations(ctx context.Context, tx *sql.Tx,
	spec managedOperationRejectionSpec,
) error {
	run, at, err := readManagedOperationRejectionAuthority(ctx, tx, spec)
	if err != nil {
		return err
	}
	operations, err := readStartedManagedOperations(ctx, tx, run)
	if err != nil {
		return err
	}
	if len(operations) > 1 {
		return fmt.Errorf("managed AgentRun has %d started operations, want at most one", len(operations))
	}
	for _, operation := range operations {
		if err := validateStartedManagedOperation(operation, run, spec.ContextHash, at); err != nil {
			return err
		}
		receipt, err := model.NewOperationRejectionReceipt(model.OperationRejectionSpec{
			OperationID: operation.ID(), Code: spec.Code, Message: spec.Message,
		})
		if err != nil {
			return fmt.Errorf("build managed operation rejection receipt: %w", err)
		}
		if err := rejectStartedManagedOperation(ctx, tx, operation, receipt.JSON(), at); err != nil {
			return err
		}
	}
	return nil
}

func readManagedOperationRejectionAuthority(ctx context.Context, tx *sql.Tx,
	spec managedOperationRejectionSpec,
) (model.AgentRun, time.Time, error) {
	if ctx == nil || tx == nil || spec.RunID.IsZero() || spec.ProfileID.IsZero() ||
		spec.ContextHash.IsZero() {
		return model.AgentRun{}, time.Time{},
			fmt.Errorf("reject started managed operations: incomplete authority")
	}
	at, err := canonicalStoreTime(spec.At)
	if err != nil || at.IsZero() {
		return model.AgentRun{}, time.Time{},
			fmt.Errorf("reject started managed operations: invalid trusted time")
	}
	run, err := readAgentRun(ctx, tx, spec.RunID)
	if err != nil {
		return model.AgentRun{}, time.Time{}, fmt.Errorf("reconstruct managed AgentRun: %w", err)
	}
	fence, hasFence := run.ClaimFenceHash()
	if run.ID() != spec.RunID || run.ProfileID() != spec.ProfileID ||
		!run.Status().OperationAuthority() || !hasFence || fence != spec.ContextHash ||
		at.Before(run.StartedAt()) {
		return model.AgentRun{}, time.Time{},
			fmt.Errorf("managed AgentRun authority differs from rejection fence")
	}
	return run, at, nil
}

func readStartedManagedOperations(ctx context.Context, tx *sql.Tx,
	run model.AgentRun,
) ([]model.Operation, error) {
	fence, _ := run.ClaimFenceHash()
	rows, err := tx.QueryContext(ctx, `SELECT operation_id FROM operations
		WHERE status='started' AND (agent_run_id=? OR (profile_id=? AND context_hash=?))
		ORDER BY operation_id LIMIT 2`, run.ID().String(), run.ProfileID().String(), fence.Bytes())
	if err != nil {
		return nil, fmt.Errorf("query started managed operations: %w", err)
	}
	ids := make([]model.OperationID, 0, 2)
	for rows.Next() {
		var idText string
		if err := rows.Scan(&idText); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan started managed operation: %w", err)
		}
		id, err := model.ParseOperationID(idText)
		if err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("invalid started managed operation ID: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("read started managed operations: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close started managed operations: %w", err)
	}
	operations := make([]model.Operation, 0, len(ids))
	for _, id := range ids {
		operation, err := readOperationByID(ctx, tx, id)
		if err != nil {
			return nil, fmt.Errorf("reconstruct started managed operation %s: %w", id.String(), err)
		}
		operations = append(operations, operation)
	}
	return operations, nil
}

func validateStartedManagedOperation(operation model.Operation, run model.AgentRun,
	contextHash model.Digest, at time.Time,
) error {
	storedContext, hasContext := operation.ContextHash()
	wantID, err := managedOperationID(operation.ProfileID(), operation.ClientKeyHash())
	if err != nil || operation.ID() != wantID || operation.AgentRunID() != run.ID() ||
		operation.ProfileID() != run.ProfileID() || operation.Status() != model.OperationStarted ||
		!hasContext || storedContext != contextHash {
		return fmt.Errorf("started managed operation authority differs from AgentRun fence")
	}
	if operation.CreatedAt().After(at) {
		return fmt.Errorf("trusted time precedes started managed operation %s", operation.ID().String())
	}
	return nil
}

func rejectStartedManagedOperation(ctx context.Context, tx *sql.Tx, operation model.Operation,
	receipt model.JSON, at time.Time,
) error {
	contextHash, _ := operation.ContextHash()
	leaseUntil, _ := operation.LeaseUntil()
	result, err := tx.ExecContext(ctx, `UPDATE operations SET status='rejected',lease_owner=NULL,
		lease_until=NULL,result_json=?,finished_at=? WHERE operation_id=? AND profile_id=?
		AND agent_run_id=? AND client_key_hash=? AND context_hash=? AND kind=? AND request_digest=?
		AND status='started' AND lease_owner=? AND lease_until=? AND result_json IS NULL
		AND finished_at IS NULL AND created_at=?`, receipt.Bytes(), storeTime(at),
		operation.ID().String(), operation.ProfileID().String(), operation.AgentRunID().String(),
		operation.ClientKeyHash().Bytes(), contextHash.Bytes(), string(operation.Kind()),
		operation.RequestDigest().Bytes(), operation.LeaseOwner(), storeTime(leaseUntil),
		storeTime(operation.CreatedAt()))
	if err != nil {
		return fmt.Errorf("reject started managed operation %s: %w", operation.ID().String(), err)
	}
	if err := requireExactlyOneRow(result,
		"reject started managed operation "+operation.ID().String()); err != nil {
		return err
	}
	return nil
}
