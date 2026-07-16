package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func readOperationByClientKey(ctx context.Context, q rowQuerier, profile model.ProfileID, key model.Digest) (model.Operation, error) {
	return scanOperation(q.QueryRowContext(ctx, operationSelect+" WHERE profile_id = ? AND client_key_hash = ?", profile.String(), key.Bytes()))
}

func readOperationByID(ctx context.Context, q rowQuerier, id model.OperationID) (model.Operation, error) {
	return scanOperation(q.QueryRowContext(ctx, operationSelect+" WHERE operation_id = ?", id.String()))
}

const operationSelect = "SELECT operation_id, profile_id, agent_run_id, client_key_hash, context_hash, kind, request_digest, status, lease_owner, lease_until, capture_json, result_json, created_at, finished_at FROM operations"

func scanOperation(row *sql.Row) (model.Operation, error) {
	var idText, profileText, runText, kindText, statusText, createdText string
	var clientKey, contextHash, requestDigest, capture, result []byte
	var leaseOwner, leaseText, finishedText sql.NullString
	if err := row.Scan(&idText, &profileText, &runText, &clientKey, &contextHash, &kindText, &requestDigest, &statusText,
		&leaseOwner, &leaseText, &capture, &result, &createdText, &finishedText); err != nil {
		return model.Operation{}, err
	}
	id, err := model.ParseOperationID(idText)
	if err != nil {
		return model.Operation{}, err
	}
	profile, err := model.ParseProfileID(profileText)
	if err != nil {
		return model.Operation{}, err
	}
	runID, err := model.ParseRunID(runText)
	if err != nil {
		return model.Operation{}, err
	}
	key, err := model.DigestFromBytes(clientKey)
	if err != nil {
		return model.Operation{}, err
	}
	request, err := model.DigestFromBytes(requestDigest)
	if err != nil {
		return model.Operation{}, err
	}
	created, err := parseCanonicalStoreTime(createdText)
	if err != nil {
		return model.Operation{}, err
	}
	spec := model.OperationSpec{ID: id, ProfileID: profile, AgentRunID: runID, ClientKeyHash: key, Kind: model.OperationKind(kindText),
		RequestDigest: request, Status: model.OperationStatus(statusText), LeaseOwner: leaseOwner.String, CreatedAt: created}
	if len(contextHash) > 0 {
		value, err := model.DigestFromBytes(contextHash)
		if err != nil {
			return model.Operation{}, err
		}
		spec.ContextHash = &value
	}
	if leaseText.Valid {
		value, err := parseCanonicalStoreTime(leaseText.String)
		if err != nil {
			return model.Operation{}, err
		}
		spec.LeaseUntil = &value
	}
	if len(capture) > 0 {
		value, err := model.NewJSON(capture)
		if err != nil {
			return model.Operation{}, err
		}
		if !bytes.Equal(value.Bytes(), capture) {
			return model.Operation{}, errors.New("operation capture JSON is not canonical")
		}
		spec.Capture = &value
	}
	if len(result) > 0 {
		value, err := model.NewJSON(result)
		if err != nil {
			return model.Operation{}, err
		}
		if !bytes.Equal(value.Bytes(), result) {
			return model.Operation{}, errors.New("operation result JSON is not canonical")
		}
		spec.Result = &value
	}
	if finishedText.Valid {
		value, err := parseCanonicalStoreTime(finishedText.String)
		if err != nil {
			return model.Operation{}, err
		}
		spec.FinishedAt = &value
	}
	return model.NewOperation(spec)
}

func operationWithLease(operation model.Operation, owner string, until time.Time) (model.Operation, error) {
	spec := model.OperationSpec{ID: operation.ID(), ProfileID: operation.ProfileID(), AgentRunID: operation.AgentRunID(), ClientKeyHash: operation.ClientKeyHash(),
		Kind: operation.Kind(), RequestDigest: operation.RequestDigest(), Status: model.OperationStarted,
		LeaseOwner: owner, LeaseUntil: &until, CreatedAt: operation.CreatedAt()}
	if contextHash, ok := operation.ContextHash(); ok {
		spec.ContextHash = &contextHash
	}
	if capture, ok := operation.Capture(); ok {
		spec.Capture = &capture
	}
	return model.NewOperation(spec)
}

func operationTerminal(operation model.Operation, status model.OperationStatus, result model.JSON, finished time.Time) (model.Operation, error) {
	spec := model.OperationSpec{ID: operation.ID(), ProfileID: operation.ProfileID(), AgentRunID: operation.AgentRunID(), ClientKeyHash: operation.ClientKeyHash(),
		Kind: operation.Kind(), RequestDigest: operation.RequestDigest(), Status: status, Result: &result,
		CreatedAt: operation.CreatedAt(), FinishedAt: &finished}
	if contextHash, ok := operation.ContextHash(); ok {
		spec.ContextHash = &contextHash
	}
	if capture, ok := operation.Capture(); ok {
		spec.Capture = &capture
	}
	return model.NewOperation(spec)
}
