package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var errEventDisseminationInvariant = errors.New("Event dissemination durable invariant violated")

// eventDisseminationReady is the single outbound gate for an Event. Artifact
// references must have their exact Event and publication pins, and every
// referenced root must be verified before either Gossip or Pull exposes it.
// A local producer must additionally have its own current operation stage
// ready; a verified root published by another operation is not sufficient.
func eventDisseminationReady(ctx context.Context, tx *sql.Tx,
	eventID model.EventID,
) (bool, error) {
	if ctx == nil || tx == nil || eventID.IsZero() {
		return false, errEventDisseminationInvariant
	}
	var present, exactPins, rootsReady int
	err := tx.QueryRowContext(ctx, `
		WITH target(artifact_roots_json) AS (
			SELECT artifact_roots_json FROM events WHERE event_id=?
		), expected(root_digest) AS (
			SELECT json_extract(entry.value,'$.root_digest')
			FROM target,json_each(target.artifact_roots_json) entry
		)
		SELECT
			(SELECT COUNT(*) FROM target),
			NOT EXISTS (
				SELECT 1 FROM expected
				WHERE NOT EXISTS (
					SELECT 1 FROM artifact_pins pin
					WHERE pin.root_digest=expected.root_digest
						AND pin.owner_kind='event' AND pin.owner_id=?
				)
				OR NOT EXISTS (
					SELECT 1 FROM artifact_pins pin
					WHERE pin.root_digest=expected.root_digest
						AND pin.owner_kind='publication' AND pin.owner_id=?
				)
			)
			AND NOT EXISTS (
				SELECT 1 FROM artifact_pins pin
				WHERE pin.owner_kind='event' AND pin.owner_id=?
					AND NOT EXISTS (
						SELECT 1 FROM expected WHERE expected.root_digest=pin.root_digest
					)
			)
			AND NOT EXISTS (
				SELECT 1 FROM artifact_pins pin
				WHERE pin.owner_kind='publication' AND pin.owner_id=?
					AND NOT EXISTS (
						SELECT 1 FROM expected WHERE expected.root_digest=pin.root_digest
					)
			),
			NOT EXISTS (
				SELECT 1 FROM expected
				LEFT JOIN artifact_roots root ON root.root_digest=expected.root_digest
				WHERE root.root_digest IS NULL OR root.state<>'verified'
					OR root.verified_at IS NULL
			)`,
		eventID.String(), eventID.String(), eventID.String(), eventID.String(),
		eventID.String()).Scan(&present, &exactPins, &rootsReady)
	if err != nil || present != 1 || exactPins != 1 {
		return false, errEventDisseminationInvariant
	}
	localReady, err := localEventArtifactPublicationReady(ctx, tx, eventID)
	if err != nil {
		return false, err
	}
	return rootsReady == 1 && localReady, nil
}

func localEventArtifactPublicationReady(ctx context.Context, tx *sql.Tx,
	eventID model.EventID,
) (bool, error) {
	var source string
	if err := tx.QueryRowContext(ctx, `SELECT source FROM events WHERE event_id=?`,
		eventID.String()).Scan(&source); err != nil {
		return false, errEventDisseminationInvariant
	}
	if source == string(model.EventSourceImported) {
		var localCapture int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM artifact_provenance
			WHERE producer_event_id=? AND relation='local_capture'`,
			eventID.String()).Scan(&localCapture); err != nil || localCapture != 0 {
			return false, errEventDisseminationInvariant
		}
		return true, nil
	}
	if source != string(model.EventSourceLocal) {
		return false, errEventDisseminationInvariant
	}

	var expected, provenance, exact, operations int
	var operationID sql.NullString
	err := tx.QueryRowContext(ctx, `
		WITH expected(root_digest) AS (
			SELECT json_extract(entry.value,'$.root_digest')
			FROM events event,json_each(event.artifact_roots_json) entry
			WHERE event.event_id=? AND json_extract(entry.value,'$.role')='produced'
		)
		SELECT
			(SELECT COUNT(*) FROM expected),
			(SELECT COUNT(*) FROM artifact_provenance
				WHERE producer_event_id=?),
			(SELECT COUNT(*) FROM artifact_provenance provenance
				JOIN expected ON expected.root_digest=provenance.root_digest
				WHERE provenance.producer_event_id=?
					AND provenance.relation='local_capture'
					AND provenance.operation_id IS NOT NULL),
			(SELECT COUNT(DISTINCT operation_id) FROM artifact_provenance
				WHERE producer_event_id=?),
			(SELECT MIN(operation_id) FROM artifact_provenance
				WHERE producer_event_id=?)`,
		eventID.String(), eventID.String(), eventID.String(), eventID.String(),
		eventID.String()).Scan(&expected, &provenance, &exact, &operations, &operationID)
	if err != nil || provenance != expected || exact != expected {
		return false, errEventDisseminationInvariant
	}
	if expected == 0 {
		return true, nil
	}
	if operations != 1 || !operationID.Valid {
		return false, errEventDisseminationInvariant
	}
	return currentLocalOperationArtifactPublicationReady(ctx, tx, eventID,
		operationID.String)
}

func currentLocalOperationArtifactPublicationReady(ctx context.Context, tx *sql.Tx,
	eventID model.EventID, operationID string,
) (bool, error) {
	var operationStatus, stageState string
	var capture, captureDigest []byte
	var exactProjection int
	err := tx.QueryRowContext(ctx, `
		WITH expected(root_digest) AS (
			SELECT json_extract(entry.value,'$.root_digest')
			FROM events event,json_each(event.artifact_roots_json) entry
			WHERE event.event_id=? AND json_extract(entry.value,'$.role')='produced'
		)
		SELECT operation.status,operation.capture_json,stage.state,stage.capture_digest,
			NOT EXISTS (
				SELECT 1 FROM expected
				WHERE NOT EXISTS (
					SELECT 1 FROM operation_artifact_roots projection
					WHERE projection.operation_id=operation.operation_id
						AND projection.root_digest=expected.root_digest
				)
			)
			AND NOT EXISTS (
				SELECT 1 FROM operation_artifact_roots projection
				WHERE projection.operation_id=operation.operation_id
					AND NOT EXISTS (
						SELECT 1 FROM expected
						WHERE expected.root_digest=projection.root_digest
					)
			)
		FROM operations operation
		JOIN operation_artifact_stages stage
			ON stage.operation_id=operation.operation_id
			AND stage.generation=(
				SELECT MAX(current.generation) FROM operation_artifact_stages current
				WHERE current.operation_id=operation.operation_id
			)
		WHERE operation.operation_id=?`,
		eventID.String(), operationID).Scan(&operationStatus, &capture, &stageState,
		&captureDigest, &exactProjection)
	if err != nil || operationStatus != string(model.OperationCommitted) ||
		len(capture) == 0 || len(captureDigest) != sha256.Size ||
		!bytes.Equal(model.Sum(capture).Bytes(), captureDigest) || exactProjection != 1 {
		return false, errEventDisseminationInvariant
	}
	switch ArtifactStageState(stageState) {
	case ArtifactStagePublishing:
		return false, nil
	case ArtifactStageReady:
		return true, nil
	default:
		return false, errEventDisseminationInvariant
	}
}
