package store

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const maxAcceptedArtifactPublishExamined = 256

// ScanAcceptedArtifactPublishesSpec bounds one read-only discovery pass.
// At is an observation boundary, not publication authority.
type ScanAcceptedArtifactPublishesSpec struct {
	At          time.Time
	MaxExamined int
	After       AcceptedArtifactPublishCursor
}

// AcceptedArtifactPublishCursor is an opaque, process-local keyset position.
// It is not durable authority and a zero value starts from the oldest row.
type AcceptedArtifactPublishCursor struct {
	updatedAt  time.Time
	kindOrder  uint8
	ownerID    string
	generation uint64
	valid      bool
}

// AcceptedArtifactPublishCandidate contains only the durable identity needed
// to invoke the exact Store reader for one accepted publishing obligation.
type AcceptedArtifactPublishCandidate struct {
	kind           artifactdomain.StageOwnerKind
	operationID    model.OperationID
	peerInboxFence PeerInboxArtifactFence
	peerInboxOwner artifactdomain.StageOwner
}

func (candidate AcceptedArtifactPublishCandidate) Kind() artifactdomain.StageOwnerKind {
	return candidate.kind
}

// OperationID is non-zero only when Kind is StageOwnerOperation.
func (candidate AcceptedArtifactPublishCandidate) OperationID() model.OperationID {
	return candidate.operationID
}

// PeerInboxFence is non-zero only when Kind is StageOwnerInbox.
func (candidate AcceptedArtifactPublishCandidate) PeerInboxFence() PeerInboxArtifactFence {
	return candidate.peerInboxFence
}

// Owner is non-zero only when Kind is StageOwnerInbox.
func (candidate AcceptedArtifactPublishCandidate) Owner() artifactdomain.StageOwner {
	return candidate.peerInboxOwner
}

// AcceptedArtifactPublishPage reports the identifier rows examined by one
// bounded discovery pass.
type AcceptedArtifactPublishPage struct {
	examined   int
	candidates []AcceptedArtifactPublishCandidate
	cursor     AcceptedArtifactPublishCursor
}

func (page AcceptedArtifactPublishPage) Examined() int { return page.examined }

func (page AcceptedArtifactPublishPage) Candidates() []AcceptedArtifactPublishCandidate {
	return append([]AcceptedArtifactPublishCandidate(nil), page.candidates...)
}

func (page AcceptedArtifactPublishPage) Cursor() AcceptedArtifactPublishCursor {
	return page.cursor
}

// ScanAcceptedArtifactPublishes enumerates bounded identity-only candidates.
// The candidate queries deliberately perform only a coarse accepted-state
// filter; callers must use ReadCommittedOperationArtifactPublish or
// ReadPeerInboxArtifactPublish before touching the filesystem.
func (s *Store) ScanAcceptedArtifactPublishes(ctx context.Context,
	spec ScanAcceptedArtifactPublishesSpec,
) (AcceptedArtifactPublishPage, error) {
	at, err := validateAcceptedArtifactPublishScan(s, ctx, spec)
	if err != nil {
		return AcceptedArtifactPublishPage{}, err
	}
	query := `SELECT owner_kind,owner_id,generation,
		lease_owner,lease_until,attempt,updated_at,kind_order FROM (
			SELECT 'operation' AS owner_kind,stage.operation_id AS owner_id,
				0 AS generation,'' AS lease_owner,'' AS lease_until,0 AS attempt,
				stage.updated_at AS updated_at,0 AS kind_order
			FROM operation_artifact_stages stage
			JOIN operations operation ON operation.operation_id=stage.operation_id
			WHERE operation.status='committed' AND stage.state='publishing'
				AND stage.cleanup_started_at IS NULL AND stage.updated_at<=?
			UNION ALL
			SELECT 'inbox' AS owner_kind,stage.inbox_id AS owner_id,
				stage.generation,stage.lease_owner,stage.lease_until,stage.attempt,
				stage.updated_at,1 AS kind_order
			FROM peer_inbox_artifact_stages stage
			WHERE stage.state='publishing' AND stage.cleanup_started_at IS NULL
				AND stage.updated_at<=? AND EXISTS (
					SELECT 1 FROM artifact_pins pin
					WHERE pin.owner_kind='inbox' AND pin.owner_id=stage.inbox_id
						AND pin.expires_at IS NULL AND pin.created_at<=?
				)
		) candidates`
	args := []any{storeTime(at), storeTime(at), storeTime(at)}
	if spec.After.valid {
		query += ` WHERE updated_at>? OR (
			updated_at=? AND (kind_order>? OR (
				kind_order=? AND (owner_id>? OR (
					owner_id=? AND generation>?
				))
			))
		)`
		args = append(args, storeTime(spec.After.updatedAt), storeTime(spec.After.updatedAt),
			spec.After.kindOrder, spec.After.kindOrder, spec.After.ownerID,
			spec.After.ownerID, spec.After.generation)
	}
	query += ` ORDER BY updated_at,kind_order,owner_id,generation LIMIT ?`
	args = append(args, spec.MaxExamined)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return AcceptedArtifactPublishPage{},
			fmt.Errorf("scan accepted Artifact publishes: query: %w", err)
	}
	defer rows.Close()

	candidates := make([]AcceptedArtifactPublishCandidate, 0, spec.MaxExamined)
	var cursor AcceptedArtifactPublishCursor
	for rows.Next() {
		candidate, position, err := scanAcceptedArtifactPublishCandidate(rows, at)
		if err != nil {
			return AcceptedArtifactPublishPage{}, err
		}
		candidates = append(candidates, candidate)
		cursor = position
	}
	if err := rows.Err(); err != nil {
		return AcceptedArtifactPublishPage{},
			fmt.Errorf("scan accepted Artifact publishes: iterate: %w", err)
	}
	return AcceptedArtifactPublishPage{
		examined: len(candidates), candidates: candidates, cursor: cursor,
	}, nil
}

func scanAcceptedArtifactPublishCandidate(rows *sql.Rows,
	at time.Time,
) (AcceptedArtifactPublishCandidate, AcceptedArtifactPublishCursor, error) {
	var kindText, ownerID, leaseOwner, leaseUntilText, updatedText string
	var generation, attempt int64
	var kindOrder uint8
	if err := rows.Scan(&kindText, &ownerID, &generation, &leaseOwner,
		&leaseUntilText, &attempt, &updatedText, &kindOrder); err != nil {
		return AcceptedArtifactPublishCandidate{}, AcceptedArtifactPublishCursor{},
			fmt.Errorf("scan accepted Artifact publishes: row: %w", err)
	}
	updatedAt, err := parseCanonicalStoreTime(updatedText)
	if err != nil || at.Before(updatedAt) {
		return AcceptedArtifactPublishCandidate{}, AcceptedArtifactPublishCursor{},
			ErrArtifactStageConflict
	}
	position := AcceptedArtifactPublishCursor{updatedAt: updatedAt, kindOrder: kindOrder,
		ownerID: ownerID, generation: uint64(generation), valid: true}
	kind := artifactdomain.StageOwnerKind(kindText)
	switch kind {
	case artifactdomain.StageOwnerOperation:
		if kindOrder != 0 || generation != 0 || attempt != 0 ||
			leaseOwner != "" || leaseUntilText != "" {
			return AcceptedArtifactPublishCandidate{}, AcceptedArtifactPublishCursor{},
				ErrArtifactStageConflict
		}
		operationID, err := model.ParseOperationID(ownerID)
		if err != nil {
			return AcceptedArtifactPublishCandidate{}, AcceptedArtifactPublishCursor{},
				ErrArtifactStageConflict
		}
		return AcceptedArtifactPublishCandidate{
			kind: kind, operationID: operationID,
		}, position, nil
	case artifactdomain.StageOwnerInbox:
		if kindOrder != 1 {
			return AcceptedArtifactPublishCandidate{}, AcceptedArtifactPublishCursor{},
				ErrArtifactStageConflict
		}
		candidate, err := scanAcceptedPeerInboxArtifactPublish(ownerID, leaseOwner,
			leaseUntilText, generation, attempt)
		return candidate, position, err
	default:
		return AcceptedArtifactPublishCandidate{}, AcceptedArtifactPublishCursor{},
			ErrArtifactStageConflict
	}
}

func scanAcceptedPeerInboxArtifactPublish(ownerID, leaseOwner, leaseUntilText string,
	generation, attempt int64,
) (AcceptedArtifactPublishCandidate, error) {
	if generation <= 0 || attempt <= 0 || attempt > math.MaxUint32 ||
		!validPublicationIdentifier(leaseOwner) {
		return AcceptedArtifactPublishCandidate{}, ErrArtifactStageConflict
	}
	inboxID, err := model.ParseInboxID(ownerID)
	if err != nil {
		return AcceptedArtifactPublishCandidate{}, ErrArtifactStageConflict
	}
	owner, err := artifactdomain.NewInboxStageOwner(inboxID, uint64(generation))
	if err != nil {
		return AcceptedArtifactPublishCandidate{}, ErrArtifactStageConflict
	}
	leaseUntil, err := parseCanonicalStoreTime(leaseUntilText)
	if err != nil {
		return AcceptedArtifactPublishCandidate{}, ErrArtifactStageConflict
	}
	return AcceptedArtifactPublishCandidate{
		kind: artifactdomain.StageOwnerInbox,
		peerInboxFence: PeerInboxArtifactFence{
			inboxID: inboxID, leaseOwner: leaseOwner,
			leaseUntil: leaseUntil, attempt: uint32(attempt),
		},
		peerInboxOwner: owner,
	}, nil
}

func validateAcceptedArtifactPublishScan(s *Store, ctx context.Context,
	spec ScanAcceptedArtifactPublishesSpec,
) (time.Time, error) {
	if s == nil || s.db == nil || ctx == nil || spec.MaxExamined <= 0 ||
		spec.MaxExamined > maxAcceptedArtifactPublishExamined {
		return time.Time{}, ErrArtifactStageConflict
	}
	at, err := canonicalStoreTime(spec.At)
	if err != nil || at != spec.At {
		return time.Time{}, ErrArtifactStageConflict
	}
	if spec.After.valid && !validAcceptedArtifactPublishCursor(spec.After, at) {
		return time.Time{}, ErrArtifactStageConflict
	}
	return at, nil
}

func validAcceptedArtifactPublishCursor(cursor AcceptedArtifactPublishCursor,
	at time.Time,
) bool {
	updatedAt, err := canonicalStoreTime(cursor.updatedAt)
	if err != nil || updatedAt != cursor.updatedAt || at.Before(updatedAt) ||
		cursor.kindOrder > 1 || cursor.ownerID == "" {
		return false
	}
	if cursor.kindOrder == 0 {
		if cursor.generation != 0 {
			return false
		}
		_, err := model.ParseOperationID(cursor.ownerID)
		return err == nil
	}
	if cursor.generation == 0 {
		return false
	}
	_, err = model.ParseInboxID(cursor.ownerID)
	return err == nil
}
