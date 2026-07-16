package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func advancePublicationHead(ctx context.Context, tx *sql.Tx, event model.Event, committedAt time.Time) error {
	scope := event.Scope()
	result, err := tx.ExecContext(ctx, `UPDATE publication_epochs SET source_head_channel_seq=?,updated_at=?
		WHERE channel_id=? AND origin_peer_id=? AND origin_epoch=? AND source_head_channel_seq=?`,
		scope.ChannelSequence(), storeTime(committedAt), scope.ChannelID().String(), scope.OriginPeerID().String(),
		scope.OriginEpoch().String(), scope.ChannelSequence()-1)
	if err != nil || exactlyOne(result) != nil {
		return fmt.Errorf("%w: publication head CAS failed", ErrAdmissionConflict)
	}
	return nil
}

func advanceNodeOriginSequence(ctx context.Context, tx *sql.Tx, scope LocalAdmissionScope, committedAt time.Time) error {
	next := scope.FirstOriginSequence() + uint64(scope.Count())
	result, err := tx.ExecContext(ctx, `UPDATE node SET next_origin_seq=?,updated_at=? WHERE singleton=1
		AND peer_id=? AND origin_epoch=? AND next_origin_seq=?`, next, storeTime(committedAt),
		scope.Node().PeerID().String(), scope.Node().OriginEpoch().String(), scope.FirstOriginSequence())
	if err != nil || exactlyOne(result) != nil {
		return fmt.Errorf("%w: origin sequence CAS failed", ErrAdmissionConflict)
	}
	return nil
}

func insertLocalProvenance(ctx context.Context, tx *sql.Tx, operation model.Operation, events []model.Event) error {
	run := operation.AgentRunID()
	for _, event := range events {
		for _, ref := range event.Artifacts() {
			if ref.Role() != model.ArtifactProduced {
				continue
			}
			operationID := operation.ID()
			provenance, err := model.NewArtifactProvenance(model.ArtifactProvenanceSpec{
				RootDigest: ref.RootDigest(), ProducerEvent: event.Key(), ProducerOriginPeer: event.Scope().OriginPeerID(),
				LocalAgentRun: &run, Operation: &operationID, Relation: model.ProvenanceLocalCapture,
				CreatedAt: event.AcceptedAt(),
			})
			if err != nil {
				return err
			}
			if _, err := insertArtifactProvenance(ctx, tx, provenance); err != nil {
				return err
			}
		}
	}
	return nil
}

func insertLocalDerivations(ctx context.Context, tx *sql.Tx, operation model.Operation,
	parent model.ReviewWork, derivations []model.WorkDerivation,
) error {
	for _, derivation := range derivations {
		if err := derivation.ValidateOperation(operation); err != nil || parent.Ref() != derivation.Parent() ||
			parent.ChannelID() != derivation.ParentChannelID() || parent.Version() != derivation.ParentVersion() ||
			parent.UpdatedBy() != derivation.ParentEventID() {
			return errors.New("commit local acceptance: derivation evidence mismatch")
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO work_derivations(operation_id,child_ordinal,
			child_channel_id,child_home_peer_id,child_work_id,parent_channel_id,parent_home_peer_id,
			parent_work_id,parent_version,parent_event_id,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`,
			derivation.OperationID().String(), derivation.ChildOrdinal(), derivation.ChildChannelID().String(),
			derivation.Child().HomePeerID().String(), derivation.Child().WorkID().String(),
			derivation.ParentChannelID().String(), derivation.Parent().HomePeerID().String(),
			derivation.Parent().WorkID().String(), derivation.ParentVersion(), derivation.ParentEventID().String(),
			storeTime(derivation.CreatedAt()))
		if err != nil {
			return fmt.Errorf("insert Work derivation: %w", err)
		}
	}
	return nil
}

func exactlyOne(result sql.Result) error {
	if result == nil {
		return errors.New("nil SQL result")
	}
	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		return errors.New("SQL mutation did not affect exactly one row")
	}
	return nil
}
