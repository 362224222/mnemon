package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func insertPublicationEvidenceDisposition(ctx context.Context, tx *sql.Tx, event model.Event,
	deliveryStatus, deliveryError string,
) error {
	return insertPublicationEvidenceDispositionWithAuthority(ctx, tx, event,
		deliveryStatus, deliveryError, model.Operation{},
		acceptanceArtifactAuthority{})
}

func insertPublicationEvidenceDispositionWithAuthority(ctx context.Context,
	tx *sql.Tx, event model.Event, deliveryStatus, deliveryError string,
	operation model.Operation, authority acceptanceArtifactAuthority,
) error {
	if (deliveryStatus != "pending" && deliveryStatus != "blocked") ||
		(deliveryStatus == "pending") != (deliveryError == "") {
		return errors.New("insert publication evidence: invalid delivery disposition")
	}
	now := storeTime(event.AcceptedAt())
	scope := event.Scope()
	_, err := tx.ExecContext(ctx, `INSERT INTO gossip_publications(event_id,channel_id,origin_peer_id,
		origin_epoch,channel_seq,status,attempts,next_attempt_at,created_at,updated_at)
		VALUES(?,?,?,?,?,'queued',0,?,?,?)`, event.ID().String(), scope.ChannelID().String(),
		scope.OriginPeerID().String(), scope.OriginEpoch().String(), scope.ChannelSequence(), now, now, now)
	if err != nil {
		return fmt.Errorf("insert Gossip publication: %w", err)
	}
	for _, ref := range event.Artifacts() {
		err := insertAcceptanceArtifactOwnerPin(ctx, tx, operation, authority,
			ref, "publication", event.ID().String(), event.AcceptedAt())
		if err != nil {
			return err
		}
	}
	for _, target := range event.Audience().Peers() {
		if err := insertPublicationDelivery(ctx, tx, event, target, deliveryStatus, deliveryError,
			now, operation, authority); err != nil {
			return err
		}
	}
	return nil
}

func insertPublicationDelivery(ctx context.Context, tx *sql.Tx, event model.Event,
	target model.PeerID, status, diagnostic, at string, operation model.Operation,
	authority acceptanceArtifactAuthority,
) error {
	deliveryID := deterministicDeliveryID(event.ID(), target)
	_, err := tx.ExecContext(ctx, `INSERT INTO peer_deliveries(delivery_id,channel_id,target_peer_id,
		event_id,status,last_error,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)`, deliveryID,
		event.Scope().ChannelID().String(), target.String(), event.ID().String(), status,
		nullText(diagnostic), at, at)
	if err != nil {
		return fmt.Errorf("insert Peer delivery for %s: %w", target.String(), err)
	}
	for _, ref := range event.Artifacts() {
		err := insertAcceptanceArtifactOwnerPin(ctx, tx, operation, authority,
			ref, "delivery", deliveryID, event.AcceptedAt())
		if err != nil {
			return err
		}
	}
	return nil
}

func insertAcceptanceArtifactOwnerPin(ctx context.Context, tx *sql.Tx,
	operation model.Operation, authority acceptanceArtifactAuthority,
	ref model.ArtifactRef, kind, owner string, at time.Time,
) error {
	if ref.Role() == model.ArtifactProduced {
		return insertAcceptedArtifactOwnerPin(ctx, tx, authority, operation,
			ref.RootDigest(), kind, owner, at)
	}
	return insertArtifactOwnerPin(ctx, tx, ref.RootDigest(), kind, owner, at)
}
