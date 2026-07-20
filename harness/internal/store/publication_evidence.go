package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func insertPublicationEvidenceDisposition(ctx context.Context, tx *sql.Tx, event model.Event,
	deliveryStatus, deliveryError string,
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
		if err := insertArtifactOwnerPin(ctx, tx, ref.RootDigest(), "publication", event.ID().String(),
			event.AcceptedAt()); err != nil {
			return err
		}
	}
	for _, target := range event.Audience().Peers() {
		if err := insertPublicationDelivery(ctx, tx, event, target, deliveryStatus, deliveryError,
			now); err != nil {
			return err
		}
	}
	return nil
}

func insertPublicationDelivery(ctx context.Context, tx *sql.Tx, event model.Event,
	target model.PeerID, status, diagnostic, at string,
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
		if err := insertArtifactOwnerPin(ctx, tx, ref.RootDigest(), "delivery", deliveryID,
			event.AcceptedAt()); err != nil {
			return err
		}
	}
	return nil
}
