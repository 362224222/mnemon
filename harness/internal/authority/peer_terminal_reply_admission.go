package authority

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

// hasOpenTerminalReplyAnchorTx locally corroborates signed origin structure.
// A peer cannot obtain no-reply projection authority merely by labelling a
// delivery as terminal: its exact correlation must already be the machine-
// derived root of an open local responsibility created by an Event that was
// actually delivered over this same enrolled route. Imported reply integration
// Handlings can therefore never bootstrap an unbounded reply chain.
func hasOpenTerminalReplyAnchorTx(ctx context.Context, tx *sql.Tx,
	verified agency.VerifiedPeerDelivery, inboundRoute agency.RouteID,
) (bool, error) {
	delivery := verified.Delivery()
	correlation, present := delivery.OriginCorrelation()
	if !present || inboundRoute.IsZero() {
		return false, nil
	}
	anchors, err := loadOpenTerminalReplyAnchorsTx(ctx, tx, verified.LocalTarget())
	if err != nil {
		return false, err
	}
	for _, anchor := range anchors {
		matched, err := terminalReplyAnchorMatchesTx(ctx, tx, anchor, correlation, inboundRoute)
		if err != nil || matched {
			return matched, err
		}
	}
	return false, nil
}

type terminalReplyAnchor struct {
	handling agency.HandlingID
	head     agency.EventRef
}

func loadOpenTerminalReplyAnchorsTx(ctx context.Context, tx *sql.Tx,
	principal agency.AgentPrincipalID,
) ([]terminalReplyAnchor, error) {
	rows, err := tx.QueryContext(ctx, `SELECT handling_id, head_event_id FROM handlings
		WHERE target_principal_id = ? AND state = 'open'
		ORDER BY created_sequence, handling_id LIMIT ?`, principal.String(),
		MaxOpenHandlingsPerPrincipal+1)
	if err != nil {
		return nil, fmt.Errorf("admit terminal PeerDelivery: load open Handlings: %w", err)
	}
	defer rows.Close()
	open := make([]terminalReplyAnchor, 0, MaxOpenHandlingsPerPrincipal)
	for rows.Next() {
		var handlingValue, headValue string
		if err := rows.Scan(&handlingValue, &headValue); err != nil {
			return nil, fmt.Errorf("admit terminal PeerDelivery: scan open Handling: %w", err)
		}
		handling, err := agency.NewHandlingID(handlingValue)
		if err != nil {
			return nil, errors.New("admit terminal PeerDelivery: corrupt Handling ID")
		}
		head, _, _, _, err := loadStoredEventTx(ctx, tx, headValue)
		if err != nil {
			return nil, err
		}
		open = append(open, terminalReplyAnchor{handling: handling, head: head})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("admit terminal PeerDelivery: iterate open Handlings: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("admit terminal PeerDelivery: close open Handlings: %w", err)
	}
	if len(open) > MaxOpenHandlingsPerPrincipal {
		return nil, errors.New("admit terminal PeerDelivery: open Handling bound violated")
	}
	return open, nil
}

func terminalReplyAnchorMatchesTx(ctx context.Context, tx *sql.Tx,
	anchor terminalReplyAnchor, correlation agency.EventRef, inboundRoute agency.RouteID,
) (bool, error) {
	creation, err := handlingCreationEventTx(ctx, tx, anchor.handling, anchor.head)
	if err != nil {
		return false, err
	}
	_, _, imported, err := peerDeliveryForLocalEventTx(ctx, tx, creation.ref)
	if err != nil || imported {
		return false, err
	}
	root := creation.ref
	if !creation.correlation.IsZero() {
		root = creation.correlation
	}
	if root != correlation {
		return false, nil
	}
	var delivered int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM peer_outbox WHERE origin_event_id = ? AND route_id = ?)`,
		creation.ref.ID().String(), inboundRoute.String()).Scan(&delivered); err != nil {
		return false, fmt.Errorf("admit terminal PeerDelivery: inspect outbound anchor: %w", err)
	}
	return delivered == 1, nil
}
