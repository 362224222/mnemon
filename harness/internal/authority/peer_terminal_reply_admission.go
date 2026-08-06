package authority

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

// hasOpenTerminalReplyAnchorTx corroborates a signed terminal candidate
// against authority frozen when the corresponding local Event was accepted.
// The peer cannot choose the route, root, local Principal, or open Handling;
// none of them is recovered from semantic content or historical ancestry.
func hasOpenTerminalReplyAnchorTx(ctx context.Context, tx *sql.Tx,
	verified agency.VerifiedPeerDelivery, inboundRoute agency.RouteID,
) (bool, error) {
	delivery := verified.Delivery()
	root, present := delivery.OriginCorrelation()
	if !present || inboundRoute.IsZero() || verified.LocalTarget().IsZero() {
		return false, nil
	}
	var matched int
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM peer_outbox outbound
		JOIN handlings anchor ON anchor.handling_id = outbound.reply_anchor_handling_id
		WHERE outbound.route_id = ?
		AND outbound.expected_reply_root_event_id = ?
		AND outbound.expected_reply_root_event_digest = ?
		AND outbound.reply_anchor_handling_id IS NOT NULL
		AND anchor.target_principal_id = ?
		AND anchor.state = 'open')`,
		inboundRoute.String(), root.ID().String(), root.Digest().String(),
		verified.LocalTarget().String()).Scan(&matched)
	if err != nil {
		return false, fmt.Errorf("admit terminal PeerDelivery: inspect reply binding: %w", err)
	}
	return matched == 1, nil
}
