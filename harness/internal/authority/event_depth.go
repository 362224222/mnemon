package authority

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

// deriveLocalEventCausalDepthTx carries the greatest already-accepted Peer
// hop depth named by the request. Local transitions do not add a hop; only an
// outbound PeerDelivery does. A root with no accepted predecessor has depth
// zero.
func deriveLocalEventCausalDepthTx(ctx context.Context, tx *sql.Tx,
	request agency.BoundIntent,
) (uint16, error) {
	refs := make([]agency.EventRef, 0, len(request.Causation())+3)
	if subject, present := request.Subject(); present {
		refs = append(refs, subject.Head())
	}
	if expected, present := request.ExpectedReference(); present && !expected.IsAbsent() {
		refs = append(refs, expected.Head())
	}
	refs = append(refs, request.Causation()...)
	if correlation, present := request.Correlation(); present {
		refs = append(refs, correlation)
	}

	var maximum uint16
	seen := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		key := ref.ID().String() + "\x00" + ref.Digest().String()
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		depth, err := exactEventDepthTx(ctx, tx, ref)
		if err != nil {
			return 0, err
		}
		if depth > maximum {
			maximum = depth
		}
	}
	return maximum, nil
}

func exactEventDepthTx(ctx context.Context, tx *sql.Tx, ref agency.EventRef) (uint16, error) {
	var digestValue, sourceValue, requestValue, acceptedValue string
	var originSequence uint64
	var depth int64
	var canonical []byte
	err := tx.QueryRowContext(ctx, `SELECT event_digest, origin_sequence, causal_depth,
		source_principal_id, request_digest, accepted_at, canonical_json FROM events
		WHERE event_id = ?`, ref.ID().String()).Scan(&digestValue, &originSequence, &depth,
		&sourceValue, &requestValue, &acceptedValue, &canonical)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, errors.New("admit Intent: causal Event is unavailable")
	}
	if err != nil {
		return 0, fmt.Errorf("admit Intent: inspect causal Event: %w", err)
	}
	if depth < 0 || depth > agency.MaxPeerCausalDepth {
		return 0, errors.New("admit Intent: causal Event authority is corrupt")
	}
	storedRef, _, _, _, err := inspectStoredEvent(ref.ID().String(), digestValue, originSequence,
		uint16(depth), sourceValue, requestValue, acceptedValue, canonical)
	if err != nil || storedRef != ref {
		return 0, errors.New("admit Intent: causal Event authority is corrupt")
	}
	return uint16(depth), nil
}
