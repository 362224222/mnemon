package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// ClaimGossipPublication recovers expired leases and claims the oldest due
// queued row for one active, joined Channel. Recovery and claim occur in one
// transaction, so restart never exposes a row with two owners or generations.
func (s *Store) ClaimGossipPublication(ctx context.Context,
	spec GossipPublicationClaimSpec,
) (GossipPublicationClaimResult, error) {
	at, leaseUntil, err := validateGossipPublicationClaimInput(s, ctx, spec)
	if err != nil {
		return GossipPublicationClaimResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GossipPublicationClaimResult{}, fmt.Errorf("claim Gossip publication: begin: %w", err)
	}
	defer tx.Rollback()
	node, authority, err := requireGossipPublicationClaimAuthority(ctx, tx, spec.ChannelID)
	if err != nil {
		return GossipPublicationClaimResult{}, err
	}
	if err := recoverExpiredGossipPublicationLeases(ctx, tx, spec.ChannelID, at); err != nil {
		return GossipPublicationClaimResult{}, err
	}
	eventID, found, err := selectDueGossipPublication(ctx, tx, spec.ChannelID, at)
	if err != nil {
		return GossipPublicationClaimResult{}, err
	}
	if !found {
		return commitEmptyGossipPublicationClaim(tx, "commit recovery")
	}
	ready, err := eventDisseminationReady(ctx, tx, eventID)
	if err != nil {
		return GossipPublicationClaimResult{}, fmt.Errorf("%w: Event readiness: %v",
			ErrGossipPublicationInvariant, err)
	}
	if !ready {
		return commitEmptyGossipPublicationClaim(tx, "commit blocked recovery")
	}
	claimed, err := claimQueuedGossipPublication(ctx, tx, node, authority, eventID,
		spec, at, leaseUntil)
	if err != nil {
		return GossipPublicationClaimResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return GossipPublicationClaimResult{}, fmt.Errorf("claim Gossip publication: commit: %w", err)
	}
	return GossipPublicationClaimResult{Claimed: true,
		Lease: GossipPublicationLease{Record: claimed.Record, Fence: claimed.Fence}}, nil
}

func validateGossipPublicationClaimInput(s *Store, ctx context.Context,
	spec GossipPublicationClaimSpec,
) (time.Time, time.Time, error) {
	if s == nil || s.db == nil || ctx == nil || spec.ChannelID.IsZero() ||
		!validPublicationIdentifier(spec.LeaseOwner) {
		return time.Time{}, time.Time{}, ErrGossipPublicationInput
	}
	at, err := canonicalStoreTime(spec.At)
	if err != nil {
		return time.Time{}, time.Time{},
			fmt.Errorf("%w: claim time: %v", ErrGossipPublicationInput, err)
	}
	leaseUntil, err := canonicalStoreTime(spec.LeaseUntil)
	if err != nil || !leaseUntil.After(at) || leaseUntil.Sub(at) > maxGossipPublicationLease {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: lease must end within %s after claim time",
			ErrGossipPublicationInput, maxGossipPublicationLease)
	}
	return at, leaseUntil, nil
}

func requireGossipPublicationClaimAuthority(ctx context.Context, tx *sql.Tx,
	channelID model.ChannelID,
) (model.Node, verifiedChannelAuthority, error) {
	node, authority, err := readGossipPublicationAuthority(ctx, tx, channelID)
	if err != nil {
		return model.Node{}, verifiedChannelAuthority{}, err
	}
	if authority.channel.Status() != model.ChannelActive ||
		authority.channel.TopicState() != model.TopicJoined {
		return model.Node{}, verifiedChannelAuthority{}, ErrGossipPublicationAuthority
	}
	return node, authority, nil
}

// recoverExpiredGossipPublicationLeases retires expired capabilities before
// selection. attempts already counted the crashed invocation, so recovery
// preserves it and makes the row due at its old lease boundary.
func recoverExpiredGossipPublicationLeases(ctx context.Context, tx *sql.Tx,
	channelID model.ChannelID, at time.Time,
) error {
	if _, err := tx.ExecContext(ctx, `UPDATE gossip_publications SET status='queued',
		lease_owner=NULL,lease_until=NULL,next_attempt_at=lease_until,
		last_error='publication lease expired',updated_at=?
		WHERE channel_id=? AND status='leased' AND lease_until<=?`, storeTime(at),
		channelID.String(), storeTime(at)); err != nil {
		return fmt.Errorf("claim Gossip publication: recover expired leases: %w", err)
	}
	return nil
}

func selectDueGossipPublication(ctx context.Context, tx *sql.Tx,
	channelID model.ChannelID, at time.Time,
) (model.EventID, bool, error) {
	var eventText string
	err := tx.QueryRowContext(ctx, `SELECT event_id FROM gossip_publications
		WHERE channel_id=? AND status='queued' AND next_attempt_at<=?
		ORDER BY next_attempt_at,channel_seq,event_id LIMIT 1`, channelID.String(),
		storeTime(at)).Scan(&eventText)
	if errors.Is(err, sql.ErrNoRows) {
		return model.EventID{}, false, nil
	}
	if err != nil {
		return model.EventID{}, false, fmt.Errorf("claim Gossip publication: select due row: %w", err)
	}
	eventID, err := model.ParseEventID(eventText)
	if err != nil {
		return model.EventID{}, false, fmt.Errorf("%w: Event ID: %v", ErrGossipPublicationInvariant, err)
	}
	return eventID, true, nil
}

func commitEmptyGossipPublicationClaim(tx *sql.Tx,
	step string,
) (GossipPublicationClaimResult, error) {
	if err := tx.Commit(); err != nil {
		return GossipPublicationClaimResult{}, fmt.Errorf("claim Gossip publication: %s: %w", step, err)
	}
	return GossipPublicationClaimResult{}, nil
}

func claimQueuedGossipPublication(ctx context.Context, tx *sql.Tx, node model.Node,
	authority verifiedChannelAuthority, eventID model.EventID, spec GossipPublicationClaimSpec,
	at, leaseUntil time.Time,
) (storedGossipPublication, error) {
	queuedState, err := readGossipPublication(ctx, tx, eventID)
	if err != nil {
		return storedGossipPublication{}, err
	}
	queued := queuedState.Record
	if queued.Status() != model.PublicationQueued || queued.Attempts() == math.MaxUint32 ||
		queued.NextAttemptAt().After(at) || at.Before(queued.UpdatedAt()) {
		return storedGossipPublication{}, ErrGossipPublicationInvariant
	}
	if err := validateGossipPublicationAuthority(node, authority, queued.Publication()); err != nil {
		return storedGossipPublication{}, err
	}
	nextAttempt := queued.Attempts() + 1
	fence := GossipPublicationFence{EventID: eventID, ChannelID: spec.ChannelID,
		LeaseOwner: spec.LeaseOwner, Attempt: nextAttempt, LeaseUntil: leaseUntil,
		RosterHead: authority.channel.RosterHead()}
	fenceJSON, err := canonicalGossipPublicationFence(fence)
	if err != nil {
		return storedGossipPublication{}, err
	}
	mutation, err := tx.ExecContext(ctx, `UPDATE gossip_publications SET status='leased',attempts=?,
		lease_owner=?,lease_until=?,lease_fence_json=?,published_at=NULL,last_error=NULL,updated_at=?
		WHERE event_id=? AND channel_id=? AND status='queued' AND attempts=? AND next_attempt_at<=?`,
		nextAttempt, spec.LeaseOwner, storeTime(leaseUntil), fenceJSON.Bytes(), storeTime(at), eventID.String(),
		spec.ChannelID.String(), queued.Attempts(), storeTime(at))
	if err != nil {
		return storedGossipPublication{}, fmt.Errorf("claim Gossip publication: claim row: %w", err)
	}
	if exactlyOne(mutation) != nil {
		return storedGossipPublication{}, ErrGossipPublicationStale
	}
	claimed, err := readGossipPublication(ctx, tx, eventID)
	if err != nil {
		return storedGossipPublication{}, err
	}
	if !claimed.HasFence || !bytes.Equal(claimed.FenceJSON.Bytes(), fenceJSON.Bytes()) {
		return storedGossipPublication{}, ErrGossipPublicationInvariant
	}
	return claimed, nil
}
