package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type peerPullIndexedEvent struct {
	id       model.EventID
	sequence uint64
}

func readContinuousPeerPullPublications(ctx context.Context, tx *sql.Tx,
	state peerPullSourceState, after uint64, limit int,
) ([]model.SignedPublication, uint64, error) {
	if after == state.sourceHead {
		return []model.SignedPublication{}, after, nil
	}
	indexed, err := readPeerPullPublicationIndex(
		ctx, tx, state, after, limit)
	if err != nil {
		return nil, 0, err
	}
	publications, err := readPeerPullIndexedPublications(
		ctx, tx, state, indexed, after)
	if err != nil {
		return nil, 0, err
	}
	return publications, after + uint64(len(publications)), nil
}

func readPeerPullPublicationIndex(ctx context.Context, tx *sql.Tx,
	state peerPullSourceState, after uint64, limit int,
) ([]peerPullIndexedEvent, error) {
	rows, err := tx.QueryContext(ctx, `SELECT event_id,channel_seq FROM events
		WHERE channel_id=? AND origin_peer_id=? AND origin_epoch=? AND source='local'
		AND channel_seq>? AND channel_seq<=? ORDER BY channel_seq LIMIT ?`,
		state.authority.channel.ID().String(), state.node.PeerID().String(),
		state.node.OriginEpoch().String(), after, state.sourceHead, limit)
	if err != nil {
		return nil, fmt.Errorf("read Peer Pull page index: %w", err)
	}
	indexed, err := scanPeerPullPublicationIndex(rows, limit)
	if err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("read Peer Pull page index: close: %w", err)
	}
	return indexed, nil
}

func scanPeerPullPublicationIndex(rows *sql.Rows,
	limit int,
) ([]peerPullIndexedEvent, error) {
	indexed := make([]peerPullIndexedEvent, 0, limit)
	for rows.Next() {
		var eventText string
		var sequence uint64
		if err := rows.Scan(&eventText, &sequence); err != nil {
			return nil, fmt.Errorf("read Peer Pull page index: scan: %w", err)
		}
		eventID, err := model.ParseEventID(eventText)
		if err != nil || sequence == 0 || sequence > model.MaxSQLiteInteger {
			return nil, fmt.Errorf("%w: invalid indexed Event identity",
				ErrPeerPullInvariant)
		}
		indexed = append(indexed, peerPullIndexedEvent{
			id: eventID, sequence: sequence,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read Peer Pull page index: iterate: %w", err)
	}
	return indexed, nil
}

func readPeerPullIndexedPublications(ctx context.Context, tx *sql.Tx,
	state peerPullSourceState, indexed []peerPullIndexedEvent, after uint64,
) ([]model.SignedPublication, error) {
	publications := make([]model.SignedPublication, 0, len(indexed))
	totalBytes := 0
	for index, item := range indexed {
		expected := after + uint64(index) + 1
		if item.sequence != expected {
			return nil, fmt.Errorf("%w: source sequence %d is missing before %d",
				ErrPeerPullInvariant, expected, item.sequence)
		}
		publication, err := readPeerPullIndexedPublication(
			ctx, tx, state, item)
		if err != nil {
			return nil, err
		}
		rawBytes := len(publication.WireJSON().Bytes())
		if totalBytes > maxPeerPullPageBytes-maxPeerPullPageEnvelopeBytes-rawBytes {
			break
		}
		totalBytes += rawBytes
		publications = append(publications, publication)
	}
	if len(publications) == 0 {
		return nil, fmt.Errorf(
			"%w: source head has no bounded next publication",
			ErrPeerPullInvariant)
	}
	return publications, nil
}

func readPeerPullIndexedPublication(ctx context.Context, tx *sql.Tx,
	state peerPullSourceState, item peerPullIndexedEvent,
) (model.SignedPublication, error) {
	ready, err := eventDisseminationReady(ctx, tx, item.id)
	if err != nil {
		return model.SignedPublication{}, fmt.Errorf(
			"%w: publication sequence %d readiness: %v",
			ErrPeerPullInvariant, item.sequence, err)
	}
	if !ready {
		return model.SignedPublication{}, ErrPeerPullPublicationPending
	}
	stored, err := readGossipPublication(ctx, tx, item.id)
	if err != nil {
		return model.SignedPublication{}, fmt.Errorf(
			"%w: publication sequence %d: %v",
			ErrPeerPullInvariant, item.sequence, err)
	}
	publication := stored.Record.Publication()
	if publication.Key().ChannelSequence() != item.sequence {
		return model.SignedPublication{}, fmt.Errorf(
			"%w: publication sequence projection mismatch",
			ErrPeerPullInvariant)
	}
	if err := validateGossipPublicationAuthority(
		state.node, state.authority, publication); err != nil {
		return model.SignedPublication{}, fmt.Errorf(
			"%w: publication sequence %d: %v",
			ErrPeerPullInvariant, item.sequence, err)
	}
	return publication, nil
}
