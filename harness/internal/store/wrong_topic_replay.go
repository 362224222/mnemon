package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var ErrWrongTopicReplay = errors.New("wrong-topic replay evidence is unavailable")

type ChannelMutationCounts struct {
	Events uint64
	Works  uint64
}

type WrongTopicReplayCandidate struct {
	SourceChannelID      model.ChannelID
	TargetChannelID      model.ChannelID
	SourceChannelDigest  model.Digest
	TargetChannelDigest  model.Digest
	Publication          model.SignedPublication
	PublicationDigest    model.Digest
	EventKey             model.EventKey
	EventDigest          model.Digest
	TargetMutationCounts ChannelMutationCounts
}

func (s *Store) ReadWrongTopicReplayCandidate(ctx context.Context,
	sourceID, targetID model.ChannelID,
) (WrongTopicReplayCandidate, error) {
	if s == nil || s.db == nil || ctx == nil || sourceID.IsZero() || targetID.IsZero() ||
		sourceID == targetID {
		return WrongTopicReplayCandidate{}, ErrWrongTopicReplay
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return WrongTopicReplayCandidate{}, fmt.Errorf("%w: begin: %v", ErrWrongTopicReplay, err)
	}
	defer tx.Rollback()
	node, err := readNode(ctx, tx)
	if err != nil {
		return WrongTopicReplayCandidate{}, fmt.Errorf("%w: read Node: %v", ErrWrongTopicReplay, err)
	}
	sourceControl, err := readChannelControlChannel(ctx, tx, node.PeerID(), sourceID)
	if err != nil {
		return WrongTopicReplayCandidate{}, fmt.Errorf("%w: source Channel: %v", ErrWrongTopicReplay, err)
	}
	targetControl, err := readChannelControlChannel(ctx, tx, node.PeerID(), targetID)
	if err != nil {
		return WrongTopicReplayCandidate{}, fmt.Errorf("%w: target Channel: %v", ErrWrongTopicReplay, err)
	}
	source, err := wrongTopicReplayStatusChannel(sourceControl)
	if err != nil {
		return WrongTopicReplayCandidate{}, err
	}
	target, err := wrongTopicReplayStatusChannel(targetControl)
	if err != nil {
		return WrongTopicReplayCandidate{}, err
	}
	publication, evidence, err := readWrongTopicReplaySourcePublication(ctx, tx, node.PeerID(), source)
	if err != nil {
		return WrongTopicReplayCandidate{}, err
	}
	counts, err := readChannelMutationCountsTx(ctx, tx, targetID)
	if err != nil {
		return WrongTopicReplayCandidate{}, err
	}
	if err := tx.Commit(); err != nil {
		return WrongTopicReplayCandidate{}, fmt.Errorf("%w: commit read: %v", ErrWrongTopicReplay, err)
	}
	return WrongTopicReplayCandidate{SourceChannelID: sourceID, TargetChannelID: targetID,
		SourceChannelDigest: source.ChannelIDDigest(), TargetChannelDigest: target.ChannelIDDigest(),
		Publication: publication, PublicationDigest: evidence.PublicationDigest(),
		EventKey: evidence.EventKey(), EventDigest: evidence.EventDigest(),
		TargetMutationCounts: counts}, nil
}

func (s *Store) ReadChannelMutationCounts(ctx context.Context,
	channelID model.ChannelID,
) (ChannelMutationCounts, error) {
	if s == nil || s.db == nil || ctx == nil || channelID.IsZero() {
		return ChannelMutationCounts{}, ErrWrongTopicReplay
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ChannelMutationCounts{}, fmt.Errorf("%w: begin: %v", ErrWrongTopicReplay, err)
	}
	defer tx.Rollback()
	node, err := readNode(ctx, tx)
	if err != nil {
		return ChannelMutationCounts{}, fmt.Errorf("%w: read Node: %v", ErrWrongTopicReplay, err)
	}
	if _, err := readChannelControlChannel(ctx, tx, node.PeerID(), channelID); err != nil {
		return ChannelMutationCounts{}, fmt.Errorf("%w: Channel: %v", ErrWrongTopicReplay, err)
	}
	counts, err := readChannelMutationCountsTx(ctx, tx, channelID)
	if err != nil {
		return ChannelMutationCounts{}, err
	}
	if err := tx.Commit(); err != nil {
		return ChannelMutationCounts{}, fmt.Errorf("%w: commit read: %v", ErrWrongTopicReplay, err)
	}
	return counts, nil
}

func wrongTopicReplayStatusChannel(control ChannelControlChannel) (ChannelStatusChannel, error) {
	channel := control.Channel()
	members := control.Roster().Members()
	if channel.ID().IsZero() || len(members) == 0 ||
		members[len(members)-1].Head() != channel.RosterHead() {
		return ChannelStatusChannel{}, fmt.Errorf("%w: Channel status projection is incomplete",
			ErrWrongTopicReplay)
	}
	head := members[len(members)-1]
	return ChannelStatusChannel{control: control,
		channelIDDigest: model.Sum([]byte(channel.ID().String())),
		rosterHead: ChannelStatusRosterHead{recordHead: head.Head(),
			ownerPeerID: channel.OwnerPeerID(), ownerSignature: head.OwnerSignature()}}, nil
}

func readWrongTopicReplaySourcePublication(ctx context.Context, tx *sql.Tx,
	local model.PeerID, source ChannelStatusChannel,
) (model.SignedPublication, ChannelStatusPublication, error) {
	rows, err := tx.QueryContext(ctx, `SELECT 'local' AS evidence_kind,e.channel_id,
		e.origin_peer_id,e.origin_peer_id AS transport_peer_id,e.origin_epoch,e.origin_seq,
		e.channel_seq,e.event_id,e.event_digest,e.publication_digest,
		e.canonical_publication_json AS publication_bytes,e.origin_signature,
		'local' AS arrival_source,1 AS is_audience,'originated' AS semantic_outcome,
		NULL AS artifact_source_peer_id
		FROM events e WHERE e.source='local' AND e.channel_id=?
		ORDER BY e.channel_seq,e.event_id LIMIT 1`, source.Channel().ID().String())
	if err != nil {
		return model.SignedPublication{}, ChannelStatusPublication{},
			fmt.Errorf("%w: read source publication: %v", ErrWrongTopicReplay, err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return model.SignedPublication{}, ChannelStatusPublication{},
				fmt.Errorf("%w: iterate source publication: %v", ErrWrongTopicReplay, err)
		}
		return model.SignedPublication{}, ChannelStatusPublication{},
			fmt.Errorf("%w: source Channel has no local publication", ErrWrongTopicReplay)
	}
	row, err := scanChannelStatusPublicationRow(rows)
	if err != nil {
		return model.SignedPublication{}, ChannelStatusPublication{}, err
	}
	if row.channelID != source.Channel().ID() || row.originPeerID != local {
		return model.SignedPublication{}, ChannelStatusPublication{},
			fmt.Errorf("%w: source publication is not local to the source Channel",
				ErrWrongTopicReplay)
	}
	signed, err := decodeChannelStatusSignedPublication(row)
	if err != nil {
		return model.SignedPublication{}, ChannelStatusPublication{}, err
	}
	if err := verifyChannelStatusPublication(source, signed); err != nil {
		return model.SignedPublication{}, ChannelStatusPublication{}, err
	}
	arrival, outcome, ignored, err := projectChannelStatusPublicationPath(row, signed.Event(), local)
	if err != nil {
		return model.SignedPublication{}, ChannelStatusPublication{}, err
	}
	evidence, err := projectChannelStatusPublication(row, source, signed, arrival, outcome, ignored)
	if err != nil {
		return model.SignedPublication{}, ChannelStatusPublication{}, err
	}
	if rows.Next() {
		return model.SignedPublication{}, ChannelStatusPublication{},
			fmt.Errorf("%w: source publication query exceeded its bound", ErrWrongTopicReplay)
	}
	if err := rows.Err(); err != nil {
		return model.SignedPublication{}, ChannelStatusPublication{},
			fmt.Errorf("%w: iterate source publication: %v", ErrWrongTopicReplay, err)
	}
	return signed, evidence, nil
}

func readChannelMutationCountsTx(ctx context.Context, tx *sql.Tx,
	channelID model.ChannelID,
) (ChannelMutationCounts, error) {
	var events, works int64
	if err := tx.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM events WHERE channel_id=?),
		(SELECT COUNT(*) FROM works WHERE channel_id=?)`,
		channelID.String(), channelID.String()).Scan(&events, &works); err != nil {
		return ChannelMutationCounts{}, fmt.Errorf("%w: read target mutation counts: %v",
			ErrWrongTopicReplay, err)
	}
	if events < 0 || works < 0 {
		return ChannelMutationCounts{}, fmt.Errorf("%w: negative target mutation counts",
			ErrWrongTopicReplay)
	}
	return ChannelMutationCounts{Events: uint64(events), Works: uint64(works)}, nil
}
