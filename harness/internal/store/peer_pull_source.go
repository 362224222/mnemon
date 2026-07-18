package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	maxPeerPullPagePublications = 32
	maxPeerPullPageBytes        = 1 << 20
	// Leave bounded room for the canonical PullPage fields, publication array
	// separators and outer Events envelope. The identifiers and three SQLite
	// sequences cannot consume this 4 KiB reserve under model bounds.
	maxPeerPullPageEnvelopeBytes = 4 << 10
)

var (
	ErrPeerPullInput         = errors.New("invalid Peer Pull source input")
	ErrPeerPullAuthority     = errors.New("Peer Pull source authority is unavailable")
	ErrPeerPullEpochMismatch = errors.New("Peer Pull source origin epoch mismatch")
	ErrPeerPullCursor        = errors.New("Peer Pull source cursor is invalid")
	ErrPeerPullHistoryGap    = errors.New("Peer Pull source history gap")
	ErrPeerPullInvariant     = errors.New("Peer Pull source durable invariant violated")
)

// PeerPullHistoryGap is a terminal, structured repair failure. SourceFloor is
// the first sequence the origin can still serve; callers must not silently
// continue there because doing so would reinterpret an incomplete history as
// a contiguous one.
type PeerPullHistoryGap struct {
	SourceFloor uint64
}

func (gap PeerPullHistoryGap) Error() string {
	return fmt.Sprintf("%s: source floor is %d", ErrPeerPullHistoryGap, gap.SourceFloor)
}

func (gap PeerPullHistoryGap) Unwrap() error { return ErrPeerPullHistoryGap }

type ReadPeerPullPageSpec struct {
	AuthenticatedPeerID  model.PeerID
	ChannelID            model.ChannelID
	OriginEpoch          model.OriginEpoch
	AfterChannelSequence uint64
	Limit                uint8
	At                   time.Time
}

// PeerPullPage contains the origin's exact immutable signed bytes. Scanned is
// the largest continuous sequence represented by this page, or the request's
// after cursor for an empty page. It never jumps over a missing durable row.
type PeerPullPage struct {
	Publications            []model.SignedPublication
	ScannedChannelSequence  uint64
	SourceFloor             uint64
	SourceHead              uint64
	OriginEpoch             model.OriginEpoch
	AcknowledgedSequence    uint64
	AcknowledgementAdvanced bool
}

type CommitPeerPullCursorAckSpec struct {
	AuthenticatedPeerID       model.PeerID
	ChannelID                 model.ChannelID
	OriginEpoch               model.OriginEpoch
	ContiguousChannelSequence uint64
	At                        time.Time
}

type CommitPeerPullCursorAckResult struct {
	AcknowledgedSequence uint64
	Advanced             bool
	Replayed             bool
}

type peerPullSourceState struct {
	node            model.Node
	authority       verifiedChannelAuthority
	sourceFloor     uint64
	sourceHead      uint64
	sourceUpdatedAt time.Time
	baseline        uint64
	acknowledged    uint64
	ackUpdatedAt    time.Time
	baselineAt      time.Time
}

// ReadPeerPullPage serves one authenticated origin-only repair request. The
// request's after cursor is also durable acknowledgement of all preceding
// receiver commits, so acknowledgement, delivery settlement and the source
// page are resolved in one SQLite snapshot.
func (s *Store) ReadPeerPullPage(ctx context.Context,
	spec ReadPeerPullPageSpec,
) (PeerPullPage, error) {
	at, err := validatePeerPullRequestInput(s, ctx, spec.AuthenticatedPeerID, spec.ChannelID,
		spec.OriginEpoch, spec.AfterChannelSequence, spec.At)
	if err != nil {
		return PeerPullPage{}, err
	}
	if spec.Limit == 0 || int(spec.Limit) > maxPeerPullPagePublications {
		return PeerPullPage{}, ErrPeerPullInput
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PeerPullPage{}, fmt.Errorf("read Peer Pull page: begin: %w", err)
	}
	defer tx.Rollback()
	state, err := readPeerPullSourceState(ctx, tx, spec.AuthenticatedPeerID, spec.ChannelID,
		spec.OriginEpoch)
	if err != nil {
		return PeerPullPage{}, err
	}
	if err := validatePeerPullCursor(state, spec.AfterChannelSequence, at); err != nil {
		return PeerPullPage{}, err
	}
	if spec.AfterChannelSequence < state.sourceFloor-1 {
		return PeerPullPage{}, PeerPullHistoryGap{SourceFloor: state.sourceFloor}
	}

	advanced, err := advancePeerPullAcknowledgement(ctx, tx, state, spec.AuthenticatedPeerID,
		spec.AfterChannelSequence, at)
	if err != nil {
		return PeerPullPage{}, err
	}
	publications, scanned, err := readContinuousPeerPullPublications(ctx, tx, state,
		spec.AfterChannelSequence, int(spec.Limit))
	if err != nil {
		return PeerPullPage{}, err
	}
	if err := tx.Commit(); err != nil {
		return PeerPullPage{}, fmt.Errorf("read Peer Pull page: commit: %w", err)
	}
	return PeerPullPage{Publications: publications, ScannedChannelSequence: scanned,
		SourceFloor: state.sourceFloor, SourceHead: state.sourceHead,
		OriginEpoch: state.node.OriginEpoch(), AcknowledgedSequence: spec.AfterChannelSequence,
		AcknowledgementAdvanced: advanced}, nil
}

// CommitPeerPullCursorAck durably commits an explicit authenticated cursor.
// An exact replay is read-only and retains the original updated_at evidence.
func (s *Store) CommitPeerPullCursorAck(ctx context.Context,
	spec CommitPeerPullCursorAckSpec,
) (CommitPeerPullCursorAckResult, error) {
	at, err := validatePeerPullRequestInput(s, ctx, spec.AuthenticatedPeerID, spec.ChannelID,
		spec.OriginEpoch, spec.ContiguousChannelSequence, spec.At)
	if err != nil {
		return CommitPeerPullCursorAckResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CommitPeerPullCursorAckResult{}, fmt.Errorf("commit Peer Pull cursor ACK: begin: %w", err)
	}
	defer tx.Rollback()
	state, err := readPeerPullSourceState(ctx, tx, spec.AuthenticatedPeerID, spec.ChannelID,
		spec.OriginEpoch)
	if err != nil {
		return CommitPeerPullCursorAckResult{}, err
	}
	if err := validatePeerPullCursor(state, spec.ContiguousChannelSequence, at); err != nil {
		return CommitPeerPullCursorAckResult{}, err
	}
	advanced, err := advancePeerPullAcknowledgement(ctx, tx, state, spec.AuthenticatedPeerID,
		spec.ContiguousChannelSequence, at)
	if err != nil {
		return CommitPeerPullCursorAckResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return CommitPeerPullCursorAckResult{}, fmt.Errorf("commit Peer Pull cursor ACK: commit: %w", err)
	}
	return CommitPeerPullCursorAckResult{AcknowledgedSequence: spec.ContiguousChannelSequence,
		Advanced: advanced, Replayed: !advanced}, nil
}

func validatePeerPullRequestInput(s *Store, ctx context.Context, authenticatedPeer model.PeerID,
	channelID model.ChannelID, originEpoch model.OriginEpoch, sequence uint64,
	atValue time.Time,
) (time.Time, error) {
	if s == nil || s.db == nil || ctx == nil || authenticatedPeer.IsZero() || channelID.IsZero() ||
		originEpoch.IsZero() || sequence > model.MaxSQLiteInteger {
		return time.Time{}, ErrPeerPullInput
	}
	at, err := canonicalStoreTime(atValue)
	if err != nil || at.IsZero() {
		return time.Time{}, ErrPeerPullInput
	}
	return at, nil
}

func readPeerPullSourceState(ctx context.Context, tx *sql.Tx, requester model.PeerID,
	channelID model.ChannelID, requestedEpoch model.OriginEpoch,
) (peerPullSourceState, error) {
	node, err := readNode(ctx, tx)
	if err != nil {
		return peerPullSourceState{}, fmt.Errorf("%w: Node: %v", ErrPeerPullAuthority, err)
	}
	if node.OriginEpoch() != requestedEpoch {
		return peerPullSourceState{}, ErrPeerPullEpochMismatch
	}
	authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), channelID)
	if err != nil {
		return peerPullSourceState{}, fmt.Errorf("%w: %v", ErrPeerPullAuthority, err)
	}
	if authority.channel.Status() != model.ChannelActive ||
		authority.channel.TopicState() != model.TopicJoined {
		return peerPullSourceState{}, ErrPeerPullAuthority
	}
	local, ok := authority.roster.CurrentMember(node.PeerID())
	if !ok || local.Status() != model.MemberActive || local.OriginEpoch() != node.OriginEpoch() {
		return peerPullSourceState{}, ErrPeerPullAuthority
	}
	var binding model.PeerBinding
	for _, candidate := range authority.bindings {
		if candidate.PeerID() == requester {
			binding = candidate
			break
		}
	}
	if binding.PeerID().IsZero() || binding.State() != model.BindingActive {
		return peerPullSourceState{}, ErrPeerPullAuthority
	}
	requesterMember, ok := authority.roster.CurrentMember(requester)
	if !ok || requesterMember.Status() != model.MemberActive ||
		requesterMember.OriginEpoch() != binding.OriginEpoch() ||
		requesterMember.Head() != binding.MemberHead() {
		return peerPullSourceState{}, ErrPeerPullAuthority
	}

	var floor, head uint64
	var epochUpdatedText string
	err = tx.QueryRowContext(ctx, `SELECT source_floor_channel_seq,source_head_channel_seq,updated_at
		FROM publication_epochs WHERE channel_id=? AND origin_peer_id=? AND origin_epoch=?`,
		channelID.String(), node.PeerID().String(), node.OriginEpoch().String()).
		Scan(&floor, &head, &epochUpdatedText)
	if err != nil {
		return peerPullSourceState{}, fmt.Errorf("read Peer Pull source epoch: %w", err)
	}
	epochUpdatedAt, err := parseCanonicalStoreTime(epochUpdatedText)
	if err != nil || floor == 0 || floor > model.MaxSQLiteInteger || head > model.MaxSQLiteInteger ||
		floor > head+1 || epochUpdatedAt.Before(authority.channel.CreatedAt()) {
		return peerPullSourceState{}, fmt.Errorf("%w: invalid publication epoch", ErrPeerPullInvariant)
	}

	var baseline, acknowledged uint64
	var confirmedText sql.NullString
	var ackUpdatedText string
	err = tx.QueryRowContext(ctx, `SELECT baseline_channel_seq,acknowledged_channel_seq,
		baseline_confirmed_at,updated_at FROM peer_pull_acks WHERE channel_id=? AND target_peer_id=?
		AND origin_peer_id=? AND origin_epoch=?`, channelID.String(), requester.String(),
		node.PeerID().String(), node.OriginEpoch().String()).
		Scan(&baseline, &acknowledged, &confirmedText, &ackUpdatedText)
	if errors.Is(err, sql.ErrNoRows) {
		return peerPullSourceState{}, ErrPeerPullAuthority
	}
	if err != nil {
		return peerPullSourceState{}, fmt.Errorf("read Peer Pull acknowledgement: %w", err)
	}
	ackUpdatedAt, parseErr := parseCanonicalStoreTime(ackUpdatedText)
	if !confirmedText.Valid {
		return peerPullSourceState{}, ErrPeerPullAuthority
	}
	if parseErr != nil || baseline > model.MaxSQLiteInteger ||
		acknowledged < baseline || acknowledged > head || ackUpdatedAt.Before(authority.channel.CreatedAt()) {
		return peerPullSourceState{}, fmt.Errorf("%w: invalid outbound acknowledgement", ErrPeerPullInvariant)
	}
	confirmedAt, parseErr := parseCanonicalStoreTime(confirmedText.String)
	if parseErr != nil || confirmedAt.Before(authority.channel.CreatedAt()) || confirmedAt.After(ackUpdatedAt) {
		return peerPullSourceState{}, fmt.Errorf("%w: invalid outbound baseline confirmation", ErrPeerPullInvariant)
	}
	return peerPullSourceState{node: node, authority: authority,
		sourceFloor: floor, sourceHead: head, baseline: baseline, acknowledged: acknowledged,
		sourceUpdatedAt: epochUpdatedAt, ackUpdatedAt: ackUpdatedAt, baselineAt: confirmedAt}, nil
}

func validatePeerPullCursor(state peerPullSourceState, sequence uint64, at time.Time) error {
	if sequence < state.baseline || sequence < state.acknowledged || sequence > state.sourceHead {
		return ErrPeerPullCursor
	}
	if at.Before(state.authority.channel.UpdatedAt()) || at.Before(state.ackUpdatedAt) ||
		at.Before(state.baselineAt) || at.Before(state.sourceUpdatedAt) {
		return ErrPeerPullInput
	}
	return nil
}

func advancePeerPullAcknowledgement(ctx context.Context, tx *sql.Tx, state peerPullSourceState,
	requester model.PeerID, sequence uint64, at time.Time,
) (bool, error) {
	if sequence == state.acknowledged {
		return false, nil
	}
	result, err := tx.ExecContext(ctx, `UPDATE peer_pull_acks SET acknowledged_channel_seq=?,updated_at=?
		WHERE channel_id=? AND target_peer_id=? AND origin_peer_id=? AND origin_epoch=?
		AND acknowledged_channel_seq=? AND baseline_channel_seq=? AND baseline_confirmed_at IS NOT NULL`,
		sequence, storeTime(at), state.authority.channel.ID().String(), requester.String(),
		state.node.PeerID().String(), state.node.OriginEpoch().String(), state.acknowledged, state.baseline)
	if err != nil {
		return false, fmt.Errorf("advance Peer Pull acknowledgement: update: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("advance Peer Pull acknowledgement: rows affected: %w", err)
	}
	if rows != 1 {
		return false, fmt.Errorf("%w: acknowledgement authority changed during transaction", ErrPeerPullInvariant)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE peer_deliveries SET status='scanned',scanned_at=?,
		last_error=NULL,updated_at=? WHERE channel_id=? AND target_peer_id=? AND status='pending'
		AND event_id IN (SELECT event_id FROM events WHERE channel_id=? AND origin_peer_id=?
		AND origin_epoch=? AND source='local' AND channel_seq<=?)`, storeTime(at), storeTime(at),
		state.authority.channel.ID().String(), requester.String(), state.authority.channel.ID().String(),
		state.node.PeerID().String(), state.node.OriginEpoch().String(), sequence); err != nil {
		return false, fmt.Errorf("advance Peer Pull acknowledgement: settle deliveries: %w", err)
	}
	return true, nil
}

func readContinuousPeerPullPublications(ctx context.Context, tx *sql.Tx,
	state peerPullSourceState, after uint64, limit int,
) ([]model.SignedPublication, uint64, error) {
	if after == state.sourceHead {
		return []model.SignedPublication{}, after, nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT event_id,channel_seq FROM events
		WHERE channel_id=? AND origin_peer_id=? AND origin_epoch=? AND source='local'
		AND channel_seq>? AND channel_seq<=? ORDER BY channel_seq LIMIT ?`,
		state.authority.channel.ID().String(), state.node.PeerID().String(),
		state.node.OriginEpoch().String(), after, state.sourceHead, limit)
	if err != nil {
		return nil, 0, fmt.Errorf("read Peer Pull page index: %w", err)
	}
	type indexedEvent struct {
		id       model.EventID
		sequence uint64
	}
	indexed := make([]indexedEvent, 0, limit)
	for rows.Next() {
		var eventText string
		var sequence uint64
		if err := rows.Scan(&eventText, &sequence); err != nil {
			rows.Close()
			return nil, 0, fmt.Errorf("read Peer Pull page index: scan: %w", err)
		}
		eventID, err := model.ParseEventID(eventText)
		if err != nil || sequence == 0 || sequence > model.MaxSQLiteInteger {
			rows.Close()
			return nil, 0, fmt.Errorf("%w: invalid indexed Event identity", ErrPeerPullInvariant)
		}
		indexed = append(indexed, indexedEvent{id: eventID, sequence: sequence})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, 0, fmt.Errorf("read Peer Pull page index: iterate: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, 0, fmt.Errorf("read Peer Pull page index: close: %w", err)
	}

	publications := make([]model.SignedPublication, 0, len(indexed))
	expected := after + 1
	totalBytes := 0
	for _, item := range indexed {
		if item.sequence != expected {
			return nil, 0, fmt.Errorf("%w: source sequence %d is missing before %d",
				ErrPeerPullInvariant, expected, item.sequence)
		}
		stored, err := readGossipPublication(ctx, tx, item.id)
		if err != nil {
			return nil, 0, fmt.Errorf("%w: publication sequence %d: %v",
				ErrPeerPullInvariant, item.sequence, err)
		}
		publication := stored.Record.Publication()
		if publication.Key().ChannelSequence() != item.sequence {
			return nil, 0, fmt.Errorf("%w: publication sequence projection mismatch", ErrPeerPullInvariant)
		}
		if err := validateGossipPublicationAuthority(state.node, state.authority, publication); err != nil {
			return nil, 0, fmt.Errorf("%w: publication sequence %d: %v",
				ErrPeerPullInvariant, item.sequence, err)
		}
		rawBytes := len(publication.WireJSON().Bytes())
		if totalBytes > maxPeerPullPageBytes-maxPeerPullPageEnvelopeBytes-rawBytes {
			break
		}
		totalBytes += rawBytes
		publications = append(publications, publication)
		expected++
	}
	if len(publications) == 0 {
		return nil, 0, fmt.Errorf("%w: source head has no bounded next publication", ErrPeerPullInvariant)
	}
	return publications, after + uint64(len(publications)), nil
}
