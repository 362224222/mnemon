package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const WorkDeadlineScanLimit = 8

var (
	ErrWorkDeadlineInput     = errors.New("invalid Work deadline input")
	ErrWorkDeadlineStale     = errors.New("Work deadline fence is stale")
	ErrWorkDeadlineInvariant = errors.New("Work deadline authority is inconsistent")
)

// WorkDeadlineCandidate is an immutable durable Work/cause pair returned only
// by ScanWorkDeadlines. PrepareWorkExpiry re-reads both values before exposing
// an Event-construction fence.
type WorkDeadlineCandidate struct {
	work  model.ReviewWork
	cause model.EventKey
}

func (c WorkDeadlineCandidate) Work() model.ReviewWork { return c.work }
func (c WorkDeadlineCandidate) Cause() model.EventKey  { return c.cause }

// WorkDeadlineScan separates actionable due Work from version-exhaustion
// diagnostics. Both collections are bounded; ExhaustedCount reports the full
// diagnostic cardinality without materializing an unbounded slice.
type WorkDeadlineScan struct {
	due                  []WorkDeadlineCandidate
	exhausted            []model.WorkRef
	moreDue              bool
	exhaustedCount       uint64
	nextDeadlineUnixNano int64
}

func (s WorkDeadlineScan) Due() []WorkDeadlineCandidate {
	return append([]WorkDeadlineCandidate(nil), s.due...)
}
func (s WorkDeadlineScan) Exhausted() []model.WorkRef {
	return append([]model.WorkRef(nil), s.exhausted...)
}
func (s WorkDeadlineScan) MoreDue() bool               { return s.moreDue }
func (s WorkDeadlineScan) ExhaustedCount() uint64      { return s.exhaustedCount }
func (s WorkDeadlineScan) NextDeadlineUnixNano() int64 { return s.nextDeadlineUnixNano }

// ScanWorkDeadlines performs one deterministic, restart-safe home scan. It
// returns at most eight healthy due candidates ordered by deadline then WorkID.
func (s *Store) ScanWorkDeadlines(ctx context.Context,
	trustedNow time.Time,
) (WorkDeadlineScan, error) {
	if s == nil || s.db == nil || ctx == nil {
		return WorkDeadlineScan{}, fmt.Errorf("%w: nil Store or context", ErrWorkDeadlineInput)
	}
	trustedNow, err := canonicalStoreTime(trustedNow)
	if err != nil || trustedNow.UnixNano() <= 0 {
		return WorkDeadlineScan{}, fmt.Errorf("%w: trusted time", ErrWorkDeadlineInput)
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return WorkDeadlineScan{}, fmt.Errorf("scan Work deadlines: begin: %w", err)
	}
	defer tx.Rollback()
	node, err := readNode(ctx, tx)
	if err != nil {
		return WorkDeadlineScan{}, fmt.Errorf("scan Work deadlines: Node: %w", err)
	}
	refs, err := readHealthyDeadlineRefs(ctx, tx, node.PeerID(), trustedNow.UnixNano())
	if err != nil {
		return WorkDeadlineScan{}, err
	}
	result := WorkDeadlineScan{moreDue: len(refs) > WorkDeadlineScanLimit}
	if result.moreDue {
		refs = refs[:WorkDeadlineScanLimit]
	}
	result.due = make([]WorkDeadlineCandidate, 0, len(refs))
	for _, ref := range refs {
		work, err := readReviewWork(ctx, tx, ref)
		if err != nil || work.Ref().HomePeerID() != node.PeerID() ||
			!work.State().DeadlineEligible() || work.Version() >= model.MaxSQLiteInteger ||
			work.DeadlineUnixNano() > trustedNow.UnixNano() {
			return WorkDeadlineScan{}, fmt.Errorf("%w: scanned Work %s changed or is invalid",
				ErrWorkDeadlineInvariant, ref.WorkID().String())
		}
		cause, err := exactDeadlineCause(ctx, tx, work)
		if err != nil {
			return WorkDeadlineScan{}, fmt.Errorf("%w: scanned Work cause: %v",
				ErrWorkDeadlineInvariant, err)
		}
		result.due = append(result.due, WorkDeadlineCandidate{work: work, cause: cause})
	}
	result.exhausted, result.exhaustedCount, err = readExhaustedDeadlineRefs(
		ctx, tx, node.PeerID(), trustedNow.UnixNano())
	if err != nil {
		return WorkDeadlineScan{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MIN(w.deadline_unix_nano),0) FROM works w
		JOIN channels c ON c.channel_id=w.channel_id WHERE w.home_peer_id=? AND c.status='active'
		AND w.state IN ('OFFERED','ACTIVE','REWORK')
		AND w.version<? AND w.deadline_unix_nano>?`, node.PeerID().String(), model.MaxSQLiteInteger,
		trustedNow.UnixNano()).Scan(&result.nextDeadlineUnixNano); err != nil {
		return WorkDeadlineScan{}, fmt.Errorf("scan Work deadlines: next deadline: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return WorkDeadlineScan{}, fmt.Errorf("scan Work deadlines: commit read: %w", err)
	}
	return result, nil
}

func readHealthyDeadlineRefs(ctx context.Context, tx *sql.Tx, home model.PeerID,
	nowUnixNano int64,
) ([]model.WorkRef, error) {
	rows, err := tx.QueryContext(ctx, `SELECT w.home_peer_id,w.work_id FROM works w
		JOIN channels c ON c.channel_id=w.channel_id WHERE w.home_peer_id=? AND c.status='active'
		AND w.state IN ('OFFERED','ACTIVE','REWORK') AND w.version<? AND w.deadline_unix_nano<=?
		ORDER BY w.deadline_unix_nano,w.work_id LIMIT ?`, home.String(), model.MaxSQLiteInteger,
		nowUnixNano, WorkDeadlineScanLimit+1)
	if err != nil {
		return nil, fmt.Errorf("scan Work deadlines: healthy index: %w", err)
	}
	return collectDeadlineRefs(rows)
}

func readExhaustedDeadlineRefs(ctx context.Context, tx *sql.Tx, home model.PeerID,
	nowUnixNano int64,
) ([]model.WorkRef, uint64, error) {
	rows, err := tx.QueryContext(ctx, `SELECT w.home_peer_id,w.work_id FROM works w
		JOIN channels c ON c.channel_id=w.channel_id WHERE w.home_peer_id=? AND c.status='active'
		AND w.state IN ('OFFERED','ACTIVE','REWORK') AND w.version=? AND w.deadline_unix_nano<=?
		ORDER BY w.deadline_unix_nano,w.work_id LIMIT ?`, home.String(), model.MaxSQLiteInteger,
		nowUnixNano, WorkDeadlineScanLimit)
	if err != nil {
		return nil, 0, fmt.Errorf("scan Work deadlines: exhausted index: %w", err)
	}
	refs, err := collectDeadlineRefs(rows)
	if err != nil {
		return nil, 0, err
	}
	var count uint64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM works w JOIN channels c ON c.channel_id=w.channel_id
		WHERE w.home_peer_id=? AND c.status='active' AND w.state IN ('OFFERED','ACTIVE','REWORK')
		AND w.version=? AND w.deadline_unix_nano<=?`,
		home.String(), model.MaxSQLiteInteger, nowUnixNano).Scan(&count); err != nil {
		return nil, 0, fmt.Errorf("scan Work deadlines: exhausted count: %w", err)
	}
	return refs, count, nil
}

func collectDeadlineRefs(rows *sql.Rows) ([]model.WorkRef, error) {
	defer rows.Close()
	refs := make([]model.WorkRef, 0, WorkDeadlineScanLimit+1)
	for rows.Next() {
		var homeText, workText string
		if err := rows.Scan(&homeText, &workText); err != nil {
			return nil, fmt.Errorf("scan Work deadlines: row: %w", err)
		}
		home, err := model.ParsePeerID(homeText)
		if err != nil {
			return nil, fmt.Errorf("scan Work deadlines: home: %w", err)
		}
		workID, err := model.ParseWorkID(workText)
		if err != nil {
			return nil, fmt.Errorf("scan Work deadlines: WorkID: %w", err)
		}
		ref, err := model.NewWorkRef(home, workID)
		if err != nil {
			return nil, fmt.Errorf("scan Work deadlines: WorkRef: %w", err)
		}
		refs = append(refs, ref)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan Work deadlines: rows: %w", err)
	}
	return refs, nil
}
