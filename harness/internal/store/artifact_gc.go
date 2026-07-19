package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	artifactGCMaxCandidates        = 256
	artifactGCMaxQueue             = 256
	artifactGCMaxQueueList         = artifactGCMaxQueue + 1
	artifactGCMaxObjectBytes       = 4 << 20
	artifactGCMaxSweepBytes        = 256 << 20
	artifactGCStagingRetention     = time.Hour
	artifactGCMaxCompletionReceipt = 256
	artifactGCMaxGeneration        = uint64(1<<63 - 1)
)

func (s *Store) OpenArtifactGCScan(ctx context.Context,
	spec artifactdomain.GCScanSpec,
) (artifactdomain.GCScanCursor, error) {
	if s == nil || s.db == nil || ctx == nil {
		return artifactdomain.GCScanCursor{}, artifactGCError("open scan", errors.New("nil Store or context"))
	}
	cutoff, err := canonicalStoreTime(spec.InitializeCutoff)
	if err != nil || cutoff != spec.InitializeCutoff {
		return artifactdomain.GCScanCursor{}, artifactGCError("open scan", errors.New("noncanonical cutoff"))
	}
	at, err := canonicalStoreTime(spec.At)
	if err != nil || at != spec.At || cutoff.After(at) {
		return artifactdomain.GCScanCursor{}, artifactGCError("open scan", errors.New("noncanonical trusted time"))
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return artifactdomain.GCScanCursor{}, fmt.Errorf("open Artifact GC scan: begin: %w", err)
	}
	defer tx.Rollback()
	if err := requireEmptyArtifactGCGuards(ctx, tx); err != nil {
		return artifactdomain.GCScanCursor{}, err
	}

	cursor, updatedAt, found, err := readArtifactGCScan(ctx, tx)
	if err != nil {
		return artifactdomain.GCScanCursor{}, err
	}
	if !found {
		if _, err := tx.ExecContext(ctx, `INSERT INTO artifact_gc_scan(singleton,cutoff,"after",done,updated_at)
			VALUES(1,?,'',0,?)`, storeTime(cutoff), storeTime(at)); err != nil {
			return artifactdomain.GCScanCursor{}, artifactGCError("open scan", err)
		}
		cursor = artifactdomain.GCScanCursor{Cutoff: cutoff}
	} else if cursor.Done {
		if at.Before(updatedAt) {
			return artifactdomain.GCScanCursor{}, artifactGCError("open scan", errors.New("trusted time regressed"))
		}
		result, err := tx.ExecContext(ctx, `UPDATE artifact_gc_scan
			SET cutoff=?,"after"='',done=0,updated_at=? WHERE singleton=1 AND done=1`,
			storeTime(cutoff), storeTime(at))
		if err != nil || exactlyOne(result) != nil {
			return artifactdomain.GCScanCursor{}, artifactGCError("open fresh scan", err)
		}
		cursor = artifactdomain.GCScanCursor{Cutoff: cutoff}
	} else if cursor.Cutoff.After(cutoff) {
		return artifactdomain.GCScanCursor{}, artifactGCError("open scan", errors.New("durable cutoff exceeds requested cutoff"))
	}
	if err := tx.Commit(); err != nil {
		return artifactdomain.GCScanCursor{}, fmt.Errorf("open Artifact GC scan: commit: %w", err)
	}
	return cursor, nil
}

func (s *Store) PrepareArtifactGC(ctx context.Context,
	spec artifactdomain.GCPrepareSpec,
) (artifactdomain.GCPrepareResult, error) {
	request, err := validateArtifactGCPrepareSpec(spec)
	if s == nil || s.db == nil || ctx == nil {
		return artifactdomain.GCPrepareResult{}, artifactGCError("prepare", errors.New("nil Store or context"))
	}
	if err != nil {
		return artifactdomain.GCPrepareResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return artifactdomain.GCPrepareResult{}, fmt.Errorf("prepare Artifact GC: begin: %w", err)
	}
	defer tx.Rollback()
	if err := requireEmptyArtifactGCGuards(ctx, tx); err != nil {
		return artifactdomain.GCPrepareResult{}, err
	}
	if err := requireCommittedArtifactGCProvenance(ctx, tx); err != nil {
		return artifactdomain.GCPrepareResult{}, err
	}

	current, updatedAt, found, err := readArtifactGCScan(ctx, tx)
	if err != nil {
		return artifactdomain.GCPrepareResult{}, err
	}
	if replayed, replayFound, replayErr := readArtifactGCPrepareReceipt(ctx, tx, request,
		spec.Current, current, found); replayErr != nil {
		return artifactdomain.GCPrepareResult{}, replayErr
	} else if replayFound {
		if err := tx.Commit(); err != nil {
			return artifactdomain.GCPrepareResult{}, fmt.Errorf("prepare Artifact GC replay: commit: %w", err)
		}
		return replayed, nil
	}
	if !found || current != spec.Current {
		return artifactdomain.GCPrepareResult{}, artifactGCError("prepare", errors.New("scan cursor changed"))
	}
	if current.Done || spec.At.Before(updatedAt) {
		return artifactdomain.GCPrepareResult{}, artifactGCError("prepare", errors.New("closed or regressed scan"))
	}
	var queueCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM artifact_gc_queue`).Scan(&queueCount); err != nil ||
		queueCount < 0 || queueCount > artifactGCMaxQueue {
		return artifactdomain.GCPrepareResult{}, artifactGCError("prepare queue count", err)
	}

	result := artifactdomain.GCPrepareResult{Next: current}
	for _, candidate := range spec.Candidates {
		var queued int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM artifact_gc_queue WHERE digest=?`,
			candidate.Digest.String()).Scan(&queued); err != nil {
			return artifactdomain.GCPrepareResult{}, artifactGCError("prepare inspect queue", err)
		}
		if queued != 0 {
			return artifactdomain.GCPrepareResult{}, artifactGCError("prepare", errors.New("candidate already queued"))
		}
		protected, err := artifactGCDigestProtected(ctx, tx, candidate.Digest, spec.At)
		if err != nil {
			return artifactdomain.GCPrepareResult{}, err
		}
		if !protected && (result.Queued >= spec.MaxQueued || queueCount >= spec.MaxQueue) {
			break
		}
		result.Examined++
		result.Next.After = candidate.Digest
		if protected {
			result.Protected++
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO artifact_gc_queue(
			digest,token,state,size_bytes,modified_at,queued_at,renamed_at,updated_at)
			VALUES(?,?,'queued',?,?,?,NULL,?)`, candidate.Digest.String(), candidate.Token[:],
			candidate.SizeBytes, storeTime(candidate.ModifiedAt), storeTime(spec.At), storeTime(spec.At)); err != nil {
			return artifactdomain.GCPrepareResult{}, artifactGCError("prepare enqueue", err)
		}
		queueCount++
		result.Queued++
		result.QueuedBytes += candidate.SizeBytes
	}
	result.Next.Done = spec.PageDone && result.Examined == len(spec.Candidates)
	update, err := tx.ExecContext(ctx, `UPDATE artifact_gc_scan SET "after"=?,done=?,updated_at=?
		WHERE singleton=1 AND cutoff=? AND "after"=? AND done=?`, digestText(result.Next.After),
		boolInt(result.Next.Done), storeTime(spec.At), storeTime(current.Cutoff),
		digestText(current.After), boolInt(current.Done))
	if err != nil || exactlyOne(update) != nil {
		return artifactdomain.GCPrepareResult{}, artifactGCError("prepare advance scan", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO artifact_gc_prepare_receipt(singleton,request,
		next_cutoff,next_after,next_done,examined,protected,queued,queued_bytes,created_at)
		VALUES(1,?,?,?,?,?,?,?,?,?) ON CONFLICT(singleton) DO UPDATE SET
		request=excluded.request,next_cutoff=excluded.next_cutoff,next_after=excluded.next_after,
		next_done=excluded.next_done,examined=excluded.examined,protected=excluded.protected,
		queued=excluded.queued,queued_bytes=excluded.queued_bytes,created_at=excluded.created_at`,
		request, storeTime(result.Next.Cutoff), digestText(result.Next.After), boolInt(result.Next.Done),
		result.Examined, result.Protected, result.Queued, result.QueuedBytes, storeTime(spec.At)); err != nil {
		return artifactdomain.GCPrepareResult{}, artifactGCError("prepare receipt", err)
	}
	if err := tx.Commit(); err != nil {
		return artifactdomain.GCPrepareResult{}, fmt.Errorf("prepare Artifact GC: commit: %w", err)
	}
	return result, nil
}

func (s *Store) OpenArtifactGCStagingScan(ctx context.Context,
	spec artifactdomain.GCScanSpec,
) (artifactdomain.GCStagingScanCursor, error) {
	if s == nil || s.db == nil || ctx == nil {
		return artifactdomain.GCStagingScanCursor{}, artifactGCError("open staging scan", errors.New("nil Store or context"))
	}
	cutoff, err := canonicalStoreTime(spec.InitializeCutoff)
	if err != nil || cutoff != spec.InitializeCutoff {
		return artifactdomain.GCStagingScanCursor{}, artifactGCError("open staging scan", errors.New("noncanonical cutoff"))
	}
	at, err := canonicalStoreTime(spec.At)
	if err != nil || at != spec.At || cutoff.After(at) {
		return artifactdomain.GCStagingScanCursor{}, artifactGCError("open staging scan", errors.New("noncanonical trusted time"))
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return artifactdomain.GCStagingScanCursor{}, fmt.Errorf("open Artifact GC staging scan: begin: %w", err)
	}
	defer tx.Rollback()
	if err := requireEmptyArtifactGCGuards(ctx, tx); err != nil {
		return artifactdomain.GCStagingScanCursor{}, err
	}

	cursor, updatedAt, found, err := readArtifactGCStagingScan(ctx, tx)
	if err != nil {
		return artifactdomain.GCStagingScanCursor{}, err
	}
	if !found {
		if _, err := tx.ExecContext(ctx, `INSERT INTO artifact_gc_staging_scan(
			singleton,generation,cutoff,"after",done,updated_at) VALUES(1,1,?,'',0,?)`,
			storeTime(cutoff), storeTime(at)); err != nil {
			return artifactdomain.GCStagingScanCursor{}, artifactGCError("open staging scan", err)
		}
		cursor = artifactdomain.GCStagingScanCursor{Generation: 1, Cutoff: cutoff}
	} else if cursor.Done {
		if at.Before(updatedAt) || cursor.Generation >= artifactGCMaxGeneration {
			return artifactdomain.GCStagingScanCursor{}, artifactGCError("open staging scan", errors.New("trusted time regressed or generation exhausted"))
		}
		generation := cursor.Generation + 1
		updated, err := tx.ExecContext(ctx, `UPDATE artifact_gc_staging_scan
			SET generation=?,cutoff=?,"after"='',done=0,updated_at=?
			WHERE singleton=1 AND generation=? AND done=1`, generation, storeTime(cutoff),
			storeTime(at), cursor.Generation)
		if err != nil || exactlyOne(updated) != nil {
			return artifactdomain.GCStagingScanCursor{}, artifactGCError("open fresh staging scan", err)
		}
		cursor = artifactdomain.GCStagingScanCursor{Generation: generation, Cutoff: cutoff}
	} else if cursor.Cutoff.After(cutoff) || at.Before(updatedAt) {
		return artifactdomain.GCStagingScanCursor{}, artifactGCError("open staging scan", errors.New("durable cutoff or trusted time exceeds request"))
	}
	if err := tx.Commit(); err != nil {
		return artifactdomain.GCStagingScanCursor{}, fmt.Errorf("open Artifact GC staging scan: commit: %w", err)
	}
	return cursor, nil
}

func (s *Store) SweepArtifactGCStaging(ctx context.Context,
	spec artifactdomain.GCStagingSweepSpec,
) (artifactdomain.GCStagingSweepResult, error) {
	current, at, err := validateArtifactGCStagingSweepSpec(spec)
	if s == nil || s.db == nil || ctx == nil {
		return artifactdomain.GCStagingSweepResult{}, artifactGCError("sweep staging", errors.New("nil Store or context"))
	}
	if err != nil {
		return artifactdomain.GCStagingSweepResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return artifactdomain.GCStagingSweepResult{}, fmt.Errorf("sweep Artifact GC staging: begin: %w", err)
	}
	defer tx.Rollback()
	if err := requireEmptyArtifactGCGuards(ctx, tx); err != nil {
		return artifactdomain.GCStagingSweepResult{}, err
	}
	if err := requireCommittedArtifactGCProvenance(ctx, tx); err != nil {
		return artifactdomain.GCStagingSweepResult{}, err
	}
	staging, updatedAt, found, err := readArtifactGCStagingScan(ctx, tx)
	if err != nil {
		return artifactdomain.GCStagingSweepResult{}, err
	}
	request := encodeArtifactGCStagingRequest(spec)
	if replayed, replayFound, replayErr := readArtifactGCStagingReceipt(ctx, tx, request,
		current, staging, found); replayErr != nil {
		return artifactdomain.GCStagingSweepResult{}, replayErr
	} else if replayFound {
		if err := tx.Commit(); err != nil {
			return artifactdomain.GCStagingSweepResult{}, fmt.Errorf("sweep Artifact GC staging replay: commit: %w", err)
		}
		return replayed, nil
	}
	if !found || staging != current {
		return artifactdomain.GCStagingSweepResult{}, artifactGCError("sweep staging", errors.New("staging cursor changed"))
	}
	if staging.Done || at.Before(updatedAt) {
		return artifactdomain.GCStagingSweepResult{}, artifactGCError("sweep staging", errors.New("closed or regressed staging scan"))
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM artifact_pins WHERE rowid IN (
		SELECT rowid FROM artifact_pins WHERE owner_kind='inbox'
		AND expires_at IS NOT NULL AND expires_at<=?
		ORDER BY expires_at,root_digest,owner_id LIMIT ?)`, storeTime(at), spec.MaxItems); err != nil {
		return artifactdomain.GCStagingSweepResult{}, artifactGCError("sweep expired Inbox pins", err)
	}

	rows, err := tx.QueryContext(ctx, `SELECT root_digest,total_bytes FROM artifact_roots
		WHERE root_digest>? AND created_at<? ORDER BY root_digest LIMIT ?`,
		digestText(staging.After), storeTime(staging.Cutoff), spec.MaxItems)
	if err != nil {
		return artifactdomain.GCStagingSweepResult{}, artifactGCError("sweep list roots", err)
	}
	type candidate struct {
		root model.Digest
		size uint64
	}
	candidates := make([]candidate, 0, spec.MaxItems)
	for rows.Next() {
		var rootText string
		var size int64
		if err := rows.Scan(&rootText, &size); err != nil {
			rows.Close()
			return artifactdomain.GCStagingSweepResult{}, artifactGCError("sweep scan root", err)
		}
		root, parseErr := model.ParseDigest(rootText)
		if parseErr != nil || size < 0 || uint64(size) > artifactGCMaxSweepBytes {
			rows.Close()
			return artifactdomain.GCStagingSweepResult{}, artifactGCError("sweep root", errors.New("invalid durable metadata"))
		}
		candidates = append(candidates, candidate{root: root, size: uint64(size)})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return artifactdomain.GCStagingSweepResult{}, artifactGCError("sweep iterate roots", err)
	}
	if err := rows.Close(); err != nil {
		return artifactdomain.GCStagingSweepResult{}, artifactGCError("sweep close roots", err)
	}

	result := artifactdomain.GCStagingSweepResult{Next: staging}
	nextAfter := staging.After
	for _, candidate := range candidates {
		protected, err := artifactGCRootOwned(ctx, tx, candidate.root, at)
		if err != nil {
			return artifactdomain.GCStagingSweepResult{}, err
		}
		if !protected && candidate.size > spec.MaxBytes-result.SweptBytes {
			break
		}
		result.Examined++
		nextAfter = candidate.root
		if protected {
			continue
		}
		blocks, err := artifactGCRootBlocks(ctx, tx, candidate.root)
		if err != nil {
			return artifactdomain.GCStagingSweepResult{}, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO artifact_gc_delete_guard(root_digest,authorized_at)
			VALUES(?,?)`, candidate.root.String(), storeTime(at)); err != nil {
			return artifactdomain.GCStagingSweepResult{}, artifactGCError("authorize staging root delete", err)
		}
		deleted, err := tx.ExecContext(ctx, `DELETE FROM artifact_roots WHERE root_digest=?`,
			candidate.root.String())
		if err != nil || exactlyOne(deleted) != nil {
			return artifactdomain.GCStagingSweepResult{}, artifactGCError("delete staging root", err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM artifact_gc_delete_guard WHERE root_digest=?`,
			candidate.root.String()); err != nil {
			return artifactdomain.GCStagingSweepResult{}, artifactGCError("close staging root delete", err)
		}
		for _, block := range blocks {
			if err := deleteOrphanArtifactGCBlock(ctx, tx, block, at); err != nil {
				return artifactdomain.GCStagingSweepResult{}, err
			}
		}
		result.Swept++
		result.SweptBytes += candidate.size
	}
	var remaining int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM artifact_roots
		WHERE root_digest>? AND created_at<?)`, digestText(nextAfter), storeTime(staging.Cutoff)).Scan(&remaining); err != nil {
		return artifactdomain.GCStagingSweepResult{}, artifactGCError("close staging page", err)
	}
	done := remaining == 0
	result.Next.After = nextAfter
	result.Next.Done = done
	updated, err := tx.ExecContext(ctx, `UPDATE artifact_gc_staging_scan
		SET "after"=?,done=?,updated_at=? WHERE singleton=1 AND generation=?
		AND cutoff=? AND "after"=? AND done=0`,
		digestText(nextAfter), boolInt(done), storeTime(at), staging.Generation,
		storeTime(staging.Cutoff), digestText(staging.After))
	if err != nil || exactlyOne(updated) != nil {
		return artifactdomain.GCStagingSweepResult{}, artifactGCError("advance staging scan", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO artifact_gc_staging_receipt(singleton,request,
		next_generation,next_cutoff,next_after,next_done,examined,swept,swept_bytes,created_at)
		VALUES(1,?,?,?,?,?,?,?,?,?) ON CONFLICT(singleton) DO UPDATE SET
		request=excluded.request,next_generation=excluded.next_generation,
		next_cutoff=excluded.next_cutoff,next_after=excluded.next_after,next_done=excluded.next_done,
		examined=excluded.examined,swept=excluded.swept,swept_bytes=excluded.swept_bytes,
		created_at=excluded.created_at`, request, result.Next.Generation,
		storeTime(result.Next.Cutoff), digestText(result.Next.After), boolInt(result.Next.Done),
		result.Examined, result.Swept, result.SweptBytes, storeTime(at)); err != nil {
		return artifactdomain.GCStagingSweepResult{}, artifactGCError("checkpoint staging receipt", err)
	}
	if err := requireEmptyArtifactGCGuards(ctx, tx); err != nil {
		return artifactdomain.GCStagingSweepResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return artifactdomain.GCStagingSweepResult{}, fmt.Errorf("sweep Artifact GC staging: commit: %w", err)
	}
	return result, nil
}

func (s *Store) ListArtifactGCQueue(ctx context.Context, limit int) ([]artifactdomain.GCQueueItem, error) {
	if s == nil || s.db == nil || ctx == nil || limit <= 0 || limit > artifactGCMaxQueueList {
		return nil, artifactGCError("list queue", errors.New("invalid input"))
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("list Artifact GC queue: begin: %w", err)
	}
	defer tx.Rollback()
	if err := requireEmptyArtifactGCGuards(ctx, tx); err != nil {
		return nil, err
	}
	if err := requireCommittedArtifactGCProvenance(ctx, tx); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT digest,token,state,size_bytes,modified_at,
		queued_at,renamed_at,updated_at FROM artifact_gc_queue ORDER BY digest,token LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("list Artifact GC queue: %w", err)
	}
	items := make([]artifactdomain.GCQueueItem, 0, limit)
	for rows.Next() {
		item, err := scanArtifactGCQueue(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, artifactGCError("list queue", err)
	}
	if err := rows.Close(); err != nil {
		return nil, artifactGCError("list queue", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("list Artifact GC queue: commit: %w", err)
	}
	return items, nil
}

func (s *Store) GetArtifactGCQueue(ctx context.Context,
	identity artifactdomain.GCQueueIdentity,
) (artifactdomain.GCQueueItem, bool, error) {
	if s == nil || s.db == nil || ctx == nil || identity.Digest.IsZero() ||
		identity.Token == ([32]byte{}) {
		return artifactdomain.GCQueueItem{}, false, artifactGCError("get queue", errors.New("invalid input"))
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return artifactdomain.GCQueueItem{}, false, fmt.Errorf("get Artifact GC queue: begin: %w", err)
	}
	defer tx.Rollback()
	if err := requireEmptyArtifactGCGuards(ctx, tx); err != nil {
		return artifactdomain.GCQueueItem{}, false, err
	}
	if err := requireCommittedArtifactGCProvenance(ctx, tx); err != nil {
		return artifactdomain.GCQueueItem{}, false, err
	}
	row := tx.QueryRowContext(ctx, `SELECT digest,token,state,size_bytes,modified_at,
		queued_at,renamed_at,updated_at FROM artifact_gc_queue WHERE digest=?`, identity.Digest.String())
	item, err := scanArtifactGCQueue(row)
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.Commit(); err != nil {
			return artifactdomain.GCQueueItem{}, false, fmt.Errorf("get Artifact GC queue: commit: %w", err)
		}
		return artifactdomain.GCQueueItem{}, false, nil
	}
	if err != nil {
		return artifactdomain.GCQueueItem{}, false, err
	}
	if item.Identity != identity {
		return artifactdomain.GCQueueItem{}, false, artifactGCError("get queue", errors.New("digest token differs"))
	}
	if err := tx.Commit(); err != nil {
		return artifactdomain.GCQueueItem{}, false, fmt.Errorf("get Artifact GC queue: commit: %w", err)
	}
	return item, true, nil
}

func (s *Store) MarkArtifactGCRenamed(ctx context.Context,
	spec artifactdomain.GCQueueTransitionSpec,
) (artifactdomain.GCQueueTransitionResult, error) {
	identity, at, err := validateArtifactGCTransitionSpec(spec)
	if s == nil || s.db == nil || ctx == nil {
		return artifactdomain.GCQueueTransitionResult{}, artifactGCError("mark renamed", errors.New("nil Store or context"))
	}
	if err != nil {
		return artifactdomain.GCQueueTransitionResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return artifactdomain.GCQueueTransitionResult{}, fmt.Errorf("mark Artifact GC renamed: begin: %w", err)
	}
	defer tx.Rollback()
	if err := requireEmptyArtifactGCGuards(ctx, tx); err != nil {
		return artifactdomain.GCQueueTransitionResult{}, err
	}
	if err := requireCommittedArtifactGCProvenance(ctx, tx); err != nil {
		return artifactdomain.GCQueueTransitionResult{}, err
	}
	item, found, err := readArtifactGCQueueTx(ctx, tx, identity.Digest)
	if err != nil {
		return artifactdomain.GCQueueTransitionResult{}, err
	}
	if !found || item.Identity != identity {
		return artifactdomain.GCQueueTransitionResult{}, artifactGCError("mark renamed", errors.New("queue identity missing"))
	}
	if item.State == artifactdomain.GCQueueRenamed {
		if item.RenamedAt != at {
			return artifactdomain.GCQueueTransitionResult{}, artifactGCError("mark renamed", errors.New("transition time differs"))
		}
		if err := tx.Commit(); err != nil {
			return artifactdomain.GCQueueTransitionResult{}, fmt.Errorf("mark Artifact GC renamed replay: commit: %w", err)
		}
		return artifactdomain.GCQueueTransitionResult{State: artifactdomain.GCQueueRenamed,
			Replayed: true, At: item.RenamedAt}, nil
	}
	if item.State != artifactdomain.GCQueueQueued || at.Before(item.QueuedAt) {
		return artifactdomain.GCQueueTransitionResult{}, artifactGCError("mark renamed", errors.New("invalid transition"))
	}
	protected, err := artifactGCDigestProtected(ctx, tx, identity.Digest, at)
	if err != nil {
		return artifactdomain.GCQueueTransitionResult{}, err
	}
	if protected {
		return artifactdomain.GCQueueTransitionResult{}, artifactGCError("mark renamed", errors.New("durable owner appeared"))
	}
	if err := deleteQueuedArtifactGCBlock(ctx, tx, identity.Digest); err != nil {
		return artifactdomain.GCQueueTransitionResult{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE artifact_gc_queue
		SET state='renamed',renamed_at=?,updated_at=? WHERE digest=? AND token=? AND state='queued'`,
		storeTime(at), storeTime(at), identity.Digest.String(), identity.Token[:])
	if err != nil || exactlyOne(result) != nil {
		return artifactdomain.GCQueueTransitionResult{}, artifactGCError("mark renamed", err)
	}
	if err := tx.Commit(); err != nil {
		return artifactdomain.GCQueueTransitionResult{}, fmt.Errorf("mark Artifact GC renamed: commit: %w", err)
	}
	return artifactdomain.GCQueueTransitionResult{State: artifactdomain.GCQueueRenamed, At: at}, nil
}

func (s *Store) CompleteArtifactGC(ctx context.Context,
	spec artifactdomain.GCQueueTransitionSpec,
) (artifactdomain.GCQueueTransitionResult, error) {
	identity, at, err := validateArtifactGCTransitionSpec(spec)
	if s == nil || s.db == nil || ctx == nil {
		return artifactdomain.GCQueueTransitionResult{}, artifactGCError("complete", errors.New("nil Store or context"))
	}
	if err != nil {
		return artifactdomain.GCQueueTransitionResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return artifactdomain.GCQueueTransitionResult{}, fmt.Errorf("complete Artifact GC: begin: %w", err)
	}
	defer tx.Rollback()
	if err := requireEmptyArtifactGCGuards(ctx, tx); err != nil {
		return artifactdomain.GCQueueTransitionResult{}, err
	}
	if err := requireCommittedArtifactGCProvenance(ctx, tx); err != nil {
		return artifactdomain.GCQueueTransitionResult{}, err
	}
	item, found, err := readArtifactGCQueueTx(ctx, tx, identity.Digest)
	if err != nil {
		return artifactdomain.GCQueueTransitionResult{}, err
	}
	if !found {
		var completedText string
		err := tx.QueryRowContext(ctx, `SELECT completed_at FROM artifact_gc_completion_receipts
			WHERE digest=? AND token=?`, identity.Digest.String(), identity.Token[:]).Scan(&completedText)
		if errors.Is(err, sql.ErrNoRows) {
			return artifactdomain.GCQueueTransitionResult{}, artifactGCError("complete", errors.New("queue identity missing"))
		}
		completedAt, parseErr := parseCanonicalStoreTime(completedText)
		if err != nil || parseErr != nil || completedAt != at {
			return artifactdomain.GCQueueTransitionResult{}, artifactGCError("complete replay", errors.New("completion identity or time differs"))
		}
		if err := tx.Commit(); err != nil {
			return artifactdomain.GCQueueTransitionResult{}, fmt.Errorf("complete Artifact GC replay: commit: %w", err)
		}
		return artifactdomain.GCQueueTransitionResult{Completed: true, Replayed: true, At: completedAt}, nil
	}
	if item.Identity != identity || item.State != artifactdomain.GCQueueRenamed || at.Before(item.RenamedAt) {
		return artifactdomain.GCQueueTransitionResult{}, artifactGCError("complete", errors.New("queue is not exact renamed identity"))
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO artifact_gc_completion_receipts(digest,token,completed_at)
		VALUES(?,?,?)`, identity.Digest.String(), identity.Token[:], storeTime(at)); err != nil {
		return artifactdomain.GCQueueTransitionResult{}, artifactGCError("complete receipt", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO artifact_gc_completion_guard(digest,token,completed_at)
		VALUES(?,?,?)`, identity.Digest.String(), identity.Token[:], storeTime(at)); err != nil {
		return artifactdomain.GCQueueTransitionResult{}, artifactGCError("authorize completion", err)
	}
	deleted, err := tx.ExecContext(ctx, `DELETE FROM artifact_gc_queue WHERE digest=? AND token=?`,
		identity.Digest.String(), identity.Token[:])
	if err != nil || exactlyOne(deleted) != nil {
		return artifactdomain.GCQueueTransitionResult{}, artifactGCError("complete queue delete", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM artifact_gc_completion_guard WHERE digest=?`,
		identity.Digest.String()); err != nil {
		return artifactdomain.GCQueueTransitionResult{}, artifactGCError("close completion authority", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM artifact_gc_completion_receipts WHERE completion_seq IN (
		SELECT completion_seq FROM artifact_gc_completion_receipts
		ORDER BY completion_seq DESC LIMIT -1 OFFSET ?)`,
		artifactGCMaxCompletionReceipt); err != nil {
		return artifactdomain.GCQueueTransitionResult{}, artifactGCError("bound completion receipts", err)
	}
	if err := tx.Commit(); err != nil {
		return artifactdomain.GCQueueTransitionResult{}, fmt.Errorf("complete Artifact GC: commit: %w", err)
	}
	return artifactdomain.GCQueueTransitionResult{Completed: true, At: at}, nil
}

func validateArtifactGCPrepareSpec(spec artifactdomain.GCPrepareSpec) ([]byte, error) {
	cutoff, cutoffErr := canonicalStoreTime(spec.Current.Cutoff)
	at, atErr := canonicalStoreTime(spec.At)
	if cutoffErr != nil || atErr != nil || cutoff != spec.Current.Cutoff || at != spec.At ||
		cutoff.After(at) || spec.Current.Done || spec.MaxQueued <= 0 ||
		spec.MaxQueued > artifactGCMaxCandidates || spec.MaxQueue <= 0 ||
		spec.MaxQueue > artifactGCMaxQueue || spec.MaxQueued > spec.MaxQueue ||
		len(spec.Candidates) > artifactGCMaxCandidates || len(spec.Candidates) == 0 && !spec.PageDone {
		return nil, artifactGCError("prepare input", errors.New("invalid envelope"))
	}
	previous := spec.Current.After
	seenTokens := make(map[[32]byte]struct{}, len(spec.Candidates))
	for _, candidate := range spec.Candidates {
		modified, err := canonicalStoreTime(candidate.ModifiedAt)
		if candidate.Digest.IsZero() || candidate.Token == ([32]byte{}) ||
			candidate.SizeBytes > artifactGCMaxObjectBytes || err != nil || modified != candidate.ModifiedAt ||
			!candidate.ModifiedAt.Before(spec.Current.Cutoff) ||
			(!previous.IsZero() && candidate.Digest.String() <= previous.String()) {
			return nil, artifactGCError("prepare input", errors.New("invalid candidate"))
		}
		if _, duplicate := seenTokens[candidate.Token]; duplicate {
			return nil, artifactGCError("prepare input", errors.New("duplicate token"))
		}
		seenTokens[candidate.Token] = struct{}{}
		previous = candidate.Digest
	}
	request := encodeArtifactGCPrepareRequest(spec)
	if len(request) == 0 || len(request) > 65536 {
		return nil, artifactGCError("prepare input", errors.New("request encoding bound"))
	}
	return request, nil
}

func validateArtifactGCStagingSweepSpec(spec artifactdomain.GCStagingSweepSpec) (artifactdomain.GCStagingScanCursor, time.Time, error) {
	cutoff, cutoffErr := canonicalStoreTime(spec.Current.Cutoff)
	at, atErr := canonicalStoreTime(spec.At)
	if cutoffErr != nil || atErr != nil || cutoff != spec.Current.Cutoff || at != spec.At || cutoff.After(at) ||
		spec.Current.Generation == 0 || spec.Current.Generation > artifactGCMaxGeneration || spec.Current.Done ||
		spec.MaxItems <= 0 || spec.MaxItems > artifactGCMaxCandidates || spec.MaxBytes == 0 ||
		spec.MaxBytes > artifactGCMaxSweepBytes {
		return artifactdomain.GCStagingScanCursor{}, time.Time{}, artifactGCError("sweep staging input", errors.New("invalid envelope"))
	}
	return spec.Current, at, nil
}

func validateArtifactGCTransitionSpec(spec artifactdomain.GCQueueTransitionSpec) (artifactdomain.GCQueueIdentity, time.Time, error) {
	at, err := canonicalStoreTime(spec.At)
	if spec.Identity.Digest.IsZero() || spec.Identity.Token == ([32]byte{}) || err != nil || at != spec.At {
		return artifactdomain.GCQueueIdentity{}, time.Time{}, artifactGCError("queue transition input", errors.New("invalid identity or time"))
	}
	return spec.Identity, at, nil
}

func readArtifactGCScan(ctx context.Context, tx *sql.Tx) (artifactdomain.GCScanCursor, time.Time, bool, error) {
	var cutoffText, afterText, updatedText string
	var done int
	err := tx.QueryRowContext(ctx, `SELECT cutoff,"after",done,updated_at FROM artifact_gc_scan
		WHERE singleton=1`).Scan(&cutoffText, &afterText, &done, &updatedText)
	if errors.Is(err, sql.ErrNoRows) {
		return artifactdomain.GCScanCursor{}, time.Time{}, false, nil
	}
	if err != nil {
		return artifactdomain.GCScanCursor{}, time.Time{}, false, artifactGCError("read scan", err)
	}
	cutoff, cutoffErr := parseCanonicalStoreTime(cutoffText)
	updated, updatedErr := parseCanonicalStoreTime(updatedText)
	after, afterErr := parseOptionalArtifactGCDigest(afterText)
	if cutoffErr != nil || updatedErr != nil || afterErr != nil || (done != 0 && done != 1) || cutoff.After(updated) {
		return artifactdomain.GCScanCursor{}, time.Time{}, false, artifactGCError("read scan", errors.New("invalid durable scan"))
	}
	return artifactdomain.GCScanCursor{Cutoff: cutoff, After: after, Done: done == 1}, updated, true, nil
}

func readArtifactGCStagingScan(ctx context.Context, tx *sql.Tx) (artifactdomain.GCStagingScanCursor, time.Time, bool, error) {
	var generation int64
	var cutoffText, afterText, updatedText string
	var done int
	err := tx.QueryRowContext(ctx, `SELECT generation,cutoff,"after",done,updated_at
		FROM artifact_gc_staging_scan WHERE singleton=1`).Scan(
		&generation, &cutoffText, &afterText, &done, &updatedText)
	if errors.Is(err, sql.ErrNoRows) {
		return artifactdomain.GCStagingScanCursor{}, time.Time{}, false, nil
	}
	if err != nil {
		return artifactdomain.GCStagingScanCursor{}, time.Time{}, false, artifactGCError("read staging scan", err)
	}
	cutoff, cutoffErr := parseCanonicalStoreTime(cutoffText)
	updated, updatedErr := parseCanonicalStoreTime(updatedText)
	after, afterErr := parseOptionalArtifactGCDigest(afterText)
	if generation <= 0 || cutoffErr != nil || updatedErr != nil || afterErr != nil ||
		(done != 0 && done != 1) || cutoff.After(updated) {
		return artifactdomain.GCStagingScanCursor{}, time.Time{}, false,
			artifactGCError("read staging scan", errors.New("invalid durable cursor"))
	}
	return artifactdomain.GCStagingScanCursor{Generation: uint64(generation), Cutoff: cutoff,
		After: after, Done: done == 1}, updated, true, nil
}

func readArtifactGCStagingReceipt(ctx context.Context, tx *sql.Tx,
	request []byte, current, durable artifactdomain.GCStagingScanCursor, durableFound bool,
) (artifactdomain.GCStagingSweepResult, bool, error) {
	var stored []byte
	var generation int64
	var cutoffText, afterText string
	var done int
	var examined, swept int
	var sweptBytes int64
	err := tx.QueryRowContext(ctx, `SELECT request,next_generation,next_cutoff,next_after,next_done,
		examined,swept,swept_bytes
		FROM artifact_gc_staging_receipt WHERE singleton=1`).Scan(
		&stored, &generation, &cutoffText, &afterText, &done, &examined, &swept, &sweptBytes)
	if errors.Is(err, sql.ErrNoRows) || err == nil && !bytes.Equal(stored, request) {
		return artifactdomain.GCStagingSweepResult{}, false, nil
	}
	if err != nil {
		return artifactdomain.GCStagingSweepResult{}, false, artifactGCError("read staging receipt", err)
	}
	cutoff, cutoffErr := parseCanonicalStoreTime(cutoffText)
	after, afterErr := parseOptionalArtifactGCDigest(afterText)
	next := artifactdomain.GCStagingScanCursor{Generation: uint64(generation), Cutoff: cutoff,
		After: after, Done: done == 1}
	if generation <= 0 || cutoffErr != nil || afterErr != nil || (done != 0 && done != 1) ||
		examined < 0 || examined > artifactGCMaxCandidates || swept < 0 || swept > examined ||
		sweptBytes < 0 || sweptBytes > artifactGCMaxSweepBytes ||
		next.Generation != current.Generation || next.Cutoff != current.Cutoff ||
		(!current.After.IsZero() && next.After.IsZero()) ||
		(!current.After.IsZero() && !next.After.IsZero() && next.After.String() < current.After.String()) ||
		(examined == 0 && next.After != current.After) ||
		(examined > 0 && next.After == current.After) || !durableFound || next != durable {
		return artifactdomain.GCStagingSweepResult{}, false,
			artifactGCError("read staging receipt", errors.New("invalid durable receipt"))
	}
	return artifactdomain.GCStagingSweepResult{Next: next, Examined: examined,
		Swept: swept, SweptBytes: uint64(sweptBytes)}, true, nil
}

func requireCommittedArtifactGCProvenance(ctx context.Context, tx *sql.Tx) error {
	var corrupt int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM operation_artifact_roots r JOIN operations o ON o.operation_id=r.operation_id
		WHERE o.status='committed' AND NOT EXISTS (
			SELECT 1 FROM artifact_provenance p
			WHERE p.root_digest=r.root_digest AND p.operation_id=r.operation_id
			AND p.relation='local_capture'
		))`).Scan(&corrupt); err != nil {
		return artifactGCError("audit committed provenance", err)
	}
	if corrupt != 0 {
		return artifactGCError("audit committed provenance", errors.New("matching local provenance is missing"))
	}
	return nil
}

func readArtifactGCPrepareReceipt(ctx context.Context, tx *sql.Tx,
	request []byte, current, durable artifactdomain.GCScanCursor, durableFound bool,
) (artifactdomain.GCPrepareResult, bool, error) {
	var storedRequest []byte
	var cutoffText, afterText string
	var done, examined, protected, queued int
	var queuedBytes int64
	err := tx.QueryRowContext(ctx, `SELECT request,next_cutoff,next_after,next_done,examined,
		protected,queued,queued_bytes FROM artifact_gc_prepare_receipt WHERE singleton=1`).Scan(
		&storedRequest, &cutoffText, &afterText, &done, &examined, &protected, &queued, &queuedBytes)
	if errors.Is(err, sql.ErrNoRows) || err == nil && !bytes.Equal(storedRequest, request) {
		return artifactdomain.GCPrepareResult{}, false, nil
	}
	if err != nil {
		return artifactdomain.GCPrepareResult{}, false, artifactGCError("read prepare receipt", err)
	}
	cutoff, cutoffErr := parseCanonicalStoreTime(cutoffText)
	after, afterErr := parseOptionalArtifactGCDigest(afterText)
	next := artifactdomain.GCScanCursor{Cutoff: cutoff, After: after, Done: done == 1}
	if cutoffErr != nil || afterErr != nil || (done != 0 && done != 1) || examined < 0 ||
		examined > artifactGCMaxCandidates || protected < 0 || queued < 0 ||
		protected+queued != examined || queuedBytes < 0 || queuedBytes > artifactGCMaxSweepBytes ||
		next.Cutoff != current.Cutoff || (!current.After.IsZero() && next.After.IsZero()) ||
		(!current.After.IsZero() && !next.After.IsZero() && next.After.String() < current.After.String()) ||
		(examined == 0 && next.After != current.After) ||
		(examined > 0 && next.After == current.After) || !durableFound || next != durable {
		return artifactdomain.GCPrepareResult{}, false, artifactGCError("read prepare receipt", errors.New("invalid durable receipt"))
	}
	return artifactdomain.GCPrepareResult{Next: next,
		Examined: examined, Protected: protected, Queued: queued, QueuedBytes: uint64(queuedBytes)}, true, nil
}

type artifactGCScanner interface {
	Scan(...any) error
}

func scanArtifactGCQueue(row artifactGCScanner) (artifactdomain.GCQueueItem, error) {
	var digestText, stateText, modifiedText, queuedText, updatedText string
	var token []byte
	var size int64
	var renamedText sql.NullString
	if err := row.Scan(&digestText, &token, &stateText, &size, &modifiedText,
		&queuedText, &renamedText, &updatedText); err != nil {
		return artifactdomain.GCQueueItem{}, err
	}
	digest, digestErr := model.ParseDigest(digestText)
	modified, modifiedErr := parseCanonicalStoreTime(modifiedText)
	queued, queuedErr := parseCanonicalStoreTime(queuedText)
	updated, updatedErr := parseCanonicalStoreTime(updatedText)
	var identityToken [32]byte
	if len(token) == len(identityToken) {
		copy(identityToken[:], token)
	}
	var renamed time.Time
	var renamedErr error
	if renamedText.Valid {
		renamed, renamedErr = parseCanonicalStoreTime(renamedText.String)
	}
	state := artifactdomain.GCQueueState(stateText)
	if digestErr != nil || modifiedErr != nil || queuedErr != nil || updatedErr != nil || renamedErr != nil ||
		identityToken == ([32]byte{}) || size < 0 || size > artifactGCMaxObjectBytes ||
		!modified.Before(queued) || (state != artifactdomain.GCQueueQueued && state != artifactdomain.GCQueueRenamed) ||
		(state == artifactdomain.GCQueueQueued && (renamedText.Valid || updated != queued)) ||
		(state == artifactdomain.GCQueueRenamed && (!renamedText.Valid || renamed.Before(queued) || updated != renamed)) {
		return artifactdomain.GCQueueItem{}, artifactGCError("read queue", errors.New("invalid durable row"))
	}
	return artifactdomain.GCQueueItem{Identity: artifactdomain.GCQueueIdentity{Digest: digest, Token: identityToken},
		SizeBytes: uint64(size), ModifiedAt: modified, QueuedAt: queued, RenamedAt: renamed, State: state}, nil
}

func readArtifactGCQueueTx(ctx context.Context, tx *sql.Tx,
	digest model.Digest,
) (artifactdomain.GCQueueItem, bool, error) {
	row := tx.QueryRowContext(ctx, `SELECT digest,token,state,size_bytes,modified_at,
		queued_at,renamed_at,updated_at FROM artifact_gc_queue WHERE digest=?`, digest.String())
	item, err := scanArtifactGCQueue(row)
	if errors.Is(err, sql.ErrNoRows) {
		return artifactdomain.GCQueueItem{}, false, nil
	}
	if err != nil {
		return artifactdomain.GCQueueItem{}, false, err
	}
	return item, true, nil
}

func artifactGCDigestProtected(ctx context.Context, tx *sql.Tx,
	digest model.Digest, at time.Time,
) (bool, error) {
	roots := make(map[model.Digest]bool)
	manifestRows, err := tx.QueryContext(ctx, `SELECT root_digest FROM artifact_roots
		WHERE manifest_digest=? ORDER BY root_digest`, digest.Bytes())
	if err != nil {
		return false, artifactGCError("inspect manifest roots", err)
	}
	for manifestRows.Next() {
		var rootText string
		if err := manifestRows.Scan(&rootText); err != nil {
			manifestRows.Close()
			return false, artifactGCError("scan manifest roots", err)
		}
		root, err := model.ParseDigest(rootText)
		if err != nil {
			manifestRows.Close()
			return false, artifactGCError("scan manifest roots", err)
		}
		roots[root] = true
	}
	if err := manifestRows.Err(); err != nil {
		manifestRows.Close()
		return false, artifactGCError("iterate manifest roots", err)
	}
	if err := manifestRows.Close(); err != nil {
		return false, artifactGCError("close manifest roots", err)
	}
	rows, err := tx.QueryContext(ctx, `SELECT root_digest FROM artifact_root_blocks
		WHERE block_digest=? ORDER BY root_digest`, digest.String())
	if err != nil {
		return false, artifactGCError("inspect block roots", err)
	}
	for rows.Next() {
		var rootText string
		if err := rows.Scan(&rootText); err != nil {
			rows.Close()
			return false, artifactGCError("scan block roots", err)
		}
		root, err := model.ParseDigest(rootText)
		if err != nil {
			rows.Close()
			return false, artifactGCError("scan block roots", err)
		}
		roots[root] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return false, artifactGCError("iterate block roots", err)
	}
	if err := rows.Close(); err != nil {
		return false, artifactGCError("close block roots", err)
	}
	ordered := make([]model.Digest, 0, len(roots))
	for root := range roots {
		ordered = append(ordered, root)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].String() < ordered[j].String() })
	protected := false
	for _, root := range ordered {
		owned, err := artifactGCRootOwned(ctx, tx, root, at)
		if err != nil {
			return false, err
		}
		if owned || roots[root] {
			protected = true
		}
	}
	return protected, nil
}

func artifactGCRootOwned(ctx context.Context, tx *sql.Tx,
	root model.Digest, at time.Time,
) (bool, error) {
	var provenance, anyPins int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM artifact_provenance WHERE root_digest=?)`, root.String()).Scan(&provenance); err != nil {
		return false, artifactGCError("inspect provenance", err)
	}
	// Even an expired row remains a foreign-key owner until the bounded pin
	// cleanup has physically removed it. Treating that residual row as
	// unowned would make the root delete fail and roll back the pin cleanup,
	// permanently repeating the same batch ordering.
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM artifact_pins
		WHERE root_digest=?)`, root.String()).Scan(&anyPins); err != nil {
		return false, artifactGCError("inspect pins", err)
	}
	protected := provenance == 1 || anyPins == 1
	rows, err := tx.QueryContext(ctx, `SELECT o.operation_id,o.status,o.finished_at
		FROM operation_artifact_roots r JOIN operations o ON o.operation_id=r.operation_id
		WHERE r.root_digest=? ORDER BY o.operation_id`, root.String())
	if err != nil {
		return false, artifactGCError("inspect operation roots", err)
	}
	defer rows.Close()
	for rows.Next() {
		var operationID, status string
		var finished sql.NullString
		if err := rows.Scan(&operationID, &status, &finished); err != nil {
			return false, artifactGCError("scan operation root", err)
		}
		switch status {
		case "started":
			if finished.Valid {
				return false, artifactGCError("operation root", errors.New("started operation has finish time"))
			}
			protected = true
		case "rejected":
			if !finished.Valid {
				return false, artifactGCError("operation root", errors.New("rejected operation lacks finish time"))
			}
			finishedAt, err := parseCanonicalStoreTime(finished.String)
			if err != nil {
				return false, artifactGCError("operation root", err)
			}
			if at.Before(finishedAt.Add(artifactGCStagingRetention)) {
				protected = true
			}
		case "committed":
			if !finished.Valid {
				return false, artifactGCError("operation root", errors.New("committed operation lacks finish time"))
			}
			var matching int
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM artifact_provenance
				WHERE root_digest=? AND operation_id=? AND relation='local_capture')`,
				root.String(), operationID).Scan(&matching); err != nil {
				return false, artifactGCError("verify committed provenance", err)
			}
			if matching != 1 {
				return false, artifactGCError("verify committed provenance", errors.New("matching local provenance is missing"))
			}
			protected = true
		default:
			return false, artifactGCError("operation root", errors.New("unknown operation status"))
		}
	}
	if err := rows.Err(); err != nil {
		return false, artifactGCError("iterate operation roots", err)
	}
	return protected, nil
}

func artifactGCRootBlocks(ctx context.Context, tx *sql.Tx, root model.Digest) ([]model.Digest, error) {
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT block_digest FROM artifact_root_blocks
		WHERE root_digest=? ORDER BY block_digest`, root.String())
	if err != nil {
		return nil, artifactGCError("read staging blocks", err)
	}
	defer rows.Close()
	result := make([]model.Digest, 0)
	for rows.Next() {
		var text string
		if err := rows.Scan(&text); err != nil {
			return nil, artifactGCError("scan staging block", err)
		}
		digest, err := model.ParseDigest(text)
		if err != nil {
			return nil, artifactGCError("scan staging block", err)
		}
		result = append(result, digest)
	}
	if err := rows.Err(); err != nil {
		return nil, artifactGCError("iterate staging blocks", err)
	}
	return result, nil
}

func deleteOrphanArtifactGCBlock(ctx context.Context, tx *sql.Tx,
	block model.Digest, at time.Time,
) error {
	var maps int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM artifact_root_blocks
		WHERE block_digest=?`, block.String()).Scan(&maps); err != nil {
		return artifactGCError("inspect orphan block", err)
	}
	if maps != 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO artifact_gc_block_delete_guard(block_digest,authorized_at)
		VALUES(?,?)`, block.String(), storeTime(at)); err != nil {
		return artifactGCError("authorize orphan block delete", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM artifact_blocks WHERE block_digest=?`, block.String()); err != nil {
		return artifactGCError("delete orphan block", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM artifact_gc_block_delete_guard WHERE block_digest=?`,
		block.String()); err != nil {
		return artifactGCError("close orphan block delete", err)
	}
	return nil
}

func deleteQueuedArtifactGCBlock(ctx context.Context, tx *sql.Tx, digest model.Digest) error {
	var maps int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM artifact_root_blocks
		WHERE block_digest=?`, digest.String()).Scan(&maps); err != nil {
		return artifactGCError("mark inspect block maps", err)
	}
	if maps != 0 {
		return artifactGCError("mark renamed", errors.New("block remains mapped"))
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM artifact_blocks WHERE block_digest=?`, digest.String()); err != nil {
		return artifactGCError("mark delete block metadata", err)
	}
	return nil
}

func requireEmptyArtifactGCGuards(ctx context.Context, tx *sql.Tx) error {
	for _, table := range []string{"artifact_gc_delete_guard", "artifact_gc_block_delete_guard", "artifact_gc_completion_guard"} {
		var count int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil || count != 0 {
			return artifactGCError("inspect transient authority", err)
		}
	}
	return nil
}

func requireArtifactGCQueueAvailableForClosure(ctx context.Context, tx *sql.Tx,
	closure VerifiedArtifactClosure,
) error {
	for _, root := range closure.Roots {
		if err := requireArtifactGCDigestNotQueued(ctx, tx, root.ManifestDigest); err != nil {
			return err
		}
		if err := requireArtifactGCQueueAvailableForRoot(ctx, tx, root.RootDigest); err != nil {
			return err
		}
	}
	for _, block := range closure.Blocks {
		if err := requireArtifactGCDigestNotQueued(ctx, tx, block.Digest); err != nil {
			return err
		}
	}
	return nil
}

func requireArtifactGCQueueAvailableForRoot(ctx context.Context, tx *sql.Tx,
	root model.Digest,
) error {
	var manifestBytes []byte
	err := tx.QueryRowContext(ctx, `SELECT manifest_digest FROM artifact_roots
		WHERE root_digest=?`, root.String()).Scan(&manifestBytes)
	if err == nil {
		manifest, parseErr := model.DigestFromBytes(manifestBytes)
		if parseErr != nil {
			return artifactGCError("inspect root manifest", parseErr)
		}
		if err := requireArtifactGCDigestNotQueued(ctx, tx, manifest); err != nil {
			return err
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return artifactGCError("inspect root manifest", err)
	}
	var queuedBlock int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM artifact_root_blocks m JOIN artifact_gc_queue q ON q.digest=m.block_digest
		WHERE m.root_digest=?)`, root.String()).Scan(&queuedBlock); err != nil {
		return artifactGCError("inspect root queue ownership", err)
	}
	if queuedBlock != 0 {
		return artifactGCError("owner creation", errors.New("Artifact closure block is queued"))
	}
	return nil
}

func requireArtifactGCDigestNotQueued(ctx context.Context, tx *sql.Tx, digest model.Digest) error {
	var queued int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM artifact_gc_queue WHERE digest=?)`, digest.String()).Scan(&queued); err != nil {
		return artifactGCError("inspect queue ownership", err)
	}
	if queued != 0 {
		return artifactGCError("owner creation", errors.New("Artifact digest is queued"))
	}
	return nil
}

func parseOptionalArtifactGCDigest(text string) (model.Digest, error) {
	if text == "" {
		return model.Digest{}, nil
	}
	return model.ParseDigest(text)
}

func digestText(digest model.Digest) string {
	if digest.IsZero() {
		return ""
	}
	return digest.String()
}

func encodeArtifactGCPrepareRequest(spec artifactdomain.GCPrepareSpec) []byte {
	var buffer bytes.Buffer
	buffer.WriteByte(1)
	writeArtifactGCString(&buffer, storeTime(spec.Current.Cutoff))
	writeArtifactGCString(&buffer, digestText(spec.Current.After))
	buffer.WriteByte(byte(boolInt(spec.Current.Done)))
	_ = binary.Write(&buffer, binary.BigEndian, uint32(len(spec.Candidates)))
	for _, candidate := range spec.Candidates {
		writeArtifactGCString(&buffer, candidate.Digest.String())
		_ = binary.Write(&buffer, binary.BigEndian, candidate.SizeBytes)
		writeArtifactGCString(&buffer, storeTime(candidate.ModifiedAt))
		buffer.Write(candidate.Token[:])
	}
	buffer.WriteByte(byte(boolInt(spec.PageDone)))
	_ = binary.Write(&buffer, binary.BigEndian, uint32(spec.MaxQueued))
	_ = binary.Write(&buffer, binary.BigEndian, uint32(spec.MaxQueue))
	writeArtifactGCString(&buffer, storeTime(spec.At))
	return buffer.Bytes()
}

func encodeArtifactGCStagingRequest(spec artifactdomain.GCStagingSweepSpec) []byte {
	var buffer bytes.Buffer
	buffer.WriteByte(2)
	_ = binary.Write(&buffer, binary.BigEndian, spec.Current.Generation)
	writeArtifactGCString(&buffer, storeTime(spec.Current.Cutoff))
	writeArtifactGCString(&buffer, digestText(spec.Current.After))
	buffer.WriteByte(byte(boolInt(spec.Current.Done)))
	_ = binary.Write(&buffer, binary.BigEndian, uint32(spec.MaxItems))
	_ = binary.Write(&buffer, binary.BigEndian, spec.MaxBytes)
	writeArtifactGCString(&buffer, storeTime(spec.At))
	return buffer.Bytes()
}

func writeArtifactGCString(buffer *bytes.Buffer, value string) {
	_ = binary.Write(buffer, binary.BigEndian, uint32(len(value)))
	buffer.WriteString(value)
}

func artifactGCError(operation string, cause error) error {
	if cause == nil {
		cause = errors.New("mutation did not affect exact state")
	}
	return fmt.Errorf("%w: %s: %v", artifactdomain.ErrGCStoreInvariant, operation, cause)
}

var _ artifactdomain.GCStore = (*Store)(nil)
