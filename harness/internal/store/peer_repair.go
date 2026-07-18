package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrPeerRepairInput     = errors.New("invalid Peer repair input")
	ErrPeerRepairAuthority = errors.New("Peer repair authority is unavailable")
	ErrPeerRepairStale     = errors.New("Peer repair authority or generation changed")
	ErrPeerRepairInvariant = errors.New("durable Peer repair invariant violated")
)

const peerRepairAuthorityPause = 30 * time.Second

// PeerRepairStatus is the closed durable anti-entropy state. A repair row is a
// checkpoint, not a queued task: ReadPeerRepairTargets derives due work from
// current signed authority and the current inbound cursor on every snapshot.
type PeerRepairStatus string

const (
	PeerRepairReady    PeerRepairStatus = "ready"
	PeerRepairProgress PeerRepairStatus = "progress"
	PeerRepairCaughtUp PeerRepairStatus = "caught_up"
	PeerRepairRetry    PeerRepairStatus = "retry"
	PeerRepairPaused   PeerRepairStatus = "paused"
	PeerRepairTerminal PeerRepairStatus = "terminal"
)

func (status PeerRepairStatus) valid() bool {
	switch status {
	case PeerRepairReady, PeerRepairProgress, PeerRepairCaughtUp, PeerRepairRetry, PeerRepairPaused,
		PeerRepairTerminal:
		return true
	default:
		return false
	}
}

// PeerRepairDiagnostic is deliberately a protocol-sized code instead of a
// free-text network error. It determines whether a repair may be scheduled
// again after restart.
type PeerRepairDiagnostic string

const (
	PeerRepairDiagnosticBusy                 PeerRepairDiagnostic = "busy"
	PeerRepairDiagnosticTransportUnavailable PeerRepairDiagnostic = "transport_unavailable"
	PeerRepairDiagnosticProtocolInvalid      PeerRepairDiagnostic = "protocol_invalid"
	PeerRepairDiagnosticHistoryGap           PeerRepairDiagnostic = "history_gap"
	PeerRepairDiagnosticNotOrigin            PeerRepairDiagnostic = "not_origin"
	PeerRepairDiagnosticNotMember            PeerRepairDiagnostic = "not_member"
	PeerRepairDiagnosticMemberRevoked        PeerRepairDiagnostic = "member_revoked"
	PeerRepairDiagnosticChannelClosed        PeerRepairDiagnostic = "channel_closed"
	PeerRepairDiagnosticOriginEpochMismatch  PeerRepairDiagnostic = "origin_epoch_mismatch"
)

func (diagnostic PeerRepairDiagnostic) retryable() bool {
	return diagnostic == PeerRepairDiagnosticBusy ||
		diagnostic == PeerRepairDiagnosticTransportUnavailable
}

func (diagnostic PeerRepairDiagnostic) terminal() bool {
	switch diagnostic {
	case PeerRepairDiagnosticProtocolInvalid, PeerRepairDiagnosticHistoryGap,
		PeerRepairDiagnosticOriginEpochMismatch:
		return true
	default:
		return false
	}
}

func (diagnostic PeerRepairDiagnostic) pausesAuthority() bool {
	switch diagnostic {
	case PeerRepairDiagnosticNotOrigin, PeerRepairDiagnosticNotMember,
		PeerRepairDiagnosticMemberRevoked, PeerRepairDiagnosticChannelClosed:
		return true
	default:
		return false
	}
}

// PeerRepairTarget is an opaque, authority-fenced instruction for one direct
// origin Pull. Returning it to CommitPeerRepair prevents a result obtained
// under an old roster/binding generation from mutating the current checkpoint.
type PeerRepairTarget struct {
	channelID       model.ChannelID
	originPeerID    model.PeerID
	originEpoch     model.OriginEpoch
	rosterHead      model.RecordHead
	memberHead      model.RecordHead
	originKeyDigest model.Digest
	authorityDigest model.Digest
	baseline        uint64
	contiguous      uint64
	observed        uint64
	status          PeerRepairStatus
	generation      uint64
	retryCount      uint64
	sourceFloor     uint64
	hasSourceFloor  bool
	sourceHead      uint64
	hasSourceHead   bool
	diagnostic      PeerRepairDiagnostic
	pausedAuthority model.Digest
	nextAttemptAt   time.Time
	updatedAt       time.Time
}

func (target PeerRepairTarget) ChannelID() model.ChannelID        { return target.channelID }
func (target PeerRepairTarget) OriginPeerID() model.PeerID        { return target.originPeerID }
func (target PeerRepairTarget) OriginEpoch() model.OriginEpoch    { return target.originEpoch }
func (target PeerRepairTarget) RosterHead() model.RecordHead      { return target.rosterHead }
func (target PeerRepairTarget) MemberHead() model.RecordHead      { return target.memberHead }
func (target PeerRepairTarget) BaselineChannelSequence() uint64   { return target.baseline }
func (target PeerRepairTarget) ContiguousChannelSequence() uint64 { return target.contiguous }
func (target PeerRepairTarget) ObservedChannelSequence() uint64   { return target.observed }
func (target PeerRepairTarget) Status() PeerRepairStatus          { return target.status }
func (target PeerRepairTarget) Generation() uint64                { return target.generation }
func (target PeerRepairTarget) RetryCount() uint64                { return target.retryCount }
func (target PeerRepairTarget) Diagnostic() PeerRepairDiagnostic  { return target.diagnostic }
func (target PeerRepairTarget) NextAttemptAt() time.Time          { return target.nextAttemptAt }
func (target PeerRepairTarget) UpdatedAt() time.Time              { return target.updatedAt }

func (target PeerRepairTarget) SourceFloor() (uint64, bool) {
	return target.sourceFloor, target.hasSourceFloor
}

func (target PeerRepairTarget) SourceHead() (uint64, bool) {
	return target.sourceHead, target.hasSourceHead
}

// CommitPeerRepairSpec records exactly one page observation, transient retry,
// or permanent protocol result. ContiguousChannelSequence is reread and CASed
// after PutPeerInboxPage, so a page commit may legitimately advance beyond the
// cursor captured in Target while concurrent progress still makes this stale.
type CommitPeerRepairSpec struct {
	Target                    PeerRepairTarget
	Status                    PeerRepairStatus
	ContiguousChannelSequence uint64
	SourceFloor               uint64
	SourceHead                uint64
	Diagnostic                PeerRepairDiagnostic
	NextAttemptAt             time.Time
	At                        time.Time
}

type CommitPeerRepairResult struct {
	Target   PeerRepairTarget
	Changed  bool
	Replayed bool
}

// ReadPeerRepairTargets returns all currently due origin repairs in stable
// ChannelID/PeerID order from one read-only SQLite snapshot. Terminal repair
// evidence and permanent origin-conflict fences are retained but never emitted.
func (s *Store) ReadPeerRepairTargets(ctx context.Context, atValue time.Time) ([]PeerRepairTarget, error) {
	if s == nil || s.db == nil || ctx == nil {
		return nil, ErrPeerRepairInput
	}
	at, err := canonicalStoreTime(atValue)
	if err != nil || at.IsZero() {
		return nil, ErrPeerRepairInput
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("read Peer repair targets: begin: %w", err)
	}
	defer tx.Rollback()
	node, err := readNode(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("%w: Node: %v", ErrPeerRepairInvariant, err)
	}
	channelIDs, err := readChannelMeshIDs(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPeerRepairInvariant, err)
	}
	targets := make([]PeerRepairTarget, 0, model.MaxChannelsPerNode*(model.MaxMembersPerChannel-1))
	for _, channelID := range channelIDs {
		authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), channelID)
		if err != nil {
			return nil, fmt.Errorf("%w: Channel %q: %v", ErrPeerRepairInvariant,
				channelID.String(), err)
		}
		if authority.channel.Status() != model.ChannelActive ||
			authority.channel.TopicState() != model.TopicJoined {
			continue
		}
		for _, binding := range authority.bindings {
			if binding.State() != model.BindingActive {
				continue
			}
			target, fenced, err := readPeerRepairTarget(ctx, tx, authority, binding, at)
			if err != nil {
				return nil, err
			}
			if fenced || target.status == PeerRepairTerminal {
				continue
			}
			if target.status == PeerRepairPaused {
				if target.pausedAuthority == target.authorityDigest && target.nextAttemptAt.After(at) {
					continue
				}
			} else if target.nextAttemptAt.After(at) {
				continue
			}
			targets = append(targets, target)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("read Peer repair targets: commit: %w", err)
	}
	return targets, nil
}

// CommitPeerRepair advances one exact durable generation. Permanent protocol
// results become immutable terminal evidence, so restart cannot turn them into
// an unbounded retry loop.
func (s *Store) CommitPeerRepair(ctx context.Context,
	spec CommitPeerRepairSpec,
) (CommitPeerRepairResult, error) {
	at, nextAttempt, err := validateCommitPeerRepairSpec(s, ctx, spec)
	if err != nil {
		return CommitPeerRepairResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CommitPeerRepairResult{}, fmt.Errorf("commit Peer repair: begin: %w", err)
	}
	defer tx.Rollback()
	current, err := readCurrentPeerRepairTarget(ctx, tx, spec.Target, spec.ContiguousChannelSequence, at)
	if err != nil {
		return CommitPeerRepairResult{}, err
	}
	if current.generation == spec.Target.generation+1 {
		replayBase := spec.Target
		replayBase.contiguous = current.contiguous
		replayBase.observed = current.observed
		replayed, replayErr := nextPeerRepairTarget(replayBase, spec, at, nextAttempt)
		if replayErr == nil && samePeerRepairResult(current, replayed) {
			if err := tx.Commit(); err != nil {
				return CommitPeerRepairResult{}, fmt.Errorf("commit Peer repair replay: %w", err)
			}
			return CommitPeerRepairResult{Target: current, Replayed: true}, nil
		}
		return CommitPeerRepairResult{}, ErrPeerRepairStale
	}
	if current.generation != spec.Target.generation || current.status != spec.Target.status ||
		current.retryCount != spec.Target.retryCount || !current.updatedAt.Equal(spec.Target.updatedAt) {
		return CommitPeerRepairResult{}, ErrPeerRepairStale
	}
	next, err := nextPeerRepairTarget(current, spec, at, nextAttempt)
	if err != nil {
		return CommitPeerRepairResult{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE peer_repairs SET status=?,generation=?,retry_count=?,
		source_floor_channel_seq=?,source_head_channel_seq=?,diagnostic_code=?,paused_authority_digest=?,
		next_attempt_at=?,updated_at=?
		WHERE channel_id=? AND origin_peer_id=? AND origin_epoch=? AND generation=? AND status=?
		AND retry_count=? AND updated_at=?`, string(next.status), next.generation, next.retryCount,
		nullRepairSequence(next.sourceFloor, next.hasSourceFloor),
		nullRepairSequence(next.sourceHead, next.hasSourceHead), nullRepairDiagnostic(next.diagnostic),
		nullRepairDigest(next.pausedAuthority), nullRepairTime(next.nextAttemptAt),
		storeTime(next.updatedAt), current.channelID.String(),
		current.originPeerID.String(), current.originEpoch.String(), current.generation,
		string(current.status), current.retryCount, storeTime(current.updatedAt))
	if err != nil {
		return CommitPeerRepairResult{}, fmt.Errorf("commit Peer repair: update: %w", err)
	}
	if err := requireExactlyOneRow(result, "commit Peer repair CAS"); err != nil {
		return CommitPeerRepairResult{}, fmt.Errorf("%w: %v", ErrPeerRepairStale, err)
	}
	if err := tx.Commit(); err != nil {
		return CommitPeerRepairResult{}, fmt.Errorf("commit Peer repair: commit: %w", err)
	}
	return CommitPeerRepairResult{Target: next, Changed: true}, nil
}

func readPeerRepairTarget(ctx context.Context, tx *sql.Tx, authority verifiedChannelAuthority,
	binding model.PeerBinding, at time.Time,
) (PeerRepairTarget, bool, error) {
	cursor, err := readPeerCursor(ctx, tx, authority.channel.ID(), binding.PeerID(), binding.OriginEpoch())
	if err != nil || cursor.BaselineChannelSequence > cursor.ContiguousChannelSequence ||
		cursor.UpdatedAt.Before(binding.JoinedAt()) || cursor.UpdatedAt.After(at) {
		return PeerRepairTarget{}, false, fmt.Errorf("%w: Channel %q origin %q cursor: %v",
			ErrPeerRepairInvariant, authority.channel.ID().String(), binding.PeerID().String(), err)
	}
	var fenced int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM origin_quarantines
		WHERE channel_id=? AND origin_peer_id=? AND origin_epoch=?)`, authority.channel.ID().String(),
		binding.PeerID().String(), binding.OriginEpoch().String()).Scan(&fenced); err != nil ||
		(fenced != 0 && fenced != 1) {
		return PeerRepairTarget{}, false, fmt.Errorf("%w: read permanent origin fence: %v",
			ErrPeerRepairInvariant, err)
	}
	target, err := scanPeerRepairTarget(ctx, tx, authority, binding, cursor)
	if err != nil {
		return PeerRepairTarget{}, false, err
	}
	if target.updatedAt.After(at) || target.nextAttemptAt.IsZero() && target.status != PeerRepairTerminal {
		return PeerRepairTarget{}, false, fmt.Errorf("%w: repair checkpoint time is invalid",
			ErrPeerRepairInvariant)
	}
	return target, fenced == 1, nil
}

func scanPeerRepairTarget(ctx context.Context, tx *sql.Tx, authority verifiedChannelAuthority,
	binding model.PeerBinding, cursor PeerCursorProjection,
) (PeerRepairTarget, error) {
	var baseline, generation, retryCount uint64
	var statusText, updatedText string
	var floor, head sql.NullInt64
	var diagnostic, nextAttempt sql.NullString
	var pausedAuthorityRaw []byte
	err := tx.QueryRowContext(ctx, `SELECT baseline_channel_seq,status,generation,retry_count,
		source_floor_channel_seq,source_head_channel_seq,diagnostic_code,paused_authority_digest,
		next_attempt_at,updated_at
		FROM peer_repairs WHERE channel_id=? AND origin_peer_id=? AND origin_epoch=?`,
		authority.channel.ID().String(), binding.PeerID().String(), binding.OriginEpoch().String()).
		Scan(&baseline, &statusText, &generation, &retryCount, &floor, &head, &diagnostic,
			&pausedAuthorityRaw,
			&nextAttempt, &updatedText)
	if err != nil {
		return PeerRepairTarget{}, fmt.Errorf("%w: Channel %q origin %q checkpoint: %v",
			ErrPeerRepairInvariant, authority.channel.ID().String(), binding.PeerID().String(), err)
	}
	status := PeerRepairStatus(statusText)
	updatedAt, parseErr := parseCanonicalStoreTime(updatedText)
	if parseErr != nil || !status.valid() || baseline != cursor.BaselineChannelSequence ||
		generation > model.MaxSQLiteInteger || retryCount > generation ||
		(floor.Valid && (floor.Int64 <= 0 || uint64(floor.Int64) > model.MaxSQLiteInteger)) ||
		(head.Valid && (head.Int64 < 0 || uint64(head.Int64) > model.MaxSQLiteInteger)) {
		return PeerRepairTarget{}, fmt.Errorf("%w: invalid repair checkpoint projection: %v",
			ErrPeerRepairInvariant, parseErr)
	}
	var nextAttemptAt time.Time
	if nextAttempt.Valid {
		nextAttemptAt, parseErr = parseCanonicalStoreTime(nextAttempt.String)
		if parseErr != nil || nextAttemptAt.Before(updatedAt) {
			return PeerRepairTarget{}, fmt.Errorf("%w: invalid repair schedule: %v",
				ErrPeerRepairInvariant, parseErr)
		}
	}
	diagnosticCode := PeerRepairDiagnostic(diagnostic.String)
	var pausedAuthority model.Digest
	if len(pausedAuthorityRaw) != 0 {
		pausedAuthority, parseErr = model.DigestFromBytes(pausedAuthorityRaw)
		if parseErr != nil {
			return PeerRepairTarget{}, fmt.Errorf("%w: invalid paused authority digest: %v",
				ErrPeerRepairInvariant, parseErr)
		}
	}
	if !validPeerRepairProjection(status, generation, retryCount, floor.Valid, head.Valid,
		diagnostic.Valid, diagnosticCode, !pausedAuthority.IsZero(), nextAttempt.Valid) {
		return PeerRepairTarget{}, fmt.Errorf("%w: inconsistent repair status", ErrPeerRepairInvariant)
	}
	if status == PeerRepairReady && uint64(head.Int64) != baseline ||
		(status == PeerRepairProgress || status == PeerRepairCaughtUp) &&
			uint64(floor.Int64) > uint64(head.Int64)+1 {
		return PeerRepairTarget{}, fmt.Errorf("%w: inconsistent repair source range",
			ErrPeerRepairInvariant)
	}
	authorityDigest, err := peerRepairAuthorityDigest(authority.channel.RosterHead(), binding,
		cursor.BaselineChannelSequence)
	if err != nil {
		return PeerRepairTarget{}, fmt.Errorf("%w: authority digest: %v", ErrPeerRepairInvariant, err)
	}
	return PeerRepairTarget{channelID: authority.channel.ID(), originPeerID: binding.PeerID(),
		originEpoch: binding.OriginEpoch(), rosterHead: authority.channel.RosterHead(),
		memberHead: binding.MemberHead(), originKeyDigest: model.Sum(binding.PublicKey()),
		authorityDigest: authorityDigest,
		baseline:        baseline, contiguous: cursor.ContiguousChannelSequence,
		observed: cursor.ObservedChannelSequence, status: status, generation: generation,
		retryCount: retryCount, sourceFloor: uint64(floor.Int64), hasSourceFloor: floor.Valid,
		sourceHead: uint64(head.Int64), hasSourceHead: head.Valid, diagnostic: diagnosticCode,
		pausedAuthority: pausedAuthority,
		nextAttemptAt:   nextAttemptAt, updatedAt: updatedAt}, nil
}

func validPeerRepairProjection(status PeerRepairStatus, generation, retryCount uint64,
	hasFloor, hasHead, hasDiagnostic bool, diagnostic PeerRepairDiagnostic, hasPausedAuthority,
	hasNext bool,
) bool {
	switch status {
	case PeerRepairReady:
		return generation == 0 && retryCount == 0 && !hasFloor && hasHead &&
			!hasDiagnostic && !hasPausedAuthority && hasNext
	case PeerRepairProgress, PeerRepairCaughtUp:
		return generation > 0 && hasFloor && hasHead && !hasDiagnostic && !hasPausedAuthority && hasNext
	case PeerRepairRetry:
		return generation > 0 && retryCount > 0 && hasDiagnostic && diagnostic.retryable() &&
			!hasPausedAuthority && hasNext
	case PeerRepairPaused:
		return generation > 0 && retryCount > 0 && hasDiagnostic && diagnostic.pausesAuthority() &&
			hasPausedAuthority && hasNext
	case PeerRepairTerminal:
		return generation > 0 && hasDiagnostic && diagnostic.terminal() && !hasPausedAuthority && !hasNext &&
			(diagnostic != PeerRepairDiagnosticHistoryGap || hasFloor)
	default:
		return false
	}
}

func readCurrentPeerRepairTarget(ctx context.Context, tx *sql.Tx, expected PeerRepairTarget,
	contiguous uint64, at time.Time,
) (PeerRepairTarget, error) {
	if expected.channelID.IsZero() || expected.originPeerID.IsZero() || expected.originEpoch.IsZero() ||
		expected.rosterHead.IsZero() || expected.memberHead.IsZero() || expected.originKeyDigest.IsZero() {
		return PeerRepairTarget{}, ErrPeerRepairInput
	}
	node, err := readNode(ctx, tx)
	if err != nil {
		return PeerRepairTarget{}, fmt.Errorf("%w: Node: %v", ErrPeerRepairInvariant, err)
	}
	authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), expected.channelID)
	if err != nil {
		return PeerRepairTarget{}, fmt.Errorf("%w: %v", ErrPeerRepairAuthority, err)
	}
	if authority.channel.Status() != model.ChannelActive ||
		authority.channel.TopicState() != model.TopicJoined ||
		authority.channel.RosterHead() != expected.rosterHead {
		return PeerRepairTarget{}, ErrPeerRepairStale
	}
	var binding model.PeerBinding
	for _, candidate := range authority.bindings {
		if candidate.PeerID() == expected.originPeerID {
			binding = candidate
			break
		}
	}
	if binding.PeerID().IsZero() || binding.State() != model.BindingActive ||
		binding.OriginEpoch() != expected.originEpoch || binding.MemberHead() != expected.memberHead ||
		model.Sum(binding.PublicKey()) != expected.originKeyDigest {
		return PeerRepairTarget{}, ErrPeerRepairStale
	}
	cursor, err := readPeerCursor(ctx, tx, expected.channelID, expected.originPeerID, expected.originEpoch)
	if err != nil {
		return PeerRepairTarget{}, fmt.Errorf("%w: cursor: %v", ErrPeerRepairInvariant, err)
	}
	if cursor.BaselineChannelSequence != expected.baseline ||
		cursor.ContiguousChannelSequence != contiguous || cursor.UpdatedAt.After(at) {
		return PeerRepairTarget{}, ErrPeerRepairStale
	}
	var fenced int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM origin_quarantines
		WHERE channel_id=? AND origin_peer_id=? AND origin_epoch=?)`, expected.channelID.String(),
		expected.originPeerID.String(), expected.originEpoch.String()).Scan(&fenced); err != nil {
		return PeerRepairTarget{}, fmt.Errorf("%w: permanent origin fence: %v", ErrPeerRepairInvariant, err)
	}
	if fenced != 0 {
		return PeerRepairTarget{}, ErrPeerRepairStale
	}
	return scanPeerRepairTarget(ctx, tx, authority, binding, cursor)
}

func validateCommitPeerRepairSpec(s *Store, ctx context.Context,
	spec CommitPeerRepairSpec,
) (time.Time, time.Time, error) {
	if s == nil || s.db == nil || ctx == nil || spec.ContiguousChannelSequence > model.MaxSQLiteInteger ||
		spec.Target.generation >= model.MaxSQLiteInteger {
		return time.Time{}, time.Time{}, ErrPeerRepairInput
	}
	at, err := canonicalStoreTime(spec.At)
	if err != nil || at.IsZero() {
		return time.Time{}, time.Time{}, ErrPeerRepairInput
	}
	var next time.Time
	if !spec.NextAttemptAt.IsZero() {
		next, err = canonicalStoreTime(spec.NextAttemptAt)
		if err != nil || next.Before(at) {
			return time.Time{}, time.Time{}, ErrPeerRepairInput
		}
	}
	switch spec.Status {
	case PeerRepairProgress, PeerRepairCaughtUp:
		if spec.SourceFloor == 0 || spec.SourceFloor > model.MaxSQLiteInteger ||
			spec.SourceHead > model.MaxSQLiteInteger || spec.SourceFloor > spec.SourceHead+1 ||
			spec.Diagnostic != "" || next.IsZero() ||
			(spec.Status == PeerRepairProgress && spec.ContiguousChannelSequence >= spec.SourceHead) ||
			(spec.Status == PeerRepairCaughtUp && spec.ContiguousChannelSequence < spec.SourceHead) ||
			spec.SourceFloor > spec.ContiguousChannelSequence+1 {
			return time.Time{}, time.Time{}, ErrPeerRepairInput
		}
	case PeerRepairRetry:
		if !spec.Diagnostic.retryable() || next.IsZero() || spec.SourceFloor != 0 || spec.SourceHead != 0 {
			return time.Time{}, time.Time{}, ErrPeerRepairInput
		}
	case PeerRepairPaused:
		if !spec.Diagnostic.pausesAuthority() || !next.IsZero() || spec.SourceFloor != 0 ||
			spec.SourceHead != 0 {
			return time.Time{}, time.Time{}, ErrPeerRepairInput
		}
		next, err = canonicalStoreTime(at.Add(peerRepairAuthorityPause))
		if err != nil {
			return time.Time{}, time.Time{}, ErrPeerRepairInput
		}
	case PeerRepairTerminal:
		if !spec.Diagnostic.terminal() || !next.IsZero() || spec.SourceHead != 0 ||
			(spec.Diagnostic == PeerRepairDiagnosticHistoryGap) != (spec.SourceFloor > 0) ||
			spec.SourceFloor > model.MaxSQLiteInteger {
			return time.Time{}, time.Time{}, ErrPeerRepairInput
		}
	default:
		return time.Time{}, time.Time{}, ErrPeerRepairInput
	}
	return at, next, nil
}

func nextPeerRepairTarget(current PeerRepairTarget, spec CommitPeerRepairSpec, at, next time.Time,
) (PeerRepairTarget, error) {
	if at.Before(current.updatedAt) {
		return PeerRepairTarget{}, ErrPeerRepairInput
	}
	result := current
	result.status = spec.Status
	result.generation++
	result.contiguous = spec.ContiguousChannelSequence
	result.diagnostic = spec.Diagnostic
	result.pausedAuthority = model.Digest{}
	result.nextAttemptAt = next
	result.updatedAt = at
	if spec.Status == PeerRepairRetry || spec.Status == PeerRepairPaused {
		result.retryCount++
	}
	if spec.Status == PeerRepairPaused {
		result.pausedAuthority = current.authorityDigest
	}
	if spec.Status == PeerRepairProgress || spec.Status == PeerRepairCaughtUp {
		if current.hasSourceFloor && spec.SourceFloor < current.sourceFloor ||
			current.hasSourceHead && spec.SourceHead < current.sourceHead {
			return PeerRepairTarget{}, ErrPeerRepairInvariant
		}
		result.sourceFloor, result.hasSourceFloor = spec.SourceFloor, true
		result.sourceHead, result.hasSourceHead = spec.SourceHead, true
	}
	if spec.Status == PeerRepairTerminal && spec.Diagnostic == PeerRepairDiagnosticHistoryGap {
		if current.hasSourceFloor && spec.SourceFloor < current.sourceFloor {
			return PeerRepairTarget{}, ErrPeerRepairInvariant
		}
		result.sourceFloor, result.hasSourceFloor = spec.SourceFloor, true
	}
	return result, nil
}

func samePeerRepairResult(left, right PeerRepairTarget) bool {
	return left.status == right.status && left.generation == right.generation &&
		left.retryCount == right.retryCount && left.sourceFloor == right.sourceFloor &&
		left.hasSourceFloor == right.hasSourceFloor && left.sourceHead == right.sourceHead &&
		left.hasSourceHead == right.hasSourceHead && left.diagnostic == right.diagnostic &&
		left.pausedAuthority == right.pausedAuthority &&
		left.nextAttemptAt.Equal(right.nextAttemptAt) && left.updatedAt.Equal(right.updatedAt)
}

func nullRepairSequence(value uint64, present bool) any {
	if !present {
		return nil
	}
	return value
}

func nullRepairDiagnostic(value PeerRepairDiagnostic) any {
	if value == "" {
		return nil
	}
	return string(value)
}

func nullRepairTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return storeTime(value)
}

func nullRepairDigest(value model.Digest) any {
	if value.IsZero() {
		return nil
	}
	return value.Bytes()
}

func peerRepairAuthorityDigest(rosterHead model.RecordHead, binding model.PeerBinding,
	baseline uint64,
) (model.Digest, error) {
	canonical, err := model.JSONFrom(struct {
		Baseline    uint64            `json:"baseline_channel_seq"`
		MemberHead  model.RecordHead  `json:"member_head"`
		OriginEpoch model.OriginEpoch `json:"origin_epoch"`
		OriginKey   model.Digest      `json:"origin_key_digest"`
		RosterHead  model.RecordHead  `json:"roster_head"`
	}{Baseline: baseline, MemberHead: binding.MemberHead(), OriginEpoch: binding.OriginEpoch(),
		OriginKey: model.Sum(binding.PublicKey()), RosterHead: rosterHead})
	if err != nil {
		return model.Digest{}, err
	}
	return model.Sum(canonical.Bytes()), nil
}
