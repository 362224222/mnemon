package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	peerInboxArtifactLease = 120 * time.Second
	minArtifactRetry       = time.Second
	maxArtifactRetry       = 300 * time.Second
)

var (
	ErrPeerInboxArtifactInput     = errors.New("invalid Peer Inbox Artifact worker input")
	ErrPeerInboxArtifactAuthority = errors.New("Peer Inbox Artifact authority is unavailable")
	ErrPeerInboxArtifactStale     = errors.New("Peer Inbox Artifact lease is stale")
	ErrPeerInboxArtifactNotReady  = errors.New("Peer Inbox Artifact closure is not ready")
	ErrPeerInboxArtifactLimit     = errors.New("Peer Inbox Artifact closure exceeds limits")
	ErrPeerInboxArtifactInvariant = errors.New("Peer Inbox Artifact durable invariant violated")
)

// PeerInboxArtifactRetryDiagnostic is the closed set of transient failures
// that remain owned by the Artifact phase. A generic Inbox retry is not
// claimable by the Artifact worker.
type PeerInboxArtifactRetryDiagnostic string

const (
	PeerInboxArtifactRetryBusy                 PeerInboxArtifactRetryDiagnostic = "artifact_busy"
	PeerInboxArtifactRetryTransportUnavailable PeerInboxArtifactRetryDiagnostic = "artifact_transport_unavailable"
	PeerInboxArtifactRetryTimeout              PeerInboxArtifactRetryDiagnostic = "artifact_timeout"
	PeerInboxArtifactRetryNotAuthorized        PeerInboxArtifactRetryDiagnostic = "artifact_not_authorized"
	PeerInboxArtifactRetryResourceExhausted    PeerInboxArtifactRetryDiagnostic = "artifact_resource_exhausted"
)

func (diagnostic PeerInboxArtifactRetryDiagnostic) Valid() bool {
	switch diagnostic {
	case PeerInboxArtifactRetryBusy, PeerInboxArtifactRetryTransportUnavailable,
		PeerInboxArtifactRetryTimeout, PeerInboxArtifactRetryNotAuthorized,
		PeerInboxArtifactRetryResourceExhausted:
		return true
	default:
		return false
	}
}

// PeerInboxArtifactPermanentDiagnostic is the closed remote-failure set that
// may terminally quarantine one Inbox row. It intentionally contains no
// local filesystem, SQL, or implementation errors.
type PeerInboxArtifactPermanentDiagnostic string

const (
	PeerInboxArtifactProtocolInvalid PeerInboxArtifactPermanentDiagnostic = "artifact_protocol_invalid"
	PeerInboxArtifactManifestInvalid PeerInboxArtifactPermanentDiagnostic = "artifact_manifest_invalid"
	PeerInboxArtifactDigestMismatch  PeerInboxArtifactPermanentDiagnostic = "artifact_digest_mismatch"
	PeerInboxArtifactLimitExceeded   PeerInboxArtifactPermanentDiagnostic = "artifact_limit_exceeded"
)

func (diagnostic PeerInboxArtifactPermanentDiagnostic) Valid() bool {
	switch diagnostic {
	case PeerInboxArtifactProtocolInvalid, PeerInboxArtifactManifestInvalid,
		PeerInboxArtifactDigestMismatch, PeerInboxArtifactLimitExceeded:
		return true
	default:
		return false
	}
}

// PeerInboxArtifactFence is an opaque durable capability. Callers can inspect
// it and return it to a settlement API, but cannot construct a different live
// Inbox generation through exported fields.
type PeerInboxArtifactFence struct {
	inboxID    model.InboxID
	leaseOwner string
	leaseUntil time.Time
	attempt    uint32
}

func (fence PeerInboxArtifactFence) InboxID() model.InboxID { return fence.inboxID }
func (fence PeerInboxArtifactFence) LeaseOwner() string     { return fence.leaseOwner }
func (fence PeerInboxArtifactFence) LeaseUntil() time.Time  { return fence.leaseUntil }
func (fence PeerInboxArtifactFence) Attempt() uint32        { return fence.attempt }

// PeerInboxArtifactClaim is the immutable network instruction paired with one
// exact lease fence. Slice and byte-backed values are returned defensively by
// their model accessors.
type PeerInboxArtifactClaim struct {
	fence         PeerInboxArtifactFence
	publication   model.SignedPublication
	channelID     model.ChannelID
	originPeerID  model.PeerID
	originEpoch   model.OriginEpoch
	requiredRoots []model.Digest
}

func (claim PeerInboxArtifactClaim) Fence() PeerInboxArtifactFence        { return claim.fence }
func (claim PeerInboxArtifactClaim) InboxID() model.InboxID               { return claim.fence.inboxID }
func (claim PeerInboxArtifactClaim) Publication() model.SignedPublication { return claim.publication }
func (claim PeerInboxArtifactClaim) ChannelID() model.ChannelID           { return claim.channelID }
func (claim PeerInboxArtifactClaim) OriginPeerID() model.PeerID           { return claim.originPeerID }
func (claim PeerInboxArtifactClaim) OriginEpoch() model.OriginEpoch       { return claim.originEpoch }
func (claim PeerInboxArtifactClaim) RequiredArtifactRoots() []model.Digest {
	return append([]model.Digest(nil), claim.requiredRoots...)
}

type ClaimPeerInboxArtifactSpec struct {
	LeaseOwner string
	At         time.Time
}

type PeerInboxArtifactClaimResult struct {
	claim PeerInboxArtifactClaim
	found bool
}

func (result PeerInboxArtifactClaimResult) Found() bool { return result.found }
func (result PeerInboxArtifactClaimResult) Claim() PeerInboxArtifactClaim {
	return result.claim
}

type RenewPeerInboxArtifactSpec struct {
	Fence PeerInboxArtifactFence
	At    time.Time
}

type RetryPeerInboxArtifactSpec struct {
	Fence      PeerInboxArtifactFence
	Diagnostic PeerInboxArtifactRetryDiagnostic
	RetryAfter time.Duration
	At         time.Time
}

type QuarantinePeerInboxArtifactSpec struct {
	Fence      PeerInboxArtifactFence
	Diagnostic PeerInboxArtifactPermanentDiagnostic
	At         time.Time
}

type MarkPeerInboxArtifactReadySpec struct {
	Fence PeerInboxArtifactFence
	At    time.Time
}

type ReadPeerInboxArtifactRootSpec struct {
	Fence      PeerInboxArtifactFence
	RootDigest model.Digest
	At         time.Time
}

type PeerInboxArtifactRenewal struct {
	fence    PeerInboxArtifactFence
	changed  bool
	replayed bool
}

func (result PeerInboxArtifactRenewal) Fence() PeerInboxArtifactFence { return result.fence }
func (result PeerInboxArtifactRenewal) Changed() bool                 { return result.changed }
func (result PeerInboxArtifactRenewal) Replayed() bool                { return result.replayed }

type PeerInboxArtifactSettlement struct {
	status        model.InboxStatus
	nextAttemptAt time.Time
	changed       bool
	replayed      bool
}

func (result PeerInboxArtifactSettlement) Status() model.InboxStatus { return result.status }
func (result PeerInboxArtifactSettlement) NextAttemptAt() time.Time  { return result.nextAttemptAt }
func (result PeerInboxArtifactSettlement) Changed() bool             { return result.changed }
func (result PeerInboxArtifactSettlement) Replayed() bool            { return result.replayed }

type peerInboxArtifactRow struct {
	inboxID       model.InboxID
	channelID     model.ChannelID
	originPeerID  model.PeerID
	originEpoch   model.OriginEpoch
	publication   model.SignedPublication
	requiredRoots []model.Digest
	status        model.InboxStatus
	attempts      uint32
	nextAttemptAt time.Time
	leaseOwner    string
	leaseUntil    time.Time
	hasLease      bool
	diagnostic    string
	receivedAt    time.Time
	updatedAt     time.Time
}

// ClaimPeerInboxArtifact claims at most one globally oldest due Artifact row.
// SQLite remains the sole queue; no publication or root list is retained in
// process unless this transaction has installed the corresponding fence.
func (s *Store) ClaimPeerInboxArtifact(ctx context.Context,
	spec ClaimPeerInboxArtifactSpec,
) (PeerInboxArtifactClaimResult, error) {
	at, err := validatePeerInboxArtifactCall(s, ctx, spec.LeaseOwner, spec.At)
	if err != nil {
		return PeerInboxArtifactClaimResult{}, err
	}
	leaseUntil, err := canonicalStoreTime(at.Add(peerInboxArtifactLease))
	if err != nil {
		return PeerInboxArtifactClaimResult{}, fmt.Errorf("%w: derived lease: %v",
			ErrPeerInboxArtifactInput, err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PeerInboxArtifactClaimResult{}, fmt.Errorf("claim Peer Inbox Artifact: begin: %w", err)
	}
	defer tx.Rollback()
	var inboxText string
	err = tx.QueryRowContext(ctx, `SELECT inbox.inbox_id FROM peer_inbox inbox
		JOIN channels channel ON channel.channel_id=inbox.channel_id
			AND channel.status='active' AND channel.topic_state='joined' AND channel.updated_at<=?
		JOIN peer_bindings origin ON origin.channel_id=inbox.channel_id
			AND origin.peer_id=inbox.origin_peer_id AND origin.origin_epoch=inbox.origin_epoch
			AND origin.state='active'
		WHERE inbox.is_audience=1 AND NOT EXISTS(
			SELECT 1 FROM origin_quarantines quarantine
			WHERE quarantine.channel_id=inbox.channel_id
				AND quarantine.origin_peer_id=inbox.origin_peer_id
				AND quarantine.origin_epoch=inbox.origin_epoch
		) AND (
			(inbox.status='stored' AND inbox.next_attempt_at<=?)
			OR (inbox.status='waiting_artifact' AND inbox.lease_until<=?)
			OR (inbox.status='retry' AND inbox.next_attempt_at<=? AND inbox.diagnostic IN (?,?,?,?,?))
		)
		ORDER BY inbox.next_attempt_at,inbox.received_at,inbox.inbox_id LIMIT 1`,
		storeTime(at), storeTime(at), storeTime(at),
		storeTime(at), string(PeerInboxArtifactRetryBusy),
		string(PeerInboxArtifactRetryTransportUnavailable), string(PeerInboxArtifactRetryTimeout),
		string(PeerInboxArtifactRetryNotAuthorized), string(PeerInboxArtifactRetryResourceExhausted)).
		Scan(&inboxText)
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.Commit(); err != nil {
			return PeerInboxArtifactClaimResult{}, fmt.Errorf("claim Peer Inbox Artifact: commit empty: %w", err)
		}
		return PeerInboxArtifactClaimResult{}, nil
	}
	if err != nil {
		return PeerInboxArtifactClaimResult{}, fmt.Errorf("claim Peer Inbox Artifact: select: %w", err)
	}
	inboxID, err := model.ParseInboxID(inboxText)
	if err != nil {
		return PeerInboxArtifactClaimResult{}, fmt.Errorf("%w: selected Inbox ID: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	row, err := readPeerInboxArtifactRow(ctx, tx, inboxID)
	if err != nil {
		return PeerInboxArtifactClaimResult{}, err
	}
	if row.attempts == math.MaxUint32 || at.Before(row.updatedAt) ||
		!peerInboxArtifactRowDue(row, at) {
		return PeerInboxArtifactClaimResult{}, ErrPeerInboxArtifactInvariant
	}
	if err := requirePeerInboxArtifactAuthority(ctx, tx, row, at); err != nil {
		return PeerInboxArtifactClaimResult{}, err
	}
	nextAttempt := row.attempts + 1
	result, err := tx.ExecContext(ctx, `UPDATE peer_inbox SET status='waiting_artifact',
		attempts=?,next_attempt_at=?,lease_owner=?,lease_until=?,diagnostic=NULL,updated_at=?
		WHERE inbox_id=? AND status=? AND attempts=? AND updated_at=?`, nextAttempt, storeTime(at),
		spec.LeaseOwner, storeTime(leaseUntil), storeTime(at), row.inboxID.String(), string(row.status),
		row.attempts, storeTime(row.updatedAt))
	if err != nil {
		return PeerInboxArtifactClaimResult{}, fmt.Errorf("%w: claim update: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	if err := requireExactlyOneRow(result, "claim Peer Inbox Artifact CAS"); err != nil {
		return PeerInboxArtifactClaimResult{}, fmt.Errorf("%w: %v", ErrPeerInboxArtifactStale, err)
	}
	if err := tx.Commit(); err != nil {
		return PeerInboxArtifactClaimResult{}, fmt.Errorf("claim Peer Inbox Artifact: commit: %w", err)
	}
	fence := PeerInboxArtifactFence{inboxID: row.inboxID, leaseOwner: spec.LeaseOwner,
		leaseUntil: leaseUntil, attempt: nextAttempt}
	claim := PeerInboxArtifactClaim{fence: fence, publication: row.publication,
		channelID: row.channelID, originPeerID: row.originPeerID, originEpoch: row.originEpoch,
		requiredRoots: append([]model.Digest(nil), row.requiredRoots...)}
	return PeerInboxArtifactClaimResult{claim: claim, found: true}, nil
}

// RenewPeerInboxArtifactLease extends one still-live fence to trusted At+120s.
// Repeating the same renewal after response loss returns the already installed
// fence without another write.
func (s *Store) RenewPeerInboxArtifactLease(ctx context.Context,
	spec RenewPeerInboxArtifactSpec,
) (PeerInboxArtifactRenewal, error) {
	at, err := validatePeerInboxArtifactSettlementCall(s, ctx, spec.Fence, spec.At)
	if err != nil {
		return PeerInboxArtifactRenewal{}, err
	}
	leaseUntil, err := canonicalStoreTime(at.Add(peerInboxArtifactLease))
	if err != nil {
		return PeerInboxArtifactRenewal{}, fmt.Errorf("%w: derived renewal: %v",
			ErrPeerInboxArtifactInput, err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PeerInboxArtifactRenewal{}, fmt.Errorf("renew Peer Inbox Artifact: begin: %w", err)
	}
	defer tx.Rollback()
	row, err := readPeerInboxArtifactRow(ctx, tx, spec.Fence.inboxID)
	if err != nil {
		return PeerInboxArtifactRenewal{}, err
	}
	if peerInboxArtifactRenewalReplay(row, spec.Fence, at, leaseUntil) {
		if err := tx.Commit(); err != nil {
			return PeerInboxArtifactRenewal{}, fmt.Errorf("renew Peer Inbox Artifact: replay commit: %w", err)
		}
		return PeerInboxArtifactRenewal{fence: PeerInboxArtifactFence{inboxID: row.inboxID,
			leaseOwner: row.leaseOwner, leaseUntil: row.leaseUntil, attempt: row.attempts}, replayed: true}, nil
	}
	if err := requireLivePeerInboxArtifactFence(row, spec.Fence, at); err != nil {
		return PeerInboxArtifactRenewal{}, err
	}
	if leaseUntil.Equal(row.leaseUntil) {
		if err := tx.Commit(); err != nil {
			return PeerInboxArtifactRenewal{}, fmt.Errorf("renew Peer Inbox Artifact: no-op commit: %w", err)
		}
		return PeerInboxArtifactRenewal{fence: spec.Fence, replayed: true}, nil
	}
	result, err := tx.ExecContext(ctx, `UPDATE peer_inbox SET lease_until=?,updated_at=?
		WHERE inbox_id=? AND status='waiting_artifact' AND attempts=? AND lease_owner=?
		AND lease_until=? AND updated_at=?`, storeTime(leaseUntil), storeTime(at), row.inboxID.String(),
		row.attempts, row.leaseOwner, storeTime(row.leaseUntil), storeTime(row.updatedAt))
	if err != nil {
		return PeerInboxArtifactRenewal{}, fmt.Errorf("%w: renew update: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	if err := requireExactlyOneRow(result, "renew Peer Inbox Artifact CAS"); err != nil {
		return PeerInboxArtifactRenewal{}, fmt.Errorf("%w: %v", ErrPeerInboxArtifactStale, err)
	}
	if err := tx.Commit(); err != nil {
		return PeerInboxArtifactRenewal{}, fmt.Errorf("renew Peer Inbox Artifact: commit: %w", err)
	}
	next := spec.Fence
	next.leaseUntil = leaseUntil
	return PeerInboxArtifactRenewal{fence: next, changed: true}, nil
}

// RetryPeerInboxArtifact releases one live lease into a bounded durable
// Artifact-only retry. Attempt already advanced at claim and is not changed.
func (s *Store) RetryPeerInboxArtifact(ctx context.Context,
	spec RetryPeerInboxArtifactSpec,
) (PeerInboxArtifactSettlement, error) {
	if !spec.Diagnostic.Valid() || spec.RetryAfter < minArtifactRetry ||
		spec.RetryAfter > maxArtifactRetry {
		return PeerInboxArtifactSettlement{}, ErrPeerInboxArtifactInput
	}
	at, err := validatePeerInboxArtifactSettlementCall(s, ctx, spec.Fence, spec.At)
	if err != nil {
		return PeerInboxArtifactSettlement{}, err
	}
	nextAttempt, err := canonicalStoreTime(at.Add(spec.RetryAfter))
	if err != nil {
		return PeerInboxArtifactSettlement{}, fmt.Errorf("%w: retry schedule: %v",
			ErrPeerInboxArtifactInput, err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PeerInboxArtifactSettlement{}, fmt.Errorf("retry Peer Inbox Artifact: begin: %w", err)
	}
	defer tx.Rollback()
	row, err := readPeerInboxArtifactRow(ctx, tx, spec.Fence.inboxID)
	if err != nil {
		return PeerInboxArtifactSettlement{}, err
	}
	if row.status == model.InboxRetry && row.attempts == spec.Fence.attempt &&
		row.diagnostic == string(spec.Diagnostic) && row.updatedAt.Equal(at) &&
		row.nextAttemptAt.Equal(nextAttempt) && !row.hasLease {
		if err := requireNoPeerInboxArtifactPins(ctx, tx, row.inboxID); err != nil {
			return PeerInboxArtifactSettlement{}, err
		}
		if err := tx.Commit(); err != nil {
			return PeerInboxArtifactSettlement{}, fmt.Errorf("retry Peer Inbox Artifact: replay commit: %w", err)
		}
		return PeerInboxArtifactSettlement{status: model.InboxRetry, nextAttemptAt: nextAttempt,
			replayed: true}, nil
	}
	if err := requireLivePeerInboxArtifactFence(row, spec.Fence, at); err != nil {
		return PeerInboxArtifactSettlement{}, err
	}
	if err := requireNoPeerInboxArtifactPins(ctx, tx, row.inboxID); err != nil {
		return PeerInboxArtifactSettlement{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE peer_inbox SET status='retry',next_attempt_at=?,
		lease_owner=NULL,lease_until=NULL,diagnostic=?,updated_at=?
		WHERE inbox_id=? AND status='waiting_artifact' AND attempts=? AND lease_owner=?
		AND lease_until=? AND updated_at=?`, storeTime(nextAttempt), string(spec.Diagnostic),
		storeTime(at), row.inboxID.String(), row.attempts, row.leaseOwner,
		storeTime(row.leaseUntil), storeTime(row.updatedAt))
	if err != nil {
		return PeerInboxArtifactSettlement{}, fmt.Errorf("%w: retry update: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	if err := requireExactlyOneRow(result, "retry Peer Inbox Artifact CAS"); err != nil {
		return PeerInboxArtifactSettlement{}, fmt.Errorf("%w: %v", ErrPeerInboxArtifactStale, err)
	}
	if err := tx.Commit(); err != nil {
		return PeerInboxArtifactSettlement{}, fmt.Errorf("retry Peer Inbox Artifact: commit: %w", err)
	}
	return PeerInboxArtifactSettlement{status: model.InboxRetry, nextAttemptAt: nextAttempt,
		changed: true}, nil
}

// QuarantinePeerInboxArtifact terminally records one closed remote protocol,
// manifest, digest, or limit failure. It never inserts domain state or pins.
func (s *Store) QuarantinePeerInboxArtifact(ctx context.Context,
	spec QuarantinePeerInboxArtifactSpec,
) (PeerInboxArtifactSettlement, error) {
	if !spec.Diagnostic.Valid() {
		return PeerInboxArtifactSettlement{}, ErrPeerInboxArtifactInput
	}
	at, err := validatePeerInboxArtifactSettlementCall(s, ctx, spec.Fence, spec.At)
	if err != nil {
		return PeerInboxArtifactSettlement{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PeerInboxArtifactSettlement{}, fmt.Errorf("quarantine Peer Inbox Artifact: begin: %w", err)
	}
	defer tx.Rollback()
	row, err := readPeerInboxArtifactRow(ctx, tx, spec.Fence.inboxID)
	if err != nil {
		return PeerInboxArtifactSettlement{}, err
	}
	if row.status == model.InboxQuarantined && row.attempts == spec.Fence.attempt &&
		row.diagnostic == string(spec.Diagnostic) && row.updatedAt.Equal(at) && !row.hasLease {
		if err := requireNoPeerInboxArtifactPins(ctx, tx, row.inboxID); err != nil {
			return PeerInboxArtifactSettlement{}, err
		}
		if err := tx.Commit(); err != nil {
			return PeerInboxArtifactSettlement{}, fmt.Errorf("quarantine Peer Inbox Artifact: replay commit: %w", err)
		}
		return PeerInboxArtifactSettlement{status: model.InboxQuarantined,
			nextAttemptAt: at, replayed: true}, nil
	}
	if err := requireLivePeerInboxArtifactFence(row, spec.Fence, at); err != nil {
		return PeerInboxArtifactSettlement{}, err
	}
	if err := requireNoPeerInboxArtifactPins(ctx, tx, row.inboxID); err != nil {
		return PeerInboxArtifactSettlement{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE peer_inbox SET status='quarantined',next_attempt_at=?,
		lease_owner=NULL,lease_until=NULL,diagnostic=?,updated_at=?
		WHERE inbox_id=? AND status='waiting_artifact' AND attempts=? AND lease_owner=?
		AND lease_until=? AND updated_at=?`, storeTime(at), string(spec.Diagnostic), storeTime(at),
		row.inboxID.String(), row.attempts, row.leaseOwner, storeTime(row.leaseUntil), storeTime(row.updatedAt))
	if err != nil {
		return PeerInboxArtifactSettlement{}, fmt.Errorf("%w: quarantine update: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	if err := requireExactlyOneRow(result, "quarantine Peer Inbox Artifact CAS"); err != nil {
		return PeerInboxArtifactSettlement{}, fmt.Errorf("%w: %v", ErrPeerInboxArtifactStale, err)
	}
	if err := requireNoPeerInboxArtifactPins(ctx, tx, row.inboxID); err != nil {
		return PeerInboxArtifactSettlement{}, err
	}
	if err := tx.Commit(); err != nil {
		return PeerInboxArtifactSettlement{}, fmt.Errorf("quarantine Peer Inbox Artifact: commit: %w", err)
	}
	return PeerInboxArtifactSettlement{status: model.InboxQuarantined,
		nextAttemptAt: at, changed: true}, nil
}

// MarkPeerInboxArtifactReady is the receiver's sole closure-to-Inbox boundary.
// Current signed authority, every exact required sealed closure, Inbox pins,
// and ready state are checked or installed in this one short transaction.
func (s *Store) MarkPeerInboxArtifactReady(ctx context.Context,
	spec MarkPeerInboxArtifactReadySpec,
) (PeerInboxArtifactSettlement, error) {
	at, err := validatePeerInboxArtifactSettlementCall(s, ctx, spec.Fence, spec.At)
	if err != nil {
		return PeerInboxArtifactSettlement{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PeerInboxArtifactSettlement{}, fmt.Errorf("mark Peer Inbox Artifact ready: begin: %w", err)
	}
	defer tx.Rollback()
	row, err := readPeerInboxArtifactRow(ctx, tx, spec.Fence.inboxID)
	if err != nil {
		return PeerInboxArtifactSettlement{}, err
	}
	replay := row.status == model.InboxReady && row.attempts == spec.Fence.attempt &&
		row.updatedAt.Equal(at) && !row.hasLease && row.diagnostic == ""
	if replay {
		if err := requirePeerInboxArtifactClosures(ctx, tx, row.requiredRoots, at); err != nil {
			return PeerInboxArtifactSettlement{}, err
		}
		if err := requireExactPeerInboxArtifactPins(ctx, tx, row.inboxID, row.requiredRoots, at); err != nil {
			return PeerInboxArtifactSettlement{}, err
		}
		if err := tx.Commit(); err != nil {
			return PeerInboxArtifactSettlement{}, fmt.Errorf("mark Peer Inbox Artifact ready: replay commit: %w", err)
		}
		return PeerInboxArtifactSettlement{status: model.InboxReady,
			nextAttemptAt: at, replayed: true}, nil
	}
	if err := requireLivePeerInboxArtifactFence(row, spec.Fence, at); err != nil {
		return PeerInboxArtifactSettlement{}, err
	}
	if err := requirePeerInboxArtifactAuthority(ctx, tx, row, at); err != nil {
		return PeerInboxArtifactSettlement{}, err
	}
	if err := requirePeerInboxArtifactClosures(ctx, tx, row.requiredRoots, at); err != nil {
		return PeerInboxArtifactSettlement{}, err
	}
	if err := requireNoPeerInboxArtifactPins(ctx, tx, row.inboxID); err != nil {
		return PeerInboxArtifactSettlement{}, err
	}
	for _, root := range row.requiredRoots {
		if err := insertPeerInboxArtifactPin(ctx, tx, row.inboxID, root, at); err != nil {
			return PeerInboxArtifactSettlement{}, err
		}
	}
	if err := requireExactPeerInboxArtifactPins(ctx, tx, row.inboxID, row.requiredRoots, at); err != nil {
		return PeerInboxArtifactSettlement{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE peer_inbox SET status='ready',next_attempt_at=?,
		lease_owner=NULL,lease_until=NULL,diagnostic=NULL,updated_at=?
		WHERE inbox_id=? AND status='waiting_artifact' AND attempts=? AND lease_owner=?
		AND lease_until=? AND updated_at=?`, storeTime(at), storeTime(at), row.inboxID.String(),
		row.attempts, row.leaseOwner, storeTime(row.leaseUntil), storeTime(row.updatedAt))
	if err != nil {
		return PeerInboxArtifactSettlement{}, fmt.Errorf("%w: ready update: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	if err := requireExactlyOneRow(result, "mark Peer Inbox Artifact ready CAS"); err != nil {
		return PeerInboxArtifactSettlement{}, fmt.Errorf("%w: %v", ErrPeerInboxArtifactStale, err)
	}
	if err := tx.Commit(); err != nil {
		return PeerInboxArtifactSettlement{}, fmt.Errorf("mark Peer Inbox Artifact ready: commit: %w", err)
	}
	return PeerInboxArtifactSettlement{status: model.InboxReady,
		nextAttemptAt: at, changed: true}, nil
}

// ReadPeerInboxArtifactRoot is the receiver's narrow cached-root probe. It
// exposes neither SQL absence nor unrelated verified roots: the requested
// digest must belong to this exact still-live claim, and only a fully sealed
// closure is returned as found.
func (s *Store) ReadPeerInboxArtifactRoot(ctx context.Context,
	spec ReadPeerInboxArtifactRootSpec,
) (VerifiedArtifactRoot, bool, error) {
	if spec.RootDigest.IsZero() {
		return VerifiedArtifactRoot{}, false, ErrPeerInboxArtifactInput
	}
	at, err := validatePeerInboxArtifactSettlementCall(s, ctx, spec.Fence, spec.At)
	if err != nil {
		return VerifiedArtifactRoot{}, false, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return VerifiedArtifactRoot{}, false, fmt.Errorf("read Peer Inbox Artifact root: begin: %w", err)
	}
	defer tx.Rollback()
	row, err := readPeerInboxArtifactRow(ctx, tx, spec.Fence.inboxID)
	if err != nil {
		return VerifiedArtifactRoot{}, false, err
	}
	if err := requireLivePeerInboxArtifactFence(row, spec.Fence, at); err != nil {
		return VerifiedArtifactRoot{}, false, err
	}
	if !containsPeerInboxArtifactRoot(row.requiredRoots, spec.RootDigest) {
		return VerifiedArtifactRoot{}, false, ErrPeerInboxArtifactInput
	}
	root, state, err := readArtifactRoot(ctx, tx, spec.RootDigest)
	if errors.Is(err, sql.ErrNoRows) || err == nil && state != "verified" {
		if err := tx.Commit(); err != nil {
			return VerifiedArtifactRoot{}, false, fmt.Errorf("read Peer Inbox Artifact root: commit miss: %w", err)
		}
		return VerifiedArtifactRoot{}, false, nil
	}
	if err != nil || root.VerifiedAt.IsZero() || root.VerifiedAt.After(at) {
		return VerifiedArtifactRoot{}, false, fmt.Errorf("%w: cached root metadata: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	if _, err := readSealedArtifactSourceClosure(ctx, tx, spec.RootDigest); err != nil {
		return VerifiedArtifactRoot{}, false, fmt.Errorf("%w: cached root closure: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	if err := tx.Commit(); err != nil {
		return VerifiedArtifactRoot{}, false, fmt.Errorf("read Peer Inbox Artifact root: commit: %w", err)
	}
	return root, true, nil
}

func validatePeerInboxArtifactCall(s *Store, ctx context.Context, owner string,
	atValue time.Time,
) (time.Time, error) {
	if s == nil || s.db == nil || ctx == nil || !validPublicationIdentifier(owner) {
		return time.Time{}, ErrPeerInboxArtifactInput
	}
	at, err := canonicalStoreTime(atValue)
	if err != nil || at.IsZero() {
		return time.Time{}, fmt.Errorf("%w: trusted time: %v", ErrPeerInboxArtifactInput, err)
	}
	return at, nil
}

func validatePeerInboxArtifactSettlementCall(s *Store, ctx context.Context,
	fence PeerInboxArtifactFence, atValue time.Time,
) (time.Time, error) {
	if s == nil || s.db == nil || ctx == nil || fence.inboxID.IsZero() ||
		fence.attempt == 0 || !validPublicationIdentifier(fence.leaseOwner) || fence.leaseUntil.IsZero() {
		return time.Time{}, ErrPeerInboxArtifactInput
	}
	leaseUntil, err := canonicalStoreTime(fence.leaseUntil)
	if err != nil || !leaseUntil.Equal(fence.leaseUntil) {
		return time.Time{}, ErrPeerInboxArtifactInput
	}
	at, err := canonicalStoreTime(atValue)
	if err != nil || at.IsZero() {
		return time.Time{}, fmt.Errorf("%w: trusted time: %v", ErrPeerInboxArtifactInput, err)
	}
	return at, nil
}

func peerInboxArtifactRowDue(row peerInboxArtifactRow, at time.Time) bool {
	switch row.status {
	case model.InboxStored:
		return !row.hasLease && !row.nextAttemptAt.After(at)
	case model.InboxWaitingArtifact:
		return row.hasLease && !row.leaseUntil.After(at)
	case model.InboxRetry:
		return !row.hasLease && !row.nextAttemptAt.After(at) &&
			PeerInboxArtifactRetryDiagnostic(row.diagnostic).Valid()
	default:
		return false
	}
}

func containsPeerInboxArtifactRoot(roots []model.Digest, root model.Digest) bool {
	for _, candidate := range roots {
		if candidate == root {
			return true
		}
	}
	return false
}

func requireLivePeerInboxArtifactFence(row peerInboxArtifactRow,
	fence PeerInboxArtifactFence, at time.Time,
) error {
	if row.inboxID != fence.inboxID || row.status != model.InboxWaitingArtifact ||
		!row.hasLease || row.attempts != fence.attempt || row.leaseOwner != fence.leaseOwner ||
		!row.leaseUntil.Equal(fence.leaseUntil) || at.Before(row.updatedAt) || !at.Before(row.leaseUntil) {
		return ErrPeerInboxArtifactStale
	}
	return nil
}

func peerInboxArtifactRenewalReplay(row peerInboxArtifactRow, fence PeerInboxArtifactFence,
	at, expectedLease time.Time,
) bool {
	return row.inboxID == fence.inboxID && row.status == model.InboxWaitingArtifact && row.hasLease &&
		row.attempts == fence.attempt && row.leaseOwner == fence.leaseOwner &&
		row.updatedAt.Equal(at) && row.leaseUntil.Equal(expectedLease) && at.Before(row.leaseUntil)
}

func requirePeerInboxArtifactAuthority(ctx context.Context, tx *sql.Tx,
	row peerInboxArtifactRow, at time.Time,
) error {
	node, err := readNode(ctx, tx)
	if err != nil {
		return fmt.Errorf("%w: Node: %v", ErrPeerInboxArtifactInvariant, err)
	}
	authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), row.channelID)
	if err != nil {
		return fmt.Errorf("%w: current Channel projection: %v", ErrPeerInboxArtifactInvariant, err)
	}
	if authority.channel.Status() != model.ChannelActive ||
		authority.channel.TopicState() != model.TopicJoined || authority.channel.UpdatedAt().After(at) {
		return ErrPeerInboxArtifactAuthority
	}
	binding, ok := activePeerInboxBinding(authority.bindings, row.originPeerID)
	if !ok || binding.OriginEpoch() != row.originEpoch {
		return ErrPeerInboxArtifactAuthority
	}
	var quarantined int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM origin_quarantines
		WHERE channel_id=? AND origin_peer_id=? AND origin_epoch=?)`, row.channelID.String(),
		row.originPeerID.String(), row.originEpoch.String()).Scan(&quarantined); err != nil {
		return fmt.Errorf("%w: origin quarantine: %v", ErrPeerInboxArtifactInvariant, err)
	}
	if quarantined != 0 {
		return ErrPeerInboxArtifactAuthority
	}
	if !row.publication.Event().Audience().Contains(node.PeerID()) {
		return ErrPeerInboxArtifactInvariant
	}
	auth := peerInboxArrivalAuthority{node: node, channel: authority, originKey: binding.PublicKey()}
	if err := authenticatePeerInboxPublication(auth, row.publication); err != nil {
		return fmt.Errorf("%w: signed publication no longer authenticates: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	return nil
}

func requirePeerInboxArtifactClosures(ctx context.Context, tx *sql.Tx,
	roots []model.Digest, at time.Time,
) error {
	if len(roots) > maxVerifiedClosureRoots {
		return fmt.Errorf("%w: required root count exceeds closure bound",
			ErrPeerInboxArtifactInvariant)
	}
	var aggregateBytes uint64
	aggregateEntries := 0
	for _, rootDigest := range roots {
		root, state, err := readArtifactRoot(ctx, tx, rootDigest)
		if errors.Is(err, sql.ErrNoRows) || err == nil && state != "verified" {
			return fmt.Errorf("%w: root %s", ErrPeerInboxArtifactNotReady, rootDigest.String())
		}
		if err != nil || root.VerifiedAt.IsZero() {
			return fmt.Errorf("%w: root %s metadata: %v",
				ErrPeerInboxArtifactInvariant, rootDigest.String(), err)
		}
		if root.VerifiedAt.After(at) {
			return fmt.Errorf("%w: root %s verified after trusted time",
				ErrPeerInboxArtifactInvariant, rootDigest.String())
		}
		closure, err := readSealedArtifactSourceClosure(ctx, tx, rootDigest)
		if err != nil {
			return fmt.Errorf("%w: root %s closure: %v",
				ErrPeerInboxArtifactInvariant, rootDigest.String(), err)
		}
		manifest, err := artifactdomain.ParseManifest(closure.root.Manifest.Bytes())
		if err != nil {
			return fmt.Errorf("%w: root %s sealed manifest: %v",
				ErrPeerInboxArtifactInvariant, rootDigest.String(), err)
		}
		if aggregateBytes > maxVerifiedClosureBytes-closure.root.TotalBytes {
			return fmt.Errorf("%w: required closure byte bound exceeded",
				ErrPeerInboxArtifactLimit)
		}
		aggregateBytes += closure.root.TotalBytes
		aggregateEntries += len(manifest.Entries())
		if aggregateEntries > maxVerifiedClosureEntries {
			return fmt.Errorf("%w: required closure entry bound exceeded",
				ErrPeerInboxArtifactLimit)
		}
	}
	return nil
}

func insertPeerInboxArtifactPin(ctx context.Context, tx *sql.Tx, inboxID model.InboxID,
	root model.Digest, at time.Time,
) error {
	_, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO artifact_pins(
		root_digest,owner_kind,owner_id,expires_at,created_at) VALUES(?,'inbox',?,NULL,?)`,
		root.String(), inboxID.String(), storeTime(at))
	if err != nil {
		return fmt.Errorf("%w: insert Inbox pin: %v", ErrPeerInboxArtifactInvariant, err)
	}
	return nil
}

func requireExactPeerInboxArtifactPins(ctx context.Context, tx *sql.Tx, inboxID model.InboxID,
	roots []model.Digest, at time.Time,
) error {
	rows, err := tx.QueryContext(ctx, `SELECT root_digest,expires_at,created_at FROM artifact_pins
		WHERE owner_kind='inbox' AND owner_id=? ORDER BY root_digest`, inboxID.String())
	if err != nil {
		return fmt.Errorf("%w: read Inbox pins: %v", ErrPeerInboxArtifactInvariant, err)
	}
	defer rows.Close()
	actual := make([]model.Digest, 0, len(roots))
	for rows.Next() {
		var rootText, createdText string
		var expires sql.NullString
		if err := rows.Scan(&rootText, &expires, &createdText); err != nil {
			return fmt.Errorf("%w: scan Inbox pin: %v", ErrPeerInboxArtifactInvariant, err)
		}
		root, parseErr := model.ParseDigest(rootText)
		createdAt, timeErr := parseCanonicalStoreTime(createdText)
		if parseErr != nil || timeErr != nil || expires.Valid || createdAt.After(at) {
			return fmt.Errorf("%w: invalid Inbox pin", ErrPeerInboxArtifactInvariant)
		}
		actual = append(actual, root)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%w: iterate Inbox pins: %v", ErrPeerInboxArtifactInvariant, err)
	}
	if len(actual) != len(roots) {
		return fmt.Errorf("%w: Inbox pin set differs from required roots", ErrPeerInboxArtifactInvariant)
	}
	for index := range roots {
		if actual[index] != roots[index] {
			return fmt.Errorf("%w: Inbox pin set differs from required roots", ErrPeerInboxArtifactInvariant)
		}
	}
	return nil
}

func requireNoPeerInboxArtifactPins(ctx context.Context, tx *sql.Tx, inboxID model.InboxID) error {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM artifact_pins
		WHERE owner_kind='inbox' AND owner_id=?`, inboxID.String()).Scan(&count); err != nil || count != 0 {
		return fmt.Errorf("%w: pre-ready Inbox pin count %d: %v",
			ErrPeerInboxArtifactInvariant, count, err)
	}
	return nil
}

func readPeerInboxArtifactRow(ctx context.Context, tx *sql.Tx,
	inboxID model.InboxID,
) (peerInboxArtifactRow, error) {
	var inboxText, channelText, transportText, originText, epochText, eventText string
	var originSequence, channelSequence uint64
	var eventDigestRaw, originMemberDigestRaw, rosterDigestRaw, publicationDigestRaw []byte
	var originMemberRevision, rosterRevision uint64
	var signature, publicationRaw, rootsRaw []byte
	var audience int
	var statusText, nextText, receivedText, updatedText string
	var attempts int64
	var leaseOwner, leaseUntil, localEvent, receipt, diagnostic sql.NullString
	var decision []byte
	err := tx.QueryRowContext(ctx, `SELECT inbox_id,channel_id,transport_peer_id,origin_peer_id,
		origin_epoch,origin_seq,channel_seq,event_id,event_digest,origin_member_revision,
		origin_member_record_hash,publication_roster_revision,publication_roster_hash,
		publication_digest,origin_signature,publication_json,is_audience,
		required_artifact_roots_json,status,attempts,next_attempt_at,lease_owner,lease_until,
		local_event_id,decision_json,receipt_event_id,diagnostic,received_at,updated_at
		FROM peer_inbox WHERE inbox_id=?`, inboxID.String()).Scan(&inboxText, &channelText,
		&transportText, &originText, &epochText, &originSequence, &channelSequence, &eventText,
		&eventDigestRaw, &originMemberRevision, &originMemberDigestRaw, &rosterRevision,
		&rosterDigestRaw, &publicationDigestRaw, &signature, &publicationRaw, &audience,
		&rootsRaw, &statusText, &attempts, &nextText, &leaseOwner, &leaseUntil, &localEvent,
		&decision, &receipt, &diagnostic, &receivedText, &updatedText)
	if errors.Is(err, sql.ErrNoRows) {
		return peerInboxArtifactRow{}, ErrPeerInboxArtifactStale
	}
	if err != nil {
		return peerInboxArtifactRow{}, fmt.Errorf("read Peer Inbox Artifact: %w", err)
	}
	parsedInbox, inboxErr := model.ParseInboxID(inboxText)
	channelID, channelErr := model.ParseChannelID(channelText)
	transport, transportErr := model.ParsePeerID(transportText)
	originPeer, originErr := model.ParsePeerID(originText)
	originEpoch, epochErr := model.ParseOriginEpoch(epochText)
	status := model.InboxStatus(statusText)
	nextAttempt, nextErr := parseCanonicalStoreTime(nextText)
	receivedAt, receivedErr := parseCanonicalStoreTime(receivedText)
	updatedAt, updatedErr := parseCanonicalStoreTime(updatedText)
	if inboxErr != nil || parsedInbox != inboxID || channelErr != nil || transportErr != nil ||
		transport.IsZero() || originErr != nil || epochErr != nil || !status.Valid() || attempts < 0 ||
		uint64(attempts) > math.MaxUint32 || nextErr != nil || receivedErr != nil || updatedErr != nil ||
		updatedAt.Before(receivedAt) || audience != 1 || localEvent.Valid || receipt.Valid || len(decision) != 0 {
		return peerInboxArtifactRow{}, fmt.Errorf("%w: malformed Inbox projection",
			ErrPeerInboxArtifactInvariant)
	}
	incumbent := peerInboxIncumbent{inboxID: inboxText, channelID: channelText,
		originPeerID: originText, originEpoch: epochText, originSequence: originSequence,
		channelSequence: channelSequence, eventID: eventText, eventDigest: eventDigestRaw,
		publicationDigest: publicationDigestRaw, signature: signature, wire: publicationRaw}
	if err := validatePeerInboxIncumbent(incumbent); err != nil {
		return peerInboxArtifactRow{}, fmt.Errorf("%w: signed publication tuple: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	parsedPublication, err := model.ParseSignedPublication(publicationRaw)
	if err != nil {
		return peerInboxArtifactRow{}, fmt.Errorf("%w: signed publication: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	publication, err := model.ProjectImportedPublication(&parsedPublication)
	if err != nil {
		return peerInboxArtifactRow{}, fmt.Errorf("%w: imported publication: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	originMemberDigest, originHeadErr := model.DigestFromBytes(originMemberDigestRaw)
	rosterDigest, rosterHeadErr := model.DigestFromBytes(rosterDigestRaw)
	originHead, originRecordErr := model.NewRecordHead(originMemberRevision, originMemberDigest)
	rosterHead, rosterRecordErr := model.NewRecordHead(rosterRevision, rosterDigest)
	scope := publication.Event().Scope()
	if originHeadErr != nil || rosterHeadErr != nil || originRecordErr != nil || rosterRecordErr != nil ||
		scope.ChannelID() != channelID || scope.OriginPeerID() != originPeer ||
		scope.OriginEpoch() != originEpoch || scope.OriginMember() != originHead ||
		scope.PublicationRoster() != rosterHead {
		return peerInboxArtifactRow{}, fmt.Errorf("%w: publication authority tuple",
			ErrPeerInboxArtifactInvariant)
	}
	expectedRoots := peerInboxArtifactRoots(publication.Event())
	expectedRootsJSON, err := model.JSONFrom(expectedRoots)
	if err != nil {
		return peerInboxArtifactRow{}, fmt.Errorf("%w: canonical roots: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	canonicalRoots, err := model.NewJSON(rootsRaw)
	if err != nil || !bytes.Equal(canonicalRoots.Bytes(), rootsRaw) ||
		!bytes.Equal(expectedRootsJSON.Bytes(), rootsRaw) {
		return peerInboxArtifactRow{}, fmt.Errorf("%w: required roots differ from immutable Event",
			ErrPeerInboxArtifactInvariant)
	}
	hasLease := leaseOwner.Valid || leaseUntil.Valid
	var parsedLease time.Time
	if leaseOwner.Valid != leaseUntil.Valid {
		return peerInboxArtifactRow{}, fmt.Errorf("%w: partial Inbox lease", ErrPeerInboxArtifactInvariant)
	}
	if hasLease {
		parsedLease, err = parseCanonicalStoreTime(leaseUntil.String)
		if err != nil || !validPublicationIdentifier(leaseOwner.String) || !parsedLease.After(updatedAt) ||
			(status != model.InboxWaitingArtifact && status != model.InboxProcessing) {
			return peerInboxArtifactRow{}, fmt.Errorf("%w: malformed Inbox lease",
				ErrPeerInboxArtifactInvariant)
		}
	} else if status == model.InboxWaitingArtifact || status == model.InboxProcessing {
		return peerInboxArtifactRow{}, fmt.Errorf("%w: missing Inbox lease", ErrPeerInboxArtifactInvariant)
	}
	if diagnostic.Valid && (diagnostic.String == "" || !validPublicationDiagnostic(diagnostic.String)) {
		return peerInboxArtifactRow{}, fmt.Errorf("%w: invalid Inbox diagnostic",
			ErrPeerInboxArtifactInvariant)
	}
	return peerInboxArtifactRow{inboxID: parsedInbox, channelID: channelID,
		originPeerID: originPeer, originEpoch: originEpoch, publication: publication,
		requiredRoots: expectedRoots, status: status, attempts: uint32(attempts),
		nextAttemptAt: nextAttempt, leaseOwner: leaseOwner.String, leaseUntil: parsedLease,
		hasLease: hasLease, diagnostic: diagnostic.String, receivedAt: receivedAt,
		updatedAt: updatedAt}, nil
}
