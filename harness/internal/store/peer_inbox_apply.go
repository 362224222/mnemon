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

const (
	minPeerInboxSemanticRetry = time.Second
	maxPeerInboxSemanticRetry = 300 * time.Second

	peerInboxSemanticSeedDomain              = "mnemon/r5/peer-inbox-semantic-decision-seed/1"
	peerInboxSemanticSnapshotDomain          = "mnemon/r5/peer-inbox-semantic-snapshot/1"
	peerInboxSemanticTransitionRequestDomain = "mnemon/r5/peer-inbox-semantic-transition-request/1"
)

var (
	ErrPeerInboxSemanticInput     = errors.New("invalid Peer Inbox semantic worker input")
	ErrPeerInboxSemanticAuthority = errors.New("Peer Inbox semantic authority is unavailable")
	ErrPeerInboxSemanticStale     = errors.New("Peer Inbox semantic lease is stale")
	ErrPeerInboxSemanticInvariant = errors.New("Peer Inbox semantic durable invariant violated")
)

// PeerInboxSemanticRetryDiagnostic is the closed set of transient failures
// owned by semantic application. Artifact receiver failures deliberately use
// a disjoint diagnostic set, so the two workers can share peer_inbox.retry
// without racing to claim each other's work.
type PeerInboxSemanticRetryDiagnostic string

const (
	PeerInboxSemanticRetryBusy                  PeerInboxSemanticRetryDiagnostic = "semantic_busy"
	PeerInboxSemanticRetryDependencyUnavailable PeerInboxSemanticRetryDiagnostic = "semantic_dependency_unavailable"
	PeerInboxSemanticRetryTimeout               PeerInboxSemanticRetryDiagnostic = "semantic_timeout"
	PeerInboxSemanticRetryResourceExhausted     PeerInboxSemanticRetryDiagnostic = "semantic_resource_exhausted"
)

func (diagnostic PeerInboxSemanticRetryDiagnostic) Valid() bool {
	switch diagnostic {
	case PeerInboxSemanticRetryBusy, PeerInboxSemanticRetryDependencyUnavailable,
		PeerInboxSemanticRetryTimeout, PeerInboxSemanticRetryResourceExhausted:
		return true
	default:
		return false
	}
}

// PeerInboxSemanticFence is an opaque capability for one exact semantic
// attempt and one exact authority snapshot. semanticNonce is intentionally not
// exposed: it binds the fence and derives the stable decision seed without
// becoming caller-selected authority.
type PeerInboxSemanticFence struct {
	inboxID        model.InboxID
	leaseOwner     string
	leaseUntil     time.Time
	attempt        uint32
	semanticNonce  [32]byte
	snapshotDigest model.Digest
}

func (fence PeerInboxSemanticFence) InboxID() model.InboxID       { return fence.inboxID }
func (fence PeerInboxSemanticFence) LeaseOwner() string           { return fence.leaseOwner }
func (fence PeerInboxSemanticFence) LeaseUntil() time.Time        { return fence.leaseUntil }
func (fence PeerInboxSemanticFence) Attempt() uint32              { return fence.attempt }
func (fence PeerInboxSemanticFence) SnapshotDigest() model.Digest { return fence.snapshotDigest }

// PeerInboxSemanticClaim is the immutable Store projection against which a
// later semantic decision must be constructed. It does not grant permission to
// mutate a domain Event; that boundary belongs to the subsequent atomic apply
// transaction.
type PeerInboxSemanticClaim struct {
	fence          PeerInboxSemanticFence
	publication    model.SignedPublication
	importedEvent  model.Event
	currentWork    model.ReviewWork
	hasCurrentWork bool
	causalEvents   []model.Event
	requiredRoots  []model.Digest
	decisionSeed   model.Digest
}

func (claim PeerInboxSemanticClaim) Fence() PeerInboxSemanticFence { return claim.fence }
func (claim PeerInboxSemanticClaim) InboxID() model.InboxID        { return claim.fence.inboxID }
func (claim PeerInboxSemanticClaim) Publication() model.SignedPublication {
	return claim.publication
}
func (claim PeerInboxSemanticClaim) ImportedEvent() model.Event { return claim.importedEvent }
func (claim PeerInboxSemanticClaim) CurrentWork() (model.ReviewWork, bool) {
	return claim.currentWork, claim.hasCurrentWork
}
func (claim PeerInboxSemanticClaim) CausalEvents() []model.Event {
	return append([]model.Event(nil), claim.causalEvents...)
}
func (claim PeerInboxSemanticClaim) RequiredArtifactRoots() []model.Digest {
	return append([]model.Digest(nil), claim.requiredRoots...)
}
func (claim PeerInboxSemanticClaim) DecisionSeed() model.Digest   { return claim.decisionSeed }
func (claim PeerInboxSemanticClaim) SnapshotDigest() model.Digest { return claim.fence.snapshotDigest }

type ClaimPeerInboxSemanticSpec struct {
	LeaseOwner string
	At         time.Time
}

type PeerInboxSemanticClaimResult struct {
	claim PeerInboxSemanticClaim
	found bool
}

func (result PeerInboxSemanticClaimResult) Found() bool { return result.found }
func (result PeerInboxSemanticClaimResult) Claim() PeerInboxSemanticClaim {
	return result.claim
}

type RenewPeerInboxSemanticSpec struct {
	Fence PeerInboxSemanticFence
	At    time.Time
}

type ProbePeerInboxSemanticAuthoritySpec struct {
	Fence PeerInboxSemanticFence
	At    time.Time
}

type RetryPeerInboxSemanticSpec struct {
	Fence      PeerInboxSemanticFence
	Diagnostic PeerInboxSemanticRetryDiagnostic
	RetryAfter time.Duration
	At         time.Time
}

type PeerInboxSemanticRenewal struct {
	fence    PeerInboxSemanticFence
	changed  bool
	replayed bool
}

func (result PeerInboxSemanticRenewal) Fence() PeerInboxSemanticFence { return result.fence }
func (result PeerInboxSemanticRenewal) Changed() bool                 { return result.changed }
func (result PeerInboxSemanticRenewal) Replayed() bool                { return result.replayed }

type PeerInboxSemanticRetry struct {
	nextAttemptAt time.Time
	changed       bool
	replayed      bool
}

func (result PeerInboxSemanticRetry) NextAttemptAt() time.Time { return result.nextAttemptAt }
func (result PeerInboxSemanticRetry) Changed() bool            { return result.changed }
func (result PeerInboxSemanticRetry) Replayed() bool           { return result.replayed }

type peerInboxSemanticRow struct {
	peerInboxArtifactRow
	semanticNonce [32]byte
}

type peerInboxSemanticSnapshot struct {
	publication    model.SignedPublication
	importedEvent  model.Event
	currentWork    model.ReviewWork
	hasCurrentWork bool
	causalEvents   []model.Event
	decisionSeed   model.Digest
	digest         model.Digest
}

type peerInboxSemanticTransitionKind string

const (
	peerInboxSemanticTransitionRenew peerInboxSemanticTransitionKind = "renew"
	peerInboxSemanticTransitionRetry peerInboxSemanticTransitionKind = "retry"
)

func (kind peerInboxSemanticTransitionKind) valid() bool {
	return kind == peerInboxSemanticTransitionRenew || kind == peerInboxSemanticTransitionRetry
}

type peerInboxSemanticResultProjection struct {
	inboxID       model.InboxID
	semanticNonce [32]byte
	status        model.InboxStatus
	attempt       uint32
	nextAttemptAt time.Time
	leaseOwner    string
	leaseUntil    time.Time
	hasLease      bool
	diagnostic    string
	updatedAt     time.Time
	hasLocalEvent bool
	hasDecision   bool
	hasReceipt    bool
}

type peerInboxSemanticTransitionReceipt struct {
	inboxID         model.InboxID
	kind            peerInboxSemanticTransitionKind
	oldLeaseOwner   string
	oldLeaseUntil   time.Time
	oldAttempt      uint32
	semanticNonce   [32]byte
	snapshotDigest  model.Digest
	requestedAt     time.Time
	requestDigest   model.Digest
	retryDiagnostic PeerInboxSemanticRetryDiagnostic
	retryAfter      time.Duration
	output          peerInboxSemanticResultProjection
}

// ClaimPeerInboxSemantic claims at most one globally oldest semantic row.
// ready rows, due semantic retries and expired processing leases share one
// total ordering; Artifact retries and non-audience evidence are never
// candidates. Every fresh claim or reclaim advances the durable attempt.
func (s *Store) ClaimPeerInboxSemantic(ctx context.Context,
	spec ClaimPeerInboxSemanticSpec,
) (PeerInboxSemanticClaimResult, error) {
	at, err := validatePeerInboxSemanticCall(s, ctx, spec.LeaseOwner, spec.At)
	if err != nil {
		return PeerInboxSemanticClaimResult{}, err
	}
	leaseUntil, err := canonicalStoreTime(at.Add(peerInboxSemanticLease))
	if err != nil {
		return PeerInboxSemanticClaimResult{}, fmt.Errorf("%w: derived lease: %v",
			ErrPeerInboxSemanticInput, err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PeerInboxSemanticClaimResult{}, fmt.Errorf("claim Peer Inbox semantic: begin: %w", err)
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
			inbox.status='ready'
			OR (inbox.status='processing' AND inbox.lease_until<=?)
			OR (inbox.status='retry' AND inbox.next_attempt_at<=? AND inbox.diagnostic IN (?,?,?,?))
		)
		ORDER BY CASE WHEN inbox.status='processing' THEN inbox.lease_until ELSE inbox.next_attempt_at END,
			inbox.received_at,inbox.inbox_id LIMIT 1`, storeTime(at), storeTime(at), storeTime(at),
		string(PeerInboxSemanticRetryBusy),
		string(PeerInboxSemanticRetryDependencyUnavailable),
		string(PeerInboxSemanticRetryTimeout),
		string(PeerInboxSemanticRetryResourceExhausted)).Scan(&inboxText)
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.Commit(); err != nil {
			return PeerInboxSemanticClaimResult{}, fmt.Errorf("claim Peer Inbox semantic: commit empty: %w", err)
		}
		return PeerInboxSemanticClaimResult{}, nil
	}
	if err != nil {
		return PeerInboxSemanticClaimResult{}, fmt.Errorf("claim Peer Inbox semantic: select: %w", err)
	}
	inboxID, err := model.ParseInboxID(inboxText)
	if err != nil {
		return PeerInboxSemanticClaimResult{}, fmt.Errorf("%w: selected Inbox ID: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	row, err := readPeerInboxSemanticRow(ctx, tx, inboxID)
	if err != nil {
		return PeerInboxSemanticClaimResult{}, err
	}
	if _, _, err := readValidatedPeerInboxSemanticTransitionReceipt(ctx, tx, inboxID); err != nil {
		return PeerInboxSemanticClaimResult{}, err
	}
	if row.attempts == math.MaxUint32 || at.Before(row.updatedAt) ||
		!peerInboxSemanticRowDue(row, at) {
		return PeerInboxSemanticClaimResult{}, ErrPeerInboxSemanticInvariant
	}
	snapshot, err := derivePeerInboxSemanticSnapshot(ctx, tx, row, at)
	if err != nil {
		return PeerInboxSemanticClaimResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM peer_inbox_semantic_transition_receipts
		WHERE inbox_id=?`, row.inboxID.String()); err != nil {
		return PeerInboxSemanticClaimResult{}, fmt.Errorf("%w: clear superseded transition receipt: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	nextAttempt := row.attempts + 1
	result, err := tx.ExecContext(ctx, `UPDATE peer_inbox SET status='processing',attempts=?,
		next_attempt_at=?,lease_owner=?,lease_until=?,diagnostic=NULL,updated_at=?
		WHERE inbox_id=? AND status=? AND attempts=? AND next_attempt_at=?
		AND lease_owner IS ? AND lease_until IS ? AND diagnostic IS ? AND updated_at=?`,
		nextAttempt, storeTime(at), spec.LeaseOwner, storeTime(leaseUntil), storeTime(at),
		row.inboxID.String(), string(row.status), row.attempts, storeTime(row.nextAttemptAt),
		nullablePeerInboxSemanticText(row.leaseOwner, row.hasLease),
		nullablePeerInboxSemanticTime(row.leaseUntil, row.hasLease),
		nullablePeerInboxSemanticText(row.diagnostic, row.diagnostic != ""), storeTime(row.updatedAt))
	if err != nil {
		return PeerInboxSemanticClaimResult{}, fmt.Errorf("%w: claim update: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	if err := requireExactlyOneRow(result, "claim Peer Inbox semantic CAS"); err != nil {
		return PeerInboxSemanticClaimResult{}, fmt.Errorf("%w: %v", ErrPeerInboxSemanticStale, err)
	}
	if err := tx.Commit(); err != nil {
		return PeerInboxSemanticClaimResult{}, fmt.Errorf("claim Peer Inbox semantic: commit: %w", err)
	}

	fence := PeerInboxSemanticFence{inboxID: row.inboxID, leaseOwner: spec.LeaseOwner,
		leaseUntil: leaseUntil, attempt: nextAttempt, semanticNonce: row.semanticNonce,
		snapshotDigest: snapshot.digest}
	return PeerInboxSemanticClaimResult{found: true, claim: peerInboxSemanticClaim(fence,
		row, snapshot)}, nil
}

// ProbePeerInboxSemanticAuthority revalidates the complete live fence and its
// exact semantic snapshot without extending the lease or changing domain
// state. Workers call it after any work performed outside SQLite and before
// entering a later atomic apply boundary.
func (s *Store) ProbePeerInboxSemanticAuthority(ctx context.Context,
	spec ProbePeerInboxSemanticAuthoritySpec,
) error {
	at, err := validatePeerInboxSemanticFenceCall(s, ctx, spec.Fence, spec.At)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("probe Peer Inbox semantic authority: begin: %w", err)
	}
	defer tx.Rollback()
	row, err := readPeerInboxSemanticRow(ctx, tx, spec.Fence.inboxID)
	if err != nil {
		return err
	}
	if err := requireLivePeerInboxSemanticFence(row, spec.Fence, at); err != nil {
		return err
	}
	snapshot, err := derivePeerInboxSemanticSnapshot(ctx, tx, row, at)
	if err != nil {
		return err
	}
	if snapshot.digest != spec.Fence.snapshotDigest {
		return ErrPeerInboxSemanticStale
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("probe Peer Inbox semantic authority: commit: %w", err)
	}
	return nil
}

// RenewPeerInboxSemantic extends one exact processing generation. A replay of
// the same old fence and trusted At returns the same new fence, including
// after process restart; it never increments the attempt.
func (s *Store) RenewPeerInboxSemantic(ctx context.Context,
	spec RenewPeerInboxSemanticSpec,
) (PeerInboxSemanticRenewal, error) {
	at, err := validatePeerInboxSemanticFenceCall(s, ctx, spec.Fence, spec.At)
	if err != nil {
		return PeerInboxSemanticRenewal{}, err
	}
	leaseUntil, err := canonicalStoreTime(at.Add(peerInboxSemanticLease))
	if err != nil {
		return PeerInboxSemanticRenewal{}, fmt.Errorf("%w: derived renewal lease: %v",
			ErrPeerInboxSemanticInput, err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PeerInboxSemanticRenewal{}, fmt.Errorf("renew Peer Inbox semantic: begin: %w", err)
	}
	defer tx.Rollback()
	receipt, hasReceipt, err := readValidatedPeerInboxSemanticTransitionReceipt(ctx, tx,
		spec.Fence.inboxID)
	if err != nil {
		return PeerInboxSemanticRenewal{}, err
	}
	if hasReceipt && receipt.matchesRequest(peerInboxSemanticTransitionRenew,
		spec.Fence, at, "", 0) {
		if err := tx.Commit(); err != nil {
			return PeerInboxSemanticRenewal{}, fmt.Errorf("renew Peer Inbox semantic: commit replay: %w", err)
		}
		newFence := spec.Fence
		newFence.leaseUntil = receipt.output.leaseUntil
		return PeerInboxSemanticRenewal{fence: newFence, replayed: true}, nil
	}
	row, err := readPeerInboxSemanticRow(ctx, tx, spec.Fence.inboxID)
	if err != nil {
		return PeerInboxSemanticRenewal{}, err
	}
	snapshot, err := derivePeerInboxSemanticSnapshot(ctx, tx, row, at)
	if err != nil {
		return PeerInboxSemanticRenewal{}, err
	}
	if snapshot.digest != spec.Fence.snapshotDigest || row.semanticNonce != spec.Fence.semanticNonce {
		return PeerInboxSemanticRenewal{}, ErrPeerInboxSemanticStale
	}
	newFence := spec.Fence
	newFence.leaseUntil = leaseUntil
	if err := requireLivePeerInboxSemanticFence(row, spec.Fence, at); err != nil {
		return PeerInboxSemanticRenewal{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE peer_inbox SET lease_until=?,updated_at=?
		WHERE inbox_id=? AND status='processing' AND attempts=? AND lease_owner=?
		AND lease_until=? AND updated_at=?`, storeTime(leaseUntil), storeTime(at),
		row.inboxID.String(), row.attempts, row.leaseOwner, storeTime(row.leaseUntil),
		storeTime(row.updatedAt))
	if err != nil {
		return PeerInboxSemanticRenewal{}, fmt.Errorf("%w: renew update: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	if err := requireExactlyOneRow(result, "renew Peer Inbox semantic CAS"); err != nil {
		return PeerInboxSemanticRenewal{}, fmt.Errorf("%w: %v", ErrPeerInboxSemanticStale, err)
	}
	output := peerInboxSemanticResultProjection{inboxID: row.inboxID,
		semanticNonce: row.semanticNonce, status: model.InboxProcessing,
		attempt: row.attempts, nextAttemptAt: row.nextAttemptAt,
		leaseOwner: row.leaseOwner, leaseUntil: leaseUntil, hasLease: true,
		updatedAt: at}
	transition, err := newPeerInboxSemanticTransitionReceipt(
		peerInboxSemanticTransitionRenew, spec.Fence, at, "", 0, output)
	if err != nil {
		return PeerInboxSemanticRenewal{}, err
	}
	if err := upsertPeerInboxSemanticTransitionReceipt(ctx, tx, transition); err != nil {
		return PeerInboxSemanticRenewal{}, err
	}
	if err := tx.Commit(); err != nil {
		return PeerInboxSemanticRenewal{}, fmt.Errorf("renew Peer Inbox semantic: commit: %w", err)
	}
	return PeerInboxSemanticRenewal{fence: newFence, changed: true}, nil
}

// RetryPeerInboxSemantic releases a live semantic fence into one bounded,
// closed diagnostic retry schedule. Attempt already advanced at claim time;
// the next claim/reclaim advances it again.
func (s *Store) RetryPeerInboxSemantic(ctx context.Context,
	spec RetryPeerInboxSemanticSpec,
) (PeerInboxSemanticRetry, error) {
	if !spec.Diagnostic.Valid() || spec.RetryAfter < minPeerInboxSemanticRetry ||
		spec.RetryAfter > maxPeerInboxSemanticRetry {
		return PeerInboxSemanticRetry{}, ErrPeerInboxSemanticInput
	}
	at, err := validatePeerInboxSemanticFenceCall(s, ctx, spec.Fence, spec.At)
	if err != nil {
		return PeerInboxSemanticRetry{}, err
	}
	nextAttempt, err := canonicalStoreTime(at.Add(spec.RetryAfter))
	if err != nil {
		return PeerInboxSemanticRetry{}, fmt.Errorf("%w: retry schedule: %v",
			ErrPeerInboxSemanticInput, err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PeerInboxSemanticRetry{}, fmt.Errorf("retry Peer Inbox semantic: begin: %w", err)
	}
	defer tx.Rollback()
	receipt, hasReceipt, err := readValidatedPeerInboxSemanticTransitionReceipt(ctx, tx,
		spec.Fence.inboxID)
	if err != nil {
		return PeerInboxSemanticRetry{}, err
	}
	if hasReceipt && receipt.matchesRequest(peerInboxSemanticTransitionRetry,
		spec.Fence, at, spec.Diagnostic, spec.RetryAfter) {
		if err := tx.Commit(); err != nil {
			return PeerInboxSemanticRetry{}, fmt.Errorf("retry Peer Inbox semantic: commit replay: %w", err)
		}
		return PeerInboxSemanticRetry{nextAttemptAt: receipt.output.nextAttemptAt,
			replayed: true}, nil
	}
	row, err := readPeerInboxSemanticRow(ctx, tx, spec.Fence.inboxID)
	if err != nil {
		return PeerInboxSemanticRetry{}, err
	}
	snapshot, err := derivePeerInboxSemanticSnapshot(ctx, tx, row, at)
	if err != nil {
		return PeerInboxSemanticRetry{}, err
	}
	if snapshot.digest != spec.Fence.snapshotDigest || row.semanticNonce != spec.Fence.semanticNonce {
		return PeerInboxSemanticRetry{}, ErrPeerInboxSemanticStale
	}
	if err := requireLivePeerInboxSemanticFence(row, spec.Fence, at); err != nil {
		return PeerInboxSemanticRetry{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE peer_inbox SET status='retry',next_attempt_at=?,
		lease_owner=NULL,lease_until=NULL,diagnostic=?,updated_at=?
		WHERE inbox_id=? AND status='processing' AND attempts=? AND lease_owner=?
		AND lease_until=? AND updated_at=?`, storeTime(nextAttempt), string(spec.Diagnostic),
		storeTime(at), row.inboxID.String(), row.attempts, row.leaseOwner,
		storeTime(row.leaseUntil), storeTime(row.updatedAt))
	if err != nil {
		return PeerInboxSemanticRetry{}, fmt.Errorf("%w: retry update: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	if err := requireExactlyOneRow(result, "retry Peer Inbox semantic CAS"); err != nil {
		return PeerInboxSemanticRetry{}, fmt.Errorf("%w: %v", ErrPeerInboxSemanticStale, err)
	}
	output := peerInboxSemanticResultProjection{inboxID: row.inboxID,
		semanticNonce: row.semanticNonce, status: model.InboxRetry,
		attempt: row.attempts, nextAttemptAt: nextAttempt,
		diagnostic: string(spec.Diagnostic), updatedAt: at}
	transition, err := newPeerInboxSemanticTransitionReceipt(
		peerInboxSemanticTransitionRetry, spec.Fence, at, spec.Diagnostic,
		spec.RetryAfter, output)
	if err != nil {
		return PeerInboxSemanticRetry{}, err
	}
	if err := upsertPeerInboxSemanticTransitionReceipt(ctx, tx, transition); err != nil {
		return PeerInboxSemanticRetry{}, err
	}
	if err := tx.Commit(); err != nil {
		return PeerInboxSemanticRetry{}, fmt.Errorf("retry Peer Inbox semantic: commit: %w", err)
	}
	return PeerInboxSemanticRetry{nextAttemptAt: nextAttempt, changed: true}, nil
}

func validatePeerInboxSemanticCall(s *Store, ctx context.Context, owner string,
	atValue time.Time,
) (time.Time, error) {
	if s == nil || s.db == nil || ctx == nil || !validPublicationIdentifier(owner) {
		return time.Time{}, ErrPeerInboxSemanticInput
	}
	at, err := canonicalStoreTime(atValue)
	if err != nil || at.IsZero() {
		return time.Time{}, fmt.Errorf("%w: trusted time: %v", ErrPeerInboxSemanticInput, err)
	}
	return at, nil
}

func validatePeerInboxSemanticFenceCall(s *Store, ctx context.Context,
	fence PeerInboxSemanticFence, atValue time.Time,
) (time.Time, error) {
	if s == nil || s.db == nil || ctx == nil || fence.inboxID.IsZero() || fence.attempt == 0 ||
		!validPublicationIdentifier(fence.leaseOwner) || fence.leaseUntil.IsZero() ||
		fence.snapshotDigest.IsZero() {
		return time.Time{}, ErrPeerInboxSemanticInput
	}
	leaseUntil, err := canonicalStoreTime(fence.leaseUntil)
	if err != nil || !leaseUntil.Equal(fence.leaseUntil) {
		return time.Time{}, ErrPeerInboxSemanticInput
	}
	at, err := canonicalStoreTime(atValue)
	if err != nil || at.IsZero() {
		return time.Time{}, fmt.Errorf("%w: trusted time: %v", ErrPeerInboxSemanticInput, err)
	}
	return at, nil
}

func readPeerInboxSemanticRow(ctx context.Context, tx *sql.Tx,
	inboxID model.InboxID,
) (peerInboxSemanticRow, error) {
	base, err := readPeerInboxArtifactRow(ctx, tx, inboxID)
	if errors.Is(err, ErrPeerInboxArtifactStale) {
		return peerInboxSemanticRow{}, ErrPeerInboxSemanticStale
	}
	if err != nil {
		return peerInboxSemanticRow{}, fmt.Errorf("%w: Inbox projection: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	var nonceRaw []byte
	if err := tx.QueryRowContext(ctx, `SELECT semantic_nonce FROM peer_inbox WHERE inbox_id=?`,
		inboxID.String()).Scan(&nonceRaw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return peerInboxSemanticRow{}, ErrPeerInboxSemanticStale
		}
		return peerInboxSemanticRow{}, fmt.Errorf("%w: semantic nonce: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	if len(nonceRaw) != 32 {
		return peerInboxSemanticRow{}, fmt.Errorf("%w: semantic nonce length",
			ErrPeerInboxSemanticInvariant)
	}
	var nonce [32]byte
	copy(nonce[:], nonceRaw)
	return peerInboxSemanticRow{peerInboxArtifactRow: base, semanticNonce: nonce}, nil
}

func peerInboxSemanticRowDue(row peerInboxSemanticRow, at time.Time) bool {
	switch row.status {
	case model.InboxReady:
		return !row.hasLease && row.diagnostic == ""
	case model.InboxProcessing:
		return row.hasLease && !row.leaseUntil.After(at) && row.diagnostic == ""
	case model.InboxRetry:
		return !row.hasLease && !row.nextAttemptAt.After(at) &&
			PeerInboxSemanticRetryDiagnostic(row.diagnostic).Valid()
	default:
		return false
	}
}

func requireLivePeerInboxSemanticFence(row peerInboxSemanticRow,
	fence PeerInboxSemanticFence, at time.Time,
) error {
	if row.inboxID != fence.inboxID || row.status != model.InboxProcessing || !row.hasLease ||
		row.attempts != fence.attempt || row.leaseOwner != fence.leaseOwner ||
		!row.leaseUntil.Equal(fence.leaseUntil) || row.semanticNonce != fence.semanticNonce ||
		at.Before(row.updatedAt) || !at.Before(row.leaseUntil) || row.diagnostic != "" {
		return ErrPeerInboxSemanticStale
	}
	return nil
}

func newPeerInboxSemanticTransitionReceipt(kind peerInboxSemanticTransitionKind,
	fence PeerInboxSemanticFence, at time.Time, diagnostic PeerInboxSemanticRetryDiagnostic,
	retryAfter time.Duration, output peerInboxSemanticResultProjection,
) (peerInboxSemanticTransitionReceipt, error) {
	if !kind.valid() || fence.inboxID.IsZero() || fence.attempt == 0 ||
		!validPublicationIdentifier(fence.leaseOwner) || fence.leaseUntil.IsZero() ||
		fence.snapshotDigest.IsZero() || !at.Before(fence.leaseUntil) ||
		output.inboxID != fence.inboxID || output.semanticNonce != fence.semanticNonce ||
		output.attempt != fence.attempt || !output.updatedAt.Equal(at) ||
		output.hasLocalEvent || output.hasDecision || output.hasReceipt {
		return peerInboxSemanticTransitionReceipt{}, fmt.Errorf("%w: invalid transition receipt authority",
			ErrPeerInboxSemanticInvariant)
	}
	switch kind {
	case peerInboxSemanticTransitionRenew:
		expectedLease, err := canonicalStoreTime(at.Add(peerInboxSemanticLease))
		if err != nil || diagnostic != "" || retryAfter != 0 ||
			output.status != model.InboxProcessing || !output.hasLease ||
			output.leaseOwner != fence.leaseOwner || !output.leaseUntil.Equal(expectedLease) ||
			output.diagnostic != "" {
			return peerInboxSemanticTransitionReceipt{}, fmt.Errorf("%w: invalid renew receipt output",
				ErrPeerInboxSemanticInvariant)
		}
	case peerInboxSemanticTransitionRetry:
		expectedNext, err := canonicalStoreTime(at.Add(retryAfter))
		if err != nil || !diagnostic.Valid() || retryAfter < minPeerInboxSemanticRetry ||
			retryAfter > maxPeerInboxSemanticRetry || output.status != model.InboxRetry ||
			output.hasLease || output.leaseOwner != "" || !output.leaseUntil.IsZero() ||
			output.diagnostic != string(diagnostic) || !output.nextAttemptAt.Equal(expectedNext) {
			return peerInboxSemanticTransitionReceipt{}, fmt.Errorf("%w: invalid retry receipt output",
				ErrPeerInboxSemanticInvariant)
		}
	}
	digest, err := peerInboxSemanticTransitionRequestDigest(kind, fence, at,
		diagnostic, retryAfter)
	if err != nil {
		return peerInboxSemanticTransitionReceipt{}, err
	}
	return peerInboxSemanticTransitionReceipt{inboxID: fence.inboxID, kind: kind,
		oldLeaseOwner: fence.leaseOwner, oldLeaseUntil: fence.leaseUntil,
		oldAttempt: fence.attempt, semanticNonce: fence.semanticNonce,
		snapshotDigest: fence.snapshotDigest, requestedAt: at, requestDigest: digest,
		retryDiagnostic: diagnostic, retryAfter: retryAfter, output: output}, nil
}

func peerInboxSemanticTransitionRequestDigest(kind peerInboxSemanticTransitionKind,
	fence PeerInboxSemanticFence, at time.Time, diagnostic PeerInboxSemanticRetryDiagnostic,
	retryAfter time.Duration,
) (model.Digest, error) {
	if !kind.valid() {
		return model.Digest{}, fmt.Errorf("%w: unknown semantic transition kind",
			ErrPeerInboxSemanticInvariant)
	}
	canonical, err := model.JSONFrom(struct {
		Attempt         uint32                          `json:"attempt"`
		Domain          string                          `json:"domain"`
		InboxID         model.InboxID                   `json:"inbox_id"`
		Kind            peerInboxSemanticTransitionKind `json:"kind"`
		LeaseOwner      string                          `json:"lease_owner"`
		LeaseUntil      string                          `json:"lease_until"`
		RequestedAt     string                          `json:"requested_at"`
		RetryAfterNS    int64                           `json:"retry_after_ns"`
		RetryDiagnostic string                          `json:"retry_diagnostic"`
		SemanticNonce   []byte                          `json:"semantic_nonce"`
		SnapshotDigest  model.Digest                    `json:"snapshot_digest"`
	}{fence.attempt, peerInboxSemanticTransitionRequestDomain, fence.inboxID, kind,
		fence.leaseOwner, storeTime(fence.leaseUntil), storeTime(at), int64(retryAfter),
		string(diagnostic), append([]byte(nil), fence.semanticNonce[:]...), fence.snapshotDigest})
	if err != nil {
		return model.Digest{}, fmt.Errorf("%w: canonical transition request: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	return model.Sum(canonical.Bytes()), nil
}

func (receipt peerInboxSemanticTransitionReceipt) matchesRequest(
	kind peerInboxSemanticTransitionKind, fence PeerInboxSemanticFence, at time.Time,
	diagnostic PeerInboxSemanticRetryDiagnostic, retryAfter time.Duration,
) bool {
	if receipt.inboxID != fence.inboxID || receipt.kind != kind ||
		receipt.oldLeaseOwner != fence.leaseOwner ||
		!receipt.oldLeaseUntil.Equal(fence.leaseUntil) || receipt.oldAttempt != fence.attempt ||
		receipt.semanticNonce != fence.semanticNonce ||
		receipt.snapshotDigest != fence.snapshotDigest || !receipt.requestedAt.Equal(at) ||
		receipt.retryDiagnostic != diagnostic || receipt.retryAfter != retryAfter {
		return false
	}
	digest, err := peerInboxSemanticTransitionRequestDigest(kind, fence, at,
		diagnostic, retryAfter)
	return err == nil && digest == receipt.requestDigest
}

func (projection peerInboxSemanticResultProjection) equal(
	other peerInboxSemanticResultProjection,
) bool {
	return projection.inboxID == other.inboxID &&
		projection.semanticNonce == other.semanticNonce &&
		projection.status == other.status && projection.attempt == other.attempt &&
		projection.nextAttemptAt.Equal(other.nextAttemptAt) &&
		projection.leaseOwner == other.leaseOwner && projection.hasLease == other.hasLease &&
		projection.leaseUntil.Equal(other.leaseUntil) &&
		projection.diagnostic == other.diagnostic &&
		projection.updatedAt.Equal(other.updatedAt) &&
		projection.hasLocalEvent == other.hasLocalEvent &&
		projection.hasDecision == other.hasDecision && projection.hasReceipt == other.hasReceipt
}

func readPeerInboxSemanticTransitionReceipt(ctx context.Context, tx *sql.Tx,
	inboxID model.InboxID,
) (peerInboxSemanticTransitionReceipt, bool, error) {
	var inboxText, kindText, oldOwner, oldLeaseText, requestedText string
	var oldAttempt, outputAttempt int64
	var nonceRaw, snapshotRaw, requestRaw []byte
	var retryDiagnostic sql.NullString
	var retryAfter sql.NullInt64
	var outputStatus, outputNextText, outputUpdatedText string
	var outputOwner, outputLease, outputDiagnostic sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT inbox_id,transition_kind,old_lease_owner,
		old_lease_until,old_attempt,semantic_nonce,snapshot_digest,requested_at,request_digest,
		retry_diagnostic,retry_after_ns,output_status,output_attempt,output_next_attempt_at,
		output_lease_owner,output_lease_until,output_diagnostic,output_updated_at
		FROM peer_inbox_semantic_transition_receipts WHERE inbox_id=?`, inboxID.String()).
		Scan(&inboxText, &kindText, &oldOwner, &oldLeaseText, &oldAttempt, &nonceRaw,
			&snapshotRaw, &requestedText, &requestRaw, &retryDiagnostic, &retryAfter,
			&outputStatus, &outputAttempt, &outputNextText, &outputOwner, &outputLease,
			&outputDiagnostic, &outputUpdatedText)
	if errors.Is(err, sql.ErrNoRows) {
		return peerInboxSemanticTransitionReceipt{}, false, nil
	}
	if err != nil {
		return peerInboxSemanticTransitionReceipt{}, false, fmt.Errorf("%w: read transition receipt: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	parsedInbox, inboxErr := model.ParseInboxID(inboxText)
	oldLease, oldLeaseErr := parseCanonicalStoreTime(oldLeaseText)
	requestedAt, requestedErr := parseCanonicalStoreTime(requestedText)
	snapshotDigest, snapshotErr := model.DigestFromBytes(snapshotRaw)
	requestDigest, requestErr := model.DigestFromBytes(requestRaw)
	if inboxErr != nil || parsedInbox != inboxID || !validPublicationIdentifier(oldOwner) ||
		oldAttempt < 1 || uint64(oldAttempt) > math.MaxUint32 || len(nonceRaw) != 32 ||
		oldLeaseErr != nil || requestedErr != nil || !requestedAt.Before(oldLease) ||
		snapshotErr != nil || requestErr != nil {
		return peerInboxSemanticTransitionReceipt{}, false, fmt.Errorf("%w: malformed transition receipt request",
			ErrPeerInboxSemanticInvariant)
	}
	var nonce [32]byte
	copy(nonce[:], nonceRaw)
	kind := peerInboxSemanticTransitionKind(kindText)
	diagnostic := PeerInboxSemanticRetryDiagnostic(retryDiagnostic.String)
	duration := time.Duration(retryAfter.Int64)
	if kind == peerInboxSemanticTransitionRenew {
		if retryDiagnostic.Valid || retryAfter.Valid {
			return peerInboxSemanticTransitionReceipt{}, false, fmt.Errorf("%w: renew receipt has retry fields",
				ErrPeerInboxSemanticInvariant)
		}
		diagnostic, duration = "", 0
	} else if kind != peerInboxSemanticTransitionRetry || !retryDiagnostic.Valid ||
		!retryAfter.Valid || !diagnostic.Valid() || duration < minPeerInboxSemanticRetry ||
		duration > maxPeerInboxSemanticRetry {
		return peerInboxSemanticTransitionReceipt{}, false, fmt.Errorf("%w: malformed transition receipt kind",
			ErrPeerInboxSemanticInvariant)
	}
	output, err := parsePeerInboxSemanticResultProjection(parsedInbox, nonce, outputStatus,
		outputAttempt, outputNextText, outputOwner, outputLease, outputDiagnostic,
		outputUpdatedText, false, false, false)
	if err != nil {
		return peerInboxSemanticTransitionReceipt{}, false, err
	}
	fence := PeerInboxSemanticFence{inboxID: parsedInbox, leaseOwner: oldOwner,
		leaseUntil: oldLease, attempt: uint32(oldAttempt), semanticNonce: nonce,
		snapshotDigest: snapshotDigest}
	validated, err := newPeerInboxSemanticTransitionReceipt(kind, fence, requestedAt,
		diagnostic, duration, output)
	if err != nil || validated.requestDigest != requestDigest {
		return peerInboxSemanticTransitionReceipt{}, false, fmt.Errorf("%w: transition receipt digest or output: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	return validated, true, nil
}

func readValidatedPeerInboxSemanticTransitionReceipt(ctx context.Context, tx *sql.Tx,
	inboxID model.InboxID,
) (peerInboxSemanticTransitionReceipt, bool, error) {
	receipt, found, err := readPeerInboxSemanticTransitionReceipt(ctx, tx, inboxID)
	if err != nil || !found {
		return receipt, found, err
	}
	current, err := readPeerInboxSemanticResultProjection(ctx, tx, inboxID)
	if err != nil {
		return peerInboxSemanticTransitionReceipt{}, false, err
	}
	if !receipt.output.equal(current) {
		return peerInboxSemanticTransitionReceipt{}, false, fmt.Errorf(
			"%w: transition receipt output no longer matches Inbox",
			ErrPeerInboxSemanticInvariant)
	}
	return receipt, true, nil
}

func readPeerInboxSemanticResultProjection(ctx context.Context, tx *sql.Tx,
	inboxID model.InboxID,
) (peerInboxSemanticResultProjection, error) {
	var inboxText, statusText, nextText, updatedText string
	var attempt int64
	var nonceRaw []byte
	var owner, lease, diagnostic, localEvent, receiptEvent sql.NullString
	var decision []byte
	err := tx.QueryRowContext(ctx, `SELECT inbox_id,semantic_nonce,status,attempts,
		next_attempt_at,lease_owner,lease_until,diagnostic,updated_at,local_event_id,
		decision_json,receipt_event_id FROM peer_inbox WHERE inbox_id=?`, inboxID.String()).
		Scan(&inboxText, &nonceRaw, &statusText, &attempt, &nextText, &owner, &lease,
			&diagnostic, &updatedText, &localEvent, &decision, &receiptEvent)
	if errors.Is(err, sql.ErrNoRows) {
		return peerInboxSemanticResultProjection{}, ErrPeerInboxSemanticStale
	}
	if err != nil {
		return peerInboxSemanticResultProjection{}, fmt.Errorf("%w: read transition output: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	parsedInbox, inboxErr := model.ParseInboxID(inboxText)
	if inboxErr != nil || parsedInbox != inboxID || len(nonceRaw) != 32 {
		return peerInboxSemanticResultProjection{}, fmt.Errorf("%w: malformed transition output identity",
			ErrPeerInboxSemanticInvariant)
	}
	var nonce [32]byte
	copy(nonce[:], nonceRaw)
	return parsePeerInboxSemanticResultProjection(parsedInbox, nonce, statusText,
		attempt, nextText, owner, lease, diagnostic, updatedText, localEvent.Valid,
		len(decision) != 0, receiptEvent.Valid)
}

func parsePeerInboxSemanticResultProjection(inboxID model.InboxID, nonce [32]byte,
	statusText string, attempt int64, nextText string, owner, lease, diagnostic sql.NullString,
	updatedText string, hasLocalEvent, hasDecision, hasReceipt bool,
) (peerInboxSemanticResultProjection, error) {
	status := model.InboxStatus(statusText)
	nextAttempt, nextErr := parseCanonicalStoreTime(nextText)
	updatedAt, updatedErr := parseCanonicalStoreTime(updatedText)
	if !status.Valid() || attempt < 0 || uint64(attempt) > math.MaxUint32 ||
		nextErr != nil || updatedErr != nil || owner.Valid != lease.Valid ||
		diagnostic.Valid && (diagnostic.String == "" || !validPublicationDiagnostic(diagnostic.String)) {
		return peerInboxSemanticResultProjection{}, fmt.Errorf("%w: malformed transition output projection",
			ErrPeerInboxSemanticInvariant)
	}
	var leaseUntil time.Time
	if lease.Valid {
		var err error
		leaseUntil, err = parseCanonicalStoreTime(lease.String)
		if err != nil || !validPublicationIdentifier(owner.String) || !leaseUntil.After(updatedAt) ||
			status != model.InboxWaitingArtifact && status != model.InboxProcessing {
			return peerInboxSemanticResultProjection{}, fmt.Errorf("%w: malformed transition output lease",
				ErrPeerInboxSemanticInvariant)
		}
	} else if status == model.InboxWaitingArtifact || status == model.InboxProcessing {
		return peerInboxSemanticResultProjection{}, fmt.Errorf("%w: missing transition output lease",
			ErrPeerInboxSemanticInvariant)
	}
	return peerInboxSemanticResultProjection{inboxID: inboxID, semanticNonce: nonce,
		status: status, attempt: uint32(attempt), nextAttemptAt: nextAttempt,
		leaseOwner: owner.String, leaseUntil: leaseUntil, hasLease: lease.Valid,
		diagnostic: diagnostic.String, updatedAt: updatedAt, hasLocalEvent: hasLocalEvent,
		hasDecision: hasDecision, hasReceipt: hasReceipt}, nil
}

func upsertPeerInboxSemanticTransitionReceipt(ctx context.Context, tx *sql.Tx,
	receipt peerInboxSemanticTransitionReceipt,
) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO peer_inbox_semantic_transition_receipts(
		inbox_id,transition_kind,old_lease_owner,old_lease_until,old_attempt,semantic_nonce,
		snapshot_digest,requested_at,request_digest,retry_diagnostic,retry_after_ns,
		output_status,output_attempt,output_next_attempt_at,output_lease_owner,
		output_lease_until,output_diagnostic,output_updated_at
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
	ON CONFLICT(inbox_id) DO UPDATE SET
		transition_kind=excluded.transition_kind,old_lease_owner=excluded.old_lease_owner,
		old_lease_until=excluded.old_lease_until,old_attempt=excluded.old_attempt,
		semantic_nonce=excluded.semantic_nonce,snapshot_digest=excluded.snapshot_digest,
		requested_at=excluded.requested_at,request_digest=excluded.request_digest,
		retry_diagnostic=excluded.retry_diagnostic,retry_after_ns=excluded.retry_after_ns,
		output_status=excluded.output_status,output_attempt=excluded.output_attempt,
		output_next_attempt_at=excluded.output_next_attempt_at,
		output_lease_owner=excluded.output_lease_owner,
		output_lease_until=excluded.output_lease_until,
		output_diagnostic=excluded.output_diagnostic,output_updated_at=excluded.output_updated_at`,
		receipt.inboxID.String(), string(receipt.kind), receipt.oldLeaseOwner,
		storeTime(receipt.oldLeaseUntil), receipt.oldAttempt, receipt.semanticNonce[:],
		receipt.snapshotDigest.Bytes(), storeTime(receipt.requestedAt), receipt.requestDigest.Bytes(),
		nullablePeerInboxSemanticText(string(receipt.retryDiagnostic), receipt.kind == peerInboxSemanticTransitionRetry),
		nullablePeerInboxSemanticInt64(int64(receipt.retryAfter), receipt.kind == peerInboxSemanticTransitionRetry),
		string(receipt.output.status), receipt.output.attempt,
		storeTime(receipt.output.nextAttemptAt),
		nullablePeerInboxSemanticText(receipt.output.leaseOwner, receipt.output.hasLease),
		nullablePeerInboxSemanticTime(receipt.output.leaseUntil, receipt.output.hasLease),
		nullablePeerInboxSemanticText(receipt.output.diagnostic, receipt.output.diagnostic != ""),
		storeTime(receipt.output.updatedAt))
	if err != nil {
		return fmt.Errorf("%w: write transition receipt: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	return nil
}

func derivePeerInboxSemanticSnapshot(ctx context.Context, tx *sql.Tx,
	row peerInboxSemanticRow, at time.Time,
) (peerInboxSemanticSnapshot, error) {
	if err := requirePeerInboxArtifactAuthority(ctx, tx, row.peerInboxArtifactRow, at); err != nil {
		if errors.Is(err, ErrPeerInboxArtifactAuthority) {
			return peerInboxSemanticSnapshot{}, ErrPeerInboxSemanticAuthority
		}
		return peerInboxSemanticSnapshot{}, fmt.Errorf("%w: signed authority: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	if err := requirePeerInboxArtifactClosures(ctx, tx, row.requiredRoots, at); err != nil {
		return peerInboxSemanticSnapshot{}, fmt.Errorf("%w: verified Artifact closure: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	if err := requireExactPeerInboxArtifactPins(ctx, tx, row.inboxID, row.requiredRoots, at); err != nil {
		return peerInboxSemanticSnapshot{}, fmt.Errorf("%w: permanent Inbox pins: %v",
			ErrPeerInboxSemanticInvariant, err)
	}

	projected, err := model.ProjectImportedPublication(&row.publication)
	if err != nil || projected.Event().Source() != model.EventSourceImported ||
		projected.Key() != row.publication.Key() || projected.Digest() != row.publication.Digest() ||
		projected.Event().Key() != row.publication.Event().Key() ||
		projected.Event().Digest() != row.publication.Event().Digest() ||
		!bytes.Equal(projected.WireJSON().Bytes(), row.publication.WireJSON().Bytes()) {
		return peerInboxSemanticSnapshot{}, fmt.Errorf("%w: imported Event projection: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	currentWork, hasCurrentWork, err := readPeerInboxSemanticCurrentWork(ctx, tx,
		projected.Event().Scope().WorkRef())
	if err != nil {
		return peerInboxSemanticSnapshot{}, err
	}
	causalEvents, err := readPeerInboxSemanticCausalEvents(ctx, tx, projected.Event())
	if err != nil {
		return peerInboxSemanticSnapshot{}, err
	}
	decisionSeed := peerInboxSemanticDecisionSeed(row.semanticNonce)
	digest, err := digestPeerInboxSemanticSnapshot(row, projected, currentWork,
		hasCurrentWork, causalEvents, decisionSeed)
	if err != nil {
		return peerInboxSemanticSnapshot{}, err
	}
	return peerInboxSemanticSnapshot{publication: projected, importedEvent: projected.Event(),
		currentWork: currentWork, hasCurrentWork: hasCurrentWork,
		causalEvents: causalEvents, decisionSeed: decisionSeed, digest: digest}, nil
}

func readPeerInboxSemanticCurrentWork(ctx context.Context, tx *sql.Tx,
	ref model.WorkRef,
) (model.ReviewWork, bool, error) {
	work, err := readReviewWork(ctx, tx, ref)
	if errors.Is(err, sql.ErrNoRows) {
		return model.ReviewWork{}, false, nil
	}
	if err != nil {
		return model.ReviewWork{}, false, fmt.Errorf("%w: current Work: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	if work.Ref() != ref || work.ChannelID().IsZero() {
		return model.ReviewWork{}, false, fmt.Errorf("%w: current Work scope",
			ErrPeerInboxSemanticInvariant)
	}
	update, err := readCurrentSourceEvent(ctx, tx, work.UpdatedBy())
	if err != nil {
		return model.ReviewWork{}, false, fmt.Errorf("%w: current Work update Event: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	facts, factsErr := decodeClosedEventPayload(update)
	exact, exactErr := currentWorkIsExactSource(update, work, facts)
	if factsErr != nil || exactErr != nil || !exact {
		return model.ReviewWork{}, false, fmt.Errorf("%w: current Work update authority (exact %t): %v",
			ErrPeerInboxSemanticInvariant, exact, errors.Join(factsErr, exactErr))
	}
	return work, true, nil
}

func readPeerInboxSemanticCausalEvents(ctx context.Context, tx *sql.Tx,
	incoming model.Event,
) ([]model.Event, error) {
	result := make([]model.Event, 0, len(incoming.CausedBy()))
	for _, key := range incoming.CausedBy() {
		var present int
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE event_id=?)`,
			key.EventID().String()).Scan(&present); err != nil {
			return nil, fmt.Errorf("%w: causal Event presence: %v",
				ErrPeerInboxSemanticInvariant, err)
		}
		if present == 0 {
			continue
		}
		event, err := readCurrentSourceEvent(ctx, tx, key.EventID())
		if err != nil || event.Key() != key {
			return nil, fmt.Errorf("%w: causal Event identity: %v",
				ErrPeerInboxSemanticInvariant, err)
		}
		result = append(result, event)
	}
	return result, nil
}

type peerInboxSemanticEventSnapshot struct {
	Canonical model.JSON        `json:"canonical_event"`
	Digest    model.Digest      `json:"event_digest"`
	Key       model.EventKey    `json:"event_key"`
	Source    model.EventSource `json:"source"`
}

type peerInboxSemanticWorkSnapshot struct {
	ChannelID        model.ChannelID `json:"channel_id"`
	DeadlineUnixNano int64           `json:"deadline_unix_nano"`
	Iteration        uint8           `json:"iteration"`
	Participants     struct {
		Initiator      model.PeerID `json:"initiator_peer_id"`
		Reviewer       model.PeerID `json:"reviewer_peer_id"`
		RosterRevision uint64       `json:"roster_revision"`
	} `json:"participants"`
	Ref       model.WorkRef   `json:"ref"`
	State     model.WorkState `json:"state"`
	StateData model.JSON      `json:"state_data"`
	UpdatedAt string          `json:"updated_at"`
	UpdatedBy model.EventID   `json:"updated_by_event"`
	Version   uint64          `json:"version"`
}

func digestPeerInboxSemanticSnapshot(row peerInboxSemanticRow,
	publication model.SignedPublication, currentWork model.ReviewWork, hasCurrentWork bool,
	causalEvents []model.Event, decisionSeed model.Digest,
) (model.Digest, error) {
	incoming := peerInboxSemanticEventSnapshot{Canonical: publication.Event().CanonicalJSON(),
		Digest: publication.Event().Digest(), Key: publication.Event().Key(),
		Source: model.EventSourceImported}
	causes := make([]peerInboxSemanticEventSnapshot, len(causalEvents))
	for index, event := range causalEvents {
		causes[index] = peerInboxSemanticEventSnapshot{Canonical: event.CanonicalJSON(),
			Digest: event.Digest(), Key: event.Key(), Source: event.Source()}
	}
	var work *peerInboxSemanticWorkSnapshot
	if hasCurrentWork {
		projection := peerInboxSemanticWorkSnapshot{ChannelID: currentWork.ChannelID(),
			DeadlineUnixNano: currentWork.DeadlineUnixNano(), Iteration: currentWork.Iteration(),
			Ref: currentWork.Ref(), State: currentWork.State(), StateData: currentWork.StateData(),
			UpdatedAt: storeTime(currentWork.UpdatedAt()), UpdatedBy: currentWork.UpdatedBy(),
			Version: currentWork.Version()}
		participants := currentWork.Participants()
		projection.Participants.Initiator = participants.InitiatorPeerID()
		projection.Participants.Reviewer = participants.ReviewerPeerID()
		projection.Participants.RosterRevision = participants.RosterRevision()
		work = &projection
	}
	canonical, err := model.JSONFrom(struct {
		CausalEvents  []peerInboxSemanticEventSnapshot `json:"causal_events"`
		CurrentWork   *peerInboxSemanticWorkSnapshot   `json:"current_work"`
		DecisionSeed  model.Digest                     `json:"decision_seed"`
		Domain        string                           `json:"domain"`
		ImportedEvent peerInboxSemanticEventSnapshot   `json:"imported_event"`
		InboxID       model.InboxID                    `json:"inbox_id"`
		Publication   model.JSON                       `json:"signed_publication"`
		RequiredRoots []model.Digest                   `json:"required_artifact_roots"`
	}{causes, work, decisionSeed, peerInboxSemanticSnapshotDomain, incoming,
		row.inboxID, publication.WireJSON(), append([]model.Digest(nil), row.requiredRoots...)})
	if err != nil {
		return model.Digest{}, fmt.Errorf("%w: canonical snapshot: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	return model.Sum(canonical.Bytes()), nil
}

func peerInboxSemanticDecisionSeed(nonce [32]byte) model.Digest {
	input := make([]byte, 0, len(peerInboxSemanticSeedDomain)+1+len(nonce))
	input = append(input, peerInboxSemanticSeedDomain...)
	input = append(input, 0)
	input = append(input, nonce[:]...)
	return model.Sum(input)
}

func peerInboxSemanticClaim(fence PeerInboxSemanticFence, row peerInboxSemanticRow,
	snapshot peerInboxSemanticSnapshot,
) PeerInboxSemanticClaim {
	return PeerInboxSemanticClaim{fence: fence, publication: snapshot.publication,
		importedEvent: snapshot.importedEvent, currentWork: snapshot.currentWork,
		hasCurrentWork: snapshot.hasCurrentWork,
		causalEvents:   append([]model.Event(nil), snapshot.causalEvents...),
		requiredRoots:  append([]model.Digest(nil), row.requiredRoots...),
		decisionSeed:   snapshot.decisionSeed}
}

func nullablePeerInboxSemanticText(value string, valid bool) any {
	if !valid {
		return nil
	}
	return value
}

func nullablePeerInboxSemanticTime(value time.Time, valid bool) any {
	if !valid {
		return nil
	}
	return storeTime(value)
}

func nullablePeerInboxSemanticInt64(value int64, valid bool) any {
	if !valid {
		return nil
	}
	return value
}
