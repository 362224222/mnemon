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
	peerInboxArtifactLease    = 120 * time.Second
	peerInboxArtifactStageTTL = time.Hour
	minArtifactRetry          = time.Second
	maxArtifactRetry          = 300 * time.Second

	peerInboxArtifactRenewRequestDomain = "mnemon/r5/peer-inbox-artifact-renew-request/1"
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

type ProbePeerInboxArtifactAuthoritySpec struct {
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

type StagePeerInboxArtifactClosureSpec struct {
	Fence   PeerInboxArtifactFence
	Closure VerifiedArtifactClosure
	At      time.Time
}

type ReadPeerInboxArtifactRootSpec struct {
	Fence      PeerInboxArtifactFence
	RootDigest model.Digest
	At         time.Time
}

type PeerInboxArtifactRootState string

const (
	PeerInboxArtifactRootStaged   PeerInboxArtifactRootState = "staged"
	PeerInboxArtifactRootVerified PeerInboxArtifactRootState = "verified"
)

func (state PeerInboxArtifactRootState) Valid() bool {
	return state == PeerInboxArtifactRootStaged || state == PeerInboxArtifactRootVerified
}

// PeerInboxArtifactRoot is a complete, fence-scoped import checkpoint. Its
// state is explicit because a staged closure is resumable metadata, not yet
// verified local Artifact authority.
type PeerInboxArtifactRoot struct {
	rootDigest     model.Digest
	manifest       model.JSON
	manifestDigest model.Digest
	totalBytes     uint64
	state          PeerInboxArtifactRootState
	createdAt      time.Time
	verifiedAt     time.Time
}

func (root PeerInboxArtifactRoot) RootDigest() model.Digest     { return root.rootDigest }
func (root PeerInboxArtifactRoot) Manifest() model.JSON         { return root.manifest }
func (root PeerInboxArtifactRoot) ManifestDigest() model.Digest { return root.manifestDigest }
func (root PeerInboxArtifactRoot) TotalBytes() uint64           { return root.totalBytes }
func (root PeerInboxArtifactRoot) State() PeerInboxArtifactRootState {
	return root.state
}
func (root PeerInboxArtifactRoot) CreatedAt() time.Time { return root.createdAt }
func (root PeerInboxArtifactRoot) VerifiedAt() (time.Time, bool) {
	return root.verifiedAt, root.state == PeerInboxArtifactRootVerified
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

type PeerInboxArtifactStage struct {
	changed  bool
	replayed bool
}

func (result PeerInboxArtifactStage) Changed() bool  { return result.changed }
func (result PeerInboxArtifactStage) Replayed() bool { return result.replayed }

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
	semanticNonce [32]byte
}

type peerInboxArtifactResultProjection struct {
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
}

type peerInboxArtifactRenewReceipt struct {
	inboxID       model.InboxID
	oldLeaseOwner string
	oldLeaseUntil time.Time
	oldAttempt    uint32
	semanticNonce [32]byte
	requestedAt   time.Time
	requestDigest model.Digest
	output        peerInboxArtifactResultProjection
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
	_, hasRenewReceipt, err := readValidatedPeerInboxArtifactRenewReceipt(ctx, tx, row)
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
	if err := deletePeerInboxArtifactRenewReceipt(ctx, tx, row.inboxID,
		hasRenewReceipt); err != nil {
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

// ProbePeerInboxArtifactAuthority is the receiver's read-only, fence-bound
// authority gate for work performed outside SQLite. It neither extends nor
// settles the lease: the exact claim generation must still be live and its
// Channel, origin binding, quarantine and signed publication authority must
// all remain current at trusted At.
func (s *Store) ProbePeerInboxArtifactAuthority(ctx context.Context,
	spec ProbePeerInboxArtifactAuthoritySpec,
) error {
	at, err := validatePeerInboxArtifactSettlementCall(s, ctx, spec.Fence, spec.At)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("%w: probe authority begin: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	defer tx.Rollback()
	row, err := readPeerInboxArtifactRow(ctx, tx, spec.Fence.inboxID)
	if err != nil {
		return err
	}
	if _, _, err := readValidatedPeerInboxArtifactRenewReceipt(ctx, tx, row); err != nil {
		return err
	}
	if err := requireLivePeerInboxArtifactFence(row, spec.Fence, at); err != nil {
		return err
	}
	if err := requirePeerInboxArtifactAuthority(ctx, tx, row, at); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%w: probe authority commit: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	return nil
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
	stageExpiresAt, err := canonicalStoreTime(leaseUntil.Add(peerInboxArtifactStageTTL))
	if err != nil {
		return PeerInboxArtifactRenewal{}, fmt.Errorf("%w: renewal stage expiry: %v",
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
	receipt, hasRenewReceipt, err := readValidatedPeerInboxArtifactRenewReceipt(ctx, tx, row)
	if err != nil {
		return PeerInboxArtifactRenewal{}, err
	}
	if hasRenewReceipt && receipt.matchesRequest(spec.Fence, at) {
		if err := requirePeerInboxArtifactStagePinsAt(ctx, tx, row.inboxID,
			row.requiredRoots, leaseUntil, stageExpiresAt, true); err != nil {
			return PeerInboxArtifactRenewal{}, err
		}
		if err := tx.Commit(); err != nil {
			return PeerInboxArtifactRenewal{}, fmt.Errorf("renew Peer Inbox Artifact: replay commit: %w", err)
		}
		return PeerInboxArtifactRenewal{fence: PeerInboxArtifactFence{inboxID: row.inboxID,
			leaseOwner: receipt.output.leaseOwner, leaseUntil: receipt.output.leaseUntil,
			attempt: receipt.output.attempt}, replayed: true}, nil
	}
	if err := requireLivePeerInboxArtifactFence(row, spec.Fence, at); err != nil {
		return PeerInboxArtifactRenewal{}, err
	}
	if err := requirePeerInboxArtifactAuthority(ctx, tx, row, at); err != nil {
		return PeerInboxArtifactRenewal{}, err
	}
	if _, err := refreshExistingPeerInboxArtifactStagePins(ctx, tx, row.inboxID,
		row.requiredRoots, at, stageExpiresAt); err != nil {
		return PeerInboxArtifactRenewal{}, err
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
	output := peerInboxArtifactResultFromRow(row)
	output.leaseUntil = leaseUntil
	output.updatedAt = at
	renewReceipt, err := newPeerInboxArtifactRenewReceipt(spec.Fence, row.semanticNonce,
		at, output)
	if err != nil {
		return PeerInboxArtifactRenewal{}, err
	}
	if err := upsertPeerInboxArtifactRenewReceipt(ctx, tx, renewReceipt); err != nil {
		return PeerInboxArtifactRenewal{}, err
	}
	if err := tx.Commit(); err != nil {
		return PeerInboxArtifactRenewal{}, fmt.Errorf("renew Peer Inbox Artifact: commit: %w", err)
	}
	next := spec.Fence
	next.leaseUntil = leaseUntil
	return PeerInboxArtifactRenewal{fence: next, changed: true}, nil
}

// StagePeerInboxArtifactClosure installs the exact immutable relational
// closure for one live Inbox fence and gives it a bounded durable staging
// owner. It deliberately does not promote any root to verified authority;
// CAS materialization and rehashing happen outside this transaction.
func (s *Store) StagePeerInboxArtifactClosure(ctx context.Context,
	spec StagePeerInboxArtifactClosureSpec,
) (PeerInboxArtifactStage, error) {
	at, err := validatePeerInboxArtifactSettlementCall(s, ctx, spec.Fence, spec.At)
	if err != nil {
		return PeerInboxArtifactStage{}, err
	}
	closure, err := validateVerifiedArtifactClosure(spec.Closure)
	if err != nil {
		return PeerInboxArtifactStage{}, fmt.Errorf("%w: staged closure: %v",
			ErrPeerInboxArtifactInput, err)
	}
	for _, root := range closure.Roots {
		if root.CreatedAt.After(at) || root.VerifiedAt.After(at) {
			return PeerInboxArtifactStage{}, fmt.Errorf("%w: staged root time exceeds trusted time",
				ErrPeerInboxArtifactInput)
		}
	}
	for _, block := range closure.Blocks {
		if block.CreatedAt.After(at) {
			return PeerInboxArtifactStage{}, fmt.Errorf("%w: staged block time exceeds trusted time",
				ErrPeerInboxArtifactInput)
		}
	}
	expiresAt, err := canonicalStoreTime(spec.Fence.leaseUntil.Add(peerInboxArtifactStageTTL))
	if err != nil {
		return PeerInboxArtifactStage{}, fmt.Errorf("%w: stage expiry: %v",
			ErrPeerInboxArtifactInput, err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PeerInboxArtifactStage{}, fmt.Errorf("stage Peer Inbox Artifact closure: begin: %w", err)
	}
	defer tx.Rollback()
	row, err := readPeerInboxArtifactRow(ctx, tx, spec.Fence.inboxID)
	if err != nil {
		return PeerInboxArtifactStage{}, err
	}
	if _, _, err := readValidatedPeerInboxArtifactRenewReceipt(ctx, tx, row); err != nil {
		return PeerInboxArtifactStage{}, err
	}
	if err := requireLivePeerInboxArtifactFence(row, spec.Fence, at); err != nil {
		return PeerInboxArtifactStage{}, err
	}
	if err := requirePeerInboxArtifactAuthority(ctx, tx, row, at); err != nil {
		return PeerInboxArtifactStage{}, err
	}
	if !equalPeerInboxArtifactClosureRoots(row.requiredRoots, closure.Roots) {
		return PeerInboxArtifactStage{}, fmt.Errorf("%w: staged roots differ from immutable Inbox roots",
			ErrPeerInboxArtifactInput)
	}
	if err := requireArtifactGCQueueAvailableForClosure(ctx, tx, closure); err != nil {
		return PeerInboxArtifactStage{}, err
	}

	replayed := true
	for _, block := range closure.Blocks {
		stored, found, err := checkpointArtifactBlock(ctx, tx, block)
		if err != nil {
			return PeerInboxArtifactStage{}, err
		}
		if stored.CreatedAt.After(at) {
			return PeerInboxArtifactStage{}, fmt.Errorf("%w: shared staged block observation is newer",
				ErrPeerInboxArtifactStale)
		}
		replayed = replayed && found
	}

	rootStates := make(map[model.Digest]string, len(closure.Roots))
	for _, root := range closure.Roots {
		stored, state, found, err := stageArtifactClosureRoot(ctx, tx, root)
		if err != nil {
			return PeerInboxArtifactStage{}, err
		}
		if stored.CreatedAt.After(at) || state == "verified" && stored.VerifiedAt.After(at) {
			return PeerInboxArtifactStage{}, fmt.Errorf("%w: shared staged root observation is newer",
				ErrPeerInboxArtifactStale)
		}
		rootStates[stored.RootDigest] = state
		replayed = replayed && found
	}
	for _, root := range closure.Roots {
		state, err := checkpointArtifactRootBlockMap(ctx, tx, root.RootDigest,
			rootBlocksForDigest(closure.RootBlocks, root.RootDigest))
		if err != nil {
			return PeerInboxArtifactStage{}, err
		}
		if state.rootState != rootStates[root.RootDigest] {
			return PeerInboxArtifactStage{}, fmt.Errorf("%w: staged root state changed",
				ErrPeerInboxArtifactInvariant)
		}
		replayed = replayed && !state.changed
	}
	pinsReplayed, err := stagePeerInboxArtifactPins(ctx, tx, row.inboxID,
		row.requiredRoots, at, expiresAt)
	if err != nil {
		return PeerInboxArtifactStage{}, err
	}
	replayed = replayed && pinsReplayed
	if err := requirePromotablePeerInboxArtifactClosures(ctx, tx, row.requiredRoots, at); err != nil {
		return PeerInboxArtifactStage{}, err
	}
	if err := tx.Commit(); err != nil {
		return PeerInboxArtifactStage{}, fmt.Errorf("stage Peer Inbox Artifact closure: commit: %w", err)
	}
	return PeerInboxArtifactStage{changed: !replayed, replayed: replayed}, nil
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
	stageExpiresAt, err := canonicalStoreTime(nextAttempt.Add(peerInboxArtifactStageTTL))
	if err != nil {
		return PeerInboxArtifactSettlement{}, fmt.Errorf("%w: retry stage expiry: %v",
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
	_, hasRenewReceipt, err := readValidatedPeerInboxArtifactRenewReceipt(ctx, tx, row)
	if err != nil {
		return PeerInboxArtifactSettlement{}, err
	}
	if row.status == model.InboxRetry && row.attempts == spec.Fence.attempt &&
		row.diagnostic == string(spec.Diagnostic) && row.updatedAt.Equal(at) &&
		row.nextAttemptAt.Equal(nextAttempt) && !row.hasLease {
		if err := requirePeerInboxArtifactStagePinsAt(ctx, tx, row.inboxID,
			row.requiredRoots, at, stageExpiresAt, true); err != nil {
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
	hasPins, err := refreshExistingPeerInboxArtifactStagePins(ctx, tx, row.inboxID,
		row.requiredRoots, at, stageExpiresAt)
	if err != nil {
		return PeerInboxArtifactSettlement{}, err
	}
	if err := deletePeerInboxArtifactRenewReceipt(ctx, tx, row.inboxID,
		hasRenewReceipt); err != nil {
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
	if hasPins {
		if err := requirePeerInboxArtifactStagePinsAt(ctx, tx, row.inboxID,
			row.requiredRoots, at, stageExpiresAt, false); err != nil {
			return PeerInboxArtifactSettlement{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return PeerInboxArtifactSettlement{}, fmt.Errorf("retry Peer Inbox Artifact: commit: %w", err)
	}
	return PeerInboxArtifactSettlement{status: model.InboxRetry, nextAttemptAt: nextAttempt,
		changed: true}, nil
}

// QuarantinePeerInboxArtifact terminally records one closed remote protocol,
// manifest, digest, or limit failure. Existing stage ownership is retained for
// one bounded cleanup window; this method never creates new pins.
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
	stageExpiresAt, err := canonicalStoreTime(at.Add(peerInboxArtifactStageTTL))
	if err != nil {
		return PeerInboxArtifactSettlement{}, fmt.Errorf("%w: quarantine stage expiry: %v",
			ErrPeerInboxArtifactInput, err)
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
	_, hasRenewReceipt, err := readValidatedPeerInboxArtifactRenewReceipt(ctx, tx, row)
	if err != nil {
		return PeerInboxArtifactSettlement{}, err
	}
	if row.status == model.InboxQuarantined && row.attempts == spec.Fence.attempt &&
		row.diagnostic == string(spec.Diagnostic) && row.updatedAt.Equal(at) &&
		row.nextAttemptAt.Equal(at) && !row.hasLease {
		if err := requirePeerInboxArtifactStagePinsAt(ctx, tx, row.inboxID,
			row.requiredRoots, at, stageExpiresAt, true); err != nil {
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
	hasPins, err := refreshExistingPeerInboxArtifactStagePins(ctx, tx, row.inboxID,
		row.requiredRoots, at, stageExpiresAt)
	if err != nil {
		return PeerInboxArtifactSettlement{}, err
	}
	if err := deletePeerInboxArtifactRenewReceipt(ctx, tx, row.inboxID,
		hasRenewReceipt); err != nil {
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
	if hasPins {
		if err := requirePeerInboxArtifactStagePinsAt(ctx, tx, row.inboxID,
			row.requiredRoots, at, stageExpiresAt, false); err != nil {
			return PeerInboxArtifactSettlement{}, err
		}
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
	_, hasRenewReceipt, err := readValidatedPeerInboxArtifactRenewReceipt(ctx, tx, row)
	if err != nil {
		return PeerInboxArtifactSettlement{}, err
	}
	replay := row.status == model.InboxReady && row.attempts == spec.Fence.attempt &&
		row.updatedAt.Equal(at) && row.nextAttemptAt.Equal(at) &&
		!row.hasLease && row.diagnostic == ""
	if replay {
		for _, root := range row.requiredRoots {
			if err := requireArtifactGCQueueAvailableForRoot(ctx, tx, root); err != nil {
				return PeerInboxArtifactSettlement{}, err
			}
		}
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
	if err := requirePromotablePeerInboxArtifactClosures(ctx, tx, row.requiredRoots, at); err != nil {
		return PeerInboxArtifactSettlement{}, err
	}
	if err := requirePeerInboxArtifactStagePinsAt(ctx, tx, row.inboxID,
		row.requiredRoots, at, time.Time{}, false); err != nil {
		return PeerInboxArtifactSettlement{}, err
	}
	for _, root := range row.requiredRoots {
		if err := requireArtifactGCQueueAvailableForRoot(ctx, tx, root); err != nil {
			return PeerInboxArtifactSettlement{}, err
		}
	}
	for _, root := range row.requiredRoots {
		result, err := tx.ExecContext(ctx, `UPDATE artifact_roots SET state='verified',verified_at=?
			WHERE root_digest=? AND state='staged' AND verified_at IS NULL`, storeTime(at), root.String())
		if err != nil {
			return PeerInboxArtifactSettlement{}, fmt.Errorf("%w: promote staged Inbox root: %v",
				ErrPeerInboxArtifactInvariant, err)
		}
		if affected, affectedErr := result.RowsAffected(); affectedErr != nil || affected > 1 {
			return PeerInboxArtifactSettlement{}, fmt.Errorf("%w: promote staged Inbox root cardinality",
				ErrPeerInboxArtifactInvariant)
		}
	}
	if len(row.requiredRoots) > 0 {
		result, err := tx.ExecContext(ctx, `UPDATE artifact_pins SET expires_at=NULL
			WHERE owner_kind='inbox' AND owner_id=? AND expires_at IS NOT NULL`, row.inboxID.String())
		if err != nil {
			return PeerInboxArtifactSettlement{}, fmt.Errorf("%w: make Inbox pins permanent: %v",
				ErrPeerInboxArtifactInvariant, err)
		}
		affected, affectedErr := result.RowsAffected()
		if affectedErr != nil || affected != int64(len(row.requiredRoots)) {
			return PeerInboxArtifactSettlement{}, fmt.Errorf("%w: make Inbox pins permanent cardinality",
				ErrPeerInboxArtifactInvariant)
		}
	}
	if err := requirePeerInboxArtifactClosures(ctx, tx, row.requiredRoots, at); err != nil {
		return PeerInboxArtifactSettlement{}, err
	}
	if err := requireExactPeerInboxArtifactPins(ctx, tx, row.inboxID, row.requiredRoots, at); err != nil {
		return PeerInboxArtifactSettlement{}, err
	}
	if err := deletePeerInboxArtifactRenewReceipt(ctx, tx, row.inboxID,
		hasRenewReceipt); err != nil {
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

// ReadPeerInboxArtifactRoot is the receiver's narrow durable import probe. It
// exposes neither SQL absence nor unrelated roots: the requested digest must
// belong to this exact still-live claim, and both staged and verified results
// require a complete immutable manifest/map/block projection.
func (s *Store) ReadPeerInboxArtifactRoot(ctx context.Context,
	spec ReadPeerInboxArtifactRootSpec,
) (PeerInboxArtifactRoot, bool, error) {
	if spec.RootDigest.IsZero() {
		return PeerInboxArtifactRoot{}, false, ErrPeerInboxArtifactInput
	}
	at, err := validatePeerInboxArtifactSettlementCall(s, ctx, spec.Fence, spec.At)
	if err != nil {
		return PeerInboxArtifactRoot{}, false, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return PeerInboxArtifactRoot{}, false, fmt.Errorf("read Peer Inbox Artifact root: begin: %w", err)
	}
	defer tx.Rollback()
	row, err := readPeerInboxArtifactRow(ctx, tx, spec.Fence.inboxID)
	if err != nil {
		return PeerInboxArtifactRoot{}, false, err
	}
	if _, _, err := readValidatedPeerInboxArtifactRenewReceipt(ctx, tx, row); err != nil {
		return PeerInboxArtifactRoot{}, false, err
	}
	if err := requireLivePeerInboxArtifactFence(row, spec.Fence, at); err != nil {
		return PeerInboxArtifactRoot{}, false, err
	}
	if !containsPeerInboxArtifactRoot(row.requiredRoots, spec.RootDigest) {
		return PeerInboxArtifactRoot{}, false, ErrPeerInboxArtifactInput
	}
	root, state, err := readArtifactRoot(ctx, tx, spec.RootDigest)
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.Commit(); err != nil {
			return PeerInboxArtifactRoot{}, false, fmt.Errorf("read Peer Inbox Artifact root: commit miss: %w", err)
		}
		return PeerInboxArtifactRoot{}, false, nil
	}
	rootState := PeerInboxArtifactRootState(state)
	if err != nil || !rootState.Valid() ||
		rootState == PeerInboxArtifactRootVerified && root.VerifiedAt.IsZero() {
		return PeerInboxArtifactRoot{}, false, fmt.Errorf("%w: durable root metadata: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	if root.CreatedAt.After(at) || rootState == PeerInboxArtifactRootVerified &&
		root.VerifiedAt.After(at) {
		return PeerInboxArtifactRoot{}, false, fmt.Errorf("%w: durable root observation is newer",
			ErrPeerInboxArtifactStale)
	}
	if rootState == PeerInboxArtifactRootStaged {
		if err := requirePeerInboxArtifactStagePinsAt(ctx, tx, row.inboxID,
			row.requiredRoots, at, time.Time{}, false); err != nil {
			if errors.Is(err, ErrPeerInboxArtifactNotReady) {
				if commitErr := tx.Commit(); commitErr != nil {
					return PeerInboxArtifactRoot{}, false,
						fmt.Errorf("read Peer Inbox Artifact root: commit unowned stage: %w", commitErr)
				}
				return PeerInboxArtifactRoot{}, false, nil
			}
			return PeerInboxArtifactRoot{}, false, err
		}
	}
	if err := requirePromotablePeerInboxArtifactClosures(ctx, tx,
		[]model.Digest{spec.RootDigest}, at); err != nil {
		return PeerInboxArtifactRoot{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return PeerInboxArtifactRoot{}, false, fmt.Errorf("read Peer Inbox Artifact root: commit: %w", err)
	}
	return PeerInboxArtifactRoot{rootDigest: root.RootDigest, manifest: root.Manifest,
		manifestDigest: root.ManifestDigest, totalBytes: root.TotalBytes, state: rootState,
		createdAt: root.CreatedAt, verifiedAt: root.VerifiedAt}, true, nil
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

func peerInboxArtifactResultFromRow(row peerInboxArtifactRow) peerInboxArtifactResultProjection {
	return peerInboxArtifactResultProjection{inboxID: row.inboxID,
		semanticNonce: row.semanticNonce, status: row.status, attempt: row.attempts,
		nextAttemptAt: row.nextAttemptAt, leaseOwner: row.leaseOwner,
		leaseUntil: row.leaseUntil, hasLease: row.hasLease, diagnostic: row.diagnostic,
		updatedAt: row.updatedAt}
}

func (projection peerInboxArtifactResultProjection) equal(
	other peerInboxArtifactResultProjection,
) bool {
	return projection.inboxID == other.inboxID &&
		projection.semanticNonce == other.semanticNonce &&
		projection.status == other.status && projection.attempt == other.attempt &&
		projection.nextAttemptAt.Equal(other.nextAttemptAt) &&
		projection.leaseOwner == other.leaseOwner && projection.hasLease == other.hasLease &&
		projection.leaseUntil.Equal(other.leaseUntil) &&
		projection.diagnostic == other.diagnostic && projection.updatedAt.Equal(other.updatedAt)
}

func newPeerInboxArtifactRenewReceipt(fence PeerInboxArtifactFence, semanticNonce [32]byte,
	at time.Time, output peerInboxArtifactResultProjection,
) (peerInboxArtifactRenewReceipt, error) {
	expectedLease, err := canonicalStoreTime(at.Add(peerInboxArtifactLease))
	if err != nil || fence.inboxID.IsZero() || fence.attempt == 0 ||
		!validPublicationIdentifier(fence.leaseOwner) || !at.Before(fence.leaseUntil) ||
		output.inboxID != fence.inboxID || output.semanticNonce != semanticNonce ||
		output.status != model.InboxWaitingArtifact || output.attempt != fence.attempt ||
		!output.hasLease || output.leaseOwner != fence.leaseOwner ||
		!output.leaseUntil.Equal(expectedLease) || output.diagnostic != "" ||
		!output.updatedAt.Equal(at) {
		return peerInboxArtifactRenewReceipt{}, fmt.Errorf("%w: invalid renew receipt authority or output",
			ErrPeerInboxArtifactInvariant)
	}
	digest, err := peerInboxArtifactRenewRequestDigest(fence, semanticNonce, at)
	if err != nil {
		return peerInboxArtifactRenewReceipt{}, err
	}
	return peerInboxArtifactRenewReceipt{inboxID: fence.inboxID,
		oldLeaseOwner: fence.leaseOwner, oldLeaseUntil: fence.leaseUntil,
		oldAttempt: fence.attempt, semanticNonce: semanticNonce, requestedAt: at,
		requestDigest: digest, output: output}, nil
}

func peerInboxArtifactRenewRequestDigest(fence PeerInboxArtifactFence,
	semanticNonce [32]byte, at time.Time,
) (model.Digest, error) {
	canonical, err := model.JSONFrom(struct {
		Attempt       uint32        `json:"attempt"`
		Domain        string        `json:"domain"`
		InboxID       model.InboxID `json:"inbox_id"`
		LeaseOwner    string        `json:"lease_owner"`
		LeaseUntil    string        `json:"lease_until"`
		RequestedAt   string        `json:"requested_at"`
		SemanticNonce []byte        `json:"semantic_nonce"`
	}{Attempt: fence.attempt, Domain: peerInboxArtifactRenewRequestDomain,
		InboxID: fence.inboxID, LeaseOwner: fence.leaseOwner,
		LeaseUntil: storeTime(fence.leaseUntil), RequestedAt: storeTime(at),
		SemanticNonce: append([]byte(nil), semanticNonce[:]...)})
	if err != nil {
		return model.Digest{}, fmt.Errorf("%w: canonical renew request: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	return model.Sum(canonical.Bytes()), nil
}

func (receipt peerInboxArtifactRenewReceipt) matchesRequest(fence PeerInboxArtifactFence,
	at time.Time,
) bool {
	if receipt.inboxID != fence.inboxID || receipt.oldLeaseOwner != fence.leaseOwner ||
		!receipt.oldLeaseUntil.Equal(fence.leaseUntil) || receipt.oldAttempt != fence.attempt ||
		!receipt.requestedAt.Equal(at) {
		return false
	}
	digest, err := peerInboxArtifactRenewRequestDigest(fence, receipt.semanticNonce, at)
	return err == nil && digest == receipt.requestDigest
}

func readPeerInboxArtifactRenewReceipt(ctx context.Context, tx *sql.Tx,
	inboxID model.InboxID,
) (peerInboxArtifactRenewReceipt, bool, error) {
	var inboxText, oldOwner, oldLeaseText, requestedText string
	var oldAttempt, outputAttempt int64
	var nonceRaw, requestRaw []byte
	var outputStatus, outputNextText, outputOwner, outputLeaseText, outputUpdatedText string
	var outputDiagnostic sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT inbox_id,old_lease_owner,old_lease_until,
		old_attempt,semantic_nonce,requested_at,request_digest,output_status,output_attempt,
		output_next_attempt_at,output_lease_owner,output_lease_until,output_diagnostic,
		output_updated_at FROM peer_inbox_artifact_renew_receipts WHERE inbox_id=?`,
		inboxID.String()).Scan(&inboxText, &oldOwner, &oldLeaseText, &oldAttempt,
		&nonceRaw, &requestedText, &requestRaw, &outputStatus, &outputAttempt,
		&outputNextText, &outputOwner, &outputLeaseText, &outputDiagnostic,
		&outputUpdatedText)
	if errors.Is(err, sql.ErrNoRows) {
		return peerInboxArtifactRenewReceipt{}, false, nil
	}
	if err != nil {
		return peerInboxArtifactRenewReceipt{}, false, fmt.Errorf("%w: read renew receipt: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	parsedInbox, inboxErr := model.ParseInboxID(inboxText)
	oldLease, oldLeaseErr := parseCanonicalStoreTime(oldLeaseText)
	requestedAt, requestedErr := parseCanonicalStoreTime(requestedText)
	requestDigest, requestErr := model.DigestFromBytes(requestRaw)
	outputNext, outputNextErr := parseCanonicalStoreTime(outputNextText)
	outputLease, outputLeaseErr := parseCanonicalStoreTime(outputLeaseText)
	outputUpdated, outputUpdatedErr := parseCanonicalStoreTime(outputUpdatedText)
	if inboxErr != nil || parsedInbox != inboxID || !validPublicationIdentifier(oldOwner) ||
		oldAttempt < 1 || uint64(oldAttempt) > math.MaxUint32 || len(nonceRaw) != 32 ||
		oldLeaseErr != nil || requestedErr != nil || !requestedAt.Before(oldLease) ||
		requestErr != nil || outputStatus != string(model.InboxWaitingArtifact) ||
		outputAttempt < 1 || uint64(outputAttempt) > math.MaxUint32 || outputNextErr != nil ||
		!validPublicationIdentifier(outputOwner) || outputLeaseErr != nil ||
		outputUpdatedErr != nil || outputDiagnostic.Valid {
		return peerInboxArtifactRenewReceipt{}, false, fmt.Errorf("%w: malformed renew receipt",
			ErrPeerInboxArtifactInvariant)
	}
	var semanticNonce [32]byte
	copy(semanticNonce[:], nonceRaw)
	output := peerInboxArtifactResultProjection{inboxID: parsedInbox,
		semanticNonce: semanticNonce, status: model.InboxWaitingArtifact,
		attempt: uint32(outputAttempt), nextAttemptAt: outputNext, leaseOwner: outputOwner,
		leaseUntil: outputLease, hasLease: true, updatedAt: outputUpdated}
	fence := PeerInboxArtifactFence{inboxID: parsedInbox, leaseOwner: oldOwner,
		leaseUntil: oldLease, attempt: uint32(oldAttempt)}
	validated, err := newPeerInboxArtifactRenewReceipt(fence, semanticNonce, requestedAt, output)
	if err != nil || validated.requestDigest != requestDigest {
		return peerInboxArtifactRenewReceipt{}, false, fmt.Errorf("%w: renew receipt digest or output: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	return validated, true, nil
}

func readValidatedPeerInboxArtifactRenewReceipt(ctx context.Context, tx *sql.Tx,
	row peerInboxArtifactRow,
) (peerInboxArtifactRenewReceipt, bool, error) {
	receipt, found, err := readPeerInboxArtifactRenewReceipt(ctx, tx, row.inboxID)
	if err != nil || !found {
		return receipt, found, err
	}
	if !receipt.output.equal(peerInboxArtifactResultFromRow(row)) {
		return peerInboxArtifactRenewReceipt{}, false, fmt.Errorf(
			"%w: renew receipt output no longer matches Inbox", ErrPeerInboxArtifactInvariant)
	}
	return receipt, true, nil
}

func upsertPeerInboxArtifactRenewReceipt(ctx context.Context, tx *sql.Tx,
	receipt peerInboxArtifactRenewReceipt,
) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO peer_inbox_artifact_renew_receipts(
		inbox_id,old_lease_owner,old_lease_until,old_attempt,semantic_nonce,requested_at,
		request_digest,output_status,output_attempt,output_next_attempt_at,
		output_lease_owner,output_lease_until,output_diagnostic,output_updated_at
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
	ON CONFLICT(inbox_id) DO UPDATE SET
		old_lease_owner=excluded.old_lease_owner,old_lease_until=excluded.old_lease_until,
		old_attempt=excluded.old_attempt,semantic_nonce=excluded.semantic_nonce,
		requested_at=excluded.requested_at,request_digest=excluded.request_digest,
		output_status=excluded.output_status,output_attempt=excluded.output_attempt,
		output_next_attempt_at=excluded.output_next_attempt_at,
		output_lease_owner=excluded.output_lease_owner,
		output_lease_until=excluded.output_lease_until,
		output_diagnostic=excluded.output_diagnostic,
		output_updated_at=excluded.output_updated_at`, receipt.inboxID.String(),
		receipt.oldLeaseOwner, storeTime(receipt.oldLeaseUntil), receipt.oldAttempt,
		receipt.semanticNonce[:], storeTime(receipt.requestedAt), receipt.requestDigest.Bytes(),
		string(receipt.output.status), receipt.output.attempt,
		storeTime(receipt.output.nextAttemptAt), receipt.output.leaseOwner,
		storeTime(receipt.output.leaseUntil), nil, storeTime(receipt.output.updatedAt))
	if err != nil {
		return fmt.Errorf("%w: write renew receipt: %v", ErrPeerInboxArtifactInvariant, err)
	}
	return nil
}

func deletePeerInboxArtifactRenewReceipt(ctx context.Context, tx *sql.Tx,
	inboxID model.InboxID, found bool,
) error {
	if !found {
		return nil
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM peer_inbox_artifact_renew_receipts
		WHERE inbox_id=?`, inboxID.String())
	if err != nil {
		return fmt.Errorf("%w: delete renew receipt: %v", ErrPeerInboxArtifactInvariant, err)
	}
	if err := requireExactlyOneRow(result, "delete Peer Inbox Artifact renew receipt"); err != nil {
		return fmt.Errorf("%w: %v", ErrPeerInboxArtifactInvariant, err)
	}
	return nil
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
			return fmt.Errorf("%w: root %s verification observation is newer",
				ErrPeerInboxArtifactStale, rootDigest.String())
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

func requirePromotablePeerInboxArtifactClosures(ctx context.Context, tx *sql.Tx,
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
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: root %s", ErrPeerInboxArtifactNotReady, rootDigest.String())
		}
		if err != nil || state != "staged" && state != "verified" ||
			state == "verified" && root.VerifiedAt.IsZero() {
			return fmt.Errorf("%w: promotable root %s metadata: %v",
				ErrPeerInboxArtifactInvariant, rootDigest.String(), err)
		}
		if root.CreatedAt.After(at) || state == "verified" && root.VerifiedAt.After(at) {
			return fmt.Errorf("%w: promotable root %s observation is newer",
				ErrPeerInboxArtifactStale, rootDigest.String())
		}
		manifest, err := artifactdomain.ParseManifest(root.Manifest.Bytes())
		if err != nil || manifest.RootDigest() != root.RootDigest ||
			manifest.ManifestDigest() != root.ManifestDigest ||
			manifest.TotalBytes() != root.TotalBytes ||
			!bytes.Equal(manifest.CanonicalJSON().Bytes(), root.Manifest.Bytes()) {
			return fmt.Errorf("%w: promotable root %s manifest: %v",
				ErrPeerInboxArtifactInvariant, rootDigest.String(), err)
		}
		expected := artifactSourceRootMap(manifest)
		actual, err := readArtifactRootBlockMap(ctx, tx, rootDigest)
		if err != nil || !equalArtifactRootBlocks(actual, expected) {
			return fmt.Errorf("%w: promotable root %s block map: %v",
				ErrPeerInboxArtifactInvariant, rootDigest.String(), err)
		}
		seenBlocks := make(map[model.Digest]uint64)
		for _, mapping := range expected {
			if size, seen := seenBlocks[mapping.BlockDigest]; seen {
				if size != mapping.LengthBytes {
					return fmt.Errorf("%w: promotable root %s block size differs",
						ErrPeerInboxArtifactInvariant, rootDigest.String())
				}
				continue
			}
			var size uint64
			var createdText string
			err := tx.QueryRowContext(ctx, `SELECT size_bytes,created_at FROM artifact_blocks
				WHERE block_digest=?`, mapping.BlockDigest.String()).Scan(&size, &createdText)
			createdAt, timeErr := parseCanonicalStoreTime(createdText)
			if err != nil || timeErr != nil || size != mapping.LengthBytes ||
				size == 0 || size > maxVerifiedClosureBlockBytes {
				return fmt.Errorf("%w: promotable root %s block metadata: %v / %v",
					ErrPeerInboxArtifactInvariant, rootDigest.String(), err, timeErr)
			}
			if createdAt.After(at) {
				return fmt.Errorf("%w: promotable root %s block observation is newer",
					ErrPeerInboxArtifactStale, rootDigest.String())
			}
			seenBlocks[mapping.BlockDigest] = size
		}
		if root.TotalBytes > maxVerifiedClosureBytes ||
			aggregateBytes > maxVerifiedClosureBytes-root.TotalBytes {
			return fmt.Errorf("%w: required closure byte bound exceeded",
				ErrPeerInboxArtifactLimit)
		}
		aggregateBytes += root.TotalBytes
		aggregateEntries += len(manifest.Entries())
		if aggregateEntries > maxVerifiedClosureEntries {
			return fmt.Errorf("%w: required closure entry bound exceeded",
				ErrPeerInboxArtifactLimit)
		}
	}
	return nil
}

func equalPeerInboxArtifactClosureRoots(required []model.Digest,
	roots []VerifiedArtifactRoot,
) bool {
	if len(required) != len(roots) {
		return false
	}
	for index := range required {
		if required[index] != roots[index].RootDigest {
			return false
		}
	}
	return true
}

type peerInboxArtifactPin struct {
	root      model.Digest
	expiresAt time.Time
	hasExpiry bool
	createdAt time.Time
}

func readPeerInboxArtifactPins(ctx context.Context, tx *sql.Tx,
	inboxID model.InboxID,
) ([]peerInboxArtifactPin, error) {
	rows, err := tx.QueryContext(ctx, `SELECT root_digest,expires_at,created_at FROM artifact_pins
		WHERE owner_kind='inbox' AND owner_id=? ORDER BY root_digest`, inboxID.String())
	if err != nil {
		return nil, fmt.Errorf("%w: read Inbox pins: %v", ErrPeerInboxArtifactInvariant, err)
	}
	defer rows.Close()
	result := make([]peerInboxArtifactPin, 0)
	for rows.Next() {
		var rootText, createdText string
		var expires sql.NullString
		if err := rows.Scan(&rootText, &expires, &createdText); err != nil {
			return nil, fmt.Errorf("%w: scan Inbox pin: %v", ErrPeerInboxArtifactInvariant, err)
		}
		root, rootErr := model.ParseDigest(rootText)
		createdAt, createdErr := parseCanonicalStoreTime(createdText)
		var expiresAt time.Time
		var expiresErr error
		if expires.Valid {
			expiresAt, expiresErr = parseCanonicalStoreTime(expires.String)
		}
		if rootErr != nil || createdErr != nil || expiresErr != nil ||
			expires.Valid && !expiresAt.After(createdAt) {
			return nil, fmt.Errorf("%w: invalid Inbox pin", ErrPeerInboxArtifactInvariant)
		}
		result = append(result, peerInboxArtifactPin{root: root, expiresAt: expiresAt,
			hasExpiry: expires.Valid, createdAt: createdAt})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate Inbox pins: %v", ErrPeerInboxArtifactInvariant, err)
	}
	return result, nil
}

func requireExactPeerInboxArtifactPinRoots(pins []peerInboxArtifactPin,
	roots []model.Digest, at time.Time,
) error {
	if len(pins) != len(roots) {
		return fmt.Errorf("%w: Inbox pin set differs from required roots",
			ErrPeerInboxArtifactInvariant)
	}
	for index := range roots {
		if pins[index].root != roots[index] || pins[index].createdAt.After(at) {
			return fmt.Errorf("%w: Inbox pin set differs from required roots",
				ErrPeerInboxArtifactInvariant)
		}
	}
	return nil
}

func stagePeerInboxArtifactPins(ctx context.Context, tx *sql.Tx, inboxID model.InboxID,
	roots []model.Digest, at, expiresAt time.Time,
) (bool, error) {
	pins, err := readPeerInboxArtifactPins(ctx, tx, inboxID)
	if err != nil {
		return false, err
	}
	if len(pins) == 0 {
		for _, root := range roots {
			if _, err := tx.ExecContext(ctx, `INSERT INTO artifact_pins(root_digest,owner_kind,
				owner_id,expires_at,created_at) VALUES(?,'inbox',?,?,?)`, root.String(),
				inboxID.String(), storeTime(expiresAt), storeTime(at)); err != nil {
				return false, fmt.Errorf("%w: insert Inbox stage pin: %v",
					ErrPeerInboxArtifactInvariant, err)
			}
		}
		return len(roots) == 0, nil
	}
	if err := requireExactPeerInboxArtifactPinRoots(pins, roots, at); err != nil {
		return false, err
	}
	currentExpiry := pins[0].expiresAt
	for _, pin := range pins {
		if !pin.hasExpiry {
			return false, fmt.Errorf("%w: staged Inbox pin is already permanent",
				ErrPeerInboxArtifactInvariant)
		}
		if pin.expiresAt != currentExpiry {
			return false, fmt.Errorf("%w: staged Inbox pin expiries differ",
				ErrPeerInboxArtifactInvariant)
		}
	}
	for _, root := range roots {
		if err := requireArtifactGCQueueAvailableForRoot(ctx, tx, root); err != nil {
			return false, err
		}
	}
	if !currentExpiry.Before(expiresAt) {
		return true, nil
	}
	result, err := tx.ExecContext(ctx, `UPDATE artifact_pins SET expires_at=?
		WHERE owner_kind='inbox' AND owner_id=? AND expires_at IS NOT NULL`,
		storeTime(expiresAt), inboxID.String())
	if err != nil {
		return false, fmt.Errorf("%w: refresh Inbox stage pins: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	affected, affectedErr := result.RowsAffected()
	if affectedErr != nil || affected != int64(len(roots)) {
		return false, fmt.Errorf("%w: refresh Inbox stage pins cardinality",
			ErrPeerInboxArtifactInvariant)
	}
	return false, nil
}

func refreshExistingPeerInboxArtifactStagePins(ctx context.Context, tx *sql.Tx,
	inboxID model.InboxID, roots []model.Digest, at, expiresAt time.Time,
) (bool, error) {
	pins, err := readPeerInboxArtifactPins(ctx, tx, inboxID)
	if err != nil || len(pins) == 0 {
		return false, err
	}
	if err := requireExactPeerInboxArtifactPinRoots(pins, roots, at); err != nil {
		return false, err
	}
	for _, pin := range pins {
		if !pin.hasExpiry {
			return false, fmt.Errorf("%w: settlement found permanent Inbox pin",
				ErrPeerInboxArtifactInvariant)
		}
	}
	currentExpiry := pins[0].expiresAt
	for _, pin := range pins[1:] {
		if pin.expiresAt != currentExpiry {
			return false, fmt.Errorf("%w: settlement Inbox pin expiries differ",
				ErrPeerInboxArtifactInvariant)
		}
	}
	for _, root := range roots {
		if err := requireArtifactGCQueueAvailableForRoot(ctx, tx, root); err != nil {
			return false, err
		}
	}
	if !currentExpiry.Before(expiresAt) {
		return true, nil
	}
	result, err := tx.ExecContext(ctx, `UPDATE artifact_pins SET expires_at=?
		WHERE owner_kind='inbox' AND owner_id=? AND expires_at IS NOT NULL`,
		storeTime(expiresAt), inboxID.String())
	if err != nil {
		return false, fmt.Errorf("%w: settle Inbox stage pins: %v",
			ErrPeerInboxArtifactInvariant, err)
	}
	affected, affectedErr := result.RowsAffected()
	if affectedErr != nil || affected != int64(len(roots)) {
		return false, fmt.Errorf("%w: settle Inbox stage pins cardinality",
			ErrPeerInboxArtifactInvariant)
	}
	return true, nil
}

func requirePeerInboxArtifactStagePinsAt(ctx context.Context, tx *sql.Tx,
	inboxID model.InboxID, roots []model.Digest, at, expectedExpiry time.Time,
	allowAbsent bool,
) error {
	pins, err := readPeerInboxArtifactPins(ctx, tx, inboxID)
	if err != nil {
		return err
	}
	if len(pins) == 0 && len(roots) > 0 {
		if allowAbsent {
			return nil
		}
		return fmt.Errorf("%w: required closure has no durable stage pins",
			ErrPeerInboxArtifactNotReady)
	}
	if err := requireExactPeerInboxArtifactPinRoots(pins, roots, at); err != nil {
		return err
	}
	for _, pin := range pins {
		if !pin.hasExpiry {
			return fmt.Errorf("%w: expected expiring Inbox stage pin",
				ErrPeerInboxArtifactInvariant)
		}
		if !expectedExpiry.IsZero() {
			if pin.expiresAt.Before(expectedExpiry) {
				return fmt.Errorf("%w: Inbox stage pin expiry differs",
					ErrPeerInboxArtifactInvariant)
			}
		} else if !pin.expiresAt.After(at) {
			return fmt.Errorf("%w: Inbox stage pin expired", ErrPeerInboxArtifactNotReady)
		}
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

func readPeerInboxArtifactRow(ctx context.Context, tx *sql.Tx,
	inboxID model.InboxID,
) (peerInboxArtifactRow, error) {
	var inboxText, channelText, transportText, originText, epochText, eventText string
	var originSequence, channelSequence uint64
	var eventDigestRaw, originMemberDigestRaw, rosterDigestRaw, publicationDigestRaw []byte
	var originMemberRevision, rosterRevision uint64
	var signature, publicationRaw, rootsRaw, semanticNonceRaw []byte
	var audience int
	var statusText, nextText, receivedText, updatedText string
	var attempts int64
	var leaseOwner, leaseUntil, localEvent, receipt, diagnostic sql.NullString
	var decision []byte
	err := tx.QueryRowContext(ctx, `SELECT inbox_id,channel_id,transport_peer_id,origin_peer_id,
		origin_epoch,origin_seq,channel_seq,event_id,event_digest,origin_member_revision,
		origin_member_record_hash,publication_roster_revision,publication_roster_hash,
			publication_digest,origin_signature,publication_json,is_audience,semantic_nonce,
			required_artifact_roots_json,status,attempts,next_attempt_at,lease_owner,lease_until,
		local_event_id,decision_json,receipt_event_id,diagnostic,received_at,updated_at
		FROM peer_inbox WHERE inbox_id=?`, inboxID.String()).Scan(&inboxText, &channelText,
		&transportText, &originText, &epochText, &originSequence, &channelSequence, &eventText,
		&eventDigestRaw, &originMemberRevision, &originMemberDigestRaw, &rosterRevision,
		&rosterDigestRaw, &publicationDigestRaw, &signature, &publicationRaw, &audience,
		&semanticNonceRaw, &rootsRaw, &statusText, &attempts, &nextText, &leaseOwner, &leaseUntil, &localEvent,
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
		uint64(attempts) > math.MaxUint32 || len(semanticNonceRaw) != 32 || nextErr != nil ||
		receivedErr != nil || updatedErr != nil ||
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
	var semanticNonce [32]byte
	copy(semanticNonce[:], semanticNonceRaw)
	return peerInboxArtifactRow{inboxID: parsedInbox, channelID: channelID,
		originPeerID: originPeer, originEpoch: originEpoch, publication: publication,
		requiredRoots: expectedRoots, status: status, attempts: uint32(attempts),
		nextAttemptAt: nextAttempt, leaseOwner: leaseOwner.String, leaseUntil: parsedLease,
		hasLease: hasLease, diagnostic: diagnostic.String, receivedAt: receivedAt,
		updatedAt: updatedAt, semanticNonce: semanticNonce}, nil
}
