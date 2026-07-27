package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// ReadOperationArtifactPublishSpec identifies one exact durable operation
// stage. The lease fence remains mandatory: a publishing checkpoint is a
// recovery obligation, not a generally readable closure cache.
type ReadOperationArtifactPublishSpec struct {
	Fence OperationArtifactStageFence
	At    time.Time
}

// OperationArtifactPublishCheckpoint is all durable input needed to resume
// publication after PrepareOperationArtifactPublish committed.
type OperationArtifactPublishCheckpoint struct {
	fence   OperationArtifactStageFence
	capture model.JSON
	closure VerifiedArtifactClosure
	state   ArtifactStageState
}

func (checkpoint OperationArtifactPublishCheckpoint) Fence() OperationArtifactStageFence {
	return checkpoint.fence
}

func (checkpoint OperationArtifactPublishCheckpoint) Capture() model.JSON {
	return checkpoint.capture
}

func (checkpoint OperationArtifactPublishCheckpoint) Closure() VerifiedArtifactClosure {
	return cloneVerifiedArtifactClosureValue(checkpoint.closure)
}

func (checkpoint OperationArtifactPublishCheckpoint) State() ArtifactStageState {
	return checkpoint.state
}

type ReadPeerInboxArtifactPublishSpec struct {
	Fence PeerInboxArtifactFence
	Owner artifactdomain.StageOwner
	At    time.Time
}

// PeerInboxArtifactPublishCheckpoint is the authoritative relational closure
// for one exact Inbox owner generation.
type PeerInboxArtifactPublishCheckpoint struct {
	closure VerifiedArtifactClosure
	state   ArtifactStageState
}

func (checkpoint PeerInboxArtifactPublishCheckpoint) Closure() VerifiedArtifactClosure {
	return cloneVerifiedArtifactClosureValue(checkpoint.closure)
}

func (checkpoint PeerInboxArtifactPublishCheckpoint) State() ArtifactStageState {
	return checkpoint.state
}

// ReadOperationArtifactPublish reconstructs a prepared closure from its
// immutable owner projection and canonical Artifact metadata. It never reads
// the workspace or filesystem stage.
func (s *Store) ReadOperationArtifactPublish(ctx context.Context,
	spec ReadOperationArtifactPublishSpec,
) (OperationArtifactPublishCheckpoint, error) {
	at, err := validateOperationArtifactFence(s, ctx, spec.Fence, spec.At)
	if err != nil {
		return OperationArtifactPublishCheckpoint{}, err
	}
	operationID, err := operationIDFromStageOwner(spec.Fence.owner)
	if err != nil {
		return OperationArtifactPublishCheckpoint{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return OperationArtifactPublishCheckpoint{},
			fmt.Errorf("read operation Artifact publish: begin: %w", err)
	}
	defer tx.Rollback()

	operation, err := readOperationByID(ctx, tx, operationID)
	if err != nil {
		return OperationArtifactPublishCheckpoint{}, err
	}
	if err := requireExactOperationArtifactFence(operation, spec.Fence.leaseOwner,
		spec.Fence.leaseUntil, at); err != nil {
		return OperationArtifactPublishCheckpoint{}, err
	}
	stage, found, err := readOperationArtifactStage(ctx, tx, operationID)
	if err != nil || !found {
		return OperationArtifactPublishCheckpoint{}, ErrArtifactStageFence
	}
	if err := requireOperationStageFence(stage, spec.Fence); err != nil {
		return OperationArtifactPublishCheckpoint{}, err
	}
	if stage.state != ArtifactStagePublishing || at.Before(stage.updatedAt) {
		return OperationArtifactPublishCheckpoint{}, ErrArtifactStageFence
	}
	capture, ok := operation.Capture()
	if !ok || capture.IsZero() || model.Sum(capture.Bytes()) != stage.payloadDigest {
		return OperationArtifactPublishCheckpoint{}, ErrArtifactStageConflict
	}
	captured, err := parseOperationCapture(capture)
	if err != nil {
		return OperationArtifactPublishCheckpoint{}, ErrArtifactStageConflict
	}
	closure, err := readOperationArtifactClosureProjection(ctx, tx, operationID)
	if err != nil || !captureMatchesClosure(captured, closure.Roots) {
		if err == nil {
			err = ErrArtifactStageConflict
		}
		return OperationArtifactPublishCheckpoint{}, err
	}
	if err := tx.Commit(); err != nil {
		return OperationArtifactPublishCheckpoint{}, err
	}
	return OperationArtifactPublishCheckpoint{
		fence: spec.Fence, capture: capture, closure: closure, state: stage.state,
	}, nil
}

type ReadCommittedOperationArtifactPublishSpec struct {
	OperationID model.OperationID
	At          time.Time
}

// ReadCommittedOperationArtifactPublish discovers the exact filesystem
// publication obligation only after the operation commit durably owns it.
func (s *Store) ReadCommittedOperationArtifactPublish(ctx context.Context,
	spec ReadCommittedOperationArtifactPublishSpec,
) (OperationArtifactPublishCheckpoint, bool, error) {
	at, err := validateCommittedOperationArtifactPublishCall(s, ctx, spec)
	if err != nil {
		return OperationArtifactPublishCheckpoint{}, false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return OperationArtifactPublishCheckpoint{}, false,
			fmt.Errorf("read committed operation Artifact publish: begin: %w", err)
	}
	defer tx.Rollback()
	operation, capture, captured, err := readCommittedOperationArtifactCapture(
		ctx, tx, spec.OperationID)
	if err != nil {
		return OperationArtifactPublishCheckpoint{}, false, err
	}
	if len(captured) == 0 {
		if err := tx.Commit(); err != nil {
			return OperationArtifactPublishCheckpoint{}, false, err
		}
		return OperationArtifactPublishCheckpoint{}, false, nil
	}
	stage, err := readCommittedOperationArtifactPublishStage(
		ctx, tx, spec.OperationID, capture, at)
	if err != nil {
		return OperationArtifactPublishCheckpoint{}, false, err
	}
	closure, err := readOperationArtifactClosureProjection(ctx, tx, spec.OperationID)
	if err != nil || !captureMatchesClosure(captured, closure.Roots) {
		if err == nil {
			err = ErrArtifactStageConflict
		}
		return OperationArtifactPublishCheckpoint{}, false, err
	}
	if err := requireCommittedArtifactClosure(ctx, tx, operation, stage,
		closure, at); err != nil {
		return OperationArtifactPublishCheckpoint{}, false, err
	}
	owner, err := artifactdomain.NewOperationStageOwner(spec.OperationID, stage.generation)
	if err != nil {
		return OperationArtifactPublishCheckpoint{}, false, ErrArtifactStageConflict
	}
	if err := tx.Commit(); err != nil {
		return OperationArtifactPublishCheckpoint{}, false, err
	}
	return OperationArtifactPublishCheckpoint{
		fence: OperationArtifactStageFence{owner: owner, leaseOwner: stage.leaseOwner,
			leaseUntil: stage.leaseUntil},
		capture: capture, closure: closure, state: stage.state,
	}, true, nil
}

func validateCommittedOperationArtifactPublishCall(s *Store, ctx context.Context,
	spec ReadCommittedOperationArtifactPublishSpec,
) (time.Time, error) {
	if s == nil || s.db == nil || ctx == nil || spec.OperationID.IsZero() {
		return time.Time{}, ErrArtifactStageFence
	}
	at, err := canonicalStoreTime(spec.At)
	if err != nil || at != spec.At {
		return time.Time{}, ErrArtifactStageFence
	}
	return at, nil
}

func requireCommittedArtifactClosure(ctx context.Context, tx *sql.Tx,
	operation model.Operation, stage durableArtifactStage,
	closure VerifiedArtifactClosure, at time.Time,
) error {
	roots := make([]model.Digest, len(closure.Roots))
	for _, root := range closure.Roots {
		value, state, err := readArtifactRoot(ctx, tx, root.RootDigest)
		if err != nil || value.ManifestDigest != root.ManifestDigest ||
			(state != "staged" && state != "verified") {
			return ErrArtifactStageConflict
		}
	}
	for index := range closure.Roots {
		roots[index] = closure.Roots[index].RootDigest
	}
	if stage.state == ArtifactStageReady {
		if err := requireReadyArtifactRoots(ctx, tx, roots, at); err != nil {
			return err
		}
	} else if stage.state == ArtifactStagePublishing {
		if err := requireAcceptedPublishingArtifactRoots(ctx, tx, roots, at); err != nil {
			return err
		}
	} else {
		return ErrArtifactStageConflict
	}
	for _, root := range roots {
		var provenanceCount, pinnedCount int
		if err := tx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM artifact_provenance
				WHERE root_digest=? AND operation_id=? AND relation='local_capture'),
			(SELECT COUNT(*) FROM artifact_provenance provenance
				JOIN artifact_pins pin ON pin.root_digest=provenance.root_digest
					AND pin.owner_kind='event'
					AND pin.owner_id=provenance.producer_event_id
					AND pin.expires_at IS NULL
				WHERE provenance.root_digest=? AND provenance.operation_id=?
					AND provenance.relation='local_capture')`,
			root.String(), operation.ID().String(),
			root.String(), operation.ID().String()).Scan(
			&provenanceCount, &pinnedCount); err != nil ||
			provenanceCount == 0 || pinnedCount != provenanceCount {
			return ErrArtifactStageConflict
		}
	}
	return nil
}

// ReadPeerInboxArtifactPublish reconstructs only one exact publishing owner
// or its ready replay. Before acceptance, publishing remains fenced by the
// live Inbox lease and Channel authority. Once exact permanent pins establish
// acceptance, recovery uses that durable proof and no later Channel or lease
// state can revoke the filesystem publication obligation.
func (s *Store) ReadPeerInboxArtifactPublish(ctx context.Context,
	spec ReadPeerInboxArtifactPublishSpec,
) (PeerInboxArtifactPublishCheckpoint, error) {
	at, err := validatePeerInboxArtifactSettlementCall(s, ctx, spec.Fence, spec.At)
	if err != nil {
		return PeerInboxArtifactPublishCheckpoint{}, err
	}
	if err := validatePeerInboxArtifactPublishOwner(spec); err != nil {
		return PeerInboxArtifactPublishCheckpoint{}, ErrArtifactStageFence
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PeerInboxArtifactPublishCheckpoint{},
			fmt.Errorf("read Peer Inbox Artifact publish: begin: %w", err)
	}
	defer tx.Rollback()

	row, err := readPeerInboxArtifactRow(ctx, tx, spec.Fence.inboxID)
	if err != nil {
		return PeerInboxArtifactPublishCheckpoint{}, err
	}
	stage, err := readExactPeerInboxArtifactPublishStage(ctx, tx, row, spec, at)
	if err != nil {
		return PeerInboxArtifactPublishCheckpoint{}, ErrArtifactStageFence
	}
	if err := requirePeerInboxArtifactPublishState(ctx, tx, row, stage,
		spec.Fence, spec.Owner, at); err != nil {
		return PeerInboxArtifactPublishCheckpoint{}, err
	}

	closure, err := readPeerInboxArtifactPublishClosure(ctx, tx, row, stage)
	if err != nil {
		return PeerInboxArtifactPublishCheckpoint{}, err
	}
	if err := tx.Commit(); err != nil {
		return PeerInboxArtifactPublishCheckpoint{}, err
	}
	return PeerInboxArtifactPublishCheckpoint{closure: closure, state: stage.state}, nil
}

func validatePeerInboxArtifactPublishOwner(spec ReadPeerInboxArtifactPublishSpec) error {
	if spec.Owner.IsZero() || spec.Owner.Kind() != artifactdomain.StageOwnerInbox ||
		spec.Owner.CanonicalID() != spec.Fence.inboxID.String() {
		return ErrArtifactStageFence
	}
	return nil
}

func readExactPeerInboxArtifactPublishStage(ctx context.Context, tx *sql.Tx,
	row peerInboxArtifactRow, spec ReadPeerInboxArtifactPublishSpec, at time.Time,
) (durableArtifactStage, error) {
	stage, found, err := readPeerInboxArtifactStage(ctx, tx, row.inboxID)
	if err != nil || !found || stage.generation != spec.Owner.Generation() ||
		stage.attempt != spec.Fence.attempt ||
		stage.leaseOwner != spec.Fence.leaseOwner ||
		!stage.leaseUntil.Equal(spec.Fence.leaseUntil) ||
		stage.semanticNonce != row.semanticNonce || at.Before(stage.updatedAt) {
		return durableArtifactStage{}, ErrArtifactStageFence
	}
	return stage, nil
}

func requirePeerInboxArtifactPublishState(ctx context.Context, tx *sql.Tx,
	row peerInboxArtifactRow, stage durableArtifactStage,
	fence PeerInboxArtifactFence, owner artifactdomain.StageOwner, at time.Time,
) error {
	switch stage.state {
	case ArtifactStagePublishing:
		if stage.cleanupClaimed {
			return ErrArtifactStageFence
		}
		accepted, err := peerInboxArtifactPublishAccepted(ctx, tx, row, stage, at)
		if err != nil {
			return err
		}
		if accepted {
			return nil
		}
		return requirePublishingPeerInboxArtifactAuthority(ctx, tx, row, fence, at)
	case ArtifactStageReady:
		return requireReadyPeerInboxArtifactProof(ctx, tx, row, stage,
			fence, owner, at)
	default:
		return ErrArtifactStageFence
	}
}

func requirePublishingPeerInboxArtifactAuthority(ctx context.Context, tx *sql.Tx,
	row peerInboxArtifactRow, fence PeerInboxArtifactFence, at time.Time,
) error {
	if _, _, err := readValidatedPeerInboxArtifactRenewReceipt(ctx, tx, row); err != nil {
		return err
	}
	if err := requireLivePeerInboxArtifactFence(row, fence, at); err != nil {
		return err
	}
	return requirePeerInboxArtifactAuthority(ctx, tx, row, at)
}

func readPeerInboxArtifactPublishClosure(ctx context.Context, tx *sql.Tx,
	row peerInboxArtifactRow, stage durableArtifactStage,
) (VerifiedArtifactClosure, error) {
	closure, err := readPeerInboxArtifactClosureProjection(ctx, tx, row.inboxID)
	if err != nil || !equalPeerInboxArtifactClosureRoots(row.requiredRoots, closure.Roots) {
		if err == nil {
			err = ErrArtifactStageConflict
		}
		return VerifiedArtifactClosure{}, err
	}
	digest, err := verifiedClosureDigest(closure)
	if err != nil || digest != stage.payloadDigest {
		return VerifiedArtifactClosure{}, ErrArtifactStageConflict
	}
	return closure, nil
}
