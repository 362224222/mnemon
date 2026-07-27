package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type acceptanceArtifactValidation struct {
	captured           map[model.Digest]model.Digest
	produced           map[model.Digest]struct{}
	referenced         map[model.Digest]struct{}
	roles              map[model.Digest]model.ArtifactRole
	operation          model.Operation
	operationAuthority bool
}

func validateAcceptanceArtifacts(ctx context.Context, tx *sql.Tx, operation model.Operation,
	spec LocalAcceptanceSpec, events []model.Event, trustedNow time.Time,
) (acceptanceArtifactAuthority, error) {
	capture, err := prepareAcceptanceCapture(ctx, tx, operation, spec.Operation != nil, trustedNow)
	if err != nil {
		return acceptanceArtifactAuthority{}, err
	}
	validation := acceptanceArtifactValidation{
		captured:           captureRootSet(capture),
		produced:           make(map[model.Digest]struct{}),
		referenced:         make(map[model.Digest]struct{}),
		roles:              make(map[model.Digest]model.ArtifactRole),
		operation:          operation,
		operationAuthority: spec.Operation != nil,
	}
	for _, event := range events {
		if err := validation.validateEvent(ctx, tx, event); err != nil {
			return acceptanceArtifactAuthority{}, err
		}
	}
	if err := validation.requireExactProducedClosure(); err != nil {
		return acceptanceArtifactAuthority{}, err
	}
	if err := validation.requireReferenceAuthority(spec.AuthorizedReferences); err != nil {
		return acceptanceArtifactAuthority{}, err
	}
	return newAcceptanceArtifactAuthority(operation, capture), nil
}

func prepareAcceptanceCapture(ctx context.Context, tx *sql.Tx, operation model.Operation,
	hasOperation bool, trustedNow time.Time,
) ([]captureRoot, error) {
	var capture []captureRoot
	if value, ok := operation.Capture(); ok {
		var err error
		capture, err = parseOperationCapture(value)
		if err != nil {
			return nil, err
		}
	}
	if hasOperation {
		if err := requireOperationArtifactProjection(ctx, tx, operation.ID(), capture); err != nil {
			return nil, err
		}
		if err := acceptCapturedOperationStage(ctx, tx, operation, capture, trustedNow); err != nil {
			return nil, err
		}
	}
	if err := requireAcceptedCaptureRoots(ctx, tx, capture); err != nil {
		return nil, err
	}
	return capture, nil
}

func acceptCapturedOperationStage(ctx context.Context, tx *sql.Tx, operation model.Operation,
	capture []captureRoot, trustedNow time.Time,
) error {
	if len(capture) == 0 {
		return nil
	}
	value, ok := operation.Capture()
	if !ok {
		return ErrArtifactStageConflict
	}
	return acceptOperationArtifactStage(ctx, tx, operation, value, trustedNow)
}

func requireAcceptedCaptureRoots(ctx context.Context, tx *sql.Tx, capture []captureRoot) error {
	for _, root := range capture {
		value, state, err := readArtifactRoot(ctx, tx, root.RootDigest)
		if err != nil || value.ManifestDigest != root.ManifestDigest ||
			(state != "staged" && state != "verified") {
			return fmt.Errorf("%w: checkpoint root or manifest is not accepted", ErrCaptureMismatch)
		}
	}
	return nil
}

func (validation *acceptanceArtifactValidation) validateEvent(ctx context.Context, tx *sql.Tx,
	event model.Event,
) error {
	if !event.Type().AllowsArtifacts() && len(event.Artifacts()) != 0 {
		return fmt.Errorf("%w: %s forbids Artifact roots", ErrArtifactReference, event.Type())
	}
	eventProduced := make(map[model.Digest]struct{})
	for _, ref := range event.Artifacts() {
		if err := validation.recordReference(ctx, tx, ref, eventProduced); err != nil {
			return err
		}
	}
	if err := validation.requireExactEventCapture(eventProduced); err != nil {
		return err
	}
	if event.Type() == model.EventReviewDelivered {
		return requireExactDeliveredArtifactClosure(ctx, tx, event)
	}
	return nil
}

func (validation *acceptanceArtifactValidation) recordReference(ctx context.Context, tx *sql.Tx,
	ref model.ArtifactRef, eventProduced map[model.Digest]struct{},
) error {
	if prior, exists := validation.roles[ref.RootDigest()]; exists && prior != ref.Role() {
		return fmt.Errorf("%w: root has mixed roles in one batch", ErrCaptureMismatch)
	}
	validation.roles[ref.RootDigest()] = ref.Role()
	switch ref.Role() {
	case model.ArtifactProduced:
		return validation.recordProduced(ref.RootDigest(), eventProduced)
	case model.ArtifactReferenced:
		return validation.recordReusableReference(ctx, tx, ref.RootDigest())
	default:
		return ErrArtifactReference
	}
}

func (validation *acceptanceArtifactValidation) recordProduced(root model.Digest,
	eventProduced map[model.Digest]struct{},
) error {
	if !validation.operationAuthority || validation.operation.AgentRunID().IsZero() {
		return fmt.Errorf("%w: produced root lacks operation Run authority", ErrCaptureMismatch)
	}
	if _, ok := validation.captured[root]; !ok {
		return fmt.Errorf("%w: produced root is absent from capture checkpoint", ErrCaptureMismatch)
	}
	validation.produced[root] = struct{}{}
	eventProduced[root] = struct{}{}
	return nil
}

func (validation *acceptanceArtifactValidation) recordReusableReference(ctx context.Context,
	tx *sql.Tx, root model.Digest,
) error {
	if _, ok := validation.captured[root]; ok {
		return fmt.Errorf("%w: checkpoint root cannot be declared referenced", ErrCaptureMismatch)
	}
	if err := requireReusableArtifactRoot(ctx, tx, root); err != nil {
		return err
	}
	validation.referenced[root] = struct{}{}
	return nil
}

func (validation *acceptanceArtifactValidation) requireExactEventCapture(
	eventProduced map[model.Digest]struct{},
) error {
	if len(eventProduced) != len(validation.captured) {
		return fmt.Errorf("%w: each expanded Event must cover the exact capture closure", ErrCaptureMismatch)
	}
	for root := range validation.captured {
		if _, ok := eventProduced[root]; !ok {
			return fmt.Errorf("%w: Event omitted a captured root", ErrCaptureMismatch)
		}
	}
	return nil
}

func (validation *acceptanceArtifactValidation) requireExactProducedClosure() error {
	if len(validation.produced) != len(validation.captured) {
		return fmt.Errorf("%w: produced roots are not the exact capture closure", ErrCaptureMismatch)
	}
	for root := range validation.captured {
		if _, ok := validation.produced[root]; !ok {
			return fmt.Errorf("%w: capture root omitted from Events", ErrCaptureMismatch)
		}
	}
	return nil
}

func (validation *acceptanceArtifactValidation) requireReferenceAuthority(
	authorized []model.Digest,
) error {
	wantReferences := sortedDigests(validation.referenced)
	if len(authorized) != len(wantReferences) {
		return fmt.Errorf("%w: referenced roots lack exact server authority", ErrArtifactReference)
	}
	for index, root := range authorized {
		if root.IsZero() || root != wantReferences[index] ||
			(index > 0 && authorized[index-1].String() >= root.String()) {
			return fmt.Errorf("%w: reference authority must be uniquely sorted and exact", ErrArtifactReference)
		}
	}
	return nil
}

// requireExactDeliveredArtifactClosure prevents a home controller from
// substituting an unrelated reusable root. A delivered Event is the semantic
// receipt for one exact imported delivery.ready Event, so it must reuse that
// source closure byte-for-byte by digest and downgrade every relation to
// referenced rather than claiming producer provenance.
func requireExactDeliveredArtifactClosure(ctx context.Context, tx *sql.Tx, event model.Event) error {
	causes := event.CausedBy()
	if len(causes) != 1 {
		return fmt.Errorf("%w: delivered Event requires exactly one source request", ErrArtifactReference)
	}
	cause := causes[0]
	var sourceType string
	var artifactJSON []byte
	if err := tx.QueryRowContext(ctx, `SELECT event_type,artifact_roots_json FROM events WHERE event_id=?
		AND origin_peer_id=? AND origin_epoch=?`, cause.EventID().String(), cause.OriginPeerID().String(),
		cause.OriginEpoch().String()).Scan(&sourceType, &artifactJSON); err != nil {
		return fmt.Errorf("%w: read delivered source closure: %v", ErrArtifactReference, err)
	}
	if model.EventType(sourceType) != model.EventReviewDeliveryReady {
		return fmt.Errorf("%w: delivered source is not review.delivery.ready", ErrArtifactReference)
	}
	sourceRefs, err := parseCanonicalDerivationArtifactRefs(artifactJSON)
	if err != nil {
		return fmt.Errorf("%w: delivered source closure is not canonical", ErrArtifactReference)
	}
	deliveredRefs := event.Artifacts()
	if len(sourceRefs) != len(deliveredRefs) {
		return fmt.Errorf("%w: delivered closure differs from source request", ErrArtifactReference)
	}
	for index, source := range sourceRefs {
		if deliveredRefs[index].Role() != model.ArtifactReferenced ||
			deliveredRefs[index].RootDigest() != source.RootDigest() {
			return fmt.Errorf("%w: delivered closure differs from source request", ErrArtifactReference)
		}
	}
	return nil
}
