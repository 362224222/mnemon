package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

// ArtifactCapturer is the live-workspace side of managed Artifact capture.
// It is intentionally absent from the durable replay path.
type ArtifactCapturer interface {
	Capture(context.Context, []string, artifact.ObjectSink) (artifact.Closure, error)
}

// ArtifactStageOpener grants only owner-scoped staging authority. In
// particular, it cannot write directly to final CAS.
type ArtifactStageOpener interface {
	OpenStage(artifact.StageOwner) (*artifact.Stage, error)
}

// ArtifactCaptureStore is the complete durable authority needed by the
// coordinator. Empty captures retain the direct operation checkpoint; every
// nonempty capture uses the explicit staged publication lifecycle.
type ArtifactCaptureStore interface {
	CheckpointOperationCapture(context.Context, model.OperationID, string, time.Time, model.JSON) (bool, error)
	BeginOperationArtifactStage(context.Context, store.BeginOperationArtifactStageSpec) (
		store.OperationArtifactStageResult, error)
	PrepareOperationArtifactPublish(context.Context, store.PrepareOperationArtifactPublishSpec) (
		store.OperationArtifactStageResult, error)
	ReadOperationArtifactPublish(context.Context, store.ReadOperationArtifactPublishSpec) (
		store.OperationArtifactPublishCheckpoint, error)
	ReadCommittedOperationArtifactPublish(context.Context,
		store.ReadCommittedOperationArtifactPublishSpec) (
		store.OperationArtifactPublishCheckpoint, bool, error)
	MarkOperationArtifactReady(context.Context, store.MarkOperationArtifactReadySpec) (
		store.OperationArtifactStageResult, error)
}

type ArtifactCaptureRoot struct {
	ManifestDigest model.Digest `json:"manifest_digest"`
	RootDigest     model.Digest `json:"root_digest"`
}

type ArtifactCaptureResult struct {
	Checkpoint model.JSON
	Roots      []ArtifactCaptureRoot
	Replayed   bool
}

// ArtifactCaptureCoordinator binds live capture to one reserved managed
// Operation. Once an Operation contains capture_json, Checkpoint never calls
// the live capturer again: the durable checkpoint is the sole replay input.
type ArtifactCaptureCoordinator struct {
	capturer ArtifactCapturer
	stages   ArtifactStageOpener
	store    ArtifactCaptureStore
	clock    ServiceClock
}

func NewArtifactCaptureCoordinator(capturer ArtifactCapturer, stages ArtifactStageOpener,
	st ArtifactCaptureStore, clock ServiceClock,
) (*ArtifactCaptureCoordinator, error) {
	if capturer == nil || stages == nil || st == nil {
		return nil, errors.New("Artifact capture coordinator requires capturer, stages and Store")
	}
	if clock == nil {
		clock = wallServiceClock{}
	}
	return &ArtifactCaptureCoordinator{capturer: capturer, stages: stages, store: st, clock: clock}, nil
}

func (coordinator *ArtifactCaptureCoordinator) Checkpoint(ctx context.Context,
	reservation store.ManagedOperationReservation, paths []string,
) (ArtifactCaptureResult, *ControlError) {
	operation := reservation.Operation
	if coordinator == nil || coordinator.capturer == nil || coordinator.stages == nil ||
		coordinator.store == nil || coordinator.clock == nil || ctx == nil || operation.ID().IsZero() {
		return ArtifactCaptureResult{}, captureAPIError(CodeInternal,
			"Artifact capture coordinator input is invalid", operation.ID())
	}

	if checkpoint, exists := operation.Capture(); exists {
		roots, err := parseArtifactCaptureCheckpoint(checkpoint)
		if err != nil {
			return ArtifactCaptureResult{}, captureAPIError(CodeInternal,
				"durable Artifact checkpoint is invalid", operation.ID())
		}
		if operation.Status().Terminal() {
			return ArtifactCaptureResult{Checkpoint: checkpoint, Roots: roots, Replayed: true}, nil
		}
		if len(roots) == 0 {
			return coordinator.checkpointEmpty(ctx, reservation, checkpoint, true)
		}
		return coordinator.checkpointNonempty(ctx, reservation, paths, true)
	}
	if operation.Status().Terminal() {
		return ArtifactCaptureResult{}, captureAPIError(CodeInternal,
			"terminal Teamwork operation has no Artifact checkpoint", operation.ID())
	}
	if len(paths) > artifact.MaxRoots {
		return ArtifactCaptureResult{}, captureAPIError(CodeArtifactTooLarge,
			"an action accepts at most 16 Artifact paths", operation.ID())
	}
	if len(paths) == 0 {
		checkpoint, err := buildArtifactCaptureCheckpoint(nil)
		if err != nil {
			return ArtifactCaptureResult{}, captureAPIError(CodeInternal,
				"empty Artifact checkpoint cannot be encoded", operation.ID())
		}
		return coordinator.checkpointEmpty(ctx, reservation, checkpoint, false)
	}
	return coordinator.checkpointNonempty(ctx, reservation, paths, false)
}

func (coordinator *ArtifactCaptureCoordinator) checkpointEmpty(ctx context.Context,
	reservation store.ManagedOperationReservation, checkpoint model.JSON, replay bool,
) (ArtifactCaptureResult, *ControlError) {
	operation := reservation.Operation
	now, apiErr := coordinator.fencedNow(reservation)
	if apiErr != nil {
		return ArtifactCaptureResult{}, apiErr
	}
	replayed, err := coordinator.store.CheckpointOperationCapture(ctx, operation.ID(),
		operation.LeaseOwner(), now, checkpoint)
	if err != nil {
		return ArtifactCaptureResult{}, mapArtifactStoreError(err, operation.ID())
	}
	if replay && !replayed {
		return ArtifactCaptureResult{}, captureAPIError(CodeInternal,
			"durable empty Artifact checkpoint replay was not recognized", operation.ID())
	}
	roots, err := parseArtifactCaptureCheckpoint(checkpoint)
	if err != nil {
		return ArtifactCaptureResult{}, captureAPIError(CodeInternal,
			"Artifact checkpoint projection is invalid", operation.ID())
	}
	return ArtifactCaptureResult{Checkpoint: checkpoint, Roots: roots, Replayed: replay || replayed}, nil
}

func (coordinator *ArtifactCaptureCoordinator) fencedNow(
	reservation store.ManagedOperationReservation,
) (time.Time, *ControlError) {
	operation := reservation.Operation
	if operation.Status() != model.OperationStarted || !reservation.Acquired {
		return time.Time{}, captureAPIError(CodeOperationPending,
			"operation capture lease is not acquired", operation.ID())
	}
	now, apiErr := coordinator.freshNow(operation.ID())
	if apiErr != nil {
		return time.Time{}, apiErr
	}
	leaseUntil, hasLease := operation.LeaseUntil()
	if !hasLease || operation.LeaseOwner() == "" || now.Before(operation.CreatedAt()) ||
		!leaseUntil.After(now) {
		return time.Time{}, captureAPIError(CodeOperationPending,
			"operation capture lease expired", operation.ID())
	}
	return now, nil
}

func (coordinator *ArtifactCaptureCoordinator) freshNow(
	operation model.OperationID,
) (time.Time, *ControlError) {
	now, err := canonicalArtifactCaptureTime(coordinator.clock.Now())
	if err != nil {
		return time.Time{}, captureAPIError(CodeInternal,
			"trusted Artifact capture clock is invalid", operation)
	}
	return now, nil
}

func parseArtifactCaptureCheckpoint(checkpoint model.JSON) ([]ArtifactCaptureRoot, error) {
	if checkpoint.IsZero() {
		return nil, errors.New("zero Artifact capture checkpoint")
	}
	var envelope struct {
		Roots json.RawMessage `json:"roots"`
	}
	decoder := json.NewDecoder(bytes.NewReader(checkpoint.Bytes()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil || len(envelope.Roots) == 0 ||
		bytes.Equal(envelope.Roots, []byte("null")) {
		return nil, errors.New("closed Artifact roots array is required")
	}
	if err := requireArtifactJSONEOF(decoder); err != nil {
		return nil, err
	}
	var encoded []struct {
		ManifestDigest string `json:"manifest_digest"`
		RootDigest     string `json:"root_digest"`
	}
	rootsDecoder := json.NewDecoder(bytes.NewReader(envelope.Roots))
	rootsDecoder.DisallowUnknownFields()
	if err := rootsDecoder.Decode(&encoded); err != nil || encoded == nil || len(encoded) > artifact.MaxRoots {
		return nil, errors.New("Artifact roots array is invalid")
	}
	if err := requireArtifactJSONEOF(rootsDecoder); err != nil {
		return nil, err
	}
	roots := make([]ArtifactCaptureRoot, len(encoded))
	for index, row := range encoded {
		manifestDigest, err := model.ParseDigest(row.ManifestDigest)
		if err != nil {
			return nil, fmt.Errorf("Artifact manifest digest %d is invalid", index)
		}
		rootDigest, err := model.ParseDigest(row.RootDigest)
		if err != nil || (index > 0 && roots[index-1].RootDigest.String() >= rootDigest.String()) {
			return nil, fmt.Errorf("Artifact root digest %d is invalid or unordered", index)
		}
		roots[index] = ArtifactCaptureRoot{ManifestDigest: manifestDigest, RootDigest: rootDigest}
	}
	canonical, err := buildArtifactCaptureCheckpoint(roots)
	if err != nil || canonical.String() != checkpoint.String() {
		return nil, errors.New("Artifact capture checkpoint is not exact canonical JSON")
	}
	return roots, nil
}

func buildArtifactCaptureCheckpoint(roots []ArtifactCaptureRoot) (model.JSON, error) {
	rows := append([]ArtifactCaptureRoot(nil), roots...)
	if rows == nil {
		rows = make([]ArtifactCaptureRoot, 0)
	}
	return model.JSONFrom(struct {
		Roots []ArtifactCaptureRoot `json:"roots"`
	}{Roots: rows})
}

func requireArtifactJSONEOF(decoder *json.Decoder) error {
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("unexpected trailing Artifact checkpoint value")
	}
	return nil
}

func canonicalArtifactCaptureTime(value time.Time) (time.Time, error) {
	value = value.Round(0).UTC()
	if value.IsZero() || value.Year() < 1 || value.Year() > 9999 ||
		!time.Unix(0, value.UnixNano()).UTC().Equal(value) {
		return time.Time{}, errors.New("time is outside canonical Unix nanoseconds")
	}
	return value, nil
}

func mapLiveArtifactError(err error, operation model.OperationID) *ControlError {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return captureAPIError(CodeOperationPending, "Artifact capture was interrupted", operation)
	case errors.Is(err, artifact.ErrArtifactLimit):
		return captureAPIError(CodeArtifactTooLarge, "Artifact capture exceeds its bound", operation)
	case errors.Is(err, artifact.ErrArtifactPath), errors.Is(err, artifact.ErrArtifactType),
		errors.Is(err, artifact.ErrArtifactChanged):
		return captureAPIError(CodeArtifactInvalid, "Artifact path cannot be captured", operation)
	case errors.Is(err, artifact.ErrCASCorruption), errors.Is(err, artifact.ErrCASInput),
		errors.Is(err, artifact.ErrInvalidManifest), errors.Is(err, artifact.ErrClosureMismatch):
		return captureAPIError(CodeInternal, "Artifact byte store rejected capture", operation)
	default:
		return captureAPIError(CodeArtifactInvalid, "Artifact path cannot be captured", operation)
	}
}

func mapArtifactVerificationError(err error, operation model.OperationID) *ControlError {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return captureAPIError(CodeOperationPending, "Artifact verification was interrupted", operation)
	}
	return captureAPIError(CodeInternal, "Artifact closure verification failed", operation)
}

func mapArtifactStoreError(err error, operation model.OperationID) *ControlError {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, store.ErrOperationFence), errors.Is(err, store.ErrOperationPending),
		errors.Is(err, store.ErrArtifactStageFence):
		return captureAPIError(CodeOperationPending, "Artifact checkpoint is pending", operation)
	case errors.Is(err, store.ErrOperationMismatch):
		return captureAPIError(CodeOperationMismatch,
			"operation Artifact checkpoint differs from durable state", operation)
	default:
		return captureAPIError(CodeInternal, "durable Artifact checkpoint failed", operation)
	}
}

func captureAPIError(code ControlErrorCode, message string, operation model.OperationID) *ControlError {
	apiErr := NewControlError(code, message)
	if !operation.IsZero() {
		operationID := operation.String()
		apiErr.OperationID = &operationID
	}
	return apiErr
}

var (
	_ ArtifactCapturer     = (*artifact.Capturer)(nil)
	_ ArtifactStageOpener  = (*artifact.CAS)(nil)
	_ ArtifactCaptureStore = (*store.Store)(nil)
)
