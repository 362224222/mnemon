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
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

// ArtifactCapturer is the live-workspace side of managed Artifact capture.
// It is intentionally absent from the durable replay path.
type ArtifactCapturer interface {
	Capture(context.Context, []string) (artifact.Closure, error)
}

// ArtifactClosureVerifier establishes that every manifest and reachable block
// in a newly captured closure still matches immutable CAS bytes.
type ArtifactClosureVerifier interface {
	VerifyClosure(context.Context, artifact.Closure) error
}

// ArtifactCaptureStore is the complete durable authority needed by the
// coordinator. Closure metadata is checkpointed before the lease-fenced
// operation checkpoint makes that exact root set reusable by admission.
type ArtifactCaptureStore interface {
	CheckpointVerifiedArtifactClosure(context.Context, store.VerifiedArtifactClosure) (
		store.VerifiedArtifactClosureCheckpoint, error)
	CheckpointOperationCapture(context.Context, model.OperationID, string, time.Time, model.JSON) (bool, error)
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
// the live capturer or CAS verifier again: the durable checkpoint is the sole
// replay authority.
type ArtifactCaptureCoordinator struct {
	capturer ArtifactCapturer
	verifier ArtifactClosureVerifier
	store    ArtifactCaptureStore
	clock    ServiceClock
}

func NewArtifactCaptureCoordinator(capturer ArtifactCapturer, verifier ArtifactClosureVerifier,
	st ArtifactCaptureStore, clock ServiceClock,
) (*ArtifactCaptureCoordinator, error) {
	if capturer == nil || verifier == nil || st == nil {
		return nil, errors.New("Artifact capture coordinator requires capturer, verifier and Store")
	}
	if clock == nil {
		clock = wallServiceClock{}
	}
	return &ArtifactCaptureCoordinator{capturer: capturer, verifier: verifier, store: st, clock: clock}, nil
}

func (coordinator *ArtifactCaptureCoordinator) Checkpoint(ctx context.Context,
	reservation store.ManagedOperationReservation, paths []string,
) (ArtifactCaptureResult, *localapi.APIError) {
	operation := reservation.Operation
	if coordinator == nil || coordinator.capturer == nil || coordinator.verifier == nil ||
		coordinator.store == nil || coordinator.clock == nil || ctx == nil || operation.ID().IsZero() {
		return ArtifactCaptureResult{}, captureAPIError(localapi.CodeInternal,
			"Artifact capture coordinator input is invalid", operation.ID())
	}

	if checkpoint, exists := operation.Capture(); exists {
		roots, err := parseArtifactCaptureCheckpoint(checkpoint)
		if err != nil {
			return ArtifactCaptureResult{}, captureAPIError(localapi.CodeInternal,
				"durable Artifact checkpoint is invalid", operation.ID())
		}
		if operation.Status().Terminal() {
			return ArtifactCaptureResult{Checkpoint: checkpoint, Roots: roots, Replayed: true}, nil
		}
		now, apiErr := coordinator.fencedNow(reservation)
		if apiErr != nil {
			return ArtifactCaptureResult{}, apiErr
		}
		replayed, err := coordinator.store.CheckpointOperationCapture(ctx, operation.ID(),
			operation.LeaseOwner(), now, checkpoint)
		if err != nil {
			return ArtifactCaptureResult{}, mapArtifactStoreError(err, operation.ID())
		}
		if !replayed {
			return ArtifactCaptureResult{}, captureAPIError(localapi.CodeInternal,
				"durable Artifact checkpoint replay was not recognized", operation.ID())
		}
		return ArtifactCaptureResult{Checkpoint: checkpoint, Roots: roots, Replayed: true}, nil
	}
	if operation.Status().Terminal() {
		return ArtifactCaptureResult{}, captureAPIError(localapi.CodeInternal,
			"terminal Teamwork operation has no Artifact checkpoint", operation.ID())
	}
	if len(paths) > artifact.MaxRoots {
		return ArtifactCaptureResult{}, captureAPIError(localapi.CodeArtifactTooLarge,
			"an action accepts at most 16 Artifact paths", operation.ID())
	}
	if _, apiErr := coordinator.fencedNow(reservation); apiErr != nil {
		return ArtifactCaptureResult{}, apiErr
	}

	var checkpoint model.JSON
	if len(paths) == 0 {
		var err error
		checkpoint, err = buildArtifactCaptureCheckpoint(nil)
		if err != nil {
			return ArtifactCaptureResult{}, captureAPIError(localapi.CodeInternal,
				"empty Artifact checkpoint cannot be encoded", operation.ID())
		}
	} else {
		closure, err := coordinator.capturer.Capture(ctx, append([]string(nil), paths...))
		if err != nil {
			return ArtifactCaptureResult{}, mapLiveArtifactError(err, operation.ID())
		}
		if err := coordinator.verifier.VerifyClosure(ctx, closure); err != nil {
			return ArtifactCaptureResult{}, mapArtifactVerificationError(err, operation.ID())
		}
		verified := storeClosureFromArtifact(closure)
		if _, err := coordinator.store.CheckpointVerifiedArtifactClosure(ctx, verified); err != nil {
			return ArtifactCaptureResult{}, mapArtifactStoreError(err, operation.ID())
		}
		checkpoint = closure.Checkpoint()
	}

	now, apiErr := coordinator.freshNow(operation.ID())
	if apiErr != nil {
		return ArtifactCaptureResult{}, apiErr
	}
	replayed, err := coordinator.store.CheckpointOperationCapture(ctx, operation.ID(),
		operation.LeaseOwner(), now, checkpoint)
	if err != nil {
		return ArtifactCaptureResult{}, mapArtifactStoreError(err, operation.ID())
	}
	roots, err := parseArtifactCaptureCheckpoint(checkpoint)
	if err != nil {
		return ArtifactCaptureResult{}, captureAPIError(localapi.CodeInternal,
			"Artifact checkpoint projection is invalid", operation.ID())
	}
	return ArtifactCaptureResult{Checkpoint: checkpoint, Roots: roots, Replayed: replayed}, nil
}

func (coordinator *ArtifactCaptureCoordinator) fencedNow(
	reservation store.ManagedOperationReservation,
) (time.Time, *localapi.APIError) {
	operation := reservation.Operation
	if operation.Status() != model.OperationStarted || !reservation.Acquired {
		return time.Time{}, captureAPIError(localapi.CodeOperationPending,
			"operation capture lease is not acquired", operation.ID())
	}
	now, apiErr := coordinator.freshNow(operation.ID())
	if apiErr != nil {
		return time.Time{}, apiErr
	}
	leaseUntil, hasLease := operation.LeaseUntil()
	if !hasLease || operation.LeaseOwner() == "" || now.Before(operation.CreatedAt()) ||
		!leaseUntil.After(now) {
		return time.Time{}, captureAPIError(localapi.CodeOperationPending,
			"operation capture lease expired", operation.ID())
	}
	return now, nil
}

func (coordinator *ArtifactCaptureCoordinator) freshNow(
	operation model.OperationID,
) (time.Time, *localapi.APIError) {
	now, err := canonicalArtifactCaptureTime(coordinator.clock.Now())
	if err != nil {
		return time.Time{}, captureAPIError(localapi.CodeInternal,
			"trusted Artifact capture clock is invalid", operation)
	}
	return now, nil
}

func storeClosureFromArtifact(closure artifact.Closure) store.VerifiedArtifactClosure {
	roots := closure.Roots()
	blocks := closure.Blocks()
	blockMap := closure.BlockMap()
	result := store.VerifiedArtifactClosure{
		Roots:      make([]store.VerifiedArtifactRoot, len(roots)),
		Blocks:     make([]store.VerifiedArtifactBlock, len(blocks)),
		RootBlocks: make([]store.VerifiedArtifactRootBlock, len(blockMap)),
	}
	for index, root := range roots {
		result.Roots[index] = store.VerifiedArtifactRoot{RootDigest: root.RootDigest,
			Manifest: root.Manifest, ManifestDigest: root.ManifestDigest, TotalBytes: root.TotalBytes,
			CreatedAt: root.CreatedAt, VerifiedAt: root.VerifiedAt}
	}
	for index, block := range blocks {
		result.Blocks[index] = store.VerifiedArtifactBlock{Digest: block.Digest,
			SizeBytes: block.SizeBytes, CreatedAt: block.CreatedAt}
	}
	for index, row := range blockMap {
		result.RootBlocks[index] = store.VerifiedArtifactRootBlock{RootDigest: row.RootDigest,
			Ordinal: row.Ordinal, LogicalPath: row.LogicalPath, OffsetBytes: row.OffsetBytes,
			LengthBytes: row.LengthBytes, BlockDigest: row.BlockDigest, Mode: row.Mode}
	}
	return result
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

func mapLiveArtifactError(err error, operation model.OperationID) *localapi.APIError {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return captureAPIError(localapi.CodeOperationPending, "Artifact capture was interrupted", operation)
	case errors.Is(err, artifact.ErrArtifactLimit):
		return captureAPIError(localapi.CodeArtifactTooLarge, "Artifact capture exceeds its bound", operation)
	case errors.Is(err, artifact.ErrArtifactPath), errors.Is(err, artifact.ErrArtifactType),
		errors.Is(err, artifact.ErrArtifactChanged):
		return captureAPIError(localapi.CodeArtifactInvalid, "Artifact path cannot be captured", operation)
	case errors.Is(err, artifact.ErrCASCorruption), errors.Is(err, artifact.ErrCASInput),
		errors.Is(err, artifact.ErrInvalidManifest), errors.Is(err, artifact.ErrClosureMismatch):
		return captureAPIError(localapi.CodeInternal, "Artifact byte store rejected capture", operation)
	default:
		return captureAPIError(localapi.CodeArtifactInvalid, "Artifact path cannot be captured", operation)
	}
}

func mapArtifactVerificationError(err error, operation model.OperationID) *localapi.APIError {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return captureAPIError(localapi.CodeOperationPending, "Artifact verification was interrupted", operation)
	}
	return captureAPIError(localapi.CodeInternal, "Artifact closure verification failed", operation)
}

func mapArtifactStoreError(err error, operation model.OperationID) *localapi.APIError {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, store.ErrOperationFence), errors.Is(err, store.ErrOperationPending):
		return captureAPIError(localapi.CodeOperationPending, "Artifact checkpoint is pending", operation)
	case errors.Is(err, store.ErrOperationMismatch):
		return captureAPIError(localapi.CodeOperationMismatch,
			"operation Artifact checkpoint differs from durable state", operation)
	default:
		return captureAPIError(localapi.CodeInternal, "durable Artifact checkpoint failed", operation)
	}
}

func captureAPIError(code localapi.ErrorCode, message string, operation model.OperationID) *localapi.APIError {
	apiErr := localapi.NewAPIError(code, message)
	if !operation.IsZero() {
		operationID := operation.String()
		apiErr.OperationID = &operationID
	}
	return apiErr
}

var (
	_ ArtifactCapturer        = (*artifact.Capturer)(nil)
	_ ArtifactClosureVerifier = (*artifact.CAS)(nil)
	_ ArtifactCaptureStore    = (*store.Store)(nil)
)
