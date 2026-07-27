package agent

import (
	"context"
	"errors"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

// ArtifactCaptureCheckpointer is the produced-byte boundary used by the role
// resolver. Its concrete implementation owns all live capture and durable
// operation replay behavior.
type ArtifactCaptureCheckpointer interface {
	Checkpoint(context.Context, store.ManagedOperationReservation, []string) (
		ArtifactCaptureResult, *ControlError)
	PublishAccepted(context.Context, model.OperationID) *ControlError
}

func (resolver *ArtifactResolver) PublishAccepted(ctx context.Context,
	operation model.OperationID,
) *ControlError {
	if resolver == nil || resolver.capture == nil || ctx == nil ||
		operation.IsZero() {
		return captureAPIError(CodeInternal,
			"Artifact resolver is unavailable", operation)
	}
	return resolver.capture.PublishAccepted(ctx, operation)
}

// ArtifactViewValidator checks only the exact receipt-bound filesystem view.
// It must not derive authority from the materialized bytes; durable root,
// provenance, and pin checks remain in the Store acceptance transaction.
type ArtifactViewValidator interface {
	Validate(context.Context, model.CurrentReadReceipt, model.CurrentArtifactRef) error
}

type ArtifactResolver struct {
	capture ArtifactCaptureCheckpointer
	views   ArtifactViewValidator
}

func NewArtifactResolver(capture ArtifactCaptureCheckpointer,
	views ArtifactViewValidator,
) (*ArtifactResolver, error) {
	if capture == nil || views == nil {
		return nil, errors.New("Artifact resolver requires capture coordinator and view validator")
	}
	return &ArtifactResolver{capture: capture, views: views}, nil
}

func (resolver *ArtifactResolver) Coordinate(ctx context.Context,
	spec ArtifactCoordinationSpec,
) (ArtifactCoordinationResult, *ControlError) {
	operation := spec.Reservation.Operation
	if resolver == nil || resolver.capture == nil || resolver.views == nil || ctx == nil || operation.ID().IsZero() {
		return ArtifactCoordinationResult{}, captureAPIError(CodeInternal,
			"Artifact resolver input is invalid", operation.ID())
	}
	if !spec.Handler.ready || spec.Handler.OperationKind() != operation.Kind() {
		return ArtifactCoordinationResult{}, captureAPIError(CodeOperationMismatch,
			"Artifact handler differs from reserved operation", operation.ID())
	}
	requestedPaths, pathErr := validateArtifactPaths(spec.Paths,
		spec.Handler.Descriptor().Artifacts())
	if pathErr != nil {
		return ArtifactCoordinationResult{}, captureAPIError(pathErr.Code,
			pathErr.Message, operation.ID())
	}

	viewAuthority := make(map[string]model.CurrentArtifactRef)
	if !spec.HasCurrent && !spec.Current.RunID().IsZero() {
		return ArtifactCoordinationResult{}, captureAPIError(CodeContextStale,
			"unexpected managed current Artifact authority", operation.ID())
	}
	if spec.HasCurrent {
		if spec.Current.RunID() != operation.AgentRunID() ||
			!spec.Current.Projection().HasMaterializedArtifactViews() {
			return ArtifactCoordinationResult{}, captureAPIError(CodeContextStale,
				"managed current Artifact authority is stale", operation.ID())
		}
		for _, ref := range spec.Current.ArtifactRefs() {
			path, ok := ref.ViewPath()
			if !ok {
				return ArtifactCoordinationResult{}, captureAPIError(CodeContextStale,
					"managed current Artifact authority is incomplete", operation.ID())
			}
			if _, duplicate := viewAuthority[path]; duplicate {
				return ArtifactCoordinationResult{}, captureAPIError(CodeContextStale,
					"managed current Artifact paths are ambiguous", operation.ID())
			}
			viewAuthority[path] = ref
		}
	}

	producedPaths := make([]string, 0, len(requestedPaths))
	referenced := make([]model.ArtifactRef, 0, len(requestedPaths))
	for _, requested := range requestedPaths {
		normalized, internal, err := normalizeProducedArtifactPath(requested)
		if err == nil {
			if currentRef, ok := viewAuthority[normalized]; ok {
				if err := resolver.views.Validate(ctx, spec.Current, currentRef); err != nil {
					return ArtifactCoordinationResult{}, captureAPIError(CodeArtifactInvalid,
						"readonly Artifact view is unavailable or changed", operation.ID())
				}
				ref, err := model.NewArtifactRef(currentRef.RootDigest(), model.ArtifactReferenced)
				if err != nil {
					return ArtifactCoordinationResult{}, captureAPIError(CodeInternal,
						"current Artifact root is invalid", operation.ID())
				}
				referenced = append(referenced, ref)
				continue
			}
		}
		if internal {
			return ArtifactCoordinationResult{}, captureAPIError(CodeArtifactInvalid,
				"readonly Artifact path is not bound by this current receipt", operation.ID())
		}
		if err != nil {
			return ArtifactCoordinationResult{}, captureAPIError(CodeArtifactInvalid,
				"Artifact path is outside produced workspace scope", operation.ID())
		}
		producedPaths = append(producedPaths, normalized)
	}

	captured, apiErr := resolver.capture.Checkpoint(ctx, spec.Reservation, producedPaths)
	if apiErr != nil {
		return ArtifactCoordinationResult{}, apiErr
	}
	checkpointRoots, err := parseArtifactCaptureCheckpoint(captured.Checkpoint)
	if err != nil || !sameArtifactCaptureRoots(checkpointRoots, captured.Roots) ||
		len(captured.Roots) != len(producedPaths) {
		return ArtifactCoordinationResult{}, captureAPIError(CodeInternal,
			"Artifact capture result differs from durable checkpoint", operation.ID())
	}

	refs := make([]model.ArtifactRef, 0, len(captured.Roots)+len(referenced))
	for _, root := range captured.Roots {
		ref, err := model.NewArtifactRef(root.RootDigest, model.ArtifactProduced)
		if err != nil {
			return ArtifactCoordinationResult{}, captureAPIError(CodeInternal,
				"Artifact capture returned an invalid root", operation.ID())
		}
		refs = append(refs, ref)
	}
	refs = append(refs, referenced...)
	sort.Slice(refs, func(left, right int) bool {
		return refs[left].RootDigest().String() < refs[right].RootDigest().String()
	})
	for index := 1; index < len(refs); index++ {
		if refs[index-1].RootDigest() == refs[index].RootDigest() {
			return ArtifactCoordinationResult{}, captureAPIError(CodeArtifactInvalid,
				"Artifact paths resolve to a duplicate or ambiguous root", operation.ID())
		}
	}
	return ArtifactCoordinationResult{References: refs}, nil
}

func normalizeProducedArtifactPath(value string) (normalized string, internal bool, err error) {
	if value == "" || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 ||
		filepath.IsAbs(value) {
		return "", false, errors.New("path must be relative UTF-8")
	}
	slashed := strings.ReplaceAll(value, `\`, "/")
	if strings.HasPrefix(slashed, "/") {
		return "", false, errors.New("path must be relative")
	}
	components := make([]string, 0)
	for _, component := range strings.Split(slashed, "/") {
		switch component {
		case "":
			return "", false, errors.New("path contains an empty component")
		case "..":
			return "", false, errors.New("path contains traversal")
		case ".":
			continue
		default:
			components = append(components, component)
		}
	}
	// A requested directory may not be an ancestor of Node state: recursively
	// capturing "." or ".mnemon" would otherwise ingest credentials and CAS.
	if len(components) == 0 {
		return "", true, nil
	}
	normalized = strings.Join(components, "/")
	if strings.EqualFold(components[0], ".mnemon") &&
		(len(components) == 1 || strings.EqualFold(components[1], "harness")) {
		return normalized, true, nil
	}
	return normalized, false, nil
}

func sameArtifactCaptureRoots(left, right []ArtifactCaptureRoot) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

var (
	_ ArtifactCaptureCheckpointer = (*ArtifactCaptureCoordinator)(nil)
	_ ArtifactCoordinator         = (*ArtifactResolver)(nil)
)
