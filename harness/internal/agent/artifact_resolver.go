package agent

import (
	"context"
	"errors"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

// ArtifactCaptureCheckpointer is the produced-byte boundary used by the role
// resolver. Its concrete implementation owns all live capture and durable
// operation replay behavior.
type ArtifactCaptureCheckpointer interface {
	Checkpoint(context.Context, store.ManagedOperationReservation, []string) (
		ArtifactCaptureResult, *localapi.APIError)
}

// ArtifactResolver implements the executor's ArtifactCoordinator for the T0
// produced-only path. Readonly Artifact views cannot be inferred from a path:
// until current carries a durable path/root mapping, every managed internal
// path is rejected rather than silently recaptured as produced bytes.
type ArtifactResolver struct {
	capture ArtifactCaptureCheckpointer
}

func NewArtifactResolver(capture ArtifactCaptureCheckpointer) (*ArtifactResolver, error) {
	if capture == nil {
		return nil, errors.New("Artifact resolver requires capture coordinator")
	}
	return &ArtifactResolver{capture: capture}, nil
}

func (resolver *ArtifactResolver) Coordinate(ctx context.Context,
	spec ArtifactCoordinationSpec,
) (ArtifactCoordinationResult, *localapi.APIError) {
	operation := spec.Reservation.Operation
	if resolver == nil || resolver.capture == nil || ctx == nil || operation.ID().IsZero() {
		return ArtifactCoordinationResult{}, captureAPIError(localapi.CodeInternal,
			"Artifact resolver input is invalid", operation.ID())
	}
	if spec.Action != operation.Kind() {
		return ArtifactCoordinationResult{}, captureAPIError(localapi.CodeOperationMismatch,
			"Artifact action differs from reserved operation", operation.ID())
	}
	allowsArtifacts := spec.Action == model.OperationTeamworkOffer ||
		spec.Action == model.OperationTeamworkDeliver || spec.Action == model.OperationTeamworkRework
	if !allowsArtifacts && len(spec.Paths) != 0 {
		return ArtifactCoordinationResult{}, captureAPIError(localapi.CodeArtifactInvalid,
			"this Teamwork action forbids Artifacts", operation.ID())
	}

	paths := make([]string, len(spec.Paths))
	for index, requested := range spec.Paths {
		normalized, internal, err := normalizeProducedArtifactPath(requested)
		if internal {
			return ArtifactCoordinationResult{}, captureAPIError(localapi.CodeArtifactInvalid,
				"readonly Artifact path has no durable current view mapping", operation.ID())
		}
		if err != nil {
			return ArtifactCoordinationResult{}, captureAPIError(localapi.CodeArtifactInvalid,
				"Artifact path is outside produced workspace scope", operation.ID())
		}
		paths[index] = normalized
	}

	captured, apiErr := resolver.capture.Checkpoint(ctx, spec.Reservation, paths)
	if apiErr != nil {
		return ArtifactCoordinationResult{}, apiErr
	}
	checkpointRoots, err := parseArtifactCaptureCheckpoint(captured.Checkpoint)
	if err != nil || !sameArtifactCaptureRoots(checkpointRoots, captured.Roots) ||
		len(captured.Roots) != len(paths) {
		return ArtifactCoordinationResult{}, captureAPIError(localapi.CodeInternal,
			"Artifact capture result differs from durable checkpoint", operation.ID())
	}

	refs := make([]model.ArtifactRef, len(captured.Roots))
	for index, root := range captured.Roots {
		ref, err := model.NewArtifactRef(root.RootDigest, model.ArtifactProduced)
		if err != nil {
			return ArtifactCoordinationResult{}, captureAPIError(localapi.CodeInternal,
				"Artifact capture returned an invalid root", operation.ID())
		}
		refs[index] = ref
	}
	sort.Slice(refs, func(left, right int) bool {
		return refs[left].RootDigest().String() < refs[right].RootDigest().String()
	})
	for index := 1; index < len(refs); index++ {
		if refs[index-1].RootDigest() == refs[index].RootDigest() {
			return ArtifactCoordinationResult{}, captureAPIError(localapi.CodeInternal,
				"Artifact capture returned duplicate roots", operation.ID())
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
	if len(components) == 0 || strings.EqualFold(components[0], ".mnemon") &&
		(len(components) == 1 || strings.EqualFold(components[1], "harness")) {
		return "", true, nil
	}
	return strings.Join(components, "/"), false, nil
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
