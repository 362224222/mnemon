package agency

import "sort"

// CapturedCandidate is the immutable result of capturing one candidate input
// for this operation. The caller may construct it only after content-addressed
// capture and hash verification; durable admission verifies availability again.
type CapturedCandidate struct {
	operation OperationKey
	input     ArtifactInput
	digest    Digest
}

func NewCapturedCandidate(operation OperationKey, input ArtifactInput, digest Digest) (CapturedCandidate, error) {
	if operation.IsZero() || input.kind != ArtifactInputCandidate || input.handle.IsZero() || digest.IsZero() {
		return CapturedCandidate{}, invalid("captured candidate", "operation, candidate input, and verified digest are required")
	}
	return CapturedCandidate{operation: operation, input: input, digest: digest}, nil
}

func (candidate CapturedCandidate) OperationKey() OperationKey { return candidate.operation }
func (candidate CapturedCandidate) Input() ArtifactInput       { return candidate.input }
func (candidate CapturedCandidate) Digest() Digest             { return candidate.digest }

// ViewArtifactOffer freezes one verified Artifact digest behind one View-only
// handle. It cannot satisfy an Agent-declared candidate input.
type ViewArtifactOffer struct {
	handle OpaqueHandle
	digest Digest
}

func NewViewArtifactOffer(handle OpaqueHandle, digest Digest) (ViewArtifactOffer, error) {
	if handle.IsZero() || digest.IsZero() {
		return ViewArtifactOffer{}, invalid("View Artifact offer", "handle and verified digest are required")
	}
	return ViewArtifactOffer{handle: handle, digest: digest}, nil
}

func (offer ViewArtifactOffer) Handle() OpaqueHandle { return offer.handle }
func (offer ViewArtifactOffer) Digest() Digest       { return offer.digest }

// ResolvedArtifact is machine evidence that one exact Agent Artifact input was
// captured or resolved to one verified content digest.
type ResolvedArtifact struct {
	input  ArtifactInput
	digest Digest
}

func (artifact ResolvedArtifact) Input() ArtifactInput { return artifact.input }
func (artifact ResolvedArtifact) Digest() Digest       { return artifact.digest }

func resolveArtifacts(operation OperationKey, inputs []ArtifactInput, view map[string]ViewArtifactOffer,
	candidates []CapturedCandidate,
) ([]ResolvedArtifact, []Digest, error) {
	captured := make(map[string]CapturedCandidate, len(candidates))
	for _, candidate := range candidates {
		if candidate.operation != operation || candidate.input.kind != ArtifactInputCandidate ||
			candidate.input.handle.IsZero() || candidate.digest.IsZero() {
			return nil, nil, invalid("BoundIntent candidates", "contains an invalid capture")
		}
		key := candidate.input.handle.String()
		if _, exists := captured[key]; exists {
			return nil, nil, invalid("BoundIntent candidates", "contains a duplicate handle")
		}
		captured[key] = candidate
	}
	resolved := make([]ResolvedArtifact, 0, len(inputs))
	usedCandidates := 0
	for _, input := range inputs {
		var digest Digest
		switch input.kind {
		case ArtifactInputCandidate:
			candidate, exists := captured[input.handle.String()]
			if !exists || candidate.input != input {
				return nil, nil, invariant("BoundIntent candidates", "candidate was not captured for this operation")
			}
			digest = candidate.digest
			usedCandidates++
		case ArtifactInputViewHandle:
			offer, exists := view[input.handle.String()]
			if !exists {
				return nil, nil, invariant("BoundIntent View Artifact", "handle was not offered by the View")
			}
			digest = offer.digest
		default:
			return nil, nil, invalid("BoundIntent Artifacts", "contains an invalid input kind")
		}
		resolved = append(resolved, ResolvedArtifact{input: input, digest: digest})
	}
	if usedCandidates != len(captured) {
		return nil, nil, invariant("BoundIntent candidates", "contains an unused candidate capture")
	}
	digests := make([]Digest, len(resolved))
	for index, artifact := range resolved {
		digests[index] = artifact.digest
	}
	sort.Slice(digests, func(i, j int) bool { return digests[i].String() < digests[j].String() })
	for index := 1; index < len(digests); index++ {
		if digests[index] == digests[index-1] {
			return nil, nil, invalid("BoundIntent Artifacts", "contains a duplicate digest")
		}
	}
	return resolved, digests, nil
}
