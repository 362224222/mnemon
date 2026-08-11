package agency

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

func NewResolvedArtifact(input ArtifactInput, digest Digest) (ResolvedArtifact, error) {
	if (input.kind != ArtifactInputCandidate && input.kind != ArtifactInputViewHandle) ||
		input.handle.IsZero() || digest.IsZero() {
		return ResolvedArtifact{}, invalid("resolved Artifact", "input and verified digest are required")
	}
	return ResolvedArtifact{input: input, digest: digest}, nil
}

func (artifact ResolvedArtifact) Input() ArtifactInput { return artifact.input }
func (artifact ResolvedArtifact) Digest() Digest       { return artifact.digest }
