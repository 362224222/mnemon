package localapi

import (
	"context"
	"crypto/subtle"
	"errors"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// MaxShutdownResponseBytes bounds both the fixed success envelope and the
// existing API error union. Shutdown carries no raw daemon state or identity.
const MaxShutdownResponseBytes = 1024

const (
	authorityDigestHeader       = "Mnemon-Authority-Digest"
	mutationShutdownHeader      = "Mnemon-Mutation-Shutdown"
	mutationShutdownHeaderValue = "required"
)

// LifecycleFunc receives an authenticated request after its success response
// has been written. It must be nonblocking; Server invokes it at most once even
// when several authenticated shutdown requests race. A concrete function type
// lets construction reject nil without permitting a typed-nil provider to
// acknowledge a shutdown it cannot deliver.
type LifecycleFunc func()

// AdmissionReleaseFunc reopens a process-local admission seal when the
// server rejects a prepared mutation shutdown. It is deliberately separate
// from LifecycleFunc: success retains the seal until process exit, while every
// failed validation must invoke this release exactly once.
type AdmissionReleaseFunc func()

// MutationShutdownPreparer atomically seals new Agent admission, drains
// in-flight Store-facing work, and proves that metadata.Profile is the exact
// idle durable authority. The returned snapshot was read in that same Store
// transaction. The server calls release on every later validation failure and
// deliberately retains the seal after a successful stopping response.
type MutationShutdownPreparer interface {
	PrepareMutationShutdown(context.Context, RequestMetadata) (
		AuthoritySnapshot, AdmissionReleaseFunc, *APIError,
	)
}

// MutationShutdownPreparerFunc adapts the closed mutation preparation
// boundary without exposing a controller or Store through the local API.
type MutationShutdownPreparerFunc func(context.Context, RequestMetadata) (
	AuthoritySnapshot, AdmissionReleaseFunc, *APIError,
)

func (prepare MutationShutdownPreparerFunc) PrepareMutationShutdown(ctx context.Context,
	metadata RequestMetadata,
) (AuthoritySnapshot, AdmissionReleaseFunc, *APIError) {
	if prepare == nil {
		return AuthoritySnapshot{}, nil, NewAPIError(CodeInternal,
			"mutation shutdown preparation is unavailable")
	}
	return prepare(ctx, metadata)
}

// ShutdownResponse is the entire successful shutdown protocol. Its echoed
// authority digest confirms which exact durable generation was accepted; no
// PID or raw process state crosses the owner-only API.
type ShutdownResponse struct {
	AuthorityDigest string `json:"authority_digest"`
	SchemaVersion   int    `json:"schema_version"`
	Status          string `json:"status"`
}

func newShutdownResponse(authorityDigest model.Digest) ShutdownResponse {
	return ShutdownResponse{AuthorityDigest: authorityDigest.String(),
		SchemaVersion: SchemaVersion, Status: "stopping"}
}

func validateShutdownResponse(response ShutdownResponse, expected model.Digest) *APIError {
	actual, err := model.ParseDigest(response.AuthorityDigest)
	if expected.IsZero() || err != nil || response.SchemaVersion != SchemaVersion ||
		response.Status != "stopping" || !sameAuthorityDigest(actual, expected) {
		return invalidControlResponse("shutdown response has an invalid lifecycle state")
	}
	raw, err := model.CanonicalMarshal(response)
	if err != nil || len(raw)+1 > MaxShutdownResponseBytes {
		return invalidControlResponse("shutdown response exceeds its closed bound")
	}
	return nil
}

func sameAuthorityDigest(left, right model.Digest) bool {
	if left.IsZero() || right.IsZero() {
		return false
	}
	leftBytes := left.Bytes()
	rightBytes := right.Bytes()
	matched := subtle.ConstantTimeCompare(leftBytes, rightBytes) == 1
	clear(leftBytes)
	clear(rightBytes)
	return matched
}

func parseAuthorityDigest(value string) (model.Digest, error) {
	digest, err := model.ParseDigest(value)
	if err != nil || digest.IsZero() || digest.String() != value {
		return model.Digest{}, errors.New("local API: authority digest is invalid")
	}
	return digest, nil
}
