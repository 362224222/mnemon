package localapi

import (
	"crypto/subtle"
	"errors"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// MaxShutdownResponseBytes bounds both the fixed success envelope and the
// existing API error union. Shutdown carries no raw daemon state or identity.
const MaxShutdownResponseBytes = 1024

const authorityDigestHeader = "Mnemon-Authority-Digest"

// LifecycleFunc receives an authenticated request after its success response
// has been written. It must be nonblocking; Server invokes it at most once even
// when several authenticated shutdown requests race. A concrete function type
// lets construction reject nil without permitting a typed-nil provider to
// acknowledge a shutdown it cannot deliver.
type LifecycleFunc func()

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
