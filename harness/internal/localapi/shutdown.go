package localapi

import (
	"crypto/subtle"
	"errors"
	"net/http"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	authorityDigestHeader       = "Mnemon-Authority-Digest"
	mutationShutdownHeader      = "Mnemon-Mutation-Shutdown"
	mutationShutdownHeaderValue = "required"
)

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

func parseMutationShutdownHeader(header http.Header) (bool, error) {
	values := header.Values(mutationShutdownHeader)
	if len(values) == 0 {
		return false, nil
	}
	if len(values) != 1 || values[0] != mutationShutdownHeaderValue {
		return false, errors.New("mutation shutdown header must have its fixed value")
	}
	return true, nil
}
