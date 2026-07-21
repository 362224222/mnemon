package localapi

import (
	"errors"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

func validateAuthorityResponse(response AuthorityResponse) *APIError {
	host := model.HostKind(response.Host)
	runtimeKind := model.RuntimeKind(response.Runtime)
	peerID, peerErr := model.ParsePeerID(response.PeerID)
	updatedAt, timeErr := time.Parse(time.RFC3339Nano, response.UpdatedAt)
	if response.SchemaVersion != SchemaVersion || peerErr != nil || timeErr != nil ||
		updatedAt.UTC().Format(time.RFC3339Nano) != response.UpdatedAt {
		return invalidControlResponse("authority response has invalid identity or time")
	}
	_, err := NewAuthorityResponse(AuthoritySnapshot{Host: host, Runtime: runtimeKind,
		Enabled: response.Enabled, AssetRevision: response.AssetRevision, UpdatedAt: updatedAt,
		PeerID: peerID, ActiveAssetRevision: response.ActiveAssetRevision})
	if err != nil {
		return invalidControlResponse("authority response has invalid durable state")
	}
	return nil
}

func NewAuthorityResponse(snapshot AuthoritySnapshot) (AuthorityResponse, error) {
	return node.NewAuthorityResponse(snapshot)
}

func AuthorityDigest(response AuthorityResponse) (model.Digest, error) {
	if apiErr := validateAuthorityResponse(response); apiErr != nil {
		return model.Digest{}, errors.New("local API: authority response is invalid")
	}
	return node.AuthorityDigest(response)
}
