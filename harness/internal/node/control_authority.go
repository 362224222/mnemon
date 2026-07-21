package node

import (
	"context"
	"crypto/subtle"
	"errors"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// MaxAuthorityResponseBytes bounds both the compact authority success envelope
// and the existing API error union. Authority observation is setup metadata,
// never a general Store export.
const MaxAuthorityResponseBytes = 2048

// AuthoritySnapshot contains only the setup-facing projection of durable
// authority. Principal and credential material deliberately have no field in
// this type, so they cannot accidentally enter the wire response.
type AuthoritySnapshot struct {
	Host                model.HostKind
	Runtime             model.RuntimeKind
	Enabled             bool
	AssetRevision       string
	UpdatedAt           time.Time
	PeerID              model.PeerID
	ActiveAssetRevision string
}

// AuthorityResponse is the one closed wire and hidden-command receipt shape
// used to observe existing R5 authority.
type AuthorityResponse struct {
	ActiveAssetRevision string `json:"active_asset_revision"`
	AssetRevision       string `json:"asset_revision"`
	Enabled             bool   `json:"enabled"`
	Host                string `json:"host"`
	PeerID              string `json:"peer_id"`
	Runtime             string `json:"runtime"`
	SchemaVersion       int    `json:"schema_version"`
	UpdatedAt           string `json:"updated_at"`
}

type AuthorityProvider interface {
	Authority(context.Context, RequestMetadata) (AuthoritySnapshot, *APIError)
}

type AuthorityProviderFunc func(context.Context, RequestMetadata) (AuthoritySnapshot, *APIError)

func (provider AuthorityProviderFunc) Authority(ctx context.Context,
	metadata RequestMetadata,
) (AuthoritySnapshot, *APIError) {
	if provider == nil {
		return AuthoritySnapshot{}, NewAPIError(CodeInternal, "authority provider is unavailable")
	}
	return provider(ctx, metadata)
}

// NewAuthorityResponse validates a typed Store projection and creates the
// canonical closed response shared by the local API and offline inspection.
func NewAuthorityResponse(snapshot AuthoritySnapshot) (AuthorityResponse, error) {
	if !snapshot.Host.Valid() || !snapshot.Runtime.Valid() {
		return AuthorityResponse{}, errors.New("local API: authority Host or Runtime is invalid")
	}
	wantedRuntime, ok := model.RuntimeForHost(snapshot.Host)
	if !ok || snapshot.Runtime != wantedRuntime {
		return AuthorityResponse{}, errors.New("local API: authority Host and Runtime differ")
	}
	if snapshot.PeerID.IsZero() {
		return AuthorityResponse{}, errors.New("local API: authority PeerID is invalid")
	}
	if _, err := model.ParseDigest(snapshot.AssetRevision); err != nil {
		return AuthorityResponse{}, errors.New("local API: Profile asset revision is invalid")
	}
	if _, err := model.ParseDigest(snapshot.ActiveAssetRevision); err != nil ||
		snapshot.ActiveAssetRevision != snapshot.AssetRevision {
		return AuthorityResponse{}, errors.New("local API: Node and Profile asset revisions differ")
	}
	updatedAt, err := canonicalAuthorityWireTime(snapshot.UpdatedAt)
	if err != nil {
		return AuthorityResponse{}, err
	}
	response := AuthorityResponse{ActiveAssetRevision: snapshot.ActiveAssetRevision,
		AssetRevision: snapshot.AssetRevision, Enabled: snapshot.Enabled, Host: string(snapshot.Host),
		PeerID: snapshot.PeerID.String(), Runtime: string(snapshot.Runtime), SchemaVersion: SchemaVersion,
		UpdatedAt: updatedAt}
	if raw, marshalErr := model.CanonicalMarshal(response); marshalErr != nil ||
		len(raw)+1 > MaxAuthorityResponseBytes {
		return AuthorityResponse{}, errors.New("local API: authority response exceeds its closed bound")
	}
	return response, nil
}

// AuthorityDigest binds a closed authority response to its canonical wire
// representation. Conditional controller operations use this digest instead
// of accepting an authority snapshot in request content.
func AuthorityDigest(response AuthorityResponse) (model.Digest, error) {
	host := model.HostKind(response.Host)
	runtimeKind := model.RuntimeKind(response.Runtime)
	peerID, peerErr := model.ParsePeerID(response.PeerID)
	updatedAt, timeErr := time.Parse(time.RFC3339Nano, response.UpdatedAt)
	if response.SchemaVersion != SchemaVersion || peerErr != nil || timeErr != nil ||
		updatedAt.UTC().Format(time.RFC3339Nano) != response.UpdatedAt {
		return model.Digest{}, errors.New("local API: authority response is invalid")
	}
	_, err := NewAuthorityResponse(AuthoritySnapshot{Host: host, Runtime: runtimeKind,
		Enabled: response.Enabled, AssetRevision: response.AssetRevision, UpdatedAt: updatedAt,
		PeerID: peerID, ActiveAssetRevision: response.ActiveAssetRevision})
	if err != nil {
		return model.Digest{}, errors.New("local API: authority response is invalid")
	}
	raw, err := model.CanonicalMarshal(response)
	if err != nil || len(raw)+1 > MaxAuthorityResponseBytes {
		return model.Digest{}, errors.New("local API: authority response is not canonical")
	}
	return model.Sum(raw), nil
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

func canonicalAuthorityWireTime(value time.Time) (string, error) {
	if value.IsZero() {
		return "", errors.New("local API: authority update time is invalid")
	}
	canonical := value.Round(0).UTC()
	wire := canonical.Format(time.RFC3339Nano)
	parsed, err := time.Parse(time.RFC3339Nano, wire)
	if err != nil || !parsed.Equal(canonical) ||
		!time.Unix(0, canonical.UnixNano()).UTC().Equal(canonical) {
		return "", errors.New("local API: authority update time is invalid")
	}
	return wire, nil
}
