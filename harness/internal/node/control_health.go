package node

import "context"

// MaxHealthResponseBytes includes both the compact success envelope and the
// existing bounded API error union.
const MaxHealthResponseBytes = 1024

// HealthProvider reads daemon readiness from the existing controller
// authorities. RequestMetadata is authenticated by the local transport before
// the provider is called; a provider must not accept identity from request
// content or create a parallel health authority.
type HealthProvider interface {
	Health(context.Context, RequestMetadata) (HealthSnapshot, *APIError)
}

// HealthProviderFunc adapts a bounded controller health read to HealthProvider.
type HealthProviderFunc func(context.Context, RequestMetadata) (HealthSnapshot, *APIError)

func (provider HealthProviderFunc) Health(ctx context.Context,
	metadata RequestMetadata,
) (HealthSnapshot, *APIError) {
	if provider == nil {
		return HealthSnapshot{}, NewAPIError(CodeInternal, "health provider is unavailable")
	}
	return provider(ctx, metadata)
}

// HealthSnapshot is the dynamic, identity-free input to the health wire
// envelope. The daemon and schema signals are established by reaching this v1
// route; the provider contributes only the active asset and aggregate worker
// readiness owned by the controller.
type HealthSnapshot struct {
	AssetRevision string
	WorkersReady  bool
}

// HealthResponse is deliberately aggregate and identity-free. In particular,
// it never exposes PeerID, Profile, worker names, queue contents, or Event data.
type HealthResponse struct {
	AssetRevision string `json:"asset_revision"`
	SchemaVersion int    `json:"schema_version"`
	Status        string `json:"status"`
}
