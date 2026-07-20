package localapi

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// ProbeHealth performs the authenticated, identity-free daemon readiness
// probe. A valid not_ready response is returned to the caller as health state,
// not rewritten into a transport error.
func (c *Client) ProbeHealth(ctx context.Context) (HealthResponse, *APIError) {
	var response HealthResponse
	if c == nil || c.http == nil || ctx == nil {
		return HealthResponse{}, invalidControlResponse("local control client is unavailable")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://mnemond"+RouteHealth, nil)
	if err != nil {
		return HealthResponse{}, invalidControlResponse("local control request cannot be created")
	}
	request.Header.Set(authorizationHeader,
		profileScheme+base64.RawURLEncoding.EncodeToString(c.token[:]))
	if apiErr := c.send(request, &response, MaxHealthResponseBytes); apiErr != nil {
		return HealthResponse{}, apiErr
	}
	if apiErr := validateHealthResponse(response); apiErr != nil {
		return HealthResponse{}, apiErr
	}
	return response, nil
}

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

func healthResponse(snapshot HealthSnapshot) (HealthResponse, error) {
	if _, err := model.ParseDigest(snapshot.AssetRevision); err != nil {
		return HealthResponse{}, errors.New("local API: health asset revision is invalid")
	}
	status := "not_ready"
	if snapshot.WorkersReady {
		status = "ready"
	}
	response := HealthResponse{AssetRevision: snapshot.AssetRevision,
		SchemaVersion: SchemaVersion, Status: status}
	if raw, err := model.CanonicalMarshal(response); err != nil || len(raw)+1 > MaxHealthResponseBytes {
		return HealthResponse{}, errors.New("local API: health response exceeds its closed bound")
	}
	return response, nil
}

func validateHealthResponse(response HealthResponse) *APIError {
	if response.SchemaVersion != SchemaVersion ||
		(response.Status != "ready" && response.Status != "not_ready") {
		return invalidControlResponse("health response has an invalid readiness state")
	}
	if _, err := model.ParseDigest(response.AssetRevision); err != nil {
		return invalidControlResponse("health response has an invalid asset revision")
	}
	return nil
}
