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
