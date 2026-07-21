package node

import (
	"errors"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func NewHealthResponse(snapshot HealthSnapshot) (HealthResponse, error) {
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

func ValidateHealthResponse(response HealthResponse) *APIError {
	if response.SchemaVersion != SchemaVersion ||
		(response.Status != "ready" && response.Status != "not_ready") {
		return NewAPIError(CodeInternal, "health response has an invalid readiness state")
	}
	if _, err := model.ParseDigest(response.AssetRevision); err != nil {
		return NewAPIError(CodeInternal, "health response has an invalid asset revision")
	}
	return nil
}
