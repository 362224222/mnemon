package localapi

import (
	"context"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestHealthResponseIsBoundedClosedAndIdentityFree(t *testing.T) {
	t.Parallel()
	revision := model.Sum([]byte("managed-assets")).String()
	response, err := healthResponse(HealthSnapshot{AssetRevision: revision, WorkersReady: true})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := model.CanonicalMarshal(response)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"asset_revision":"` + revision + `","schema_version":1,"status":"ready"}`
	if string(raw) != want || len(raw)+1 > MaxHealthResponseBytes {
		t.Fatalf("health response = %s (%d bytes)", raw, len(raw)+1)
	}
	if apiErr := validateHealthResponse(response); apiErr != nil {
		t.Fatal(apiErr)
	}
}

func TestHealthResponseRejectsInvalidProviderAndWireStates(t *testing.T) {
	t.Parallel()
	revision := model.Sum([]byte("managed-assets")).String()
	if _, err := healthResponse(HealthSnapshot{AssetRevision: "asset-r5", WorkersReady: true}); err == nil {
		t.Fatal("nondigest asset revision was accepted")
	}
	for _, response := range []HealthResponse{
		{AssetRevision: revision, SchemaVersion: 2, Status: "ready"},
		{AssetRevision: revision, SchemaVersion: 1, Status: "starting"},
		{AssetRevision: "asset-r5", SchemaVersion: 1, Status: "not_ready"},
	} {
		if apiErr := validateHealthResponse(response); apiErr == nil || apiErr.Code != CodeInternal {
			t.Fatalf("invalid health response accepted: %#v", response)
		}
	}
}

func TestHealthProviderFuncRetainsAuthenticatedMetadata(t *testing.T) {
	t.Parallel()
	called := false
	provider := HealthProviderFunc(func(ctx context.Context,
		metadata RequestMetadata,
	) (HealthSnapshot, *APIError) {
		called = ctx != nil && metadata.Profile.ID() == model.TeamworkProfileID()
		return HealthSnapshot{AssetRevision: model.Sum([]byte("assets")).String(), WorkersReady: true}, nil
	})
	if _, apiErr := provider.Health(context.Background(),
		RequestMetadata{Profile: localAPITestProfile()}); apiErr != nil || !called {
		t.Fatalf("Health() called=%t error=%#v", called, apiErr)
	}
}
