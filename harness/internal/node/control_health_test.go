package node

import (
	"context"
	"testing"
)

func TestControlHealthProviderFuncRejectsNil(t *testing.T) {
	if _, apiErr := (HealthProviderFunc(nil)).Health(context.Background(), RequestMetadata{}); apiErr == nil ||
		apiErr.Code != CodeInternal {
		t.Fatalf("nil health provider did not fail closed: %#v", apiErr)
	}
}
