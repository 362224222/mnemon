package node

import (
	"context"
	"testing"
)

func TestControlStatusProviderFuncRejectsNil(t *testing.T) {
	t.Parallel()
	if _, apiErr := (StatusProviderFunc(nil)).Status(context.Background(), RequestMetadata{}); apiErr == nil ||
		apiErr.Code != CodeInternal {
		t.Fatalf("nil status provider did not fail closed: %#v", apiErr)
	}
}
