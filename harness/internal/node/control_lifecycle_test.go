package node

import (
	"context"
	"testing"
)

func TestControlMutationShutdownPreparerFuncRejectsNil(t *testing.T) {
	if _, _, apiErr := (MutationShutdownPreparerFunc(nil)).
		PrepareMutationShutdown(context.Background(), RequestMetadata{}); apiErr == nil ||
		apiErr.Code != CodeInternal {
		t.Fatalf("nil mutation shutdown preparer did not fail closed: %#v", apiErr)
	}
}
