package node

import (
	"context"
	"errors"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestInstallationVerifierUsesTheCallerContext(t *testing.T) {
	called := false
	verify := InstallationVerifierFunc(func(ctx context.Context, _ model.Profile) error {
		called = true
		return ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := verify.Verify(ctx, model.Profile{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Verify(cancelled) error = %v", err)
	}
	if called {
		t.Fatal("pre-cancelled verification reached the installation callback")
	}
	if err := verify.Verify(nil, model.Profile{}); err == nil {
		t.Fatal("Verify(nil) succeeded")
	}
}
