package node

import (
	"context"
	"errors"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// InstallationVerifier is supplied by the outer composition layer. Node does
// not import Host integration; it only requires a current Profile to remain
// bound to its canonical Node bundle and Host projection.
type InstallationVerifier interface {
	Verify(context.Context, model.Profile) error
}

type InstallationVerifierFunc func(context.Context, model.Profile) error

func (verify InstallationVerifierFunc) Verify(ctx context.Context, profile model.Profile) error {
	if verify == nil || ctx == nil {
		return errors.New("managed installation verifier is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := verify(ctx, profile); err != nil {
		return err
	}
	return ctx.Err()
}
