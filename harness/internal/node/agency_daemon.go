package node

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/authority"
)

type daemonAgencyRuntime struct {
	store   *authority.Store
	service *LocalAgencyService
}

// openDaemonAgencyRuntime composes the R7 authority only from provisioned
// state and the existing immutable CAS. It never initializes agency.db or
// enrolls a Principal during serve.
func openDaemonAgencyRuntime(ctx context.Context, nodeState, profilePrincipal string,
	clock Clock,
) (_ *daemonAgencyRuntime, err error) {
	if ctx == nil || ctx.Err() != nil || nodeState == "" || clock == nil {
		return nil, errors.New("mnemond Agency runtime authority is unavailable")
	}
	principal, err := agencyPrincipalForValue(profilePrincipal)
	if err != nil {
		return nil, fmt.Errorf("derive Agency Principal: %w", err)
	}
	cas, err := artifact.NewCAS(filepath.Join(nodeState, "objects", "sha256"))
	if err != nil {
		return nil, fmt.Errorf("open Agency Artifact CAS: %w", err)
	}
	adapter, err := newR7ArtifactAdapter(cas)
	if err != nil {
		return nil, err
	}
	st, err := authority.OpenExistingWithArtifactVerifier(ctx,
		filepath.Join(nodeState, "agency.db"), adapter)
	if err != nil {
		return nil, fmt.Errorf("open existing Agency authority: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, st.Close())
		}
	}()
	if err := st.RequirePrincipal(ctx, principal); err != nil {
		return nil, fmt.Errorf("verify Agency Principal: %w", err)
	}
	service, err := NewLocalAgencyService(principal, st, adapter,
		LocalAgencyServiceOptions{Clock: clock})
	if err != nil {
		return nil, err
	}
	return &daemonAgencyRuntime{store: st, service: service}, nil
}

func (runtime *daemonAgencyRuntime) Close() error {
	if runtime == nil || runtime.store == nil {
		return nil
	}
	return runtime.store.Close()
}
