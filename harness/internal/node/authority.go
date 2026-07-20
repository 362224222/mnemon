package node

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

var ErrAuthorityInspection = errors.New("inspect existing mnemond authority")

// InspectAuthority observes one existing Node while mnemond is stopped. It
// acquires the same exclusive Store writer guard as daemon startup, so an
// active writer fails explicitly. It never initializes, migrates, repairs or
// projects state, and a disabled Profile remains observable for setup repair.
func InspectAuthority(ctx context.Context, workspace string) (_ localapi.AuthoritySnapshot, err error) {
	if ctx == nil {
		return localapi.AuthoritySnapshot{}, fmt.Errorf("%w: context is unavailable", ErrAuthorityInspection)
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return localapi.AuthoritySnapshot{}, fmt.Errorf("%w: %w", ErrAuthorityInspection, contextErr)
	}
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	authority, openErr := openExistingStoredAuthority(ctx, workspace, nodeState, true)
	if openErr != nil {
		return localapi.AuthoritySnapshot{}, fmt.Errorf("%w: %w", ErrAuthorityInspection, openErr)
	}
	defer func() {
		if closeErr := authority.store.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("%w: close Store: %v", ErrAuthorityInspection, closeErr))
		}
	}()
	if contextErr := ctx.Err(); contextErr != nil {
		return localapi.AuthoritySnapshot{}, fmt.Errorf("%w: %w", ErrAuthorityInspection, contextErr)
	}
	return authoritySnapshot(authority.authority), nil
}

func authoritySnapshot(authority store.LocalAuthority) localapi.AuthoritySnapshot {
	return localapi.AuthoritySnapshot{Host: authority.Profile.Host(), Runtime: authority.Profile.Runtime(),
		Enabled: authority.Profile.Enabled(), AssetRevision: authority.Profile.ActiveAssetRevision(),
		UpdatedAt: authority.Profile.UpdatedAt(), PeerID: authority.Node.PeerID(),
		ActiveAssetRevision: authority.Node.ActiveAssetRevision()}
}
