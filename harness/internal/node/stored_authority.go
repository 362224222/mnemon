package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type storedAuthorityDatabase struct {
	store     *store.Store
	authority store.LocalAuthority
}

func openStoredAuthorityDatabase(ctx context.Context, nodeState string) (storedAuthorityDatabase, error) {
	databasePath := filepath.Join(nodeState, "node.db")
	databaseInfo, err := os.Lstat(databasePath)
	if err != nil {
		return storedAuthorityDatabase{}, fmt.Errorf("%w: inspect node.db: %v", ErrDaemonAuthority, err)
	}
	if _, err := validateIdentityOwnerPath(databaseInfo, 0o600, false); err != nil {
		return storedAuthorityDatabase{}, fmt.Errorf("%w: node.db: %v", ErrDaemonAuthority, err)
	}
	st, err := store.OpenExisting(ctx, databasePath)
	if err != nil {
		return storedAuthorityDatabase{}, fmt.Errorf("%w: open Store: %w", ErrDaemonAuthority, err)
	}
	database := storedAuthorityDatabase{store: st}
	authority, err := st.ReadLocalAuthority(ctx)
	if err != nil {
		return storedAuthorityDatabase{}, database.closeForAuthorityError(
			fmt.Errorf("%w: %w", ErrDaemonAuthority, err))
	}
	if err := ctx.Err(); err != nil {
		return storedAuthorityDatabase{}, database.closeForAuthorityError(
			fmt.Errorf("%w: %w", ErrDaemonAuthority, err))
	}
	database.authority = authority
	return database, nil
}

func (database storedAuthorityDatabase) closeForAuthorityError(cause error) error {
	if closeErr := database.store.Close(); closeErr != nil {
		cause = errors.Join(cause, fmt.Errorf("%w: close Store: %v", ErrDaemonAuthority, closeErr))
	}
	return cause
}
