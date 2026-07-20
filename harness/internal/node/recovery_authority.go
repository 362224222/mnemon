package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

var ErrRecoveryAuthority = errors.New("inspect recovery mnemond authority")

// InspectRecoveryAuthority reads only the existing durable Node authority. It
// deliberately does not require identity-key or Profile-token projections:
// those are common reasons for invoking whole-Node reset. The owner-only Node
// directory, node.db, workspace binding, schema, and exclusive Store writer
// remain mandatory.
func InspectRecoveryAuthority(ctx context.Context,
	workspace string,
) (_ localapi.AuthoritySnapshot, err error) {
	authority, err := openRecoveryStoredAuthority(ctx, workspace)
	if err != nil {
		return localapi.AuthoritySnapshot{}, fmt.Errorf("%w: %v", ErrRecoveryAuthority, err)
	}
	defer func() {
		if closeErr := authority.store.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("%w: close Store: %v", ErrRecoveryAuthority, closeErr))
		}
	}()
	if err := ctx.Err(); err != nil {
		return localapi.AuthoritySnapshot{}, fmt.Errorf("%w: %v", ErrRecoveryAuthority, err)
	}
	return authoritySnapshot(authority.authority), nil
}

// ConfirmRecoveryOfflineAuthority is the keyless/tokenless writer-release
// proof used only while setup.lock and ensure.lock fence the complete Node.
func ConfirmRecoveryOfflineAuthority(ctx context.Context, workspace string,
	expected model.Digest,
) (localapi.AuthorityResponse, error) {
	return confirmOfflineAuthorityWith(ctx, workspace, expected, localapi.RemoveStaleOwnerUnix,
		func(ctx context.Context, workspace, _ string, _ bool) (existingDaemonAuthority, error) {
			return openRecoveryStoredAuthority(ctx, workspace)
		})
}

func openRecoveryStoredAuthority(ctx context.Context,
	workspace string,
) (existingDaemonAuthority, error) {
	if ctx == nil {
		return existingDaemonAuthority{}, fmt.Errorf("%w: context is unavailable", ErrDaemonAuthority)
	}
	if err := ctx.Err(); err != nil {
		return existingDaemonAuthority{}, fmt.Errorf("%w: %v", ErrDaemonAuthority, err)
	}
	validatedWorkspace, err := validateDaemonWorkspace(workspace)
	if err != nil {
		return existingDaemonAuthority{}, err
	}
	nodeState := filepath.Join(validatedWorkspace, ".mnemon", "harness", "node")
	databasePath := filepath.Join(nodeState, "node.db")
	databaseInfo, err := os.Lstat(databasePath)
	if err != nil {
		return existingDaemonAuthority{}, fmt.Errorf("%w: inspect node.db: %v", ErrDaemonAuthority, err)
	}
	if _, err := validateIdentityOwnerPath(databaseInfo, 0o600, false); err != nil {
		return existingDaemonAuthority{}, fmt.Errorf("%w: node.db: %v", ErrDaemonAuthority, err)
	}
	st, err := store.OpenExisting(ctx, databasePath)
	if err != nil {
		return existingDaemonAuthority{}, fmt.Errorf("%w: open Store: %w", ErrDaemonAuthority, err)
	}
	fail := func(cause error) (existingDaemonAuthority, error) {
		if closeErr := st.Close(); closeErr != nil {
			cause = errors.Join(cause, fmt.Errorf("%w: close Store: %v", ErrDaemonAuthority, closeErr))
		}
		return existingDaemonAuthority{}, cause
	}
	authority, err := st.ReadLocalAuthority(ctx)
	if err != nil {
		return fail(fmt.Errorf("%w: %v", ErrDaemonAuthority, err))
	}
	if authority.Profile.ID() != model.TeamworkProfileID() ||
		authority.Profile.WorkspaceRoot() != validatedWorkspace || authority.Node.PeerID().IsZero() {
		return fail(fmt.Errorf("%w: durable recovery binding is invalid", ErrDaemonAuthority))
	}
	if err := ctx.Err(); err != nil {
		return fail(fmt.Errorf("%w: %v", ErrDaemonAuthority, err))
	}
	return existingDaemonAuthority{store: st, authority: authority}, nil
}
