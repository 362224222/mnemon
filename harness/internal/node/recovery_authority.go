package node

import (
	"context"
	"errors"
	"fmt"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"path/filepath"
)

var ErrRecoveryAuthority = errors.New("inspect recovery mnemond authority")

// InspectRecoveryAuthority reads only the existing durable Node authority. It
// deliberately does not require identity-key or Profile-token projections:
// those are common reasons for invoking whole-Node reset. The owner-only Node
// directory, node.db, workspace binding, schema, and exclusive Store writer
// remain mandatory.
func InspectRecoveryAuthority(ctx context.Context,
	workspace string,
) (_ AuthoritySnapshot, err error) {
	authority, err := openRecoveryStoredAuthority(ctx, workspace)
	if err != nil {
		return AuthoritySnapshot{}, fmt.Errorf("%w: %v", ErrRecoveryAuthority, err)
	}
	defer func() {
		if closeErr := authority.store.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("%w: close Store: %v", ErrRecoveryAuthority, closeErr))
		}
	}()
	if err := ctx.Err(); err != nil {
		return AuthoritySnapshot{}, fmt.Errorf("%w: %v", ErrRecoveryAuthority, err)
	}
	return authoritySnapshot(authority.authority), nil
}

// ConfirmRecoveryOfflineAuthority is the keyless/tokenless writer-release
// proof used only while setup.lock and ensure.lock fence the complete Node.
func ConfirmRecoveryOfflineAuthority(ctx context.Context, workspace string,
	expected model.Digest,
) (AuthorityResponse, error) {
	return ConfirmRecoveryOfflineAuthorityWithControl(ctx, workspace, expected, nil)
}

func ConfirmRecoveryOfflineAuthorityWithControl(ctx context.Context, workspace string,
	expected model.Digest, control ControlRuntime,
) (AuthorityResponse, error) {
	control, controlErr := requireControlRuntime(control)
	if controlErr != nil {
		return AuthorityResponse{}, fmt.Errorf("%w: %w", ErrRecoveryAuthority,
			controlErr)
	}
	return confirmOfflineAuthorityWith(ctx, workspace, expected, control.RemoveStaleOwnerUnix,
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
	database, err := openStoredAuthorityDatabase(ctx, nodeState)
	if err != nil {
		return existingDaemonAuthority{}, err
	}
	fail := func(cause error) (existingDaemonAuthority, error) {
		return existingDaemonAuthority{}, database.closeForAuthorityError(cause)
	}
	authority := database.authority
	if authority.Profile.ID() != model.TeamworkProfileID() ||
		authority.Profile.WorkspaceRoot() != validatedWorkspace || authority.Node.PeerID().IsZero() {
		return fail(fmt.Errorf("%w: durable recovery binding is invalid", ErrDaemonAuthority))
	}
	if err := ctx.Err(); err != nil {
		return fail(fmt.Errorf("%w: %v", ErrDaemonAuthority, err))
	}
	return existingDaemonAuthority{store: database.store, authority: authority}, nil
}
