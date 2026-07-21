package node

import (
	"context"
	"errors"
	"fmt"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"os"
	"path/filepath"
)

var (
	ErrOfflineAuthority       = errors.New("confirm offline mnemond authority")
	ErrOfflineAuthorityActive = errors.New("offline mnemond writer is still active")
)

// OfflineAuthorityActiveExitCode is the one closed companion-process signal
// for retryable Store writer contention. It carries no diagnostics or state.
const OfflineAuthorityActiveExitCode = 75

// ConfirmOfflineAuthority is the companion-owned writer-release proof for a
// lifecycle transaction. It opens the exact existing Store writer, compares
// the complete durable authority with the caller's closed digest, and only
// while retaining that writer authority removes an unreachable owner socket.
//
// The caller must retain the Node ensure.lock across this command and every
// subsequent offline mutation. Managed daemon startup requires that same
// launch permit, so a successful return remains fenced after this short-lived
// companion releases the Store writer.
func ConfirmOfflineAuthority(ctx context.Context, workspace string,
	expected model.Digest,
) (response AuthorityResponse, err error) {
	return ConfirmOfflineAuthorityWithControl(ctx, workspace, expected, nil)
}

func ConfirmOfflineAuthorityWithControl(ctx context.Context, workspace string,
	expected model.Digest, control ControlRuntime,
) (response AuthorityResponse, err error) {
	control, controlErr := requireControlRuntime(control)
	if controlErr != nil {
		return AuthorityResponse{}, offlineAuthorityError(
			controlErr)
	}
	return confirmOfflineAuthority(ctx, workspace, expected, control.RemoveStaleOwnerUnix,
		control)
}

type removeStaleOwnerUnixFunc func(context.Context, string) (bool, error)

type existingStoredAuthorityOpener func(context.Context, string, string, bool) (
	existingDaemonAuthority, error,
)

// confirmOfflineAuthority keeps the stale-socket operation as an internal
// dependency so the writer-held ordering can be proven without exporting a
// test hook or weakening the production boundary.
func confirmOfflineAuthority(ctx context.Context, workspace string,
	expected model.Digest, removeStale removeStaleOwnerUnixFunc,
	credentials ...ProfileCredentialAuthority,
) (response AuthorityResponse, err error) {
	var credentialAuthority ProfileCredentialAuthority
	if len(credentials) > 0 {
		credentialAuthority = credentials[0]
	}
	return confirmOfflineAuthorityWith(ctx, workspace, expected, removeStale,
		func(ctx context.Context, workspace, nodeState string, allowDisabled bool) (
			existingDaemonAuthority, error,
		) {
			return openExistingStoredAuthority(ctx, workspace, nodeState, allowDisabled,
				credentialAuthority)
		})
}

func confirmOfflineAuthorityWith(ctx context.Context, workspace string,
	expected model.Digest, removeStale removeStaleOwnerUnixFunc,
	openAuthority existingStoredAuthorityOpener,
) (response AuthorityResponse, err error) {
	if ctx == nil || expected.IsZero() {
		return AuthorityResponse{}, offlineAuthorityError(
			errors.New("context or expected authority digest is unavailable"))
	}
	if removeStale == nil || openAuthority == nil {
		return AuthorityResponse{}, offlineAuthorityError(
			errors.New("stale socket recovery is unavailable"))
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return AuthorityResponse{}, offlineAuthorityError(contextErr)
	}
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	authority, openErr := openAuthority(ctx, workspace, nodeState, true)
	if openErr != nil {
		if errors.Is(openErr, store.ErrWriterActive) {
			openErr = errors.Join(ErrOfflineAuthorityActive, openErr)
		}
		return AuthorityResponse{}, offlineAuthorityError(openErr)
	}
	defer func() {
		if closeErr := authority.store.Close(); closeErr != nil {
			response = AuthorityResponse{}
			err = errors.Join(err, offlineAuthorityError(fmt.Errorf("close Store: %w", closeErr)))
		}
	}()

	response, err = NewAuthorityResponse(authoritySnapshot(authority.authority))
	if err != nil {
		return AuthorityResponse{}, offlineAuthorityError(err)
	}
	observed, digestErr := AuthorityDigest(response)
	if digestErr != nil || observed != expected {
		if digestErr == nil {
			digestErr = errors.New("durable authority differs from expected")
		}
		return AuthorityResponse{}, offlineAuthorityError(digestErr)
	}
	if err := ctx.Err(); err != nil {
		return AuthorityResponse{}, offlineAuthorityError(err)
	}
	socketPath := filepath.Join(nodeState, controlSocketName)
	if _, err := removeStale(ctx, socketPath); err != nil {
		return AuthorityResponse{}, offlineAuthorityError(err)
	}
	if _, err := os.Lstat(socketPath); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			err = errors.New("control socket remains after offline confirmation")
		}
		return AuthorityResponse{}, offlineAuthorityError(err)
	}
	if err := ctx.Err(); err != nil {
		return AuthorityResponse{}, offlineAuthorityError(err)
	}
	return response, nil
}

func offlineAuthorityError(err error) error {
	if err == nil {
		err = errors.New("unknown offline authority failure")
	}
	return fmt.Errorf("%w: %w", ErrOfflineAuthority, err)
}
