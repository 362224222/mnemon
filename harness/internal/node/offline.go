package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
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
	expected model.Digest, credentials ProfileCredentialVerifier, removeStale ControlSocketRecovery,
) (response Authority, err error) {
	return confirmOfflineAuthority(ctx, workspace, expected, credentials, removeStale)
}

type ControlSocketRecovery func(context.Context, string) (bool, error)

// confirmOfflineAuthority keeps the stale-socket operation as an internal
// dependency so the writer-held ordering can be proven without exporting a
// test hook or weakening the production boundary.
func confirmOfflineAuthority(ctx context.Context, workspace string,
	expected model.Digest, credentials ProfileCredentialVerifier, removeStale ControlSocketRecovery,
) (response Authority, err error) {
	if ctx == nil || expected.IsZero() {
		return Authority{}, offlineAuthorityError(
			errors.New("context or expected authority digest is unavailable"))
	}
	if isNilNodeInterface(credentials) || removeStale == nil {
		return Authority{}, offlineAuthorityError(
			errors.New("credential or stale socket recovery is unavailable"))
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return Authority{}, offlineAuthorityError(contextErr)
	}
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	authority, openErr := openExistingStoredAuthority(ctx, workspace, nodeState, true, credentials)
	if openErr != nil {
		if errors.Is(openErr, store.ErrWriterActive) {
			openErr = errors.Join(ErrOfflineAuthorityActive, openErr)
		}
		return Authority{}, offlineAuthorityError(openErr)
	}
	defer func() {
		if closeErr := authority.store.Close(); closeErr != nil {
			response = Authority{}
			err = errors.Join(err, offlineAuthorityError(fmt.Errorf("close Store: %w", closeErr)))
		}
	}()

	response, err = authorityValue(authority.authority)
	if err != nil {
		return Authority{}, offlineAuthorityError(err)
	}
	observed, digestErr := response.Digest()
	if digestErr != nil || observed != expected {
		if digestErr == nil {
			digestErr = errors.New("durable authority differs from expected")
		}
		return Authority{}, offlineAuthorityError(digestErr)
	}
	if err := ctx.Err(); err != nil {
		return Authority{}, offlineAuthorityError(err)
	}
	socketPath := filepath.Join(nodeState, controlSocketName)
	if _, err := removeStale(ctx, socketPath); err != nil {
		return Authority{}, offlineAuthorityError(err)
	}
	if _, err := os.Lstat(socketPath); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			err = errors.New("control socket remains after offline confirmation")
		}
		return Authority{}, offlineAuthorityError(err)
	}
	if err := ctx.Err(); err != nil {
		return Authority{}, offlineAuthorityError(err)
	}
	return response, nil
}

func offlineAuthorityError(err error) error {
	if err == nil {
		err = errors.New("unknown offline authority failure")
	}
	return fmt.Errorf("%w: %w", ErrOfflineAuthority, err)
}
