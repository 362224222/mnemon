package exchange

import "github.com/mnemon-dev/mnemon/harness/internal/contract"

// RemoteWorkspace is the local mnemond side of the Remote Workspace sync ABI.
//
// The method names intentionally match the existing wire verbs so the current
// HTTP mnemon-hub client satisfies this interface without a wrapper. Future
// backends must present the same accepted synced-envelope behavior to the local
// sync loop.
type RemoteWorkspace interface {
	SyncPush(contract.SyncPushRequest) (contract.SyncPushResponse, error)
	SyncPull(contract.SyncPullRequest) (contract.SyncPullResponse, error)
	SyncStatus() (contract.SyncStatusResponse, error)
}
