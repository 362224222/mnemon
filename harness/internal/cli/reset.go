package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

type resetSetupLock interface {
	Close() error
	CloseAfterRename() error
}

type resetAuthorityClient interface {
	node.DaemonLifecycleClient
	ReadAuthority(context.Context) (localapi.AuthorityResponse, *localapi.APIError)
}

type resetLifecycle interface {
	Quiesce(context.Context, node.DaemonLifecycleClient, node.DaemonOfflineConfirmer,
		localapi.AuthorityResponse) (localapi.AuthorityResponse, error)
	Quarantine(context.Context, localapi.AuthorityResponse, time.Time) (
		node.RecoveryQuarantineResult, error)
	Close() error
}

type resetDependencies struct {
	workingDirectory func() (string, error)
	acquireLock      func(context.Context, string) (resetSetupLock, error)
	acquireLifecycle func(context.Context, node.DaemonLifecycleOptions) (resetLifecycle, error)
	newClient        func(string) (resetAuthorityClient, error)
	inspect          func(context.Context, string) (localapi.AuthoritySnapshot, error)
	confirmOffline   func(context.Context, string, model.Digest) (localapi.AuthorityResponse, error)
	now              func() time.Time
}

type resetRequest struct {
	confirmPeer model.PeerID
}

type resetReceipt struct {
	PeerID        string `json:"peer_id"`
	RecoveryPath  string `json:"recovery_path"`
	RenamedAt     string `json:"renamed_at"`
	SchemaVersion int    `json:"schema_version"`
	Status        string `json:"status"`
}

type resetApp struct {
	stdout io.Writer
	stderr io.Writer
	deps   resetDependencies
}

func productionResetDependencies() resetDependencies {
	return resetDependencies{
		workingDirectory: os.Getwd,
		acquireLock: func(ctx context.Context, nodeState string) (resetSetupLock, error) {
			return acquireSetupLock(ctx, nodeState)
		},
		acquireLifecycle: func(ctx context.Context,
			options node.DaemonLifecycleOptions,
		) (resetLifecycle, error) {
			return node.AcquireRecoveryLifecycle(ctx, options)
		},
		newClient: func(nodeState string) (resetAuthorityClient, error) {
			return localapi.NewClient(nodeState)
		},
		inspect:        node.InspectRecoveryAuthority,
		confirmOffline: node.ConfirmRecoveryOfflineAuthority,
		now:            time.Now,
	}
}

// RunReset is intentionally absent from ordinary help. It is the bounded
// doctor-guided escape hatch for a Node whose projections cannot be repaired.
func RunReset(ctx context.Context, args []string, stdout, stderr io.Writer, _ string) int {
	app := &resetApp{stdout: stdout, stderr: stderr, deps: productionResetDependencies()}
	return app.run(ctx, args)
}

func (app *resetApp) run(ctx context.Context, args []string) int {
	if app == nil || ctx == nil || app.stdout == nil || app.stderr == nil ||
		!validResetDependencies(app.deps) {
		return 1
	}
	request, apiErr := parseResetRequest(args)
	if apiErr != nil {
		return app.writeError(apiErr)
	}
	receipt, apiErr := app.execute(ctx, request)
	if apiErr != nil {
		return app.writeError(apiErr)
	}
	raw, err := model.CanonicalMarshal(receipt)
	if err != nil {
		return 1
	}
	if _, err := app.stdout.Write(append(raw, '\n')); err != nil {
		return 1
	}
	return 0
}

func (app *resetApp) execute(ctx context.Context,
	request resetRequest,
) (resetReceipt, *localapi.APIError) {
	workspace, err := resolveSetupWorkspace("", app.deps.workingDirectory)
	if err != nil {
		return resetReceipt{}, setupError(localapi.CodeInvalidArgument,
			"current project root must be an existing physical directory")
	}
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	lock, err := app.deps.acquireLock(ctx, nodeState)
	if err != nil || lock == nil {
		return resetReceipt{}, setupAuthError("managed Node is not initialized or is unsafe")
	}
	result, moved, apiErr := app.executeLocked(ctx, workspace, nodeState, request)
	var closeErr error
	if moved {
		closeErr = lock.CloseAfterRename()
	} else {
		closeErr = lock.Close()
	}
	if closeErr != nil {
		return resetReceipt{}, setupAuthError("managed setup authority changed during reset")
	}
	return result, apiErr
}

func (app *resetApp) executeLocked(ctx context.Context, workspace, nodeState string,
	request resetRequest,
) (resetReceipt, bool, *localapi.APIError) {
	lease, err := app.deps.acquireLifecycle(ctx, node.DaemonLifecycleOptions{
		Workspace: workspace, NodeState: nodeState,
	})
	if err != nil || lease == nil {
		return resetReceipt{}, false,
			setupUnavailableError("managed daemon lifecycle is unavailable")
	}
	receipt, moved, apiErr := app.executeLeased(ctx, workspace, nodeState, request, lease)
	if closeErr := lease.Close(); closeErr != nil {
		return resetReceipt{}, moved,
			setupAuthError("managed daemon lifecycle changed during reset")
	}
	return receipt, moved, apiErr
}

func (app *resetApp) executeLeased(ctx context.Context, workspace, nodeState string,
	request resetRequest, lease resetLifecycle,
) (resetReceipt, bool, *localapi.APIError) {
	authority, client, apiErr := app.observeAuthority(ctx, workspace, nodeState)
	if apiErr != nil {
		return resetReceipt{}, false, apiErr
	}
	if authority.PeerID != request.confirmPeer.String() {
		return resetReceipt{}, false, setupError(localapi.CodeOperationMismatch,
			"--confirm-peer does not match the current durable Node")
	}
	confirmer := node.DaemonOfflineConfirmerFunc(func(ctx context.Context,
		expected localapi.AuthorityResponse,
	) (localapi.AuthorityResponse, error) {
		digest, err := localapi.AuthorityDigest(expected)
		if err != nil {
			return localapi.AuthorityResponse{}, err
		}
		return app.deps.confirmOffline(ctx, workspace, digest)
	})
	quiesced, err := lease.Quiesce(ctx, client, confirmer, authority)
	if err != nil || quiesced != authority {
		return resetReceipt{}, false,
			setupUnavailableError("managed daemon could not be stopped and fenced safely")
	}
	quarantined, err := lease.Quarantine(ctx, authority, app.deps.now())
	moved := quarantined.NodePath != ""
	if err != nil {
		return resetReceipt{}, moved,
			setupError(localapi.CodeInternal, "managed Node could not be quarantined durably")
	}
	if quarantined.PeerID != authority.PeerID || !filepath.IsAbs(quarantined.NodePath) ||
		quarantined.RenamedAt.IsZero() {
		return resetReceipt{}, moved,
			setupError(localapi.CodeInternal, "managed reset returned invalid forensic evidence")
	}
	return resetReceipt{PeerID: quarantined.PeerID, RecoveryPath: quarantined.NodePath,
		RenamedAt:     quarantined.RenamedAt.UTC().Format(time.RFC3339Nano),
		SchemaVersion: localapi.SchemaVersion, Status: "reset"}, true, nil
}

func (app *resetApp) observeAuthority(ctx context.Context, workspace,
	nodeState string,
) (localapi.AuthorityResponse, node.DaemonLifecycleClient, *localapi.APIError) {
	client, clientErr := app.deps.newClient(nodeState)
	if clientErr == nil && client != nil {
		authority, apiErr := client.ReadAuthority(ctx)
		if apiErr == nil {
			return authority, client, nil
		}
		if apiErr.Code != localapi.CodeMnemondUnavailable {
			return localapi.AuthorityResponse{}, nil, normalizeSetupAPIError(apiErr)
		}
	}
	snapshot, err := app.deps.inspect(ctx, workspace)
	if err != nil {
		return localapi.AuthorityResponse{}, nil,
			setupUnavailableError("durable Node authority could not be inspected exclusively")
	}
	authority, err := localapi.NewAuthorityResponse(snapshot)
	if err != nil {
		return localapi.AuthorityResponse{}, nil,
			setupAuthError("durable Node authority is invalid")
	}
	if clientErr != nil || client == nil {
		client = resetUnavailableClient{}
	}
	return authority, client, nil
}

type resetUnavailableClient struct{}

func (resetUnavailableClient) ReadAuthority(context.Context) (
	localapi.AuthorityResponse, *localapi.APIError,
) {
	return localapi.AuthorityResponse{}, localapi.NewAPIError(localapi.CodeMnemondUnavailable,
		"mnemond local control is unavailable")
}

func (resetUnavailableClient) ShutdownForMutation(context.Context,
	localapi.AuthorityResponse,
) (localapi.ShutdownResponse, *localapi.APIError) {
	return localapi.ShutdownResponse{}, localapi.NewAPIError(localapi.CodeMnemondUnavailable,
		"mnemond local control is unavailable")
}

func parseResetRequest(args []string) (resetRequest, *localapi.APIError) {
	force := false
	confirmation := ""
	for index := 0; index < len(args); index++ {
		switch args[index] {
		case "--force":
			if force {
				return resetRequest{}, invalidResetRequest()
			}
			force = true
		case "--confirm-peer":
			if confirmation != "" || index+1 >= len(args) || args[index+1] == "" ||
				strings.HasPrefix(args[index+1], "--") {
				return resetRequest{}, invalidResetRequest()
			}
			index++
			confirmation = args[index]
		default:
			return resetRequest{}, invalidResetRequest()
		}
	}
	peerID, err := model.ParsePeerID(confirmation)
	if !force || err != nil || peerID.String() != confirmation {
		return resetRequest{}, invalidResetRequest()
	}
	return resetRequest{confirmPeer: peerID}, nil
}

func invalidResetRequest() *localapi.APIError {
	return setupError(localapi.CodeInvalidArgument,
		"reset requires exactly --force --confirm-peer <current-peer-id>")
}

func validResetDependencies(deps resetDependencies) bool {
	return deps.workingDirectory != nil && deps.acquireLock != nil &&
		deps.acquireLifecycle != nil && deps.newClient != nil && deps.inspect != nil &&
		deps.confirmOffline != nil && deps.now != nil
}

func (app *resetApp) writeError(apiErr *localapi.APIError) int {
	if apiErr == nil {
		apiErr = setupError(localapi.CodeInternal, "internal reset error")
	}
	if _, err := fmt.Fprintf(app.stderr, "%s: %s\n", apiErr.Code, apiErr.Message); err != nil {
		return 1
	}
	return apiErr.ExitStatus()
}
