package driver

import (
	"context"

	"github.com/mnemon-dev/mnemon/harness/internal/drive"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation"
)

type HTTPRenderClient = drive.HTTPRenderClient
type FileManagedWakeLedger = drive.FileManagedWakeLedger
type CodexAppServerTurnClient = drive.CodexAppServerTurnClient

func ManagedWakeCandidatesFromRender(principal string, resp presentation.Response) []ManagedWakeCandidate {
	return drive.ManagedWakeCandidatesFromRender(principal, resp)
}

func NewFileManagedWakeLedger(path string) *FileManagedWakeLedger {
	return drive.NewFileManagedWakeLedger(path)
}

func CodexAppServerAdditionalContext(ctx context.Context, workspace string, env []string, query string) (map[string]any, error) {
	return drive.CodexAppServerAdditionalContext(ctx, workspace, env, query)
}
