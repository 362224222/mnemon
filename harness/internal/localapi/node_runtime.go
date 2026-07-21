package localapi

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

type NodeRuntime struct{}

func (NodeRuntime) EnsureProfileCredential(nodeState string) (model.Digest, bool, error) {
	return EnsureProfileCredential(nodeState)
}

func (NodeRuntime) VerifyProfileCredential(nodeState string, expected model.Digest) error {
	return VerifyProfileCredential(nodeState, expected)
}

func (NodeRuntime) NewControlServer(authenticator node.Authenticator, service node.Service,
	health node.HealthProvider, status node.StatusProvider, authority node.AuthorityProvider,
	lifecycle node.LifecycleFunc, mutation node.MutationShutdownPreparer,
) (node.ControlServer, error) {
	return NewServerWithStatusLifecycle(authenticator, service, health, status, authority,
		lifecycle, mutation)
}

func (NodeRuntime) NewControlClient(nodeState string) (node.DaemonHealthProbe, error) {
	return NewClient(nodeState)
}

func (NodeRuntime) ListenOwnerUnix(socketPath string) (net.Listener, error) {
	return ListenOwnerUnix(socketPath)
}

func (NodeRuntime) RemoveStaleOwnerUnix(ctx context.Context,
	socketPath string,
) (bool, error) {
	return RemoveStaleOwnerUnix(ctx, socketPath)
}

func (NodeRuntime) NewRunAttachmentFilesystem(nodeState string) (node.RunAttachmentFilesystem, error) {
	if nodeState == "" {
		return nil, errors.New("local API: Node state is unavailable")
	}
	return nodeRunAttachmentFilesystem{nodeState: nodeState}, nil
}

type nodeRunAttachmentFilesystem struct {
	nodeState string
}

func (filesystem nodeRunAttachmentFilesystem) ListCandidates() ([]node.RunAttachmentCandidate, error) {
	page, err := ListRunAttachmentCandidates(filesystem.nodeState)
	if err != nil {
		return nil, err
	}
	source := page.Candidates()
	result := make([]node.RunAttachmentCandidate, len(source))
	for index, candidate := range source {
		result[index] = node.RunAttachmentCandidate{
			RunID: candidate.RunID(), TokenHash: candidate.TokenHash(),
		}
	}
	return result, nil
}

func (filesystem nodeRunAttachmentFilesystem) RemoveReapable(runID model.RunID,
	tokenHash model.Digest,
) (bool, error) {
	return RemoveReapableRunAttachment(filesystem.nodeState, runID, tokenHash)
}

func (filesystem nodeRunAttachmentFilesystem) CleanupStages(at time.Time) (int, error) {
	return CleanupRunAttachmentStages(filesystem.nodeState, at)
}

func (filesystem nodeRunAttachmentFilesystem) Stage(random io.Reader) (node.StagedRunAttachment, error) {
	staged, err := StageRunAttachment(filesystem.nodeState, random)
	if err != nil {
		return nil, err
	}
	return nodeStagedRunAttachment{staged: staged}, nil
}

func (filesystem nodeRunAttachmentFilesystem) Remove(attachment node.RunAttachment) error {
	switch value := attachment.(type) {
	case nodeRunAttachment:
		return RemoveRunAttachment(filesystem.nodeState, value.attachment)
	case *nodeRunAttachment:
		if value == nil {
			return ErrUnsafeClientState
		}
		return RemoveRunAttachment(filesystem.nodeState, value.attachment)
	default:
		return unsafeClientState("Run attachment was not issued by the local API")
	}
}

type nodeStagedRunAttachment struct {
	staged StagedRunAttachment
}

func (stage nodeStagedRunAttachment) TokenHash() model.Digest {
	return stage.staged.TokenHash()
}

func (stage nodeStagedRunAttachment) Publish(runID model.RunID) (node.RunAttachment, error) {
	attachment, err := stage.staged.Publish(runID)
	if err != nil {
		return nil, err
	}
	return nodeRunAttachment{attachment: attachment}, nil
}

func (stage nodeStagedRunAttachment) Discard() error {
	return stage.staged.Discard()
}

type nodeRunAttachment struct {
	attachment RunAttachment
}

func (attachment nodeRunAttachment) Path() string {
	return attachment.attachment.Path()
}

var _ node.ControlRuntime = NodeRuntime{}
var _ node.RunAttachmentFilesystem = nodeRunAttachmentFilesystem{}
var _ node.StagedRunAttachment = nodeStagedRunAttachment{}
var _ node.RunAttachment = nodeRunAttachment{}
