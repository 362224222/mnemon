package nodecontrol

import (
	"io"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

// AgentAttachmentFilesystem adapts the owner-only local capability files to
// Node's transport-neutral wake attachment port. Paths, inode identities and
// raw capabilities remain private to the local API implementation.
type AgentAttachmentFilesystem struct {
	nodeState string
}

func NewAgentAttachmentFilesystem(nodeState string) *AgentAttachmentFilesystem {
	return &AgentAttachmentFilesystem{nodeState: nodeState}
}

func (filesystem *AgentAttachmentFilesystem) ListCandidates() ([]node.WakeAttachmentCandidate, error) {
	page, err := localapi.ListRunAttachmentCandidates(filesystem.nodeState)
	if err != nil {
		return nil, err
	}
	source := page.Candidates()
	result := make([]node.WakeAttachmentCandidate, len(source))
	for index, candidate := range source {
		result[index] = node.WakeAttachmentCandidate{
			RunID: candidate.RunID(), TokenHash: candidate.TokenHash(),
		}
	}
	return result, nil
}

func (filesystem *AgentAttachmentFilesystem) RemoveReapable(runID model.RunID,
	tokenHash model.Digest,
) (bool, error) {
	return localapi.RemoveReapableRunAttachment(filesystem.nodeState, runID, tokenHash)
}

func (filesystem *AgentAttachmentFilesystem) CleanupStages(at time.Time) (int, error) {
	return localapi.CleanupRunAttachmentStages(filesystem.nodeState, at)
}

func (filesystem *AgentAttachmentFilesystem) Stage(random io.Reader) (node.StagedRunAttachment, error) {
	staged, err := localapi.StageRunAttachment(filesystem.nodeState, random)
	if err != nil {
		return nil, err
	}
	return &agentAttachmentStage{nodeState: filesystem.nodeState, staged: staged}, nil
}

type agentAttachmentStage struct {
	nodeState string
	staged    localapi.StagedRunAttachment
}

func (stage *agentAttachmentStage) TokenHash() model.Digest { return stage.staged.TokenHash() }

func (stage *agentAttachmentStage) Publish(runID model.RunID) (node.RunAttachment, error) {
	attachment, err := stage.staged.Publish(runID)
	if err != nil {
		return nil, err
	}
	return &agentRunAttachment{nodeState: stage.nodeState, attachment: attachment}, nil
}

func (stage *agentAttachmentStage) Discard() error { return stage.staged.Discard() }

type agentRunAttachment struct {
	nodeState  string
	attachment localapi.RunAttachment
}

func (attachment *agentRunAttachment) Path() string { return attachment.attachment.Path() }

func (attachment *agentRunAttachment) Remove() error {
	return localapi.RemoveRunAttachment(attachment.nodeState, attachment.attachment)
}

var _ node.WakeAttachmentFilesystem = (*AgentAttachmentFilesystem)(nil)
var _ node.StagedRunAttachment = (*agentAttachmentStage)(nil)
var _ node.RunAttachment = (*agentRunAttachment)(nil)
