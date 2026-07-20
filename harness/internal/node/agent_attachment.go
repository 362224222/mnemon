package node

import (
	"io"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// agentAttachmentFilesystem keeps the secure local filesystem implementation
// at the Node composition boundary. Agent owns only the operations and values
// needed to preserve wake crash ordering.
type agentAttachmentFilesystem struct {
	nodeState string
}

func newAgentAttachmentFilesystem(nodeState string) *agentAttachmentFilesystem {
	return &agentAttachmentFilesystem{nodeState: nodeState}
}

func (filesystem *agentAttachmentFilesystem) ListCandidates() ([]agent.WakeAttachmentCandidate, error) {
	page, err := localapi.ListRunAttachmentCandidates(filesystem.nodeState)
	if err != nil {
		return nil, err
	}
	source := page.Candidates()
	result := make([]agent.WakeAttachmentCandidate, len(source))
	for index, candidate := range source {
		result[index] = agent.WakeAttachmentCandidate{
			RunID: candidate.RunID(), TokenHash: candidate.TokenHash(),
		}
	}
	return result, nil
}

func (filesystem *agentAttachmentFilesystem) RemoveReapable(runID model.RunID,
	tokenHash model.Digest,
) (bool, error) {
	return localapi.RemoveReapableRunAttachment(filesystem.nodeState, runID, tokenHash)
}

func (filesystem *agentAttachmentFilesystem) CleanupStages(at time.Time) (int, error) {
	return localapi.CleanupRunAttachmentStages(filesystem.nodeState, at)
}

func (filesystem *agentAttachmentFilesystem) Stage(random io.Reader) (agent.StagedRunAttachment, error) {
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

func (stage *agentAttachmentStage) Publish(runID model.RunID) (agent.RunAttachment, error) {
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

var _ agent.WakeAttachmentFilesystem = (*agentAttachmentFilesystem)(nil)
var _ agent.StagedRunAttachment = (*agentAttachmentStage)(nil)
var _ agent.RunAttachment = (*agentRunAttachment)(nil)
