package node

import (
	"io"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// agentAttachmentFilesystem keeps the secure local filesystem implementation
// at the Node composition boundary. Agent owns only the operations and values
// needed to preserve wake crash ordering.
type agentAttachmentFilesystem struct {
	nodeState string
	files     RunAttachmentFilesystem
	err       error
}

func newAgentAttachmentFilesystem(nodeState string,
	factories ...RunAttachmentFilesystemFactory,
) *agentAttachmentFilesystem {
	var factory RunAttachmentFilesystemFactory
	switch len(factories) {
	case 0:
		factory = defaultControlRuntime
	case 1:
		factory = factories[0]
	default:
		return &agentAttachmentFilesystem{nodeState: nodeState,
			err: errControlRuntimeUnavailable("run attachment filesystem")}
	}
	if factory == nil {
		return &agentAttachmentFilesystem{nodeState: nodeState,
			err: errControlRuntimeUnavailable("run attachment filesystem")}
	}
	files, err := factory.NewRunAttachmentFilesystem(nodeState)
	if err != nil {
		return &agentAttachmentFilesystem{nodeState: nodeState, err: err}
	}
	if files == nil {
		return &agentAttachmentFilesystem{nodeState: nodeState,
			err: errControlRuntimeUnavailable("run attachment filesystem")}
	}
	return &agentAttachmentFilesystem{nodeState: nodeState, files: files}
}

func (filesystem *agentAttachmentFilesystem) ListCandidates() ([]agent.WakeAttachmentCandidate, error) {
	if filesystem != nil && filesystem.err != nil {
		return nil, filesystem.err
	}
	if filesystem == nil || filesystem.files == nil {
		return nil, errControlRuntimeUnavailable("run attachment filesystem")
	}
	source, err := filesystem.files.ListCandidates()
	if err != nil {
		return nil, err
	}
	result := make([]agent.WakeAttachmentCandidate, len(source))
	for index, candidate := range source {
		result[index] = agent.WakeAttachmentCandidate{
			RunID: candidate.RunID, TokenHash: candidate.TokenHash,
		}
	}
	return result, nil
}

func (filesystem *agentAttachmentFilesystem) RemoveReapable(runID model.RunID,
	tokenHash model.Digest,
) (bool, error) {
	if filesystem != nil && filesystem.err != nil {
		return false, filesystem.err
	}
	if filesystem == nil || filesystem.files == nil {
		return false, errControlRuntimeUnavailable("run attachment filesystem")
	}
	return filesystem.files.RemoveReapable(runID, tokenHash)
}

func (filesystem *agentAttachmentFilesystem) CleanupStages(at time.Time) (int, error) {
	if filesystem != nil && filesystem.err != nil {
		return 0, filesystem.err
	}
	if filesystem == nil || filesystem.files == nil {
		return 0, errControlRuntimeUnavailable("run attachment filesystem")
	}
	return filesystem.files.CleanupStages(at)
}

func (filesystem *agentAttachmentFilesystem) Stage(random io.Reader) (agent.StagedRunAttachment, error) {
	if filesystem != nil && filesystem.err != nil {
		return nil, filesystem.err
	}
	if filesystem == nil || filesystem.files == nil {
		return nil, errControlRuntimeUnavailable("run attachment filesystem")
	}
	staged, err := filesystem.files.Stage(random)
	if err != nil {
		return nil, err
	}
	if staged == nil {
		return nil, errControlRuntimeUnavailable("run attachment stage")
	}
	return &agentAttachmentStage{filesystem: filesystem.files, staged: staged}, nil
}

type agentAttachmentStage struct {
	filesystem RunAttachmentFilesystem
	staged     StagedRunAttachment
}

func (stage *agentAttachmentStage) TokenHash() model.Digest { return stage.staged.TokenHash() }

func (stage *agentAttachmentStage) Publish(runID model.RunID) (agent.RunAttachment, error) {
	attachment, err := stage.staged.Publish(runID)
	if err != nil {
		return nil, err
	}
	if attachment == nil {
		return nil, errControlRuntimeUnavailable("run attachment")
	}
	return &agentRunAttachment{filesystem: stage.filesystem, attachment: attachment}, nil
}

func (stage *agentAttachmentStage) Discard() error { return stage.staged.Discard() }

type agentRunAttachment struct {
	filesystem RunAttachmentFilesystem
	attachment RunAttachment
}

func (attachment *agentRunAttachment) Path() string { return attachment.attachment.Path() }

func (attachment *agentRunAttachment) Remove() error {
	if attachment == nil || attachment.filesystem == nil || attachment.attachment == nil {
		return errControlRuntimeUnavailable("run attachment")
	}
	return attachment.filesystem.Remove(attachment.attachment)
}

var _ agent.WakeAttachmentFilesystem = (*agentAttachmentFilesystem)(nil)
var _ agent.StagedRunAttachment = (*agentAttachmentStage)(nil)
var _ agent.RunAttachment = (*agentRunAttachment)(nil)
