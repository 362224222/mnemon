package store

import "github.com/mnemon-dev/mnemon/harness/internal/model"

type acceptanceArtifactAuthority struct {
	operationID model.OperationID
	runID       model.RunID
	capture     []captureRoot
	roots       map[model.Digest]model.Digest
}

func newAcceptanceArtifactAuthority(operation model.Operation,
	capture []captureRoot,
) acceptanceArtifactAuthority {
	authority := acceptanceArtifactAuthority{
		operationID: operation.ID(),
		runID:       operation.AgentRunID(),
		capture:     append([]captureRoot(nil), capture...),
		roots:       make(map[model.Digest]model.Digest, len(capture)),
	}
	for _, root := range capture {
		authority.roots[root.RootDigest] = root.ManifestDigest
	}
	return authority
}

func (authority acceptanceArtifactAuthority) allows(operation model.OperationID,
	run model.RunID, root model.Digest,
) (model.Digest, bool) {
	manifest, ok := authority.roots[root]
	return manifest, ok && !operation.IsZero() && operation == authority.operationID &&
		!run.IsZero() && run == authority.runID
}
