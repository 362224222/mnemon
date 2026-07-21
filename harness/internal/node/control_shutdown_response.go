package node

import "github.com/mnemon-dev/mnemon/harness/internal/model"

func newShutdownResponse(authorityDigest model.Digest) ShutdownResponse {
	return ShutdownResponse{AuthorityDigest: authorityDigest.String(),
		SchemaVersion: SchemaVersion, Status: "stopping"}
}
