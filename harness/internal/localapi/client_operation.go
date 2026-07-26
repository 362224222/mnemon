package localapi

import (
	"crypto/subtle"
	"os"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type clientHeaders struct {
	operation  string
	claim      string
	attachment string
}

func (c *Client) operationHeaders(request any, contextFile *ContextFile,
	journal PendingJournal,
) (clientHeaders, *APIError) {
	if c == nil {
		return clientHeaders{}, invalidControlResponse("local control client is unavailable")
	}
	body, err := model.CanonicalMarshal(request)
	if err != nil || len(body) == 0 || body[0] != '{' {
		return clientHeaders{}, NewAPIError(CodeInvalidArgument, "request cannot be encoded canonically")
	}
	if journal.requestDigest != model.Sum(body) {
		return clientHeaders{}, NewAPIError(CodeOperationMismatch, "pending operation does not match request")
	}
	headers, currentJournal, apiErr := c.pendingOperationHeaders(journal.requestDigest, journal)
	if apiErr != nil {
		return clientHeaders{}, apiErr
	}
	if contextFile == nil {
		if currentJournal.hasContext {
			return clientHeaders{}, NewAPIError(CodeOperationMismatch, "pending operation expects managed context")
		}
		return headers, nil
	}
	verified, err := ReadContextFile(c.nodeState, contextFile.path)
	if err != nil || contextFile.identity == nil || !os.SameFile(verified.identity, contextFile.identity) ||
		verified.runID != contextFile.runID || verified.digest != contextFile.digest ||
		subtle.ConstantTimeCompare(verified.token[:], contextFile.token[:]) != 1 {
		return clientHeaders{}, NewAPIError(CodeContextInvalid, "managed context identity is invalid")
	}
	if !currentJournal.hasContext || currentJournal.contextFileDigest != verified.digest {
		return clientHeaders{}, NewAPIError(CodeOperationMismatch, "pending operation context does not match")
	}
	headers.claim = verified.HeaderValue()
	return headers, nil
}

func (c *Client) pendingOperationHeaders(requestDigest model.Digest,
	journal PendingJournal,
) (clientHeaders, PendingJournal, *APIError) {
	if c == nil || requestDigest.IsZero() || journal.requestDigest != requestDigest {
		return clientHeaders{}, PendingJournal{},
			NewAPIError(CodeOperationMismatch, "pending operation does not match request")
	}
	currentJournal, err := c.readExpectedJournal(journal)
	if err != nil {
		return clientHeaders{}, PendingJournal{},
			NewAPIError(CodeOperationMismatch, "pending operation identity is invalid")
	}
	if currentJournal.requestDigest != journal.requestDigest ||
		subtle.ConstantTimeCompare(currentJournal.operationKey[:], journal.operationKey[:]) != 1 {
		return clientHeaders{}, PendingJournal{},
			NewAPIError(CodeOperationMismatch, "pending operation identity changed")
	}
	headers := clientHeaders{operation: currentJournal.OperationKeyHeader()}
	return headers, currentJournal, nil
}

func (c *Client) readExpectedJournal(expected PendingJournal) (PendingJournal, error) {
	if expected.identity == nil || expected.path == "" || expected.requestDigest.IsZero() {
		return PendingJournal{}, ErrUnsafeClientState
	}
	operations, uid, err := requireOwnerSubdirectory(c.nodeState, "operations")
	if err != nil || uid != c.ownerUID {
		return PendingJournal{}, ErrUnsafeClientState
	}
	if _, err := parsePendingJournalPath(operations, expected.path); err != nil {
		return PendingJournal{}, err
	}
	var current PendingJournal
	err = withOwnerDirectoryLock(operations, c.ownerUID, func() error {
		var readErr error
		current, readErr = readPendingJournalFile(operations, expected.path, c.ownerUID)
		return readErr
	})
	if err != nil || !os.SameFile(current.identity, expected.identity) ||
		current.fileDigest != expected.fileDigest || current.createdAt != expected.createdAt ||
		current.requestDigest != expected.requestDigest ||
		!sameContextDigest(current, expected.contextFileDigest, expected.hasContext) ||
		subtle.ConstantTimeCompare(current.operationKey[:], expected.operationKey[:]) != 1 {
		return PendingJournal{}, ErrUnsafeClientState
	}
	return current, nil
}
