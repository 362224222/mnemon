package cli

import (
	"context"
	"crypto/subtle"
	"errors"

	"github.com/mnemon-dev/mnemon/internal/agency"
)

type hookStatus struct {
	Schema  string `json:"schema"`
	Status  string `json:"status"`
	Version int    `json:"version"`
}

func (app *App) runAttach(ctx context.Context, store *journalStore, client agencyClient,
	boundary agency.Digest,
) int {
	err := store.withLock(true, func(directory *lockedJournalDirectory) error {
		preserved, attach, err := app.reconcileAttachBoundary(ctx, directory, client, boundary)
		if err != nil || !attach {
			return err
		}
		return app.issueAttachBoundary(ctx, directory, client, boundary, preserved)
	})
	if err != nil {
		return app.writeCommandError(err)
	}
	return app.writeJSON(hookStatus{Schema: "mnemon.hook.attach", Version: 1, Status: "ready"})
}

func (app *App) reconcileAttachBoundary(ctx context.Context, directory *lockedJournalDirectory,
	client agencyClient, boundary agency.Digest,
) ([]capturedBinding, bool, error) {
	journal, err := directory.load()
	if errors.Is(err, errJournalAbsent) {
		return nil, true, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer journal.clear()
	if journal.BoundaryDigest == boundary {
		return nil, false, app.reconcileSameAttachBoundary(ctx, directory, client, journal)
	}
	return app.retirePredecessorBoundary(ctx, directory, client, journal)
}

func (app *App) reconcileSameAttachBoundary(ctx context.Context,
	directory *lockedJournalDirectory, client agencyClient, journal clientJournal,
) error {
	if validTerminalName(journal.fileName) && journal.CurrentOperation.IsZero() {
		if apiErr := client.End(ctx, journal.Attachment); apiErr != nil {
			return apiErr
		}
		if err := directory.remove(journal); err != nil {
			return err
		}
		return newControlError(codeContextStale, "Hook boundary already completed")
	}
	replayed, apiErr := client.Attach(ctx, journal.BoundaryDigest)
	if apiErr != nil {
		return apiErr
	}
	defer clear(replayed.Credential)
	if !sameAttachmentProof(journal.Attachment, replayed) {
		return newControlError(codeAuthenticationFailed,
			"authority attachment replay diverged from the private journal")
	}
	if !app.deps.clock().Before(replayed.ExpiresAt) {
		return newControlError(codeContextStale, "Hook boundary attachment expired")
	}
	return nil
}

func sameAttachmentProof(left, right attachment) bool {
	return left.ID == right.ID && left.ExpiresAt.Equal(right.ExpiresAt) &&
		len(left.Credential) == journalCredentialBytes &&
		len(right.Credential) == journalCredentialBytes &&
		subtle.ConstantTimeCompare(left.Credential, right.Credential) == 1
}

func (app *App) retirePredecessorBoundary(ctx context.Context,
	directory *lockedJournalDirectory, client agencyClient, journal clientJournal,
) ([]capturedBinding, bool, error) {
	terminal := validTerminalName(journal.fileName)
	if terminal && !journal.CurrentOperation.IsZero() {
		return nil, false, newControlError(codeOperationPending,
			"an accepted R7 receipt is awaiting presentation")
	}
	var preserved []capturedBinding
	if !terminal {
		preserved = append([]capturedBinding(nil), journal.Candidates...)
	}
	if apiErr := client.End(ctx, journal.Attachment); apiErr != nil {
		return nil, false, apiErr
	}
	if terminal {
		if err := directory.remove(journal); err != nil {
			return nil, false, err
		}
	}
	return preserved, true, nil
}

func (app *App) issueAttachBoundary(ctx context.Context, directory *lockedJournalDirectory,
	client agencyClient, boundary agency.Digest, preserved []capturedBinding,
) error {
	attachment, apiErr := client.Attach(ctx, boundary)
	if apiErr != nil {
		return apiErr
	}
	defer clear(attachment.Credential)
	if !app.deps.clock().Before(attachment.ExpiresAt) {
		return newControlError(codeContextStale,
			"Hook boundary attachment expired before local journal commit")
	}
	journal, err := newClientJournal(attachment, boundary)
	if err != nil {
		return err
	}
	journal.Candidates = preserved
	defer journal.clear()
	return directory.write(journal)
}

func (app *App) runEnd(ctx context.Context, store *journalStore, client agencyClient,
	boundary agency.Digest,
) int {
	err := store.withLock(false, func(directory *lockedJournalDirectory) error {
		journal, err := directory.load()
		if errors.Is(err, errJournalAbsent) {
			return nil
		}
		if err != nil {
			return err
		}
		defer journal.clear()
		if journal.BoundaryDigest != boundary {
			return nil
		}
		if validTerminalName(journal.fileName) && !journal.CurrentOperation.IsZero() {
			return newControlError(codeOperationPending,
				"an accepted R7 receipt still requires exact presentation replay")
		}
		if apiErr := client.End(ctx, journal.Attachment); apiErr != nil {
			return apiErr
		}
		return directory.remove(journal)
	})
	if errors.Is(err, errJournalAbsent) {
		err = nil
	}
	if err != nil {
		return app.writeCommandError(err)
	}
	return app.writeJSON(hookStatus{Schema: "mnemon.hook.end", Version: 1, Status: "ended"})
}
