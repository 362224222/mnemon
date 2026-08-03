package cli

import (
	"context"
	"errors"
	"io"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

const maxIntentInputBytes = agency.MaxIntentCanonicalBytes

type captureStatus struct {
	ByteSize int64  `json:"byte_size"`
	Handle   string `json:"handle"`
	Schema   string `json:"schema"`
	Version  int    `json:"version"`
}

func (app *App) runCurrent(ctx context.Context, store *journalStore, client agencyClient) int {
	var projection []byte
	err := store.withLock(false, func(directory *lockedJournalDirectory) error {
		journal, err := directory.load()
		if err != nil {
			return err
		}
		if validTerminalName(journal.fileName) {
			if !journal.CurrentOperation.IsZero() {
				journal.clear()
				return newControlError(codeOperationPending,
					"an accepted R7 receipt is awaiting presentation")
			}
			active, err := directory.activatePresented(journal)
			journal.clear()
			if err != nil {
				return err
			}
			journal = active
		}
		defer journal.clear()
		if journal.CurrentOperation.IsZero() {
			operation, err := newCurrentOperation(app.deps.random)
			if err != nil {
				return err
			}
			journal.CurrentOperation = operation
			if err := directory.write(journal); err != nil {
				return err
			}
		}
		view, apiErr := client.Current(ctx, journal.Attachment, journal.CurrentOperation.String())
		if apiErr != nil {
			return apiErr
		}
		currentProjection, err := classifyCurrentProjection(view)
		if err != nil {
			return err
		}
		journal.CurrentProjection = currentProjection
		if err := directory.write(journal); err != nil {
			return err
		}
		projection = append([]byte(nil), view...)
		return nil
	})
	if err != nil {
		return app.writeCommandError(err)
	}
	return app.writeCanonicalProjection(projection)
}

func classifyCurrentProjection(view []byte) (string, error) {
	hasCurrent, err := agency.AgentViewProjectionHasCurrent(view)
	if err != nil {
		return "", errors.New("R7 Current response is invalid")
	}
	if !hasCurrent {
		return currentProjectionEmpty, nil
	}
	return currentProjectionSubject, nil
}

func (app *App) runCapture(ctx context.Context, store *journalStore, client agencyClient) int {
	content, apiErr := readBoundedInput(app.stdin, maxArtifactInputBytes,
		codeArtifactTooLarge, "Artifact input exceeds its closed byte bound")
	if apiErr != nil {
		return app.writeError(apiErr)
	}
	defer clear(content)
	var captured artifactCapture
	err := store.withLock(false, func(directory *lockedJournalDirectory) error {
		journal, err := directory.load()
		if err != nil {
			return err
		}
		defer journal.clear()
		if validTerminalName(journal.fileName) {
			return newControlError(codeOperationPending,
				"an accepted R7 receipt is awaiting presentation")
		}
		capture, apiErr := client.Capture(ctx, content)
		if apiErr != nil {
			return apiErr
		}
		if err := journal.addCandidate(capture); err != nil {
			return err
		}
		if err := directory.write(journal); err != nil {
			return err
		}
		captured = capture
		return nil
	})
	if err != nil {
		return app.writeCommandError(err)
	}
	return app.writeJSON(captureStatus{Schema: "mnemon.artifact.capture", Version: 1,
		Handle: captured.Handle, ByteSize: captured.ByteSize})
}

func (app *App) runReadArtifact(ctx context.Context, store *journalStore, client agencyClient,
	handle string,
) int {
	var content []byte
	err := store.withLock(false, func(directory *lockedJournalDirectory) error {
		journal, err := directory.load()
		if err != nil {
			return err
		}
		defer journal.clear()
		if validTerminalName(journal.fileName) {
			return newControlError(codeOperationPending,
				"an accepted R7 receipt is awaiting presentation")
		}
		if journal.CurrentOperation.IsZero() {
			return newControlError(codeContextRequired,
				"agent current must establish a bounded View before Artifact read")
		}
		read, apiErr := client.ReadArtifact(ctx, journal.Attachment,
			journal.CurrentOperation.String(), handle)
		if apiErr != nil {
			return apiErr
		}
		content = append([]byte(nil), read...)
		clear(read)
		return nil
	})
	if err != nil {
		clear(content)
		return app.writeCommandError(err)
	}
	defer clear(content)
	if _, err := app.stdout.Write(content); err != nil {
		return 1
	}
	return 0
}

func (app *App) runSubmit(ctx context.Context, store *journalStore, client agencyClient) int {
	raw, apiErr := readBoundedInput(app.stdin, maxIntentInputBytes,
		codeContentTooLarge, "Intent input exceeds its closed byte bound")
	if apiErr != nil {
		return app.writeError(apiErr)
	}
	defer clear(raw)
	intent, err := agency.ParseAgentIntentJSON(raw)
	if err != nil {
		return app.writeError(newControlError(codeInvalidArgument,
			"Intent input is not one canonical AgentIntent"))
	}
	var receipt []byte
	var terminal clientJournal
	err = store.withLock(false, func(directory *lockedJournalDirectory) error {
		journal, err := directory.load()
		if err != nil {
			return err
		}
		defer journal.clear()
		if journal.CurrentOperation.IsZero() {
			return newControlError(codeContextRequired,
				"agent current must establish a bounded View before submit")
		}
		candidates, err := journal.bindCandidates(intent)
		if err != nil {
			return err
		}
		operation, err := deriveAdmissionOperation(journal.CurrentOperation, intent, candidates)
		if err != nil {
			return err
		}
		if validTerminalName(journal.fileName) {
			expected, err := terminalOperation(journal.fileName)
			if err != nil || expected != operation {
				return newControlError(codeOperationMismatch,
					"terminal R7 replay requires the exact prior Intent")
			}
		}
		projected, apiErr := client.Submit(ctx, journal.Attachment, journal.CurrentOperation.String(),
			operation.String(), intent.CanonicalJSON(), candidates)
		if apiErr != nil {
			return apiErr
		}
		outcome, err := agentReceiptOutcome(projected)
		if err != nil {
			return err
		}
		if validTerminalName(journal.fileName) && outcome != "accepted" {
			return errors.New("terminal R7 replay returned a non-accepted receipt")
		}
		if outcome == "accepted" && journal.fileName == journalActiveName {
			terminal, err = directory.markTerminal(journal, operation)
			if err != nil {
				return err
			}
		} else if validTerminalName(journal.fileName) {
			terminal = journal
			terminal.Attachment.Credential = append([]byte(nil), journal.Attachment.Credential...)
		}
		receipt = append([]byte(nil), projected...)
		return nil
	})
	if err != nil {
		terminal.clear()
		return app.writeCommandError(err)
	}
	if app.writeCanonicalProjection(receipt) != 0 {
		terminal.clear()
		return 1
	}
	if terminal.fileName == "" {
		return 0 // Rejected receipts retain the same View for an amended Intent.
	}
	defer terminal.clear()
	app.finishPresentedReceipt(ctx, store, client, intent, terminal)
	return 0
}

func (app *App) finishPresentedReceipt(ctx context.Context, store *journalStore,
	client agencyClient, intent agency.AgentIntent, terminal clientJournal,
) {
	var presented clientJournal
	if err := store.withLock(false, func(directory *lockedJournalDirectory) error {
		var err error
		presented, err = directory.markPresented(terminal)
		return err
	}); err != nil {
		// The Receipt reached stdout, but the durable presentation transition
		// did not. The exact terminal operation remains replayable.
		presented.clear()
		return
	}
	defer presented.clear()
	if referenceConsequence(intent.Consequence()) {
		if err := store.withLock(false, func(directory *lockedJournalDirectory) error {
			active, err := directory.activatePresented(presented)
			active.clear()
			return err
		}); err != nil {
			// The Receipt was already presented. The presented terminal phase
			// remains recoverable without replaying the prior Intent.
			return
		}
		return
	}
	if apiErr := client.End(ctx, presented.Attachment); apiErr != nil {
		// The accepted Receipt was already presented. The terminal journal
		// preserves an idempotent End retry without retaining the Intent.
		return
	}
	if err := store.withLock(false, func(directory *lockedJournalDirectory) error {
		return directory.remove(presented)
	}); err != nil {
		// End is durable. A later boundary or Hook end may idempotently replay
		// End and remove the presented terminal phase.
		return
	}
}

func referenceConsequence(consequence agency.Consequence) bool {
	return consequence == agency.ConsequencePublishReference ||
		consequence == agency.ConsequenceSupersedeReference ||
		consequence == agency.ConsequenceRetractReference
}

func readBoundedInput(reader io.Reader, maximum int, code controlErrorCode,
	message string,
) ([]byte, *controlError) {
	if reader == nil || maximum <= 0 {
		return nil, newControlError(codeInternal, "bounded stdin is unavailable")
	}
	raw, err := io.ReadAll(io.LimitReader(reader, int64(maximum)+1))
	if err != nil {
		clear(raw)
		return nil, newControlError(codeInvalidArgument, "stdin cannot be read")
	}
	if len(raw) > maximum {
		clear(raw)
		return nil, newControlError(code, message)
	}
	if len(raw) == 0 {
		return nil, newControlError(codeContentRequired, "stdin must not be empty")
	}
	return raw, nil
}

func agentReceiptOutcome(raw []byte) (string, error) {
	receipt, err := agency.ParseAgentReceiptProjectionCanonicalJSON(raw)
	if err != nil {
		return "", errors.New("mnemond returned an invalid AgentReceipt")
	}
	return receipt.Outcome().String(), nil
}

func (app *App) writeCanonicalProjection(raw []byte) int {
	if len(raw) < 2 || raw[0] != '{' || raw[len(raw)-1] != '}' {
		return app.writeError(newControlError(codeInternal,
			"mnemond returned an invalid R7 projection"))
	}
	if _, err := app.stdout.Write(append(append([]byte(nil), raw...), '\n')); err != nil {
		return 1
	}
	return 0
}

func (app *App) writeJSON(value any) int {
	raw, err := marshalClosedJSON(value)
	if err != nil {
		return 1
	}
	return app.writeCanonicalProjection(raw)
}

func (app *App) writeCommandError(err error) int {
	var apiErr *controlError
	if errors.As(err, &apiErr) {
		return app.writeError(apiErr)
	}
	return app.writeError(clientStateError())
}

func (app *App) writeError(apiErr *controlError) int {
	if apiErr == nil {
		apiErr = newControlError(codeInternal, "internal R7 Agent terminal error")
	}
	if app.writeJSON(apiErr) != 0 {
		return 1
	}
	return apiErr.exitStatus()
}
