package agencycli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

const maxIntentInputBytes = agency.MaxIntentCanonicalBytes

type hookStatus struct {
	Schema  string `json:"schema"`
	Version int    `json:"version"`
	Status  string `json:"status"`
}

type captureStatus struct {
	Schema   string `json:"schema"`
	Version  int    `json:"version"`
	Handle   string `json:"handle"`
	ByteSize int64  `json:"byte_size"`
}

type agencyStatus struct {
	Schema  string `json:"schema"`
	Version int    `json:"version"`
	Status  string `json:"status"`
}

func (app *App) runAttach(ctx context.Context, store *journalStore, client agencyClient) int {
	err := store.withLock(true, func(directory *lockedJournalDirectory) error {
		var preserved []capturedBinding
		journal, err := directory.load()
		if err == nil {
			if validTerminalName(journal.fileName) || app.deps.clock().Before(journal.Attachment.ExpiresAt) {
				journal.clear()
				return nil
			}
			preserved = append([]capturedBinding(nil), journal.Candidates...)
			if err := directory.remove(journal); err != nil {
				journal.clear()
				return err
			}
			journal.clear()
		}
		if err != nil && !errors.Is(err, errJournalAbsent) {
			return err
		}
		attachment, apiErr := client.Attach(ctx)
		if apiErr != nil {
			return apiErr
		}
		defer clear(attachment.Credential)
		journal, err = newClientJournal(attachment)
		if err != nil {
			return err
		}
		journal.Candidates = preserved
		defer journal.clear()
		return directory.write(journal)
	})
	if err != nil {
		return app.writeCommandError(err)
	}
	return app.writeJSON(hookStatus{Schema: "mnemon.hook.attach", Version: 1, Status: "ready"})
}

func (app *App) runCurrent(ctx context.Context, store *journalStore, client agencyClient) int {
	var projection []byte
	err := store.withLock(false, func(directory *lockedJournalDirectory) error {
		journal, err := directory.load()
		if err != nil {
			return err
		}
		defer journal.clear()
		if validTerminalName(journal.fileName) {
			return localapi.NewAPIError(localapi.CodeOperationPending,
				"an accepted R7 receipt is awaiting presentation")
		}
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
		projection = append([]byte(nil), view...)
		return nil
	})
	if err != nil {
		return app.writeCommandError(err)
	}
	return app.writeCanonicalProjection(projection)
}

func (app *App) runCapture(ctx context.Context, store *journalStore, client agencyClient) int {
	content, apiErr := readBoundedInput(app.stdin, node.MaxAgencyArtifactBytes,
		localapi.CodeArtifactTooLarge, "Artifact input exceeds its closed byte bound")
	if apiErr != nil {
		return app.writeError(apiErr)
	}
	defer clear(content)
	var captured node.AgencyArtifactCapture
	err := store.withLock(false, func(directory *lockedJournalDirectory) error {
		journal, err := directory.load()
		if err != nil {
			return err
		}
		defer journal.clear()
		if validTerminalName(journal.fileName) {
			return localapi.NewAPIError(localapi.CodeOperationPending,
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

func (app *App) runSubmit(ctx context.Context, store *journalStore, client agencyClient) int {
	raw, apiErr := readBoundedInput(app.stdin, maxIntentInputBytes,
		localapi.CodeContentTooLarge, "Intent input exceeds its closed byte bound")
	if apiErr != nil {
		return app.writeError(apiErr)
	}
	defer clear(raw)
	intent, err := agency.ParseAgentIntentJSON(raw)
	if err != nil {
		return app.writeError(localapi.NewAPIError(localapi.CodeInvalidArgument,
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
			return localapi.NewAPIError(localapi.CodeContextRequired,
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
				return localapi.NewAPIError(localapi.CodeOperationMismatch,
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
	if err := store.withLock(false, func(directory *lockedJournalDirectory) error {
		return directory.remove(terminal)
	}); err != nil {
		// The accepted Receipt was already presented. Retaining the exact
		// terminal replay handle is safer than obscuring that accepted fact.
		return 0
	}
	return 0
}

func (app *App) runStatus(ctx context.Context, client agencyClient) int {
	status, apiErr := client.Status(ctx)
	if apiErr != nil {
		return app.writeError(apiErr)
	}
	value := "not_ready"
	if status.Ready {
		value = "ready"
	}
	return app.writeJSON(agencyStatus{Schema: "mnemon.agency.status", Version: 1, Status: value})
}

func readBoundedInput(reader io.Reader, maximum int, code localapi.ErrorCode,
	message string,
) ([]byte, *localapi.APIError) {
	if reader == nil || maximum <= 0 {
		return nil, localapi.NewAPIError(localapi.CodeInternal, "bounded stdin is unavailable")
	}
	raw, err := io.ReadAll(io.LimitReader(reader, int64(maximum)+1))
	if err != nil {
		clear(raw)
		return nil, localapi.NewAPIError(localapi.CodeInvalidArgument, "stdin cannot be read")
	}
	if len(raw) > maximum {
		clear(raw)
		return nil, localapi.NewAPIError(code, message)
	}
	if len(raw) == 0 {
		return nil, localapi.NewAPIError(localapi.CodeContentRequired, "stdin must not be empty")
	}
	return raw, nil
}

func agentReceiptOutcome(raw []byte) (string, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var wire struct {
		Schema     string `json:"schema"`
		Version    int    `json:"version"`
		Outcome    string `json:"outcome"`
		Replayed   bool   `json:"replayed"`
		Diagnostic string `json:"diagnostic,omitempty"`
	}
	if err := decoder.Decode(&wire); err != nil {
		return "", errors.New("mnemond returned an invalid AgentReceipt")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) ||
		wire.Schema != agency.AgentReceiptSchema || wire.Version != agency.AgentReceiptVersion ||
		(wire.Outcome != "accepted" && wire.Outcome != "rejected") ||
		(wire.Outcome == "accepted" && wire.Diagnostic != "") {
		return "", errors.New("mnemond returned an invalid AgentReceipt")
	}
	return wire.Outcome, nil
}

func (app *App) writeCanonicalProjection(raw []byte) int {
	if len(raw) < 2 || raw[0] != '{' || raw[len(raw)-1] != '}' {
		return app.writeError(localapi.NewAPIError(localapi.CodeInternal,
			"mnemond returned an invalid R7 projection"))
	}
	if _, err := app.stdout.Write(append(append([]byte(nil), raw...), '\n')); err != nil {
		return 1
	}
	return 0
}

func (app *App) writeJSON(value any) int {
	raw, err := model.CanonicalMarshal(value)
	if err != nil {
		return 1
	}
	return app.writeCanonicalProjection(raw)
}

func (app *App) writeCommandError(err error) int {
	var apiErr *localapi.APIError
	if errors.As(err, &apiErr) {
		return app.writeError(apiErr)
	}
	return app.writeError(clientStateError())
}

func (app *App) writeError(apiErr *localapi.APIError) int {
	if apiErr == nil {
		apiErr = localapi.NewAPIError(localapi.CodeInternal, "internal R7 Agent terminal error")
	}
	if app.writeJSON(apiErr) != 0 {
		return 1
	}
	return apiErr.ExitStatus()
}
