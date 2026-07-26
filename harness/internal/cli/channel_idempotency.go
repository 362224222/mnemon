package cli

import (
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func (app *channelApp) beginChannelMutation(nodeState string,
	requestDigest model.Digest,
) (journalStore, localapi.PendingJournal, *localapi.APIError) {
	journals, err := app.deps.newJournals(nodeState)
	if err != nil {
		return nil, localapi.PendingJournal{}, localapi.NewAPIError(localapi.CodeInternal,
			"pending Channel mutation journal is unavailable")
	}
	pending, _, err := journals.FindOrCreate(requestDigest, nil)
	if err != nil {
		return nil, localapi.PendingJournal{}, localapi.NewAPIError(localapi.CodeOperationMismatch,
			"pending Channel mutation journal is invalid")
	}
	return journals, pending, nil
}

func (app *channelApp) presentChannelMutation(journals journalStore,
	pending localapi.PendingJournal, jsonOutput bool, response any, writeText func() int,
) int {
	terminal, err := journals.MarkTerminal(pending)
	if err != nil {
		return app.writeError(localapi.NewAPIError(localapi.CodeInternal,
			"terminal Channel mutation journal could not be persisted"))
	}
	exit := 0
	if jsonOutput {
		exit = app.writeJSON(response)
	} else {
		exit = writeText()
	}
	if exit != 0 {
		return exit
	}
	if _, err := journals.MarkPresented(terminal); err != nil {
		// The validated token response is already visible. Preserve that result
		// and leave the terminal handle for exact replay/repair.
		return 0
	}
	return 0
}
