package localapi

import (
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func validateOperationResponse(response OperationResponse, expectedAction string) *APIError {
	if !validOperationResponseEnvelope(response, expectedAction) {
		return invalidControlResponse("operation response has an invalid envelope")
	}
	if _, err := model.ParseOperationID(response.OperationID); err != nil {
		return invalidControlResponse("operation response has an invalid identity")
	}
	if !operationResponseStatusMatches(response.Status, expectedAction) {
		return invalidControlResponse("operation response status does not match its action")
	}
	if response.Status == "resolved" && len(response.Results) != 0 {
		return invalidControlResponse("handling resolution returned domain results")
	}
	for _, result := range response.Results {
		if !validOperationResult(result) {
			return invalidControlResponse("operation response contains an invalid result")
		}
	}
	if response.Handling != nil && !validHandlingStatus(response.Handling.Status) {
		return invalidControlResponse("operation response has an invalid handling receipt")
	}
	if response.Status == "resolved" && response.Handling == nil {
		return invalidControlResponse("handling resolution lacks a handling receipt")
	}
	return nil
}

func validOperationResponseEnvelope(response OperationResponse, expectedAction string) bool {
	return response.SchemaVersion == SchemaVersion && response.Action == expectedAction &&
		(response.Status == "accepted" || response.Status == "resolved") &&
		response.OperationID != "" && response.Receipt != "" &&
		len(response.Receipt) <= MaxDiagnosticBytes && response.Results != nil &&
		len(response.Results) <= model.MaxChildWorks
}

func operationResponseStatusMatches(status, action string) bool {
	return strings.HasPrefix(action, "teamwork.") == (status == "accepted") &&
		strings.HasPrefix(action, "agent.resolve.") == (status == "resolved")
}

func validOperationResult(result OperationResult) bool {
	if _, err := model.ParseEventID(result.EventID); err != nil {
		return false
	}
	return result.EventType != "" && result.Work.Ref != "" && result.Work.Version != 0 &&
		model.WorkState(result.Work.State).Valid()
}

func validHandlingStatus(status string) bool {
	return status == "completed" || status == "requeued" || status == "rejected"
}
