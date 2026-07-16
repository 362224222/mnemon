package localapi

import "github.com/mnemon-dev/mnemon/harness/internal/model"

// MaxShutdownResponseBytes bounds both the fixed success envelope and the
// existing API error union. Shutdown carries no daemon state or identity.
const MaxShutdownResponseBytes = 1024

// LifecycleFunc receives an authenticated request after its success response
// has been written. It must be nonblocking; Server invokes it at most once even
// when several authenticated shutdown requests race. A concrete function type
// lets construction reject nil without permitting a typed-nil provider to
// acknowledge a shutdown it cannot deliver.
type LifecycleFunc func()

// ShutdownResponse is the entire successful shutdown protocol. Reaching this
// closed response is sufficient confirmation; no PID or process state crosses
// the owner-only API.
type ShutdownResponse struct {
	SchemaVersion int    `json:"schema_version"`
	Status        string `json:"status"`
}

func newShutdownResponse() ShutdownResponse {
	return ShutdownResponse{SchemaVersion: SchemaVersion, Status: "stopping"}
}

func validateShutdownResponse(response ShutdownResponse) *APIError {
	if response != newShutdownResponse() {
		return invalidControlResponse("shutdown response has an invalid lifecycle state")
	}
	raw, err := model.CanonicalMarshal(response)
	if err != nil || len(raw)+1 > MaxShutdownResponseBytes {
		return invalidControlResponse("shutdown response exceeds its closed bound")
	}
	return nil
}
