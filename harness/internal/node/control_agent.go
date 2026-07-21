package node

import (
	"context"
	"encoding/json"
)

type Service interface {
	HookCheck(context.Context, RequestMetadata, HookCheckRequest) (HookCheckResponse, *APIError)
	AgentCurrent(context.Context, RequestMetadata, AgentCurrentRequest) (AgentCurrentResponse, *APIError)
	TeamworkAction(context.Context, RequestMetadata, TeamworkActionRequest) (OperationResponse, *APIError)
	AgentResolve(context.Context, RequestMetadata, AgentResolveRequest) (OperationResponse, *APIError)
}

type HookCheckRequest struct{}

type HookCheckResponse struct {
	SchemaVersion int  `json:"schema_version"`
	Pending       bool `json:"pending"`
}

type AgentCurrentRequest struct{}

// AgentCurrentResponse is the private server-to-client envelope. ClaimSecret
// and RunID are consumed by mnemon-harness and must never be copied into the
// Agent-visible projection.
type AgentCurrentResponse struct {
	SchemaVersion int             `json:"schema_version"`
	Status        string          `json:"status"`
	RunID         string          `json:"run_id,omitempty"`
	ClaimSecret   string          `json:"claim_secret,omitempty"`
	Projection    json.RawMessage `json:"projection,omitempty"`
}

type InitiationParticipant struct {
	EffectiveAlias string `json:"effective_alias"`
	Eligible       bool   `json:"eligible"`
	Reachable      bool   `json:"reachable"`
}

type InitiationChannel struct {
	AllowTeam    bool                    `json:"allow_team"`
	LocalAlias   string                  `json:"local_alias"`
	Participants []InitiationParticipant `json:"participants"`
}

type InitiationProjection struct {
	InitiationContext struct {
		Channels []InitiationChannel `json:"channels"`
	} `json:"initiation_context"`
	SchemaVersion int `json:"schema_version"`
}

type TeamworkActionRequest struct {
	Action    string   `json:"action"`
	Channel   string   `json:"channel,omitempty"`
	To        string   `json:"to,omitempty"`
	Deadline  string   `json:"deadline,omitempty"`
	Content   string   `json:"content,omitempty"`
	Artifacts []string `json:"artifacts,omitempty"`
}

type AgentResolveRequest struct {
	Decision string `json:"decision"`
	Content  string `json:"content,omitempty"`
}

type OperationResult struct {
	EventID   string      `json:"event_id"`
	EventType string      `json:"event_type"`
	Work      WorkReceipt `json:"work"`
}

type WorkReceipt struct {
	Ref     string `json:"ref"`
	Version uint64 `json:"version"`
	State   string `json:"state"`
}

type HandlingReceipt struct {
	Status string `json:"status"`
}

type OperationResponse struct {
	SchemaVersion int               `json:"schema_version"`
	Status        string            `json:"status"`
	Action        string            `json:"action"`
	OperationID   string            `json:"operation_id"`
	Replayed      bool              `json:"replayed"`
	Handling      *HandlingReceipt  `json:"handling"`
	Results       []OperationResult `json:"results"`
	Receipt       string            `json:"receipt"`
}
