package model

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

const (
	DefaultClaimLeaseSeconds   = 300
	DefaultHandlingMaxAttempts = 3
	DefaultRetryInitialSeconds = 5
	DefaultRetryMaxSeconds     = 300
	DefaultCurrentJSONBytes    = 256 << 10
	DefaultCurrentArtifactRefs = MaxCurrentArtifactRefs
	DefaultCurrentPathBytes    = 512
)

// HandlingBudgetSpec is the complete T0 controller budget. There are no
// implicit defaults in persisted JSON; setup writes every field explicitly.
type HandlingBudgetSpec struct {
	MaxConcurrency         int `json:"max_concurrency"`
	ClaimLeaseSeconds      int `json:"claim_lease_seconds"`
	MaxAttempts            int `json:"max_attempts"`
	RetryInitialSeconds    int `json:"retry_initial_seconds"`
	RetryMaxSeconds        int `json:"retry_max_seconds"`
	MaxCurrentJSONBytes    int `json:"max_current_json_bytes"`
	MaxCurrentArtifactRefs int `json:"max_current_artifact_refs"`
	MaxCurrentPathBytes    int `json:"max_current_path_bytes"`
}

type HandlingBudget struct {
	spec      HandlingBudgetSpec
	canonical JSON
}

func DefaultHandlingBudget() HandlingBudget {
	budget, err := NewHandlingBudget(HandlingBudgetSpec{
		MaxConcurrency: 1, ClaimLeaseSeconds: DefaultClaimLeaseSeconds,
		MaxAttempts: DefaultHandlingMaxAttempts, RetryInitialSeconds: DefaultRetryInitialSeconds,
		RetryMaxSeconds: DefaultRetryMaxSeconds, MaxCurrentJSONBytes: DefaultCurrentJSONBytes,
		MaxCurrentArtifactRefs: DefaultCurrentArtifactRefs, MaxCurrentPathBytes: DefaultCurrentPathBytes,
	})
	if err != nil {
		panic(err)
	}
	return budget
}

func NewHandlingBudget(spec HandlingBudgetSpec) (HandlingBudget, error) {
	if err := validateHandlingBudget(spec); err != nil {
		return HandlingBudget{}, err
	}
	canonical, err := JSONFrom(spec)
	if err != nil {
		return HandlingBudget{}, fmt.Errorf("handling budget: %w", err)
	}
	return HandlingBudget{spec: spec, canonical: canonical}, nil
}

func ParseHandlingBudget(value JSON) (HandlingBudget, error) {
	if value.IsZero() || len(value.raw) == 0 || value.raw[0] != '{' {
		return HandlingBudget{}, invalid("handling_budget", "must be a canonical JSON object")
	}
	decoder := json.NewDecoder(bytes.NewReader(value.Bytes()))
	decoder.DisallowUnknownFields()
	var spec HandlingBudgetSpec
	if err := decoder.Decode(&spec); err != nil {
		return HandlingBudget{}, fmt.Errorf("handling budget: %w", err)
	}
	if decoder.More() {
		return HandlingBudget{}, errors.New("handling budget: trailing value")
	}
	return NewHandlingBudget(spec)
}

func validateHandlingBudget(spec HandlingBudgetSpec) error {
	if spec.MaxConcurrency != 1 {
		return invalid("handling_budget.max_concurrency", "T0 requires exactly 1")
	}
	if spec.ClaimLeaseSeconds < 30 || spec.ClaimLeaseSeconds > 3600 {
		return invalid("handling_budget.claim_lease_seconds", "must be between 30 and 3600")
	}
	if spec.MaxAttempts < 1 || spec.MaxAttempts > 10 {
		return invalid("handling_budget.max_attempts", "must be between 1 and 10")
	}
	if spec.RetryInitialSeconds < 1 || spec.RetryInitialSeconds > 300 ||
		spec.RetryMaxSeconds < spec.RetryInitialSeconds || spec.RetryMaxSeconds > 3600 {
		return invalid("handling_budget.retry", "initial/max backoff is out of range")
	}
	if spec.MaxCurrentJSONBytes < 1024 || spec.MaxCurrentJSONBytes > DefaultCurrentJSONBytes {
		return invalid("handling_budget.max_current_json_bytes", "must be between 1024 and 262144")
	}
	if spec.MaxCurrentArtifactRefs < 1 || spec.MaxCurrentArtifactRefs > MaxCurrentArtifactRefs {
		return invalid("handling_budget.max_current_artifact_refs", "must be between 1 and 112")
	}
	if spec.MaxCurrentPathBytes < 64 || spec.MaxCurrentPathBytes > DefaultCurrentPathBytes {
		return invalid("handling_budget.max_current_path_bytes", "must be between 64 and 512")
	}
	return nil
}

func (b HandlingBudget) Spec() HandlingBudgetSpec { return b.spec }
func (b HandlingBudget) JSON() JSON               { return b.canonical }
