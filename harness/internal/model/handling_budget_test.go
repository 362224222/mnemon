package model

import (
	"errors"
	"testing"
)

func TestHandlingBudgetDefaultRoundTripIsComplete(t *testing.T) {
	t.Parallel()
	budget := DefaultHandlingBudget()
	parsed, err := ParseHandlingBudget(budget.JSON())
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Spec() != budget.Spec() || parsed.Spec().MaxConcurrency != 1 ||
		parsed.Spec().MaxCurrentJSONBytes != 256<<10 || parsed.Spec().MaxCurrentArtifactRefs != 112 {
		t.Fatalf("default budget round trip = %#v", parsed.Spec())
	}
}

func TestHandlingBudgetRejectsMissingUnknownAndOutOfRangeFields(t *testing.T) {
	t.Parallel()
	tests := []string{
		`{}`,
		`{"claim_lease_seconds":300,"max_attempts":3,"max_concurrency":2,"max_current_artifact_refs":112,"max_current_json_bytes":262144,"max_current_path_bytes":512,"retry_initial_seconds":5,"retry_max_seconds":300}`,
		`{"claim_lease_seconds":300,"extra":1,"max_attempts":3,"max_concurrency":1,"max_current_artifact_refs":112,"max_current_json_bytes":262144,"max_current_path_bytes":512,"retry_initial_seconds":5,"retry_max_seconds":300}`,
		`{"claim_lease_seconds":300,"max_attempts":3,"max_concurrency":1,"max_current_artifact_refs":113,"max_current_json_bytes":262144,"max_current_path_bytes":512,"retry_initial_seconds":5,"retry_max_seconds":300}`,
		`{"claim_lease_seconds":300,"max_attempts":3,"max_concurrency":1,"max_current_artifact_refs":112,"max_current_json_bytes":262144,"max_current_path_bytes":512,"retry_initial_seconds":301,"retry_max_seconds":300}`,
	}
	for _, raw := range tests {
		value, err := NewJSON([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ParseHandlingBudget(value); err == nil {
			t.Fatalf("ParseHandlingBudget(%s) accepted invalid budget", raw)
		}
	}
	bad, _ := NewHandlingBudget(HandlingBudgetSpec{})
	if !bad.JSON().IsZero() {
		t.Fatal("invalid budget returned a value")
	}
	if _, err := NewHandlingBudget(HandlingBudgetSpec{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("NewHandlingBudget() error = %v", err)
	}
}
