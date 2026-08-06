package observer

import "testing"

func TestRuntimeObservationEvidenceIsClosed(t *testing.T) {
	attempts, successes, count := 2, 1, 1
	domain := Fact{Kind: "runtime.domain.operation", Fields: FactFields{
		Action: "mutation", AttemptCount: &attempts, SuccessCount: &successes,
	}}
	if err := validateKindEvidence(domain, 1); err != nil {
		t.Fatalf("valid domain observation: %v", err)
	}
	domain.Causes = []string{"trace:not-allowed"}
	if err := validateKindEvidence(domain, 1); err == nil {
		t.Fatal("domain observation accepted a causal edge")
	}
	domain.Causes = nil
	domain.Fields.SuccessCount = &attempts
	zero := 0
	domain.Fields.AttemptCount = &zero
	if err := validateKindEvidence(domain, 1); err == nil {
		t.Fatal("domain observation accepted successes without attempts")
	}

	denial := Fact{Kind: "runtime.intent.denied", Fields: FactFields{
		Action: "submit", Code: "authentication_failed", Count: &count,
	}}
	if err := validateKindEvidence(denial, 2); err != nil {
		t.Fatalf("valid Intent denial: %v", err)
	}
	denial.Fields.Code = "provider-prose"
	if err := validateKindEvidence(denial, 2); err == nil {
		t.Fatal("Intent denial accepted an open diagnostic class")
	}
}
