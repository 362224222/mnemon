package model

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestOperationRejectionReceiptRoundTripsCanonicalEvidence(t *testing.T) {
	t.Parallel()

	operationID, _ := ParseOperationID("operation-rejected")
	receipt, err := NewOperationRejectionReceipt(OperationRejectionSpec{
		Code: "work_expired", Message: "工作期限已过", OperationID: operationID})
	if err != nil {
		t.Fatalf("NewOperationRejectionReceipt() error = %v", err)
	}
	want := `{"code":"work_expired","message":"工作期限已过","operation_id":"operation-rejected",` +
		`"replayed":false,"retryable":false,"schema_version":1,"status":"error"}`
	if receipt.JSON().String() != want || receipt.Code() != "work_expired" ||
		receipt.Message() != "工作期限已过" || receipt.OperationID() != operationID {
		t.Fatalf("receipt = %#v, JSON = %s", receipt, receipt.JSON().String())
	}
	parsed, err := ParseOperationRejectionReceipt(receipt.JSON().Bytes(), operationID)
	if err != nil || parsed.JSON().String() != want || parsed.OperationID() != operationID {
		t.Fatalf("ParseOperationRejectionReceipt() = (%#v, %v)", parsed, err)
	}
}

func TestOperationRejectionReceiptRejectsInvalidFields(t *testing.T) {
	t.Parallel()

	operationID, _ := ParseOperationID("operation-rejected")
	tests := []OperationRejectionSpec{
		{Code: "", Message: "rejected", OperationID: operationID},
		{Code: "1invalid", Message: "rejected", OperationID: operationID},
		{Code: "invalid__code", Message: "rejected", OperationID: operationID},
		{Code: "invalid-code", Message: "rejected", OperationID: operationID},
		{Code: strings.Repeat("a", MaxLabelBytes+1), Message: "rejected", OperationID: operationID},
		{Code: "invalid_argument", Message: "", OperationID: operationID},
		{Code: "invalid_argument", Message: " rejected", OperationID: operationID},
		{Code: "invalid_argument", Message: strings.Repeat("m", MaxOperationRejectionMessageBytes+1), OperationID: operationID},
		{Code: "invalid_argument", Message: string([]byte{0xff}), OperationID: operationID},
		{Code: "invalid_argument", Message: "rejected"},
	}
	for _, test := range tests {
		if _, err := NewOperationRejectionReceipt(test); err == nil {
			t.Errorf("NewOperationRejectionReceipt(%#v) accepted invalid field", test)
		}
	}
}

func TestOperationRejectionReceiptParserRejectsWireDrift(t *testing.T) {
	t.Parallel()

	operationID, _ := ParseOperationID("operation-rejected")
	valid := `{"code":"work_expired","message":"expired","operation_id":"operation-rejected",` +
		`"replayed":false,"retryable":false,"schema_version":1,"status":"error"}`
	tests := []string{
		strings.Replace(valid, `"code":"work_expired"`, `"code":"work_expired","extra":true`, 1),
		valid + `{}`,
		strings.Replace(valid, `"schema_version":1`, `"schema_version":2`, 1),
		strings.Replace(valid, `"status":"error"`, `"status":"rejected"`, 1),
		strings.Replace(valid, `"retryable":false`, `"retryable":true`, 1),
		strings.Replace(valid, `"replayed":false`, `"replayed":true`, 1),
		strings.Replace(valid, `"operation-rejected"`, `"operation-other"`, 1),
		strings.Replace(valid, `{"code"`, `{ "code"`, 1),
	}
	for _, raw := range tests {
		if _, err := ParseOperationRejectionReceipt([]byte(raw), operationID); err == nil {
			t.Errorf("ParseOperationRejectionReceipt(%q) accepted wire drift", raw)
		}
	}
	if _, err := ParseOperationRejectionReceipt([]byte(valid), OperationID{}); err == nil {
		t.Fatalf("ParseOperationRejectionReceipt() accepted zero expected Operation")
	}
}

func TestNewOperationRequiresBoundRejectionReceipt(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	finished := now.Add(time.Second)
	operationID, _ := ParseOperationID("operation-rejected")
	runID, _ := ParseRunID("run-rejected")
	receipt, _ := NewOperationRejectionReceipt(OperationRejectionSpec{
		Code: "work_expired", Message: "expired", OperationID: operationID})
	result := receipt.JSON()
	spec := OperationSpec{ID: operationID, ProfileID: TeamworkProfileID(), AgentRunID: runID,
		ClientKeyHash: Sum([]byte("client")), Kind: OperationTeamworkOffer,
		RequestDigest: Sum([]byte("request")), Status: OperationRejected,
		Result: &result, CreatedAt: now, FinishedAt: &finished}
	if _, err := NewOperation(spec); err != nil {
		t.Fatalf("NewOperation(rejected) error = %v", err)
	}
	forgedID, _ := ParseOperationID("operation-forged")
	forged, _ := NewOperationRejectionReceipt(OperationRejectionSpec{
		Code: "work_expired", Message: "expired", OperationID: forgedID})
	forgedJSON := forged.JSON()
	spec.Result = &forgedJSON
	if _, err := NewOperation(spec); !errors.Is(err, ErrInvariant) {
		t.Fatalf("forged rejection binding error = %v, want ErrInvariant", err)
	}
	legacy, _ := NewJSON([]byte(`{"code":"work_expired","operation_id":"operation-rejected"}`))
	spec.Result = &legacy
	if _, err := NewOperation(spec); !errors.Is(err, ErrInvalid) {
		t.Fatalf("legacy rejection shape error = %v, want ErrInvalid", err)
	}
}
