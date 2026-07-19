package model

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const MaxOperationRejectionMessageBytes = 512

type OperationRejectionSpec struct {
	Code        string
	Message     string
	OperationID OperationID
}

// OperationRejectionReceipt is the durable, transport-neutral rejection
// evidence for a managed operation. The Agent layer owns the closed meaning of
// Code; the model owns only its bounded wire shape and Operation identity.
type OperationRejectionReceipt struct {
	code        string
	message     string
	operationID OperationID
	canonical   JSON
}

type operationRejectionWire struct {
	Code          string `json:"code"`
	Message       string `json:"message"`
	OperationID   string `json:"operation_id"`
	Replayed      bool   `json:"replayed"`
	Retryable     bool   `json:"retryable"`
	SchemaVersion int    `json:"schema_version"`
	Status        string `json:"status"`
}

func NewOperationRejectionReceipt(spec OperationRejectionSpec) (OperationRejectionReceipt, error) {
	if spec.OperationID.IsZero() {
		return OperationRejectionReceipt{}, invalid("operation rejection", "Operation identity is required")
	}
	if err := validateOperationRejectionCode(spec.Code); err != nil {
		return OperationRejectionReceipt{}, err
	}
	if err := validateText("operation rejection message", spec.Message,
		MaxOperationRejectionMessageBytes, false); err != nil {
		return OperationRejectionReceipt{}, err
	}
	if strings.TrimSpace(spec.Message) != spec.Message {
		return OperationRejectionReceipt{}, invalid("operation rejection message",
			"must not have surrounding whitespace")
	}
	canonical, err := JSONFrom(operationRejectionWire{Code: spec.Code, Message: spec.Message,
		OperationID: spec.OperationID.String(), SchemaVersion: SchemaVersion, Status: "error"})
	if err != nil {
		return OperationRejectionReceipt{}, fmt.Errorf("encode operation rejection: %w", err)
	}
	return OperationRejectionReceipt{code: spec.Code, message: spec.Message,
		operationID: spec.OperationID, canonical: canonical}, nil
}

// ParseOperationRejectionReceipt accepts only the exact canonical schema and
// binds its embedded Operation identity to expectedOperationID.
func ParseOperationRejectionReceipt(raw []byte,
	expectedOperationID OperationID,
) (OperationRejectionReceipt, error) {
	if expectedOperationID.IsZero() {
		return OperationRejectionReceipt{}, invalid("operation rejection", "expected Operation identity is required")
	}
	var wire operationRejectionWire
	if err := decodeOperationRejectionWire(raw, &wire); err != nil {
		return OperationRejectionReceipt{}, err
	}
	if err := validateOperationRejectionWire(wire); err != nil {
		return OperationRejectionReceipt{}, err
	}
	operationID, err := ParseOperationID(wire.OperationID)
	if err != nil {
		return OperationRejectionReceipt{}, err
	}
	if operationID != expectedOperationID {
		return OperationRejectionReceipt{}, invariant("operation rejection receipt binds a different Operation")
	}
	receipt, err := NewOperationRejectionReceipt(OperationRejectionSpec{
		Code: wire.Code, Message: wire.Message, OperationID: operationID})
	if err != nil {
		return OperationRejectionReceipt{}, err
	}
	if !bytes.Equal(receipt.JSON().Bytes(), raw) {
		return OperationRejectionReceipt{}, invalid("operation rejection", "must use exact canonical JSON")
	}
	return receipt, nil
}

func (r OperationRejectionReceipt) JSON() JSON               { return r.canonical }
func (r OperationRejectionReceipt) Code() string             { return r.code }
func (r OperationRejectionReceipt) Message() string          { return r.message }
func (r OperationRejectionReceipt) OperationID() OperationID { return r.operationID }

func validateOperationRejectionCode(code string) error {
	if len(code) == 0 {
		return invalid("operation rejection code", "must not be empty")
	}
	if len(code) > MaxLabelBytes {
		return limit("operation rejection code", len(code), MaxLabelBytes)
	}
	for index, value := range []byte(code) {
		if value >= 'a' && value <= 'z' {
			continue
		}
		if index > 0 && value >= '0' && value <= '9' {
			continue
		}
		if index > 0 && index < len(code)-1 && value == '_' && code[index-1] != '_' {
			continue
		}
		return invalid("operation rejection code", "must be lowercase snake_case")
	}
	return nil
}

func decodeOperationRejectionWire(raw []byte, wire *operationRejectionWire) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(wire); err != nil {
		return fmt.Errorf("parse operation rejection: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return invalid("operation rejection", "unexpected trailing JSON value")
	}
	return nil
}

func validateOperationRejectionWire(wire operationRejectionWire) error {
	if wire.SchemaVersion != SchemaVersion {
		return invalid("operation rejection", "unsupported schema version")
	}
	if wire.Status != "error" {
		return invalid("operation rejection", "status must be error")
	}
	if wire.Retryable {
		return invalid("operation rejection", "durable rejection cannot be retryable")
	}
	if wire.Replayed {
		return invalid("operation rejection", "durable receipt cannot be a replay projection")
	}
	return nil
}
