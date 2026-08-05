package agency

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

const MaxReceiptCanonicalBytes = 4 << 10

// ParseReceiptCanonicalJSON reconstructs a private durable Receipt. It accepts
// only the exact canonical encoding produced by the Receipt constructors; the
// caller remains responsible for comparing Digest with its separately stored
// integrity digest.
func ParseReceiptCanonicalJSON(data []byte) (Receipt, error) {
	if len(data) == 0 {
		return Receipt{}, invalid("Receipt JSON", "must not be empty")
	}
	if len(data) > MaxReceiptCanonicalBytes {
		return Receipt{}, limit("Receipt JSON bytes", len(data), MaxReceiptCanonicalBytes)
	}
	if err := rejectDuplicateJSONKeys("Receipt JSON", data); err != nil {
		return Receipt{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var wire receiptWire
	if err := decoder.Decode(&wire); err != nil {
		return Receipt{}, fmt.Errorf("agency: decode Receipt: %w", err)
	}
	if err := requireJSONEOF("Receipt JSON", decoder); err != nil {
		return Receipt{}, err
	}
	receipt, err := receiptFromWire(wire)
	if err != nil {
		return Receipt{}, err
	}
	canonical, digest, err := canonicalJSON(receipt.wire())
	if err != nil {
		return Receipt{}, err
	}
	if !bytes.Equal(canonical, data) {
		return Receipt{}, invalid("Receipt JSON", "is not the canonical Receipt encoding")
	}
	receipt.canonical, receipt.digest = canonical, digest
	return receipt, nil
}

func receiptFromWire(wire receiptWire) (Receipt, error) {
	if wire.SchemaVersion != 1 {
		return Receipt{}, invalid("Receipt schema version", "must be 1")
	}
	operationKey, err := NewOperationKey(wire.OperationKey)
	if err != nil {
		return Receipt{}, err
	}
	requestDigest, err := ParseDigest(wire.RequestDigest)
	if err != nil {
		return Receipt{}, err
	}
	recordedAt, err := parseCanonicalReceiptTime(wire.RecordedAt)
	if err != nil {
		return Receipt{}, err
	}

	receipt := Receipt{operationKey: operationKey, requestDigest: requestDigest, recordedAt: recordedAt}
	switch wire.Outcome {
	case "accepted":
		receipt.outcome = ReceiptOutcomeAccepted
		if wire.Event == nil || wire.Code != "" || wire.Diagnostic != "" {
			return Receipt{}, invalid("accepted Receipt", "requires one Event and no rejection fields")
		}
		receipt.event, err = parseReceiptEventRef(*wire.Event)
	case "rejected":
		receipt.outcome = ReceiptOutcomeRejected
		if wire.Event != nil || wire.Code == "" {
			return Receipt{}, invalid("rejected Receipt", "requires a code and no Event")
		}
		receipt.code, err = NewSemanticLabel(wire.Code)
		if err == nil {
			if len(wire.Diagnostic) > MaxDiagnosticBytes {
				err = limit("Receipt diagnostic", len(wire.Diagnostic), MaxDiagnosticBytes)
			} else {
				_, err = NewSemanticPayload(wire.Diagnostic)
			}
		}
		receipt.diagnostic = wire.Diagnostic
	default:
		return Receipt{}, invalid("Receipt outcome", "must be accepted or rejected")
	}
	if err != nil {
		return Receipt{}, err
	}
	return receipt, nil
}

func parseCanonicalReceiptTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, invalid("Receipt recorded time", "must use RFC3339Nano")
	}
	canonical, err := canonicalTime("Receipt recorded time", parsed)
	if err != nil {
		return time.Time{}, err
	}
	if value != canonical.Format(time.RFC3339Nano) {
		return time.Time{}, invalid("Receipt recorded time", "must use canonical UTC RFC3339Nano")
	}
	return canonical, nil
}

func parseReceiptEventRef(wire eventRefWire) (EventRef, error) {
	id, err := NewEventID(wire.ID)
	if err != nil {
		return EventRef{}, err
	}
	digest, err := ParseDigest(wire.Digest)
	if err != nil {
		return EventRef{}, err
	}
	return NewEventRef(id, digest)
}
