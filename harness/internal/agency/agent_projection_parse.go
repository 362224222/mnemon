package agency

// ValidateAgentViewProjectionCanonicalJSON validates only the public View
// wire shape and its exact canonical bytes. Authority-dependent reconstruction
// remains ParseAgentViewCanonicalJSON's job inside the daemon boundary.
func ValidateAgentViewProjectionCanonicalJSON(data []byte) error {
	var wire agentViewWire
	if err := decodeCanonicalObject("Agent View projection JSON", data,
		MaxAgentViewCanonicalBytes, &wire); err != nil {
		return err
	}
	if wire.Schema != AgentViewSchema || wire.Version != AgentViewVersion {
		return invalid("Agent View projection envelope", "has an unsupported schema or version")
	}
	return nil
}

// ParseAgentReceiptProjectionCanonicalJSON reconstructs the complete public
// Receipt projection without importing private admission authority.
func ParseAgentReceiptProjectionCanonicalJSON(data []byte) (AgentReceipt, error) {
	var wire agentReceiptWire
	if err := decodeCanonicalObject("Agent Receipt projection JSON", data,
		MaxAgentReceiptCanonicalBytes, &wire); err != nil {
		return AgentReceipt{}, err
	}
	if wire.Schema != AgentReceiptSchema || wire.Version != AgentReceiptVersion {
		return AgentReceipt{}, invalid("Agent Receipt projection envelope",
			"has an unsupported schema or version")
	}
	var outcome ReceiptOutcome
	switch wire.Outcome {
	case "accepted":
		if wire.Diagnostic != "" {
			return AgentReceipt{}, invalid("accepted Agent Receipt projection",
				"must not contain a diagnostic")
		}
		outcome = ReceiptOutcomeAccepted
	case "rejected":
		if wire.Diagnostic == "" || len(wire.Diagnostic) > MaxDiagnosticBytes {
			return AgentReceipt{}, invalid("rejected Agent Receipt projection",
				"requires one bounded diagnostic")
		}
		if _, err := NewSemanticPayload(wire.Diagnostic); err != nil {
			return AgentReceipt{}, err
		}
		outcome = ReceiptOutcomeRejected
	default:
		return AgentReceipt{}, invalid("Agent Receipt projection outcome",
			"must be accepted or rejected")
	}
	return AgentReceipt{outcome: outcome, replayed: wire.Replayed,
		diagnostic: wire.Diagnostic, canonical: copyBytes(data)}, nil
}
