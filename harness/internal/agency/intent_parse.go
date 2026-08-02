package agency

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// ParseAgentIntentJSON decodes only the existing Agent-owned Intent wire. It
// rejects unknown or machine-owned fields and returns the same canonical value
// produced by NewAgentIntent.
func ParseAgentIntentJSON(data []byte) (AgentIntent, error) {
	if len(data) == 0 {
		return AgentIntent{}, invalid("Intent JSON", "must not be empty")
	}
	if len(data) > MaxIntentCanonicalBytes {
		return AgentIntent{}, limit("Intent JSON bytes", len(data), MaxIntentCanonicalBytes)
	}
	if err := rejectDuplicateJSONKeys("Agent Intent JSON", data); err != nil {
		return AgentIntent{}, err
	}
	if err := requireExactIntentWireKeys(data); err != nil {
		return AgentIntent{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var wire intentWire
	if err := decoder.Decode(&wire); err != nil {
		return AgentIntent{}, fmt.Errorf("agency: decode Agent Intent: %w", err)
	}
	if err := requireJSONEOF("Agent Intent JSON", decoder); err != nil {
		return AgentIntent{}, err
	}
	return intentFromWire(wire)
}

func requireExactIntentWireKeys(data []byte) error {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return fmt.Errorf("agency: inspect Agent Intent fields: %w", err)
	}
	allowed := map[string]struct{}{
		"kind": {}, "payload": {}, "consequence": {}, "subject_handling": {}, "successors": {},
		"reference_key": {}, "reference_head": {}, "artifacts": {}, "causation_handles": {},
		"correlation_handle": {},
	}
	for key := range object {
		if _, exists := allowed[key]; !exists {
			return invalid("Intent JSON", "contains a non-canonical field name")
		}
	}
	for _, required := range []string{"kind", "payload", "consequence"} {
		if _, exists := object[required]; !exists {
			return invalid("Intent JSON", "omits a required field")
		}
	}
	if raw, exists := object["successors"]; exists {
		if err := requireObjectArrayKeys("Intent successor", raw, "self", "alias"); err != nil {
			return err
		}
	}
	if raw, exists := object["artifacts"]; exists {
		if err := requireObjectArrayKeys("Intent Artifact", raw, "kind", "handle"); err != nil {
			return err
		}
	}
	return nil
}

func requireObjectArrayKeys(field string, raw json.RawMessage, keys ...string) error {
	var objects []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &objects); err != nil {
		return fmt.Errorf("agency: inspect %s fields: %w", field, err)
	}
	allowed := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		allowed[key] = struct{}{}
	}
	for _, object := range objects {
		for key := range object {
			if _, exists := allowed[key]; !exists {
				return invalid(field, "contains a non-canonical field name")
			}
		}
	}
	return nil
}

func rejectDuplicateJSONKeys(field string, data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := scanJSONValue(field, decoder); err != nil {
		return fmt.Errorf("agency: inspect %s: %w", field, err)
	}
	return nil
}

func scanJSONValue(field string, decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, structured := token.(json.Delim)
	if !structured {
		return nil
	}
	switch delimiter {
	case '{':
		return scanJSONObject(field, decoder)
	case '[':
		for decoder.More() {
			if err := scanJSONValue(field, decoder); err != nil {
				return err
			}
		}
	default:
		return invalid(field, "contains an unexpected delimiter")
	}
	_, err = decoder.Token()
	return err
}

func scanJSONObject(field string, decoder *json.Decoder) error {
	seen := make(map[string]struct{})
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		key, ok := token.(string)
		if !ok {
			return invalid(field, "object key must be a string")
		}
		if _, duplicate := seen[key]; duplicate {
			return invalid(field, "contains a duplicate object key")
		}
		seen[key] = struct{}{}
		if err := scanJSONValue(field, decoder); err != nil {
			return err
		}
	}
	_, err := decoder.Token()
	return err
}

func requireJSONEOF(field string, decoder *json.Decoder) error {
	var trailing any
	err := decoder.Decode(&trailing)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("agency: decode trailing %s: %w", field, err)
	}
	return invalid(field, "must contain exactly one value")
}

func intentFromWire(wire intentWire) (AgentIntent, error) {
	kind, err := NewSemanticLabel(wire.Kind)
	if err != nil {
		return AgentIntent{}, err
	}
	payload, err := NewSemanticPayload(wire.Payload)
	if err != nil {
		return AgentIntent{}, err
	}
	consequence, err := parseConsequence(wire.Consequence)
	if err != nil {
		return AgentIntent{}, err
	}
	subject, err := parseOptionalHandle("subject Handling", wire.SubjectHandling)
	if err != nil {
		return AgentIntent{}, err
	}
	referenceHead, err := parseOptionalHandle("Reference head", wire.ReferenceHead)
	if err != nil {
		return AgentIntent{}, err
	}
	correlation, err := parseOptionalHandle("correlation", wire.CorrelationHandle)
	if err != nil {
		return AgentIntent{}, err
	}
	referenceKey, err := parseOptionalReferenceKey(wire.ReferenceKey)
	if err != nil {
		return AgentIntent{}, err
	}
	successors, err := parseTargets(wire.Successors)
	if err != nil {
		return AgentIntent{}, err
	}
	artifacts, err := parseArtifactInputs(wire.Artifacts)
	if err != nil {
		return AgentIntent{}, err
	}
	causation, err := parseHandles("causation", wire.CausationHandles)
	if err != nil {
		return AgentIntent{}, err
	}
	return NewAgentIntent(IntentSpec{
		Kind: kind, Payload: payload, Consequence: consequence, SubjectHandling: subject,
		Successors: successors, ReferenceKey: referenceKey, ReferenceHead: referenceHead,
		Artifacts: artifacts, CausationHandles: causation, CorrelationHandle: correlation,
	})
}

func parseConsequence(value string) (Consequence, error) {
	for consequence := ConsequenceCreateHandlings; consequence <= ConsequenceRetractReference; consequence++ {
		if consequence.String() == value {
			return consequence, nil
		}
	}
	return ConsequenceInvalid, invalid("Intent consequence", "is not a closed consequence")
}

func parseOptionalHandle(field, value string) (OpaqueHandle, error) {
	if value == "" {
		return OpaqueHandle{}, nil
	}
	handle, err := NewOpaqueHandle(value)
	if err != nil {
		return OpaqueHandle{}, fmt.Errorf("%s: %w", field, err)
	}
	return handle, nil
}

func parseOptionalReferenceKey(value string) (ReferenceKey, error) {
	if value == "" {
		return ReferenceKey{}, nil
	}
	return NewReferenceKey(value)
}

func parseTargets(wires []targetWire) ([]TargetRef, error) {
	result := make([]TargetRef, 0, len(wires))
	for _, wire := range wires {
		if wire.Self == (wire.Alias != "") {
			return nil, invalid("Intent target", "must contain exactly one of self or alias")
		}
		if wire.Self {
			result = append(result, SelfTarget())
			continue
		}
		alias, err := NewOpaqueHandle(wire.Alias)
		if err != nil {
			return nil, err
		}
		target, err := AliasTarget(alias)
		if err != nil {
			return nil, err
		}
		result = append(result, target)
	}
	return result, nil
}

func parseArtifactInputs(wires []artifactInputWire) ([]ArtifactInput, error) {
	result := make([]ArtifactInput, 0, len(wires))
	for _, wire := range wires {
		handle, err := NewOpaqueHandle(wire.Handle)
		if err != nil {
			return nil, err
		}
		var input ArtifactInput
		switch wire.Kind {
		case "candidate":
			input, err = NewArtifactCandidate(handle)
		case "view_handle":
			input, err = NewArtifactViewHandle(handle)
		default:
			return nil, invalid("Intent Artifact kind", "is not candidate or view_handle")
		}
		if err != nil {
			return nil, err
		}
		result = append(result, input)
	}
	return result, nil
}

func parseHandles(field string, values []string) ([]OpaqueHandle, error) {
	result := make([]OpaqueHandle, 0, len(values))
	for _, value := range values {
		handle, err := NewOpaqueHandle(value)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", field, err)
		}
		result = append(result, handle)
	}
	return result, nil
}
