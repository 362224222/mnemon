package agency

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// decodeCanonicalObject performs the JSON-level checks shared by durable
// agency parsers. Domain constructors remain responsible for typed values,
// bounds inside the object, and authority relationships.
func decodeCanonicalObject(field string, data []byte, maximum int, destination any) error {
	if len(data) == 0 {
		return invalid(field, "must not be empty")
	}
	if len(data) > maximum {
		return limit(field+" bytes", len(data), maximum)
	}
	if err := rejectDuplicateJSONKeys(field, data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("agency: decode %s: %w", field, err)
	}
	if err := requireJSONEOF(field, decoder); err != nil {
		return err
	}
	canonical, _, err := canonicalJSON(destination)
	if err != nil {
		return err
	}
	if !bytes.Equal(canonical, data) {
		return invalid(field, "is not the canonical encoding")
	}
	return nil
}

func requireReconstructedCanonical(field string, data, reconstructed []byte) error {
	if !bytes.Equal(data, reconstructed) {
		return invariant(field, "does not match the authenticated authority")
	}
	return nil
}
