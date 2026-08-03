package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

func marshalClosedJSON(value any) ([]byte, error) { return json.Marshal(value) }

// decodeClosedJSON is deliberately not a general canonical JSON
// implementation. Private control DTOs have one closed struct encoding, so
// strict decode plus exact re-marshal is their complete wire check.
func decodeClosedJSON(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("control JSON contains a trailing value")
	}
	rebuilt, err := marshalClosedJSON(target)
	if err != nil || !bytes.Equal(rebuilt, raw) {
		return errors.New("control JSON does not match its closed schema")
	}
	return nil
}
