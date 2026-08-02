package selector

import (
	"encoding/json"
	"fmt"
)

const MaxParticipantIDBytes = 192

// ParticipantID is the selector-local canonical label for an identity already
// authenticated by the transport adapter. It provides no authentication,
// routing, cryptographic, or network semantics of its own.
type ParticipantID struct {
	value string
}

func NewParticipantID(value string) (ParticipantID, error) {
	if value == "" {
		return ParticipantID{}, fmt.Errorf("participant ID is empty: %w", ErrInvalid)
	}
	if len(value) > MaxParticipantIDBytes {
		return ParticipantID{}, fmt.Errorf("participant ID has %d bytes (max %d): %w",
			len(value), MaxParticipantIDBytes, ErrLimit)
	}
	for index := 0; index < len(value); index++ {
		if value[index] < 0x21 || value[index] > 0x7e {
			return ParticipantID{}, fmt.Errorf("participant ID is not canonical printable ASCII: %w", ErrInvalid)
		}
	}
	return ParticipantID{value: value}, nil
}

func (id ParticipantID) String() string { return id.value }
func (id ParticipantID) IsZero() bool   { return id.value == "" }

// canonicalMarshal is sufficient because selector wires are fixed structs and
// every variable collection is normalized before encoding.
func canonicalMarshal(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal selector canonical JSON: %w", err)
	}
	return encoded, nil
}
