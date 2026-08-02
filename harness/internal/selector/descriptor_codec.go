package selector

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

func parseDescriptorCanonical(value []byte) (SelectionDescriptor, error) {
	if len(value) == 0 {
		return SelectionDescriptor{}, fmt.Errorf("decode selection descriptor: %w", ErrInvalid)
	}
	var wire descriptorWire
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return SelectionDescriptor{}, fmt.Errorf("decode selection descriptor: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return SelectionDescriptor{}, fmt.Errorf("decode selection descriptor: %w", err)
	}
	if wire.Version != DescriptorVersion {
		return SelectionDescriptor{}, fmt.Errorf("descriptor version %d: %w", wire.Version, ErrInvalid)
	}
	question, err := agency.ParseDigest(wire.QuestionArtifactDigest)
	if err != nil {
		return SelectionDescriptor{}, fmt.Errorf("descriptor question digest: %w", err)
	}
	candidateA, err := agency.ParseDigest(wire.CandidateAArtifactDigest)
	if err != nil {
		return SelectionDescriptor{}, fmt.Errorf("descriptor candidate A digest: %w", err)
	}
	candidateB, err := agency.ParseDigest(wire.CandidateBArtifactDigest)
	if err != nil {
		return SelectionDescriptor{}, fmt.Errorf("descriptor candidate B digest: %w", err)
	}
	profile, err := NewProfile(wire.MachineProfile.SampleSize, wire.MachineProfile.Alpha,
		wire.MachineProfile.Threshold, wire.MachineProfile.MaxRounds,
		time.Duration(wire.MachineProfile.RoundTimeoutMillis)*time.Millisecond)
	if err != nil {
		return SelectionDescriptor{}, err
	}
	roster := make([]ParticipantID, len(wire.ParticipantRoster))
	for index, raw := range wire.ParticipantRoster {
		roster[index], err = NewParticipantID(raw)
		if err != nil {
			return SelectionDescriptor{}, fmt.Errorf("descriptor participant %d: %w", index, err)
		}
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, wire.ExpiresAt)
	if err != nil {
		return SelectionDescriptor{}, fmt.Errorf("descriptor expiry: %w", err)
	}
	descriptor, err := NewSelectionDescriptor(question, candidateA, candidateB, roster, profile, expiresAt)
	if err != nil {
		return SelectionDescriptor{}, err
	}
	if !bytes.Equal(value, descriptor.CanonicalBytes()) {
		return SelectionDescriptor{}, fmt.Errorf("descriptor is not exact canonical JSON: %w", ErrInvalid)
	}
	return descriptor, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); errors.Is(err, io.EOF) {
		return nil
	} else if err != nil {
		return err
	}
	return errors.New("trailing JSON value")
}
