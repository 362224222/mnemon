package node

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func ParseInitiationProjection(raw []byte) (InitiationProjection, error) {
	projection, err := decodeInitiationProjection(raw)
	if err != nil {
		return InitiationProjection{}, err
	}
	if err := validateInitiationProjection(raw, projection); err != nil {
		return InitiationProjection{}, err
	}
	return projection, nil
}

func decodeInitiationProjection(raw []byte) (InitiationProjection, error) {
	canonical, err := model.CanonicalizeJSON(raw)
	if err != nil || !bytes.Equal(canonical, raw) {
		return InitiationProjection{}, errors.New("initiation projection is not canonical JSON")
	}
	var projection InitiationProjection
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&projection); err != nil {
		return InitiationProjection{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return InitiationProjection{}, errors.New("initiation projection has trailing input")
	}
	return projection, nil
}

func validateInitiationProjection(raw []byte, projection InitiationProjection) error {
	closed, err := model.CanonicalMarshal(projection)
	if err != nil || !bytes.Equal(closed, raw) || projection.SchemaVersion != SchemaVersion ||
		projection.InitiationContext.Channels == nil ||
		len(projection.InitiationContext.Channels) > model.MaxChannelsPerNode {
		return errors.New("initiation projection has an invalid schema")
	}
	previousChannel := ""
	for _, channel := range projection.InitiationContext.Channels {
		if err := validateInitiationChannel(channel, previousChannel); err != nil {
			return err
		}
		previousChannel = channel.LocalAlias
	}
	return nil
}

func validateInitiationChannel(channel InitiationChannel, previous string) error {
	if !validControlAlias(channel.LocalAlias) ||
		(previous != "" && previous >= channel.LocalAlias) ||
		channel.Participants == nil || len(channel.Participants) > model.MaxChildWorks {
		return errors.New("initiation Channel projection is invalid")
	}
	return validateInitiationParticipants(channel.Participants)
}

func validateInitiationParticipants(participants []InitiationParticipant) error {
	previous := ""
	for _, participant := range participants {
		if !validControlAlias(participant.EffectiveAlias) ||
			(previous != "" && previous >= participant.EffectiveAlias) {
			return errors.New("initiation participant projection is invalid")
		}
		previous = participant.EffectiveAlias
	}
	return nil
}
