package localapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func ParseInitiationProjection(raw []byte) (InitiationProjection, error) {
	var projection InitiationProjection
	canonical, err := model.CanonicalizeJSON(raw)
	if err != nil || !bytes.Equal(canonical, raw) {
		return projection, errors.New("initiation projection is not canonical JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&projection); err != nil {
		return InitiationProjection{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return InitiationProjection{}, errors.New("initiation projection has trailing input")
	}
	closed, err := model.CanonicalMarshal(projection)
	if err != nil || !bytes.Equal(closed, raw) || projection.SchemaVersion != SchemaVersion ||
		projection.InitiationContext.Channels == nil ||
		len(projection.InitiationContext.Channels) > model.MaxChannelsPerNode {
		return InitiationProjection{}, errors.New("initiation projection has an invalid schema")
	}
	previousChannel := ""
	for _, channel := range projection.InitiationContext.Channels {
		if !validInitiationAlias(channel.LocalAlias) ||
			(previousChannel != "" && previousChannel >= channel.LocalAlias) ||
			channel.Participants == nil || len(channel.Participants) > model.MaxChildWorks {
			return InitiationProjection{}, errors.New("initiation Channel projection is invalid")
		}
		previousChannel = channel.LocalAlias
		previousParticipant := ""
		eligible := false
		for _, participant := range channel.Participants {
			if !validInitiationAlias(participant.EffectiveAlias) ||
				participant.EffectiveAlias == "auto" || participant.EffectiveAlias == "team" ||
				(previousParticipant != "" && previousParticipant >= participant.EffectiveAlias) {
				return InitiationProjection{}, errors.New("initiation participant projection is invalid")
			}
			previousParticipant = participant.EffectiveAlias
			eligible = eligible || participant.Eligible
		}
		if channel.AllowTeam != eligible {
			return InitiationProjection{}, errors.New("initiation team projection is invalid")
		}
	}
	return projection, nil
}

func validInitiationAlias(value string) bool {
	if value == "" || !utf8.ValidString(value) || len(value) > model.MaxLabelBytes ||
		strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if character <= 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}
