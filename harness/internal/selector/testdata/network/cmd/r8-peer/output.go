package main

import (
	"encoding/json"

	"github.com/mnemon-dev/mnemon/harness/internal/selector"
)

type snapshotOutput struct {
	Schema      string          `json:"schema"`
	Version     uint32          `json:"version"`
	SelectionID string          `json:"selection_id"`
	Self        string          `json:"self"`
	Phase       string          `json:"phase"`
	Revision    uint64          `json:"revision"`
	Preference  string          `json:"preference,omitempty"`
	Round       uint32          `json:"round,omitempty"`
	Observation json.RawMessage `json:"observation,omitempty"`
}

type initOutput struct {
	snapshotOutput
	SeedOpinionDigest string `json:"seed_opinion_digest"`
	SeedEventID       string `json:"seed_event_id"`
	SeedEventDigest   string `json:"seed_event_digest"`
}

func projectSnapshot(snapshot selector.SelectionSnapshot) snapshotOutput {
	output := snapshotOutput{Schema: "mnemon.r8.network.status", Version: 1,
		SelectionID: snapshot.Descriptor().ID().String(), Self: snapshot.Self().String(),
		Phase: string(snapshot.Phase()), Revision: snapshot.Revision()}
	if state, present := snapshot.State(); present {
		output.Preference = state.Preference().String()
		output.Round = state.Round()
	}
	if observation, present := snapshot.Observation(); present {
		output.Observation = json.RawMessage(observation.CanonicalBytes())
	}
	return output
}

func projectInitSnapshot(snapshot selector.SelectionSnapshot,
	seed selector.AcceptedSeedOpinion,
) initOutput {
	return initOutput{snapshotOutput: projectSnapshot(snapshot),
		SeedOpinionDigest: seed.Opinion().Digest().String(),
		SeedEventID:       seed.Event().ID().String(), SeedEventDigest: seed.Event().Digest().String()}
}
