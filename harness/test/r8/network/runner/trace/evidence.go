package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const maxEvidenceBytes = 64 << 10

var nodes = []string{"peer-a", "peer-b", "peer-c", "peer-d", "peer-e"}

type snapshotInput struct {
	Schema      string            `json:"schema"`
	Version     int               `json:"version"`
	SelectionID string            `json:"selection_id"`
	Self        string            `json:"self"`
	Phase       string            `json:"phase"`
	Revision    int               `json:"revision"`
	Preference  string            `json:"preference"`
	Round       int               `json:"round"`
	Observation *observationInput `json:"observation"`
}

type initInput struct {
	snapshotInput
	SeedOpinionDigest string `json:"seed_opinion_digest"`
	SeedEventID       string `json:"seed_event_id"`
	SeedEventDigest   string `json:"seed_event_digest"`
}

type roundInput struct {
	snapshotInput
	Evidence roundEvidenceInput `json:"round_evidence"`
}

type roundEvidenceInput struct {
	Round            int    `json:"round"`
	SampleSize       int    `json:"sample_size"`
	Alpha            int    `json:"alpha"`
	VotesA           int    `json:"votes_a"`
	VotesB           int    `json:"votes_b"`
	PreferenceBefore string `json:"preference_before"`
	PreferenceAfter  string `json:"preference_after"`
	MarginBefore     int    `json:"margin_before"`
	MarginAfter      int    `json:"margin_after"`
	Recolored        bool   `json:"recolored"`
}

type observationInput struct {
	Margin      int     `json:"margin"`
	Preference  *string `json:"preference"`
	Profile     string  `json:"profile_digest"`
	Reason      *string `json:"reason"`
	Result      string  `json:"result"`
	Roster      string  `json:"roster_digest"`
	Rounds      int     `json:"rounds"`
	SelectionID string  `json:"selection_id"`
}

type probeInput struct {
	Mode          string `json:"mode"`
	HTTPStatus    int    `json:"http_status"`
	Authenticated bool   `json:"authenticated"`
	NoVote        bool   `json:"no_vote"`
}

type evidence struct {
	inits            map[string]initInput
	round            roundInput
	noVote           probeInput
	identityMismatch probeInput
	restarted        snapshotInput
}

func loadEvidence(directory string) (evidence, error) {
	proof := evidence{inits: make(map[string]initInput, len(nodes))}
	for _, node := range nodes {
		var value initInput
		if err := readJSON(filepath.Join(directory, node+"-init.json"), &value); err != nil {
			return evidence{}, err
		}
		proof.inits[node] = value
	}
	files := []struct {
		name  string
		value any
	}{
		{"observation.json", &proof.round},
		{"no-vote.json", &proof.noVote},
		{"identity-mismatch.json", &proof.identityMismatch},
		{"peer-a-status.json", &proof.restarted},
	}
	for _, file := range files {
		if err := readJSON(filepath.Join(directory, file.name), file.value); err != nil {
			return evidence{}, err
		}
	}
	if err := validateEvidence(proof); err != nil {
		return evidence{}, err
	}
	return proof, nil
}

func validateEvidence(proof evidence) error {
	selection := proof.round.SelectionID
	if err := validateSeeds(proof, selection); err != nil {
		return err
	}
	if err := validateRound(proof.round, selection); err != nil {
		return err
	}
	if err := validateProbes(proof.noVote, proof.identityMismatch); err != nil {
		return err
	}
	if proof.restarted.SelectionID != selection || proof.restarted.Phase != "observed" ||
		proof.restarted.Observation == nil ||
		!equalObservation(*proof.restarted.Observation, *proof.round.Observation) {
		return errors.New("restart did not preserve the exact local observation")
	}
	return nil
}

func validateSeeds(proof evidence, selection string) error {
	if selection == "" || proof.round.Schema != "mnemon.r8.network.status" ||
		proof.round.Version != 1 || proof.round.Self != proof.inits["peer-a"].Self ||
		proof.round.Phase != "observed" || proof.round.Observation == nil {
		return errors.New("round evidence is incomplete")
	}
	for _, node := range nodes {
		seed := proof.inits[node]
		want := "B"
		if node == "peer-a" {
			want = "A"
		}
		if seed.Schema != "mnemon.r8.network.status" || seed.Version != 1 ||
			seed.SelectionID != selection || seed.Phase != "active" || seed.Preference != want ||
			seed.Revision < 1 || seed.SeedEventID == "" || seed.SeedEventDigest == "" ||
			seed.SeedOpinionDigest == "" {
			return fmt.Errorf("%s seed evidence is inconsistent", node)
		}
	}
	return nil
}

func validateRound(roundInput roundInput, selection string) error {
	round := roundInput.Evidence
	observation := roundInput.Observation
	if round.Round != 1 || round.SampleSize != 1 || round.Alpha != 1 || round.VotesA != 0 ||
		round.VotesB != 1 || round.PreferenceBefore != "A" || round.PreferenceAfter != "B" ||
		round.MarginBefore != 0 || round.MarginAfter != -1 || !round.Recolored ||
		roundInput.Preference != "B" || roundInput.Round != 1 ||
		observation.SelectionID != selection || observation.Result != "threshold_reached" ||
		observation.Preference == nil || *observation.Preference != "B" ||
		observation.Margin != -1 || observation.Rounds != 1 {
		return errors.New("round did not prove one authenticated A-to-B recolor")
	}
	return nil
}

func validateProbes(noVote, identityMismatch probeInput) error {
	if noVote.Mode != "no-vote" || noVote.HTTPStatus != 200 ||
		!noVote.Authenticated || !noVote.NoVote {
		return errors.New("authenticated no-vote evidence is missing")
	}
	if identityMismatch.Mode != "identity-mismatch" ||
		identityMismatch.HTTPStatus != 401 || identityMismatch.Authenticated {
		return errors.New("identity-mismatch rejection evidence is missing")
	}
	return nil
}

func equalObservation(left, right observationInput) bool {
	return left.Margin == right.Margin && left.Result == right.Result && left.Rounds == right.Rounds &&
		left.SelectionID == right.SelectionID && left.Profile == right.Profile &&
		left.Roster == right.Roster && equalOptional(left.Preference, right.Preference) &&
		equalOptional(left.Reason, right.Reason)
}

func equalOptional(left, right *string) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

func readJSON(path string, destination any) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open bounded evidence %s: %w", filepath.Base(path), err)
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, maxEvidenceBytes+1))
	if err != nil {
		return fmt.Errorf("read bounded evidence %s: %w", filepath.Base(path), err)
	}
	if len(raw) > maxEvidenceBytes {
		return fmt.Errorf("bounded evidence %s exceeds %d bytes", filepath.Base(path), maxEvidenceBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode bounded evidence %s: %w", filepath.Base(path), err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("bounded evidence %s has trailing or excessive data", filepath.Base(path))
	}
	return nil
}
