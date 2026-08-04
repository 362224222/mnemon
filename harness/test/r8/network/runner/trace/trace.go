package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/mnemon-dev/mnemon/harness/test/observer"
)

const (
	scenarioID    = "r8-real-network-recolor"
	scenarioShape = "r8-network-v1|nodes=5|peer-a=A|others=B|k=1|alpha=1|threshold=1|max-rounds=2"
)

func writeTrace(destination io.Writer, config options, proof evidence) error {
	participants := make([]observer.Participant, 0, len(nodes))
	for _, node := range nodes {
		participants = append(participants, observer.Participant{Node: node, Runtime: "r8-peer"})
	}
	writer, err := observer.NewWriter(destination, observer.Run{
		ID: config.runID, Scenario: observer.Scenario{ID: scenarioID, Digest: scenarioDigest()},
		StartedAt: config.startedAt, CandidateDigest: config.candidate, Participants: participants,
	})
	if err != nil {
		return err
	}
	capturedAt := config.finishedAt
	for _, node := range nodes {
		seed := proof.inits[node]
		if _, err := writer.Append(observer.Fact{
			ID: "trace:r8.seed." + node, CapturedAt: capturedAt,
			Source: observer.Source{Class: observer.SourceR8Selector, Node: node},
			Kind:   "r8.selection.seeded", Truth: observer.TruthLocalPreference,
			References: observer.References{Artifact: seed.SeedOpinionDigest,
				Event: seed.SeedEventID, EventDigest: seed.SeedEventDigest, Selection: seed.SelectionID},
			Fields: observer.FactFields{PreferenceAfter: seed.Preference, Phase: seed.Phase,
				SemanticKind: "selection.seed"},
		}); err != nil {
			return err
		}
	}
	gateFacts, err := appendRoundAndGates(writer, proof, capturedAt)
	if err != nil {
		return err
	}
	gates := make([]observer.Gate, 0, len(gateFacts))
	for gateID, factID := range gateFacts {
		gates = append(gates, observer.Gate{ID: gateID, Status: observer.GatePass,
			Evidence: []string{factID}})
	}
	slices.SortFunc(gates, func(left, right observer.Gate) int {
		return bytes.Compare([]byte(left.ID), []byte(right.ID))
	})
	return writer.Finish(observer.Result{Status: observer.ResultPassed,
		FinishedAt: config.finishedAt, Gates: gates})
}

func appendRoundAndGates(writer *observer.Writer, proof evidence,
	capturedAt time.Time,
) (map[string]string, error) {
	selection := proof.round.SelectionID
	round := proof.round.Evidence
	observation := *proof.round.Observation
	intValue := func(value int) *int { return &value }
	boolValue := func(value bool) *bool { return &value }
	facts := []observer.Fact{
		{ID: "trace:r8.round.peer-a.1.freeze", CapturedAt: capturedAt,
			Source: observer.Source{Class: observer.SourceR8Selector, Node: "peer-a"},
			Kind:   "r8.round.frozen", Truth: observer.TruthLocalPreference,
			Causes: []string{"trace:r8.seed.peer-a"}, References: observer.References{Selection: selection},
			Fields: observer.FactFields{Round: intValue(round.Round), SampleSize: intValue(round.SampleSize),
				Alpha: intValue(round.Alpha), PreferenceBefore: round.PreferenceBefore,
				MarginBefore: intValue(round.MarginBefore), Phase: "active"}},
		{ID: "trace:r8.round.peer-a.1.vote", CapturedAt: capturedAt,
			Source: observer.Source{Class: observer.SourceR8Selector, Node: "peer-a"},
			Kind:   "r8.vote.observed", Truth: observer.TruthObservation,
			Causes:     []string{"trace:r8.round.peer-a.1.freeze"},
			References: observer.References{Selection: selection},
			Fields: observer.FactFields{Round: intValue(round.Round), VotesA: intValue(round.VotesA),
				VotesB: intValue(round.VotesB), Authenticated: boolValue(true)}},
		{ID: "trace:r8.round.peer-a.1.settle", CapturedAt: capturedAt,
			Source: observer.Source{Class: observer.SourceR8Selector, Node: "peer-a"},
			Kind:   "r8.round.settled", Truth: observer.TruthLocalPreference,
			Causes:     []string{"trace:r8.round.peer-a.1.vote"},
			References: observer.References{Selection: selection},
			Fields: observer.FactFields{Round: intValue(round.Round), PreferenceBefore: round.PreferenceBefore,
				PreferenceAfter: round.PreferenceAfter, MarginBefore: intValue(round.MarginBefore),
				MarginAfter: intValue(round.MarginAfter), Recolored: boolValue(round.Recolored), Phase: "observed"}},
		{ID: "trace:r8.observation.peer-a", CapturedAt: capturedAt,
			Source: observer.Source{Class: observer.SourceR8Selector, Node: "peer-a"},
			Kind:   "r8.observation.produced", Truth: observer.TruthLocalPreference,
			Causes:     []string{"trace:r8.round.peer-a.1.settle"},
			References: observer.References{Selection: selection},
			Fields: observer.FactFields{Round: intValue(observation.Rounds), Result: observation.Result,
				PreferenceAfter: *observation.Preference, MarginAfter: intValue(observation.Margin),
				Phase: "observed"}},
	}
	for _, fact := range facts {
		if _, err := writer.Append(fact); err != nil {
			return nil, err
		}
	}
	return appendGateFacts(writer, selection, capturedAt)
}

func appendGateFacts(writer *observer.Writer, selection string,
	capturedAt time.Time,
) (map[string]string, error) {
	gates := []struct {
		id    string
		cause string
		code  string
	}{
		{"r8.real-recolor", "trace:r8.observation.peer-a", "authenticated-sample"},
		{"r8.authenticated-no-vote", "", "unknown-selection"},
		{"r8.identity-binding", "", "claimed-source-rejected"},
		{"r8.restart-persistence", "trace:r8.observation.peer-a", "exact-observation"},
	}
	result := make(map[string]string, len(gates))
	for _, gate := range gates {
		factID := "trace:r8.gate." + gate.id
		causes := []string(nil)
		if gate.cause != "" {
			causes = []string{gate.cause}
		}
		if _, err := writer.Append(observer.Fact{
			ID: factID, CapturedAt: capturedAt,
			Source: observer.Source{Class: observer.SourceOracle, Node: "runner"},
			Kind:   "test.gate.checked", Truth: observer.TruthAssertion, Causes: causes,
			References: observer.References{Selection: selection},
			Fields:     observer.FactFields{GateID: gate.id, Status: "pass", Code: gate.code},
		}); err != nil {
			return nil, err
		}
		result[gate.id] = factID
	}
	return result, nil
}

func scenarioDigest() string {
	digest := sha256.Sum256([]byte(scenarioShape))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func writeAtomic(path string, write func(io.Writer) error) error {
	if write == nil {
		return errors.New("trace writer callback is required")
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return fmt.Errorf("create trace directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".r8-trace-*")
	if err != nil {
		return fmt.Errorf("create temporary trace: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := write(temporary); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("protect temporary trace: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary trace: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("publish trace: %w", err)
	}
	return nil
}
