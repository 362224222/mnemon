package main

import (
	"strconv"
	"time"

	"github.com/mnemon-dev/mnemon/harness/test/observer"
)

func appendFailedAttentionFacts(writer *observer.Writer, value *openAttentionSettlement,
	capturedAt time.Time,
) ([]string, error) {
	if value == nil {
		return nil, nil
	}
	used := 0
	for _, wave := range value.Waves {
		if _, err := appendAttentionSnapshot(writer, value, wave.Wave, used,
			"test.attention.wave", wave.Nodes, capturedAt); err != nil {
			return nil, err
		}
		for _, node := range wave.Nodes {
			if node.OpenUnclaimed > 0 {
				used++
			}
		}
	}
	finalKind := "test.attention.exhausted"
	if value.Status == "claim_occupied" {
		finalKind = "test.attention.occupied"
	}
	return appendAttentionSnapshot(writer, value, len(value.Waves)+1, value.TurnsUsed,
		finalKind, value.Final, capturedAt)
}

func appendAttentionSnapshot(writer *observer.Writer, value *openAttentionSettlement,
	wave, used int, kind string, nodes []openAttentionNode, capturedAt time.Time,
) ([]string, error) {
	facts := make([]string, 0, len(nodes))
	for _, node := range nodes {
		factID := hashedFactID("attention", value.Episode, strconv.Itoa(wave), node.Role, kind)
		fields := observer.FactFields{
			Episode:        value.Episode,
			Role:           node.Role,
			Round:          intPointer(wave),
			OpenUnclaimed:  intPointer(node.OpenUnclaimed),
			OccupiedClaims: intPointer(node.OccupiedClaims),
			TurnLimit:      intPointer(value.TurnLimit),
			TurnsUsed:      intPointer(used),
		}
		if _, err := writer.Append(observer.Fact{ID: factID, CapturedAt: capturedAt,
			Source: observer.Source{Class: observer.SourceOracle, Node: "runner"},
			Kind:   kind, Truth: observer.TruthAssertion, Fields: fields}); err != nil {
			return nil, err
		}
		facts = append(facts, factID)
	}
	return facts, nil
}
