package main

import (
	"errors"

	"github.com/mnemon-dev/mnemon/harness/test/observer"
)

func appendRuntimeFacts(writer *observer.Writer, turns []turnSummary) error {
	for _, turn := range turns {
		capturedAt, err := parseReportTime("turn captured_at", turn.CapturedAt)
		if err != nil {
			return err
		}
		facts := []observer.Fact{}
		if turn.HookCues > 0 {
			value := true
			facts = append(facts, observer.Fact{ID: runtimeFactID(turn, "hook"),
				CapturedAt: capturedAt, Source: observer.Source{Class: observer.SourceRuntime, Node: turn.Role},
				Agent: turn.Role, Turn: turn.Turn, Kind: "runtime.hook.cue",
				Truth: observer.TruthObservation, Fields: observer.FactFields{HookCue: &value}})
		}
		if turn.CurrentReads > 0 {
			facts = append(facts, observer.Fact{ID: runtimeFactID(turn, "view"),
				CapturedAt: capturedAt, Source: observer.Source{Class: observer.SourceRuntime, Node: turn.Role},
				Agent: turn.Role, Turn: turn.Turn, Kind: "runtime.view.received",
				Truth:  observer.TruthDerivedProjection,
				Fields: observer.FactFields{Action: "current"}})
		}
		if turn.DelegateCalls > 0 {
			facts = append(facts, observer.Fact{ID: runtimeFactID(turn, "delegate"),
				CapturedAt: capturedAt, Source: observer.Source{Class: observer.SourceRuntime, Node: turn.Role},
				Agent: turn.Role, Turn: turn.Turn, Kind: "runtime.delegate.invoked",
				Truth: observer.TruthObservation})
		}
		if turn.IntentSubmits > 0 {
			facts = append(facts, observer.Fact{ID: runtimeFactID(turn, "intent"),
				CapturedAt: capturedAt, Source: observer.Source{Class: observer.SourceRuntime, Node: turn.Role},
				Agent: turn.Role, Turn: turn.Turn, Kind: "runtime.intent.submitted",
				Truth: observer.TruthObservation, Fields: observer.FactFields{Action: "submit"}})
		}
		facts = append(facts, observer.Fact{ID: runtimeFactID(turn, "ended"),
			CapturedAt: capturedAt, Source: observer.Source{Class: observer.SourceRuntime, Node: turn.Role},
			Agent: turn.Role, Turn: turn.Turn, Kind: "runtime.turn.ended",
			Truth: observer.TruthObservation})
		for _, fact := range facts {
			// Sanitized turn counters establish observations, not causal
			// relationships. In particular, no runtime Fact causes an Event.
			if len(fact.Causes) != 0 {
				return errors.New("runtime observation unexpectedly carries a causal edge")
			}
			if _, err := writer.Append(fact); err != nil {
				return err
			}
		}
	}
	return nil
}
