package agency

import "sort"

// AgentViewRelatedSpec is one read-only Event directly related to the current
// responsibility. Event is provenance, never a writable subject.
type AgentViewRelatedSpec struct {
	Event     OpaqueHandle
	Relation  AgentViewRelation
	Kind      SemanticLabel
	Payload   SemanticPayload
	Artifacts []OpaqueHandle
}

// AgentViewRelation is the closed machine-derived edge between Current and a
// related Event. Open semantic kind labels never extend this set.
type AgentViewRelation uint8

const (
	AgentViewRelationInvalid AgentViewRelation = iota
	AgentViewRelationCorrelation
)

func (relation AgentViewRelation) String() string {
	if relation == AgentViewRelationCorrelation {
		return "correlation"
	}
	return ""
}

// AgentViewOutstanding is a bounded, read-only projection. Counts describe
// locally accepted open Handlings; they grant no claim, fence, or action.
type AgentViewOutstanding struct {
	OpenTotal        int
	RelatedTotal     int
	RelatedProjected int
	Truncated        bool
}

func projectRelated(specs []AgentViewRelatedSpec, outstanding AgentViewOutstanding,
	authority ViewAuthority,
) ([]agentViewRelatedWire, map[string]struct{}, error) {
	if len(specs) > MaxAgentViewRelatedOpen {
		return nil, nil, limit("Agent View related open", len(specs), MaxAgentViewRelatedOpen)
	}
	if outstanding.OpenTotal < 0 || outstanding.RelatedTotal < 0 ||
		outstanding.RelatedProjected < 0 || outstanding.RelatedTotal > outstanding.OpenTotal ||
		outstanding.RelatedProjected != len(specs) ||
		outstanding.Truncated != (outstanding.RelatedProjected < outstanding.RelatedTotal) {
		return nil, nil, invariant("Agent View outstanding", "counts do not match the related projection")
	}
	artifacts := make(map[string]struct{})
	seenEvents := make(map[string]struct{}, len(specs))
	wires := make([]agentViewRelatedWire, 0, len(specs))
	for _, spec := range specs {
		wire, err := projectRelatedEvent(spec, authority, artifacts, seenEvents)
		if err != nil {
			return nil, nil, err
		}
		wires = append(wires, wire)
	}
	sort.Slice(wires, func(i, j int) bool { return wires[i].Facts.Event < wires[j].Facts.Event })
	return wires, artifacts, nil
}

func projectRelatedEvent(spec AgentViewRelatedSpec, authority ViewAuthority,
	artifacts, seenEvents map[string]struct{},
) (agentViewRelatedWire, error) {
	if spec.Event.IsZero() || spec.Kind.IsZero() || spec.Relation.String() == "" {
		return agentViewRelatedWire{}, invalid("Agent View related Event", "event, relation, and kind are required")
	}
	if _, offered := authority.provenance[spec.Event.String()]; !offered {
		return agentViewRelatedWire{}, invariant("Agent View related Event", "event was not offered as provenance")
	}
	if _, duplicate := seenEvents[spec.Event.String()]; duplicate {
		return agentViewRelatedWire{}, invalid("Agent View related Event", "contains a duplicate event")
	}
	seenEvents[spec.Event.String()] = struct{}{}
	artifactWires := make([]agentViewArtifactWire, 0, len(spec.Artifacts))
	for _, handle := range spec.Artifacts {
		offer, offered := authority.artifacts[handle.String()]
		if handle.IsZero() || !offered {
			return agentViewRelatedWire{}, invariant("Agent View related Artifacts", "handle was not offered")
		}
		if _, duplicate := artifacts[handle.String()]; duplicate {
			return agentViewRelatedWire{}, invalid("Agent View related Artifacts", "contains a duplicate handle")
		}
		artifacts[handle.String()] = struct{}{}
		artifactWires = append(artifactWires, publicArtifactWire(handle, offer.digest))
	}
	sort.Slice(artifactWires, func(i, j int) bool { return artifactWires[i].Handle < artifactWires[j].Handle })
	return agentViewRelatedWire{
		Facts: agentViewRelatedFactsWire{Event: spec.Event.String(), Relation: spec.Relation.String(),
			Artifacts: artifactWires},
		Semantic: agentViewSemanticWire{Kind: spec.Kind.String(), Payload: spec.Payload.String()},
	}, nil
}

func projectOutstanding(value AgentViewOutstanding) agentViewOutstandingWire {
	return agentViewOutstandingWire{OpenTotal: value.OpenTotal, RelatedTotal: value.RelatedTotal,
		RelatedProjected: value.RelatedProjected, Truncated: value.Truncated}
}

type agentViewRelatedWire struct {
	Facts    agentViewRelatedFactsWire `json:"facts"`
	Semantic agentViewSemanticWire     `json:"semantic"`
}

type agentViewRelatedFactsWire struct {
	Event     string                  `json:"event"`
	Relation  string                  `json:"relation"`
	Artifacts []agentViewArtifactWire `json:"artifacts,omitempty"`
}

type agentViewOutstandingWire struct {
	OpenTotal        int  `json:"open_total"`
	RelatedTotal     int  `json:"related_total"`
	RelatedProjected int  `json:"related_projected"`
	Truncated        bool `json:"truncated"`
}
