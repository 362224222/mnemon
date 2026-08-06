package agency

// ParseAgentViewCanonicalJSON reconstructs one exact model-visible View and
// proves that all of its machine facts are the projection of authority. It
// retains no private authority and exposes only AgentView's safe handle and
// canonical byte accessors.
func ParseAgentViewCanonicalJSON(data []byte, authority ViewAuthority) (AgentView, error) {
	var wire agentViewWire
	if err := decodeCanonicalObject("Agent View JSON", data, MaxAgentViewCanonicalBytes, &wire); err != nil {
		return AgentView{}, err
	}
	if wire.Schema != AgentViewSchema || wire.Version != AgentViewVersion {
		return AgentView{}, invalid("Agent View envelope", "has an unsupported schema or version")
	}
	handle, err := NewOpaqueHandle(wire.View)
	if err != nil {
		return AgentView{}, err
	}
	current, err := agentViewCurrentSpecFromWire(wire.Current, authority)
	if err != nil {
		return AgentView{}, err
	}
	related, outstanding, err := agentViewRelatedSpecsFromWire(wire.Related,
		wire.Outstanding, authority)
	if err != nil {
		return AgentView{}, err
	}
	references, err := agentViewReferenceSpecsFromWire(wire.References, authority)
	if err != nil {
		return AgentView{}, err
	}
	if err := validateAgentViewDerivedWire(wire, authority); err != nil {
		return AgentView{}, err
	}
	view, err := NewAgentView(AgentViewSpec{
		Handle: handle, Authority: authority, Current: current, Related: related,
		Outstanding: outstanding, References: references,
	})
	if err != nil {
		return AgentView{}, err
	}
	if err := requireReconstructedCanonical("Agent View JSON", data, view.CanonicalJSON()); err != nil {
		return AgentView{}, err
	}
	return view, nil
}

func agentViewRelatedSpecsFromWire(wires []agentViewRelatedWire,
	outstandingWire agentViewOutstandingWire, authority ViewAuthority,
) ([]AgentViewRelatedSpec, AgentViewOutstanding, error) {
	result := make([]AgentViewRelatedSpec, 0, len(wires))
	for _, wire := range wires {
		spec, err := agentViewRelatedSpecFromWire(wire, authority)
		if err != nil {
			return nil, AgentViewOutstanding{}, err
		}
		result = append(result, spec)
	}
	outstanding := AgentViewOutstanding{OpenTotal: outstandingWire.OpenTotal,
		RelatedTotal:     outstandingWire.RelatedTotal,
		RelatedProjected: outstandingWire.RelatedProjected, Truncated: outstandingWire.Truncated}
	return result, outstanding, nil
}

func agentViewRelatedSpecFromWire(wire agentViewRelatedWire,
	authority ViewAuthority,
) (AgentViewRelatedSpec, error) {
	event, err := NewOpaqueHandle(wire.Facts.Event)
	if err != nil {
		return AgentViewRelatedSpec{}, err
	}
	if _, offered := authority.provenance[event.String()]; !offered {
		return AgentViewRelatedSpec{}, invariant("Agent View related Event",
			"does not match a sealed provenance offer")
	}
	relation, outcome, err := parseAgentViewRelatedRelation(wire.Facts)
	if err != nil {
		return AgentViewRelatedSpec{}, err
	}
	kind, err := NewSemanticLabel(wire.Semantic.Kind)
	if err != nil {
		return AgentViewRelatedSpec{}, err
	}
	payload, err := NewSemanticPayload(wire.Semantic.Payload)
	if err != nil {
		return AgentViewRelatedSpec{}, err
	}
	artifacts, err := parseAgentViewRelatedArtifacts(wire.Facts.Artifacts, authority)
	if err != nil {
		return AgentViewRelatedSpec{}, err
	}
	return AgentViewRelatedSpec{Event: event, Relation: relation, Outcome: outcome,
		Kind: kind, Payload: payload, Artifacts: artifacts}, nil
}

func parseAgentViewRelatedRelation(facts agentViewRelatedFactsWire) (
	AgentViewRelation, AgentViewTerminalOutcome, error,
) {
	switch facts.Relation {
	case "correlation":
		if facts.Outcome != "" {
			return AgentViewRelationInvalid, AgentViewTerminalOutcomeInvalid,
				invalid("Agent View related outcome", "must be absent for correlation")
		}
		return AgentViewRelationCorrelation, AgentViewTerminalOutcomeInvalid, nil
	case "terminal_reply":
		outcome, err := parseAgentViewTerminalOutcome(facts.Outcome)
		return AgentViewRelationTerminalReply, outcome, err
	default:
		return AgentViewRelationInvalid, AgentViewTerminalOutcomeInvalid,
			invalid("Agent View related relation", "must be correlation or terminal_reply")
	}
}

func parseAgentViewTerminalOutcome(value string) (AgentViewTerminalOutcome, error) {
	switch value {
	case "completed":
		return AgentViewTerminalOutcomeCompleted, nil
	case "declined":
		return AgentViewTerminalOutcomeDeclined, nil
	case "unresolved":
		return AgentViewTerminalOutcomeUnresolved, nil
	default:
		return AgentViewTerminalOutcomeInvalid, invalid("Agent View related outcome",
			"must be completed, declined, or unresolved for terminal_reply")
	}
}

func parseAgentViewRelatedArtifacts(wires []agentViewArtifactWire,
	authority ViewAuthority,
) ([]OpaqueHandle, error) {
	artifacts := make([]OpaqueHandle, 0, len(wires))
	for _, wire := range wires {
		handle, err := validatePublicArtifact(wire, authority)
		if err != nil {
			return nil, err
		}
		artifacts = append(artifacts, handle)
	}
	return artifacts, nil
}

func agentViewCurrentSpecFromWire(wire *agentViewCurrentWire, authority ViewAuthority) (*AgentViewCurrentSpec, error) {
	if wire == nil {
		return nil, nil
	}
	subject, err := NewOpaqueHandle(wire.Facts.Handle)
	if err != nil {
		return nil, err
	}
	replyTo, err := NewOpaqueHandle(wire.Facts.ReplyTo)
	if err != nil {
		return nil, err
	}
	if replyTo != authority.replyTo {
		return nil, invariant("Agent View current", "reply-to does not match sealed reply authority")
	}
	if _, offered := authority.provenance[replyTo.String()]; !offered {
		return nil, invariant("Agent View current", "reply-to does not match sealed provenance")
	}
	replyTarget, err := projectReplyTarget(authority)
	if err != nil {
		return nil, err
	}
	if wire.Facts.ReplyTarget != replyTarget {
		return nil, invariant("Agent View current", "reply-target does not match sealed target authority")
	}
	if wire.Facts.ReplyRequired != (replyTarget != "") {
		return nil, invariant("Agent View current", "reply requirement does not match sealed target authority")
	}
	kind, err := NewSemanticLabel(wire.Semantic.Kind)
	if err != nil {
		return nil, err
	}
	payload, err := NewSemanticPayload(wire.Semantic.Payload)
	if err != nil {
		return nil, err
	}
	artifacts := make([]OpaqueHandle, 0, len(wire.Facts.Artifacts))
	for _, artifactWire := range wire.Facts.Artifacts {
		handle, err := validatePublicArtifact(artifactWire, authority)
		if err != nil {
			return nil, err
		}
		artifacts = append(artifacts, handle)
	}
	return &AgentViewCurrentSpec{Subject: subject, ReplyTo: replyTo, Kind: kind,
		Payload: payload, Artifacts: artifacts}, nil
}

func agentViewReferenceSpecsFromWire(wires []agentViewReferenceWire, authority ViewAuthority) (
	[]AgentViewReferenceSpec, error,
) {
	result := make([]AgentViewReferenceSpec, 0, len(wires))
	for _, wire := range wires {
		head, err := NewOpaqueHandle(wire.Facts.Head)
		if err != nil {
			return nil, err
		}
		reference, offered := authority.references[head.String()]
		if !offered || wire.Facts.Key != reference.key.String() {
			return nil, invariant("Agent View Reference", "does not match a sealed Reference offer")
		}
		spec := AgentViewReferenceSpec{Head: head}
		if wire.Facts.TerminalOutcomes != nil {
			spec.TerminalOutcomes = AgentViewTerminalOutcomes{
				Completed:  wire.Facts.TerminalOutcomes.Completed,
				Declined:   wire.Facts.TerminalOutcomes.Declined,
				Unresolved: wire.Facts.TerminalOutcomes.Unresolved,
			}
		}
		switch wire.Facts.State {
		case "active":
			if wire.Facts.Artifact == nil {
				return nil, invalid("Agent View active Reference", "requires an Artifact")
			}
			spec.State = AgentViewReferenceStateActive
			spec.Artifact, err = validatePublicArtifact(*wire.Facts.Artifact, authority)
		case "retracted":
			if wire.Facts.Artifact != nil {
				return nil, invalid("Agent View retracted Reference", "must not contain an Artifact")
			}
			spec.State = AgentViewReferenceStateRetracted
		default:
			return nil, invalid("Agent View Reference state", "must be active or retracted")
		}
		if err != nil {
			return nil, err
		}
		result = append(result, spec)
	}
	return result, nil
}

func validatePublicArtifact(wire agentViewArtifactWire, authority ViewAuthority) (OpaqueHandle, error) {
	handle, err := NewOpaqueHandle(wire.Handle)
	if err != nil {
		return OpaqueHandle{}, err
	}
	digest, err := ParseDigest(wire.Digest)
	if err != nil {
		return OpaqueHandle{}, err
	}
	offer, offered := authority.artifacts[handle.String()]
	if !offered || offer.digest != digest {
		return OpaqueHandle{}, invariant("Agent View Artifact", "does not match a sealed Artifact offer")
	}
	return handle, nil
}

func validateAgentViewDerivedWire(wire agentViewWire, authority ViewAuthority) error {
	if err := validatePublicTargets(wire.Targets, authority); err != nil {
		return err
	}
	if err := validatePublicIntentShapes(wire.AllowedIntents, authority); err != nil {
		return err
	}
	return validatePublicProvenance(wire.Provenance, authority)
}

func validatePublicTargets(values []string, authority ViewAuthority) error {
	expected := projectTargetAliases(authority.targets)
	if len(values) != len(expected) {
		return invariant("Agent View targets", "do not match sealed target offers")
	}
	seen := make(map[string]struct{}, len(values))
	for index, value := range values {
		if value != "self" {
			if _, err := NewOpaqueHandle(value); err != nil {
				return err
			}
		}
		if _, duplicate := seen[value]; duplicate {
			return invalid("Agent View targets", "contains a duplicate target")
		}
		seen[value] = struct{}{}
		if value != expected[index] {
			return invariant("Agent View targets", "do not match sealed target offers")
		}
	}
	return nil
}

func validatePublicIntentShapes(wires []agentViewIntentShapeWire, authority ViewAuthority) error {
	expected := projectAllowedIntentShapes(authority.consequences)
	if len(wires) != len(expected) {
		return invariant("Agent View allowed intents", "do not match sealed consequences")
	}
	seen := make(map[Consequence]struct{}, len(wires))
	for index, wire := range wires {
		consequence, err := parseConsequence(wire.Consequence)
		if err != nil {
			return err
		}
		if _, duplicate := seen[consequence]; duplicate {
			return invalid("Agent View allowed intents", "contains a duplicate consequence")
		}
		seen[consequence] = struct{}{}
		if wire != expected[index] {
			return invariant("Agent View allowed intent", "does not match the closed consequence shape")
		}
	}
	return nil
}

func validatePublicProvenance(values []string, authority ViewAuthority) error {
	expected := projectProvenanceHandles(authority.provenance)
	if len(values) != len(expected) {
		return invariant("Agent View provenance", "does not match sealed provenance offers")
	}
	for index, value := range values {
		if _, err := NewOpaqueHandle(value); err != nil {
			return err
		}
		if index > 0 && value == values[index-1] {
			return invalid("Agent View provenance", "contains a duplicate handle")
		}
		if value != expected[index] {
			return invariant("Agent View provenance", "does not match sealed provenance offers")
		}
	}
	return nil
}
