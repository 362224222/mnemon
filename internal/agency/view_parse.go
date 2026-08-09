package agency

// ParseViewAuthorityCanonicalJSON reconstructs one private View authority from
// its exact durable encoding. The encoding deliberately omits the short-lived
// Attachment envelope, so callers must supply the separately authenticated
// Attachment that owns this view.
func ParseViewAuthorityCanonicalJSON(data []byte, attachment Attachment) (ViewAuthority, error) {
	var wire machineViewWire
	if err := decodeCanonicalObject("View authority JSON", data, MaxViewCanonicalBytes, &wire); err != nil {
		return ViewAuthority{}, err
	}
	if wire.SchemaVersion != viewAuthorityVersion {
		return ViewAuthority{}, invalid("View authority schema version", "is unsupported")
	}
	principal, err := NewAgentPrincipalID(wire.SourcePrincipal)
	if err != nil {
		return ViewAuthority{}, err
	}
	if attachment.ID().IsZero() || principal != attachment.Principal() || wire.MayInitiate != attachment.MayInitiate() {
		return ViewAuthority{}, invariant("View authority Attachment", "Principal or initiation authority differs")
	}

	spec, err := machineViewSpecFromWire(wire, attachment)
	if err != nil {
		return ViewAuthority{}, err
	}
	view, err := NewViewAuthority(spec)
	if err != nil {
		return ViewAuthority{}, err
	}
	if err := requireReconstructedCanonical("View authority JSON", data, view.CanonicalJSON()); err != nil {
		return ViewAuthority{}, err
	}
	return view, nil
}

func machineViewSpecFromWire(wire machineViewWire, attachment Attachment) (MachineViewSpec, error) {
	spec := MachineViewSpec{Attachment: attachment,
		ReplyObservationPending: wire.ReplyObservationPending}
	var err error
	if spec.Consequences, err = parseViewConsequences(wire.Consequences); err != nil {
		return MachineViewSpec{}, err
	}
	if spec.Subjects, err = parseViewSubjects(wire.Subjects); err != nil {
		return MachineViewSpec{}, err
	}
	if spec.References, err = parseViewReferences(wire.References); err != nil {
		return MachineViewSpec{}, err
	}
	if spec.Targets, err = parseViewTargets(wire.Targets); err != nil {
		return MachineViewSpec{}, err
	}
	if wire.ReplyTo != "" {
		if spec.ReplyTo, err = NewOpaqueHandle(wire.ReplyTo); err != nil {
			return MachineViewSpec{}, err
		}
	}
	if spec.ReplyTarget, err = parseViewReplyTarget(wire.ReplyTarget); err != nil {
		return MachineViewSpec{}, err
	}
	if wire.ReplyDelivery != "" {
		if spec.ReplyDelivery, err = ParseDeliveryID(wire.ReplyDelivery); err != nil {
			return MachineViewSpec{}, err
		}
	}
	if spec.Artifacts, err = parseViewArtifacts(wire.Artifacts); err != nil {
		return MachineViewSpec{}, err
	}
	if spec.Provenance, err = parseViewProvenance(wire.Provenance); err != nil {
		return MachineViewSpec{}, err
	}
	return spec, nil
}

func parseViewReplyTarget(wire *targetWire) (TargetRef, error) {
	if wire == nil {
		return TargetRef{}, nil
	}
	target, err := parseTarget(*wire)
	if err != nil {
		return TargetRef{}, err
	}
	if target.IsSelf() {
		return TargetRef{}, invalid("View reply target", "must be a remote alias")
	}
	return target, nil
}

func parseViewConsequences(values []string) ([]Consequence, error) {
	result := make([]Consequence, 0, len(values))
	for _, value := range values {
		consequence, err := parseConsequence(value)
		if err != nil {
			return nil, err
		}
		result = append(result, consequence)
	}
	return result, nil
}

func parseViewSubjects(wires []viewSubjectWire) ([]SubjectBinding, error) {
	result := make([]SubjectBinding, 0, len(wires))
	for _, wire := range wires {
		handle, err := NewOpaqueHandle(wire.Handle)
		if err != nil {
			return nil, err
		}
		handlingID, err := NewHandlingID(wire.Binding.HandlingID)
		if err != nil {
			return nil, err
		}
		head, err := parseEventRef(wire.Binding.Head)
		if err != nil {
			return nil, err
		}
		binding, err := NewSubjectBinding(handle, handlingID, head, wire.Binding.Fence,
			wire.Binding.ObservationRevision)
		if err != nil {
			return nil, err
		}
		result = append(result, binding)
	}
	return result, nil
}

func parseViewReferences(wires []viewReferenceWire) ([]ReferenceExpectation, error) {
	result := make([]ReferenceExpectation, 0, len(wires))
	for _, wire := range wires {
		if wire.Head.Absent || wire.Head.Head == nil {
			return nil, invalid("View Reference", "must contain an exact locally accepted head")
		}
		handle, err := NewOpaqueHandle(wire.Handle)
		if err != nil {
			return nil, err
		}
		key, err := NewReferenceKey(wire.Head.Key)
		if err != nil {
			return nil, err
		}
		head, err := parseEventRef(*wire.Head.Head)
		if err != nil {
			return nil, err
		}
		outcomes := AgentViewTerminalOutcomes{}
		if wire.TerminalOutcomes != nil {
			outcomes = AgentViewTerminalOutcomes{Completed: wire.TerminalOutcomes.Completed,
				Declined:   wire.TerminalOutcomes.Declined,
				Unresolved: wire.TerminalOutcomes.Unresolved}
		}
		expectation, err := ExpectReferenceHeadWithOutcomes(handle, key, head, outcomes)
		if err != nil {
			return nil, err
		}
		result = append(result, expectation)
	}
	return result, nil
}

func parseViewTargets(wires []viewTargetWire) ([]ResolvedTarget, error) {
	result := make([]ResolvedTarget, 0, len(wires))
	for _, wire := range wires {
		requested, err := parseTarget(wire.Requested)
		if err != nil {
			return nil, err
		}
		target, err := parseResolvedTarget(requested, wire.Resolved)
		if err != nil {
			return nil, err
		}
		result = append(result, target)
	}
	return result, nil
}

func parseResolvedTarget(requested TargetRef, wire resolvedTargetWire) (ResolvedTarget, error) {
	switch wire.Destination {
	case "local":
		if wire.RemoteRoute != "" || wire.RemoteAlias != "" {
			return ResolvedTarget{}, invalid("View local target", "must not contain remote authority")
		}
		principal, err := NewAgentPrincipalID(wire.LocalPrincipal)
		if err != nil {
			return ResolvedTarget{}, err
		}
		return ResolveLocalTarget(requested, principal)
	case "remote":
		if wire.LocalPrincipal != "" {
			return ResolvedTarget{}, invalid("View remote target", "must not contain local authority")
		}
		route, err := NewRouteID(wire.RemoteRoute)
		if err != nil {
			return ResolvedTarget{}, err
		}
		alias, err := NewOpaqueHandle(wire.RemoteAlias)
		if err != nil {
			return ResolvedTarget{}, err
		}
		return ResolveRemoteTarget(requested, route, alias)
	default:
		return ResolvedTarget{}, invalid("View target destination", "must be local or remote")
	}
}

func parseTarget(wire targetWire) (TargetRef, error) {
	if wire.Self == (wire.Alias != "") {
		return TargetRef{}, invalid("View requested target", "must contain exactly one of self or alias")
	}
	if wire.Self {
		return SelfTarget(), nil
	}
	alias, err := NewOpaqueHandle(wire.Alias)
	if err != nil {
		return TargetRef{}, err
	}
	return AliasTarget(alias)
}

func parseViewArtifacts(wires []viewArtifactOfferWire) ([]ViewArtifactOffer, error) {
	result := make([]ViewArtifactOffer, 0, len(wires))
	for _, wire := range wires {
		handle, err := NewOpaqueHandle(wire.Handle)
		if err != nil {
			return nil, err
		}
		digest, err := ParseDigest(wire.Digest)
		if err != nil {
			return nil, err
		}
		offer, err := NewViewArtifactOffer(handle, digest)
		if err != nil {
			return nil, err
		}
		result = append(result, offer)
	}
	return result, nil
}

func parseViewProvenance(wires []viewProvenanceOfferWire) ([]ProvenanceOffer, error) {
	result := make([]ProvenanceOffer, 0, len(wires))
	for _, wire := range wires {
		handle, err := NewOpaqueHandle(wire.Handle)
		if err != nil {
			return nil, err
		}
		event, err := parseEventRef(wire.Event)
		if err != nil {
			return nil, err
		}
		offer, err := NewProvenanceOffer(handle, event)
		if err != nil {
			return nil, err
		}
		result = append(result, offer)
	}
	return result, nil
}

func parseEventRef(wire eventRefWire) (EventRef, error) {
	id, err := NewEventID(wire.ID)
	if err != nil {
		return EventRef{}, err
	}
	digest, err := ParseDigest(wire.Digest)
	if err != nil {
		return EventRef{}, err
	}
	return NewEventRef(id, digest)
}
