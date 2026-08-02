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
	if wire.SchemaVersion != 1 {
		return ViewAuthority{}, invalid("View authority schema version", "must be 1")
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
	spec := MachineViewSpec{Attachment: attachment}
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
	if spec.Artifacts, err = parseViewArtifacts(wire.Artifacts); err != nil {
		return MachineViewSpec{}, err
	}
	if spec.Provenance, err = parseViewProvenance(wire.Provenance); err != nil {
		return MachineViewSpec{}, err
	}
	return spec, nil
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
		binding, err := NewSubjectBinding(handle, handlingID, head, wire.Binding.Fence)
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
		expectation, err := ExpectReferenceHead(handle, key, head)
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
		var target ResolvedTarget
		switch wire.Resolved.Destination {
		case "local":
			if wire.Resolved.RemoteRoute != "" || wire.Resolved.RemoteAlias != "" {
				return nil, invalid("View local target", "must not contain remote authority")
			}
			principal, parseErr := NewAgentPrincipalID(wire.Resolved.LocalPrincipal)
			if parseErr != nil {
				return nil, parseErr
			}
			target, err = ResolveLocalTarget(requested, principal)
		case "remote":
			if wire.Resolved.LocalPrincipal != "" {
				return nil, invalid("View remote target", "must not contain local authority")
			}
			route, parseErr := NewRouteID(wire.Resolved.RemoteRoute)
			if parseErr != nil {
				return nil, parseErr
			}
			alias, parseErr := NewOpaqueHandle(wire.Resolved.RemoteAlias)
			if parseErr != nil {
				return nil, parseErr
			}
			target, err = ResolveRemoteTarget(requested, route, alias)
		default:
			return nil, invalid("View target destination", "must be local or remote")
		}
		if err != nil {
			return nil, err
		}
		result = append(result, target)
	}
	return result, nil
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
