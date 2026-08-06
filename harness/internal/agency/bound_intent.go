package agency

// BoundIntentSpec contains only the Agent candidate, stable replay key, sealed
// View, and immutable Artifact captures for that operation. All other
// authority is resolved from the View inside BindIntent.
type BoundIntentSpec struct {
	Intent       AgentIntent
	OperationKey OperationKey
	View         ViewAuthority
	Candidates   []CapturedCandidate
}

// BoundIntent is the canonical local request. Unlike AgentIntent, it contains
// machine-owned authority and resolved effects. Construction is the authority
// cut; callers cannot append authoritative fields afterward.
type BoundIntent struct {
	intent            AgentIntent
	operationKey      OperationKey
	attachment        Attachment
	viewDigest        Digest
	subject           *SubjectBinding
	expectedReference *ReferenceExpectation
	targets           []ResolvedTarget
	resolvedArtifacts []ResolvedArtifact
	artifacts         []Digest
	causation         []EventRef
	correlation       EventRef
	canonical         []byte
	digest            Digest
}

func BindIntent(spec BoundIntentSpec) (BoundIntent, error) {
	if len(spec.Intent.canonical) == 0 || spec.OperationKey.IsZero() ||
		len(spec.View.canonical) == 0 || spec.View.digest.IsZero() || spec.View.attachment.id.IsZero() {
		return BoundIntent{}, invalid("BoundIntent", "Intent, operation, and sealed View are required")
	}
	if !spec.View.offers(spec.Intent.consequence) {
		return BoundIntent{}, invariant("Intent binding", "consequence was not offered by the View")
	}
	if spec.Intent.consequence == ConsequenceCreateHandlings && !spec.View.attachment.mayInitiate {
		return BoundIntent{}, invariant("BoundIntent", "Attachment may not initiate root responsibility")
	}

	subject, expectedReference, err := resolveSubjectAndReference(spec.Intent, spec.View)
	if err != nil {
		return BoundIntent{}, err
	}
	targets, err := resolveTargets(spec.Intent.successors, spec.View.targets)
	if err != nil {
		return BoundIntent{}, err
	}
	resolvedArtifacts, artifacts, err := resolveArtifacts(spec.OperationKey, spec.Intent.artifacts,
		spec.View.artifacts, spec.Candidates)
	if err != nil {
		return BoundIntent{}, err
	}
	if spec.Intent.consequence == ConsequenceResolveCompleted && len(artifacts) == 0 {
		return BoundIntent{}, invariant("completed consequence", "requires a verified Artifact")
	}
	causation, correlation, err := resolveProvenance(spec.Intent, spec.View.provenance)
	if err != nil {
		return BoundIntent{}, err
	}
	if err := requireLocalResponsibilityAnchor(spec.Intent.consequence, targets,
		spec.View, spec.Intent.correlationHandle, correlation); err != nil {
		return BoundIntent{}, err
	}

	result := BoundIntent{
		intent:            spec.Intent,
		operationKey:      spec.OperationKey,
		attachment:        spec.View.attachment,
		viewDigest:        spec.View.digest,
		subject:           subject,
		expectedReference: expectedReference,
		targets:           targets,
		resolvedArtifacts: resolvedArtifacts,
		artifacts:         artifacts,
		causation:         causation,
		correlation:       correlation,
	}
	_, digest, err := canonicalJSON(result.requestWire())
	if err != nil {
		return BoundIntent{}, err
	}
	result.digest = digest
	canonical, _, err := canonicalJSON(result.wire())
	if err != nil {
		return BoundIntent{}, err
	}
	result.canonical = canonical
	return result, nil
}

func resolveSubjectAndReference(intent AgentIntent, view ViewAuthority) (
	*SubjectBinding, *ReferenceExpectation, error,
) {
	if intent.consequence.subjectBound() {
		subject, offered := view.subjects[intent.subjectHandling.String()]
		if !offered {
			return nil, nil, invariant("BoundIntent subject", "handle was not offered as a subject by the View")
		}
		copyValue := subject
		return &copyValue, nil, nil
	}
	if !intent.consequence.referenceBound() {
		return nil, nil, nil
	}
	if intent.consequence == ConsequencePublishReference {
		expected, err := ExpectAbsentReference(intent.referenceKey)
		if err != nil {
			return nil, nil, err
		}
		return nil, &expected, nil
	}
	expected, offered := view.references[intent.referenceHead.String()]
	if !offered {
		return nil, nil, invariant("BoundIntent Reference", "head was not offered by the View")
	}
	copyValue := expected
	return nil, &copyValue, nil
}

func resolveTargets(requested []TargetRef, offers map[string]ResolvedTarget) ([]ResolvedTarget, error) {
	targets := make([]ResolvedTarget, 0, len(requested))
	seenDestinations := make(map[resolvedTargetDestination]struct{}, len(requested))
	for _, target := range requested {
		resolved, offered := offers[target.canonicalKey()]
		if !offered {
			return nil, invariant("BoundIntent targets", "successor was not offered by the View")
		}
		destination := resolved.destinationKey()
		if _, duplicate := seenDestinations[destination]; duplicate {
			return nil, invariant("BoundIntent targets", "contains a duplicate resolved destination")
		}
		seenDestinations[destination] = struct{}{}
		targets = append(targets, resolved)
	}
	return targets, nil
}

type resolvedTargetDestination struct {
	kind           TargetDestination
	localPrincipal AgentPrincipalID
	remoteRoute    RouteID
	remoteAlias    OpaqueHandle
}

func (target ResolvedTarget) destinationKey() resolvedTargetDestination {
	return resolvedTargetDestination{
		kind: target.destination, localPrincipal: target.localPrincipal,
		remoteRoute: target.remoteRoute, remoteAlias: target.remoteAlias,
	}
}

func requireLocalResponsibilityAnchor(consequence Consequence, targets []ResolvedTarget,
	view ViewAuthority, correlationHandle OpaqueHandle, correlation EventRef,
) error {
	remote, local := false, false
	for _, target := range targets {
		remote = remote || target.destination == TargetDestinationRemote
		local = local || target.destination == TargetDestinationLocal
	}
	if !remote || consequence == ConsequenceAdvanceHandling {
		return nil
	}
	if isTerminalConsequence(consequence) &&
		exactCorrelatedReply(targets, view, correlationHandle, correlation) {
		return nil
	}
	if (consequence == ConsequenceCreateHandlings ||
		consequence == ConsequenceResolveCompleted ||
		consequence == ConsequenceResolveDeclined ||
		consequence == ConsequenceResolveUnresolved) && !local {
		return invariant("remote responsibility", "request must leave one causal local Handling open")
	}
	return nil
}

func isTerminalConsequence(consequence Consequence) bool {
	return consequence == ConsequenceResolveCompleted ||
		consequence == ConsequenceResolveDeclined ||
		consequence == ConsequenceResolveUnresolved
}

func exactCorrelatedReply(targets []ResolvedTarget, view ViewAuthority,
	correlationHandle OpaqueHandle, correlation EventRef,
) bool {
	if len(targets) != 1 || view.replyTo.IsZero() || view.replyTarget.IsZero() ||
		correlationHandle != view.replyTo || correlation.IsZero() ||
		targets[0].destination != TargetDestinationRemote ||
		targets[0].requested != view.replyTarget {
		return false
	}
	expected, offered := view.provenance[view.replyTo.String()]
	return offered && expected == correlation
}

func resolveProvenance(intent AgentIntent, offers map[string]EventRef) ([]EventRef, EventRef, error) {
	causation := make([]EventRef, 0, len(intent.causationHandles))
	for _, handle := range intent.causationHandles {
		event, offered := offers[handle.String()]
		if !offered {
			return nil, EventRef{}, invariant("BoundIntent causation", "handle was not offered as provenance by the View")
		}
		causation = append(causation, event)
	}
	var correlation EventRef
	if !intent.correlationHandle.IsZero() {
		var offered bool
		correlation, offered = offers[intent.correlationHandle.String()]
		if !offered {
			return nil, EventRef{}, invariant("BoundIntent correlation", "handle was not offered as provenance by the View")
		}
	}
	return causation, correlation, nil
}

func (intent BoundIntent) Intent() AgentIntent        { return intent.intent }
func (intent BoundIntent) OperationKey() OperationKey { return intent.operationKey }
func (intent BoundIntent) Attachment() Attachment     { return intent.attachment }
func (intent BoundIntent) ViewDigest() Digest         { return intent.viewDigest }
func (intent BoundIntent) Subject() (SubjectBinding, bool) {
	if intent.subject == nil {
		return SubjectBinding{}, false
	}
	return *intent.subject, true
}
func (intent BoundIntent) ExpectedReference() (ReferenceExpectation, bool) {
	if intent.expectedReference == nil {
		return ReferenceExpectation{}, false
	}
	return *intent.expectedReference, true
}
func (intent BoundIntent) Targets() []ResolvedTarget {
	return append([]ResolvedTarget(nil), intent.targets...)
}
func (intent BoundIntent) ResolvedArtifacts() []ResolvedArtifact {
	return append([]ResolvedArtifact(nil), intent.resolvedArtifacts...)
}
func (intent BoundIntent) Artifacts() []Digest { return append([]Digest(nil), intent.artifacts...) }
func (intent BoundIntent) Causation() []EventRef {
	return append([]EventRef(nil), intent.causation...)
}
func (intent BoundIntent) Correlation() (EventRef, bool) {
	return intent.correlation, !intent.correlation.IsZero()
}
func (intent BoundIntent) CanonicalJSON() []byte { return copyBytes(intent.canonical) }
func (intent BoundIntent) RequestDigest() Digest { return intent.digest }
