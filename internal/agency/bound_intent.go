package agency

import "sort"

// BoundIntentSpec contains only already-resolved machine authority.
// NewBoundIntent validates its structural closure and canonicalizes it; it
// deliberately does not decide whether a View offered any value. That policy
// belongs to the authority package.
type BoundIntentSpec struct {
	Intent            AgentIntent
	OperationKey      OperationKey
	Attachment        Attachment
	ViewDigest        Digest
	Subject           *SubjectBinding
	ExpectedReference *ReferenceExpectation
	Targets           []ResolvedTarget
	Artifacts         []ResolvedArtifact
	Causation         []EventRef
	Correlation       EventRef
	InReplyToDelivery DeliveryID
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
	inReplyToDelivery DeliveryID
	canonical         []byte
	digest            Digest
}

// NewBoundIntent seals already-resolved authority into one canonical request.
// It prevents malformed values from being represented but performs no handle
// lookup and makes no admission decision.
func NewBoundIntent(spec BoundIntentSpec) (BoundIntent, error) {
	if len(spec.Intent.canonical) == 0 || spec.OperationKey.IsZero() ||
		spec.Attachment.id.IsZero() || spec.ViewDigest.IsZero() {
		return BoundIntent{}, invalid("BoundIntent", "Intent, operation, Attachment, and View digest are required")
	}
	if err := validateResolvedBoundIntent(spec); err != nil {
		return BoundIntent{}, err
	}
	artifacts := make([]Digest, len(spec.Artifacts))
	for index, resolved := range spec.Artifacts {
		artifacts[index] = resolved.digest
	}
	sortDigests(artifacts)
	for index := 1; index < len(artifacts); index++ {
		if artifacts[index] == artifacts[index-1] {
			return BoundIntent{}, invalid("BoundIntent Artifacts", "contains a duplicate digest")
		}
	}
	result := BoundIntent{intent: spec.Intent, operationKey: spec.OperationKey,
		attachment: spec.Attachment, viewDigest: spec.ViewDigest,
		targets:           append([]ResolvedTarget(nil), spec.Targets...),
		resolvedArtifacts: append([]ResolvedArtifact(nil), spec.Artifacts...),
		artifacts:         artifacts, causation: append([]EventRef(nil), spec.Causation...),
		correlation: spec.Correlation, inReplyToDelivery: spec.InReplyToDelivery}
	if spec.Subject != nil {
		copyValue := *spec.Subject
		result.subject = &copyValue
	}
	if spec.ExpectedReference != nil {
		copyValue := *spec.ExpectedReference
		result.expectedReference = &copyValue
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

func validateResolvedBoundIntent(spec BoundIntentSpec) error {
	consequence := spec.Intent.consequence
	if consequence.subjectBound() != (spec.Subject != nil) {
		return invariant("BoundIntent subject", "must match the closed consequence")
	}
	if consequence.referenceBound() != (spec.ExpectedReference != nil) {
		return invariant("BoundIntent Reference", "must match the closed consequence")
	}
	if spec.Subject != nil && (spec.Subject.handle != spec.Intent.subjectHandling ||
		spec.Subject.handlingID.IsZero() || spec.Subject.head.IsZero() || spec.Subject.fence == 0) {
		return invariant("BoundIntent subject", "does not bind the Intent handle")
	}
	if err := validateResolvedReference(spec.Intent, spec.ExpectedReference); err != nil {
		return err
	}
	if len(spec.Targets) != len(spec.Intent.successors) {
		return invariant("BoundIntent targets", "must resolve every requested successor exactly once")
	}
	for index, target := range spec.Targets {
		if target.requested != spec.Intent.successors[index] {
			return invariant("BoundIntent targets", "do not preserve requested successor order")
		}
	}
	if len(spec.Artifacts) != len(spec.Intent.artifacts) {
		return invariant("BoundIntent Artifacts", "must resolve every Artifact input exactly once")
	}
	for index, resolved := range spec.Artifacts {
		if resolved.input != spec.Intent.artifacts[index] || resolved.digest.IsZero() {
			return invariant("BoundIntent Artifacts", "do not bind the requested inputs")
		}
	}
	if consequence == ConsequenceResolveCompleted && len(spec.Artifacts) == 0 {
		return invariant("completed consequence", "requires a verified Artifact")
	}
	if len(spec.Causation) != len(spec.Intent.causationHandles) {
		return invariant("BoundIntent causation", "must resolve every provenance handle exactly once")
	}
	if spec.Correlation.IsZero() != spec.Intent.correlationHandle.IsZero() {
		return invariant("BoundIntent correlation", "must match the Intent correlation handle")
	}
	if !spec.InReplyToDelivery.IsZero() && !isTerminalConsequence(consequence) {
		return invariant("BoundIntent reply", "is allowed only for a terminal consequence")
	}
	return nil
}

func validateResolvedReference(intent AgentIntent, expected *ReferenceExpectation) error {
	if expected == nil {
		return nil
	}
	switch intent.consequence {
	case ConsequencePublishReference:
		if !expected.absent || expected.key != intent.referenceKey || !expected.head.IsZero() {
			return invariant("BoundIntent Reference", "does not bind first publication")
		}
	case ConsequenceSupersedeReference, ConsequenceRetractReference:
		if expected.absent || expected.handle != intent.referenceHead || expected.key.IsZero() || expected.head.IsZero() {
			return invariant("BoundIntent Reference", "does not bind the offered exact head")
		}
	}
	return nil
}

func sortDigests(values []Digest) {
	sort.Slice(values, func(i, j int) bool { return values[i].String() < values[j].String() })
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

func isTerminalConsequence(consequence Consequence) bool {
	return consequence == ConsequenceResolveCompleted ||
		consequence == ConsequenceResolveDeclined ||
		consequence == ConsequenceResolveUnresolved
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
func (intent BoundIntent) InReplyToDelivery() (DeliveryID, bool) {
	return intent.inReplyToDelivery, !intent.inReplyToDelivery.IsZero()
}
func (intent BoundIntent) CanonicalJSON() []byte { return copyBytes(intent.canonical) }
func (intent BoundIntent) RequestDigest() Digest { return intent.digest }
