package agency

import (
	"fmt"
	"sort"
)

// Consequence is the complete closed set of Agent-declarable R7 effects.
// Semantic kind labels do not extend this set.
type Consequence uint8

const (
	ConsequenceInvalid Consequence = iota
	ConsequenceCreateHandlings
	ConsequenceAdvanceHandling
	ConsequenceResolveCompleted
	ConsequenceResolveDeclined
	ConsequenceResolveUnresolved
	ConsequencePublishReference
	ConsequenceSupersedeReference
	ConsequenceRetractReference
)

func (c Consequence) String() string {
	switch c {
	case ConsequenceCreateHandlings:
		return "handling.create"
	case ConsequenceAdvanceHandling:
		return "handling.advance"
	case ConsequenceResolveCompleted:
		return "handling.resolve.completed"
	case ConsequenceResolveDeclined:
		return "handling.resolve.declined"
	case ConsequenceResolveUnresolved:
		return "handling.resolve.unresolved"
	case ConsequencePublishReference:
		return "reference.publish"
	case ConsequenceSupersedeReference:
		return "reference.supersede"
	case ConsequenceRetractReference:
		return "reference.retract"
	default:
		return ""
	}
}

func (c Consequence) Valid() bool {
	return c >= ConsequenceCreateHandlings && c <= ConsequenceRetractReference
}

func (c Consequence) subjectBound() bool {
	return c >= ConsequenceAdvanceHandling && c <= ConsequenceResolveUnresolved
}

func (c Consequence) referenceBound() bool {
	return c >= ConsequencePublishReference && c <= ConsequenceRetractReference
}

type TargetRef struct {
	self  bool
	alias OpaqueHandle
}

func SelfTarget() TargetRef { return TargetRef{self: true} }

func AliasTarget(alias OpaqueHandle) (TargetRef, error) {
	if alias.IsZero() {
		return TargetRef{}, invalid("target alias", "must not be zero")
	}
	if alias.String() == "self" {
		return TargetRef{}, invalid("target alias", "self is reserved for the local Principal")
	}
	return TargetRef{alias: alias}, nil
}

func (target TargetRef) IsSelf() bool        { return target.self }
func (target TargetRef) Alias() OpaqueHandle { return target.alias }
func (target TargetRef) IsZero() bool        { return !target.self && target.alias.IsZero() }
func (target TargetRef) canonicalKey() string {
	if target.self {
		return "self"
	}
	return "alias:" + target.alias.String()
}

type ArtifactInputKind uint8

const (
	ArtifactInputInvalid ArtifactInputKind = iota
	ArtifactInputCandidate
	ArtifactInputViewHandle
)

type ArtifactInput struct {
	kind   ArtifactInputKind
	handle OpaqueHandle
}

func NewArtifactCandidate(handle OpaqueHandle) (ArtifactInput, error) {
	return newArtifactInput(ArtifactInputCandidate, handle)
}

func NewArtifactViewHandle(handle OpaqueHandle) (ArtifactInput, error) {
	return newArtifactInput(ArtifactInputViewHandle, handle)
}

func newArtifactInput(kind ArtifactInputKind, handle OpaqueHandle) (ArtifactInput, error) {
	if handle.IsZero() {
		return ArtifactInput{}, invalid("Artifact input", "handle must not be zero")
	}
	return ArtifactInput{kind: kind, handle: handle}, nil
}

func (input ArtifactInput) Kind() ArtifactInputKind { return input.kind }
func (input ArtifactInput) Handle() OpaqueHandle    { return input.handle }
func (input ArtifactInput) canonicalKey() string {
	return fmt.Sprintf("%d:%s", input.kind, input.handle.String())
}

type IntentSpec struct {
	Kind              SemanticLabel
	Payload           SemanticPayload
	Consequence       Consequence
	SubjectHandling   OpaqueHandle
	Successors        []TargetRef
	ReferenceKey      ReferenceKey
	ReferenceHead     OpaqueHandle
	Artifacts         []ArtifactInput
	CausationHandles  []OpaqueHandle
	CorrelationHandle OpaqueHandle
}

// AgentIntent contains only Agent-selectable semantics. It intentionally has
// no identity, timestamp, principal, attachment, fence, digest, or accepted
// state field.
type AgentIntent struct {
	kind              SemanticLabel
	payload           SemanticPayload
	consequence       Consequence
	subjectHandling   OpaqueHandle
	successors        []TargetRef
	referenceKey      ReferenceKey
	referenceHead     OpaqueHandle
	artifacts         []ArtifactInput
	causationHandles  []OpaqueHandle
	correlationHandle OpaqueHandle
	canonical         []byte
}

func NewAgentIntent(spec IntentSpec) (AgentIntent, error) {
	if spec.Kind.IsZero() {
		return AgentIntent{}, invalid("Intent kind", "must not be zero")
	}
	if !spec.Consequence.Valid() {
		return AgentIntent{}, invalid("Intent consequence", "must be a closed consequence")
	}
	if err := validateIntentShape(spec); err != nil {
		return AgentIntent{}, err
	}
	if len(spec.Successors) > MaxSuccessors {
		return AgentIntent{}, limit("Intent successors", len(spec.Successors), MaxSuccessors)
	}
	for _, target := range spec.Successors {
		if target.IsZero() {
			return AgentIntent{}, invalid("Intent successors", "contains a zero target")
		}
	}
	seenTargets := make(map[string]struct{}, len(spec.Successors))
	for _, target := range spec.Successors {
		key := target.canonicalKey()
		if _, exists := seenTargets[key]; exists {
			return AgentIntent{}, invalid("Intent successors", "contains a duplicate target")
		}
		seenTargets[key] = struct{}{}
	}
	if len(spec.Artifacts) > MaxArtifactInputs {
		return AgentIntent{}, limit("Intent Artifacts", len(spec.Artifacts), MaxArtifactInputs)
	}
	seenArtifacts := make(map[string]struct{}, len(spec.Artifacts))
	for _, artifact := range spec.Artifacts {
		if artifact.kind != ArtifactInputCandidate && artifact.kind != ArtifactInputViewHandle {
			return AgentIntent{}, invalid("Intent Artifacts", "contains an invalid input kind")
		}
		if artifact.handle.IsZero() {
			return AgentIntent{}, invalid("Intent Artifacts", "contains a zero handle")
		}
		key := artifact.canonicalKey()
		if _, exists := seenArtifacts[key]; exists {
			return AgentIntent{}, invalid("Intent Artifacts", "contains a duplicate input")
		}
		seenArtifacts[key] = struct{}{}
	}
	causation, err := normalizeHandles("Intent causation", spec.CausationHandles, MaxCausationHandles)
	if err != nil {
		return AgentIntent{}, err
	}
	intent := AgentIntent{kind: spec.Kind, payload: spec.Payload, consequence: spec.Consequence,
		subjectHandling: spec.SubjectHandling, successors: append([]TargetRef(nil), spec.Successors...),
		referenceKey: spec.ReferenceKey, referenceHead: spec.ReferenceHead,
		artifacts: append([]ArtifactInput(nil), spec.Artifacts...), causationHandles: causation,
		correlationHandle: spec.CorrelationHandle}
	canonical, _, err := canonicalJSON(intent.wire())
	if err != nil {
		return AgentIntent{}, err
	}
	if len(canonical) > MaxIntentCanonicalBytes {
		return AgentIntent{}, limit("Intent canonical bytes", len(canonical), MaxIntentCanonicalBytes)
	}
	intent.canonical = canonical
	return intent, nil
}

func validateIntentShape(spec IntentSpec) error {
	switch spec.Consequence {
	case ConsequenceCreateHandlings:
		return validateRootIntentShape(spec)
	case ConsequenceAdvanceHandling, ConsequenceResolveCompleted,
		ConsequenceResolveDeclined, ConsequenceResolveUnresolved:
		return validateSubjectIntentShape(spec)
	case ConsequencePublishReference:
		return validateReferencePublishShape(spec)
	case ConsequenceSupersedeReference:
		return validateReferenceSupersedeShape(spec)
	case ConsequenceRetractReference:
		return validateReferenceRetractShape(spec)
	}
	return nil
}

func validateRootIntentShape(spec IntentSpec) error {
	if !spec.SubjectHandling.IsZero() || !spec.ReferenceKey.IsZero() || !spec.ReferenceHead.IsZero() {
		return invariant("root Intent", "cannot name a subject Handling or Reference")
	}
	if len(spec.Successors) == 0 {
		return invariant("root Intent", "must create at least one successor")
	}
	return nil
}

func validateSubjectIntentShape(spec IntentSpec) error {
	if spec.SubjectHandling.IsZero() || !spec.ReferenceKey.IsZero() || !spec.ReferenceHead.IsZero() {
		return invariant("subject Intent", "requires one subject and no Reference")
	}
	return nil
}

func validateReferencePublishShape(spec IntentSpec) error {
	if !spec.SubjectHandling.IsZero() || len(spec.Successors) != 0 || spec.ReferenceKey.IsZero() ||
		!spec.ReferenceHead.IsZero() || len(spec.Artifacts) != 1 {
		return invariant("Reference publish", "requires one new key and one Artifact only")
	}
	return nil
}

func validateReferenceSupersedeShape(spec IntentSpec) error {
	if !spec.SubjectHandling.IsZero() || len(spec.Successors) != 0 || !spec.ReferenceKey.IsZero() ||
		spec.ReferenceHead.IsZero() || len(spec.Artifacts) != 1 {
		return invariant("Reference supersede", "requires one offered head and one Artifact only")
	}
	return nil
}

func validateReferenceRetractShape(spec IntentSpec) error {
	if !spec.SubjectHandling.IsZero() || len(spec.Successors) != 0 || !spec.ReferenceKey.IsZero() ||
		spec.ReferenceHead.IsZero() || len(spec.Artifacts) != 0 {
		return invariant("Reference retract", "requires one offered head and no Artifact")
	}
	return nil
}

func normalizeHandles(field string, handles []OpaqueHandle, maximum int) ([]OpaqueHandle, error) {
	if len(handles) > maximum {
		return nil, limit(field, len(handles), maximum)
	}
	result := append([]OpaqueHandle(nil), handles...)
	for _, handle := range result {
		if handle.IsZero() {
			return nil, invalid(field, "contains a zero handle")
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].String() < result[j].String() })
	for index := 1; index < len(result); index++ {
		if result[index] == result[index-1] {
			return nil, invalid(field, "contains a duplicate handle")
		}
	}
	return result, nil
}

func (intent AgentIntent) Kind() SemanticLabel           { return intent.kind }
func (intent AgentIntent) Payload() SemanticPayload      { return intent.payload }
func (intent AgentIntent) Consequence() Consequence      { return intent.consequence }
func (intent AgentIntent) SubjectHandling() OpaqueHandle { return intent.subjectHandling }
func (intent AgentIntent) Successors() []TargetRef {
	return append([]TargetRef(nil), intent.successors...)
}
func (intent AgentIntent) ReferenceKey() ReferenceKey  { return intent.referenceKey }
func (intent AgentIntent) ReferenceHead() OpaqueHandle { return intent.referenceHead }
func (intent AgentIntent) Artifacts() []ArtifactInput {
	return append([]ArtifactInput(nil), intent.artifacts...)
}
func (intent AgentIntent) CausationHandles() []OpaqueHandle {
	return append([]OpaqueHandle(nil), intent.causationHandles...)
}
func (intent AgentIntent) CorrelationHandle() OpaqueHandle { return intent.correlationHandle }
func (intent AgentIntent) CanonicalJSON() []byte           { return copyBytes(intent.canonical) }

type intentWire struct {
	Kind              string              `json:"kind"`
	Payload           string              `json:"payload"`
	Consequence       string              `json:"consequence"`
	SubjectHandling   string              `json:"subject_handling,omitempty"`
	Successors        []targetWire        `json:"successors,omitempty"`
	ReferenceKey      string              `json:"reference_key,omitempty"`
	ReferenceHead     string              `json:"reference_head,omitempty"`
	Artifacts         []artifactInputWire `json:"artifacts,omitempty"`
	CausationHandles  []string            `json:"causation_handles,omitempty"`
	CorrelationHandle string              `json:"correlation_handle,omitempty"`
}

type targetWire struct {
	Self  bool   `json:"self,omitempty"`
	Alias string `json:"alias,omitempty"`
}

type artifactInputWire struct {
	Kind   string `json:"kind"`
	Handle string `json:"handle"`
}

func (intent AgentIntent) wire() intentWire {
	wire := intentWire{Kind: intent.kind.String(), Payload: intent.payload.String(),
		Consequence: intent.consequence.String(), SubjectHandling: intent.subjectHandling.String(),
		ReferenceKey: intent.referenceKey.String(), ReferenceHead: intent.referenceHead.String(),
		CorrelationHandle: intent.correlationHandle.String()}
	for _, target := range intent.successors {
		wire.Successors = append(wire.Successors, targetWire{Self: target.self, Alias: target.alias.String()})
	}
	for _, artifact := range intent.artifacts {
		kind := "candidate"
		if artifact.kind == ArtifactInputViewHandle {
			kind = "view_handle"
		}
		wire.Artifacts = append(wire.Artifacts, artifactInputWire{Kind: kind, Handle: artifact.handle.String()})
	}
	for _, handle := range intent.causationHandles {
		wire.CausationHandles = append(wire.CausationHandles, handle.String())
	}
	return wire
}

func (intent AgentIntent) String() string {
	return fmt.Sprintf("AgentIntent{%s %s}", intent.kind.String(), intent.consequence.String())
}
