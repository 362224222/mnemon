package agency

import "sort"

const (
	AgentViewSchema              = "mnemon.agent.view"
	AgentViewVersion             = 2
	MaxAgentViewCanonicalBytes   = 16 << 10
	MaxAgentViewReferences       = 16
	MaxAgentViewCurrentArtifacts = MaxArtifactInputs
)

// AgentViewCurrentSpec supplies the semantic content associated with the one
// current responsibility. Subject and Artifact handles must already be typed
// offers in Authority; their machine-owned bindings never enter AgentView.
type AgentViewCurrentSpec struct {
	Subject   OpaqueHandle
	Kind      SemanticLabel
	Payload   SemanticPayload
	Artifacts []OpaqueHandle
}

// AgentViewReferenceState is the closed projected state of a locally accepted
// Reference head. A retracted tombstone remains offerable for supersession but
// carries no Artifact.
type AgentViewReferenceState uint8

const (
	AgentViewReferenceStateInvalid AgentViewReferenceState = iota
	AgentViewReferenceStateActive
	AgentViewReferenceStateRetracted
)

// AgentViewReferenceSpec associates one offered Reference head with its
// projected state and, when active, its offered Artifact. The private Event
// head remains sealed in ViewAuthority.
type AgentViewReferenceSpec struct {
	Head             OpaqueHandle
	State            AgentViewReferenceState
	Artifact         OpaqueHandle
	TerminalOutcomes AgentViewTerminalOutcomes
}

// AgentViewTerminalOutcomes is a bounded factual projection of accepted
// terminal Events that directly cited one exact Reference head. These counts
// are neither a quality score nor authority used by admission.
type AgentViewTerminalOutcomes struct {
	Completed  int64
	Declined   int64
	Unresolved int64
}

// AgentViewSpec is the narrow projection seam used by the local authority.
// ViewAuthority remains the sole source of offered handles and consequences;
// this spec adds only bounded Agent-facing semantics and associations.
type AgentViewSpec struct {
	Handle     OpaqueHandle
	Authority  ViewAuthority
	Current    *AgentViewCurrentSpec
	References []AgentViewReferenceSpec
}

// AgentView is the canonical bounded world shown to an Agent. It deliberately
// contains no private authority digest, identity, fence, route, credential, or
// operation material.
type AgentView struct {
	handle    OpaqueHandle
	canonical []byte
}

func NewAgentView(spec AgentViewSpec) (AgentView, error) {
	if spec.Handle.IsZero() || spec.Authority.digest.IsZero() || len(spec.Authority.canonical) == 0 {
		return AgentView{}, invalid("Agent View", "handle and sealed authority are required")
	}
	current, artifactHandles, err := projectCurrent(spec.Current, spec.Authority)
	if err != nil {
		return AgentView{}, err
	}
	references, referenceArtifacts, err := projectReferences(spec.References, spec.Authority)
	if err != nil {
		return AgentView{}, err
	}
	for handle := range referenceArtifacts {
		artifactHandles[handle] = struct{}{}
	}
	if err := requireExactArtifactProjection(artifactHandles, spec.Authority.artifacts); err != nil {
		return AgentView{}, err
	}
	if err := validateProjectedIntentShapes(spec.Authority, current, len(references)); err != nil {
		return AgentView{}, err
	}

	wire := agentViewWire{
		Schema:         AgentViewSchema,
		Version:        AgentViewVersion,
		View:           spec.Handle.String(),
		Current:        current,
		References:     references,
		Targets:        projectTargetAliases(spec.Authority.targets),
		AllowedIntents: projectAllowedIntentShapes(spec.Authority.consequences),
		Provenance:     projectProvenanceHandles(spec.Authority.provenance),
	}
	canonical, _, err := canonicalJSON(wire)
	if err != nil {
		return AgentView{}, err
	}
	if len(canonical) > MaxAgentViewCanonicalBytes {
		return AgentView{}, limit("Agent View canonical bytes", len(canonical), MaxAgentViewCanonicalBytes)
	}
	return AgentView{handle: spec.Handle, canonical: canonical}, nil
}

func validateProjectedIntentShapes(authority ViewAuthority, current *agentViewCurrentWire, referenceCount int) error {
	for consequence := range authority.consequences {
		if consequence.subjectBound() && current == nil {
			return invariant("Agent View allowed intents", "subject consequence requires a current responsibility")
		}
		if consequence == ConsequenceCreateHandlings && len(authority.targets) == 0 {
			return invariant("Agent View allowed intents", "create consequence requires a target")
		}
		if (consequence == ConsequenceSupersedeReference || consequence == ConsequenceRetractReference) &&
			referenceCount == 0 {
			return invariant("Agent View allowed intents", "head-bound Reference consequence requires a Reference")
		}
	}
	return nil
}

func (view AgentView) Handle() OpaqueHandle  { return view.handle }
func (view AgentView) CanonicalJSON() []byte { return copyBytes(view.canonical) }

func projectCurrent(spec *AgentViewCurrentSpec, authority ViewAuthority) (*agentViewCurrentWire, map[string]struct{}, error) {
	artifacts := make(map[string]struct{})
	if spec == nil {
		if len(authority.subjects) != 0 {
			return nil, nil, invariant("Agent View current", "sealed subject must be projected")
		}
		return nil, artifacts, nil
	}
	if spec.Subject.IsZero() || spec.Kind.IsZero() {
		return nil, nil, invalid("Agent View current", "subject and kind are required")
	}
	if len(spec.Artifacts) > MaxAgentViewCurrentArtifacts {
		return nil, nil, limit("Agent View current Artifacts", len(spec.Artifacts), MaxAgentViewCurrentArtifacts)
	}
	if _, offered := authority.subjects[spec.Subject.String()]; !offered || len(authority.subjects) != 1 {
		return nil, nil, invariant("Agent View current", "subject was not the sealed current subject")
	}
	artifactWires := make([]agentViewArtifactWire, 0, len(spec.Artifacts))
	for _, handle := range spec.Artifacts {
		offer, offered := authority.artifacts[handle.String()]
		if handle.IsZero() || !offered {
			return nil, nil, invariant("Agent View current Artifacts", "handle was not offered by sealed authority")
		}
		if _, duplicate := artifacts[handle.String()]; duplicate {
			return nil, nil, invalid("Agent View current Artifacts", "contains a duplicate handle")
		}
		artifacts[handle.String()] = struct{}{}
		artifactWires = append(artifactWires, publicArtifactWire(handle, offer.digest))
	}
	sort.Slice(artifactWires, func(i, j int) bool { return artifactWires[i].Handle < artifactWires[j].Handle })
	return &agentViewCurrentWire{
		Facts:    agentViewCurrentFactsWire{Handle: spec.Subject.String(), Artifacts: artifactWires},
		Semantic: agentViewSemanticWire{Kind: spec.Kind.String(), Payload: spec.Payload.String()},
	}, artifacts, nil
}

func projectReferences(specs []AgentViewReferenceSpec, authority ViewAuthority) (
	[]agentViewReferenceWire, map[string]struct{}, error,
) {
	if len(specs) > MaxAgentViewReferences {
		return nil, nil, limit("Agent View References", len(specs), MaxAgentViewReferences)
	}
	if len(specs) != len(authority.references) {
		return nil, nil, invariant("Agent View References", "every sealed Reference head must be projected")
	}
	artifacts := make(map[string]struct{}, len(specs))
	seenHeads := make(map[string]struct{}, len(specs))
	wires := make([]agentViewReferenceWire, 0, len(specs))
	for _, spec := range specs {
		reference, offered := authority.references[spec.Head.String()]
		if spec.Head.IsZero() || !offered {
			return nil, nil, invariant("Agent View Reference", "head must be a sealed offer")
		}
		if spec.TerminalOutcomes != reference.TerminalOutcomes() {
			return nil, nil, invariant("Agent View Reference outcomes",
				"do not match the sealed outcome projection")
		}
		if _, duplicate := seenHeads[spec.Head.String()]; duplicate {
			return nil, nil, invalid("Agent View References", "contains a duplicate head")
		}
		seenHeads[spec.Head.String()] = struct{}{}
		state, artifactWire, err := projectReferenceState(spec, authority.artifacts)
		if err != nil {
			return nil, nil, err
		}
		outcomes, err := projectTerminalOutcomes(spec.TerminalOutcomes)
		if err != nil {
			return nil, nil, err
		}
		if !spec.Artifact.IsZero() {
			artifacts[spec.Artifact.String()] = struct{}{}
		}
		wires = append(wires, agentViewReferenceWire{
			Facts: agentViewReferenceFactsWire{Key: reference.key.String(), Head: spec.Head.String(),
				State: state, Artifact: artifactWire, TerminalOutcomes: outcomes},
		})
	}
	sort.Slice(wires, func(i, j int) bool { return wires[i].Facts.Head < wires[j].Facts.Head })
	return wires, artifacts, nil
}

func projectTerminalOutcomes(outcomes AgentViewTerminalOutcomes) (*agentViewTerminalOutcomesWire, error) {
	if err := validateTerminalOutcomes(outcomes); err != nil {
		return nil, err
	}
	if outcomes == (AgentViewTerminalOutcomes{}) {
		return nil, nil
	}
	return &agentViewTerminalOutcomesWire{Completed: outcomes.Completed,
		Declined: outcomes.Declined, Unresolved: outcomes.Unresolved}, nil
}

func validateTerminalOutcomes(outcomes AgentViewTerminalOutcomes) error {
	if outcomes.Completed < 0 || outcomes.Declined < 0 || outcomes.Unresolved < 0 {
		return invalid("Agent View terminal outcomes", "counts must not be negative")
	}
	return nil
}

func projectReferenceState(spec AgentViewReferenceSpec, offers map[string]ViewArtifactOffer) (
	string, *agentViewArtifactWire, error,
) {
	switch spec.State {
	case AgentViewReferenceStateActive:
		artifact, offered := offers[spec.Artifact.String()]
		if spec.Artifact.IsZero() || !offered {
			return "", nil, invariant("Agent View active Reference", "Artifact must be a sealed offer")
		}
		wire := publicArtifactWire(spec.Artifact, artifact.digest)
		return "active", &wire, nil
	case AgentViewReferenceStateRetracted:
		if !spec.Artifact.IsZero() {
			return "", nil, invariant("Agent View retracted Reference", "must not carry an Artifact")
		}
		return "retracted", nil, nil
	default:
		return "", nil, invalid("Agent View Reference state", "must be active or retracted")
	}
}

func requireExactArtifactProjection(projected map[string]struct{}, offered map[string]ViewArtifactOffer) error {
	if len(projected) != len(offered) {
		return invariant("Agent View Artifacts", "every sealed Artifact offer must be projected")
	}
	for handle := range offered {
		if _, exists := projected[handle]; !exists {
			return invariant("Agent View Artifacts", "sealed Artifact offer is not projected")
		}
	}
	return nil
}

func projectTargetAliases(targets map[string]ResolvedTarget) []string {
	result := make([]string, 0, len(targets))
	for _, target := range targets {
		if target.requested.IsSelf() {
			result = append(result, "self")
		} else {
			result = append(result, target.requested.Alias().String())
		}
	}
	sort.Strings(result)
	return result
}

func projectProvenanceHandles(provenance map[string]EventRef) []string {
	result := make([]string, 0, len(provenance))
	for handle := range provenance {
		result = append(result, handle)
	}
	sort.Strings(result)
	return result
}

func projectAllowedIntentShapes(consequences map[Consequence]struct{}) []agentViewIntentShapeWire {
	result := make([]agentViewIntentShapeWire, 0, len(consequences))
	for consequence := range consequences {
		result = append(result, allowedIntentShape(consequence))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Consequence < result[j].Consequence })
	return result
}

func allowedIntentShape(consequence Consequence) agentViewIntentShapeWire {
	shape := agentViewIntentShapeWire{Consequence: consequence.String(), Subject: "none", Artifacts: "optional"}
	switch consequence {
	case ConsequenceCreateHandlings:
		shape.Successors = "required"
	case ConsequenceAdvanceHandling:
		shape.Subject, shape.Successors = "current", "optional"
	case ConsequenceResolveCompleted:
		shape.Subject, shape.Successors, shape.Artifacts = "current", "optional", "at_least_one"
	case ConsequenceResolveDeclined, ConsequenceResolveUnresolved:
		shape.Subject, shape.Successors = "current", "optional"
	case ConsequencePublishReference:
		shape.Reference, shape.Artifacts = "new_key", "exactly_one"
	case ConsequenceSupersedeReference:
		shape.Reference, shape.Artifacts = "offered_head", "exactly_one"
	case ConsequenceRetractReference:
		shape.Reference, shape.Artifacts = "offered_head", "none"
	}
	return shape
}

func publicArtifactWire(handle OpaqueHandle, digest Digest) agentViewArtifactWire {
	return agentViewArtifactWire{Handle: handle.String(), Digest: digest.String()}
}

type agentViewWire struct {
	Schema         string                     `json:"schema"`
	Version        int                        `json:"version"`
	View           string                     `json:"view"`
	Current        *agentViewCurrentWire      `json:"current,omitempty"`
	References     []agentViewReferenceWire   `json:"references,omitempty"`
	Targets        []string                   `json:"targets,omitempty"`
	AllowedIntents []agentViewIntentShapeWire `json:"allowed_intents"`
	Provenance     []string                   `json:"provenance_handles,omitempty"`
}

type agentViewCurrentWire struct {
	Facts    agentViewCurrentFactsWire `json:"facts"`
	Semantic agentViewSemanticWire     `json:"semantic"`
}

type agentViewCurrentFactsWire struct {
	Handle    string                  `json:"handle"`
	Artifacts []agentViewArtifactWire `json:"artifacts,omitempty"`
}

type agentViewSemanticWire struct {
	Kind    string `json:"kind"`
	Payload string `json:"payload"`
}

type agentViewReferenceWire struct {
	Facts agentViewReferenceFactsWire `json:"facts"`
}

type agentViewReferenceFactsWire struct {
	Key              string                         `json:"key"`
	Head             string                         `json:"head"`
	State            string                         `json:"state"`
	Artifact         *agentViewArtifactWire         `json:"artifact,omitempty"`
	TerminalOutcomes *agentViewTerminalOutcomesWire `json:"terminal_outcomes,omitempty"`
}

type agentViewTerminalOutcomesWire struct {
	Completed  int64 `json:"completed,omitempty"`
	Declined   int64 `json:"declined,omitempty"`
	Unresolved int64 `json:"unresolved,omitempty"`
}

type agentViewArtifactWire struct {
	Handle string `json:"handle"`
	Digest string `json:"digest"`
}

type agentViewIntentShapeWire struct {
	Consequence string `json:"consequence"`
	Subject     string `json:"subject"`
	Successors  string `json:"successors,omitempty"`
	Reference   string `json:"reference,omitempty"`
	Artifacts   string `json:"artifacts"`
}
