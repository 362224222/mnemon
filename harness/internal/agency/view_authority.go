package agency

import "sort"

const viewAuthorityVersion = 3

// MachineViewSpec is the complete machine-owned authority behind one bounded
// Agent View. Its typed offers prevent an opaque handle from being repurposed
// across authority classes.
type MachineViewSpec struct {
	Attachment   Attachment
	Consequences []Consequence
	Subjects     []SubjectBinding
	References   []ReferenceExpectation
	Targets      []ResolvedTarget
	ReplyTarget  TargetRef
	Artifacts    []ViewArtifactOffer
	Provenance   []ProvenanceOffer
}

// ViewAuthority seals an exact Attachment and the read-set projected at that
// boundary. Its digest deliberately excludes the short-lived Attachment
// envelope while binding the stable Principal, initiation authority, and all
// typed offers.
type ViewAuthority struct {
	attachment   Attachment
	consequences map[Consequence]struct{}
	subjects     map[string]SubjectBinding
	references   map[string]ReferenceExpectation
	targets      map[string]ResolvedTarget
	replyTarget  TargetRef
	artifacts    map[string]ViewArtifactOffer
	provenance   map[string]EventRef
	canonical    []byte
	digest       Digest
}

func NewViewAuthority(spec MachineViewSpec) (ViewAuthority, error) {
	if spec.Attachment.id.IsZero() {
		return ViewAuthority{}, invalid("View authority", "Attachment is required")
	}
	if len(spec.Consequences) > MaxViewConsequences {
		return ViewAuthority{}, limit("View consequences", len(spec.Consequences), MaxViewConsequences)
	}
	if err := validateSubjectOfferBound(spec.Subjects); err != nil {
		return ViewAuthority{}, err
	}
	if len(spec.Targets) > MaxViewTargets {
		return ViewAuthority{}, limit("View targets", len(spec.Targets), MaxViewTargets)
	}
	handleCount := len(spec.Subjects) + len(spec.References) + len(spec.Artifacts) + len(spec.Provenance)
	for _, target := range spec.Targets {
		if !target.requested.IsSelf() {
			handleCount++
		}
	}
	if handleCount > MaxViewHandles {
		return ViewAuthority{}, limit("View handles", handleCount, MaxViewHandles)
	}

	view := ViewAuthority{
		attachment:   spec.Attachment,
		consequences: make(map[Consequence]struct{}, len(spec.Consequences)),
		subjects:     make(map[string]SubjectBinding, len(spec.Subjects)),
		references:   make(map[string]ReferenceExpectation, len(spec.References)),
		targets:      make(map[string]ResolvedTarget, len(spec.Targets)),
		artifacts:    make(map[string]ViewArtifactOffer, len(spec.Artifacts)),
		provenance:   make(map[string]EventRef, len(spec.Provenance)),
	}
	if err := view.load(spec); err != nil {
		return ViewAuthority{}, err
	}
	canonical, digest, err := canonicalJSON(view.wire())
	if err != nil {
		return ViewAuthority{}, err
	}
	if len(canonical) > MaxViewCanonicalBytes {
		return ViewAuthority{}, limit("View canonical bytes", len(canonical), MaxViewCanonicalBytes)
	}
	view.canonical, view.digest = canonical, digest
	return view, nil
}

func validateSubjectOfferBound(offers []SubjectBinding) error {
	if len(offers) <= 1 {
		return nil
	}
	seen := make(map[string]struct{}, len(offers))
	for _, subject := range offers {
		key := subject.handle.String()
		if _, exists := seen[key]; exists {
			return invalid("View subjects", "contains a duplicate handle")
		}
		seen[key] = struct{}{}
	}
	return limit("View subjects", len(offers), 1)
}

func (view *ViewAuthority) load(spec MachineViewSpec) error {
	if err := loadConsequences(view.consequences, spec.Consequences); err != nil {
		return err
	}
	if err := loadSubjects(view.subjects, spec.Subjects); err != nil {
		return err
	}
	if err := loadReferences(view.references, spec.References); err != nil {
		return err
	}
	if err := loadTargets(view.targets, spec.Targets, spec.Attachment); err != nil {
		return err
	}
	if err := loadReplyTarget(&view.replyTarget, spec.ReplyTarget, view.subjects, view.targets); err != nil {
		return err
	}
	if err := loadArtifacts(view.artifacts, spec.Artifacts); err != nil {
		return err
	}
	if err := loadProvenance(view.provenance, spec.Provenance); err != nil {
		return err
	}
	return nil
}

func loadReplyTarget(destination *TargetRef, requested TargetRef,
	subjects map[string]SubjectBinding, targets map[string]ResolvedTarget,
) error {
	if requested.IsZero() {
		return nil
	}
	if requested.IsSelf() || len(subjects) != 1 {
		return invalid("View reply target", "requires one current subject and one remote alias")
	}
	resolved, offered := targets[requested.canonicalKey()]
	if !offered || resolved.destination != TargetDestinationRemote || resolved.requested != requested {
		return invariant("View reply target", "must be one exact offered remote target")
	}
	*destination = requested
	return nil
}

func loadConsequences(destination map[Consequence]struct{}, offers []Consequence) error {
	for _, consequence := range offers {
		if !consequence.Valid() {
			return invalid("View authority", "contains an invalid consequence")
		}
		if _, exists := destination[consequence]; exists {
			return invalid("View authority", "contains a duplicate consequence")
		}
		destination[consequence] = struct{}{}
	}
	return nil
}

func loadSubjects(destination map[string]SubjectBinding, offers []SubjectBinding) error {
	for _, subject := range offers {
		if subject.handle.IsZero() || subject.handlingID.IsZero() || subject.head.IsZero() || subject.fence == 0 {
			return invalid("View subject", "contains an invalid binding")
		}
		key := subject.handle.String()
		if _, exists := destination[key]; exists {
			return invalid("View subject", "contains a duplicate handle")
		}
		destination[key] = subject
	}
	return nil
}

func loadReferences(destination map[string]ReferenceExpectation, offers []ReferenceExpectation) error {
	for _, reference := range offers {
		if reference.absent || reference.handle.IsZero() || reference.key.IsZero() || reference.head.IsZero() {
			return invalid("View Reference", "must contain an exact locally accepted head")
		}
		key := reference.handle.String()
		if _, exists := destination[key]; exists {
			return invalid("View Reference", "contains a duplicate handle")
		}
		destination[key] = reference
	}
	return nil
}

func loadTargets(destination map[string]ResolvedTarget, offers []ResolvedTarget, attachment Attachment) error {
	for _, target := range offers {
		if err := validateResolvedTarget(target, attachment); err != nil {
			return err
		}
		key := target.requested.canonicalKey()
		if _, exists := destination[key]; exists {
			return invalid("View targets", "contains a duplicate requested target")
		}
		destination[key] = target
	}
	return nil
}

func loadArtifacts(destination map[string]ViewArtifactOffer, offers []ViewArtifactOffer) error {
	for _, artifact := range offers {
		if artifact.handle.IsZero() || artifact.digest.IsZero() {
			return invalid("View Artifacts", "contains an invalid offer")
		}
		key := artifact.handle.String()
		if _, exists := destination[key]; exists {
			return invalid("View Artifacts", "contains a duplicate handle")
		}
		destination[key] = artifact
	}
	return nil
}

func loadProvenance(destination map[string]EventRef, offers []ProvenanceOffer) error {
	for _, provenance := range offers {
		if provenance.handle.IsZero() || provenance.event.IsZero() {
			return invalid("View provenance", "contains an invalid offer")
		}
		key := provenance.handle.String()
		if _, exists := destination[key]; exists {
			return invalid("View provenance", "contains a duplicate handle")
		}
		destination[key] = provenance.event
	}
	return nil
}

func validateResolvedTarget(target ResolvedTarget, attachment Attachment) error {
	if target.requested.IsZero() {
		return invalid("View target", "requested target is required")
	}
	switch target.destination {
	case TargetDestinationLocal:
		if target.localPrincipal.IsZero() || !target.remoteRoute.IsZero() || !target.remoteAlias.IsZero() {
			return invalid("View target", "invalid local resolution")
		}
		if target.requested.IsSelf() && target.localPrincipal != attachment.principal {
			return invariant("View target", "self must resolve to the Attachment Principal")
		}
	case TargetDestinationRemote:
		if target.requested.IsSelf() || target.remoteRoute.IsZero() || target.remoteAlias.IsZero() ||
			!target.localPrincipal.IsZero() {
			return invalid("View target", "invalid remote resolution")
		}
	default:
		return invalid("View target", "unknown destination")
	}
	return nil
}

func (view ViewAuthority) Attachment() Attachment { return view.attachment }
func (view ViewAuthority) CanonicalJSON() []byte  { return copyBytes(view.canonical) }
func (view ViewAuthority) Digest() Digest         { return view.digest }

// ResolveOfferedArtifact returns the machine-owned digest behind one exact
// handle offered by this frozen View. Callers cannot use a known digest or a
// handle from another View to widen the read-set.
func (view ViewAuthority) ResolveOfferedArtifact(handle OpaqueHandle) (Digest, error) {
	if handle.IsZero() {
		return Digest{}, invalid("View Artifact read", "handle is required")
	}
	offer, offered := view.artifacts[handle.String()]
	if !offered || offer.handle != handle || offer.digest.IsZero() {
		return Digest{}, invariant("View Artifact read", "handle was not offered by the View")
	}
	return offer.digest, nil
}

func (view ViewAuthority) offers(consequence Consequence) bool {
	_, exists := view.consequences[consequence]
	return exists
}

type machineViewWire struct {
	SchemaVersion   int                       `json:"schema_version"`
	SourcePrincipal string                    `json:"source_principal"`
	MayInitiate     bool                      `json:"may_initiate"`
	Consequences    []string                  `json:"consequences,omitempty"`
	Subjects        []viewSubjectWire         `json:"subjects,omitempty"`
	References      []viewReferenceWire       `json:"references,omitempty"`
	Targets         []viewTargetWire          `json:"targets,omitempty"`
	ReplyTarget     *targetWire               `json:"reply_target,omitempty"`
	Artifacts       []viewArtifactOfferWire   `json:"artifacts,omitempty"`
	Provenance      []viewProvenanceOfferWire `json:"provenance,omitempty"`
}

type viewSubjectWire struct {
	Handle  string             `json:"handle"`
	Binding subjectBindingWire `json:"binding"`
}

type viewReferenceWire struct {
	Handle           string                    `json:"handle"`
	Head             referenceExpectationWire  `json:"head"`
	TerminalOutcomes *viewTerminalOutcomesWire `json:"terminal_outcomes,omitempty"`
}

type viewTerminalOutcomesWire struct {
	Completed  int64 `json:"completed,omitempty"`
	Declined   int64 `json:"declined,omitempty"`
	Unresolved int64 `json:"unresolved,omitempty"`
}

type viewTargetWire struct {
	Requested targetWire         `json:"requested"`
	Resolved  resolvedTargetWire `json:"resolved"`
}

type viewArtifactOfferWire struct {
	Handle string `json:"handle"`
	Digest string `json:"digest"`
}

type viewProvenanceOfferWire struct {
	Handle string       `json:"handle"`
	Event  eventRefWire `json:"event"`
}

func (view ViewAuthority) wire() machineViewWire {
	wire := machineViewWire{SchemaVersion: viewAuthorityVersion,
		SourcePrincipal: view.attachment.principal.String(),
		MayInitiate:     view.attachment.mayInitiate}
	for consequence := range view.consequences {
		wire.Consequences = append(wire.Consequences, consequence.String())
	}
	for handle, subject := range view.subjects {
		wire.Subjects = append(wire.Subjects, viewSubjectWire{Handle: handle,
			Binding: subjectBindingWire{HandlingID: subject.handlingID.String(),
				Head: subject.head.canonical().(eventRefWire), Fence: subject.fence}})
	}
	for handle, reference := range view.references {
		head := reference.head.canonical().(eventRefWire)
		projected := viewReferenceWire{Handle: handle,
			Head: referenceExpectationWire{Key: reference.key.String(), Head: &head}}
		if reference.outcomes != (AgentViewTerminalOutcomes{}) {
			projected.TerminalOutcomes = &viewTerminalOutcomesWire{
				Completed: reference.outcomes.Completed, Declined: reference.outcomes.Declined,
				Unresolved: reference.outcomes.Unresolved}
		}
		wire.References = append(wire.References, projected)
	}
	for _, target := range view.targets {
		wire.Targets = append(wire.Targets, viewTargetWire{
			Requested: targetWire{Self: target.requested.self, Alias: target.requested.alias.String()},
			Resolved:  target.resolvedWire(),
		})
	}
	if !view.replyTarget.IsZero() {
		wire.ReplyTarget = &targetWire{Alias: view.replyTarget.Alias().String()}
	}
	for handle, artifact := range view.artifacts {
		wire.Artifacts = append(wire.Artifacts, viewArtifactOfferWire{Handle: handle, Digest: artifact.digest.String()})
	}
	for handle, event := range view.provenance {
		wire.Provenance = append(wire.Provenance, viewProvenanceOfferWire{
			Handle: handle, Event: event.canonical().(eventRefWire)})
	}
	sort.Strings(wire.Consequences)
	sort.Slice(wire.Subjects, func(i, j int) bool { return wire.Subjects[i].Handle < wire.Subjects[j].Handle })
	sort.Slice(wire.References, func(i, j int) bool { return wire.References[i].Handle < wire.References[j].Handle })
	sort.Slice(wire.Targets, func(i, j int) bool {
		return targetWireKey(wire.Targets[i].Requested) < targetWireKey(wire.Targets[j].Requested)
	})
	sort.Slice(wire.Artifacts, func(i, j int) bool { return wire.Artifacts[i].Handle < wire.Artifacts[j].Handle })
	sort.Slice(wire.Provenance, func(i, j int) bool { return wire.Provenance[i].Handle < wire.Provenance[j].Handle })
	return wire
}

func targetWireKey(target targetWire) string {
	if target.Self {
		return "self"
	}
	return "alias:" + target.Alias
}
