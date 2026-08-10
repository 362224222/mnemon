package agency

const viewAuthorityVersion = 7

// MachineViewSpec is the complete machine-owned authority behind one bounded
// Agent View. Its typed offers prevent an opaque handle from being repurposed
// across authority classes.
type MachineViewSpec struct {
	Attachment    Attachment
	Consequences  []Consequence
	Subjects      []SubjectBinding
	References    []ReferenceExpectation
	Targets       []ResolvedTarget
	ReplyTo       OpaqueHandle
	ReplyTarget   TargetRef
	ReplyDelivery DeliveryID
	// ReplyObservationPending is a machine-derived fact about the current
	// subject's existing outbound reply bindings. It grants no reply authority
	// and does not constrain which offered subject consequence may be selected.
	ReplyObservationPending bool
	Artifacts               []ViewArtifactOffer
	Provenance              []ProvenanceOffer
}

// ViewAuthority seals an exact Attachment and the read-set projected at that
// boundary. Its digest deliberately excludes the short-lived Attachment
// envelope while binding the stable Principal, initiation authority, and all
// typed offers.
type ViewAuthority struct {
	attachment              Attachment
	consequences            map[Consequence]struct{}
	subjects                map[string]SubjectBinding
	references              map[string]ReferenceExpectation
	targets                 map[string]ResolvedTarget
	replyTo                 OpaqueHandle
	replyTarget             TargetRef
	replyDelivery           DeliveryID
	replyObservationPending bool
	artifacts               map[string]ViewArtifactOffer
	provenance              map[string]EventRef
	canonical               []byte
	digest                  Digest
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
	if err := loadArtifacts(view.artifacts, spec.Artifacts); err != nil {
		return err
	}
	if err := loadProvenance(view.provenance, spec.Provenance); err != nil {
		return err
	}
	if err := loadReplyContext(&view.replyTo, &view.replyTarget, &view.replyDelivery, spec.ReplyTo,
		spec.ReplyTarget, spec.ReplyDelivery, view.subjects, view.targets, view.provenance); err != nil {
		return err
	}
	if spec.ReplyObservationPending && len(view.subjects) != 1 {
		return invalid("View reply observation", "pending state requires one current subject")
	}
	view.replyObservationPending = spec.ReplyObservationPending
	return nil
}

func loadReplyContext(replyTo *OpaqueHandle, replyTarget *TargetRef, replyDelivery *DeliveryID,
	requestedReplyTo OpaqueHandle, requestedTarget TargetRef, requestedDelivery DeliveryID,
	subjects map[string]SubjectBinding, targets map[string]ResolvedTarget,
	provenance map[string]EventRef,
) error {
	if requestedReplyTo.IsZero() {
		if !requestedTarget.IsZero() || !requestedDelivery.IsZero() {
			return invalid("View reply context", "target and Delivery require a reply-to offer")
		}
		return nil
	}
	if len(subjects) != 1 {
		return invalid("View reply context", "requires one current subject")
	}
	if _, offered := provenance[requestedReplyTo.String()]; !offered {
		return invariant("View reply context", "reply-to must be one exact provenance offer")
	}
	*replyTo = requestedReplyTo
	if requestedTarget.IsZero() {
		if !requestedDelivery.IsZero() {
			return invalid("View reply context", "Delivery requires a remote reply target")
		}
		return nil
	}
	if requestedDelivery.IsZero() {
		return invalid("View reply context", "remote reply target requires an exact Delivery")
	}
	if requestedTarget.IsSelf() {
		return invalid("View reply target", "must be a remote alias")
	}
	resolved, offered := targets[requestedTarget.canonicalKey()]
	if !offered || resolved.destination != TargetDestinationRemote || resolved.requested != requestedTarget {
		return invariant("View reply target", "must be one exact offered remote target")
	}
	*replyTarget = requestedTarget
	*replyDelivery = requestedDelivery
	return nil
}

func loadConsequences(destination map[Consequence]struct{}, offers []Consequence) error {
	for _, consequence := range offers {
		if !consequence.agentDeclarable() {
			return invalid("View authority", "contains a non-Agent-declarable consequence")
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

// Allows reports whether this exact sealed View offered a closed consequence.
// It exposes no authority beyond the immutable offer itself.
func (view ViewAuthority) Allows(consequence Consequence) bool {
	_, exists := view.consequences[consequence]
	return exists
}

// ResolveSubject returns the exact Handling authority behind one View-scoped
// handle. Resolution policy remains with the authority package.
func (view ViewAuthority) ResolveSubject(handle OpaqueHandle) (SubjectBinding, bool) {
	value, ok := view.subjects[handle.String()]
	return value, ok && !handle.IsZero()
}

// ResolveReference returns the exact local Reference head behind one
// View-scoped handle.
func (view ViewAuthority) ResolveReference(handle OpaqueHandle) (ReferenceExpectation, bool) {
	value, ok := view.references[handle.String()]
	return value, ok && !handle.IsZero()
}

// ResolveTarget returns the exact local or remote destination offered for one
// Agent-visible target.
func (view ViewAuthority) ResolveTarget(target TargetRef) (ResolvedTarget, bool) {
	value, ok := view.targets[target.canonicalKey()]
	return value, ok && !target.IsZero()
}

// ResolveProvenance returns the exact accepted Event behind one View-scoped
// causation or correlation handle.
func (view ViewAuthority) ResolveProvenance(handle OpaqueHandle) (EventRef, bool) {
	value, ok := view.provenance[handle.String()]
	return value, ok && !handle.IsZero()
}

// ReplyContext returns the complete machine-offered remote reply binding. A
// partial binding is unrepresentable because NewViewAuthority rejects it.
func (view ViewAuthority) ReplyContext() (OpaqueHandle, TargetRef, DeliveryID, bool) {
	if view.replyTo.IsZero() {
		return OpaqueHandle{}, TargetRef{}, DeliveryID{}, false
	}
	return view.replyTo, view.replyTarget, view.replyDelivery, true
}
