package agency

import "sort"

type machineViewWire struct {
	SchemaVersion           int                       `json:"schema_version"`
	SourcePrincipal         string                    `json:"source_principal"`
	MayInitiate             bool                      `json:"may_initiate"`
	Consequences            []string                  `json:"consequences,omitempty"`
	Subjects                []viewSubjectWire         `json:"subjects,omitempty"`
	References              []viewReferenceWire       `json:"references,omitempty"`
	Targets                 []viewTargetWire          `json:"targets,omitempty"`
	ReplyTo                 string                    `json:"reply_to,omitempty"`
	ReplyTarget             *targetWire               `json:"reply_target,omitempty"`
	ReplyDelivery           string                    `json:"reply_delivery_id,omitempty"`
	ReplyObservationPending bool                      `json:"reply_observation_pending"`
	Artifacts               []viewArtifactOfferWire   `json:"artifacts,omitempty"`
	Provenance              []viewProvenanceOfferWire `json:"provenance,omitempty"`
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
		SourcePrincipal:         view.attachment.principal.String(),
		MayInitiate:             view.attachment.mayInitiate,
		ReplyObservationPending: view.replyObservationPending}
	for consequence := range view.consequences {
		wire.Consequences = append(wire.Consequences, consequence.String())
	}
	for handle, subject := range view.subjects {
		wire.Subjects = append(wire.Subjects, viewSubjectWire{Handle: handle,
			Binding: subjectBindingWire{HandlingID: subject.handlingID.String(),
				Head: subject.head.canonical().(eventRefWire), Fence: subject.fence,
				ObservationRevision: subject.observationRevision}})
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
	if !view.replyTo.IsZero() {
		wire.ReplyTo = view.replyTo.String()
	}
	if !view.replyTarget.IsZero() {
		wire.ReplyTarget = &targetWire{Alias: view.replyTarget.Alias().String()}
	}
	if !view.replyDelivery.IsZero() {
		wire.ReplyDelivery = view.replyDelivery.String()
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
