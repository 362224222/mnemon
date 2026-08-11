package agency

import "time"

type requestDigestWire struct {
	SchemaVersion     int                       `json:"schema_version"`
	SourcePrincipal   string                    `json:"source_principal"`
	MayInitiate       bool                      `json:"may_initiate"`
	ViewDigest        string                    `json:"view_digest"`
	Intent            intentWire                `json:"intent"`
	Subject           *subjectBindingWire       `json:"subject,omitempty"`
	ExpectedReference *referenceExpectationWire `json:"expected_reference,omitempty"`
	Targets           []resolvedTargetWire      `json:"targets,omitempty"`
	Artifacts         []resolvedArtifactWire    `json:"artifacts,omitempty"`
	Causation         []eventRefWire            `json:"causation,omitempty"`
	Correlation       *eventRefWire             `json:"correlation,omitempty"`
	InReplyToDelivery string                    `json:"in_reply_to_delivery_id,omitempty"`
}

type boundIntentWire struct {
	SchemaVersion    int               `json:"schema_version"`
	OperationKey     string            `json:"operation_key"`
	AttachmentID     string            `json:"attachment_id"`
	AttachmentIssue  string            `json:"attachment_issued_at"`
	AttachmentExpiry string            `json:"attachment_expires_at"`
	RequestDigest    string            `json:"request_digest"`
	Request          requestDigestWire `json:"request"`
}

type subjectBindingWire struct {
	HandlingID          string       `json:"handling_id"`
	Head                eventRefWire `json:"head"`
	Fence               uint64       `json:"fence"`
	ObservationRevision uint64       `json:"observation_revision"`
}

type referenceExpectationWire struct {
	Absent bool          `json:"absent"`
	Key    string        `json:"key"`
	Head   *eventRefWire `json:"head,omitempty"`
}

type resolvedTargetWire struct {
	Destination    string `json:"destination"`
	LocalPrincipal string `json:"local_principal,omitempty"`
	RemoteRoute    string `json:"remote_route,omitempty"`
	RemoteAlias    string `json:"remote_alias,omitempty"`
}

type resolvedArtifactWire struct {
	Input  artifactInputWire `json:"input"`
	Digest string            `json:"digest"`
}

func (intent BoundIntent) requestWire() requestDigestWire {
	wire := requestDigestWire{SchemaVersion: 3, SourcePrincipal: intent.attachment.principal.String(),
		MayInitiate: intent.attachment.mayInitiate, ViewDigest: intent.viewDigest.String(),
		Intent: intent.intent.wire()}
	if intent.subject != nil {
		wire.Subject = &subjectBindingWire{HandlingID: intent.subject.handlingID.String(),
			Head: intent.subject.head.canonical().(eventRefWire), Fence: intent.subject.fence,
			ObservationRevision: intent.subject.observationRevision}
	}
	if intent.expectedReference != nil {
		expected := intent.expectedReference
		wire.ExpectedReference = &referenceExpectationWire{Absent: expected.absent, Key: expected.key.String()}
		if !expected.head.IsZero() {
			head := expected.head.canonical().(eventRefWire)
			wire.ExpectedReference.Head = &head
		}
	}
	for _, target := range intent.targets {
		wire.Targets = append(wire.Targets, target.resolvedWire())
	}
	for _, artifact := range intent.resolvedArtifacts {
		kind := "candidate"
		if artifact.input.kind == ArtifactInputViewHandle {
			kind = "view_handle"
		}
		wire.Artifacts = append(wire.Artifacts, resolvedArtifactWire{
			Input:  artifactInputWire{Kind: kind, Handle: artifact.input.handle.String()},
			Digest: artifact.digest.String()})
	}
	for _, event := range intent.causation {
		wire.Causation = append(wire.Causation, event.canonical().(eventRefWire))
	}
	if !intent.correlation.IsZero() {
		correlation := intent.correlation.canonical().(eventRefWire)
		wire.Correlation = &correlation
	}
	if !intent.inReplyToDelivery.IsZero() {
		wire.InReplyToDelivery = intent.inReplyToDelivery.String()
	}
	return wire
}

func (target ResolvedTarget) resolvedWire() resolvedTargetWire {
	destination := "local"
	if target.destination == TargetDestinationRemote {
		destination = "remote"
	}
	return resolvedTargetWire{Destination: destination,
		LocalPrincipal: target.localPrincipal.String(), RemoteRoute: target.remoteRoute.String(),
		RemoteAlias: target.remoteAlias.String()}
}

func (intent BoundIntent) wire() boundIntentWire {
	return boundIntentWire{SchemaVersion: 3, OperationKey: intent.operationKey.String(),
		AttachmentID:     intent.attachment.id.String(),
		AttachmentIssue:  intent.attachment.issuedAt.Format(time.RFC3339Nano),
		AttachmentExpiry: intent.attachment.expiresAt.Format(time.RFC3339Nano),
		RequestDigest:    intent.digest.String(), Request: intent.requestWire()}
}
