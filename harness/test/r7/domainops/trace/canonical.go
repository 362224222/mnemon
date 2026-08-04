package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

type eventRefWire struct {
	ID     string `json:"id"`
	Digest string `json:"digest"`
}

type subjectWire struct {
	HandlingID string       `json:"handling_id"`
	Head       eventRefWire `json:"head"`
	Fence      uint64       `json:"fence"`
}

type expectedReferenceWire struct {
	Absent bool          `json:"absent"`
	Key    string        `json:"key"`
	Head   *eventRefWire `json:"head,omitempty"`
}

type targetWire struct {
	Destination    string `json:"destination"`
	LocalPrincipal string `json:"local_principal,omitempty"`
	RemoteRoute    string `json:"remote_route,omitempty"`
	RemoteAlias    string `json:"remote_alias,omitempty"`
}

type eventWire struct {
	SchemaVersion int `json:"schema_version"`
	Machine       struct {
		EventID           string                 `json:"event_id"`
		AcceptedAt        string                 `json:"accepted_at"`
		OriginSequence    uint64                 `json:"origin_sequence"`
		CausalDepth       uint16                 `json:"causal_depth"`
		SourcePrincipal   string                 `json:"source_principal"`
		OperationKey      string                 `json:"operation_key"`
		RequestDigest     string                 `json:"request_digest"`
		Consequence       string                 `json:"consequence"`
		Subject           *subjectWire           `json:"subject,omitempty"`
		ExpectedReference *expectedReferenceWire `json:"expected_reference,omitempty"`
		Targets           []targetWire           `json:"targets,omitempty"`
	} `json:"machine"`
	Semantic struct {
		Kind    string `json:"kind"`
		Payload string `json:"payload"`
	} `json:"semantic"`
	Evidence struct {
		Artifacts   []string       `json:"artifacts,omitempty"`
		Causation   []eventRefWire `json:"causation,omitempty"`
		Correlation *eventRefWire  `json:"correlation,omitempty"`
	} `json:"evidence"`
}

type eventEvidence struct {
	Node            string
	ID              string
	Digest          string
	AcceptedAt      time.Time
	OriginSequence  uint64
	CausalDepth     uint16
	SourcePrincipal string
	OperationKey    string
	RequestDigest   string
	SemanticKind    string
	PayloadBytes    int
	Consequence     string
	Targets         []string
	SubjectHandling string
	SubjectHead     *eventRefWire
	ReferenceKey    string
	ReferenceHead   string
	ReferenceDigest string
	Artifacts       []string
	Causation       []eventRefWire
	Correlation     *eventRefWire
}

type storedEventRow struct {
	ID              string
	Digest          string
	OriginSequence  int64
	SourcePrincipal string
	RequestDigest   string
	CausalDepth     int64
	AcceptedAt      string
	Canonical       []byte
}

func parseStoredEvent(node string, row storedEventRow) (eventEvidence, error) {
	var wire eventWire
	if err := decodeCanonicalObject("Event", row.Canonical, 32<<10, &wire); err != nil {
		return eventEvidence{}, fmt.Errorf("%s Event %q: %w", node, row.ID, err)
	}
	acceptedAt, err := validateStoredEvent(node, row, wire)
	if err != nil {
		return eventEvidence{}, fmt.Errorf("%s Event %q: %w", node, row.ID, err)
	}
	subject := ""
	var subjectHead *eventRefWire
	if wire.Machine.Subject != nil {
		subject = wire.Machine.Subject.HandlingID
		subjectHead = cloneEventRef(&wire.Machine.Subject.Head)
	}
	referenceKey, referenceHead, referenceDigest := "", "", ""
	if wire.Machine.ExpectedReference != nil {
		referenceKey = wire.Machine.ExpectedReference.Key
		if wire.Machine.ExpectedReference.Head != nil {
			referenceHead = wire.Machine.ExpectedReference.Head.ID
			referenceDigest = wire.Machine.ExpectedReference.Head.Digest
		}
	}
	targets := make([]string, 0, len(wire.Machine.Targets))
	for _, target := range wire.Machine.Targets {
		if target.Destination == "local" {
			targets = append(targets, target.LocalPrincipal)
		} else {
			targets = append(targets, target.RemoteAlias)
		}
	}
	return eventEvidence{
		Node: node, ID: row.ID, Digest: row.Digest, AcceptedAt: acceptedAt,
		OriginSequence: wire.Machine.OriginSequence, CausalDepth: wire.Machine.CausalDepth,
		SourcePrincipal: row.SourcePrincipal, OperationKey: wire.Machine.OperationKey,
		RequestDigest: row.RequestDigest,
		SemanticKind:  wire.Semantic.Kind, PayloadBytes: len([]byte(wire.Semantic.Payload)),
		Consequence: wire.Machine.Consequence, Targets: targets,
		SubjectHandling: subject, SubjectHead: subjectHead, ReferenceKey: referenceKey,
		ReferenceHead: referenceHead, ReferenceDigest: referenceDigest,
		Artifacts:   append([]string{}, wire.Evidence.Artifacts...),
		Causation:   append([]eventRefWire{}, wire.Evidence.Causation...),
		Correlation: cloneEventRef(wire.Evidence.Correlation),
	}, nil
}

func validateStoredEvent(node string, row storedEventRow, wire eventWire) (time.Time, error) {
	if wire.SchemaVersion != 2 || wire.Machine.EventID != row.ID ||
		wire.Machine.OriginSequence != uint64(row.OriginSequence) ||
		wire.Machine.CausalDepth != uint16(row.CausalDepth) ||
		wire.Machine.SourcePrincipal != row.SourcePrincipal ||
		wire.Machine.RequestDigest != row.RequestDigest {
		return time.Time{}, errors.New("columns differ from canonical authority")
	}
	if row.OriginSequence <= 0 || row.CausalDepth < 0 || row.CausalDepth > agency.MaxPeerCausalDepth ||
		agency.Sum(row.Canonical).String() != row.Digest {
		return time.Time{}, errors.New("invalid sequence, depth, or digest")
	}
	if err := validateEventScalarValues(row, wire); err != nil {
		return time.Time{}, err
	}
	acceptedAt, err := parseCanonicalTime("Event accepted_at", wire.Machine.AcceptedAt)
	if err != nil {
		return time.Time{}, err
	}
	storedAt, err := parseStoredTime("Event stored accepted_at", row.AcceptedAt)
	if err != nil || !storedAt.Equal(acceptedAt) {
		return time.Time{}, fmt.Errorf("%s Event %q accepted time differs from canonical authority", node, row.ID)
	}
	if err := validateEventShape(wire); err != nil {
		return time.Time{}, err
	}
	if err := validateEventEvidence(wire.Evidence.Artifacts, wire.Evidence.Causation,
		wire.Evidence.Correlation); err != nil {
		return time.Time{}, err
	}
	return acceptedAt, nil
}

func validateEventScalarValues(row storedEventRow, wire eventWire) error {
	checks := []func() error{
		func() error { _, err := agency.NewEventID(row.ID); return err },
		func() error { _, err := agency.ParseDigest(row.Digest); return err },
		func() error { _, err := agency.NewAgentPrincipalID(row.SourcePrincipal); return err },
		func() error { _, err := agency.NewOperationKey(wire.Machine.OperationKey); return err },
		func() error { _, err := agency.ParseDigest(row.RequestDigest); return err },
		func() error { _, err := agency.NewSemanticLabel(wire.Semantic.Kind); return err },
		func() error { _, err := agency.NewSemanticPayload(wire.Semantic.Payload); return err },
	}
	for _, check := range checks {
		if err := check(); err != nil {
			return err
		}
	}
	return nil
}

func validateEventEvidence(artifacts []string, causation []eventRefWire,
	correlation *eventRefWire,
) error {
	for _, artifact := range artifacts {
		if _, err := agency.ParseDigest(artifact); err != nil {
			return err
		}
	}
	for _, causal := range causation {
		if err := validateEventRef(causal); err != nil {
			return err
		}
	}
	if correlation != nil {
		return validateEventRef(*correlation)
	}
	return nil
}

func validateEventShape(wire eventWire) error {
	machine := wire.Machine
	if err := validateEventTargets(machine.Targets); err != nil {
		return err
	}
	switch machine.Consequence {
	case "handling.create":
		return validateRootEvent(machine.Subject, machine.ExpectedReference, machine.Targets)
	case "handling.advance", "handling.resolve.completed", "handling.resolve.declined",
		"handling.resolve.unresolved":
		return validateSubjectEvent(machine.Subject, machine.ExpectedReference)
	case "reference.publish", "reference.supersede", "reference.retract":
		return validateReferenceEvent(machine.Consequence, machine.Subject,
			machine.ExpectedReference, machine.Targets, wire.Evidence.Artifacts)
	default:
		return errors.New("Event has an unknown closed consequence")
	}
}

func validateEventTargets(targets []targetWire) error {
	for _, target := range targets {
		if err := validateEventTarget(target); err != nil {
			return err
		}
	}
	return nil
}

func validateEventTarget(target targetWire) error {
	switch target.Destination {
	case "local":
		if target.LocalPrincipal == "" || target.RemoteRoute != "" || target.RemoteAlias != "" {
			return errors.New("Event has an invalid local target")
		}
		_, err := agency.NewAgentPrincipalID(target.LocalPrincipal)
		return err
	case "remote":
		if target.LocalPrincipal != "" || target.RemoteRoute == "" || target.RemoteAlias == "" {
			return errors.New("Event has an invalid remote target")
		}
		if _, err := agency.NewRouteID(target.RemoteRoute); err != nil {
			return err
		}
		_, err := agency.NewOpaqueHandle(target.RemoteAlias)
		return err
	default:
		return errors.New("Event has an unknown target destination")
	}
}

func validateRootEvent(subject *subjectWire, expected *expectedReferenceWire,
	targets []targetWire,
) error {
	if subject != nil || expected != nil || len(targets) == 0 {
		return errors.New("root Event has an invalid authority shape")
	}
	return nil
}

func validateSubjectEvent(subject *subjectWire, expected *expectedReferenceWire) error {
	if subject == nil || expected != nil {
		return errors.New("subject Event has an invalid authority shape")
	}
	if _, err := agency.NewHandlingID(subject.HandlingID); err != nil {
		return err
	}
	if subject.Fence == 0 {
		return errors.New("subject Event has no positive fence")
	}
	return validateEventRef(subject.Head)
}

func validateReferenceEvent(consequence string, subject *subjectWire,
	expected *expectedReferenceWire, targets []targetWire, artifacts []string,
) error {
	if subject != nil || len(targets) != 0 || expected == nil {
		return errors.New("Reference Event has an invalid authority shape")
	}
	if _, err := agency.NewReferenceKey(expected.Key); err != nil {
		return err
	}
	if consequence == "reference.publish" {
		if !expected.Absent || expected.Head != nil || len(artifacts) != 1 {
			return errors.New("Reference publish has an invalid CAS shape")
		}
		return nil
	}
	if expected.Absent || expected.Head == nil {
		return errors.New("Reference mutation has an invalid CAS shape")
	}
	if err := validateEventRef(*expected.Head); err != nil {
		return err
	}
	if consequence == "reference.retract" && len(artifacts) != 0 {
		return errors.New("Reference retract carries an Artifact")
	}
	return nil
}

func validateEventRef(reference eventRefWire) error {
	if _, err := agency.NewEventID(reference.ID); err != nil {
		return err
	}
	_, err := agency.ParseDigest(reference.Digest)
	return err
}
