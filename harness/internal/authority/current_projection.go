package authority

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

func projectBoundViewTx(ctx context.Context, tx *sql.Tx, attachment agency.Attachment,
	claim *projectedClaim, operation agency.OperationKey,
) (BoundView, error) {
	references, err := loadReferencesTx(ctx, tx)
	if err != nil {
		return BoundView{}, err
	}
	self, err := agency.ResolveLocalTarget(agency.SelfTarget(), attachment.Principal())
	if err != nil {
		return BoundView{}, err
	}
	spec := agency.MachineViewSpec{Attachment: attachment,
		Consequences: projectedConsequences(claim, references, attachment.MayInitiate())}
	if claim != nil || attachment.MayInitiate() {
		spec.Targets = []agency.ResolvedTarget{self}
	}
	publicSpec := agency.AgentViewSpec{}
	if claim != nil {
		if err := projectClaim(claim, &spec, &publicSpec); err != nil {
			return BoundView{}, err
		}
	}
	for _, reference := range references {
		if err := projectReference(reference, &spec, &publicSpec); err != nil {
			return BoundView{}, err
		}
	}
	authorityView, err := agency.NewViewAuthority(spec)
	if err != nil {
		return BoundView{}, err
	}
	viewHandle, err := currentViewHandle(attachment, operation, authorityView.Digest())
	if err != nil {
		return BoundView{}, err
	}
	publicSpec.Handle = viewHandle
	publicSpec.Authority = authorityView
	publicView, err := agency.NewAgentView(publicSpec)
	if err != nil {
		return BoundView{}, err
	}
	return BoundView{authority: authorityView, public: publicView}, nil
}

func projectedConsequences(claim *projectedClaim,
	references []projectedReference, mayInitiate bool,
) []agency.Consequence {
	var result []agency.Consequence
	if len(references) < maxProjectedReferences {
		result = append(result, agency.ConsequencePublishReference)
	}
	if claim == nil {
		if mayInitiate {
			result = append(result, agency.ConsequenceCreateHandlings)
		}
	} else {
		result = append(result, agency.ConsequenceAdvanceHandling, agency.ConsequenceResolveCompleted,
			agency.ConsequenceResolveDeclined, agency.ConsequenceResolveUnresolved)
	}
	if len(references) == 0 {
		return result
	}
	result = append(result, agency.ConsequenceSupersedeReference)
	for _, reference := range references {
		if reference.state == "active" {
			return append(result, agency.ConsequenceRetractReference)
		}
	}
	return result
}

func currentViewHandle(attachment agency.Attachment, operation agency.OperationKey,
	authorityDigest agency.Digest,
) (agency.OpaqueHandle, error) {
	if attachment.ID().IsZero() || operation.IsZero() || authorityDigest.IsZero() {
		return agency.OpaqueHandle{}, errors.New("current View: handle authority is incomplete")
	}
	return deterministicHandle("view", attachment.ID().String(), operation.String(),
		authorityDigest.String())
}

func projectClaim(claim *projectedClaim, spec *agency.MachineViewSpec,
	publicSpec *agency.AgentViewSpec,
) error {
	subjectHandle, err := deterministicHandle("subject", claim.handlingID.String(),
		claim.head.ID().String(), claim.head.Digest().String(), fmt.Sprint(claim.fence))
	if err != nil {
		return err
	}
	subject, err := agency.NewSubjectBinding(subjectHandle, claim.handlingID, claim.head, claim.fence)
	if err != nil {
		return err
	}
	provenance, err := agency.NewProvenanceOffer(subjectHandle, claim.head)
	if err != nil {
		return err
	}
	spec.Subjects = append(spec.Subjects, subject)
	spec.Provenance = append(spec.Provenance, provenance)
	current := agency.AgentViewCurrentSpec{Subject: subjectHandle, Kind: claim.kind, Payload: claim.payload}
	for _, digest := range claim.artifacts {
		handle, err := deterministicHandle("artifact", claim.head.ID().String(), digest.String())
		if err != nil {
			return err
		}
		offer, err := agency.NewViewArtifactOffer(handle, digest)
		if err != nil {
			return err
		}
		spec.Artifacts = append(spec.Artifacts, offer)
		current.Artifacts = append(current.Artifacts, handle)
	}
	publicSpec.Current = &current
	return nil
}

func projectReference(reference projectedReference, spec *agency.MachineViewSpec,
	publicSpec *agency.AgentViewSpec,
) error {
	headHandle, err := deterministicHandle("reference", reference.key.String(),
		reference.head.ID().String(), reference.head.Digest().String())
	if err != nil {
		return err
	}
	expectation, err := agency.ExpectReferenceHead(headHandle, reference.key, reference.head)
	if err != nil {
		return err
	}
	provenance, err := agency.NewProvenanceOffer(headHandle, reference.head)
	if err != nil {
		return err
	}
	spec.References = append(spec.References, expectation)
	spec.Provenance = append(spec.Provenance, provenance)
	publicReference := agency.AgentViewReferenceSpec{Head: headHandle,
		State: agency.AgentViewReferenceStateRetracted}
	if reference.state == "active" {
		publicReference.State = agency.AgentViewReferenceStateActive
		artifactHandle, err := deterministicHandle("reference-artifact", reference.head.ID().String(),
			reference.artifact.String())
		if err != nil {
			return err
		}
		offer, err := agency.NewViewArtifactOffer(artifactHandle, reference.artifact)
		if err != nil {
			return err
		}
		spec.Artifacts = append(spec.Artifacts, offer)
		publicReference.Artifact = artifactHandle
	}
	publicSpec.References = append(publicSpec.References, publicReference)
	return nil
}

func loadReferencesTx(ctx context.Context, tx *sql.Tx) ([]projectedReference, error) {
	rows, err := tx.QueryContext(ctx, `SELECT r.reference_key, r.state, r.artifact_digest,
		r.head_event_id, e.event_digest
		FROM active_references r JOIN events e ON e.event_id = r.head_event_id
		ORDER BY r.reference_key LIMIT ?`, maxProjectedReferences+1)
	if err != nil {
		return nil, fmt.Errorf("current View: load References: %w", err)
	}
	defer rows.Close()
	var result []projectedReference
	for rows.Next() {
		reference, err := scanProjectedReference(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, reference)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("current View: iterate References: %w", err)
	}
	if len(result) > maxProjectedReferences {
		return nil, fmt.Errorf("current View: Reference projection exceeds %d", maxProjectedReferences)
	}
	return result, nil
}

func scanProjectedReference(row rowScanner) (projectedReference, error) {
	var keyValue, state, eventValue, digestValue string
	var artifactValue sql.NullString
	if err := row.Scan(&keyValue, &state, &artifactValue, &eventValue, &digestValue); err != nil {
		return projectedReference{}, fmt.Errorf("current View: scan Reference: %w", err)
	}
	key, err := agency.NewReferenceKey(keyValue)
	if err != nil {
		return projectedReference{}, errors.New("current View: corrupt Reference key")
	}
	eventID, err := agency.NewEventID(eventValue)
	if err != nil {
		return projectedReference{}, errors.New("current View: corrupt Reference Event ID")
	}
	digest, err := agency.ParseDigest(digestValue)
	if err != nil {
		return projectedReference{}, errors.New("current View: corrupt Reference Event digest")
	}
	head, err := agency.NewEventRef(eventID, digest)
	if err != nil {
		return projectedReference{}, err
	}
	reference := projectedReference{key: key, head: head, state: state}
	if state == "retracted" && !artifactValue.Valid {
		return reference, nil
	}
	if state != "active" || !artifactValue.Valid {
		return projectedReference{}, errors.New("current View: corrupt Reference state")
	}
	reference.artifact, err = agency.ParseDigest(artifactValue.String)
	if err != nil {
		return projectedReference{}, errors.New("current View: corrupt Reference Artifact digest")
	}
	return reference, nil
}

func loadEventArtifactsTx(ctx context.Context, tx *sql.Tx,
	eventID agency.EventID,
) ([]agency.Digest, error) {
	rows, err := tx.QueryContext(ctx, `SELECT artifact_digest FROM event_artifacts
		WHERE event_id = ? ORDER BY artifact_digest`, eventID.String())
	if err != nil {
		return nil, fmt.Errorf("current View: load Event Artifacts: %w", err)
	}
	defer rows.Close()
	var result []agency.Digest
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		digest, err := agency.ParseDigest(value)
		if err != nil {
			return nil, errors.New("current View: corrupt Event Artifact digest")
		}
		result = append(result, digest)
	}
	return result, rows.Err()
}

type storedEventProjection struct {
	SchemaVersion int `json:"schema_version"`
	Machine       struct {
		ID             string `json:"event_id"`
		AcceptedAt     string `json:"accepted_at"`
		OriginSequence uint64 `json:"origin_sequence"`
		Source         string `json:"source_principal"`
		RequestDigest  string `json:"request_digest"`
	} `json:"machine"`
	Semantic struct {
		Kind    string `json:"kind"`
		Payload string `json:"payload"`
	} `json:"semantic"`
	Evidence struct {
		Artifacts []string `json:"artifacts"`
	} `json:"evidence"`
}

func loadStoredEventTx(ctx context.Context, tx *sql.Tx, idValue string) (
	agency.EventRef, agency.SemanticLabel, agency.SemanticPayload, []agency.Digest, error,
) {
	var digestValue, sourceValue, requestValue, acceptedValue string
	var originSequence uint64
	var canonical []byte
	err := tx.QueryRowContext(ctx, `SELECT event_digest, origin_sequence, source_principal_id,
		request_digest, accepted_at, canonical_json FROM events WHERE event_id = ?`, idValue).
		Scan(&digestValue, &originSequence, &sourceValue, &requestValue, &acceptedValue, &canonical)
	if err != nil {
		return agency.EventRef{}, agency.SemanticLabel{}, agency.SemanticPayload{}, nil,
			fmt.Errorf("current View: load Event: %w", err)
	}
	eventRef, kind, payload, canonicalArtifacts, err := inspectStoredEvent(idValue, digestValue,
		originSequence, sourceValue, requestValue, acceptedValue, canonical)
	if err != nil {
		return agency.EventRef{}, agency.SemanticLabel{}, agency.SemanticPayload{}, nil, err
	}
	artifacts, err := loadEventArtifactsTx(ctx, tx, eventRef.ID())
	if err != nil {
		return agency.EventRef{}, agency.SemanticLabel{}, agency.SemanticPayload{}, nil, err
	}
	if !slices.Equal(canonicalArtifacts, artifacts) {
		return agency.EventRef{}, agency.SemanticLabel{}, agency.SemanticPayload{}, nil,
			errors.New("current View: Event Artifact pins diverge from canonical bytes")
	}
	return eventRef, kind, payload, artifacts, nil
}

func inspectStoredEvent(idValue, digestValue string, originSequence uint64,
	sourceValue, requestValue, acceptedValue string, canonical []byte,
) (agency.EventRef, agency.SemanticLabel, agency.SemanticPayload, []agency.Digest, error) {
	eventID, err := agency.NewEventID(idValue)
	if err != nil {
		return agency.EventRef{}, agency.SemanticLabel{}, agency.SemanticPayload{}, nil,
			errors.New("current View: corrupt Event ID")
	}
	digest, err := agency.ParseDigest(digestValue)
	if err != nil || agency.Sum(canonical) != digest {
		return agency.EventRef{}, agency.SemanticLabel{}, agency.SemanticPayload{}, nil,
			errors.New("current View: corrupt Event bytes")
	}
	var wire storedEventProjection
	if err := json.Unmarshal(canonical, &wire); err != nil || wire.SchemaVersion != 1 {
		return agency.EventRef{}, agency.SemanticLabel{}, agency.SemanticPayload{}, nil,
			errors.New("current View: invalid Event projection")
	}
	if err := validateStoredEventAuthority(wire, idValue, originSequence, sourceValue,
		requestValue, acceptedValue); err != nil {
		return agency.EventRef{}, agency.SemanticLabel{}, agency.SemanticPayload{}, nil, err
	}
	kind, err := agency.NewSemanticLabel(wire.Semantic.Kind)
	if err != nil {
		return agency.EventRef{}, agency.SemanticLabel{}, agency.SemanticPayload{}, nil,
			errors.New("current View: invalid Event semantic kind")
	}
	payload, err := agency.NewSemanticPayload(wire.Semantic.Payload)
	if err != nil {
		return agency.EventRef{}, agency.SemanticLabel{}, agency.SemanticPayload{}, nil,
			errors.New("current View: invalid Event semantic payload")
	}
	artifacts, err := parseStoredEventArtifacts(wire.Evidence.Artifacts)
	if err != nil {
		return agency.EventRef{}, agency.SemanticLabel{}, agency.SemanticPayload{}, nil, err
	}
	eventRef, err := agency.NewEventRef(eventID, digest)
	return eventRef, kind, payload, artifacts, err
}

func validateStoredEventAuthority(wire storedEventProjection, idValue string,
	originSequence uint64, sourceValue, requestValue, acceptedValue string,
) error {
	if wire.Machine.ID != idValue || wire.Machine.OriginSequence != originSequence ||
		wire.Machine.Source != sourceValue || wire.Machine.RequestDigest != requestValue {
		return errors.New("current View: Event authority columns diverge from canonical bytes")
	}
	acceptedAt, acceptedErr := parseTime(acceptedValue)
	wireAcceptedAt, wireAcceptedErr := time.Parse(time.RFC3339Nano, wire.Machine.AcceptedAt)
	if acceptedErr != nil || wireAcceptedErr != nil || !acceptedAt.Equal(wireAcceptedAt) {
		return errors.New("current View: Event accepted time diverges from canonical bytes")
	}
	if _, err := agency.NewAgentPrincipalID(sourceValue); err != nil {
		return errors.New("current View: corrupt Event source Principal")
	}
	if _, err := agency.ParseDigest(requestValue); err != nil || originSequence == 0 {
		return errors.New("current View: corrupt Event machine authority")
	}
	return nil
}

func parseStoredEventArtifacts(values []string) ([]agency.Digest, error) {
	artifacts := make([]agency.Digest, len(values))
	for index, value := range values {
		var err error
		artifacts[index], err = agency.ParseDigest(value)
		if err != nil {
			return nil, errors.New("current View: invalid Event Artifact digest")
		}
	}
	slices.SortFunc(artifacts, func(left, right agency.Digest) int {
		return strings.Compare(left.String(), right.String())
	})
	for index := 1; index < len(artifacts); index++ {
		if artifacts[index] == artifacts[index-1] {
			return nil, errors.New("current View: duplicate Event Artifact digest")
		}
	}
	return artifacts, nil
}

func deterministicHandle(domain string, values ...string) (agency.OpaqueHandle, error) {
	material := domain + "\x00" + strings.Join(values, "\x00")
	digest := agency.Sum([]byte(material)).String()
	return agency.NewOpaqueHandle("r7:" + domain + ":" + strings.TrimPrefix(digest, "sha256:"))
}
