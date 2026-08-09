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
		ID                string `json:"event_id"`
		AcceptedAt        string `json:"accepted_at"`
		OriginSequence    uint64 `json:"origin_sequence"`
		CausalDepth       uint16 `json:"causal_depth"`
		Source            string `json:"source_principal"`
		RequestDigest     string `json:"request_digest"`
		Consequence       string `json:"consequence"`
		InReplyToDelivery string `json:"in_reply_to_delivery_id,omitempty"`
	} `json:"machine"`
	Semantic struct {
		Kind    string `json:"kind"`
		Payload string `json:"payload"`
	} `json:"semantic"`
	Evidence struct {
		Artifacts   []string                   `json:"artifacts"`
		Causation   []storedEventRefProjection `json:"causation"`
		Correlation *storedEventRefProjection  `json:"correlation"`
	} `json:"evidence"`
}

type storedEventRefProjection struct {
	ID     string `json:"id"`
	Digest string `json:"digest"`
}

type storedEventDetails struct {
	ref         agency.EventRef
	kind        agency.SemanticLabel
	payload     agency.SemanticPayload
	artifacts   []agency.Digest
	causation   []agency.EventRef
	correlation agency.EventRef
	consequence agency.Consequence
	inReplyTo   agency.DeliveryID
}

func loadStoredEventTx(ctx context.Context, tx *sql.Tx, idValue string) (
	agency.EventRef, agency.SemanticLabel, agency.SemanticPayload, []agency.Digest, error,
) {
	details, err := loadStoredEventDetailsTx(ctx, tx, idValue)
	if err != nil {
		return agency.EventRef{}, agency.SemanticLabel{}, agency.SemanticPayload{}, nil, err
	}
	return details.ref, details.kind, details.payload, details.artifacts, nil
}

func loadStoredEventDetailsTx(ctx context.Context, tx *sql.Tx,
	idValue string,
) (storedEventDetails, error) {
	var digestValue, sourceValue, requestValue, acceptedValue string
	var originSequence uint64
	var causalDepth uint16
	var canonical []byte
	err := tx.QueryRowContext(ctx, `SELECT event_digest, origin_sequence, causal_depth,
		source_principal_id, request_digest, accepted_at, canonical_json FROM events WHERE event_id = ?`, idValue).
		Scan(&digestValue, &originSequence, &causalDepth, &sourceValue, &requestValue,
			&acceptedValue, &canonical)
	if err != nil {
		return storedEventDetails{}, fmt.Errorf("current View: load Event: %w", err)
	}
	details, canonicalArtifacts, err := inspectStoredEventDetails(idValue, digestValue,
		originSequence, causalDepth, sourceValue, requestValue, acceptedValue, canonical)
	if err != nil {
		return storedEventDetails{}, err
	}
	artifacts, err := loadEventArtifactsTx(ctx, tx, details.ref.ID())
	if err != nil {
		return storedEventDetails{}, err
	}
	if !slices.Equal(canonicalArtifacts, artifacts) {
		return storedEventDetails{}, errors.New("current View: Event Artifact pins diverge from canonical bytes")
	}
	details.artifacts = artifacts
	return details, nil
}

func inspectStoredEvent(idValue, digestValue string, originSequence uint64, causalDepth uint16,
	sourceValue, requestValue, acceptedValue string, canonical []byte,
) (agency.EventRef, agency.SemanticLabel, agency.SemanticPayload, []agency.Digest, error) {
	details, artifacts, err := inspectStoredEventDetails(idValue, digestValue, originSequence,
		causalDepth, sourceValue, requestValue, acceptedValue, canonical)
	if err != nil {
		return agency.EventRef{}, agency.SemanticLabel{}, agency.SemanticPayload{}, nil, err
	}
	return details.ref, details.kind, details.payload, artifacts, nil
}

func inspectStoredEventDetails(idValue, digestValue string, originSequence uint64, causalDepth uint16,
	sourceValue, requestValue, acceptedValue string, canonical []byte,
) (storedEventDetails, []agency.Digest, error) {
	eventID, err := agency.NewEventID(idValue)
	if err != nil {
		return storedEventDetails{}, nil, errors.New("current View: corrupt Event ID")
	}
	digest, err := agency.ParseDigest(digestValue)
	if err != nil || agency.Sum(canonical) != digest {
		return storedEventDetails{}, nil, errors.New("current View: corrupt Event bytes")
	}
	var wire storedEventProjection
	if err := json.Unmarshal(canonical, &wire); err != nil || wire.SchemaVersion != 3 {
		return storedEventDetails{}, nil, errors.New("current View: invalid Event projection")
	}
	if err := validateStoredEventAuthority(wire, idValue, originSequence, causalDepth, sourceValue,
		requestValue, acceptedValue); err != nil {
		return storedEventDetails{}, nil, err
	}
	kind, err := agency.NewSemanticLabel(wire.Semantic.Kind)
	if err != nil {
		return storedEventDetails{}, nil, errors.New("current View: invalid Event semantic kind")
	}
	payload, err := agency.NewSemanticPayload(wire.Semantic.Payload)
	if err != nil {
		return storedEventDetails{}, nil, errors.New("current View: invalid Event semantic payload")
	}
	artifacts, err := parseStoredEventArtifacts(wire.Evidence.Artifacts)
	if err != nil {
		return storedEventDetails{}, nil, err
	}
	eventRef, err := agency.NewEventRef(eventID, digest)
	if err != nil {
		return storedEventDetails{}, nil, err
	}
	causation, correlation, err := parseStoredEventRelations(wire.Evidence.Causation,
		wire.Evidence.Correlation)
	if err != nil {
		return storedEventDetails{}, nil, err
	}
	consequence, err := parseStoredEventConsequence(wire.Machine.Consequence)
	if err != nil {
		return storedEventDetails{}, nil, err
	}
	var inReplyTo agency.DeliveryID
	if wire.Machine.InReplyToDelivery != "" {
		inReplyTo, err = agency.ParseDeliveryID(wire.Machine.InReplyToDelivery)
		if err != nil {
			return storedEventDetails{}, nil, errors.New("current View: invalid Event reply Delivery")
		}
	}
	return storedEventDetails{ref: eventRef, kind: kind, payload: payload,
		causation: causation, correlation: correlation, consequence: consequence,
		inReplyTo: inReplyTo}, artifacts, nil
}

func parseStoredEventConsequence(value string) (agency.Consequence, error) {
	for consequence := agency.ConsequenceCreateHandlings; consequence <= agency.ConsequenceObserveUnresolved; consequence++ {
		if consequence.String() == value {
			return consequence, nil
		}
	}
	return agency.ConsequenceInvalid, errors.New("current View: invalid Event consequence")
}

func parseStoredEventRelations(causationWires []storedEventRefProjection,
	correlationWire *storedEventRefProjection,
) ([]agency.EventRef, agency.EventRef, error) {
	if len(causationWires) > agency.MaxCausationHandles {
		return nil, agency.EventRef{}, errors.New("current View: excessive Event causation")
	}
	causation := make([]agency.EventRef, 0, len(causationWires))
	seen := make(map[string]struct{}, len(causationWires))
	for _, wire := range causationWires {
		ref, err := parseStoredEventRef(wire)
		if err != nil {
			return nil, agency.EventRef{}, err
		}
		key := ref.ID().String() + "\x00" + ref.Digest().String()
		if _, duplicate := seen[key]; duplicate {
			return nil, agency.EventRef{}, errors.New("current View: duplicate Event causation")
		}
		seen[key] = struct{}{}
		causation = append(causation, ref)
	}
	var correlation agency.EventRef
	if correlationWire != nil {
		var err error
		correlation, err = parseStoredEventRef(*correlationWire)
		if err != nil {
			return nil, agency.EventRef{}, err
		}
	}
	return causation, correlation, nil
}

func parseStoredEventRef(wire storedEventRefProjection) (agency.EventRef, error) {
	id, err := agency.NewEventID(wire.ID)
	if err != nil {
		return agency.EventRef{}, errors.New("current View: invalid Event relation ID")
	}
	digest, err := agency.ParseDigest(wire.Digest)
	if err != nil {
		return agency.EventRef{}, errors.New("current View: invalid Event relation digest")
	}
	ref, err := agency.NewEventRef(id, digest)
	if err != nil {
		return agency.EventRef{}, errors.New("current View: invalid Event relation")
	}
	return ref, nil
}

func validateStoredEventAuthority(wire storedEventProjection, idValue string,
	originSequence uint64, causalDepth uint16, sourceValue, requestValue, acceptedValue string,
) error {
	if wire.Machine.ID != idValue || wire.Machine.OriginSequence != originSequence ||
		wire.Machine.CausalDepth != causalDepth || wire.Machine.Source != sourceValue ||
		wire.Machine.RequestDigest != requestValue {
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
	if _, err := agency.ParseDigest(requestValue); err != nil || originSequence == 0 ||
		causalDepth > agency.MaxPeerCausalDepth {
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
