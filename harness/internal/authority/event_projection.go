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
		ID             string `json:"event_id"`
		AcceptedAt     string `json:"accepted_at"`
		OriginSequence uint64 `json:"origin_sequence"`
		CausalDepth    uint16 `json:"causal_depth"`
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
	var causalDepth uint16
	var canonical []byte
	err := tx.QueryRowContext(ctx, `SELECT event_digest, origin_sequence, causal_depth,
		source_principal_id, request_digest, accepted_at, canonical_json FROM events WHERE event_id = ?`, idValue).
		Scan(&digestValue, &originSequence, &causalDepth, &sourceValue, &requestValue,
			&acceptedValue, &canonical)
	if err != nil {
		return agency.EventRef{}, agency.SemanticLabel{}, agency.SemanticPayload{}, nil,
			fmt.Errorf("current View: load Event: %w", err)
	}
	eventRef, kind, payload, canonicalArtifacts, err := inspectStoredEvent(idValue, digestValue,
		originSequence, causalDepth, sourceValue, requestValue, acceptedValue, canonical)
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

func inspectStoredEvent(idValue, digestValue string, originSequence uint64, causalDepth uint16,
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
	if err := json.Unmarshal(canonical, &wire); err != nil || wire.SchemaVersion != 2 {
		return agency.EventRef{}, agency.SemanticLabel{}, agency.SemanticPayload{}, nil,
			errors.New("current View: invalid Event projection")
	}
	if err := validateStoredEventAuthority(wire, idValue, originSequence, causalDepth, sourceValue,
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
