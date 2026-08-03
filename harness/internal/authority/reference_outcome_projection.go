package authority

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

const (
	referenceOutcomeRebuildBatch = 64
	maxReferenceOutcomeCount     = int64(1<<63 - 1)
)

type referenceOutcome uint8

const (
	referenceOutcomeInvalid referenceOutcome = iota
	referenceOutcomeCompleted
	referenceOutcomeDeclined
	referenceOutcomeUnresolved
)

func (outcome referenceOutcome) column() string {
	switch outcome {
	case referenceOutcomeCompleted:
		return "completed_count"
	case referenceOutcomeDeclined:
		return "declined_count"
	case referenceOutcomeUnresolved:
		return "unresolved_count"
	default:
		return ""
	}
}

func terminalReferenceOutcome(consequence agency.Consequence) (referenceOutcome, bool) {
	switch consequence {
	case agency.ConsequenceResolveCompleted:
		return referenceOutcomeCompleted, true
	case agency.ConsequenceResolveDeclined:
		return referenceOutcomeDeclined, true
	case agency.ConsequenceResolveUnresolved:
		return referenceOutcomeUnresolved, true
	default:
		return referenceOutcomeInvalid, false
	}
}

func updateReferenceOutcomeProjectionTx(ctx context.Context, tx *sql.Tx, event agency.Event) error {
	outcome, terminal := terminalReferenceOutcome(event.Consequence())
	if !terminal {
		return nil
	}
	return incrementReferenceOutcomesTx(ctx, tx, outcome, directEventReferences(event))
}

func directEventReferences(event agency.Event) []agency.EventRef {
	values := event.Causation()
	if correlation, present := event.Correlation(); present {
		values = append(values, correlation)
	}
	return deduplicateEventRefs(values)
}

func deduplicateEventRefs(values []agency.EventRef) []agency.EventRef {
	result := make([]agency.EventRef, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		key := value.ID().String() + "\x00" + value.Digest().String()
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, value)
	}
	return result
}

func incrementReferenceOutcomesTx(ctx context.Context, tx *sql.Tx, outcome referenceOutcome,
	references []agency.EventRef,
) error {
	column := outcome.column()
	if column == "" {
		return errors.New("reference outcome projection: invalid terminal outcome")
	}
	for _, reference := range references {
		matches, err := exactReferenceExistsTx(ctx, tx, reference)
		if err != nil {
			return err
		}
		if !matches {
			continue
		}
		query := fmt.Sprintf(`INSERT INTO reference_outcome_projection(
			reference_event_id, %s) VALUES(?, 1)
			ON CONFLICT(reference_event_id) DO UPDATE SET %s = %s + 1
			WHERE %s < ?`, column, column, column, column)
		result, err := tx.ExecContext(ctx, query, reference.ID().String(), maxReferenceOutcomeCount)
		if err != nil {
			return fmt.Errorf("reference outcome projection: increment %s: %w", column, err)
		}
		if err := requireOneRow(result, "Reference outcome projection"); err != nil {
			return err
		}
	}
	return nil
}

func exactReferenceExistsTx(ctx context.Context, tx *sql.Tx, reference agency.EventRef) (bool, error) {
	var exists int
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM reference_lineage l JOIN events e ON e.event_id = l.event_id
		WHERE l.event_id = ? AND e.event_digest = ?)`, reference.ID().String(),
		reference.Digest().String()).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("reference outcome projection: verify exact Reference: %w", err)
	}
	return exists == 1, nil
}

// rebuildReferenceOutcomeProjection atomically reconstructs the bounded-read
// projection from immutable Events and terminal Handlings. It is deliberately
// not a daemon API: no production caller may use a derived projection to
// mutate authority.
func (s *Store) rebuildReferenceOutcomeProjection(ctx context.Context) error {
	if ctx == nil {
		return errors.New("rebuild reference outcome projection: nil context")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.requireOpen(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("rebuild reference outcome projection: begin: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM reference_outcome_projection`); err != nil {
		return fmt.Errorf("rebuild reference outcome projection: clear: %w", err)
	}
	if err := rebuildReferenceOutcomesTx(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("rebuild reference outcome projection: commit: %w", err)
	}
	return nil
}

type terminalOutcomeEvent struct {
	handlingID string
	eventID    agency.EventID
	digest     agency.Digest
	canonical  []byte
	outcome    referenceOutcome
}

func rebuildReferenceOutcomesTx(ctx context.Context, tx *sql.Tx) error {
	cursor := ""
	for {
		batch, err := loadTerminalOutcomeBatchTx(ctx, tx, cursor)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		for _, item := range batch {
			references, err := parseTerminalOutcomeReferences(item)
			if err != nil {
				return err
			}
			if err := incrementReferenceOutcomesTx(ctx, tx, item.outcome, references); err != nil {
				return err
			}
		}
		cursor = batch[len(batch)-1].handlingID
	}
}

func loadTerminalOutcomeBatchTx(ctx context.Context, tx *sql.Tx,
	cursor string,
) ([]terminalOutcomeEvent, error) {
	rows, err := tx.QueryContext(ctx, `SELECT h.handling_id, e.event_id, e.event_digest,
		e.canonical_json, h.outcome FROM handlings h JOIN events e ON e.event_id = h.head_event_id
		WHERE h.state = 'terminal' AND h.handling_id > ?
		ORDER BY h.handling_id LIMIT ?`, cursor, referenceOutcomeRebuildBatch)
	if err != nil {
		return nil, fmt.Errorf("rebuild reference outcome projection: load batch: %w", err)
	}
	defer rows.Close()
	batch := make([]terminalOutcomeEvent, 0, referenceOutcomeRebuildBatch)
	for rows.Next() {
		var handlingID, eventIDValue, digestValue, outcomeValue string
		var canonical []byte
		if err := rows.Scan(&handlingID, &eventIDValue, &digestValue, &canonical, &outcomeValue); err != nil {
			return nil, fmt.Errorf("rebuild reference outcome projection: scan batch: %w", err)
		}
		eventID, err := agency.NewEventID(eventIDValue)
		if err != nil {
			return nil, errors.New("rebuild reference outcome projection: corrupt Event ID")
		}
		digest, err := agency.ParseDigest(digestValue)
		if err != nil {
			return nil, errors.New("rebuild reference outcome projection: corrupt Event digest")
		}
		outcome, err := parseStoredReferenceOutcome(outcomeValue)
		if err != nil {
			return nil, err
		}
		batch = append(batch, terminalOutcomeEvent{handlingID: handlingID, eventID: eventID,
			digest: digest, canonical: append([]byte(nil), canonical...), outcome: outcome})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rebuild reference outcome projection: iterate batch: %w", err)
	}
	return batch, nil
}

func parseStoredReferenceOutcome(value string) (referenceOutcome, error) {
	switch value {
	case "completed":
		return referenceOutcomeCompleted, nil
	case "declined":
		return referenceOutcomeDeclined, nil
	case "unresolved":
		return referenceOutcomeUnresolved, nil
	default:
		return referenceOutcomeInvalid, errors.New("rebuild reference outcome projection: corrupt outcome")
	}
}

type outcomeEventRefWire struct {
	ID     string `json:"id"`
	Digest string `json:"digest"`
}

type outcomeEventWire struct {
	SchemaVersion int `json:"schema_version"`
	Machine       struct {
		ID          string `json:"event_id"`
		Consequence string `json:"consequence"`
	} `json:"machine"`
	Evidence struct {
		Causation   []outcomeEventRefWire `json:"causation"`
		Correlation *outcomeEventRefWire  `json:"correlation"`
	} `json:"evidence"`
}

func parseTerminalOutcomeReferences(item terminalOutcomeEvent) ([]agency.EventRef, error) {
	if agency.Sum(item.canonical) != item.digest {
		return nil, errors.New("rebuild reference outcome projection: Event digest mismatch")
	}
	var wire outcomeEventWire
	if err := json.Unmarshal(item.canonical, &wire); err != nil {
		return nil, fmt.Errorf("rebuild reference outcome projection: parse Event: %w", err)
	}
	if wire.SchemaVersion != 2 || wire.Machine.ID != item.eventID.String() ||
		wire.Machine.Consequence != outcomeConsequence(item.outcome) {
		return nil, errors.New("rebuild reference outcome projection: terminal Event mismatch")
	}
	wires := append([]outcomeEventRefWire(nil), wire.Evidence.Causation...)
	if wire.Evidence.Correlation != nil {
		wires = append(wires, *wire.Evidence.Correlation)
	}
	if len(wires) > agency.MaxCausationHandles+1 {
		return nil, errors.New("rebuild reference outcome projection: Event references exceed bound")
	}
	references := make([]agency.EventRef, 0, len(wires))
	for _, value := range wires {
		id, err := agency.NewEventID(value.ID)
		if err != nil {
			return nil, errors.New("rebuild reference outcome projection: corrupt cited Event ID")
		}
		digest, err := agency.ParseDigest(value.Digest)
		if err != nil {
			return nil, errors.New("rebuild reference outcome projection: corrupt cited Event digest")
		}
		reference, err := agency.NewEventRef(id, digest)
		if err != nil {
			return nil, err
		}
		references = append(references, reference)
	}
	return deduplicateEventRefs(references), nil
}

func outcomeConsequence(outcome referenceOutcome) string {
	switch outcome {
	case referenceOutcomeCompleted:
		return agency.ConsequenceResolveCompleted.String()
	case referenceOutcomeDeclined:
		return agency.ConsequenceResolveDeclined.String()
	case referenceOutcomeUnresolved:
		return agency.ConsequenceResolveUnresolved.String()
	default:
		return ""
	}
}
