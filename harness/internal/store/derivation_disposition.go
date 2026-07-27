package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var ErrDerivationDispositionConflict = errors.New("Work derivation disposition conflicts with durable state")

const (
	derivationHandlingDomain = "mnemon/r5/derivation-handling/v1\x00"
	parentStaleDisposition   = "parent_stale"
)

// ReconcileWorkDerivationDisposition is the restart/recovery entry point for
// the same transaction helper used after a terminal child Work CAS. It is a
// no-op when child is not derived or remains nonterminal.
func (s *Store) ReconcileWorkDerivationDisposition(ctx context.Context, child model.WorkRef) error {
	if s == nil || s.db == nil || ctx == nil || child.IsZero() {
		return errors.New("reconcile Work derivation disposition: incomplete input")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("reconcile Work derivation disposition: begin: %w", err)
	}
	defer tx.Rollback()
	if err := reconcileWorkDerivationDisposition(ctx, tx, child); err != nil {
		return fmt.Errorf("reconcile Work derivation disposition: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("reconcile Work derivation disposition: commit: %w", err)
	}
	return nil
}

// reconcileWorkDerivationDisposition must run in the transaction that makes a
// derived child terminal. It rereads the immutable derivation before choosing
// exactly one durable outcome.
func reconcileWorkDerivationDisposition(ctx context.Context, tx *sql.Tx, child model.WorkRef) error {
	if ctx == nil || tx == nil || child.IsZero() {
		return errors.New("derivation disposition: incomplete input")
	}
	authority, found, err := readWorkDerivationAuthority(ctx, tx, child)
	if err != nil || !found {
		return err
	}
	derivation := authority.derivation
	childWork, err := readReviewWork(ctx, tx, derivation.Child())
	if err != nil {
		return fmt.Errorf("derivation disposition: read child: %w", err)
	}
	if childWork.ChannelID() != derivation.ChildChannelID() || childWork.Ref() != derivation.Child() {
		return fmt.Errorf("%w: child scope changed", ErrDerivationDispositionConflict)
	}
	if !childWork.State().Terminal() {
		return nil
	}

	parent, err := readReviewWork(ctx, tx, derivation.Parent())
	if err != nil {
		return fmt.Errorf("derivation disposition: read latest parent: %w", err)
	}
	if parent.ChannelID() != derivation.ParentChannelID() ||
		parent.Participants().ReviewerPeerID() != derivation.Child().HomePeerID() {
		return fmt.Errorf("%w: parent or child-home scope changed", ErrDerivationDispositionConflict)
	}
	frozenState, err := readFrozenDerivationParentState(ctx, tx, authority)
	if err != nil {
		return err
	}
	terminalEvent, artifactRoots, err := readTerminalChildEvent(ctx, tx, authority, childWork)
	if err != nil {
		return err
	}

	handlingID, err := deterministicDerivationHandlingID(derivation.OperationID())
	if err != nil {
		return err
	}
	if existing, err := readAgentHandling(ctx, tx, handlingID); err == nil {
		parentStale, err := validateExistingDerivationHandling(existing, terminalEvent.id)
		if err != nil {
			return err
		}
		if parentStale {
			return requireExactDerivationHandlingPins(ctx, tx, handlingID, nil, existing.CreatedAt())
		}
		return requireExactDerivationHandlingPins(ctx, tx, handlingID, artifactRoots, existing.CreatedAt())
	} else if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("derivation disposition: read existing Handling: %w", err)
	}

	parentExact := parent.Version() == derivation.ParentVersion() && parent.State() == frozenState &&
		parent.UpdatedBy() == derivation.ParentEventID()
	status, disposition := model.HandlingPending, ""
	if !parentExact {
		status, disposition = model.HandlingCompleted, parentStaleDisposition
	}
	handling, err := model.NewHandling(model.HandlingSpec{
		ID:              handlingID,
		ProfileID:       model.TeamworkProfileID(),
		EventID:         terminalEvent.id,
		Status:          status,
		AvailableAt:     terminalEvent.acceptedAt,
		LastDisposition: disposition,
		CreatedAt:       terminalEvent.acceptedAt,
		UpdatedAt:       terminalEvent.acceptedAt,
	})
	if err != nil {
		return fmt.Errorf("derivation disposition: construct Handling: %w", err)
	}
	if _, err := insertAgentHandling(ctx, tx, handling); err != nil {
		if errors.Is(err, ErrHandlingConflict) {
			return fmt.Errorf("%w: terminal Event already has another Handling", ErrDerivationDispositionConflict)
		}
		return fmt.Errorf("derivation disposition: insert Handling: %w", err)
	}
	if status == model.HandlingPending {
		return requireExactDerivationHandlingPins(ctx, tx, handlingID, artifactRoots, handling.CreatedAt())
	}
	if err := requireExactDerivationHandlingPins(ctx, tx, handlingID, nil, handling.CreatedAt()); err != nil {
		return err
	}
	return nil
}

type workDerivationAuthority struct {
	derivation model.WorkDerivation
	childEpoch model.OriginEpoch
}

func readWorkDerivationAuthority(ctx context.Context, tx *sql.Tx,
	child model.WorkRef,
) (workDerivationAuthority, bool, error) {
	var operationText, childChannel, childHome, childWork, parentChannel, parentHome, parentWork string
	var parentEvent, created string
	var parentVersion uint64
	err := tx.QueryRowContext(ctx, `SELECT operation_id,child_channel_id,
		child_home_peer_id,child_work_id,parent_channel_id,parent_home_peer_id,parent_work_id,
		parent_version,parent_event_id,created_at FROM work_derivations
		WHERE child_home_peer_id=? AND child_work_id=?`,
		child.HomePeerID().String(), child.WorkID().String()).Scan(&operationText,
		&childChannel, &childHome, &childWork, &parentChannel, &parentHome, &parentWork,
		&parentVersion, &parentEvent, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return workDerivationAuthority{}, false, nil
	}
	if err != nil {
		return workDerivationAuthority{}, false, fmt.Errorf("derivation disposition: read derivation: %w", err)
	}
	derivation, err := parseWorkDerivationRow(operationText, childChannel, childHome,
		childWork, parentChannel, parentHome, parentWork, parentVersion, parentEvent, created)
	if err != nil {
		return workDerivationAuthority{}, false, err
	}
	if derivation.Child() != child {
		return workDerivationAuthority{}, false,
			fmt.Errorf("%w: derivation child identity changed", ErrDerivationDispositionConflict)
	}
	if err := requireCommittedDerivationOperation(ctx, tx, derivation.OperationID()); err != nil {
		return workDerivationAuthority{}, false, err
	}
	var count uint64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM work_derivations WHERE operation_id=?`,
		derivation.OperationID().String()).Scan(&count); err != nil {
		return workDerivationAuthority{}, false,
			fmt.Errorf("derivation disposition: count operation derivations: %w", err)
	}
	if count != 1 {
		return workDerivationAuthority{}, false,
			fmt.Errorf("%w: operation does not have exactly one derivation", ErrDerivationDispositionConflict)
	}
	node, err := readNode(ctx, tx)
	if err != nil {
		return workDerivationAuthority{}, false, fmt.Errorf("derivation disposition: read local Node: %w", err)
	}
	if derivation.Child().HomePeerID() != node.PeerID() {
		return workDerivationAuthority{}, false, fmt.Errorf("%w: derived child is not homed by the local Node",
			ErrDerivationDispositionConflict)
	}
	return workDerivationAuthority{derivation: derivation, childEpoch: node.OriginEpoch()}, true, nil
}

func parseWorkDerivationRow(operationText, childChannelText, childHomeText,
	childWorkText, parentChannelText, parentHomeText, parentWorkText string, parentVersion uint64,
	parentEventText, createdText string,
) (model.WorkDerivation, error) {
	operation, err := model.ParseOperationID(operationText)
	if err != nil {
		return model.WorkDerivation{}, fmt.Errorf("%w: invalid derivation operation", ErrDerivationDispositionConflict)
	}
	childChannel, err := model.ParseChannelID(childChannelText)
	if err != nil {
		return model.WorkDerivation{}, err
	}
	childHome, err := model.ParsePeerID(childHomeText)
	if err != nil {
		return model.WorkDerivation{}, err
	}
	childWork, err := model.ParseWorkID(childWorkText)
	if err != nil {
		return model.WorkDerivation{}, err
	}
	child, err := model.NewWorkRef(childHome, childWork)
	if err != nil {
		return model.WorkDerivation{}, err
	}
	parentChannel, err := model.ParseChannelID(parentChannelText)
	if err != nil {
		return model.WorkDerivation{}, err
	}
	parentHome, err := model.ParsePeerID(parentHomeText)
	if err != nil {
		return model.WorkDerivation{}, err
	}
	parentWork, err := model.ParseWorkID(parentWorkText)
	if err != nil {
		return model.WorkDerivation{}, err
	}
	parent, err := model.NewWorkRef(parentHome, parentWork)
	if err != nil {
		return model.WorkDerivation{}, err
	}
	parentEvent, err := model.ParseEventID(parentEventText)
	if err != nil {
		return model.WorkDerivation{}, err
	}
	created, err := parseCanonicalStoreTime(createdText)
	if err != nil {
		return model.WorkDerivation{}, err
	}
	row, err := model.NewWorkDerivation(model.WorkDerivationSpec{OperationID: operation,
		ChildChannelID: childChannel, Child: child,
		ParentChannelID: parentChannel, Parent: parent, ParentVersion: parentVersion,
		ParentEventID: parentEvent, CreatedAt: created})
	if err != nil {
		return model.WorkDerivation{}, fmt.Errorf("%w: %v", ErrDerivationDispositionConflict, err)
	}
	return row, nil
}

func requireCommittedDerivationOperation(ctx context.Context, tx *sql.Tx, operation model.OperationID) error {
	var kind, status string
	var contextHash []byte
	if err := tx.QueryRowContext(ctx, `SELECT kind,status,context_hash FROM operations WHERE operation_id=?`,
		operation.String()).Scan(&kind, &status, &contextHash); err != nil {
		return fmt.Errorf("derivation disposition: read operation: %w", err)
	}
	if model.OperationKind(kind) != model.OperationTeamworkOffer || model.OperationStatus(status) != model.OperationCommitted {
		return fmt.Errorf("%w: derivation operation is not a committed teamwork.offer", ErrDerivationDispositionConflict)
	}
	if _, err := model.DigestFromBytes(contextHash); err != nil {
		return fmt.Errorf("%w: derivation operation has no canonical context", ErrDerivationDispositionConflict)
	}
	return nil
}

func readFrozenDerivationParentState(ctx context.Context, tx *sql.Tx,
	authority workDerivationAuthority,
) (model.WorkState, error) {
	derivation := authority.derivation
	var eventType, source, channel, origin, home, work string
	err := tx.QueryRowContext(ctx, `SELECT event_type,source,channel_id,origin_peer_id,work_home_peer_id,work_id
		FROM events WHERE event_id=?`, derivation.ParentEventID().String()).
		Scan(&eventType, &source, &channel, &origin, &home, &work)
	if err != nil {
		return "", fmt.Errorf("derivation disposition: read frozen parent Event: %w", err)
	}
	if source != string(model.EventSourceImported) || channel != derivation.ParentChannelID().String() ||
		origin != derivation.Parent().HomePeerID().String() ||
		home != derivation.Parent().HomePeerID().String() || work != derivation.Parent().WorkID().String() {
		return "", fmt.Errorf("%w: frozen parent Event scope changed", ErrDerivationDispositionConflict)
	}
	switch model.EventType(eventType) {
	case model.EventReviewAccepted:
		return model.WorkActive, nil
	case model.EventReviewReworkRequested:
		return model.WorkRework, nil
	default:
		return "", fmt.Errorf("%w: frozen parent Event cannot identify ACTIVE or REWORK", ErrDerivationDispositionConflict)
	}
}

type terminalChildEvent struct {
	id         model.EventID
	acceptedAt time.Time
}

func readTerminalChildEvent(ctx context.Context, tx *sql.Tx,
	authority workDerivationAuthority, child model.ReviewWork,
) (terminalChildEvent, []model.Digest, error) {
	var eventText, eventType, source, channel, originText, epochText, home, work, acceptedText string
	var artifactJSON, payloadJSON []byte
	var sequence uint64
	err := tx.QueryRowContext(ctx, `SELECT event_id,event_type,source,channel_id,origin_peer_id,origin_epoch,
		origin_seq,work_home_peer_id,work_id,accepted_at,artifact_roots_json,payload_json
		FROM events WHERE event_id=?`,
		child.UpdatedBy().String()).Scan(&eventText, &eventType, &source, &channel, &originText, &epochText,
		&sequence, &home, &work, &acceptedText, &artifactJSON, &payloadJSON)
	if err != nil {
		return terminalChildEvent{}, nil, fmt.Errorf("derivation disposition: read terminal child Event: %w", err)
	}
	eventID, err := model.ParseEventID(eventText)
	if err != nil {
		return terminalChildEvent{}, nil, err
	}
	origin, err := model.ParsePeerID(originText)
	if err != nil {
		return terminalChildEvent{}, nil, err
	}
	epoch, err := model.ParseOriginEpoch(epochText)
	if err != nil {
		return terminalChildEvent{}, nil, err
	}
	acceptedAt, err := parseCanonicalStoreTime(acceptedText)
	if err != nil {
		return terminalChildEvent{}, nil, err
	}
	if err := validateTerminalChildEventScope(authority, child, eventID, model.EventType(eventType),
		source, channel, origin, epoch, home, work, acceptedAt); err != nil {
		return terminalChildEvent{}, nil, err
	}
	payload, err := parseDerivationVersionPayload(model.EventType(eventType), payloadJSON)
	if err != nil || child.Version() <= 1 || payload.WorkVersion != child.Version()-1 ||
		payload.Iteration != child.Iteration() || !bytes.Equal(child.StateData().Bytes(), payloadJSON) {
		return terminalChildEvent{}, nil,
			fmt.Errorf("%w: terminal child payload version/iteration mismatch", ErrDerivationDispositionConflict)
	}
	terminalRefs, err := parseCanonicalDerivationArtifactRefs(artifactJSON)
	if err != nil {
		return terminalChildEvent{}, nil,
			fmt.Errorf("%w: terminal child Artifact roots: %v", ErrDerivationDispositionConflict, err)
	}
	if len(terminalRefs) != 0 {
		return terminalChildEvent{}, nil, fmt.Errorf("%w: terminal child Event carries Artifact roots",
			ErrDerivationDispositionConflict)
	}
	var roots []model.Digest
	if child.State() == model.WorkClosed {
		roots, err = readClosedChildResultArtifacts(ctx, tx, authority, child, eventID, sequence)
		if err != nil {
			return terminalChildEvent{}, nil, err
		}
	}
	if len(roots) > model.MaxCurrentArtifactRefs {
		return terminalChildEvent{}, nil, fmt.Errorf("%w: child result Artifact roots exceed current projection bound",
			ErrDerivationDispositionConflict)
	}
	return terminalChildEvent{id: eventID, acceptedAt: acceptedAt}, roots, nil
}

func validateTerminalChildEventScope(authority workDerivationAuthority, child model.ReviewWork,
	eventID model.EventID, eventType model.EventType, source, channel string,
	origin model.PeerID, epoch model.OriginEpoch, home, work string, acceptedAt time.Time,
) error {
	derivation := authority.derivation
	if eventID != child.UpdatedBy() || eventType != terminalEventType(child.State()) ||
		source != string(model.EventSourceLocal) || epoch != authority.childEpoch ||
		channel != derivation.ChildChannelID().String() || origin != derivation.Child().HomePeerID() ||
		home != derivation.Child().HomePeerID().String() ||
		work != child.Ref().WorkID().String() || !acceptedAt.Equal(child.UpdatedAt()) {
		return fmt.Errorf("%w: terminal child Event mismatch", ErrDerivationDispositionConflict)
	}
	return nil
}

func readClosedChildResultArtifacts(ctx context.Context, tx *sql.Tx,
	authority workDerivationAuthority,
	child model.ReviewWork, closedEvent model.EventID, closedSequence uint64,
) ([]model.Digest, error) {
	derivation := authority.derivation
	var causedByJSON []byte
	if err := tx.QueryRowContext(ctx, "SELECT caused_by_json FROM events WHERE event_id=?",
		closedEvent.String()).Scan(&causedByJSON); err != nil {
		return nil, fmt.Errorf("derivation disposition: read closed child causality: %w", err)
	}
	causes, err := parseCanonicalDerivationEventKeys(causedByJSON)
	if err != nil || len(causes) != 1 {
		return nil, fmt.Errorf("%w: CLOSED child requires exactly one canonical cause",
			ErrDerivationDispositionConflict)
	}
	cause := causes[0]
	var eventType, source, channel, origin, epoch, home, work, acceptedText string
	var artifactJSON, payloadJSON []byte
	var deliveredSequence uint64
	err = tx.QueryRowContext(ctx, `SELECT event_type,source,channel_id,origin_peer_id,origin_epoch,
		origin_seq,work_home_peer_id,work_id,artifact_roots_json,payload_json,accepted_at
		FROM events WHERE event_id=?`,
		cause.EventID().String()).Scan(&eventType, &source, &channel, &origin, &epoch, &deliveredSequence,
		&home, &work, &artifactJSON, &payloadJSON, &acceptedText)
	if err != nil {
		return nil, fmt.Errorf("derivation disposition: read delivered result Event: %w", err)
	}
	deliveredAt, timeErr := parseCanonicalStoreTime(acceptedText)
	if model.EventType(eventType) != model.EventReviewDelivered || source != string(model.EventSourceLocal) ||
		channel != derivation.ChildChannelID().String() || origin != derivation.Child().HomePeerID().String() ||
		epoch != authority.childEpoch.String() || home != child.Ref().HomePeerID().String() ||
		work != child.Ref().WorkID().String() || cause.OriginPeerID() != derivation.Child().HomePeerID() ||
		cause.OriginEpoch() != authority.childEpoch || deliveredSequence >= closedSequence || timeErr != nil ||
		deliveredAt.After(child.UpdatedAt()) {
		return nil, fmt.Errorf("%w: CLOSED cause is not the exact local same-Work delivered Event",
			ErrDerivationDispositionConflict)
	}
	payload, err := parseDerivationVersionPayload(model.EventReviewDelivered, payloadJSON)
	if err != nil || child.Version() <= 2 || payload.WorkVersion != child.Version()-2 ||
		payload.Iteration != child.Iteration() {
		return nil, fmt.Errorf("%w: delivered cause payload is not the adjacent Work version/iteration",
			ErrDerivationDispositionConflict)
	}
	refs, err := parseCanonicalDerivationArtifactRefs(artifactJSON)
	if err != nil {
		return nil, fmt.Errorf("%w: delivered result Artifact roots: %v", ErrDerivationDispositionConflict, err)
	}
	roots := make([]model.Digest, len(refs))
	for index, ref := range refs {
		if ref.Role() != model.ArtifactReferenced {
			return nil, fmt.Errorf("%w: delivered child result Artifact is not referenced",
				ErrDerivationDispositionConflict)
		}
		if err := requireReusableArtifactRoot(ctx, tx, ref.RootDigest()); err != nil {
			return nil, fmt.Errorf("%w: delivered child result Artifact lacks reusable provenance: %v",
				ErrDerivationDispositionConflict, err)
		}
		roots[index] = ref.RootDigest()
	}
	return roots, nil
}

func parseDerivationVersionPayload(eventType model.EventType, raw []byte) (versionPayload, error) {
	canonical, err := model.NewJSON(raw)
	if err != nil || !bytes.Equal(canonical.Bytes(), raw) {
		return versionPayload{}, ErrDerivationDispositionConflict
	}
	decode := func(destination any) error {
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(destination); err != nil {
			return err
		}
		if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			return ErrDerivationDispositionConflict
		}
		return nil
	}
	var version, iteration uint64
	switch eventType {
	case model.EventReviewClosed:
		value := struct {
			Iteration   *uint64 `json:"iteration"`
			Note        *string `json:"note"`
			WorkVersion *uint64 `json:"work_version"`
		}{}
		if err := decode(&value); err != nil || value.Iteration == nil || value.Note == nil || value.WorkVersion == nil {
			return versionPayload{}, ErrDerivationDispositionConflict
		}
		version, iteration = *value.WorkVersion, *value.Iteration
	case model.EventReviewCancelled:
		value := struct {
			Content     *string `json:"content"`
			Iteration   *uint64 `json:"iteration"`
			WorkVersion *uint64 `json:"work_version"`
		}{}
		if err := decode(&value); err != nil || value.Content == nil || *value.Content == "" ||
			value.Iteration == nil || value.WorkVersion == nil {
			return versionPayload{}, ErrDerivationDispositionConflict
		}
		version, iteration = *value.WorkVersion, *value.Iteration
	case model.EventReviewExpired:
		value := struct {
			Deadline    *string `json:"deadline"`
			Iteration   *uint64 `json:"iteration"`
			WorkVersion *uint64 `json:"work_version"`
		}{}
		if err := decode(&value); err != nil || value.Deadline == nil || value.Iteration == nil ||
			value.WorkVersion == nil {
			return versionPayload{}, ErrDerivationDispositionConflict
		}
		deadline, err := time.Parse(time.RFC3339Nano, *value.Deadline)
		if err != nil || deadline.UnixNano() <= 0 || deadline.UTC().Format(time.RFC3339Nano) != *value.Deadline {
			return versionPayload{}, ErrDerivationDispositionConflict
		}
		version, iteration = *value.WorkVersion, *value.Iteration
	case model.EventReviewDeclined, model.EventReviewDelivered:
		value := struct {
			Iteration   *uint64 `json:"iteration"`
			WorkVersion *uint64 `json:"work_version"`
		}{}
		if err := decode(&value); err != nil || value.Iteration == nil || value.WorkVersion == nil {
			return versionPayload{}, ErrDerivationDispositionConflict
		}
		version, iteration = *value.WorkVersion, *value.Iteration
	default:
		return versionPayload{}, ErrDerivationDispositionConflict
	}
	if version == 0 || iteration < 1 || iteration > 2 {
		return versionPayload{}, ErrDerivationDispositionConflict
	}
	return versionPayload{WorkVersion: version, Iteration: uint8(iteration)}, nil
}

func parseCanonicalDerivationArtifactRefs(raw []byte) ([]model.ArtifactRef, error) {
	canonical, err := model.NewJSON(raw)
	if err != nil || !bytes.Equal(canonical.Bytes(), raw) {
		return nil, ErrArtifactReference
	}
	var encoded []struct {
		RootDigest string             `json:"root_digest"`
		Role       model.ArtifactRole `json:"role"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&encoded); err != nil || encoded == nil {
		return nil, ErrArtifactReference
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) || len(encoded) > model.MaxArtifactRefs {
		return nil, ErrArtifactReference
	}
	refs := make([]model.ArtifactRef, len(encoded))
	roots := make([]model.Digest, len(encoded))
	for index, item := range encoded {
		root, err := model.ParseDigest(item.RootDigest)
		if err != nil || !item.Role.Valid() || (index > 0 && roots[index-1].String() >= root.String()) {
			return nil, ErrArtifactReference
		}
		ref, err := model.NewArtifactRef(root, item.Role)
		if err != nil {
			return nil, ErrArtifactReference
		}
		refs[index], roots[index] = ref, root
	}
	rebuilt, err := model.JSONFrom(refs)
	if err != nil || !bytes.Equal(rebuilt.Bytes(), raw) {
		return nil, ErrArtifactReference
	}
	return refs, nil
}

func parseCanonicalDerivationEventKeys(raw []byte) ([]model.EventKey, error) {
	canonical, err := model.NewJSON(raw)
	if err != nil || !bytes.Equal(canonical.Bytes(), raw) {
		return nil, ErrArtifactReference
	}
	var encoded []struct {
		EventID      string `json:"event_id"`
		OriginEpoch  string `json:"origin_epoch"`
		OriginPeerID string `json:"origin_peer_id"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&encoded); err != nil || encoded == nil {
		return nil, ErrArtifactReference
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) || len(encoded) > model.MaxCausalityRefs {
		return nil, ErrArtifactReference
	}
	keys := make([]model.EventKey, len(encoded))
	previous := ""
	for index, item := range encoded {
		peer, err := model.ParsePeerID(item.OriginPeerID)
		if err != nil {
			return nil, ErrArtifactReference
		}
		epoch, err := model.ParseOriginEpoch(item.OriginEpoch)
		if err != nil {
			return nil, ErrArtifactReference
		}
		event, err := model.ParseEventID(item.EventID)
		if err != nil {
			return nil, ErrArtifactReference
		}
		key, err := model.NewEventKey(peer, epoch, event)
		if err != nil {
			return nil, ErrArtifactReference
		}
		order := peer.String() + "\x00" + epoch.String() + "\x00" + event.String()
		if index > 0 && previous >= order {
			return nil, ErrArtifactReference
		}
		previous, keys[index] = order, key
	}
	rebuilt, err := model.JSONFrom(keys)
	if err != nil || !bytes.Equal(rebuilt.Bytes(), raw) {
		return nil, ErrArtifactReference
	}
	return keys, nil
}

func requireExactDerivationHandlingPins(ctx context.Context, tx *sql.Tx, handling model.HandlingID,
	roots []model.Digest, createdAt time.Time,
) error {
	expected := make(map[model.Digest]struct{}, len(roots))
	for _, root := range roots {
		if root.IsZero() {
			return fmt.Errorf("%w: zero Artifact root in resume authorization", ErrDerivationDispositionConflict)
		}
		expected[root] = struct{}{}
	}
	rows, err := tx.QueryContext(ctx, `SELECT root_digest,created_at FROM artifact_pins
		WHERE owner_kind='handling' AND owner_id=? ORDER BY root_digest`, handling.String())
	if err != nil {
		return fmt.Errorf("derivation disposition: read Handling Artifact pins: %w", err)
	}
	present := make(map[model.Digest]struct{}, len(expected))
	for rows.Next() {
		var rootText, createdText string
		if err := rows.Scan(&rootText, &createdText); err != nil {
			rows.Close()
			return fmt.Errorf("derivation disposition: scan Handling Artifact pin: %w", err)
		}
		root, err := model.ParseDigest(rootText)
		if err != nil {
			rows.Close()
			return fmt.Errorf("%w: invalid Handling Artifact pin root", ErrDerivationDispositionConflict)
		}
		storedAt, err := parseCanonicalStoreTime(createdText)
		if _, ok := expected[root]; !ok || err != nil || !storedAt.Equal(createdAt) {
			rows.Close()
			return fmt.Errorf("%w: unexpected or noncanonical Handling Artifact pin",
				ErrDerivationDispositionConflict)
		}
		present[root] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("derivation disposition: iterate Handling Artifact pins: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("derivation disposition: close Handling Artifact pins: %w", err)
	}
	for _, root := range roots {
		if _, ok := present[root]; ok {
			continue
		}
		if err := insertArtifactOwnerPin(ctx, tx, root, "handling", handling.String(), createdAt); err != nil {
			return fmt.Errorf("derivation disposition: pin child result Artifact: %w", err)
		}
	}
	return nil
}

func terminalEventType(state model.WorkState) model.EventType {
	switch state {
	case model.WorkClosed:
		return model.EventReviewClosed
	case model.WorkDeclined:
		return model.EventReviewDeclined
	case model.WorkExpired:
		return model.EventReviewExpired
	case model.WorkCancelled:
		return model.EventReviewCancelled
	default:
		return ""
	}
}

func deterministicDerivationHandlingID(operation model.OperationID) (model.HandlingID, error) {
	digest := sha256.Sum256([]byte(derivationHandlingDomain + operation.String()))
	id, err := model.ParseHandlingID("handling-derivation-" + hex.EncodeToString(digest[:]))
	if err != nil {
		return model.HandlingID{}, fmt.Errorf("derivation disposition: deterministic Handling ID: %w", err)
	}
	return id, nil
}

func validateExistingDerivationHandling(handling model.Handling, event model.EventID) (bool, error) {
	if handling.ProfileID() != model.TeamworkProfileID() || handling.EventID() != event {
		return false, fmt.Errorf("%w: deterministic Handling identity or source Event changed",
			ErrDerivationDispositionConflict)
	}
	if handling.LastDisposition() == parentStaleDisposition {
		if handling.Status() != model.HandlingCompleted || handling.Attempts() != 0 {
			return false, fmt.Errorf("%w: parent_stale evidence is wakeable or attempted",
				ErrDerivationDispositionConflict)
		}
		return true, nil
	}
	// Before its first claim, a resume is exactly a pending, undisposed
	// Handling. Once claimed, normal Handling lifecycle may make it pending or
	// terminal again; a positive attempt distinguishes that durable progression
	// from forged zero-attempt evidence.
	if handling.Attempts() == 0 &&
		(handling.Status() != model.HandlingPending || handling.LastDisposition() != "") {
		return false, fmt.Errorf("%w: zero-attempt resume Handling is not canonical",
			ErrDerivationDispositionConflict)
	}
	return false, nil
}
