package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrArtifactConflict   = errors.New("artifact metadata conflicts with durable state")
	ErrArtifactUnverified = errors.New("artifact root is not verified")
	ErrArtifactReference  = errors.New("artifact Event reference is invalid")
)

// VerifiedArtifactRoot is the DB-side checkpoint for a closure already
// validated by the Artifact byte store. Stage 2 intentionally does not move
// or verify filesystem bytes.
type VerifiedArtifactRoot struct {
	RootDigest     model.Digest
	Manifest       model.JSON
	ManifestDigest model.Digest
	TotalBytes     uint64
	CreatedAt      time.Time
	VerifiedAt     time.Time
}

type ArtifactRootCheckpoint struct {
	Root     VerifiedArtifactRoot
	Replayed bool
}

// CheckpointVerifiedArtifactRoot durably records verified immutable metadata.
// Replays return the original timestamps; a root can never be rebound to a
// different manifest digest, canonical manifest, or size.
func (s *Store) CheckpointVerifiedArtifactRoot(ctx context.Context,
	requested VerifiedArtifactRoot,
) (ArtifactRootCheckpoint, error) {
	if s == nil || s.db == nil || ctx == nil {
		return ArtifactRootCheckpoint{}, errors.New("checkpoint Artifact root: nil store or context")
	}
	root, err := validateVerifiedArtifactRoot(requested)
	if err != nil {
		return ArtifactRootCheckpoint{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ArtifactRootCheckpoint{}, fmt.Errorf("checkpoint Artifact root: begin: %w", err)
	}
	defer tx.Rollback()
	if err := requireArtifactGCDigestNotQueued(ctx, tx, root.ManifestDigest); err != nil {
		return ArtifactRootCheckpoint{}, err
	}
	if err := requireArtifactGCQueueAvailableForRoot(ctx, tx, root.RootDigest); err != nil {
		return ArtifactRootCheckpoint{}, err
	}

	existing, state, err := readArtifactRoot(ctx, tx, root.RootDigest)
	if err == nil {
		if !sameArtifactContent(existing, root) {
			return ArtifactRootCheckpoint{}, ErrArtifactConflict
		}
		if state == "verified" {
			if err := tx.Commit(); err != nil {
				return ArtifactRootCheckpoint{}, fmt.Errorf("checkpoint Artifact root: replay commit: %w", err)
			}
			return ArtifactRootCheckpoint{Root: existing, Replayed: true}, nil
		}
		if state != "staged" {
			return ArtifactRootCheckpoint{}, ErrArtifactConflict
		}
		if root.VerifiedAt.Before(existing.CreatedAt) {
			return ArtifactRootCheckpoint{}, ErrArtifactConflict
		}
		if _, err := tx.ExecContext(ctx,
			"UPDATE artifact_roots SET state = 'verified', verified_at = ? WHERE root_digest = ? AND state = 'staged'",
			storeTime(root.VerifiedAt), root.RootDigest.String()); err != nil {
			return ArtifactRootCheckpoint{}, fmt.Errorf("checkpoint Artifact root: promote: %w", err)
		}
		root.CreatedAt = existing.CreatedAt
	} else if errors.Is(err, sql.ErrNoRows) {
		_, err = tx.ExecContext(ctx, `INSERT INTO artifact_roots(
			root_digest, manifest_json, manifest_digest, total_bytes, state, created_at, verified_at
		) VALUES(?, ?, ?, ?, 'verified', ?, ?)`, root.RootDigest.String(), root.Manifest.Bytes(),
			root.ManifestDigest.Bytes(), root.TotalBytes, storeTime(root.CreatedAt), storeTime(root.VerifiedAt))
		if err != nil {
			return ArtifactRootCheckpoint{}, fmt.Errorf("checkpoint Artifact root: insert: %w", err)
		}
	} else {
		return ArtifactRootCheckpoint{}, fmt.Errorf("checkpoint Artifact root: inspect: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return ArtifactRootCheckpoint{}, fmt.Errorf("checkpoint Artifact root: commit: %w", err)
	}
	return ArtifactRootCheckpoint{Root: root}, nil
}

// GetVerifiedArtifactRoot returns only closure metadata ready for pinning or
// provenance. A staged row is never projected as usable Artifact state.
func (s *Store) GetVerifiedArtifactRoot(ctx context.Context, root model.Digest) (VerifiedArtifactRoot, error) {
	if s == nil || s.db == nil || ctx == nil || root.IsZero() {
		return VerifiedArtifactRoot{}, errors.New("get verified Artifact root: incomplete input")
	}
	value, err := requireVerifiedArtifactRoot(ctx, s.db, root)
	if err != nil {
		return VerifiedArtifactRoot{}, fmt.Errorf("get verified Artifact root: %w", err)
	}
	return value, nil
}

func validateVerifiedArtifactRoot(root VerifiedArtifactRoot) (VerifiedArtifactRoot, error) {
	if root.RootDigest.IsZero() || root.Manifest.IsZero() || root.ManifestDigest.IsZero() {
		return VerifiedArtifactRoot{}, errors.New("checkpoint Artifact root: incomplete digest or manifest")
	}
	manifest := root.Manifest.Bytes()
	if len(manifest) == 0 || manifest[0] != '{' || model.Sum(manifest) != root.ManifestDigest {
		return VerifiedArtifactRoot{}, errors.New("checkpoint Artifact root: manifest digest mismatch")
	}
	if root.TotalBytes > model.MaxSQLiteInteger {
		return VerifiedArtifactRoot{}, errors.New("checkpoint Artifact root: total bytes exceed SQLite integer")
	}
	createdAt, err := canonicalStoreTime(root.CreatedAt)
	if err != nil {
		return VerifiedArtifactRoot{}, fmt.Errorf("checkpoint Artifact root: invalid created time: %w", err)
	}
	root.CreatedAt = createdAt
	verifiedAt, err := canonicalStoreTime(root.VerifiedAt)
	if err != nil {
		return VerifiedArtifactRoot{}, fmt.Errorf("checkpoint Artifact root: invalid verified time: %w", err)
	}
	root.VerifiedAt = verifiedAt
	if root.VerifiedAt.Before(root.CreatedAt) {
		return VerifiedArtifactRoot{}, errors.New("checkpoint Artifact root: invalid verification times")
	}
	return root, nil
}

func readArtifactRoot(ctx context.Context, q rowQuerier,
	root model.Digest,
) (VerifiedArtifactRoot, string, error) {
	var rootText, state, createdText string
	var manifest, manifestDigest []byte
	var totalBytes int64
	var verifiedText sql.NullString
	err := q.QueryRowContext(ctx, `SELECT root_digest, manifest_json, manifest_digest, total_bytes,
		state, created_at, verified_at FROM artifact_roots WHERE root_digest = ?`, root.String()).
		Scan(&rootText, &manifest, &manifestDigest, &totalBytes, &state, &createdText, &verifiedText)
	if err != nil {
		return VerifiedArtifactRoot{}, "", err
	}
	rootDigest, err := model.ParseDigest(rootText)
	if err != nil {
		return VerifiedArtifactRoot{}, "", err
	}
	manifestJSON, err := model.NewJSON(manifest)
	if err != nil || !bytes.Equal(manifestJSON.Bytes(), manifest) {
		return VerifiedArtifactRoot{}, "", errors.New("stored Artifact manifest is not canonical JSON")
	}
	digest, err := model.DigestFromBytes(manifestDigest)
	if err != nil || model.Sum(manifest) != digest || totalBytes < 0 {
		return VerifiedArtifactRoot{}, "", errors.New("stored Artifact metadata is corrupt")
	}
	created, err := parseCanonicalStoreTime(createdText)
	if err != nil {
		return VerifiedArtifactRoot{}, "", err
	}
	result := VerifiedArtifactRoot{rootDigest, manifestJSON, digest, uint64(totalBytes), created, time.Time{}}
	if verifiedText.Valid {
		result.VerifiedAt, err = parseCanonicalStoreTime(verifiedText.String)
		if err != nil {
			return VerifiedArtifactRoot{}, "", err
		}
	}
	if (state == "verified") != verifiedText.Valid ||
		(verifiedText.Valid && result.VerifiedAt.Before(result.CreatedAt)) {
		return VerifiedArtifactRoot{}, "", errors.New("stored Artifact verification state is corrupt")
	}
	return result, state, nil
}

func requireVerifiedArtifactRoot(ctx context.Context, q rowQuerier,
	root model.Digest,
) (VerifiedArtifactRoot, error) {
	value, state, err := readArtifactRoot(ctx, q, root)
	if err != nil {
		return VerifiedArtifactRoot{}, fmt.Errorf("%w: %v", ErrArtifactUnverified, err)
	}
	if state != "verified" || value.VerifiedAt.IsZero() {
		return VerifiedArtifactRoot{}, ErrArtifactUnverified
	}
	return value, nil
}

func sameArtifactContent(left, right VerifiedArtifactRoot) bool {
	return left.RootDigest == right.RootDigest && left.Manifest.String() == right.Manifest.String() &&
		left.ManifestDigest == right.ManifestDigest && left.TotalBytes == right.TotalBytes
}

// requireReusableArtifactRoot is the referenced-role gate used before local
// acceptance: verified bytes alone are insufficient without prior provenance.
func requireReusableArtifactRoot(ctx context.Context, q rowQuerier, root model.Digest) error {
	if _, err := requireVerifiedArtifactRoot(ctx, q, root); err != nil {
		return err
	}
	var present int
	if err := q.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM artifact_provenance WHERE root_digest = ?)", root.String()).Scan(&present); err != nil {
		return fmt.Errorf("require reusable Artifact root: %w", err)
	}
	if present != 1 {
		return ErrArtifactReference
	}
	return nil
}

// insertEventArtifactPin binds retention to an already accepted Event and is
// intentionally transaction-scoped.
func insertEventArtifactPin(ctx context.Context, tx *sql.Tx, root model.Digest,
	event model.EventID, createdAt time.Time,
) (bool, error) {
	if ctx == nil || tx == nil || root.IsZero() || event.IsZero() || createdAt.IsZero() {
		return false, errors.New("insert Event Artifact pin: incomplete input")
	}
	if _, err := requireVerifiedArtifactRoot(ctx, tx, root); err != nil {
		return false, err
	}
	if err := requireArtifactGCQueueAvailableForRoot(ctx, tx, root); err != nil {
		return false, err
	}
	if _, _, _, _, _, err := readEventArtifactRole(ctx, tx, event, root); err != nil {
		return false, err
	}
	createdAt = createdAt.Round(0).UTC()
	var stored string
	err := tx.QueryRowContext(ctx, `SELECT created_at FROM artifact_pins
		WHERE root_digest = ? AND owner_kind = 'event' AND owner_id = ?`, root.String(), event.String()).Scan(&stored)
	if err == nil {
		if _, err := parseCanonicalStoreTime(stored); err != nil {
			return false, err
		}
		return true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("insert Event Artifact pin: inspect replay: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO artifact_pins(root_digest, owner_kind, owner_id, created_at)
		VALUES(?, 'event', ?, ?)`, root.String(), event.String(), storeTime(createdAt))
	if err != nil {
		return false, fmt.Errorf("insert Event Artifact pin: %w", err)
	}
	return false, nil
}

// insertArtifactProvenance records only a produced Event root. referenced roots
// use requireReusableArtifactRoot plus an Event pin and never acquire a new
// producer identity.
func insertArtifactProvenance(ctx context.Context, tx *sql.Tx,
	provenance model.ArtifactProvenance,
) (bool, error) {
	if ctx == nil || tx == nil || provenance.RootDigest().IsZero() || !provenance.Relation().Valid() {
		return false, errors.New("insert Artifact provenance: incomplete input")
	}
	verifiedRoot, err := requireVerifiedArtifactRoot(ctx, tx, provenance.RootDigest())
	if err != nil {
		return false, err
	}
	if err := requireArtifactGCQueueAvailableForRoot(ctx, tx, provenance.RootDigest()); err != nil {
		return false, err
	}
	role, source, origin, epoch, actor, err := readEventArtifactRole(ctx, tx,
		provenance.ProducerEvent().EventID(), provenance.RootDigest())
	if err != nil {
		return false, err
	}
	if role != model.ArtifactProduced || origin != provenance.ProducerOriginPeerID().String() ||
		epoch != provenance.ProducerEvent().OriginEpoch().String() {
		return false, ErrArtifactReference
	}
	runID, hasRun := provenance.LocalAgentRunID()
	operationID, hasOperation := provenance.OperationID()
	if provenance.Relation() == model.ProvenanceLocalCapture {
		if source != string(model.EventSourceLocal) || !hasRun || !hasOperation {
			return false, ErrArtifactReference
		}
		var runProfile, operationProfile, operationStatus, principal string
		var capture []byte
		err = tx.QueryRowContext(ctx, `SELECT r.profile_id, o.profile_id, o.status,
			o.capture_json, p.principal
			FROM agent_runs r JOIN operations o ON o.operation_id = ? AND o.agent_run_id = r.run_id
			JOIN profiles p ON p.profile_id = r.profile_id WHERE r.run_id = ?`,
			operationID.String(), runID.String()).Scan(&runProfile, &operationProfile, &operationStatus,
			&capture, &principal)
		captureJSON, captureErr := model.NewJSON(capture)
		captureRoots, rootsErr := parseOperationCapture(captureJSON)
		captureMatches := false
		for _, captured := range captureRoots {
			if captured.RootDigest == provenance.RootDigest() && captured.ManifestDigest == verifiedRoot.ManifestDigest {
				captureMatches = true
				break
			}
		}
		if err != nil || runProfile != model.TeamworkProfileID().String() || operationProfile != runProfile ||
			operationStatus != "committed" || captureErr != nil || !bytes.Equal(captureJSON.Bytes(), capture) ||
			rootsErr != nil || !captureMatches || actor != principal {
			return false, fmt.Errorf("%w: local Run/operation/Profile mismatch", ErrArtifactReference)
		}
	} else if source != string(model.EventSourceImported) || hasRun || hasOperation {
		return false, ErrArtifactReference
	}

	var storedOrigin, storedRelation, storedCreated string
	var storedRun, storedOperation sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT producer_origin_peer_id, local_agent_run_id, operation_id,
		relation, created_at FROM artifact_provenance WHERE root_digest = ? AND producer_event_id = ?`,
		provenance.RootDigest().String(), provenance.ProducerEvent().EventID().String()).
		Scan(&storedOrigin, &storedRun, &storedOperation, &storedRelation, &storedCreated)
	if err == nil {
		if storedOrigin != origin || storedRun.String != runID.String() || storedRun.Valid != hasRun ||
			storedOperation.String != operationID.String() || storedOperation.Valid != hasOperation ||
			storedRelation != string(provenance.Relation()) {
			return false, ErrArtifactConflict
		}
		if _, err := parseCanonicalStoreTime(storedCreated); err != nil {
			return false, err
		}
		return true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("insert Artifact provenance: inspect replay: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO artifact_provenance(root_digest, producer_event_id,
		producer_origin_peer_id, local_agent_run_id, operation_id, relation, created_at)
		VALUES(?, ?, ?, ?, ?, ?, ?)`, provenance.RootDigest().String(),
		provenance.ProducerEvent().EventID().String(), origin, nullableID(runID, hasRun),
		nullableID(operationID, hasOperation), string(provenance.Relation()), storeTime(provenance.CreatedAt()))
	if err != nil {
		return false, fmt.Errorf("insert Artifact provenance: %w", err)
	}
	return false, nil
}

func readEventArtifactRole(ctx context.Context, q rowQuerier, event model.EventID,
	root model.Digest,
) (role model.ArtifactRole, source, origin, epoch, actor string, err error) {
	var raw []byte
	err = q.QueryRowContext(ctx, `SELECT source, origin_peer_id, origin_epoch, actor_principal,
		artifact_roots_json FROM events WHERE event_id = ?`, event.String()).
		Scan(&source, &origin, &epoch, &actor, &raw)
	if err != nil {
		return "", "", "", "", "", fmt.Errorf("Artifact producer Event: %w", err)
	}
	canonical, err := model.NewJSON(raw)
	if err != nil || !bytes.Equal(canonical.Bytes(), raw) {
		return "", "", "", "", "", ErrArtifactReference
	}
	var refs []struct {
		RootDigest string             `json:"root_digest"`
		Role       model.ArtifactRole `json:"role"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&refs); err != nil {
		return "", "", "", "", "", ErrArtifactReference
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return "", "", "", "", "", ErrArtifactReference
	}
	if len(refs) > model.MaxArtifactRefs {
		return "", "", "", "", "", ErrArtifactReference
	}
	found := false
	var foundRole model.ArtifactRole
	for _, ref := range refs {
		digest, parseErr := model.ParseDigest(ref.RootDigest)
		if parseErr != nil || !ref.Role.Valid() {
			return "", "", "", "", "", ErrArtifactReference
		}
		if digest == root {
			if found {
				return "", "", "", "", "", ErrArtifactReference
			}
			found, foundRole = true, ref.Role
		}
	}
	if found {
		return foundRole, source, origin, epoch, actor, nil
	}
	return "", "", "", "", "", ErrArtifactReference
}

func nullableID(value interface{ String() string }, present bool) any {
	if !present {
		return nil
	}
	return value.String()
}
