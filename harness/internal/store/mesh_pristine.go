package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	// ErrMeshPristineAuthority is the stable endpoint-bootstrap read category.
	ErrMeshPristineAuthority = errors.New("durable mesh-pristine authority is unavailable")
	// ErrMeshNotPristine means durable mesh evidence already exists.
	ErrMeshNotPristine = errors.New("durable mesh authority is not pristine")
)

// MeshPristineAuthority proves that one SQLite snapshot contains an initialized
// Node/Profile pair and no durable mesh authority. The lifecycle owner must
// still hold ensure.lock through subsequent filesystem publication.
type MeshPristineAuthority struct {
	node    model.Node
	profile model.Profile
}

func (authority MeshPristineAuthority) Node() model.Node       { return authority.node }
func (authority MeshPristineAuthority) Profile() model.Profile { return authority.profile }

// ReadMeshPristineAuthority verifies the complete bootstrap preimage in one
// read-only transaction; an empty ReadChannelMeshAuthority alone is not proof.
func (s *Store) ReadMeshPristineAuthority(ctx context.Context) (MeshPristineAuthority, error) {
	if s == nil || s.db == nil || ctx == nil {
		return MeshPristineAuthority{}, fmt.Errorf("%w: Store is unavailable", ErrMeshPristineAuthority)
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return MeshPristineAuthority{}, meshPristineFailure("begin", err)
	}
	defer tx.Rollback()

	node, profile, err := readMeshPristineLocalAuthority(ctx, tx)
	if err != nil {
		return MeshPristineAuthority{}, err
	}
	if err := verifyMeshPristineForeignKeys(ctx, tx); err != nil {
		return MeshPristineAuthority{}, err
	}
	channelCount, err := verifyMeshPristineChannels(ctx, tx, node.PeerID())
	if err != nil {
		return MeshPristineAuthority{}, err
	}
	reservationCount, err := verifyMeshPristineReservations(ctx, tx, node)
	if err != nil {
		return MeshPristineAuthority{}, err
	}
	if err := verifyMeshPristinePressure(ctx, tx); err != nil {
		return MeshPristineAuthority{}, err
	}
	hasMeshAuthority, err := readMeshAuthorityPresence(ctx, tx)
	if err != nil {
		return MeshPristineAuthority{}, meshPristineFailure("inspect mesh rows", err)
	}
	if channelCount != 0 || reservationCount != 0 || hasMeshAuthority {
		return MeshPristineAuthority{}, fmt.Errorf("%w: %w",
			ErrMeshPristineAuthority, ErrMeshNotPristine)
	}
	if err := tx.Commit(); err != nil {
		return MeshPristineAuthority{}, meshPristineFailure("commit read", err)
	}
	return MeshPristineAuthority{node: node, profile: profile}, nil
}

func readMeshPristineLocalAuthority(ctx context.Context, tx *sql.Tx) (model.Node, model.Profile, error) {
	node, err := readNode(ctx, tx)
	if err != nil {
		return model.Node{}, model.Profile{}, meshPristineLocalReadFailure(ctx, "read Node", err)
	}
	profile, err := readProfile(ctx, tx)
	if err != nil {
		return model.Node{}, model.Profile{}, meshPristineLocalReadFailure(ctx, "read Profile", err)
	}
	if node.ActiveAssetRevision() != profile.ActiveAssetRevision() {
		return model.Node{}, model.Profile{}, meshPristineInvariant(
			"Node and Profile asset revisions differ", nil)
	}
	if node.NextOriginSequence() != 1 {
		return model.Node{}, model.Profile{}, meshPristineInvariant(
			"Node publication sequence is not initial", nil)
	}
	return node, profile, nil
}

func verifyMeshPristineForeignKeys(ctx context.Context, tx *sql.Tx) error {
	var violations int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_foreign_key_check").Scan(&violations); err != nil {
		return meshPristineFailure("inspect foreign keys", err)
	}
	if violations != 0 {
		return meshPristineInvariant("mesh foreign-key projection is inconsistent", nil)
	}
	return nil
}

func verifyMeshPristineChannels(ctx context.Context, tx *sql.Tx,
	localPeer model.PeerID,
) (int, error) {
	rows, err := tx.QueryContext(ctx, "SELECT channel_id FROM channels ORDER BY channel_id")
	if err != nil {
		return 0, meshPristineFailure("list Channels", err)
	}
	defer rows.Close()
	var channelIDs []model.ChannelID
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return 0, meshPristineValidationFailure(ctx, "scan Channel ID", err)
		}
		channelID, err := model.ParseChannelID(raw)
		if err != nil || channelID.String() != raw {
			return 0, meshPristineInvariant("Channel ID projection is invalid", err)
		}
		channelIDs = append(channelIDs, channelID)
	}
	if err := rows.Err(); err != nil {
		return 0, meshPristineFailure("iterate Channels", err)
	}
	if err := rows.Close(); err != nil {
		return 0, meshPristineFailure("close Channels", err)
	}
	for _, channelID := range channelIDs {
		if _, err := readVerifiedChannelAuthority(ctx, tx, localPeer, channelID); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return 0, meshPristineFailure("verify Channel", ctxErr)
			}
			return 0, fmt.Errorf("%w: Channel %q: %w",
				ErrMeshPristineAuthority, channelID.String(), err)
		}
	}
	return len(channelIDs), nil
}

func verifyMeshPristineReservations(ctx context.Context, tx *sql.Tx, node model.Node) (int, error) {
	rows, err := tx.QueryContext(ctx, `SELECT request_id,channel_id,grant_id,join_identity_digest,
		descriptor_digest,local_peer_id,origin_epoch,local_alias,attempt,state,created_at,updated_at
		FROM channel_join_reservations ORDER BY request_id`)
	if err != nil {
		return 0, meshPristineFailure("read join reservations", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
		var requestText, channelText, grantText, peerText, epochText, alias, state string
		var createdText, updatedText string
		var joinDigestRaw, descriptorDigestRaw []byte
		var attempt uint64
		if err := rows.Scan(&requestText, &channelText, &grantText, &joinDigestRaw,
			&descriptorDigestRaw, &peerText, &epochText, &alias, &attempt, &state,
			&createdText, &updatedText); err != nil {
			return 0, meshPristineValidationFailure(ctx, "scan join reservation", err)
		}
		if !validMeshPristineReservation(node, requestText, channelText, grantText,
			joinDigestRaw, descriptorDigestRaw, peerText, epochText, alias, attempt,
			state, createdText, updatedText) {
			return 0, meshPristineInvariant("join reservation projection is invalid", nil)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, meshPristineFailure("iterate join reservations", err)
	}
	if err := rows.Close(); err != nil {
		return 0, meshPristineFailure("close join reservations", err)
	}
	var conflicts int
	err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM channel_join_reservations reservation
		WHERE EXISTS (SELECT 1 FROM channels channel_state
			WHERE channel_state.channel_id=reservation.channel_id
				OR channel_state.local_alias=reservation.local_alias)`).Scan(&conflicts)
	if err != nil {
		return 0, meshPristineFailure("inspect join reservation scope", err)
	}
	if conflicts != 0 {
		return 0, meshPristineInvariant("join reservation conflicts with installed Channel", nil)
	}
	return count, nil
}

func validMeshPristineReservation(node model.Node, requestText, channelText, grantText string,
	joinDigestRaw, descriptorDigestRaw []byte, peerText, epochText, alias string,
	attempt uint64, state, createdText, updatedText string,
) bool {
	parsed, ok := parseMeshPristineReservation(requestText, channelText, grantText,
		joinDigestRaw, descriptorDigestRaw, peerText, epochText, alias, createdText, updatedText)
	if !ok {
		return false
	}
	return validMeshPristineReservationIdentity(node, parsed, requestText, channelText,
		grantText, peerText, epochText, alias) && validMeshPristineReservationState(
		parsed, attempt, state)
}

type parsedMeshPristineReservation struct {
	requestID        model.EnrollmentRequestID
	channelID        model.ChannelID
	grantID          model.GrantID
	descriptorDigest model.Digest
	peerID           model.PeerID
	epoch            model.OriginEpoch
	alias            model.ChannelID
	createdAt        time.Time
	updatedAt        time.Time
	derivedRequest   model.EnrollmentRequestID
}

func parseMeshPristineReservation(requestText, channelText, grantText string,
	joinDigestRaw, descriptorDigestRaw []byte, peerText, epochText, alias,
	createdText, updatedText string,
) (parsedMeshPristineReservation, bool) {
	var result parsedMeshPristineReservation
	var err error
	if result.requestID, err = model.ParseEnrollmentRequestID(requestText); err != nil {
		return parsedMeshPristineReservation{}, false
	}
	if result.channelID, err = model.ParseChannelID(channelText); err != nil {
		return parsedMeshPristineReservation{}, false
	}
	if result.grantID, err = model.ParseGrantID(grantText); err != nil {
		return parsedMeshPristineReservation{}, false
	}
	joinDigest, err := model.DigestFromBytes(joinDigestRaw)
	if err != nil {
		return parsedMeshPristineReservation{}, false
	}
	if result.descriptorDigest, err = model.DigestFromBytes(descriptorDigestRaw); err != nil {
		return parsedMeshPristineReservation{}, false
	}
	if result.peerID, err = model.ParsePeerID(peerText); err != nil {
		return parsedMeshPristineReservation{}, false
	}
	if result.epoch, err = model.ParseOriginEpoch(epochText); err != nil {
		return parsedMeshPristineReservation{}, false
	}
	if result.alias, err = model.ParseChannelID(alias); err != nil {
		return parsedMeshPristineReservation{}, false
	}
	if result.createdAt, err = parseCanonicalStoreTime(createdText); err != nil {
		return parsedMeshPristineReservation{}, false
	}
	if result.updatedAt, err = parseCanonicalStoreTime(updatedText); err != nil {
		return parsedMeshPristineReservation{}, false
	}
	if result.derivedRequest, err = model.EnrollmentRequestIDForJoinIdentity(joinDigest); err != nil {
		return parsedMeshPristineReservation{}, false
	}
	return result, true
}

func validMeshPristineReservationIdentity(node model.Node, parsed parsedMeshPristineReservation,
	requestText, channelText, grantText, peerText, epochText, alias string,
) bool {
	return parsed.requestID == parsed.derivedRequest && requestText == parsed.requestID.String() &&
		channelText == parsed.channelID.String() && grantText == parsed.grantID.String() &&
		parsed.peerID == node.PeerID() && peerText == node.PeerID().String() &&
		parsed.epoch == node.OriginEpoch() && epochText == node.OriginEpoch().String() &&
		alias == parsed.alias.String()
}

func validMeshPristineReservationState(parsed parsedMeshPristineReservation,
	attempt uint64, state string,
) bool {
	validState := state == "reserved" || state == "commit_unknown"
	return !parsed.descriptorDigest.IsZero() && attempt > 0 && attempt <= model.MaxSQLiteInteger &&
		validState && !parsed.updatedAt.Before(parsed.createdAt)
}

func verifyMeshPristinePressure(ctx context.Context, tx *sql.Tx) error {
	var nodeRows, nodeBytes, expectedBytes, mismatchedChannels, missingChannels int64
	err := tx.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM peer_inbox_node_pressure),
		COALESCE((SELECT pending_bytes FROM peer_inbox_node_pressure WHERE singleton_id=1),-1),
		COALESCE((SELECT SUM(length(publication_json)+length(required_artifact_roots_json))
			FROM peer_inbox WHERE status IN ('stored','waiting_artifact','ready','processing','retry')),0),
		(SELECT COUNT(*) FROM peer_inbox_pressure pressure
			WHERE pressure.pending_bytes <> COALESCE((SELECT SUM(length(inbox.publication_json)+
				length(inbox.required_artifact_roots_json)) FROM peer_inbox inbox
				WHERE inbox.channel_id=pressure.channel_id AND inbox.status IN
				('stored','waiting_artifact','ready','processing','retry')),0)),
		(SELECT COUNT(*) FROM (SELECT channel_id FROM peer_inbox WHERE status IN
			('stored','waiting_artifact','ready','processing','retry') GROUP BY channel_id) pending
			WHERE NOT EXISTS (SELECT 1 FROM peer_inbox_pressure pressure
				WHERE pressure.channel_id=pending.channel_id))`).Scan(&nodeRows, &nodeBytes,
		&expectedBytes, &mismatchedChannels, &missingChannels)
	if err != nil {
		return meshPristineFailure("inspect Inbox pressure", err)
	}
	if nodeRows != 1 || nodeBytes != expectedBytes || mismatchedChannels != 0 || missingChannels != 0 {
		return meshPristineInvariant("Inbox pressure projection is inconsistent", nil)
	}
	return nil
}

type meshPristineTableClass uint8

const (
	meshTableBootstrap meshPristineTableClass = iota + 1
	meshTableEvidence
	meshTableNonMesh
)

type meshPristineTable struct {
	name          string
	class         meshPristineTableClass
	meshPredicate string
}

// This registry classifies every application table. Mesh predicates are
// internal reviewed SQL constants, never caller input. Evidence presence is a
// reason to reject pristine bootstrap; it does not claim that the rows are a
// complete or valid authority projection.
var meshPristineTables = [...]meshPristineTable{
	{"node", meshTableBootstrap, ""}, {"profiles", meshTableBootstrap, ""},
	{"operations", meshTableNonMesh, ""}, {"operation_artifact_roots", meshTableNonMesh, ""},
	{"events", meshTableEvidence, ""}, {"works", meshTableNonMesh, ""},
	{"work_members", meshTableNonMesh, ""}, {"work_derivations", meshTableNonMesh, ""},
	{"agent_handlings", meshTableNonMesh, ""}, {"agent_runs", meshTableNonMesh, ""},
	{"artifact_roots", meshTableNonMesh, ""}, {"artifact_blocks", meshTableNonMesh, ""},
	{"artifact_root_blocks", meshTableNonMesh, ""},
	{"artifact_pins", meshTableEvidence, "owner_kind IN ('event','handling','publication','delivery','inbox')"},
	{"artifact_provenance", meshTableEvidence, ""}, {"artifact_gc_scan", meshTableNonMesh, ""},
	{"artifact_gc_staging_scan", meshTableNonMesh, ""}, {"artifact_gc_staging_receipt", meshTableNonMesh, ""},
	{"artifact_gc_queue", meshTableNonMesh, ""}, {"artifact_gc_prepare_receipt", meshTableNonMesh, ""},
	{"artifact_gc_completion_receipts", meshTableNonMesh, ""}, {"artifact_gc_delete_guard", meshTableNonMesh, ""},
	{"artifact_gc_block_delete_guard", meshTableNonMesh, ""}, {"artifact_gc_completion_guard", meshTableNonMesh, ""},
	{"channels", meshTableEvidence, ""}, {"channel_members", meshTableEvidence, ""},
	{"channel_conflicts", meshTableEvidence, ""}, {"enrollment_grants", meshTableEvidence, ""},
	{"enrollment_grant_uses", meshTableEvidence, ""}, {"enrollment_receipts", meshTableEvidence, ""},
	{"channel_join_reservations", meshTableEvidence, ""}, {"channel_leave_requests", meshTableEvidence, ""},
	{"peer_bindings", meshTableEvidence, ""}, {"gossip_publications", meshTableEvidence, ""},
	{"peer_deliveries", meshTableEvidence, ""}, {"peer_inbox", meshTableEvidence, ""},
	{"peer_inbox_semantic_transition_receipts", meshTableEvidence, ""},
	{"peer_inbox_artifact_renew_receipts", meshTableEvidence, ""},
	{"peer_inbox_pressure", meshTableEvidence, ""}, {"peer_inbox_node_pressure", meshTableBootstrap, ""},
	{"publication_conflicts", meshTableEvidence, ""}, {"origin_quarantines", meshTableEvidence, ""},
	{"peer_cursors", meshTableEvidence, ""}, {"peer_repairs", meshTableEvidence, ""},
	{"publication_epochs", meshTableEvidence, ""}, {"peer_pull_acks", meshTableEvidence, ""},
}

func readMeshAuthorityPresence(ctx context.Context, tx *sql.Tx) (bool, error) {
	checks := make([]string, 0, len(meshPristineTables))
	for _, table := range meshPristineTables {
		if table.class != meshTableEvidence {
			continue
		}
		check := "EXISTS(SELECT 1 FROM " + table.name
		if table.meshPredicate != "" {
			check += " WHERE " + table.meshPredicate
		}
		checks = append(checks, check+")")
	}
	query := "SELECT CASE WHEN " + strings.Join(checks, " OR ") + " THEN 1 ELSE 0 END"
	var present int
	err := tx.QueryRowContext(ctx, query).Scan(&present)
	return present == 1, err
}

func meshPristineInvariant(reason string, cause error) error {
	if cause != nil {
		return fmt.Errorf("%w: %w: %s: %w", ErrMeshPristineAuthority,
			ErrChannelAuthorityInvariant, reason, cause)
	}
	return fmt.Errorf("%w: %w: %s", ErrMeshPristineAuthority,
		ErrChannelAuthorityInvariant, reason)
}

func meshPristineFailure(reason string, cause error) error {
	return fmt.Errorf("%w: %s: %w", ErrMeshPristineAuthority, reason, cause)
}

func meshPristineValidationFailure(ctx context.Context, reason string, cause error) error {
	if err := ctx.Err(); err != nil {
		return meshPristineFailure(reason, err)
	}
	return meshPristineInvariant(reason, cause)
}

func meshPristineLocalReadFailure(ctx context.Context, reason string, cause error) error {
	if err := ctx.Err(); err != nil {
		return meshPristineFailure(reason, err)
	}
	structural := errors.Is(cause, sql.ErrNoRows) || errors.Is(cause, model.ErrInvalid) ||
		errors.Is(cause, model.ErrInvariant) || errors.Is(cause, model.ErrLimit) ||
		strings.HasPrefix(cause.Error(), "read node:") || strings.HasPrefix(cause.Error(), "read profile:")
	if structural {
		return meshPristineInvariant(reason, cause)
	}
	return meshPristineFailure(reason, cause)
}
