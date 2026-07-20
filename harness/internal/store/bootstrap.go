package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var ErrInitializationConflict = errors.New("node initialization conflicts with durable state")

var errStoredBootstrapStructure = errors.New("stored Node/Profile structure is invalid")

// NodeInitializationState is the closed bootstrap classification observed from
// one Store snapshot. The zero value is deliberately invalid so callers must
// handle every durable state explicitly.
type NodeInitializationState uint8

const (
	NodeInitializationFresh NodeInitializationState = iota + 1
	NodeInitializationExisting
)

// Fixed-width UTC keeps SQLite TEXT ordering equal to chronological ordering
// for leases and ready queues while remaining valid RFC3339 nanosecond time.
const storeTimeLayout = "2006-01-02T15:04:05.000000000Z"

// InitializationResult is the durable, inactive T0 identity. Created is true
// only when InitializeNode inserted the Node/Profile pair.
type InitializationResult struct {
	Created bool
	Node    model.Node
	Profile model.Profile
}

// ClassifyNodeInitialization decides whether Provision may create projections
// or must use strict existing-only reads. Only an entirely absent Node/Profile
// pair is fresh. A partial pair, malformed row, or inconsistent pair is a
// durable conflict and must never fall through to an Ensure path.
func (s *Store) ClassifyNodeInitialization(ctx context.Context) (NodeInitializationState, error) {
	if s == nil || s.db == nil {
		return 0, errors.New("classify node initialization: nil store")
	}
	if ctx == nil {
		return 0, errors.New("classify node initialization: nil context")
	}
	if cause := context.Cause(ctx); cause != nil {
		return 0, fmt.Errorf("classify node initialization: %w", errors.Join(ctx.Err(), cause))
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return 0, initializationClassificationFailure(ctx, "begin", err)
	}
	defer tx.Rollback()

	nodeRows, profileRows, err := readInitializationCardinality(ctx, tx)
	if err != nil {
		return 0, initializationClassificationFailure(ctx, "count Node/Profile rows", err)
	}
	if cause := context.Cause(ctx); cause != nil {
		return 0, fmt.Errorf("classify node initialization: %w", errors.Join(ctx.Err(), cause))
	}
	switch {
	case nodeRows == 0 && profileRows == 0:
		if err := tx.Commit(); err != nil {
			return 0, initializationClassificationFailure(ctx, "commit fresh read", err)
		}
		if cause := context.Cause(ctx); cause != nil {
			return 0, fmt.Errorf("classify node initialization: %w", errors.Join(ctx.Err(), cause))
		}
		return NodeInitializationFresh, nil
	case nodeRows != 1 || profileRows != 1:
		return 0, fmt.Errorf("%w: Node/Profile row cardinality is %d/%d",
			ErrInitializationConflict, nodeRows, profileRows)
	}

	node, err := readNode(ctx, tx)
	if err != nil {
		return 0, classifyInitializationReadFailure(ctx, "Node", err)
	}
	profile, err := readProfile(ctx, tx)
	if err != nil {
		return 0, classifyInitializationReadFailure(ctx, "Teamwork Profile", err)
	}
	if node.ActiveAssetRevision() != profile.ActiveAssetRevision() {
		return 0, fmt.Errorf("%w: durable Node/Profile authority is inconsistent", ErrInitializationConflict)
	}
	if err := tx.Commit(); err != nil {
		return 0, initializationClassificationFailure(ctx, "commit existing read", err)
	}
	if cause := context.Cause(ctx); cause != nil {
		return 0, fmt.Errorf("classify node initialization: %w", errors.Join(ctx.Err(), cause))
	}
	return NodeInitializationExisting, nil
}

func readInitializationCardinality(ctx context.Context, tx *sql.Tx) (int64, int64, error) {
	var nodeRows, profileRows int64
	err := tx.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM node),
		(SELECT COUNT(*) FROM profiles)`).Scan(&nodeRows, &profileRows)
	return nodeRows, profileRows, err
}

func classifyInitializationReadFailure(ctx context.Context, authority string, err error) error {
	if cause := context.Cause(ctx); cause != nil {
		return fmt.Errorf("classify node initialization: read %s: %w", authority,
			errors.Join(ctx.Err(), cause, err))
	}
	if errors.Is(err, sql.ErrNoRows) || errors.Is(err, errStoredBootstrapStructure) {
		return fmt.Errorf("%w: read %s: %w", ErrInitializationConflict, authority, err)
	}
	return fmt.Errorf("classify node initialization: read %s: %w", authority, err)
}

func initializationClassificationFailure(ctx context.Context, operation string, err error) error {
	if cause := context.Cause(ctx); cause != nil {
		return fmt.Errorf("classify node initialization: %s: %w", operation,
			errors.Join(ctx.Err(), cause, err))
	}
	return fmt.Errorf("classify node initialization: %s: %w", operation, err)
}

// InitializeNode persists the one Node/Profile identity before Host assets are
// projected or self-checked. Replays never change active authority: supplied
// Host, Runtime, asset and budget values are merely the caller's staging
// intent. ActivateProfile is the only path that can publish that intent.
func (s *Store) InitializeNode(ctx context.Context, node model.Node, profile model.Profile) (InitializationResult, error) {
	if s == nil || s.db == nil {
		return InitializationResult{}, errors.New("initialize node: nil store")
	}
	if ctx == nil {
		return InitializationResult{}, errors.New("initialize node: nil context")
	}
	if node.PeerID().IsZero() || profile.ID().IsZero() {
		return InitializationResult{}, errors.New("initialize node: incomplete model values")
	}
	if profile.Enabled() {
		return InitializationResult{}, fmt.Errorf("%w: initial Teamwork Profile must be disabled", ErrInitializationConflict)
	}
	if node.ActiveAssetRevision() != profile.ActiveAssetRevision() {
		return InitializationResult{}, fmt.Errorf("%w: intended Node/Profile asset revisions differ", ErrInitializationConflict)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return InitializationResult{}, fmt.Errorf("initialize node: begin: %w", err)
	}
	defer tx.Rollback()

	durableNode, nodeErr := readNode(ctx, tx)
	durableProfile, profileErr := readProfile(ctx, tx)
	switch {
	case errors.Is(nodeErr, sql.ErrNoRows) && errors.Is(profileErr, sql.ErrNoRows):
		if err := insertNodeRecord(ctx, tx, node); err != nil {
			return InitializationResult{}, err
		}
		if err := insertProfileRecord(ctx, tx, profile); err != nil {
			return InitializationResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return InitializationResult{}, fmt.Errorf("initialize node: commit: %w", err)
		}
		return InitializationResult{Created: true, Node: node, Profile: profile}, nil
	case nodeErr != nil && !errors.Is(nodeErr, sql.ErrNoRows):
		return InitializationResult{}, fmt.Errorf("initialize node: read identity: %w", nodeErr)
	case profileErr != nil && !errors.Is(profileErr, sql.ErrNoRows):
		return InitializationResult{}, fmt.Errorf("initialize node: read Profile: %w", profileErr)
	case errors.Is(nodeErr, sql.ErrNoRows) || errors.Is(profileErr, sql.ErrNoRows):
		return InitializationResult{}, fmt.Errorf("%w: partial Node/Profile identity", ErrInitializationConflict)
	}

	if !sameNodeIdentity(durableNode, node) {
		return InitializationResult{}, fmt.Errorf("%w: Node identity differs", ErrInitializationConflict)
	}
	if !sameProfileIdentity(durableProfile, profile) {
		return InitializationResult{}, fmt.Errorf("%w: Teamwork Profile identity differs", ErrInitializationConflict)
	}
	if durableNode.ActiveAssetRevision() != durableProfile.ActiveAssetRevision() {
		return InitializationResult{}, fmt.Errorf("%w: durable Node/Profile authority is inconsistent", ErrInitializationConflict)
	}
	return InitializationResult{Node: durableNode, Profile: durableProfile}, nil
}

type rowQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readNode(ctx context.Context, q rowQuerier) (model.Node, error) {
	var peerRaw, epochRaw, nextRaw, assetRaw, createdRaw, updatedRaw any
	err := q.QueryRowContext(ctx,
		"SELECT peer_id, origin_epoch, next_origin_seq, active_asset_rev, created_at, updated_at FROM node WHERE singleton = 1",
	).Scan(&peerRaw, &epochRaw, &nextRaw, &assetRaw, &createdRaw, &updatedRaw)
	if err != nil {
		return model.Node{}, err
	}
	peerText, err := storedBootstrapText("read node: peer ID storage", peerRaw)
	if err != nil {
		return model.Node{}, err
	}
	epochText, err := storedBootstrapText("read node: origin epoch storage", epochRaw)
	if err != nil {
		return model.Node{}, err
	}
	next, err := storedBootstrapInteger("read node: next sequence storage", nextRaw)
	if err != nil {
		return model.Node{}, err
	}
	if next < 0 {
		return model.Node{}, storedBootstrapError("read node: next sequence storage", errors.New("negative integer"))
	}
	asset, err := storedBootstrapText("read node: asset revision storage", assetRaw)
	if err != nil {
		return model.Node{}, err
	}
	createdText, err := storedBootstrapText("read node: created_at storage", createdRaw)
	if err != nil {
		return model.Node{}, err
	}
	updatedText, err := storedBootstrapText("read node: updated_at storage", updatedRaw)
	if err != nil {
		return model.Node{}, err
	}
	peer, err := model.ParsePeerID(peerText)
	if err != nil {
		return model.Node{}, storedBootstrapError("read node: peer ID", err)
	}
	epoch, err := model.ParseOriginEpoch(epochText)
	if err != nil {
		return model.Node{}, storedBootstrapError("read node: origin epoch", err)
	}
	created, err := parseCanonicalStoreTime(createdText)
	if err != nil {
		return model.Node{}, storedBootstrapError("read node: created_at", err)
	}
	updated, err := parseCanonicalStoreTime(updatedText)
	if err != nil {
		return model.Node{}, storedBootstrapError("read node: updated_at", err)
	}
	node, err := model.NewNode(model.NodeSpec{PeerID: peer, OriginEpoch: epoch,
		NextOriginSequence:  uint64(next),
		ActiveAssetRevision: asset, CreatedAt: created, UpdatedAt: updated})
	if err != nil {
		return model.Node{}, storedBootstrapError("read node: value", err)
	}
	return node, nil
}

func readProfile(ctx context.Context, q rowQuerier) (model.Profile, error) {
	values := make([]any, 11)
	destinations := make([]any, len(values))
	for index := range values {
		destinations[index] = &values[index]
	}
	err := q.QueryRowContext(ctx, "SELECT profile_id, principal, workspace_root, host, runtime_kind, credential_hash, active_asset_rev, handling_budget_json, enabled, created_at, updated_at FROM profiles WHERE profile_id = ?", model.TeamworkProfileID().String()).
		Scan(destinations...)
	if err != nil {
		return model.Profile{}, err
	}
	texts := make([]string, 8)
	for output, input := range []int{0, 1, 2, 3, 4, 6, 9, 10} {
		texts[output], err = storedBootstrapText("read profile: TEXT storage", values[input])
		if err != nil {
			return model.Profile{}, err
		}
	}
	idText, principal, workspace, hostText := texts[0], texts[1], texts[2], texts[3]
	runtimeText, asset, createdText, updatedText := texts[4], texts[5], texts[6], texts[7]
	credential, err := storedBootstrapBytes("read profile: credential storage", values[5])
	if err != nil {
		return model.Profile{}, err
	}
	budget, err := storedBootstrapBytes("read profile: handling budget storage", values[7])
	if err != nil {
		return model.Profile{}, err
	}
	enabled, err := storedBootstrapInteger("read profile: enabled storage", values[8])
	if err != nil {
		return model.Profile{}, err
	}
	id, err := model.ParseProfileID(idText)
	if err != nil {
		return model.Profile{}, storedBootstrapError("read profile: ID", err)
	}
	digest, err := model.DigestFromBytes(credential)
	if err != nil {
		return model.Profile{}, storedBootstrapError("read profile: credential", err)
	}
	budgetJSON, err := model.NewJSON(budget)
	if err != nil {
		return model.Profile{}, storedBootstrapError("read profile: handling budget", err)
	}
	if !bytes.Equal(budgetJSON.Bytes(), budget) {
		return model.Profile{}, storedBootstrapError("read profile: handling budget",
			errors.New("not canonical JSON"))
	}
	if enabled != 0 && enabled != 1 {
		return model.Profile{}, storedBootstrapError("read profile: enabled",
			errors.New("not a canonical boolean"))
	}
	created, err := parseCanonicalStoreTime(createdText)
	if err != nil {
		return model.Profile{}, storedBootstrapError("read profile: created_at", err)
	}
	updated, err := parseCanonicalStoreTime(updatedText)
	if err != nil {
		return model.Profile{}, storedBootstrapError("read profile: updated_at", err)
	}
	profile, err := model.NewProfile(model.ProfileSpec{ID: id, Principal: principal, WorkspaceRoot: workspace,
		Host: model.HostKind(hostText), Runtime: model.RuntimeKind(runtimeText), CredentialHash: digest,
		ActiveAssetRevision: asset, HandlingBudget: budgetJSON, Enabled: enabled == 1,
		CreatedAt: created, UpdatedAt: updated})
	if err != nil {
		return model.Profile{}, storedBootstrapError("read profile: value", err)
	}
	return profile, nil
}

func storedBootstrapError(operation string, cause error) error {
	return fmt.Errorf("%s: %w: %w", operation, errStoredBootstrapStructure, cause)
}

func storedBootstrapText(operation string, value any) (string, error) {
	text, ok := value.(string)
	if !ok {
		return "", storedBootstrapError(operation, errors.New("storage class is not TEXT"))
	}
	return text, nil
}

func storedBootstrapInteger(operation string, value any) (int64, error) {
	integer, ok := value.(int64)
	if !ok {
		return 0, storedBootstrapError(operation, errors.New("storage class is not INTEGER"))
	}
	return integer, nil
}

func storedBootstrapBytes(operation string, value any) ([]byte, error) {
	raw, ok := value.([]byte)
	if !ok {
		return nil, storedBootstrapError(operation, errors.New("storage class is not BLOB"))
	}
	return append([]byte(nil), raw...), nil
}

func insertNodeRecord(ctx context.Context, tx *sql.Tx, node model.Node) error {
	_, err := tx.ExecContext(ctx, "INSERT INTO node(singleton, peer_id, origin_epoch, next_origin_seq, active_asset_rev, created_at, updated_at) VALUES(1, ?, ?, ?, ?, ?, ?)",
		node.PeerID().String(), node.OriginEpoch().String(), node.NextOriginSequence(), node.ActiveAssetRevision(),
		storeTime(node.CreatedAt()), storeTime(node.UpdatedAt()))
	if err != nil {
		return fmt.Errorf("initialize node: insert identity: %w", err)
	}
	return nil
}

func insertProfileRecord(ctx context.Context, tx *sql.Tx, profile model.Profile) error {
	_, err := tx.ExecContext(ctx, "INSERT INTO profiles(profile_id, principal, workspace_root, host, runtime_kind, credential_hash, active_asset_rev, handling_budget_json, enabled, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		profile.ID().String(), profile.Principal(), profile.WorkspaceRoot(), string(profile.Host()), string(profile.Runtime()),
		profile.CredentialHash().Bytes(), profile.ActiveAssetRevision(), profile.HandlingBudget().Bytes(), boolInt(profile.Enabled()),
		storeTime(profile.CreatedAt()), storeTime(profile.UpdatedAt()))
	if err != nil {
		return fmt.Errorf("initialize node: insert Profile: %w", err)
	}
	return nil
}

func sameNodeIdentity(a, b model.Node) bool {
	return a.PeerID() == b.PeerID() && a.OriginEpoch() == b.OriginEpoch()
}

func sameProfileIdentity(a, b model.Profile) bool {
	return a.ID() == b.ID() && a.Principal() == b.Principal() && a.WorkspaceRoot() == b.WorkspaceRoot() &&
		a.CredentialHash() == b.CredentialHash()
}

func storeTime(value time.Time) string { return value.Round(0).UTC().Format(storeTimeLayout) }

func canonicalStoreTime(value time.Time) (time.Time, error) {
	canonical := value.Round(0).UTC()
	parsed, err := parseCanonicalStoreTime(storeTime(canonical))
	if err != nil || !parsed.Equal(canonical) {
		return time.Time{}, errors.New("time does not round-trip through canonical SQLite encoding")
	}
	return canonical, nil
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
