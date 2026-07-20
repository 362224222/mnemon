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
	var peerText, epochText, asset, createdText, updatedText string
	var next uint64
	err := q.QueryRowContext(ctx,
		"SELECT peer_id, origin_epoch, next_origin_seq, active_asset_rev, created_at, updated_at FROM node WHERE singleton = 1",
	).Scan(&peerText, &epochText, &next, &asset, &createdText, &updatedText)
	if err != nil {
		return model.Node{}, err
	}
	peer, err := model.ParsePeerID(peerText)
	if err != nil {
		return model.Node{}, fmt.Errorf("read node: peer ID: %w", err)
	}
	epoch, err := model.ParseOriginEpoch(epochText)
	if err != nil {
		return model.Node{}, fmt.Errorf("read node: origin epoch: %w", err)
	}
	created, err := parseCanonicalStoreTime(createdText)
	if err != nil {
		return model.Node{}, fmt.Errorf("read node: created_at: %w", err)
	}
	updated, err := parseCanonicalStoreTime(updatedText)
	if err != nil {
		return model.Node{}, fmt.Errorf("read node: updated_at: %w", err)
	}
	return model.NewNode(model.NodeSpec{PeerID: peer, OriginEpoch: epoch, NextOriginSequence: next,
		ActiveAssetRevision: asset, CreatedAt: created, UpdatedAt: updated})
}

func readProfile(ctx context.Context, q rowQuerier) (model.Profile, error) {
	var idText, principal, workspace, hostText, runtimeText, asset, createdText, updatedText string
	var credential, budget []byte
	var enabled int
	err := q.QueryRowContext(ctx, "SELECT profile_id, principal, workspace_root, host, runtime_kind, credential_hash, active_asset_rev, handling_budget_json, enabled, created_at, updated_at FROM profiles WHERE profile_id = ?", model.TeamworkProfileID().String()).
		Scan(&idText, &principal, &workspace, &hostText, &runtimeText, &credential, &asset, &budget, &enabled, &createdText, &updatedText)
	if err != nil {
		return model.Profile{}, err
	}
	id, err := model.ParseProfileID(idText)
	if err != nil {
		return model.Profile{}, fmt.Errorf("read profile: ID: %w", err)
	}
	digest, err := model.DigestFromBytes(credential)
	if err != nil {
		return model.Profile{}, fmt.Errorf("read profile: credential: %w", err)
	}
	budgetJSON, err := model.NewJSON(budget)
	if err != nil {
		return model.Profile{}, fmt.Errorf("read profile: handling budget: %w", err)
	}
	if !bytes.Equal(budgetJSON.Bytes(), budget) {
		return model.Profile{}, errors.New("read profile: handling budget is not canonical JSON")
	}
	created, err := parseCanonicalStoreTime(createdText)
	if err != nil {
		return model.Profile{}, fmt.Errorf("read profile: created_at: %w", err)
	}
	updated, err := parseCanonicalStoreTime(updatedText)
	if err != nil {
		return model.Profile{}, fmt.Errorf("read profile: updated_at: %w", err)
	}
	return model.NewProfile(model.ProfileSpec{ID: id, Principal: principal, WorkspaceRoot: workspace,
		Host: model.HostKind(hostText), Runtime: model.RuntimeKind(runtimeText), CredentialHash: digest,
		ActiveAssetRevision: asset, HandlingBudget: budgetJSON, Enabled: enabled == 1,
		CreatedAt: created, UpdatedAt: updated})
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
