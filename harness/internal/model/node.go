package model

import (
	"path/filepath"
	"time"
)

type HostKind string

const (
	HostCodex      HostKind = "codex"
	HostClaudeCode HostKind = "claude-code"
)

func (h HostKind) Valid() bool { return h == HostCodex || h == HostClaudeCode }

type RuntimeKind string

const (
	RuntimeCodexAppServer RuntimeKind = "codex-app-server"
	RuntimeClaudeCLI      RuntimeKind = "claude-cli"
)

func (r RuntimeKind) Valid() bool {
	return r == RuntimeCodexAppServer || r == RuntimeClaudeCLI
}

func RuntimeForHost(host HostKind) (RuntimeKind, bool) {
	switch host {
	case HostCodex:
		return RuntimeCodexAppServer, true
	case HostClaudeCode:
		return RuntimeClaudeCLI, true
	default:
		return "", false
	}
}

type NodeSpec struct {
	PeerID              PeerID
	OriginEpoch         OriginEpoch
	NextOriginSequence  uint64
	ActiveAssetRevision string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

type Node struct {
	spec NodeSpec
}

func NewNode(spec NodeSpec) (Node, error) {
	if spec.PeerID.IsZero() || spec.OriginEpoch.IsZero() {
		return Node{}, invalid("node", "PeerID, origin epoch and positive next sequence are required")
	}
	if err := validateSQLitePositive("next origin sequence", spec.NextOriginSequence); err != nil {
		return Node{}, err
	}
	if err := validateIdentifier("active_asset_revision", spec.ActiveAssetRevision); err != nil {
		return Node{}, err
	}
	createdAt, err := canonicalTime(spec.CreatedAt)
	if err != nil {
		return Node{}, err
	}
	updatedAt, err := canonicalTime(spec.UpdatedAt)
	if err != nil {
		return Node{}, err
	}
	if updatedAt.Before(createdAt) {
		return Node{}, invariant("Node update time precedes creation time")
	}
	spec.CreatedAt, spec.UpdatedAt = createdAt, updatedAt
	return Node{spec: spec}, nil
}

func (n Node) PeerID() PeerID              { return n.spec.PeerID }
func (n Node) OriginEpoch() OriginEpoch    { return n.spec.OriginEpoch }
func (n Node) NextOriginSequence() uint64  { return n.spec.NextOriginSequence }
func (n Node) ActiveAssetRevision() string { return n.spec.ActiveAssetRevision }
func (n Node) CreatedAt() time.Time        { return n.spec.CreatedAt }
func (n Node) UpdatedAt() time.Time        { return n.spec.UpdatedAt }
func (n Node) Spec() NodeSpec              { return n.spec }

type ProfileSpec struct {
	ID                  ProfileID
	Principal           string
	WorkspaceRoot       string
	Host                HostKind
	Runtime             RuntimeKind
	CredentialHash      Digest
	ActiveAssetRevision string
	HandlingBudget      JSON
	Enabled             bool
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

type Profile struct {
	spec ProfileSpec
}

func NewProfile(spec ProfileSpec) (Profile, error) {
	if spec.ID != TeamworkProfileID() {
		return Profile{}, invariant("T0 ProfileID must be teamwork-default")
	}
	if err := validateIdentifier("principal", spec.Principal); err != nil {
		return Profile{}, err
	}
	if !filepath.IsAbs(spec.WorkspaceRoot) || filepath.Clean(spec.WorkspaceRoot) != spec.WorkspaceRoot {
		return Profile{}, invalid("workspace_root", "must be an absolute canonical path")
	}
	if expected, ok := RuntimeForHost(spec.Host); !ok || spec.Runtime != expected {
		return Profile{}, invariant("Host and Runtime adapter must use the frozen T0 mapping")
	}
	if spec.CredentialHash.IsZero() {
		return Profile{}, invalid("credential_hash", "must not be zero")
	}
	if err := validateIdentifier("active_asset_revision", spec.ActiveAssetRevision); err != nil {
		return Profile{}, err
	}
	if _, err := ParseHandlingBudget(spec.HandlingBudget); err != nil {
		return Profile{}, err
	}
	createdAt, err := canonicalTime(spec.CreatedAt)
	if err != nil {
		return Profile{}, err
	}
	updatedAt, err := canonicalTime(spec.UpdatedAt)
	if err != nil {
		return Profile{}, err
	}
	if updatedAt.Before(createdAt) {
		return Profile{}, invariant("Profile update time precedes creation time")
	}
	spec.CreatedAt, spec.UpdatedAt = createdAt, updatedAt
	return Profile{spec: spec}, nil
}

func (p Profile) ID() ProfileID               { return p.spec.ID }
func (p Profile) Principal() string           { return p.spec.Principal }
func (p Profile) WorkspaceRoot() string       { return p.spec.WorkspaceRoot }
func (p Profile) Host() HostKind              { return p.spec.Host }
func (p Profile) Runtime() RuntimeKind        { return p.spec.Runtime }
func (p Profile) CredentialHash() Digest      { return p.spec.CredentialHash }
func (p Profile) ActiveAssetRevision() string { return p.spec.ActiveAssetRevision }
func (p Profile) HandlingBudget() JSON        { return p.spec.HandlingBudget }
func (p Profile) Enabled() bool               { return p.spec.Enabled }
func (p Profile) CreatedAt() time.Time        { return p.spec.CreatedAt }
func (p Profile) UpdatedAt() time.Time        { return p.spec.UpdatedAt }
func (p Profile) Spec() ProfileSpec           { return p.spec }
