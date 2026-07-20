package node

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

var ErrAuthorityInspection = errors.New("inspect existing mnemond authority")

const authorityDigestDomain = "mnemon.node.authority.v1"

// ProfileCredentialVerifier is the read-only owner credential port used by
// existing-authority operations. Node stores and compares only the digest.
type ProfileCredentialVerifier interface {
	Verify(string, model.Digest) error
}

// ProfileCredentialProvisioner extends verification only for the sole Node
// initialization path that may create the owner credential projection.
type ProfileCredentialProvisioner interface {
	ProfileCredentialVerifier
	Ensure(string) (model.Digest, bool, error)
}

// Authority is the transport-neutral projection of the singleton durable
// Node and Teamwork Profile generation. It deliberately contains no schema
// version or wire representation.
type Authority struct {
	Host                model.HostKind
	Runtime             model.RuntimeKind
	Enabled             bool
	AssetRevision       string
	UpdatedAt           time.Time
	PeerID              model.PeerID
	ActiveAssetRevision string
}

type authorityDigestRecord struct {
	ActiveAssetRevision string `json:"active_asset_revision"`
	AssetRevision       string `json:"asset_revision"`
	Domain              string `json:"domain"`
	Enabled             bool   `json:"enabled"`
	Host                string `json:"host"`
	PeerID              string `json:"peer_id"`
	Runtime             string `json:"runtime"`
	UpdatedAt           string `json:"updated_at"`
}

func (authority Authority) Validate() error {
	if !authority.Host.Valid() || !authority.Runtime.Valid() {
		return errors.New("Node authority Host or Runtime is invalid")
	}
	wantedRuntime, ok := model.RuntimeForHost(authority.Host)
	if !ok || authority.Runtime != wantedRuntime {
		return errors.New("Node authority Host and Runtime differ")
	}
	if authority.PeerID.IsZero() {
		return errors.New("Node authority PeerID is invalid")
	}
	if _, err := model.ParseDigest(authority.AssetRevision); err != nil {
		return errors.New("Node authority Profile asset revision is invalid")
	}
	if _, err := model.ParseDigest(authority.ActiveAssetRevision); err != nil ||
		authority.ActiveAssetRevision != authority.AssetRevision {
		return errors.New("Node and Profile asset revisions differ")
	}
	canonical := authority.UpdatedAt.Round(0).UTC()
	if canonical.IsZero() || canonical.UnixNano() <= 0 ||
		!time.Unix(0, canonical.UnixNano()).UTC().Equal(canonical) ||
		authority.UpdatedAt != canonical {
		return errors.New("Node authority update time is invalid")
	}
	return nil
}

// Digest binds lifecycle operations to one exact semantic authority
// generation without depending on a transport envelope or schema version.
func (authority Authority) Digest() (model.Digest, error) {
	if err := authority.Validate(); err != nil {
		return model.Digest{}, err
	}
	raw, err := model.CanonicalMarshal(authorityDigestRecord{
		ActiveAssetRevision: authority.ActiveAssetRevision,
		AssetRevision:       authority.AssetRevision, Domain: authorityDigestDomain,
		Enabled: authority.Enabled, Host: string(authority.Host), PeerID: authority.PeerID.String(),
		Runtime: string(authority.Runtime), UpdatedAt: authority.UpdatedAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return model.Digest{}, errors.New("Node authority cannot be canonicalized")
	}
	return model.Sum(raw), nil
}

// InspectAuthority observes one existing Node while mnemond is stopped. It
// acquires the same exclusive Store writer guard as daemon startup, so an
// active writer fails explicitly. It never initializes, migrates, repairs or
// projects state, and a disabled Profile remains observable for setup repair.
func InspectAuthority(ctx context.Context, workspace string,
	credentials ProfileCredentialVerifier,
) (_ Authority, err error) {
	if ctx == nil || isNilNodeInterface(credentials) {
		return Authority{}, fmt.Errorf("%w: context or credential authority is unavailable",
			ErrAuthorityInspection)
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return Authority{}, fmt.Errorf("%w: %w", ErrAuthorityInspection, contextErr)
	}
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	authority, openErr := openExistingStoredAuthority(ctx, workspace, nodeState, true, credentials)
	if openErr != nil {
		return Authority{}, fmt.Errorf("%w: %w", ErrAuthorityInspection, openErr)
	}
	defer func() {
		if closeErr := authority.store.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("%w: close Store: %v", ErrAuthorityInspection, closeErr))
		}
	}()
	if contextErr := ctx.Err(); contextErr != nil {
		return Authority{}, fmt.Errorf("%w: %w", ErrAuthorityInspection, contextErr)
	}
	return authorityValue(authority.authority)
}

func authorityValue(authority store.LocalAuthority) (Authority, error) {
	result := Authority{Host: authority.Profile.Host(), Runtime: authority.Profile.Runtime(),
		Enabled: authority.Profile.Enabled(), AssetRevision: authority.Profile.ActiveAssetRevision(),
		UpdatedAt: authority.Profile.UpdatedAt(), PeerID: authority.Node.PeerID(),
		ActiveAssetRevision: authority.Node.ActiveAssetRevision()}
	if err := result.Validate(); err != nil {
		return Authority{}, err
	}
	return result, nil
}
