// Package authoritycas connects the R7 authority boundary to the existing
// immutable Harness CAS. It contains no Event, Handling, Reference, or case
// semantics.
package node

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
	"github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/authority"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// Adapter is the explicit digest boundary between the R7 agency types and the
// pre-existing byte CAS. The CAS remains the only owner of Artifact bytes;
// the authority store keeps only verified metadata and pins.
type r7ArtifactAdapter struct {
	cas *artifact.CAS
}

func newR7ArtifactAdapter(cas *artifact.CAS) (*r7ArtifactAdapter, error) {
	if cas == nil || cas.Root() == "" {
		return nil, errors.New("authority CAS: immutable CAS is required")
	}
	return &r7ArtifactAdapter{cas: cas}, nil
}

// Put verifies the exact bounded bytes and durably places them in the CAS. It
// returns authority metadata for the caller to catalog in the SQLite writer.
func (adapter *r7ArtifactAdapter) Put(ctx context.Context, content []byte,
	verifiedAt time.Time,
) (authority.VerifiedArtifact, error) {
	if adapter == nil || adapter.cas == nil || ctx == nil {
		return authority.VerifiedArtifact{}, errors.New("authority CAS: unavailable")
	}
	if err := ctx.Err(); err != nil {
		return authority.VerifiedArtifact{}, err
	}
	verified, err := authority.VerifyArtifact(content, verifiedAt)
	if err != nil {
		return authority.VerifiedArtifact{}, err
	}
	legacy, err := legacyDigest(verified.Digest())
	if err != nil {
		return authority.VerifiedArtifact{}, err
	}
	if _, err := adapter.cas.Put(legacy, content); err != nil {
		return authority.VerifiedArtifact{}, fmt.Errorf("authority CAS: put: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return authority.VerifiedArtifact{}, err
	}
	return verified, nil
}

// VerifyArtifact implements authority.ArtifactVerifier. It re-reads the
// complete object under the caller's exact catalog size and re-hashes it on
// every admission attempt; catalog presence alone is never treated as bytes.
func (adapter *r7ArtifactAdapter) VerifyArtifact(ctx context.Context, digest agency.Digest,
	byteSize int64,
) error {
	if adapter == nil || adapter.cas == nil || ctx == nil || digest.IsZero() ||
		byteSize < 0 || byteSize > authority.MaxArtifactBytes {
		return errors.New("authority CAS: invalid verification request")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	legacy, err := legacyDigest(digest)
	if err != nil {
		return err
	}
	content, err := adapter.cas.Read(legacy, int(byteSize))
	if err != nil {
		return fmt.Errorf("authority CAS: read verified object: %w", err)
	}
	if int64(len(content)) != byteSize || agency.Sum(content) != digest {
		return errors.New("authority CAS: object does not match catalog metadata")
	}
	return ctx.Err()
}

// Read returns one verified object for Agent or peer projection. The caller
// receives bytes, never a path into the CAS.
func (adapter *r7ArtifactAdapter) Read(ctx context.Context, digest agency.Digest) ([]byte, error) {
	if adapter == nil || adapter.cas == nil || ctx == nil || digest.IsZero() {
		return nil, errors.New("authority CAS: invalid read request")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	legacy, err := legacyDigest(digest)
	if err != nil {
		return nil, err
	}
	content, err := adapter.cas.Read(legacy, authority.MaxArtifactBytes)
	if err != nil {
		return nil, fmt.Errorf("authority CAS: read: %w", err)
	}
	if agency.Sum(content) != digest {
		return nil, errors.New("authority CAS: read returned mismatched bytes")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return content, nil
}

func legacyDigest(digest agency.Digest) (model.Digest, error) {
	parsed, err := model.ParseDigest(digest.String())
	if err != nil || parsed.IsZero() {
		return model.Digest{}, errors.New("authority CAS: digest cannot cross the adapter boundary")
	}
	return parsed, nil
}
