package agency

import (
	"sort"
	"time"
)

const MaxPeerArtifactBytes = 4 << 20

// VerifiedPeerArtifact is machine evidence that one exact referenced Artifact
// was read within the byte bound and matched its digest. Construction is the
// boundary at which a caller asserts that byte-level verification completed.
type VerifiedPeerArtifact struct {
	digest     Digest
	byteSize   int64
	verifiedAt time.Time
}

func NewVerifiedPeerArtifact(digest Digest, byteSize int64,
	verifiedAt time.Time,
) (VerifiedPeerArtifact, error) {
	if digest.IsZero() || byteSize < 0 || byteSize > MaxPeerArtifactBytes {
		return VerifiedPeerArtifact{}, invalid("verified peer Artifact",
			"nonzero digest and bounded byte size are required")
	}
	canonicalVerifiedAt, err := canonicalTime("verified peer Artifact time", verifiedAt)
	if err != nil {
		return VerifiedPeerArtifact{}, err
	}
	return VerifiedPeerArtifact{digest: digest, byteSize: byteSize, verifiedAt: canonicalVerifiedAt}, nil
}

func (artifact VerifiedPeerArtifact) Digest() Digest        { return artifact.digest }
func (artifact VerifiedPeerArtifact) ByteSize() int64       { return artifact.byteSize }
func (artifact VerifiedPeerArtifact) VerifiedAt() time.Time { return artifact.verifiedAt }

// VerifiedPeerDelivery is the only peer-originated admission candidate. It can
// be constructed only from a strictly parsed envelope, machine-resolved local
// source and target Principals, and the complete verified Artifact set. It
// carries no receiver-local consequence selection; that decision belongs
// to the local authority that may later admit it.
type VerifiedPeerDelivery struct {
	delivery  PeerDelivery
	source    AgentPrincipalID
	target    AgentPrincipalID
	artifacts []VerifiedPeerArtifact
}

func NewVerifiedPeerDelivery(parsed ParsedPeerDelivery, localSource, localTarget AgentPrincipalID,
	artifacts []VerifiedPeerArtifact,
) (VerifiedPeerDelivery, error) {
	if !parsed.valid() || localSource.IsZero() || localTarget.IsZero() {
		return VerifiedPeerDelivery{}, invalid("VerifiedPeerDelivery",
			"parsed delivery and machine-resolved local source and target are required")
	}
	verified, err := requireCompletePeerArtifacts(parsed.delivery.artifacts, artifacts)
	if err != nil {
		return VerifiedPeerDelivery{}, err
	}
	if parsed.delivery.RequiresTerminalReplyMatch() &&
		parsed.delivery.originConsequence == ConsequenceResolveCompleted && len(verified) == 0 {
		return VerifiedPeerDelivery{}, invariant("VerifiedPeerDelivery completed reply",
			"requires a verified Artifact")
	}
	return VerifiedPeerDelivery{
		delivery: parsed.delivery.clone(), source: localSource, target: localTarget, artifacts: verified,
	}, nil
}

func requireCompletePeerArtifacts(required []Digest,
	artifacts []VerifiedPeerArtifact,
) ([]VerifiedPeerArtifact, error) {
	if len(artifacts) != len(required) {
		return nil, invariant("VerifiedPeerDelivery Artifacts", "verified set must exactly match delivery refs")
	}
	result := append([]VerifiedPeerArtifact(nil), artifacts...)
	sort.Slice(result, func(i, j int) bool { return result[i].digest.String() < result[j].digest.String() })
	for index, artifact := range result {
		if artifact.digest.IsZero() || artifact.byteSize < 0 || artifact.byteSize > MaxPeerArtifactBytes ||
			artifact.verifiedAt.IsZero() {
			return nil, invalid("VerifiedPeerDelivery Artifacts", "contains incomplete verification metadata")
		}
		if index > 0 && artifact.digest == result[index-1].digest {
			return nil, invalid("VerifiedPeerDelivery Artifacts", "contains a duplicate digest")
		}
		if artifact.digest != required[index] {
			return nil, invariant("VerifiedPeerDelivery Artifacts", "verified set must exactly match delivery refs")
		}
	}
	return result, nil
}

func (verified VerifiedPeerDelivery) Delivery() PeerDelivery { return verified.delivery.clone() }
func (verified VerifiedPeerDelivery) LocalSource() AgentPrincipalID {
	return verified.source
}
func (verified VerifiedPeerDelivery) LocalTarget() AgentPrincipalID {
	return verified.target
}
func (verified VerifiedPeerDelivery) Artifacts() []VerifiedPeerArtifact {
	return append([]VerifiedPeerArtifact(nil), verified.artifacts...)
}

func (verified VerifiedPeerDelivery) InReplyToDelivery() (DeliveryID, bool) {
	return verified.delivery.InReplyToDelivery()
}
