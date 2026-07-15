package model

import (
	"sort"
	"time"
)

type ArtifactRole string

const (
	ArtifactProduced   ArtifactRole = "produced"
	ArtifactReferenced ArtifactRole = "referenced"
)

func (r ArtifactRole) Valid() bool {
	return r == ArtifactProduced || r == ArtifactReferenced
}

type ArtifactRef struct {
	rootDigest Digest
	role       ArtifactRole
}

func NewArtifactRef(rootDigest Digest, role ArtifactRole) (ArtifactRef, error) {
	if rootDigest.IsZero() {
		return ArtifactRef{}, invalid("artifact root", "digest must not be zero")
	}
	if !role.Valid() {
		return ArtifactRef{}, invalid("artifact role", "unknown closed enum value")
	}
	return ArtifactRef{rootDigest: rootDigest, role: role}, nil
}

func (r ArtifactRef) RootDigest() Digest { return r.rootDigest }
func (r ArtifactRef) Role() ArtifactRole { return r.role }

func (r ArtifactRef) MarshalJSON() ([]byte, error) {
	if r.rootDigest.IsZero() || !r.role.Valid() {
		return nil, invalid("artifact_ref", "zero or invalid reference")
	}
	return CanonicalMarshal(struct {
		RootDigest Digest       `json:"root_digest"`
		Role       ArtifactRole `json:"role"`
	}{r.rootDigest, r.role})
}

func normalizeArtifactRefs(refs []ArtifactRef, max int) ([]ArtifactRef, error) {
	if len(refs) > max {
		return nil, limit("artifact refs", len(refs), max)
	}
	result := append([]ArtifactRef{}, refs...)
	for _, ref := range result {
		if ref.rootDigest.IsZero() || !ref.role.Valid() {
			return nil, invalid("artifact refs", "contains a zero or invalid reference")
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].rootDigest.String() == result[j].rootDigest.String() {
			return result[i].role < result[j].role
		}
		return result[i].rootDigest.String() < result[j].rootDigest.String()
	})
	for i := 1; i < len(result); i++ {
		if result[i].rootDigest == result[i-1].rootDigest {
			return nil, invalid("artifact refs", "one root cannot have duplicate or conflicting roles")
		}
	}
	return result, nil
}

type ProvenanceRelation string

const (
	ProvenanceLocalCapture ProvenanceRelation = "local_capture"
	ProvenanceReplica      ProvenanceRelation = "replica"
)

func (r ProvenanceRelation) Valid() bool {
	return r == ProvenanceLocalCapture || r == ProvenanceReplica
}

type ArtifactProvenanceSpec struct {
	RootDigest         Digest
	ProducerEvent      EventKey
	ProducerOriginPeer PeerID
	LocalAgentRun      *RunID
	Operation          *OperationID
	Relation           ProvenanceRelation
	CreatedAt          time.Time
}

type ArtifactProvenance struct {
	rootDigest         Digest
	producerEvent      EventKey
	producerOriginPeer PeerID
	localAgentRun      RunID
	hasLocalAgentRun   bool
	operation          OperationID
	hasOperation       bool
	relation           ProvenanceRelation
	createdAt          time.Time
}

func NewArtifactProvenance(spec ArtifactProvenanceSpec) (ArtifactProvenance, error) {
	if spec.RootDigest.IsZero() || spec.ProducerEvent.IsZero() || spec.ProducerOriginPeer.IsZero() {
		return ArtifactProvenance{}, invalid("artifact provenance", "root and producer identity are required")
	}
	if spec.ProducerEvent.OriginPeerID() != spec.ProducerOriginPeer {
		return ArtifactProvenance{}, invariant("producer Event origin must match provenance origin")
	}
	if !spec.Relation.Valid() {
		return ArtifactProvenance{}, invalid("provenance relation", "unknown closed enum value")
	}
	createdAt, err := canonicalTime(spec.CreatedAt)
	if err != nil {
		return ArtifactProvenance{}, err
	}
	result := ArtifactProvenance{
		rootDigest: spec.RootDigest, producerEvent: spec.ProducerEvent,
		producerOriginPeer: spec.ProducerOriginPeer, relation: spec.Relation, createdAt: createdAt,
	}
	if spec.Relation == ProvenanceLocalCapture {
		if spec.LocalAgentRun == nil || spec.LocalAgentRun.IsZero() || spec.Operation == nil || spec.Operation.IsZero() {
			return ArtifactProvenance{}, invariant("local capture requires AgentRun and operation")
		}
		result.localAgentRun, result.hasLocalAgentRun = *spec.LocalAgentRun, true
		result.operation, result.hasOperation = *spec.Operation, true
	} else if spec.LocalAgentRun != nil || spec.Operation != nil {
		return ArtifactProvenance{}, invariant("replica provenance cannot claim a local AgentRun or operation")
	}
	return result, nil
}

func (p ArtifactProvenance) RootDigest() Digest           { return p.rootDigest }
func (p ArtifactProvenance) ProducerEvent() EventKey      { return p.producerEvent }
func (p ArtifactProvenance) ProducerOriginPeerID() PeerID { return p.producerOriginPeer }
func (p ArtifactProvenance) Relation() ProvenanceRelation { return p.relation }
func (p ArtifactProvenance) CreatedAt() time.Time         { return p.createdAt }
func (p ArtifactProvenance) LocalAgentRunID() (RunID, bool) {
	return p.localAgentRun, p.hasLocalAgentRun
}
func (p ArtifactProvenance) OperationID() (OperationID, bool) { return p.operation, p.hasOperation }
