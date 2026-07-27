package artifact

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	stagingOwnerDomain      = "mnemon/artifact-stage-owner/v1\x00"
	stageOwnerMarkerName    = ".owner.json"
	maxStageOwnerMarkerSize = 1024
)

// StageOwnerKind is a closed Artifact staging authority kind.
type StageOwnerKind string

const (
	StageOwnerOperation StageOwnerKind = "operation"
	StageOwnerInbox     StageOwnerKind = "inbox"
)

// StageOwner identifies one exact staging generation. Its fields are opaque so
// callers cannot construct a non-canonical ID or reuse generation zero.
type StageOwner struct {
	kind       StageOwnerKind
	canonical  string
	generation uint64
}

type stageOwnerMarkerWire struct {
	Kind       StageOwnerKind `json:"kind"`
	ID         string         `json:"id"`
	Generation uint64         `json:"generation"`
}

func NewOperationStageOwner(id model.OperationID, generation uint64) (StageOwner, error) {
	if id.IsZero() {
		return StageOwner{}, fmt.Errorf("%w: zero operation staging owner", ErrCASInput)
	}
	return newStageOwner(StageOwnerOperation, id.String(), generation)
}

func NewInboxStageOwner(id model.InboxID, generation uint64) (StageOwner, error) {
	if id.IsZero() {
		return StageOwner{}, fmt.Errorf("%w: zero Inbox staging owner", ErrCASInput)
	}
	return newStageOwner(StageOwnerInbox, id.String(), generation)
}

func newStageOwner(kind StageOwnerKind, canonical string, generation uint64) (StageOwner, error) {
	if generation == 0 {
		return StageOwner{}, fmt.Errorf("%w: staging generation must be positive", ErrCASInput)
	}
	switch kind {
	case StageOwnerOperation:
		id, err := model.ParseOperationID(canonical)
		if err != nil || id.String() != canonical {
			return StageOwner{}, fmt.Errorf("%w: non-canonical operation staging owner", ErrCASInput)
		}
	case StageOwnerInbox:
		id, err := model.ParseInboxID(canonical)
		if err != nil || id.String() != canonical {
			return StageOwner{}, fmt.Errorf("%w: non-canonical Inbox staging owner", ErrCASInput)
		}
	default:
		return StageOwner{}, fmt.Errorf("%w: unsupported staging owner kind", ErrCASInput)
	}
	return StageOwner{kind: kind, canonical: canonical, generation: generation}, nil
}

func (owner StageOwner) Kind() StageOwnerKind { return owner.kind }

func (owner StageOwner) CanonicalID() string { return owner.canonical }

func (owner StageOwner) Generation() uint64 { return owner.generation }

func (owner StageOwner) IsZero() bool {
	return owner.kind == "" || owner.canonical == "" || owner.generation == 0
}

func (owner StageOwner) validate() error {
	if owner.IsZero() {
		return fmt.Errorf("%w: zero staging owner", ErrCASInput)
	}
	canonical, err := newStageOwner(owner.kind, owner.canonical, owner.generation)
	if err != nil || canonical != owner {
		if err != nil {
			return err
		}
		return fmt.Errorf("%w: non-canonical staging owner", ErrCASInput)
	}
	return nil
}

func (owner StageOwner) directoryName() (string, error) {
	if err := owner.validate(); err != nil {
		return "", err
	}
	generation := make([]byte, 8)
	binary.BigEndian.PutUint64(generation, owner.generation)
	material := make([]byte, 0, len(stagingOwnerDomain)+len(owner.kind)+len(owner.canonical)+10)
	material = append(material, stagingOwnerDomain...)
	material = append(material, owner.kind...)
	material = append(material, 0)
	material = append(material, owner.canonical...)
	material = append(material, 0)
	material = append(material, generation...)
	return hex.EncodeToString(model.Sum(material).Bytes()), nil
}

func (owner StageOwner) markerBytes() ([]byte, error) {
	if err := owner.validate(); err != nil {
		return nil, err
	}
	canonical, err := model.JSONFrom(stageOwnerMarkerWire{
		Kind: owner.kind, ID: owner.canonical, Generation: owner.generation,
	})
	if err != nil || len(canonical.Bytes()) > maxStageOwnerMarkerSize {
		return nil, fmt.Errorf("%w: encode Artifact stage owner marker", ErrCASInput)
	}
	return canonical.Bytes(), nil
}

func parseStageOwnerMarker(content []byte) (StageOwner, error) {
	if len(content) == 0 || len(content) > maxStageOwnerMarkerSize {
		return StageOwner{}, fmt.Errorf("%w: malformed Artifact stage owner marker",
			ErrCASCorruption)
	}
	canonical, err := model.NewJSON(content)
	if err != nil || !bytes.Equal(canonical.Bytes(), content) {
		return StageOwner{}, fmt.Errorf("%w: noncanonical Artifact stage owner marker",
			ErrCASCorruption)
	}
	var wire stageOwnerMarkerWire
	if err := json.Unmarshal(content, &wire); err != nil {
		return StageOwner{}, fmt.Errorf("%w: decode Artifact stage owner marker",
			ErrCASCorruption)
	}
	owner, err := newStageOwner(wire.Kind, wire.ID, wire.Generation)
	if err != nil {
		return StageOwner{}, fmt.Errorf("%w: invalid Artifact stage owner marker",
			ErrCASCorruption)
	}
	expected, err := owner.markerBytes()
	if err != nil || !bytes.Equal(expected, content) {
		return StageOwner{}, fmt.Errorf("%w: Artifact stage owner marker shape differs",
			ErrCASCorruption)
	}
	return owner, nil
}
