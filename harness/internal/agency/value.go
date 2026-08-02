package agency

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	MaxSemanticLabelBytes   = 96
	MaxReferenceKeyBytes    = 160
	MaxOpaqueHandleBytes    = 192
	MaxSemanticPayloadBytes = 8 * 1024
	MaxDiagnosticBytes      = 512
	MaxSuccessors           = 16
	MaxArtifactInputs       = 16
	MaxCausationHandles     = 16
	MaxViewConsequences     = 8
	MaxViewTargets          = 64
	MaxViewHandles          = 128
	MaxIntentCanonicalBytes = 12 << 10
	MaxViewCanonicalBytes   = 16 << 10
)

var (
	ErrInvalid   = errors.New("agency: invalid value")
	ErrLimit     = errors.New("agency: limit exceeded")
	ErrInvariant = errors.New("agency: invariant violated")
)

type ValidationError struct {
	Category error
	Field    string
	Problem  string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s: %s: %s", e.Category, e.Field, e.Problem)
}

func (e *ValidationError) Unwrap() error { return e.Category }

func invalid(field, problem string) error {
	return &ValidationError{Category: ErrInvalid, Field: field, Problem: problem}
}

func limit(field string, size, maximum int) error {
	return &ValidationError{Category: ErrLimit, Field: field,
		Problem: fmt.Sprintf("size %d exceeds maximum %d", size, maximum)}
}

func invariant(field, problem string) error {
	return &ValidationError{Category: ErrInvariant, Field: field, Problem: problem}
}

type Digest [sha256.Size]byte

func Sum(value []byte) Digest { return sha256.Sum256(value) }

func ParseDigest(value string) (Digest, error) {
	if !strings.HasPrefix(value, "sha256:") {
		return Digest{}, invalid("digest", "must use sha256:<lowercase-hex>")
	}
	hexValue := strings.TrimPrefix(value, "sha256:")
	decoded, err := hex.DecodeString(hexValue)
	if err != nil || len(decoded) != sha256.Size {
		return Digest{}, invalid("digest", "must use 64 lowercase hexadecimal characters")
	}
	if hexValue != strings.ToLower(hexValue) {
		return Digest{}, invalid("digest", "must use lowercase hexadecimal")
	}
	var digest Digest
	copy(digest[:], decoded)
	if digest.IsZero() {
		return Digest{}, invalid("digest", "must not be zero")
	}
	return digest, nil
}

func (d Digest) String() string { return "sha256:" + hex.EncodeToString(d[:]) }

func (d Digest) IsZero() bool { return d == Digest{} }

type SemanticLabel struct{ value string }

func NewSemanticLabel(value string) (SemanticLabel, error) {
	if err := validateToken("semantic label", value, MaxSemanticLabelBytes, true); err != nil {
		return SemanticLabel{}, err
	}
	return SemanticLabel{value: value}, nil
}

func (l SemanticLabel) String() string { return l.value }
func (l SemanticLabel) IsZero() bool   { return l.value == "" }

type ReferenceKey struct{ value string }

func NewReferenceKey(value string) (ReferenceKey, error) {
	if err := validateToken("reference key", value, MaxReferenceKeyBytes, true); err != nil {
		return ReferenceKey{}, err
	}
	return ReferenceKey{value: value}, nil
}

func (k ReferenceKey) String() string { return k.value }
func (k ReferenceKey) IsZero() bool   { return k.value == "" }

type OpaqueHandle struct{ value string }

func NewOpaqueHandle(value string) (OpaqueHandle, error) {
	if err := validateToken("opaque handle", value, MaxOpaqueHandleBytes, false); err != nil {
		return OpaqueHandle{}, err
	}
	return OpaqueHandle{value: value}, nil
}

func (h OpaqueHandle) String() string { return h.value }
func (h OpaqueHandle) IsZero() bool   { return h.value == "" }

type SemanticPayload struct{ value string }

func NewSemanticPayload(value string) (SemanticPayload, error) {
	if !utf8.ValidString(value) || strings.ContainsRune(value, '\x00') {
		return SemanticPayload{}, invalid("semantic payload", "must be valid UTF-8 without NUL")
	}
	if len(value) > MaxSemanticPayloadBytes {
		return SemanticPayload{}, limit("semantic payload", len(value), MaxSemanticPayloadBytes)
	}
	return SemanticPayload{value: value}, nil
}

func (p SemanticPayload) String() string { return p.value }

func validateToken(field, value string, maximum int, lowercase bool) error {
	if value == "" {
		return invalid(field, "must not be empty")
	}
	if len(value) > maximum {
		return limit(field, len(value), maximum)
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if isASCIILetter(character, lowercase) || isASCIIDigit(character) {
			continue
		}
		if index > 0 && (character == '.' || character == '_' || character == '-' ||
			character == ':' || (!lowercase && character == '/')) {
			continue
		}
		return invalid(field, "contains a non-canonical character")
	}
	last := value[len(value)-1]
	if !isASCIILetter(last, lowercase) && !isASCIIDigit(last) {
		return invalid(field, "must end with an ASCII letter or digit")
	}
	return nil
}

func isASCIILetter(character byte, lowercase bool) bool {
	if character >= 'a' && character <= 'z' {
		return true
	}
	return !lowercase && character >= 'A' && character <= 'Z'
}

func isASCIIDigit(character byte) bool { return character >= '0' && character <= '9' }

type identifier struct{ value string }

func newIdentifier(field, value string) (identifier, error) {
	if err := validateToken(field, value, MaxOpaqueHandleBytes, false); err != nil {
		return identifier{}, err
	}
	return identifier{value: value}, nil
}

type AgentPrincipalID struct{ identifier }
type AttachmentID struct{ identifier }
type EventID struct{ identifier }
type HandlingID struct{ identifier }
type OperationKey struct{ identifier }
type RouteID struct{ identifier }

func NewAgentPrincipalID(value string) (AgentPrincipalID, error) {
	id, err := newIdentifier("AgentPrincipalID", value)
	return AgentPrincipalID{id}, err
}

func NewAttachmentID(value string) (AttachmentID, error) {
	id, err := newIdentifier("AttachmentID", value)
	return AttachmentID{id}, err
}

func NewEventID(value string) (EventID, error) {
	id, err := newIdentifier("EventID", value)
	return EventID{id}, err
}

func NewHandlingID(value string) (HandlingID, error) {
	id, err := newIdentifier("HandlingID", value)
	return HandlingID{id}, err
}

func NewOperationKey(value string) (OperationKey, error) {
	id, err := newIdentifier("operation key", value)
	return OperationKey{id}, err
}

func NewRouteID(value string) (RouteID, error) {
	id, err := newIdentifier("route ID", value)
	return RouteID{id}, err
}

func (id AgentPrincipalID) String() string { return id.value }
func (id AttachmentID) String() string     { return id.value }
func (id EventID) String() string          { return id.value }
func (id HandlingID) String() string       { return id.value }
func (id OperationKey) String() string     { return id.value }
func (id RouteID) String() string          { return id.value }

func (id AgentPrincipalID) IsZero() bool { return id.value == "" }
func (id AttachmentID) IsZero() bool     { return id.value == "" }
func (id EventID) IsZero() bool          { return id.value == "" }
func (id HandlingID) IsZero() bool       { return id.value == "" }
func (id OperationKey) IsZero() bool     { return id.value == "" }
func (id RouteID) IsZero() bool          { return id.value == "" }

type EventRef struct {
	id     EventID
	digest Digest
}

func NewEventRef(id EventID, digest Digest) (EventRef, error) {
	if id.IsZero() || digest.IsZero() {
		return EventRef{}, invalid("Event reference", "ID and digest are required")
	}
	return EventRef{id: id, digest: digest}, nil
}

func (r EventRef) ID() EventID    { return r.id }
func (r EventRef) Digest() Digest { return r.digest }
func (r EventRef) IsZero() bool   { return r.id.IsZero() || r.digest.IsZero() }
func (r EventRef) canonical() any { return eventRefWire{r.id.String(), r.digest.String()} }

type eventRefWire struct {
	ID     string `json:"id"`
	Digest string `json:"digest"`
}

func canonicalJSON(value any) ([]byte, Digest, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, Digest{}, fmt.Errorf("agency: canonical JSON: %w", err)
	}
	return encoded, Sum(encoded), nil
}

func canonicalTime(field string, value time.Time) (time.Time, error) {
	if value.IsZero() {
		return time.Time{}, invalid(field, "must not be zero")
	}
	canonical := value.Round(0).UTC()
	unixNano := canonical.UnixNano()
	if !time.Unix(0, unixNano).UTC().Equal(canonical) {
		return time.Time{}, invalid(field, "must round-trip through Unix nanoseconds")
	}
	encoded := canonical.Format(time.RFC3339Nano)
	decoded, err := time.Parse(time.RFC3339Nano, encoded)
	if err != nil || decoded.UnixNano() != unixNano || !decoded.Equal(canonical) {
		return time.Time{}, invalid(field, "must round-trip through RFC3339Nano")
	}
	return canonical, nil
}

func copyBytes(value []byte) []byte { return append([]byte(nil), value...) }
